#
# coding=UTF-8
# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

'''Interface to LSF. Provides a Popen like class to setup, run and query jobs on an LSF (multi)cluster'''
from __future__ import annotations

import datetime
import logging
import multiprocessing
import os
import queue
import re
import shutil
import signal
import stat
import sys
import threading
import time
from typing import Any, Dict, Tuple
from functools import reduce

import experiment.model.errors
import experiment.runtime.errors

try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess

import experiment.utilities.data
import experiment.appenv
import experiment.model.codes
import experiment.model.executors
import experiment.runtime.monitor
import experiment.runtime.task

from collections import OrderedDict

from typing import OrderedDict as TOrderedDict

#import random



directoryLock = threading.Lock()

moduleLogger = logging.getLogger("LSF")



class LSFStageOutPathError(Exception):
    '''Raised if there is a problem with the paths given to DMStageOutCommand'''

    pass

def ProcessDMTransferStatus(s):
    
    '''Process a single transfer entry.

    Parameters:
        s: A list of strings.
        The string must be the 7 lines output by bdata cache for a transfer
    
    The five lines are
    - Transfer type
    - src
    - TO i.e. the string TO
    - dst
    - blank line
    - the transfer table (2 lines)'''

    s = [el.strip() for el in s]

    data = {}
    data['type'] = s[0]
    data['src'] = s[1]
    data['dst'] = s[3]

    data['table'] = experiment.utilities.data.matrixFromCSVRepresentation("\n".join(s[5:7]), readHeaders=True)

    if data['table'].column('STATUS')[0] in ['TRANSFERRED', 'ERROR']:
        data['isFinished'] = True
    else: 
        data['isFinished'] = False

    return data

def ProcessStageOutDataStatus(d):

    '''Processes the output of bdata cache for a job for OUTPUT transfers'''

    d = d.split('\n')
    start = []
    count  = 0
    for l in d:
        if l[:6] == 'OUTPUT':
            start.append(count)

        count += 1      

    f = []
    for i in start:        
        el = ProcessDMTransferStatus(d[i:i+7])
        f.append(el)

    return f       


def StageInReference(src, dst):

    '''Assembles commands for staging in reference

    In particular efficiently handles directories and executables.

    Params:
        src : The path to the file or dir to be staged in (full path)
        dst:  The destination path (must be full path if a directory)

    Returns:
            A tuple with two elements.
            First element is a list of commands (min 1) that should be added to the LSF job preexec
            The second is an entry to be added to the stage-in dataspec file
    '''

    stage = []
    post = []

    linkString = ""

    log = logging.getLogger()

    if os.path.isdir(src):
        #Archive the dir
        originalSrc = src
        originalDst = dst

        shutil.make_archive(originalSrc, 'gztar', os.path.split(originalSrc)[0], os.path.split(originalSrc)[1])

        src = originalSrc + '.tar.gz'
        dst = originalDst + '.tar.gz'

        #Note: Now stage-in archive to containing directory - this should create it
        post.append('tar -zxf %s -C %s' % (dst, os.path.split(originalDst)[0]))
        post.append('rm %s' % dst)

        log.info('Original: %s -> %s' % (originalSrc, originalDst))
        log.info('New: %s -> %s' % (src, dst))
        log.info('Post: %s' % post)
    else:     
        #Do not link executables - some executables are affected by the directory the executable is in
        try:
            if not (stat.S_IXUSR & os.stat(src)[stat.ST_MODE]):
                linkString = "-link"
        except OSError as error:
            log.warning('OSError while checking if %s is executable' % src)
            log.warning(error)
            log.warning('Ignoring ...')

    stage.append('bstage in -src "%s" -dst %s %s ' % (src, dst, linkString))
    dataSpecString = "%s" % src

    return stage+post, dataSpecString


class DMStageInCommand(experiment.model.executors.AggregatedCommand, experiment.model.executors.BackendCommandExtensions):

    '''A class for handling copying files to a remote host using LSF DataManager

    A DMStageInCommand object is a subclass of AggregatedCommand.'''

    @classmethod
    def commandFromOptions(cls, options):

        '''
        Creates a DMStageInCommand for copying a set of files

        Parameters:
           options: A dictionary with at least one-key, 'payload', which is a list of (src,dst) pairs.
           Any other __init__ keywords can also be given in options.

        Returns:
            An executors.AggregateCommand subclass
        '''

        if experiment.appenv.HybridConfiguration.defaultConfiguration().isHybrid is False:
            log = logging.getLogger('DMStageInCommand')
            log.warning('Skipping lsf-dm-in with options %s because hybrid-configuration is disabled' % options)
            return None

        return DMStageInCommand(**options)

    @classmethod
    def commandFromOptionsAndRewriteRule(cls, options, rule):

        '''As commandFromOptions but applies a rewrite rule to generate destination paths from source paths

        Parameters:
            options: A dictionary with a one or both of the following keys
                payload: Value is a a list of (source,destination) path tuples
                    The rewrite rule is applied to transform the destination paths
                sources: Value is a list of source paths
                    The rewrite rule is applied to the source paths to form the destination paths
            rule: A dictionary with two keys.
                'pattern': A regular expression that identifies strings to replace
                'replacement: The string to substitute
            '''

        dsts = []
        sources = []
        if 'sources' in options:
            for src in options['sources']:
                if rule is not None:
                    dst = re.sub(rule['pattern'], rule['replacement'], src)
                else:
                    dst = src
                dsts.append(dst)
                sources.append(src)

            options.pop('sources')

        if 'payload' in options:
            for src,dst in options['payload']:
                if rule is not None:
                    dst = re.sub(rule['pattern'], rule['replacement'], dst)
                dsts.append(dst)
                sources.append(src)

        options['payload'] = list(zip(sources, dsts))

        return cls.commandFromOptions(options)

    def __init__(self, payload,
             workingDir=None):

        '''Creates an DMStageInCommand instance

        Note: This class overrides AggregatedCommands __init__ to allow the object
        to create the list of Command objects itself

        Parameters:
            payload: A list of (src,dst) path pairs
            workingDir: Working directory for the command.
                Used for writing the dataspec.txt file

        Note: This class does not support resolving command lines in the shell

        Note: Multi-Host Command
             Rewriting is disabled for this class as it would change both src and dst paths.
             Rewrites have to be applied before init is called
        '''

        self.dataSpecStrings = []
        commandList = []
        self.payload = payload
        for ref in payload:
            #Note: By defaults bstage-in will use the LSF jobs CWD as the destination dir
            #This means we could specify remote relative paths and only have to set CWD corrrectly
            #to handle non-mirrored filesystems
            #However this is more difficult when copying directories by taring.
            #This is because we stage in a tara and then afterwards have to copy it.
            #May be able to use LSF envars?
            commands, dataSpecString = StageInReference(ref[0], ref[1])
            for command in commands:
                d = command.split()
                c = experiment.model.executors.Command(executable=d[0],
                                                             arguments=" ".join(d[1:]))
                commandList.append(c)

            self.dataSpecStrings.append(dataSpecString)

        experiment.model.executors.AggregatedCommand.__init__(self,
                                                                    commandList,
                                                                    workingDir=workingDir,
                                                                    environment=None,
                                                                    resolveShellSubstitutions=False,
                                                                    useCommandEnvironment=False)

    def setBackendOptions(self, submitreq):

        '''Sets the required stage-in options in the LSF submitreq structure'''

        # Add -data argument to bsub - it seems this must be done via a file
        # Note: dataspec file must be a absolute real path
        submitreq.dataSpecFile = os.path.join(os.path.realpath(self._workingDir), 'dataspec.txt')
        try:
            with open(submitreq.dataSpecFile, 'w') as f:
                print('#@dataspec', file=f)
                for dataSpecString in self.dataSpecStrings:
                    print(dataSpecString, file=f)
        except Exception as error:
            self.log.warning('Unable to write dataspec file')
            raise experiment.model.errors.FilesystemInconsistencyError(
                    "Unable to write dataspec file",
                    error)

        self.log.debug(
            "Staging in using preExecCommand %s and dataspec file %s" % (self.commandLine, submitreq.dataSpecFile))

        submitreq.options4 = submitreq.options4 | lsf.SUB4_DATA_STAGING_REQ

    # Override rewriting
    def setRewriteRule(self, rule):

        self.log.debug('Not applying rewrites to DMStageInCommand')

    def _applyRewriteRule(self, string):

        return string

class DMStageOutCommand(experiment.model.executors.AggregatedCommand, experiment.model.executors.BackendCommandExtensions):

    '''Handles copying files from one machine to another via DM'''

    @classmethod
    def commandFromOptionsAndRewriteRule(cls, options, rule):

        '''As commandFromOptions but applies a rewrite rule to generate source paths from destination paths

        Use when the source path is some simple rewrite of the destination path

        Parameters:
            options: A dictionary with one key, 'destinations', a list of destination paths.
            The rewrite rule is applied to the destination paths to form the source paths

            rule: A dictionary with two keys.
                'pattern': A regular expression that identifies strings to replace
                'replacement: The string to substitute
            '''

        if experiment.appenv.HybridConfiguration.defaultConfiguration().isHybrid is False:
            log = logging.getLogger('DMStageOutCommand')
            log.warning('Skipping lsf-dm-out with options %s because hybrid-configuration is disabled' % options)
            return None

        srcs = []
        if rule:
            for dst in options['destinations']:
                src = re.sub(rule['pattern'], rule['replacement'], dst)
                srcs.append(src)

        options['payload'] = list(zip(srcs, options['destinations']))
        options.pop('destinations')
        return cls.commandFromOptions(options)

    @classmethod
    def commandFromOptions(cls, options):

        '''
        Creates a DMStageOutCommand for copying a set of files


        Parameters:
           options: A dictionary with one-key, 'payload', which is a list of (src,dst) pairs.
            Other valid __init__ keyword parameters in options will also be used.

        Returns:
            An executors.AggregateCommand subclass
        '''

        return DMStageOutCommand(**options)

    def __init__(self, payload,
                 outputDir=None):

        '''Creates an DMStageOutCommand instance

        Note: This class overrides AggregatedCommands __init__ to allow the object
        to create the list of Command objects itself

        Note: This class does not support resolving command lines in the shell

        Parameters:
            payload: A list of (src,dst) path pairs, or (src) values.
            outputDir: Target directory for stage-out. This is also the receivers workingDir.
                Defaults to the CWD.

            If src is not a full path it will be interpreted relative to the LSF CWD
            If dst is not a full path it will be interpreted relative to the workingDir (output dir)
            If dst if not given the src is copied to the output dir with the same name.
                If src is absolute but is contained by the workingDir it will be made relative to workingDir
                Otherwise an error will be raised

        Note: Multi-Host Command
             Rewriting is disabled for this class as it would change both src and dst paths.

            Since bstage-out can work effectively with relative paths it is up to the caller to rewrite correctly
            any absolute SRC paths they want to pass on init
            '''

        if outputDir is None:
            outputDir = os.getcwd()

        self.outputDir = outputDir

        commands = []
        self.payload = payload
        for el in self.payload:
            if len(el) > 1:
                src,dst = el
                # If the dst does not contain a path it will be interpreted relative to outdir
                # (the cwd) which is set below
                # We could also set it explicitly here but using outdir provides a shortcut
                # to rewrites.
                c = experiment.model.executors.Command(executable="bstage",
                                                             arguments="out -src %s -dst %s " % (src, dst),
                                                             resolveShellSubstitutions=False)
            else:
                src = el
                if os.path.isabs(src):
                    src = os.path.normpath(src)
                    wd = os.path.normpath(outputDir)
                    if src.startswith(wd+os.sep):
                        src = os.path.relpath(src, wd)
                        self.log.warning('Converted absolute src path (%s) contained by input dir to a relative path (%s) as no dst given' % (el, src))
                    else:
                        raise LSFStageOutPathError('Absolute src (%s) not in input dir given without a dst' % el)

                c = experiment.model.executors.Command(executable="bstage",
                                                             arguments="out -src %s" % src,
                                                             resolveShellSubstitutions=False)

            commands.append(c)

        # The CAVEAT to the above is that the CWD is where we think it is. If the stage-out commands are
        # executed in POST_EXEC then the CWD is under /tmp.
        # Note: I think bstage out will actually use LSF_EXCCWD which is still set in post-exec ...
        # Update: It doesn't!!
        commands.insert(0, experiment.model.executors.Command(executable="cd",
                                                                    arguments="$LS_EXECCWD",
                                                                    resolveShellSubstitutions=False))

        experiment.model.executors.AggregatedCommand.__init__(self,
                                                                    commands,
                                                                    workingDir=outputDir,
                                                                    environment=None,
                                                                    resolveShellSubstitutions=False,
                                                                    useCommandEnvironment=False)

    def setBackendOptions(self, submitreq):

        '''Sets LSF stage-out related options in submitreq.

        Constructs output stage-out command based on submitreq.'''

        #Stage out the LSF output file
        #Issue: The outFile is a local path
        #Does this local path A) exists on the remote host or B) not?
        #The remote output-file will be in a different location given (A) or (B)
        #Since we don't know which it will be we can't set the src in bstage out
        #However if only a file name is given it will be written to CWD locally which could be anywhere

        #Solution: We will use $LSB_JOBFILENAME.out
        c = experiment.model.executors.Command(executable='bstage',
                                                     arguments='out -src $LSB_JOBFILENAME.out -dst %s' % submitreq.outFile)
        self.addCommand(c)

        #Set the default output dir
        submitreq.outdir = self.outputDir
        submitreq.options4 = submitreq.options4 | lsf.SUB4_OUTDIR

        self.log.debug("Setting CWD for this command chain to: %s" % self.outputDir)
        self.log.debug("Set output dir on local side to: %s" % submitreq.outdir)

    # Override rewriting
    def setRewriteRule(self, rule):

        self.log.debug('Not applying rewrites to DMStageInCommand')

    def _applyRewriteRule(self, string):

        return string

class DMStageOutDirectoryCommand(DMStageOutCommand):

    '''Handles copying newest files in a directory from one machine to another via DM'''

    @classmethod
    def commandFromOptionsAndRewriteRule(cls, options, rule):

        '''Applies a rewrite rule to generate the source directory path

        Use when the source directory being staged out is some simple rewrite of the destination directory

        Parameters:
            options: A dictionary with one key, destination, whose value is a path (the output directory)
                The rewrite rule is applied to the destination directory to form the source directory
            rule: A dictionary with two keys.
                'pattern': A regular expression that identifies strings to replace
                'replacement: The string to substitute
            '''

        if experiment.appenv.HybridConfiguration.defaultConfiguration().isHybrid is False:
            log = logging.getLogger('DMStageOutDirectoryCommand')
            log.warning('Skipping lsf-dm-out with options %s because hybrid-configuration is disabled' % options)
            return None

        src = options['destination']

        if rule is not None:
            src = re.sub(rule['pattern'], rule['replacement'], options['destination'])

        #FIXME: This should be in the path
        moduleDir = os.path.split(os.path.abspath(__file__))[0]

        if rule:
            moduleDir = re.sub(rule['pattern'], rule['replacement'], moduleDir)

        return cls(src, outputDir=options['destination'], remoteScriptDir=moduleDir)

    def __init__(self, directory, outputDir=None, remoteScriptDir=None):

        if outputDir is None:
            outputDir = os.getcwd()

        self.outputDir = outputDir

        if remoteScriptDir is None:
            remoteScriptDir = os.path.split(os.path.abspath(__file__))[0]

        commands = []
        commands.append(
            experiment.model.executors.Command(executable=os.path.join(remoteScriptDir, "stage-out.sh"),
                                                     arguments=directory))

        experiment.model.executors.AggregatedCommand.__init__(self,
                                                                    commands,
                                                                    workingDir=outputDir,
                                                                    environment=None,
                                                                    resolveShellSubstitutions=False,
                                                                    useCommandEnvironment=False)



class LSFHostUnreachableError(Exception):

    '''Exception for when LSF cannot be contacted'''

    pass

class LSFUnknownJobIdError(Exception):

    '''Exception for when LSF says there is no job with a given id'''

    pass

def IsReservationActive(resid):

    '''Returns True if reservation resid is active

    Parameters:
        resid: The reservation id (string)

     Returns:
        True if reservation is active, false otherwise'''

    numberReservations = 0
    reservationInfo = lsf.lsb_reservationinfo(resid, numberReservations, None)

    print(reservationInfo.name, reservationInfo.timeWindow, reservationInfo.state, file=sys.stderr)

    return reservationInfo


LSF_VERSION: float | None = None
lsfAvailableCodes: Dict[str, str] = {}

try:
    import pythonlsf.lsf as lsf
except ImportError:
    lsf = None
    print("Unable to import pythonlsf - limited LSF functionality will be available", file=sys.stderr)
else:
    LSF_VERSION = float(lsf.THIS_VERSION)

    code_desc_exitReason: Dict[str, Tuple[str, str]] = {
        # Cancelled
        "TERM_ADMIN": ("Job killed by root or LSF administrator", experiment.model.codes.exitReasons["Cancelled"]),
        "TERM_BUCKET_KILL": ("Job killed with bkill -b", experiment.model.codes.exitReasons["Cancelled"]),
        "TERM_OTHER": ("Member of a chunk job in WAIT state killed and requeued after being switched to another",
                       experiment.model.codes.exitReasons["Cancelled"]),
        "TERM_OWNER": ("Job killed by owner", experiment.model.codes.exitReasons["Cancelled"]),
        "TERM_PREEMPT": ("Job killed after premption", experiment.model.codes.exitReasons["Cancelled"]),
        "TERM_REQUEUE_ADMIN": (
            "Job killed and requeued by root or LSF administrator", experiment.model.codes.exitReasons["Cancelled"]),
        "TERM_REQUEUE_OWNER": ("Job killed and requeued by owner", experiment.model.codes.exitReasons["Cancelled"]),

        # Killed
        "TERM_CHKPNT": ("Job killed after checkpointing", experiment.model.codes.exitReasons["Killed"]),
        "TERM_EXTERNAL_SIGNAL": (
            "Job killed by a signal external to LSF", experiment.model.codes.exitReasons["Killed"]),
        "TERM_FORCE_ADMIN": ("Job killed by root or LSF administrator without time for cleanup",
                             experiment.model.codes.exitReasons["Killed"]),
        "TERM_FORCE_OWNER": (
            "Job killed by owner without time for cleanip", experiment.model.codes.exitReasons["Killed"]),

        # ResourceExhausted
        "TERM_CPULIMIT": (
        "Job killed after reaching LSF CPU usage limit", experiment.model.codes.exitReasons["ResourceExhausted"]),
        "TERM_LOAD": (
            "Job killed after load exceeds threshold", experiment.model.codes.exitReasons["ResourceExhausted"]),
        "TERM_MEMLIMIT": (
            "Job killed after reaching LSF memory usage limit",
            experiment.model.codes.exitReasons["ResourceExhausted"]),
        "TERM_PROCESSLIMIT": (
            "Job killed after reaching LSF process limit", experiment.model.codes.exitReasons["ResourceExhausted"]),
        "TERM_RUNLIMIT": (
            "Job killed after reaching LSF run time limit", experiment.model.codes.exitReasons["ResourceExhausted"]),
        "TERM_SWAP": (
            "Job killed after reaching LSF swap usage limit", experiment.model.codes.exitReasons["ResourceExhausted"]),
        "TERM_THREADLIMIT": (
            "Job killed after reaching LSF thread limit", experiment.model.codes.exitReasons["ResourceExhausted"]),

        # SubmissionFailed
        "TERM_CWD_NOTEXIST": ("Current working directory is not accessible or does not exist on the execution host",
                              experiment.model.codes.exitReasons["SubmissionFailed"]),
        "TERM_DATA": ("Error staging job's data", experiment.model.codes.exitReasons["SubmissionFailed"]),
        "TERM_DEADLINE": ("Job killed after deadline", experiment.model.codes.exitReasons["SubmissionFailed"]),
        "TERM_MC_RECALL": ("Job recalled due to max MAX_RSCHED_TIME (the job is sent back to the submission cluster)",
                           experiment.model.codes.exitReasons["SubmissionFailed"]),
        "TERM_PRE_EXEC_FAIL": (
            "Error running job's pre-exec command", experiment.model.codes.exitReasons["SubmissionFailed"]),
        "TERM_WINDOW": (
            "Job killed after queue run window closed", experiment.model.codes.exitReasons["SubmissionFailed"]),

        # SystemIssue
        "TERM_CTRL_PID": (
            "Job killed because the controller PID died", experiment.model.codes.exitReasons["SystemIssue"]),
        "TERM_ORPHAN_SYSTEM": (
            "Orphan Job was automatically terminated by LSF", experiment.model.codes.exitReasons["SystemIssue"]),
        "TERM_RC_RECLAIM": ("Job killed and requeued when an LSF resource connector execution host is reclaimed by EGO",
                            experiment.model.codes.exitReasons["SystemIssue"]),
        "TERM_REMOVE_HUNG_JOB": ("Job removed from LSF because the execution host is unavailable",
                                 experiment.model.codes.exitReasons["SystemIssue"]),
        "TERM_RMS": ("Job exited from an RMS system error", experiment.model.codes.exitReasons["SystemIssue"]),
        "TERM_SLURM": (
            "Job terminated abnormally in SLURM (node failure)", experiment.model.codes.exitReasons["SystemIssue"]),
        "TERM_ZOMBIE": ("Job exited while LSF is not available", experiment.model.codes.exitReasons["SystemIssue"]),

        # UnknownIssue
        "TERM_UNKNOWN": (
            "LSF cannot determine a termination reason", experiment.model.codes.exitReasons["UnknownIssue"]),
    }

    lsfExitReasons: Dict[int, str] = {}
    lsfReasonMap: Dict[int, str] = {}

    for code_name in code_desc_exitReason:
        desc, exitReason = code_desc_exitReason[code_name]
        try:
            lsf_code: int = lsf.__getattribute__(code_name)
        except AttributeError:
            pass
        else:
            lsfExitReasons[lsf_code] = desc
            lsfReasonMap[lsf_code] = exitReason

            lsfAvailableCodes[code_name] = desc

    try:
        _ = lsf.TERM_RC_RECALL
    except AttributeError:
        print("lsf.TERM_RC_RECALL is unknown, this is not a multi-cluster setup", file=sys.stderr)

    lsfStatusToStateMap = {lsf.JOB_STAT_PEND: experiment.model.codes.RESOURCE_WAIT_STATE,
                           lsf.JOB_STAT_PSUSP: experiment.model.codes.SUSPENDED_STATE,
                           lsf.JOB_STAT_SSUSP: experiment.model.codes.SUSPENDED_STATE,
                           lsf.JOB_STAT_USUSP: experiment.model.codes.SUSPENDED_STATE,
                           lsf.JOB_STAT_RUN: experiment.model.codes.RUNNING_STATE,
                           lsf.JOB_STAT_DONE: experiment.model.codes.FINISHED_STATE,
                           lsf.JOB_STAT_EXIT: experiment.model.codes.FAILED_STATE,
                           lsf.JOB_STAT_PDONE: experiment.model.codes.FINISHED_STATE,  #Shouldn't occur with single jobs
                           lsf.JOB_STAT_PERR: experiment.model.codes.FAILED_STATE,  #Shouldn't occur with single jobs
                           lsf.JOB_STAT_WAIT: experiment.model.codes.RESOURCE_WAIT_STATE,
                           lsf.JOB_STAT_UNKWN: experiment.model.codes.POSTMORTEM_STATE  #Perhaps should have unknown state,
                           }


class LSFInfoWrapper(object):

    '''For handling the fact the LSF jobInfo struct may not be thread/process safe'''

    def __init__(self, jobInfo):

        self.status = jobInfo.status
        self.exitStatus = jobInfo.exitStatus
        self.exitInfo = jobInfo.exitInfo
        self.jobPid = jobInfo.jobPid
        self.dstCluster = jobInfo.dstCluster
        self.dstJobId = jobInfo.dstJobId
        self.outFile = jobInfo.submit.outFile
        self.errFile = jobInfo.submit.errFile

        #runRusage is a SWIG type and not pickle-able.
        #Extract the required bits to a dict instead
        self.runRusage = {}
        self.runRusage['power'] = jobInfo.runRusage.power
        self.runRusage['npids'] = jobInfo.runRusage.npids
        self.runRusage['nthreads'] = jobInfo.runRusage.nthreads
        self.runRusage['utime'] = jobInfo.runRusage.utime
        self.runRusage['stime'] = jobInfo.runRusage.stime
        self.runRusage['mem'] = jobInfo.runRusage.mem,
        self.runRusage['swap'] = jobInfo.runRusage.swap,

        #The CPUTime consumed by the job (aggregate seconds across all threads/processes)
        #Equivalent to the 'CPU Time used' line value in bjobs -l
        self.cpuTime = jobInfo.cpuTime
        #The real runtime on the execution host - time to execute pre/main/post
        self.runTime = jobInfo.runTime
        self.maxMem = jobInfo.maxMem
        self.avgMem = jobInfo.avgMem

        self.numExHosts = jobInfo.numExHosts,
        self.numToHosts4Slots = jobInfo.numToHosts4Slots
        self.brunJobTime = jobInfo.brunJobTime
        self.duration = jobInfo.duration

        #The time the job was submitted (on the local host) - SSE
        self.submitTime = jobInfo.submitTime
        self.reserveTime = jobInfo.reserveTime
        #The time the job started running - seconds since Epoch (SSE)
        self.startTime = jobInfo.startTime
        #The job termination time, if finished (SSE)
        self.endTime = jobInfo.endTime
        #The time the job was forwarded
        self.fwdTime = jobInfo.fwdTime
        #The total time the job was in PEND state (includes pre and post running)
        #Unsure if the post running includes Data Transfers (10.1+ only)
        #self.totalPendingTimeJ = jobInfo.totalPendingTimeJ

class LSFJobInfo(object):

    def __init__(self, jobId, remote=False):

        '''
        Raises LSFHostUnreachableError if LSF cannot be contacted
        Raise LSFUnknownJobIdError if LSF says there is no job with the given id

        Latter is usually due to Task.terminate() being called on the job '''

        self.jobId = jobId
        self.log = logging.getLogger("lsf.jobinfo.%s" % jobId)
        #LSF python API has been observed to be returning this when LSF is overloaded
        #Reason unknown but using so this does not lead to jobs being id'd as failures
        lsfOverloadCode = 70366456447680

        numJobs = lsf.lsb_openjobinfo(jobId, None, None, None, None, lsf.ALL_JOB)
        #Shouldn't raise exception in init ...
        if numJobs == -1:
            self.log.warning('LSF errno %d' % lsf.lsb_errno())

            lsf.lsb_perror("LSFJobInfo")
            errno = lsf.lsb_errno()
            if errno == lsf.LSBE_BAD_HOST or errno == lsfOverloadCode:
                #if the host is None and LSF returns LSBE_BAD_HOST
                #(on a normally working system) it implies that LSF has been overloaded with requests
                #and cannot return data
                raise LSFHostUnreachableError("Unable to obtain data from LSF")
            else:
                #NOTE: Task.terminate calls  lsb_deletejob  which removes the job from the batch-system
                #and lsf_readjobinfo will no longer work on it
                #Instantiating LSfJobInfo with that job id after the terminate hence will cause this problem
                raise LSFUnknownJobIdError("No job with id %d exists - may be removed from lsf database" % jobId)
        elif numJobs > 1:
            raise ValueError("LSF inconsistency - expected to return one job for job id %s (%d returned)" % (jobId, numJobs))

        self.readValues = {}
        #Its possible that state may be a bitwise OR of states - particularly if there is a post-processing job attached
        #In this case take the first state 
        self._state = None

        #NOTE: Behaviour has been observed that implies the values in self.jobInfo are mutable
        #For example exitInfo will be a value when printed, say 5, then when exitReason() is executed,
        #which translates this to a Flow exit-reason an incompatible results is returned
        #JIC we will read all these values into ivars of a wrapper class and then use the ivars

        try:
            self.jobInfo = LSFInfoWrapper(lsf.lsb_readjobinfo(None))
        except AttributeError as error:
            #This will occur if lsb_readjobinfo returns None
            self.log.warning('Error reading info for job %s. Error was %s' % (jobId, error))
            self.log.warning('Assuming this is due to error communicating with LSF master')
            raise LSFHostUnreachableError("LSF returned None on request to read info of %s" % jobId)

        if self.jobInfo is None:
            self.log.warning('LSF errno %d' % lsf.lsb_errno())

        try:
            self._state = lsfStatusToStateMap[self.jobInfo.status]
        except KeyError:
            states = list(lsfStatusToStateMap.keys())
            states.sort()
            for state in states:
                if (state & self.jobInfo.status) == state:
                    self._state = lsfStatusToStateMap[state]
                    break

        if self._state is None:
            raise ValueError("LSF returned unknown status value %d" % self.jobInfo.status)

        self.terminationStringPresent = False

        self.log = logging.getLogger("lsf.jobinfo.%s" % jobId)
        if self._state in [experiment.model.codes.FINISHED_STATE, experiment.model.codes.FAILED_STATE]:
            self.log.debug("LSF status %d. Exit Status %d. ExitInfo %d" % (self.jobInfo.status, self.jobInfo.exitStatus, self.jobInfo.exitInfo))
            self.log.debug("State %s. Returncode %s. Signal %s. Exit Reason %s" % (self._state, self.returncode, self.signal, self.exitReason))

        lsf.lsb_closejobinfo()

    def __getstate__(self):

        '''Cant pickle the log ivar - handled here'''

        d = self.__dict__.copy()
        d['log'] = None

        return d

    def __setstate__(self, state):

        self.__dict__.update(state)
        self.log = logging.getLogger("lsf.jobinfo.%s" % self.jobId)

    def outputsTransferStatus(self):

        '''Returns information on the outputs that will be returned for a remote job.

        If job is not remote this method returns None.
        If the job has no output requirements this method returns an empty list.

        FIXME: The data-manager daemon has a tendency not to be contactable.
        At the moment if this occurs it will appear as if the job has no output transfers and
        hence this method will return True'''

        f = None

        if self.isRemote:
            self.log.debug('Checking output transfers of finished job %d@%s' % (self.jobInfo.dstJobId, self.jobInfo.dstCluster))
            process = subprocess.Popen("bdata cache -dmd %s %d" % (self.jobInfo.dstCluster, 
                self.jobInfo.dstJobId), 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE, 
                shell=True,
                universal_newlines=True)
            stdout, stderr = process.communicate()
            if stderr.strip() != "":
                self.log.warning("Error querying data cache using 'bdata cache -dmd %s %d'" % (self.jobInfo.dstCluster, self.jobInfo.dstJobId))
                self.log.warning("bdata stderr: %s", stderr)

            f = ProcessStageOutDataStatus(stdout)

        return f
        

    @property
    def outputsTransfered(self):

        '''Returns True if the output requirements of a remote job have been transferred

        Returns False otherwise.

        If a job has no output requirements this method will return True

        If a job is not remote this method raises ValueError'''

        retval = 'False'

        if self.returncode is not None:
            f = self.outputsTransferStatus()
            if f is None:
                raise ValueError('Job is not remote so no transfers')

            if len(f) > 0:
                retval = reduce(lambda x,y: x and y, [e['isFinished'] for e in f])
            else:
                retval = True

            self.log.debug('There are %d output files. Transfer status is %d' % (len(f), retval))

        return retval            

    @property
    def isRemote(self):     

       '''Returns True if a job has been forwarded to a remote host AND has an id
       
       Note: A job that will be forward may not have been when the JobInfo class was initiated'''

       if self.jobInfo.dstJobId > 0:
           return True

    @property
    def state(self):        

        return self._state

    @state.setter
    def state(self, value):

        self._state = value

    @property
    def outputFile(self):

        '''Returns the name of the file where the job commands output will/is/has gone

        Returns None if no output file is set

        Note: The existance of an output file depends on the details of how the job was launched'''

        return self.jobInfo.outFile

    @property
    def errorFile(self):

        '''Returns the name of the file where the runjob commands error will/is/has gone

        Returns None if no error file is set

        Note: The existance of an error file depends on the details of how runjob was launched'''

        return self.jobInfo.errFile

    @property 
    def isAbnormalTermination(self):

        '''Returns True if the job exited Abnormally

        Exceptions:

        None

        '''

        retval = False
        if self.terminationStringPresent is True:
            if "isAbnormalTermination" in self.readValues:
                retval = True
        else:        
            if (self.jobInfo.status & lsf.JOB_STAT_EXIT) == lsf.JOB_STAT_EXIT:
                retval = True
                self.terminationStringPresent = True
                self.readValues["isAbnormalTermination"] = True

        return retval    

    @property 
    def isNormalTermination(self):

        '''Returns True if the job exited Normally

        Note: LSF and runjob differ in classification of exit(non-zero)
            LSF: exit(non-zero) is considered abnormal termination
            RunJob: exit(non-zero) is normal termination

        Exceptions:

        None
        '''

        retval = False
        if self.terminationStringPresent is True:
            if "isNormalTermination" in self.readValues:
                retval = True
        else:        
            if (self.jobInfo.status & lsf.JOB_STAT_DONE) == lsf.JOB_STAT_DONE:
                retval = True
                self.terminationStringPresent = True
                self.readValues["isNormalTermination"] = True

        return retval    

    @property 
    def isFailedJobStart(self):

        '''Returns True if the job failed to start

        Exceptions:

        None

        '''

        #Lsf also defines exit code 127 to be job failure
        #FIXME: A job specifying an executable that doesn't exist will exit with 127
        #As it stands this will be id'd as SubmissionFailed and a restart attempted ...
        #How to handle??
        retval = False
        if lsfReasonMap[self.jobInfo.exitInfo] == experiment.model.codes.exitReasons["SubmissionFailed"]:
            retval = True
            self.terminationStringPresent = True
            self.readValues["isFailedJobStart"] = True
        elif self.returncode == 127:    
            retval = True
            self.terminationStringPresent = True
            self.readValues["isFailedJobStart"] = True

        return retval    

    @property
    def signal(self):

        '''Returns the signal number recieved by the job (if any)

        If no signal was received returns None.
        If job is not finished returns None.

        Note1: This differs to runjob where this method returns the signal recieved by runjob

        Note2: This assumes that a signal is indicated by exit code > 128
        This is true for all catchable signals - uncatchable signals will have an exit-code
        == signal value and this method will not recognise them!

        Exceptions:

        None

        '''

        retval = None
        if self.state in [experiment.model.codes.FINISHED_STATE, experiment.model.codes.FAILED_STATE]:
            if "signal" in self.readValues:
                retval = self.readValues["signal"]
            else:    
                returncode, signal = divmod(self.jobInfo.exitStatus, 256)
                if signal > 0:
                    retval = signal
                elif returncode > 128:    
                    retval = returncode - 128
                    self.readValues["signal"] = retval

        return retval    

    @property
    def returncode(self):

        '''Returns the job exit code'''

        #LSF gives the exit status from wait
        #This is a 16 bit int whose low byte is the signal number (if there is one)
        #and whose high byte is the exit code
        #We can obtain these bytes in an endian independent manner by dividing by 256 (2^8 = 1 byte)
        #divmod(exitStatus, 256) = (div,mod) where div*256+mod = exitStatus
        #div is the highbyte == exit status
        #mod is the lowbyte == signal

        retval = None
        if self.state in [experiment.model.codes.FINISHED_STATE, experiment.model.codes.FAILED_STATE]:
            if "returncode" in self.readValues:
                retval = self.readValues["returncode"]
            else:    
                retval = divmod(self.jobInfo.exitStatus, 256)[0]

        return retval       

    @property
    def isFinished(self):

        '''Returns True if process is finished, False otherwise'''

        retval = False
        try:
            if self.isNormalTermination or self.isAbnormalTermination or self.isFailedJobStart:
                retval = True
        except Exception as error:
            logging.getLogger("bgq.runjob").warning("Unexpected exception while trying to determine if runjob finished  - %s" % error)

        return retval            

    @property
    def exitReason(self):

        '''Returns the reason for job exit

        Values: Normal, KnownIssue, UnknownIssue, Cancelled, ResourceExhausted, Killed, SubmissionFailed
        
        If job is still running returns None. 
        If the job was force terminated in certain ways this script may return None when the job was killed.
        
        Note: If the runjob file can't be read this method does not raise an exception'''

        log = logging.getLogger('lsf.job')
        reason = None
        code = self.returncode
        terminationCode = self.jobInfo.exitInfo

        #First check for non-zero termination code from lsf
        if terminationCode != 0:
            reason = lsfReasonMap[terminationCode]
        elif self.isFailedJobStart:    
            reason = experiment.model.codes.exitReasons["SubmissionFailed"]
        elif code is not None:
            if code == 0:
                #exit by calling exit(0)
                reason = experiment.model.codes.exitReasons["Success"]
            elif (code > 0) and (code < 128):    
                #This can be because of exit(non-zero) OR  in certain cases when a signal is recieved
                #FIXE: For the moment its assumed the latter case will result in non-zero termination code from LSF
                #Hence 0 < code  < 128 is due to exit(code)
                reason = experiment.model.codes.exitReasons["KnownIssue"]
            elif code > 128:
                #Kills/Cancel signals (SIGTERM/SIGINT/SIGKILL) should be handled via terminationCode
                #i.e. they will cause terminationCode to be non-0
                #Just in case they're not handled some particular cases here
                #FIXME: Assuming the following are SIGINT, SIGKILL, SIGTERM and SIGXCPU
                knownSignals = [2,9,15,24]
                if self.signal not in knownSignals:
                    reason = experiment.model.codes.exitReasons["UnknownIssue"]
                elif self.signal in [2,15]:    
                    reason = experiment.model.codes.exitReasons["Cancelled"]
                elif self.signal == 24:    
                    reason = experiment.model.codes.exitReasons["ResourceExhausted"]
                else:   
                    reason = experiment.model.codes.exitReasons["Killed"]
           
        return reason    

    @property
    def pid(self):

        return self.jobInfo.jobPid

    @property
    def energyUsage(self):

        return {'Power': self.jobInfo.runRusage['power']}


    @property
    def computeResources(self):

        '''Processs and Threads don't seem to bear a relation to reality ...'''

        return {'NumberExecutionHosts':self.jobInfo.numExHosts,
                'NumberSlots':self.jobInfo.numToHosts4Slots,
                'NumberProcesses':self.jobInfo.runRusage['npids'],
                'NumberThreads':self.jobInfo.runRusage['nthreads']
                }

    @property
    def computeUsage(self):

        '''

        Returns:
            RunTime: Total run-time (pre/main/post)
            CPUTime: Cumulative CPU time of the process
            UserTime: Cumulative user-space time of the process
            SystemTime: Cumulative system-time of the process (not working?)


        '''

        return {'UserTime': self.jobInfo.runRusage['utime'],
                'SystemTime': self.jobInfo.runRusage['stime'],
                'CPUTime': self.jobInfo.cpuTime,
                'RunTime': self.jobInfo.runTime
                #Only in v10.1
                #'CPUPercent':self.jobInfo.runRusage.cpuUT,
                #'IO': self.jobInfo.runRusage.io,
                #'IORate': self.jobInfo.runRusage.ioRate,

                }

    @property
    def memoryUsage(self):

        return {'Memory': self.jobInfo.runRusage['mem'],
                'Swap': self.jobInfo.runRusage['swap'],
                'MaxMemory': self.jobInfo.maxMem,
                'AverageMemory': self.jobInfo.avgMem
                }

    @property
    def schedulingEventTimes(self):

        '''Returns the times at which scheduling events occurred'''

        submit = self.jobInfo.submitTime

        return {'submit': self.jobInfo.submitTime,
                'forwarded': self.jobInfo.fwdTime,
                'started': self.jobInfo.startTime,
                'finished': self.jobInfo.endTime
                }

    @property
    def schedulingBreakdown(self):

        '''

        Subsequent test have shown that startTime is the time of main job start and not pre-exec
        However jobInfo.runTime IS the total exec time
        Also fwdTime has the correct time when the job is running, but appears to become 0 on exit
        Duration and brunJobTime are 0 in all tests
        The PendPostTime appears to come from Flow sending a delete after the job has finished but before
        it notices. This delete is regestered in bhist and the delta to when the job finished is counted as pending

        Returns:
            QueueWaitTime - Time from when job was submitted until it was forwarded (if avaulable)
            PendStartTime - Time between forwarding/submit and job main start - (staging data?)
                - This should include transfer job time from host (if any) + pre-exec time
                - Therefore we can get copy over time from
                     - PreExecTime = RunTime - MainRunTime
                     - TransferIn = PendStartTime - PreExecTime
            MainRunTime - Time for main stage run - NOT equivalent to bhists RUN (which is pre-post)
                - This could include post - not clear
            CompletionTime - Total time to job exit from submission - equivalent to bhists TOTAL (usually)
             - If there is post PEND time then we won't id this here and the totals will be different.
            epoch-submitted - Timestamp of when the job was submitted to the LSF backend
            epoch-started - Timestamp of when the job started its flight (stage-in, execution, stage-out)
            epoch-finished - Timestamp of when the job finished its flight

        Timestamps have are in the "%d%m%y-%H%M%S" format.

        Note: Reserving TotalTime for time including bstage-out
        '''

        submit = self.jobInfo.submitTime

        #With 9.1.3 fwdTime seems to be set to 0 after job exit
        if self.jobInfo.fwdTime != 0:
            pendForward = self.jobInfo.fwdTime - submit
        else:
            pendForward = 0

        #The time to wait to start after forwarding
        pendStartMain = self.jobInfo.startTime - submit - pendForward
        mainRunTime = self.jobInfo.endTime - self.jobInfo.startTime
        preExecTime = self.jobInfo.runTime - mainRunTime

        if self.jobInfo.submitTime > 0:
            epoch_submitted = datetime.datetime.fromtimestamp(self.jobInfo.submitTime).strftime("%d%m%y-%H%M%S")
        else:
            epoch_submitted = 'None'

        if self.jobInfo.startTime > 0:
            epoch_started = datetime.datetime.fromtimestamp(self.jobInfo.startTime).strftime("%d%m%y-%H%M%S")
        else:
            epoch_started = 'None'

        if self.jobInfo.endTime > 0:
            epoch_finished = datetime.datetime.fromtimestamp(self.jobInfo.endTime).strftime("%d%m%y-%H%M%S")
        else:
            epoch_finished = 'None'

        return {
            'wait-time': pendForward,
            'pend-start-time': pendStartMain - preExecTime,
            'pre-execution-time': preExecTime,
            'command-execution-time': mainRunTime,
            # totalPendingTimeJ in v10.1+
            # 'PendPostTime': self.jobInfo.totalPendingTimeJ - pendStarted - pendForward,
            # 'TotalTime': self.jobInfo.endTime - self.jobInfo.startTime + self.jobInfo.totalPendingTimeJ,
            # 'TotalPendTime':self.jobInfo.totalPendingTimeJ,
            'completion-time': self.jobInfo.endTime - submit,
            'epoch-submitted': epoch_submitted,
            'epoch-started': epoch_started,
            'epoch-finished': epoch_finished,
        }


def signal_handler(sig, frame):

    if sig == signal.SIGTERM:
        logging.getLogger().warning('Caught finish signal %s - exiting', str(sig))
        sys.exit(0)
    else:
        logging.getLogger().warning('Caught signal %s - ignoring', str(sig))

def RequestHandler(workerId, requestQueue, replyQueue, aliveQueue, loggingDir, notifyInterval=300):

    #Install signal-handler for SIGINT and SIGTERM
    #We need to keep processing requests when elaunch receives CTRL^C etc
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if lsf.lsb_init("st4sd-runtime-core") > 0:
         raise ValueError("Unable to initialise LSF environment")

    #We don't want to use inherited file descriptors so create our own
    #In python3.4+ we would have to do this anyway as fds are not inherited

    #Remove all handlers from root logger
    log = logging.getLogger()
    #Note: Using log.removeHandler results in the handler stream being flushed
    #This in turn uses a lock that is inherited from the calling process
    #This lock can be inherited locked causing a deadlock as it will never appeared freed in this process!
    #Instead we just set the handlers ivar of root logger to an empty list
    log.handlers = []

    #As there are no handlers anymore getting this logger will call basicConfig which will recreate a logger to
    #stderr/stdout inherited from calling process!
    #Incredibly this reuses the lock (perhaps because it reuses the existing handler??) leading to same problem above
    log = logging.getLogger('lsf.requesthandler%s' % workerId)
    log.addHandler(logging.FileHandler(filename=os.path.join(loggingDir, 'lsf_request_handler_%s.log' % workerId)))
    #Remove the handlers added by basicConfig to the rootLogger - probably could just do this the once here
    logging.getLogger().handlers = []

    count = 0
    totalRequests = 0
    intervalRequests = 0
    while True:
        try:
            jobid = requestQueue.get(block=False)
            # Start signal
            log.info('Will process request for %s' % jobid)
            replyQueue.put([jobid, None])
        except queue.Empty:
            pass
        else:
            error = None
            info = None
            try:
                info = LSFJobInfo(int(jobid))
            except LSFHostUnreachableError as e:
                # Host could not be reached - keep last obtained status
                error = e
                log.warning("LSF could not be reached - assuming request overload. Keeping previous status")
            except LSFUnknownJobIdError as e:
                # The job has disappeared from LSF.
                # This will happen if Task.terminate is called as this removes the job from LSF
                # (see terminate() docs for more)
                # In this case self.terminated will be True and we can set the state/exitReason accordingly
                error = e
                log.warning("LSF had no information on job id %s" % jobid)
            except Exception as e:
                error = e
                log.warning("Unexpected exception while trying to obtain status for job with id %s" % jobid)
                log.warning("Exception was %s" % error)
                log.warning("Request Handling process will exit")
                raise

            totalRequests += 1
            intervalRequests += 1
            if error is None:
                log.debug('Got info for %s' % jobid)
                replyQueue.put([jobid, info])
                log.info('Sent info for %s' % jobid)
            elif isinstance(error, LSFUnknownJobIdError):
                log.debug('Got exception for %s' % jobid)
                replyQueue.put([jobid, error])
                log.info('Sent exception for %s' % jobid)

        time.sleep(0.5)
        count += 0.5

        if count > notifyInterval:
            aliveQueue.put([workerId, datetime.datetime.now()])
            log.info('Total requests %d' % totalRequests)
            log.info(
                'Requests last interval %d. Last Interval requests per second %8.2lf' % (intervalRequests, intervalRequests/float(notifyInterval)))
            count = 0
            intervalRequests = 0

def ProcessWorkers(instance):

    '''

    Args:
        instance (LSFRequestArbitrator): The object to call processWorkers on

    Returns:
        None
    '''

    log = logging.getLogger("lsf.task.ProcessWorkkers")

    while True:
        try:
            instance.processWorkers()
        except BaseException as error:
            log.warning('Encountered error processing workers: %s.' % error)
            raise

        time.sleep(20)

class LSFRequestArbitrator(object):
    '''Singleton class for managing requests to LSF'''

    defaultArbitrator = None
    arbitratorLock = threading.Lock()


    @classmethod
    def defaultRequestArbitrator(cls, loggingDir=os.getcwd()):

        if LSFRequestArbitrator.defaultArbitrator is None:
            try:
                LSFRequestArbitrator.arbitratorLock.acquire()
                if LSFRequestArbitrator.defaultArbitrator is None:
                    LSFRequestArbitrator.defaultArbitrator = LSFRequestArbitrator(loggingDir=loggingDir)
            finally:
                LSFRequestArbitrator.arbitratorLock.release()

        return LSFRequestArbitrator.defaultArbitrator

    def __init__(self, numberWorkers=4, requestInterval=60, loggingDir=os.getcwd()):


        self.numberWorkers=numberWorkers
        self.requestHandlers = {}
        self.requestInterval = requestInterval
        self.workerNotifyInterval = 300
        self.requestLock = threading.Lock()

        #Stores the last retrieved job-info for each jobid
        self.jobInfoDict = {}
        #Stores when last request was made for job status
        self.requestStatusDict = {}
        #Tracks if workers are alive
        self.workerStatusDict = {}

        self.requestQueue = multiprocessing.Queue()
        self.replyQueue = multiprocessing.Queue()
        self.aliveQueue = multiprocessing.Queue()
        self.handlerLoggingDir = loggingDir

        pids = []
        for i in range(self.numberWorkers):
             p = multiprocessing.Process(target=RequestHandler,
                                            args=(i,
                                                    self.requestQueue,
                                                    self.replyQueue,
                                                    self.aliveQueue,
                                                    self.handlerLoggingDir,
                                                    self.workerNotifyInterval
                                                    ))
             p.daemon = True
             p.start()
             pids.append(p.pid)
             self.requestHandlers[i] = p

        self.log = logging.getLogger("lsf.task.abritrator")
        for i,pid in enumerate(pids):
             self.log.info('Worker %d PID: %s' % (i, pid))

        t = threading.Thread(target=ProcessWorkers,
                             args=[self])
        t.daemon = True
        t.start()

    def getJobInfo(self, jobId):

        '''Returns the last obtained status for jobId or None if there is None

        Note None can be returned both when the first request has not been processed and
        when no request has been ever been processed succesfully.

        Args:
            jobId (int): The id of the job to get info for

        Returns:
              (LSFJobInfo|Exception|None): If no request has been processed/succeeded yet for jobId None is returned

        Exceptions:

        This methods should not raise any exceptions'''

        #Check last request time

        try:
            self.requestLock.acquire()
            lastRequestTime = self.requestStatusDict.get(jobId)

            #If its more than 60 secs submit and one isn't in flight make a request
            currentTime = datetime.datetime.now()
            requestStatus = False
            if lastRequestTime is not None:
                delta = (currentTime - lastRequestTime)
                if delta.seconds > self.requestInterval:
                    requestStatus = True
                    self.log.debug(
                        'Status request interval exceeed (%d seconds since last request) - acquiring new data ' % delta.seconds)
            else:
                requestStatus = True
                self.log.debug('First status check')

            if requestStatus:
                self.requestStatusDict[jobId] = currentTime
                self.requestQueue.put(jobId)

        finally:
            self.requestLock.release()

        return self.jobInfoDict.get(jobId)

    def cleanJobInfo(self, jobId):

        try:
            self.jobInfoDict.pop(jobId)
            self.requestStatusDict.pop(jobId)
        except KeyError:
            pass

    def processWorkers(self):

        '''Retrieves information from the worker processes

        Also checks they are still running'''

        #Processing information coming from workers
        empty = False
        while empty is False:
            try:
                jobId, jobInfo = self.replyQueue.get(block=False)
                self.log.debug('Reply queue: Got %s %s' % (jobId, jobInfo))
            except queue.Empty:
                empty = True
            else:
                if jobInfo is None:
                    self.requestStatusDict[jobId] = datetime.datetime.now()
                else:
                    self.jobInfoDict[jobId] = jobInfo

        #Get alive updates from workers
        empty = False
        while empty is False:
            try:
                workerId, aliveTime = self.aliveQueue.get(block=False)
                self.log.debug('Alive queue: Got %s %s' % (workerId, aliveTime))
            except queue.Empty:
                empty = True
            else:
                self.workerStatusDict[workerId] = aliveTime

        # Check if anyone is stuck
        # If no-one has been heard from in notifyInterval+60s kill
        now = datetime.datetime.now()
        timeout = self.workerNotifyInterval + 60
        for workerId in list(self.workerStatusDict.keys()):
            delta = now - self.workerStatusDict[workerId]
            if delta > datetime.timedelta(seconds=timeout):
                self.log.warning('Worker process %d has not reported in %f seconds - terminating' % (
                    workerId, delta.total_seconds()))
                self.requestHandlers[workerId].terminate()
                p = multiprocessing.Process(target=RequestHandler,
                                            args=(workerId,
                                                    self.requestQueue,
                                                    self.replyQueue,
                                                    self.aliveQueue,
                                                    self.handlerLoggingDir,
                                                    self.workerNotifyInterval))
                p.daemon = True
                p.start()
                self.log.info('New worker %d PID: %s' % (workerId, p.pid))
                self.requestHandlers[workerId] = p

class Task(experiment.runtime.task.Task):

    '''Represents a executable running on an LSF cluster'''

    @classmethod
    def taskFromOptions(cls, command,
                 queue="Normal",
                 reservation=None,
                 numberProcesses=1,
                 ranksPerNode=1,
                 numberThreads=1,
                 threadsPerCore=1,
                 cwd=None,
                 arguments="",
                 resourceString="",
                 env={},
                 stageInRefs=None,
                 stageOutRefs=None,
                 stdout=sys.stdout,
                 stderr=sys.stderr,
                 shell=True,
                 waitOnOutput=False,
                 statusRequestInterval=20,
                 checkCWD=True):

        '''

                Note: The only required parameter is the command string to execute.

        If numberProcesses > 1 then the job is assumed to be a parallel mpi job.
        It will be launched by mpirun (will be able to set at later date)

        Remote Jobs:
            If the queue given is remote then you may have to specify stagein/stageout requirements
            TODO: Check if LSF will create the specified CWD on remote host if it doesn't exist

        Parameters:
            command - The command to be executed as a string.
            arguments - The commands arguments as a string.
            queue - The queue to submit the job to. Defaults to "Normal".
            reservation - The reservation to submit the job to if one exists.
            cwd - The working directory for the task.
                  If not supplied this will be the current working directory.
            env - A dictionary containing the env-vars for the task.
            stageInRefs - A list of file/directory path tuples.
                    The first element in the tuple is the file/directory to be copied to the remote host
                    The second element in the tuple is the location the file/directory will be copied to.
                    Default None
            stageOutRefs - A list of files paths specifying data to be copied back to the submission host on job completion
                    The data is copied to the jobs working directory
		    Specify "all" to stage-out all files created since jobs launch
                    Default None
            stdout - A file stream open to the file to use for task stdout - defaults to sys.stdout
            stderr - A file stream open to the file to use for task stderr - defaults to sys.stderr
            numberProcesses - The number of processes in the task (for MPI jobs). Default is 1.
            ranksPerNode - The number of processes on a node. Default is 1.
            numberThreads - The number of threads per process
            threadsPerCore - The number of threads to place on each core i.e. number of hardware threads per core
            shell - If True the argument string is pre-processed by the shell
                The shell used will be /bin/sh.
            statusRequestInterval - How often to wait before acquiring new status information i.e. status is cached for this time period
            waitOnOutput - If True the stdout and stderr will be essentially buffered and only be written once job is finished
                This prevents continuous monitoring and copying of the qcloud Task output to the specified stdout/stderr as job runs
            checkCWD - If false the existence of the specified cwd is not checked.
                This is to allow the case where the cwd is not accessible from the client side.


        '''

        executor = experiment.model.executors.Command(executable=command,
                                                            arguments=arguments,
                                                            workingDir=cwd,
                                                            environment=env,
                                                            resolveShellSubstitutions=shell)

        options = {'queue':queue,
                   'resourceString':resourceString,
                   'statusRequestInterval':statusRequestInterval,
                   'reservation':reservation}

        resourceRequest = {'numberProcesses':numberProcesses,
                           'ranksPerNode':ranksPerNode,
                           'numberThreads':numberThreads,
                           'threadsPerCore':threadsPerCore}


        isHybrid = False
        preCommands = []
        if stageInRefs is not None and len(stageInRefs) != 0:
            preCommands.append(DMStageInCommand(stageInRefs))
            isHybrid = True

        postCommands = []
        if stageOutRefs is not None and len(stageOutRefs) != 0:
            postCommands.append(DMStageOutCommand([(el,el) for el in stageOutRefs], outputDir=cwd))
            isHybrid = True

        return Task(executor,
                    preCommands=preCommands,
                    postCommands=postCommands,
                    options=options,
                    resourceRequest=resourceRequest,
                    stdout=stdout,
                    stderr=stderr,
                    waitOnOutput=waitOnOutput,
                    checkCWD=checkCWD,
                    isHybrid=isHybrid)

    def __init__(self, executor,
                 preCommands=[],
                 postCommands=[],
                 options=None,
                 resourceRequest=None,
                 stdout=sys.stdout,
                 stderr=sys.stderr,
                 waitOnOutput=False,
                 checkCWD=True,
                 isHybrid=False,
                 oneRankMPI=False):

        '''

        Args:
            executor:
            preCommands:
            postCommands:
            options:
            resourceRequest:
            stdout:
            stderr:
            waitOnOutput:
            checkCWD:
            oneRankMPI: If true Task requesting 1 processes will still be launched with MPI
                Default False. Tasks requesting 1 process are run as normal.
        '''

        #FIXME: Move to class method
        if lsf.lsb_init("st4sd-runtime-core") > 0:
                raise ValueError("Unable to initialise LSF environment")

        if checkCWD is True:
            if not os.path.exists(executor.workingDir):
                raise ValueError("Invalid working directory specified: %s" % executor.workingDir)

        self.log = logging.getLogger("lsf.task.%s" % os.path.split(executor.endChain.executable)[1])

        self.executionStack = None
        self.options = options
        self.taskid = None
        self.jobOutputStream = None
        self.waitOutput = waitOnOutput
        self.isHybrid = isHybrid
        self.oneRankMPI = oneRankMPI

        self.resourceRequest = {'numberProcesses':1,
                           'ranksPerNode':1,
                           'numberThreads':1,
                           'threadsPerCore':1,
                           'memory': None}

        # VV: Ensure that resourceRequest only makes use of fields that it knows about
        if resourceRequest is not None:
            resourceRequest = {
                key: resourceRequest[key] for key in resourceRequest if key in list(self.resourceRequest.keys())
            }
            self.resourceRequest.update(resourceRequest)

        # Status checking
        self.lastStatusRequestTime = None
        self.statusRequestInterval = 20.0
        if 'statusRequestInterval' in options:
            self.statusRequestInterval = options['statusRequestInterval']

        self.log.debug("Status request interval %lf" % self.statusRequestInterval)
        self.log.debug("Stdout is %s" % stdout)

        self.schedulingData = self.default_performance_info(self.statusRequestInterval)

        if not self.isHybrid:
            # Confirm this is true by checking for stagein/stageout commands
            clist = []
            clist.extend(preCommands)
            clist.extend(postCommands)

            for c in clist:
               if isinstance(c, DMStageInCommand) or isinstance(c, DMStageOutCommand):
                   self.isHybrid = True
                   break

        #State Ivars
        self.terminated = False
        #This is a triplet - state (string), exit reason (string), return code (int)
        self.lastReportedState = (None, None, None)

        #Events and Locks
        self.cancelCopy = threading.Event()
        self.cancelMonitor = threading.Event()
        self.requestLock = threading.Lock()
        self.copyThread = None

        self.outputFileName = stdout.name
        # FIXME: Check given stream is actually attached to a file
        self.stdout = os.fdopen(os.dup(stdout.fileno()), 'a')

        self._constructExecutionStack(executor, preCommands, postCommands)
        self._submitTask()

        #Monitor to ensure everything (threads, streams) is cleanedup correctly in asyncrhonous use i.e. if wait() is not called 
        #Note: This monitor will be disabled if wait() is called (synchronous case)
        self.log.debug('Creating cleanup monitor')
        monitor = experiment.runtime.monitor.CreateDeathAction(lambda: self.isAlive(),
                                                               20.0,
                                                               lambda: self.cleanup(),
                                                               self.cancelMonitor)
        monitor()

        self.log.debug('Task init finished')

    def _constructExecutionStack(self, executor, preCommands, postCommands):

        '''Private Method - Constructs the execution stack'''

        self.log.debug("Constructing Stack")
        self.executionStack = experiment.model.executors.ExecutionStack(target=executor)


        workingDir = self.executionStack.target.workingDir

        for c in preCommands:
            self.executionStack.add(c, level='pre')

        for c in postCommands:
            self.executionStack.add(c, level='post')

        if self.resourceRequest['numberProcesses'] > 1 or self.oneRankMPI:
            # FIXME: Need to insert mpiex in correct place
            mpiExecutor = experiment.model.executors.MPIExecutor.executorFromOptions(self.executionStack.target,
                                                                                           self.resourceRequest)
            self.executionStack.target = mpiExecutor

        # Add some custom pre and post exec commands
        if self.resourceRequest["numberProcesses"] > 1:
            c = experiment.model.executors.Command(executable="cat",
                                                         arguments="$LSB_RANK_HOSTFILE > %s" % os.path.join(workingDir,
                                                                                        'djobs.txt'),
                                                         resolveShellSubstitutions=False)
            self.executionStack.add(c, level="pre")

        c = experiment.model.executors.Command(executable="cat",
                                                     arguments="$LSB_AFFINITY_HOSTFILE > %s" % os.path.join(workingDir,
                                                                                        'affinity.txt'),
                                                     resolveShellSubstitutions=False)
        self.executionStack.add(c, level="pre")

        c = experiment.model.executors.Command(executable="printenv",
                                                     arguments=" > %s/environment.txt" % workingDir,
                                                     resolveShellSubstitutions=False)
        self.executionStack.add(c, level="post")

        # Hybrid
        # If the job has been forwarded to a remote host add some extra pre/post commands
        if self.isHybrid:
            #Add a stage-out to the pre-exec command to notify local side of job start
            #Working dir will be rewritten if a rewrite was specified (it will be the remote)
            ref = os.path.join(workingDir, "started.txt")
            #FIXME: Also need to get the non-rewritten working dir - this is a bit hacky
            #The problem is although DMStageOutCommand contains this information but there may be no stage-out commands
            #We can't use the submission dir as this could be anywhere
            #Here we directly access the (private) workingDir ivar of the end of the execution chain to get this info
            localWorkingDir = self.executionStack.target.endChain._workingDir
            commands = []
            commands.append(experiment.model.executors.Command(executable='echo',
                                                                     arguments="started > %s" % ref,
                                                                     resolveShellSubstitutions=False))
            commands.append(experiment.model.executors.Command(executable='sleep',
                                                                     arguments="2",
                                                                     resolveShellSubstitutions=False))

            #This will stageout to wherever DMStageOutCommand set its outdir to.
            #If there is no DMStageOutCommand then this will be the job submission dir
            commands.append(experiment.model.executors.Command(executable='bstage',
                                                                     arguments="out -src %s -dst %s" % (ref,
                                                                        os.path.join(localWorkingDir, 'started.txt')),
                                                                     resolveShellSubstitutions=False))

            self.executionStack.add(experiment.model.executors.AggregatedCommand(commands), level='pre')

            #Check there is a DMStageOutCommand
            #If there isn't we need to add one in-order to get the output file back
            stageOut = [x for x in self.executionStack.postTarget() if isinstance(x, DMStageOutCommand)]
            if len(stageOut) == 0:
                self.log.info('Adding stage-out command to get job stdout/stderr as no stage-out reqs given')
                #Note: The output file staging out is setup when setBackendOptions is called on this instance
                stageOutCommand = DMStageOutCommand(payload=[], outputDir=localWorkingDir)
                self.executionStack.add(stageOutCommand, level='post')

    @classmethod
    def _parse_lsf_rusage(cls, resource_str: str) \
            -> Tuple[TOrderedDict[str, str], int, int]:
        """Parses just the rusage[key1=value1:key2:value2...]
            in a LSF resource-request string

        Returns a tuple with 3 items:
          1. OrderedDict with the keys and values of rusage[] options
          2. start position of rusage[] string
          3. end position of rusage[] string

        If no valid rusage[] string is found then method returns:
            [OrderedDict(), -1, -1]
        """
        ret = OrderedDict()
        start = end = -1

        resource_str = resource_str or ""
        pattern = re.compile(r"(rusage\[)([^[]*)(\])")
        where = pattern.search(resource_str)

        if where is not None:
            rusage = where.group(2)
            start = where.start()
            end = where.end()
            for key, value in [x.split('=') for x in rusage.split(":")]:
                ret[key.strip()] = value.strip()

        return ret, start, end

    @classmethod
    def _update_rusage(cls, resource_str: str, memory: int | None) -> str:
        """Patches the rusage[k1=v1:k2=v2...] part of a lsf resource string with mem=${memory/(1000*1000)}MB

        Args:
            resource_str: A LSF resource string
            memory: Bytes to request, if set to None this method immediately returns @resource_str

        Returns:
            The patched LSF resource string (lsf.submit().resReq)
        """
        if memory is None:
            return resource_str

        # VV: The idea is to extract rusage[] from the resource string, patch its
        # `mem` key-value, put the whole thing back into a string while maintaining the order of options
        # and finally rewrite resource_str (specifically the `rusage[...]` part).
        (rusage_contents, start, end) = cls._parse_lsf_rusage(resource_str)

        # VV: Specify memory in MB
        # This is the default for LSF
        # https://www.ibm.com/docs/en/spectrum-lsf/10.1.0?topic=requirements-resource-requirement-strings
        # TODO: LSF administrators can change the default unit for limits (LSF_UNIT_FOR_LIMITS in lsf.conf)
        # Find a way to detect LSF_UNIT_FOR_LIMITS (doesn't appear to be included in pythonlsf.lsf)
        rusage_contents["mem"] = f"{memory/(1000.0*1000.0)}"

        options_str = ':'.join([f"{key}={rusage_contents[key]}" for key in rusage_contents])
        rusage = f"rusage[{options_str}]"

        if start != -1:
            prefix = resource_str[:start]
            postfix = resource_str[end:]
            whole = ''.join((prefix, rusage, postfix))
        elif resource_str:
            whole = ' '.join((resource_str, rusage))
        else:
            whole = rusage

        return whole

    def _processResourceRequest(self, submitreq, numberProcesses, ranksPerNode, numberThreads, threadsPerCore, memory):

        '''Set LSF resource request and affinity based on arguments'''

        #
        # Note: LSF deals in processors i.e. CPUs (or possibly hwthreads)
        # MPIrun and this class deal in processes e.g. ranks
        # This distinction is important when you want > 1 thread per process
        #
        # MPI Run Terminolgy:
        # ppr == processes per resource (syntax ppr:$NumberProcesses:$Resource)
        # PE == Processing Elements (per process) PE=2 - 1 processing element == 1 core by default
        #
        # LSF                           INPUT                                                MPIRun
        #
        # numberProcessors              numberProcesses*(threadsPerProcess/threadsPerCore)   (-n)*(PE=x)
        # processorsPerNode [ptile]     ranksPerNode*(threadsPerProcess/threadsPerCore)      (ppr:X:node)*(PE=x)
        #                              numProcesses                                          -n
        #                              threadsPerProcess                                    (-PE=x)*? (hw threads aren't specified to mpirun)
        #                              ranksPerNode                                         (map-by ppr:rpn:node)
        #
        # If a queue is exclusive you could use numProcessors == numProcesses and ptile == ranksPerNoppr:rpn:de
        # However this is a abuse of lsf as you intend to use e.g. 36 processors, but request 12 with 3 per node
        # and because the queue is exclusive you know the job will also have access to the other CPUs on each node.

        # Affinity:
        # LSF - Allocate slots on hosts
        # mpiRun - Will bind the mpi-tasks to subsets of the resources
        #
        # However LSF does not exclusively allocate CPUS on a host!
        # For example Node with 12 CPUS - run 2 MPI jobs each with 6 ranks
        #
        # LSF allocates 6 slots on the node to job1, 6 to job2 - the node is closed
        #
        # mpirun sees the hosts as 6*nodename - its does NOT see the cpus.
        # The first MPIrun binds 6 task to the node, 1 CPU per task, starting with the first
        # The second mpirun does the exact same - both jobs are bound to the same cores!
        #
        # Two ways around - don't bind and let the OS migrate tasks to CPUs
        # Use affinity
        #
        # This gives $coresPerProcess specific cores to each MPI tasks.
        # The tasks is given exclusive use of the cores from all jobs
        # affinity syntax = affinity[core($coresPerProcess),exclusive(core,alljobs)]
        # Memory requests go under `rusage[mem=MEGABYTES]
        # Actually, MEGABYTES is the default unit in LSF but it is configurable via
        # the LSF_UNIT_FOR_LIMITS option in lsf.conf)

        coresPerProcess = numberThreads/threadsPerCore
        # In general allocation assumes a minimum of 1 core per MPI task
        # The way to request this depends on if affinity and/or PARALLEL_SCHED_BY_SLOT is on
        #
        # CASE 1: 1 slot == 1 core
        # Request numProcessors == number tasks*coresPerTask - same for affinity
        #
        # CASE 2: 1 slot == 1 hw thread no affinity (PARALLEL_SCHED_BY_SLOT=Y)
        # Request numProcessors == num hw threads
        # This is to take the correct number of cores from LSF and prevent LSF scheduling more jobs than you want onto a node
        #
        # CASE 3: 1 slot == 1 hw thread and affinity
        # Request numberProcesses == num Tasks*coresPerTask
        # Affinity will place each rank in the correct place and prevent other jobs taking slots not requested (if alljobs is supported)
        # For example with affniity on
        # numberProcessors=4 - Request 4 total slot
        # ptile=4 - Request 4 processors (slots) per node
        # affinity(core(1,exclusizve(core,alljobs) - Each slot is associated with a core exclusively
        #
        # If the above was done using numberProcesses == numhwthreads thren
        # numberProcessors=32 - Request 32 total slots (hw-threads)
        # ptile=32 - Request 32 hwthreads per node (this would be 4 cores on P8)
        # affinity(core(1,exclusizve(core,alljobs) - Each slot is associated with a core exclusively
        # Job fails! as you've ask for 32 hwthreads each on its own core ....
        #
        #
        # Affinity is applied to each task. In LSF the number of tasks on a node is equivalent to submitReq.numProcessors

        # We could set ptile independently but its not sure how this is interperted w.r.t number of tasks

        submitreq.numProcessors = numberProcesses
        submitreq.maxNumProcessors = numberProcesses
        # We request ranksPerNode*coresPerProcess processors per node (ptile) (SLOTS)
        # For each task we request coresPerProcess cores exclusively

        # VV: At this point we prefix current resReq (which is provided by user) with CPU-request
        resReq = "span[ptile=%d] affinity[core(%d,exclusive=(core,alljobs))]" % (
                ranksPerNode, coresPerProcess)
        if submitreq.resReq:
            resReq = " ".join((resReq, submitreq.resReq))

        # submitreq.resReq = "span[ptile=%d]" % (ranksPerNode)

        # VV: Next, we patch the rusage[] substr in resReq (if found) else, we add it as a suffix
        resReq = self._update_rusage(resReq, memory)

        submitreq.resReq = resReq

    def _pretty_print(self, obj: Any):
        """Iterates the attributes of an object and prints them with self.log.debug

        **Do not** try to use this method on an @obj with fields that are properties which are not stateless. This
        method will INVOKE these methods.
        """

        fields = dir(obj)

        for x in fields:
            if x.startswith("_"):
                continue
            try:
                self.log.debug(f"  {x}: {obj.__getattribute__(x)}")
            except Exception as e:
                self.log.debug(f"  Cannot print {x} due to {type(e)}: {e} - will continue")


    def _submitTask(self):     

        '''Private Method - Submits the task to LSF'''

        self.log.debug("Submitting job")

        # NOTE: In the case of remote jobs symbolic links can't be staged.
        # This means we have to ensure all paths given to LSF are real
        # NOTE: The pre-submission shell expansion behaviour is set in the target

        # CONSTRUCT SUBMIT REQUEST
        # 1. Pre/Main/Post Command Lines
        # 2. Resource Limits
        # 3. Resource Requests + Affinity
        # 4. Output and Working Dir
        # 5. Environment

        submitreq = lsf.submit()
        submitreq.options = 0
        submitreq.options2 = 0
        submitreq.options3 = 0
        submitreq.options4 = 0

        #RESOURCE LIMITS

        limits = []
        for i in range(0, lsf.LSF_RLIM_NLIMITS):
                limits.append(lsf.DEFAULT_RLIMIT)

        #Note: you must set limits before assiging to submitreq structure
        #This was a HACK for Unilever Trumbull POC but keeping here to
        #show how to set WALL TIME
        #There is some bug with restarting (about 5% of time) with LSF
        #Here setting walltime to upperlimit directry and adding jitter
        #as the bug seems to be casued when multiple process restart together
        if self.options.get("walltime") is not None:
            limits[lsf.LSF_RLIMIT_RUN] = self.options['walltime']*60
        else:
            self.log.warning('No walltime specified - default to 60mins')
            limits[lsf.LSF_RLIMIT_RUN] = 60 * 60

        submitreq.rLimits = limits
        submitreq.beginTime = 0
        submitreq.termTime = 0
        submitreq.resReq = ""

        # VV: First set defined resource request string
        if self.options.get("resourceString"):
            submitreq.resReq = f"{self.options['resourceString']}"

        # VV: Then prefix the resourceString request with CPU request and patch the resulting
        # string with memory request (i.e. `rusage[mem=MEGABYTES]")
        self._processResourceRequest(submitreq, **self.resourceRequest)

        #Quote resource requirements
        submitreq.resReq = "\"%s\"" % submitreq.resReq

        #OPTION SET 1
        #FIXME: Setting stdout == stderr for now - implied by not setting anything for stderr
        # FIXME: If hybrid and no DMStageOutCommand is passed the output file will not be staged back
        # We have to create a DMStageOutCommand if there is none
        submitreq.outFile = self.outputFileName
        submitreq.queue = self.options['queue']

        #submitreq.loginShell = "bash"
        submitreq.options = submitreq.options | lsf.SUB_OUT_FILE | lsf.SUB_QUEUE | lsf.SUB_RES_REQ #| lsf.SUB_LOGIN_SHELL

        #OPTIONS SET 2
        submitreq.options2 = submitreq.options2 | lsf.SUB2_OVERWRITE_OUT_FILE

        if self.options.get("reservation") is not None:
            submitreq.rsvId = self.options["reservation"]
            self.log.info("Reservation %s" % submitreq.rsvId)
            submitreq.options2 = submitreq.options2 | lsf.SUB2_USE_RSV

        #OPTIONS SET 3
        submitreq.cwd = self.executionStack.target.workingDir
        self.log.debug("Set cwd to %s" % submitreq.cwd)
        submitreq.options3 = submitreq.options3 | lsf.SUB3_CWD
        #Might be useful
        #submitreq.options3 = lsf.SUB3_CREATE_CWD_DIRECTORY

        #
        #BACKEND SPECIFIC PRE/POST COMMAND MODIFICATIONS
        #
        for c in self.executionStack.preTarget():
            if isinstance(c, experiment.model.executors.BackendCommandExtensions):
                c.setBackendOptions(submitreq)

        for c in self.executionStack.postTarget():
            if isinstance(c, experiment.model.executors.BackendCommandExtensions):
                c.setBackendOptions(submitreq)

        #
        #COMMAND LINES
        #
        #Have to resolve the command lines before setting environment variables
        #since  for LSF 9.1.2 the envars have to be added to the command line
        #Note: setting post/pre exec with empty strings can cause seg faults

        submitreq.command = self.executionStack.target.commandLine
        self.log.info("Command is %s" % submitreq.command)

        # SUB_PRE_EXEC only available in 9.13+
        if LSF_VERSION > 9.12 and len(self.executionStack.preTarget()) > 0:
            submitreq.options = submitreq.options | lsf.SUB_PRE_EXEC
            submitreq.preExecCmd = "; ".join([el.commandLine for el in self.executionStack.preTarget()])

        if len(self.executionStack.postTarget()) > 0:
            submitreq.options3 = submitreq.options3 | lsf.SUB3_POST_EXEC
            submitreq.postExecCmd = "; ".join([el.commandLine for el in self.executionStack.postTarget()])

        #OPTIONS SET 4 (Environment)

        #Convert self.env into a string for LSF
        #
        #NOTE: LSF requires comma with no space between envvars
        #Furthermore it appears LSF does not expand some environment variables e.g. $HOME
        #As a result all are being expanded here
        #NOTE2: Only variables which exist in the calling programs environment are expanded
        #Variables which are defined and used in env are not expanded ...

        env = self.executionStack.target.environment
        # Check if OMP_NUM_THREADS is in env
        # If present remove it - it will be readded at submission time with the value of self.numberThreads
        if "OMP_NUM_THREADS" in env:
            env.pop("OMP_NUM_THREADS")

        temp = ["%s=%s" % (key, os.path.expandvars(env[key])) for key in env if env[key].strip() != ""]
        envstring = ",".join(temp)

        #Add OMP vars
        #Note: Using OpenMP v3.1 envars 
        #With OpenMP 4.0 could also use OMP_PLACES and more precise OMP_PROC_BIND options
        #envstring = "OMP_DISPLAY_ENV=TRUE,OMP_PROC_BIND=TRUE,OMP_NUM_THREADS=%d,%s" % (self.numberThreads, envstring)
        envstring = "OMP_DISPLAY_ENV=TRUE,OMP_NUM_THREADS=%d,%s" % (self.resourceRequest['numberThreads'], envstring)

        # Add Docker Image environment
        if self.options.get('dockerImage') is not None:
            envstring = "LSB_CONTAINER_IMAGE=%s,%s" % (self.options['dockerImage'], envstring)
            submitreq.app = self.options['dockerProfileApp']
            submitreq.options3 = submitreq.options3 | lsf.SUB3_APP
        # Add Docker options to env variable; this is then loaded by script specified in application profile!
        if self.options.get('dockerOptions') is not None:
            envstring = "DOCKER_OPTIONS=%s,%s" % (self.options['dockerOptions'], envstring)

        #Check length of environment and emit warning if > 4096 (LSF env limit in 9.13+)
        if len(envstring) > 4096:
            self.log.warning("Environment exceeds 4096 bytes in length (%d) - this may cause an error" % len(envstring))

        #SUB4_SUBMISSION_ENV_VARS only available in 9.13+
        if LSF_VERSION > 9.12:
            submitreq.subEnvVars=envstring
            submitreq.options4 = submitreq.options4 | lsf.SUB4_SUBMISSION_ENV_VARS
        else:		
            #Prepend env-vars to command string
            #First, repace commas with spaces
            envstring = envstring.replace(',', " ")
            submitreq.command = "%s %s" % (envstring, submitreq.command)

        #submitreq.networkReq = "type=sn_all:protocol=mpi:usage=shared:mode=us:instance=1"

        #Print 
        self.log.debug("rlimits:%s" % submitreq.rLimits)
        self.log.debug("beginTime:%d" % submitreq.beginTime)
        self.log.debug("termTime:%d" % submitreq.termTime)
        self.log.debug("numProcessors:%d" % submitreq.numProcessors)
        self.log.debug("outFile:%s" % submitreq.outFile)
        self.log.debug("queue:%s" % submitreq.queue)
        self.log.debug("res-req:%s" % submitreq.resReq)
        self.log.debug("cwd:%s" % submitreq.cwd)

        if LSF_VERSION > 9.12:
            self.log.debug("env:%s" % submitreq.subEnvVars)

        if len(submitreq.preExecCmd) > 4094:
            self.log.warning('pre-exec command exceeds max length 4094 (length %d)' % len(submitreq.preExecCmd))
            self.log.warning("pre:%s" % submitreq.preExecCmd)
        else:     
            self.log.debug("pre:%s" % submitreq.preExecCmd)

        if len(submitreq.postExecCmd) > 4094:
            self.log.warning('post-exec command exceeds max length 4094 (length %d)' % len(submitreq.postExecCmd))
            self.log.warning("post:%s" % submitreq.postExecCmd)
        else:     
            self.log.debug("post:%s" % submitreq.postExecCmd)

        #NOTE: The SUBCWD will be the currentdir
        #However since threads may be chaning the current dir we can't rely on this
        #Therefore have to be careful of anything using LSB_SUBDIR value e.g. LSB_OUTDIR
        #is set explicitly.

        #Stores the reply from LSF
        submitreply = lsf.submitReply()
        self.taskid = lsf.lsb_submit(submitreq, submitreply)

        if self.taskid == -1:
            lsf.lsb_perror("lsb_submit returned -1 for task id. Reason: ")
            self.log.debug(f"LSF SubmitReq -> {self.taskid}")
            self._pretty_print(submitreq)
            self.log.debug("LSF SubmitReply")
            self._pretty_print(submitreply)
            self.log.info('LSF errno %d' % lsf.lsb_errno())

        #FIXME: Handle job submission failure here?

        self.log.info("Job submitted. Id %s" % self.taskid)

    @property
    def schedulerId(self):
        """Returns the ID of the job which LSF creates"""
        return self.taskid

    def _requestStatus(self):    

        '''Returns status of a job. 

        The status is obtained from the QCloud instance running the job.
        
        The status returned is a tuple with the following elements
        
        (state, exitreason, returncode)'''

        #NOTE: qcloud returns a experiment.bgq.RunJobCommand instance

        #Once the job is finished/failed stop asking LSF about it

        #Lock checking lastReportedState to prevent read/write clashes
        getNewState = True
        try:
            #self.log.debug('Acquiring request lock 1')
            self.requestLock.acquire()
            if self.lastReportedState[0] in [experiment.model.codes.FAILED_STATE, experiment.model.codes.FINISHED_STATE]:
                getNewState = False
        finally:
            #self.log.debug('Releasing request lock 1')
            self.requestLock.release()
            #self.log.debug('Released request lock 1')

        if not getNewState:
            self.log.debug('Returning fixed state info. %s %s %s' % self.lastReportedState)
            #Remove this task from the abitrator since we no longer need to store data on it
            LSFRequestArbitrator.defaultRequestArbitrator().cleanJobInfo(self.taskid)
            return self.lastReportedState

        self.log.debug('Checking status of job %s' % self.taskid)
        state = experiment.model.codes.INITIALISING_STATE
        exitreason = None
        returncode = None
        #Lock so multiple process don't write to lastReportedState
        #self.log.debug('Acquiring request lock 3')

        last_epoch_info = {
            'epoch-submitted': 'none',
            'epoch-started': 'none',
            'epoch-finished': 'none',
        }

        self.requestLock.acquire()
        try:
            info = LSFRequestArbitrator.defaultRequestArbitrator().getJobInfo(self.taskid)
        except Exception as error:
            #getJobInfo should not raise exceptions - its a bug if it does
            self.log.warning("Exception while trying to obtain status for job with id %s" % self.taskid)
            self.log.warning("Exception was %s" % error)
            state = experiment.model.codes.FAILED_STATE
            returncode = 1
            exitreason = experiment.model.codes.exitReasons['UnknownIssue']
        else:
            if isinstance(info, LSFJobInfo):
                job = info
                state = job.state
                #These will be None if job is not in experiment.codes.FAILED_STATE or FINISHED_STATE
                returncode = job.returncode
                exitreason = job.exitReason

                idx_transfer_estimate = self.schedulingData.indexOfColumnWithHeader("transfer-out-time-estimate")
                idx_completion_time = self.schedulingData.indexOfColumnWithHeader("total-time")

                self.log.debug("Status of job %s: %s (returncode %s. exitReason %s)" % (self.taskid, job.state, returncode, exitreason))

                #Update scheduling info
                #Wait time and pend-start-time need to be handled differently due to bug in LSF
                #The bug will cause these to change on job completion
                #wait-time will go to zero and pend-start-time will become the sum of prevous values.
                #To avoid this we don't change these after they are first set.
                currentInfo = job.schedulingBreakdown
                #FIXME: The following is possible
                #1. wait-time: non-zero pend-start-time: 0 : wait-time is set
                #2. wait-time: zero pend-start-time:value : pend-start-time is set (will include the wait time)
                for el in ['wait-time', 'pend-start-time']:
                    index = self.schedulingData.indexOfColumnWithHeader(el)
                    if currentInfo[el] > 0 and self.schedulingData.matrix[0][index] == -1:
                        self.schedulingData.matrix[0][index] = currentInfo[el]

                # VV: Cache most recent epoch info but store it *after* returncode in the `finally` section
                for epoch in last_epoch_info:
                    last_epoch_info[epoch] = currentInfo[epoch]

                #TODO: Can these change once greater than
                #If not we can combine with above ....
                for el in ["pre-execution-time", "command-execution-time", "completion-time"]:
                    if currentInfo[el] > 0:
                        index = self.schedulingData.indexOfColumnWithHeader(el)
                        self.schedulingData.matrix[0][index] = currentInfo[el]

                #If job is remote wait for output data before setting state to finished
                if job.state in [experiment.model.codes.FAILED_STATE, experiment.model.codes.FINISHED_STATE] and job.isRemote:
                    if not job.outputsTransfered:
                        state = experiment.model.codes.OUTPUT_DATA_WAIT_STATE
                        self.log.info('Waiting on data transfer to complete')
                    else:
                        #Output transfered or no outputs - update matrix
                        finishedTime = job.schedulingEventTimes['finished']
                        transferEstimate = int(time.time()) - finishedTime
                        self.schedulingData.matrix[0][idx_transfer_estimate] = transferEstimate
                        self.schedulingData.matrix[0][idx_completion_time] = currentInfo['completion-time'] + transferEstimate
                else:
                    self.schedulingData.matrix[0][idx_transfer_estimate] = 0
                    self.schedulingData.matrix[0][idx_completion_time] = currentInfo['completion-time']
            elif isinstance(info, LSFUnknownJobIdError):
                # The job has disappeared from LSF.
                # This will happen if Task.terminate is called as this removes the job from LSF
                # (see terminate() docs for more)
                # In this case self.terminated will be True and we can set the state/exitReason accordingly
                if self.terminated:
                    self.log.warning("terminate called on Task instance of job %s. This deleted job record from LSF. "
                                  "Setting state to Failed and exit-reason to Killed " % self.taskid)
                    exitreason = experiment.model.codes.exitReasons['Killed']
                else:
                    # The job could have been deleted from LSF externally via e.g. bkill -r
                    # but its unknown if other possibilities exist
                    # In this case the job state is set to Failed but exit-code to UnknownIssue
                    self.log.warning("LSF has no record for job %s - perhaps it was externally removed? "
                                  "Setting state Failed and exit-reason to UnknownIssue" % self.taskid)
                    exitreason = experiment.model.codes.exitReasons['UnknownIssue']

                state = experiment.model.codes.FAILED_STATE
                returncode = 1
        finally:
            #Store last state - we will use above to stop checking if job is failed
            self.lastReportedState = (state, exitreason, returncode)
            self.log.debug('Set new last reported state to %s %s %s' % self.lastReportedState)

            for epoch in ['epoch-submitted', 'epoch-started', 'epoch-finished']:
                index = self.schedulingData.indexOfColumnWithHeader(epoch)
                if self.schedulingData.matrix[0][index].lower() == 'none':
                    self.schedulingData.matrix[0][index] = last_epoch_info[epoch]

            #self.log.debug('Releasing request lock 3')
            self.requestLock.release()
            #self.log.debug('Released request lock 3')

        return self.lastReportedState

    @property
    def status(self):

        '''Returns the tasks status (aka state)

        This is one of the state codes defined in the experiment.codes module:

        [INITIALISING_STATE, FINISHED_STATE, SUSPENDED_STATE,
         RUNNING_STATE, RESOURCE_WAIT_STATE,
         OUTPUT_DATA_WAIT_STATE, FAILED_STATE, POSTMORTEM_STATE]
        '''

        status, exitreason, returncode = self._requestStatus()

        return status

    @property
    def returncode(self):    

        '''Returns the tasks returncode (an int), or None if the task's job is not finished.

        Note: A task can have a returncode and not be finished.
        This occurs if post-execution jobs, e.g. data transfer, are still running.
        Use isAlive to determine if task is alive'''

        status, exitreason, returncode = self._requestStatus()

        #NB: The returncode obtained above is the runjob returncode
        #In certain circumstances this can be None even though the job is finished.
        #i.e runjob did not emit a returncode
        #Hence if the rc is None we also check status to see is job dead

        if returncode is None:
            if status == experiment.model.codes.FAILED_STATE:
                returncode = 1 
            elif status == experiment.model.codes.FINISHED_STATE:
                returncode = 0

        if returncode is not None:
            self.log.debug('Job finished. Return code %s. exitReason %s status %s' % (returncode, exitreason, status))

        return returncode

    @property 
    def exitReason(self):

        '''Returns a string desribing the reason for tasks exit.

        The possible exit reasons strings are defined in experiment.codes.exitReasons dictionary'''

        status, exitreason, returncode = self._requestStatus()

        return exitreason

    def wait(self):

        '''This method blocks the calling thread until the task has exited.
        
        If the task is remote and stages out data it blocks until data is copied by default'''

        #If wait is called disable the monitor since we are operating in synchronous manner
        self.cancelMonitor.set()
        try:
            while self.isAlive() is True:
                time.sleep(10.0)
        except KeyboardInterrupt:                
            self.terminate()
            #Other higher level code may be waiting on KeyboardInterrupt so re-raise
            raise
        finally:
            self.cleanup()
           
    def terminate(self, asyncClean=True):

        '''Terminates the task
        
        If asyncClean is True (default) the cleanUp is performed in separate thread
        This means the method returns once its received confirmation from Qcloud'''

        #Disable the cleanup monitor since we're going to kill directly here
        self.cancelMonitor.set()

        retval = 0
        try:
            self.log.warning("Terminating task %s (executable %s)" % (self.taskid, self.executionStack.target.endChain.executable))
            #This removes the job from the batch-system and lsf_readjobinfo will no longer work on it
            #However a bhist will still show it
            retval = lsf.lsb_deletejob(self.taskid, 0, 0)
        except:
            self.log.warning("Error sending termination command to task %s (executable %s)" % (self.taskid, self.executionStack.target.endChain.executable))
        finally:
            time.sleep(2.0)
            if retval == -1:
                self.log.warning("Error sending termination command to task %s (executable %s)" % (self.taskid, self.executionStack.target.endChain.executable))
            else:     
                self.log.debug("Termination command sent")

        if asyncClean is True:
            cleanThread = threading.Thread(target=Task.cleanup,args=[self],
                                           name=("qcclient.task.cleanup.%s" % self.executionStack.target.endChain.executable))
            cleanThread.start()
        else:
            self.cleanup()

        self.terminated = True

    def kill(self):

        '''Alternative name for terminate'''

        self.terminate()

    def poll(self):

        '''For compatibility with Popen interface - same as returncode property'''

        return self.returncode

    def isAlive(self):

        '''Returns True if the task is alive

        If terminate() has not been called this method uses the LSF processes returncode to determine if its alive.
        If terminate has been called this method always returns FALSE.
        
        In addition if task has a stage-out data requirment the process isAlive if the data has not been transfered (default)
        '''

        retval = True
        if self.exitReason is not None:
            retval = False

        if self.status == experiment.model.codes.OUTPUT_DATA_WAIT_STATE:
            self.log.debug('Job waiting on final output data transfer')

        return retval

    def cleanup(self):

        '''Clean up the task. This method should not need to be called by external object.

        Cleaning-up consists of:

        - Stopping the thread which copies stdout/stderr from the QCloud task 
        - Stopping the thread monitoring if task is alive
        - Performing final reads/copies of stdout/stderr'''

        pass

        #FIXME: Other methods may need to know if cleanup has been called ...

      # self.log.debug("Cleaning up for task %s" % self.taskid)

        try:
      #     self.log.debug("Cancelling copy thread (if active)")
      #     self.cancelCopy.set()            
            self.log.debug("Cancelling monitor (if active)")
            self.cancelMonitor.set()
      #     if self.copyThread != None:
      #         self.log.debug("Joining copy thread")
      #         self.copyThread.join()

      #     #If jobOutputStream is not None, the job got to running state
      #     if self.jobOutputStream is not None:            
      #         #Read anything remaining
      #         self.log.debug("Final read from jobOutputStream: %s" % self.jobOutputStream)
      #         data = self.jobOutputStream.read()
      #         self.log.debug("Writing to: %s" % self.stdout)
      #         self.stdout.write(data)
      #         self.log.debug("Closing jobOutputStream: %s" % self.jobOutputStream)
      #         self.jobOutputStream.close()
      #         self.jobOutputStream = None
      #     else:                
      #         if self.waitOutput is False:
      #             #The job didn't get running - open and read whatever is in the qcloud output file
      #             self.log.info("Job never started - accessing final output")
      #         else:
      #             self.log.debug("Getting stdout of job")

      #         try:
      #             job = utilities.comms.SendAndReceiveData(self.interface, self.queryPort, self.taskid, verbose=True)
      #             with open(job.outputFile, 'r') as f:
      #                 self.stdout.write(f.read())
      #         except:                        
      #             #Ignore error
      #             pass

            #Close our duplicate of the output stream passed on init
            self.stdout.close()     
        except Exception as error:                
            self.log.info("Caught exception, %s, while cleaning up task %s- ignoring" % (error, self.taskid))


    @property
    def schedulingInfo(self):

        '''Returns details on the time the task spent in various states in the scheduler

        Returns:
            An a experiment.utilities.data.Matrix instance with one row and the following columns:
            - wait-time
            - pend-start-time
            - pre-execution-time
            - main-execution-time
            - completion-time
            - transfer-out-time-estimate
            - total-time
        Info

        wait-time and transfer-out-time-estimate are impacted by the statusRequestInterval

        wait-time will not be accurate if the completion-time < statusRequestInterval
        as it needs to be obtained when job is running

        transfer-out-time is always an estimate as we only know its finish to a statusRequestInterval resolution.

        '''
        # VV: request status populates the schedulingData ivar
        status, exitreason, returncode = self._requestStatus()
        return self.schedulingData[:]

    @property
    def executionInfo(self):

        '''Returns details on the execution performance of the job.

        Overview and per host

        '''

        #Execution
        #CPUTime/SystemTime/UserTime
        #Above per process
        #Above per process per thread

        return None

    @property
    def performanceInfo(self):

        #Current just return this - may combine with executionInfo
        #Or possible merge executionInfo and schedulingInfo into this property
        return self.schedulingInfo

    @classmethod
    def default_performance_info(cls, statusRequestInterval=-1):
        # For storing scheduling information
        schedulingHeaders = [
            "request-interval",
            "wait-time",
            "pend-start-time",
            "pre-execution-time",
            "command-execution-time",
            "completion-time",
            "transfer-out-time-estimate",
            "total-time",
            "epoch-submitted",
            "epoch-started",
            "epoch-finished"
        ]

        r = [statusRequestInterval] + [-1] * (len(schedulingHeaders) - 4) + ['None'] * 3
        schedulingData = experiment.utilities.data.Matrix(
            rows=[r],
            headers=schedulingHeaders,
            name='SchedulingData'
        )

        return schedulingData

if __name__ == "__main__":

    if lsf.lsb_init("st4sd-runtime-core") > 0:
            raise ValueError("Unable to initialise LSF environment")

    if len(sys.argv) == 1:
        numJobs = lsf.lsb_openjobinfo(0, None, None, None, None, lsf.ALL_JOB)
        if numJobs == -1:
            lsf.lsb_perror(None)
            sys.exit(1)
        else:     
            print('There are %d jobs available for querying' % numJobs)

        for i in range(numJobs):
            j = lsf.lsb_readjobinfo(None)
            print(j, j.jobId)

        lsf.lsb_closejobinfo()       
    else:   
        r = LSFJobInfo(int(sys.argv[1]))
        print('PID:', r.pid)
        print('Job State:', r.state)
        print('Job Finished: ', r.isFinished)
        print('Exit Reason:', r.exitReason)
        print('Return-Code:', r.returncode)
        print('Normal Term:', r.isNormalTermination)
        print('Abnormal Term:', r.isAbnormalTermination)
        print('Signal Received:', r.signal)
        print('Failed Start:', r.isFailedJobStart)
        print(r.energyUsage)
        print(r.computeResources)
        print(r.computeUsage)
        print(r.memoryUsage)
        print(r.schedulingEventTimes)
        print(r.schedulingBreakdown)
        if r.isRemote:
            print('Outputs Transfered:', r.outputsTransfered)
