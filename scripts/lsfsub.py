#! /usr/bin/env python
# coding=UTF-8

# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Michael Johnston

'''Provides a simplified interface to LSF using sane defaults for various LSF arguments and removing environment crud.
Also works on hybrid systems. This assumes A) The executable exists on the remote system at the given path B) The filesystems
are mirrored i.e. all paths on local side exist on remote side. 
'''
from __future__ import print_function

import collections
import configparser
import logging
import optparse
import os
import sys

import experiment.model.codes
import experiment.model.executors
import experiment.runtime.backend_interfaces.lsf

#This is platform dependent - below is for panther
#env-pythonlsf = { PATH=%(venvpath)s/pycaf-fen/bin:/gpfs/panther/local/apps/gcc/gnuplot/5.0.3/bin:$PATH
#LD_LIBRARY_PATH=/gpfs/panther/local/apps/gcc/utilities/lib:/gpfs/panther/local/apps/gcc/lapack/3.6.0/lib64:$LD_LIBRARY_PATH
#PYTHONPATH=/gpfs/panther/local/apps/python2/2.7.8/lib/python2.7/site-packages:/gpfs/panther/local/HCRI003/rla09/shared/virtualenvs/pycaf-fen/lib/python2.7/site-packages
#OMP_NUM_THREADS=4

#This is platform dependent - below is for panther
#envmpi = { "LSF_MPIRUN":"/gpfs/panther/local/apps/ibm/openmpi/1.10.2/bin/mpirun", "LD_LIBRARY_PATH": "/gpfs/panther/local/apps/ibm/openmpi/1.10.2/lib/:$LD_LIBRARY_PATH", "OMP_DISPLAY_ENV":"VERBOSE" }

if __name__ == "__main__":

    usage = "usage: %prog [options] [executable] [arguments]"

    parser = optparse.OptionParser(usage=usage, version="% 0.1", description=__doc__)

    hpcGroup = optparse.OptionGroup(parser, "Specify parallel program options")
    hybridGroup = optparse.OptionGroup(parser, "Options controlling behaviour on systems using data-manager")
    environmentGroup = optparse.OptionGroup(parser, "Options controlling the jobs environment")

    parser.add_option_group(hpcGroup)
    parser.add_option_group(environmentGroup)
    parser.add_option_group(hybridGroup)

    parser.add_option("-q", "--queue", dest="queue",
            help="Name of queue. Default %default ",
            default='standard',
            metavar="QUEUE")
    environmentGroup.add_option("-e", "--environment", dest="environment",
            help="Environment variables for process as comma separated string of key-value pairs."
            " These will override the same variable defined in a named environment.",
            default=None,
            metavar="ENVIRONMENT")
    environmentGroup.add_option("-d", "--namedEnvironment", dest="namedEnvironments",
            help="Adds the given named environment to the job. "
            " Named environments are defined in a file, environment.conf, in the submitting directory and/or" 
            " in a file .lsfsub/environment.conf in your $HOME directory. These environment files can also define"
            " default environments for different types of jobs.",
            action='append',
            default=[],
            metavar="NAMED_ENV")
    environmentGroup.add_option("-m", "--inheritEnvironment", dest="inheritEnvironment",
            help="Job inherits all environment variables in calling process. Values from NAMED_ENV and ENVIRONMENT will override",
            action='store_true',
            default=False,
            metavar="INHERIT_ENVIRONMENT")
    parser.add_option("-v", "--verbose", dest="verbose",
            help="Turns on debugging output",
            action="store_true",
            default=False,
            metavar="VERBOSE")
    parser.add_option("--keepBackslash", "--keepBackslash", dest="keepBackslash",
            help="Retains backslashes in executable arguments. By default they are removed."
            " Note: They are required when program arguments would otherwise be interpreted by the shell or by this script", 
            action="store_true",
            default=False,
            metavar="VERBOSE")
    parser.add_option("", "--cwd", dest="cwd",
            help="Working directory for the job. Default %default",
            default=os.getcwd(),
            metavar="WORKINGDIR")
    parser.add_option("-l", "--logLevel", dest="logLevel",
            help="Logging level. Default %default",
            type="int",
            default=30,
            metavar="LOG_LEVEL")

    hpcGroup.add_option("-n", "--numberProcesses", dest="numberProcesses",
            help="Number of MPI ranks. Default %default",
            type="int",
            default=1,
            metavar="NO_PROCESSES")
    hpcGroup.add_option("-r", "--ranksPerNode", dest="ranksPerNode",
            help="Ranks per node. Default %default",
            type="int",
            default=1,
            metavar="RANKS")
    hpcGroup.add_option("-t", "--threads", dest="threads",
            help="Threads per rank. Default %default",
            type="int",
            default=1,
            metavar="THREADS")
    hpcGroup.add_option("-s", "--smt", dest="threadsPerCore",
            help="Number of SMT threads per core. Total cores is THREADS divided by this value. Default %default",
            type="int",
            default=1,
            metavar="SMT")
    hpcGroup.add_option("-w", "--walltime", dest="walltime",
                        help="Walltime for the job in minutes. Default %default",
                        type="int",
                        default=60,
                        metavar="WALLTIME")
    hpcGroup.add_option("", "--oneRankMPI", dest="oneRankMPI",
                        help="Run with MPI even if NO_PROCESSES is one. By default 1 process tasks are not run with MPI",
                        action="store_true",
                        metavar="ONE_RANK_MPI")
    hpcGroup.add_option("", "--resourceString", dest="resourceString",
                        help="LSF resource string - anything that is valid for the -R option of bsub",
                        default="",
                        metavar="RESOURCE_STRING")

    hybridGroup.add_option("-i", "--stagein", dest="stagein",
            help="Absolute path to file/directory to stage in. "
            "Specifying this option engages hybrid mode and assumes LSF datamanager is available",
            action="append",
            metavar="STAGEIN")
    hybridGroup.add_option("-u", "--stageout", dest="stageout",
            help="Absolute path to file/directory to stage out. Path must exist on remote machine."
            "Specifying this option engages hybrid mode and assumes LSF datamanager is available",
            action="append",
            metavar="STAGEOUT")
    hybridGroup.add_option("", "--hybrid", dest="isHybrid",
            help="Specifying this option engages hybrid mode and assumes LSF datamanager is available. "
            "Use if you need to run on hybrid machine but have no stagein/out requirements other than stderr/stdout",
            action='store_true',
            metavar="HYBRID")

    options, args = parser.parse_args()
    env = {}


    FORMAT = '%(levelname)-9s %(threadName)-30s %(name)-30s: %(funcName)-20s %(asctime)-15s: %(message)s'
    logging.basicConfig(format=FORMAT)
    rootLogger = logging.getLogger() 
    rootLogger.setLevel(options.logLevel)

    if len(args) ==0:
        print("No executable specified", file=sys.stderr)
        sys.exit(1)

    executable = args[0]    
    arguments = ""
    if len(args) > 1:
        arguments = " ".join(args[1:])

    if not options.keepBackslash:
        arguments = arguments.replace("\\", "")     

    stageInRefs = None
    stageOutRefs = None
        
    if options.stagein is not None:
        stageInRefs = []

        for path in options.stagein:
            path = os.path.realpath(path)
            stageInRefs.append(path)

        rootLogger.info('Staging in %s' % stageInRefs)
        options.isHybrid = True
    
    if options.stageout is not None:
        stageOutRefs = [os.path.realpath(p) for p in options.stageout if p != 'all']
        if 'all' in  options.stageout:
            stageOutRefs.append('all')

        rootLogger.info('Staging out %s' % stageOutRefs)
        options.isHybrid = True

    if stageInRefs is None and stageOutRefs is None and options.isHybrid:
        stageInRefs = []
        stageOutRefs = []

    rootLogger.info('Procesess %d. Processes Per Node %d. Threads %d. Threads Per Core %d' % (options.numberProcesses, options.ranksPerNode, 
        options.threads, options.threadsPerCore))

    if options.inheritEnvironment:
        rootLogger.info('Job will inherit all current environment variables')
        env.update(os.environ)

    if len(options.namedEnvironments) != 0:            
        configuration = configparser.SafeConfigParser(dict_type=collections.OrderedDict)
        configuration.optionxform = str
        configuration.read([os.path.join(os.path.expanduser('~'), '.lsfsub/environment.conf'), 'environment.conf'])
        for name in options.namedEnvironments:
            rootLogger.info('Reading variables from named environment: %s' % name)
            variables = configuration.options(name)
            for variable in variables:
                env[variable] = configuration.get(name, variable)

    if options.environment is not None:
        variables = [el.strip() for el in options.environment.split(',')]
        for var in variables:
            key, value = [el.strip() for el in var.split('=')]
            env[key]=value
        
    rootLogger.info('User job environment variables:')
    for key in list(env.keys()):
         rootLogger.info('%s=%s' % (key, env[key]))

    #HACK: Rewrite rule - replace 'cds/fairthorpe' with 'panther' or 'paragon' depending on queue
    #Fixme: enhance syntax and read from file
    rule = None
    if options.isHybrid:
        if options.queue in ['paragon', 'paragonI']:
            rule = {'pattern':'cds|fairthorpe', 'replacement':'paragon'}
        elif options.queue in ['panther', 'pantherI']:
            rule = {'pattern':'cds|fairthorpe', 'replacement':'panther'}
        else:
            rootLogger.critical("Unknown queue %s. Cannot apply rewrite rules for destination cluster" % options.queue)
            sys.exit(1)

    #1. Create command executor
    executor = experiment.model.executors.Command(executable=executable,
                                                        arguments=arguments,
                                                        workingDir=options.cwd,
                                                        environment=env,
                                                        resolveShellSubstitutions=True)

    #2. Set rule to command executor (command line, working dir and environment)
    if options.isHybrid:
        executor.setRewriteRule(rule)

    preCommands = []
    if stageInRefs is not None and len(stageInRefs) != 0:
        d = {'sources':stageInRefs, 'workingDir':options.cwd}
        preCommands.append(
            experiment.runtime.backend_interfaces.lsf.DMStageInCommand.commandFromOptionsAndRewriteRule(d, rule))

    postCommands = []
    if stageOutRefs is not None and len(stageOutRefs) != 0:
        if 'all' in stageOutRefs:
            rootLogger.warning('All specified in stage-out refs. Ignoring other stage-out requests')
            d = {'destination':options.cwd}
            postCommands.append(
                experiment.runtime.backend_interfaces.lsf.DMStageOutDirectoryCommand.commandFromOptionsAndRewriteRule(d, rule))
        else:
            d = {'destinations':stageOutRefs, 'outputDir':options.cwd}
            postCommands.append(
                experiment.runtime.backend_interfaces.lsf.DMStageOutCommand.commandFromOptionsAndRewriteRule(d, rule))

    resourceManagement = {'queue': options.queue,
                          'resourceString': options.resourceString,
                          'statusRequestInterval': 5,
                          'reservation': None,
                          'walltime':options.walltime}

    resourceRequest = {'numberProcesses': options.numberProcesses,
                       'ranksPerNode': options.ranksPerNode,
                       'numberThreads': options.threads,
                       'threadsPerCore': options.threadsPerCore}

    with open(os.path.join(options.cwd, 'powersub.txt'), 'w+') as f:
            task = experiment.runtime.backend_interfaces.lsf.Task(executor,
                                                                  preCommands=preCommands,
                                                                  postCommands=postCommands,
                                                                  options=resourceManagement,
                                                                  resourceRequest=resourceRequest,
                                                                  stdout=f,
                                                                  waitOnOutput=False,
                                                                  checkCWD=False,
                                                                  isHybrid=options.isHybrid,
                                                                  oneRankMPI=options.oneRankMPI)

            task.wait()

    r = experiment.runtime.backend_interfaces.lsf.LSFJobInfo(task.taskid)
    print('PID:', r.pid)
    print('Job State:', r.state)
    print('Job Finished: ', r.isFinished)
    print('Exit Reason:', r.exitReason)
    print('Return-Code:', r.returncode)
    print('Normal Term:', r.isNormalTermination)
    print('Abnormal Term:', r.isAbnormalTermination)
    print('Signal Received:', r.signal)
    print('Failed Start:', r.isFailedJobStart)

    print('Scheduling/Performance')
    print(task.schedulingInfo.csvRepresentation())

    #If we have a return code use it
    #otherwise exit 1 or 0 depending on FINISHED or not
    if r.returncode is not None:
        sys.exit(r.returncode)
    else:
        if r.state == experiment.model.codes.FINISHED_STATE:
            sys.exit(0)
        else:
            sys.exit(1)
