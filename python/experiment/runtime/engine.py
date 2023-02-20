#
# coding=UTF-8
# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston


'''Defines Engine classes which drive components'''

from __future__ import annotations

import collections
import datetime
import glob
import logging
import os
import shutil
import threading
import traceback
from six import string_types
from typing import (TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple)

import pandas

import reactivex
import reactivex.scheduler
import reactivex.subject
import reactivex.operators as op
import reactivex.internal.exceptions

import experiment.utilities.data
import experiment.model.errors
import experiment.model.codes
import experiment.model.data
import experiment.model.graph
import experiment.runtime.backends
import experiment.runtime.errors
import experiment.runtime.monitor
import experiment.runtime.optimizer
import experiment.runtime.task
import experiment.runtime.utilities.rx
import experiment.model.hooks

if TYPE_CHECKING:
    from experiment.runtime.task import Task

moduleLogger = logging.getLogger('engine')

#Note (REACTIVE): How to put a delay between elements in a list
#Change the list of numbers into a list of Observables each of which observes a list of 1 element
#Then concatenate the inner sequences of 1 element - however the cocatenation will be delay
#delay() on each concat i.e. it pull the 1 element from the observable but this hits the delay
#a = rx.Observable.from_(range(10)).map(lambda x: rx.Observable.just(x).delay(5000)).concat_all()

ENGINE_RUN_START_DELAY_SECONDS = 1.0
ENGINE_LAUNCH_DELAY_SECONDS = 5.0


def LogClosure(label):
    def Log(e):
        moduleLogger.warning("%s: %s" % (label, e))
        return e

    return Log

def compute_is_alive_from_exit_reason(exitReason):

    return True if exitReason is None else False

def compute_returncode_from_exit_reason(exitReason):

    retval = None
    if not compute_is_alive_from_exit_reason(exitReason):
        retval = 0
        if exitReason != experiment.model.codes.exitReasons['Success']:
            retval = 1

    return retval



def DLMESORestart(workingDirectory, restarts, componentName, log, exitReason, exitCode):
    """Prepares for a DLMESO restart

    Parameters
    ----------
    workingDirectory: str
        Directory containing simulation to be restarted
    restarts: int
        The number of times this function for this working directory
    componentName: str
        The label the workflow engine uses to id this component
    log: logging.Logger
        A logger used to write output messages
    exitReason: str
        Defines why the program exited
    exitCode: int
        The exit-code returned by the program

    Returns
    -------
    bool
        True if the function prepared for a restart (or can restart if no preparation needed)
        False if the function can't prepare for a restart given exitReasons
    """

    if exitReason != experiment.model.codes.exitReasons['ResourceExhausted']:
        return False

    restartPossible = True

    # Add 'restart' to control
    control = os.path.join(workingDirectory, 'CONTROL')
    # Inefficient ...
    try:
        with open(control, 'r+') as f:
            lines = f.readlines()
            f.seek(0)
            if lines[-2] != 'restart\n':
                lines.insert(-1, 'restart\n')
                f.writelines(lines)
    except IOError as error:
        log.warning("Error trying to update dlmeso CONTROL file for restart - error %s (job: %s)" % (error, componentName))
        raise

    # Move OUTPUT of previous job
    try:
        name = os.path.join(workingDirectory, 'OUTPUT')
        basename = os.path.split(name)[1]
        tempname = os.path.join(workingDirectory, '%s%d' % (basename, restarts))
        shutil.move(name, tempname)
    except Exception as error:
        log.warning("Unexpected event when trying restart DLEMSO job %s" % componentName)
        log.warning("Exception while archiving previous runs OUTPUT file. %s" % str(error))
        log.warning("Ignoring")

    return restartPossible


def archive_stream(filepath, storage_dir, stream_type, logger, max_files=5):
    # type: (str, str, str, logging.Logger, int) -> None
    index = 0

    if os.path.exists(storage_dir):
        search = os.path.join(storage_dir, '*')
        existing = glob.glob(search)
        existing = [experiment.model.graph.stream_path_to_index(path, stream_type) for path in existing]
        existing = [i for i in existing if i is not None]

        max_of_existing_files = max_files - 1

        if len(existing) >= max_of_existing_files:
            existing = sorted(existing)
            for oldest in existing[:len(existing) - max_of_existing_files]:
                old_file = os.path.join(storage_dir, '%d.%s' % (oldest, stream_type))
                try:
                    logger.debug("Removing %s" % old_file)
                    os.remove(old_file)
                except Exception:
                    logger.warning("Could not remove archived stream %s" % old_file)

        if existing:
            index = max(existing) + 1
    else:
        os.makedirs(storage_dir)

    final_location = os.path.join(storage_dir, '%d.%s' % (index, stream_type))
    temp_location = os.path.join(storage_dir, 'temp_%d.%s' % (index, stream_type))

    try:
        shutil.copyfile(filepath, temp_location)
    except:
        logger.debug("Could not copy stream %s to %s" % (filepath, temp_location))
    else:
        try:
            os.rename(temp_location, final_location)
            logger.debug("New archive stream under '%s'" % final_location)
        except:
            logger.debug("Could not rename archived stream %s to %s" % (
                temp_location, final_location
            ))


#Refactor stage 2
#
#Merge RepeatingEngine and Engine once observation/notification in place
#Move all TaskGeneration functions to Task classes OR factory functions using Executors.


class Engine:
    """Class which handles launching and monitoring of Job
    """
    # VV: Each poolScheduler will be instantiated by a single Engine (not necessarily the same Engine) and all
    # engines will re-use them

    # VV: emits the observables of engines
    enginePoolScheduler: reactivex.scheduler.ThreadPoolScheduler | None = None

    # VV: triggers emissions to flow skipping the 5 seconds wait time
    triggerPoolScheduler: reactivex.scheduler.ThreadPoolScheduler | None = None

    # VV: threads in here call the task.wait() method, they continue when the task terminates
    taskPoolScheduler: reactivex.scheduler.ThreadPoolScheduler | None = None

    @classmethod
    def engineForComponentSpecification(cls, job):
        """Returns the Engine for the given Job

        Parameters
        ----------
        cls: Engine
            Class type
        job: experiment.data.Job
            The job specification

        Returns
        -------
        Union[Engine, RepeatingEngine]


        Exceptions
        ----------
        Raises ValueError if Job is of unknown type.
        """

        try:
            taskGenerator = experiment.runtime.backends.backendGeneratorMap[job.type]
        except KeyError:
            raise ValueError('Unknown backend %s' % job.type)

        if job.isRepeat:
            engine = RepeatingEngine(job, taskGenerator)
        else:
            engine = Engine(job, taskGenerator)

        return engine

    def __init__(self, componentSpecification, taskGenerator, init_state_updates=True):
        """A job manager

        Parameters
        ----------
        componentSpecification: experiment.data.Job
            The job/component specification
        taskGenerator:  Callable[[experiment.data.Job], experiment.task.Task]
            Backend code which instantiates a Job
        init_state_updates: When true state updates observable is created. You may wish
            to manually create the state updates observable if you use `Engine` as a base
            for a child class that extends stateDictionary using information that it generates
            in its own constructor. When this value is set to False the child class is expected
            to properly initialize state_updates. (default is True).
        """
        # VV: ThreadPoolGenerator.get_pool() is threadSafe, so if multiple threads call it concurrently they'll all get
        # the same pool. GIL will then guarantee that every thread will read the same value even if multiple threads
        # end up "updating" Engine.enginePoolScheduler.
        tpg = experiment.runtime.utilities.rx.ThreadPoolGenerator
        if Engine.enginePoolScheduler is None:
            Engine.enginePoolScheduler = tpg.get_pool(tpg.Pools.Engine)

        if Engine.triggerPoolScheduler is None:
            Engine.triggerPoolScheduler = tpg.get_pool(tpg.Pools.EngineTrigger)

        if Engine.taskPoolScheduler is None:
            Engine.taskPoolScheduler = tpg.get_pool(tpg.Pools.EngineTask)

        # VV: Set to True when run is Called, RepeatingEngine sets to True when canConsume() first return True
        self._consume = False

        self.log = logging.getLogger('eng.%s' % componentSpecification.reference.lower())

        # VV: Triggers the terminationObservable to emit (by calling _termination_subject.on_next(<exit reason string>)
        # it is created (and re-created) inside _create_termination_observable()
        self._termination_subject: reactivex.subject.ReplaySubject | None = None

        # An observable that emits the first item that goes on the TERMINATION subject before completing
        # If such an event happens it is replayed to every subscriber no matter when they connect
        # the observable is created inside _create_termination_observable() (method used by restart() too)
        self.terminationObservable: reactivex.Observable | None = None

        self._create_termination_observable()

        def termination_observable_completed():
            self.log.debug("Termination observable is completed")
            self._termination_subject.on_completed()

        #Handles terminations before a process is launched i.e is up-to-the moment before run() is called
        #Note: Cancels the termination subject on completion (if it processes a kill event)
        #on_error will be triggered if there is a termination after task launch  (it completes before anything emitted)
        set_exitreason = experiment.runtime.utilities.rx.report_exceptions(self._setExitReason, self.log, 'setExitReason')
        check_state = experiment.runtime.utilities.rx.report_exceptions(CheckState, self.log, 'CheckState')
        self.terminationObservable.pipe(
            op.take_while(lambda x: self._runCalled is None)
        ).subscribe(on_next=set_exitreason,
                    on_completed=termination_observable_completed,
                    on_error=check_state)

        self.job = componentSpecification  # type: experiment.model.data.Job
        self.process = None  # type: experiment.runtime.task.Task

        #This is used to provide an exitReason when
        #1) Process can't because e.g. it raised an Exception in init
        #2) We want to override the processes exitReason()
        self._exitReason = None

        #For canceling a monitor (in this case monitor waiting on initial output)
        self.cancelMonitorEvent = threading.Event()

        #Use to indicate the engine will no longer run
        #Required to stop the detailedState observable from emitting state
        self._shutdown = False

        self.taskGenerator = taskGenerator  # type: Callable[[experiment.model.data.Job, str, str], experiment.runtime.task.Task]

        #Tracking restarts and launches
        self.restarts = 0
        self._resubmissionAttempts = 0

        #Recording state-change times
        #TODO: Update name of this variable - it should be `initialisationTime` or similar
        # VV: Engines are created at time 0 so _lastLaunched should not be initialized here.
        #     Initialize it when executing run() (*only* during the very first time)
        self._lastLaunched = None  # type: Optional[datetime.datetime]
        self._taskLaunched = None  # type: Optional[datetime.datetime]
        #Indicates that run() method has been called.
        #Allows identifying how long the program is waiting on output
        self._runCalled = None
        self._taskFinished = None

        # Set up performance measurement
        # TODO: Not used in non-repeating Engine yet
        self.performanceMatrix = experiment.utilities.data.Matrix()
        self.performanceHeaders = ['component-launch-time',
                                   'can-consume',
                                   'output-available',
                                   'delta-last-task',
                                   'task-run-time',
                                   'data-produced',
                                   'task-production-rate',
                                   'component-production-rate',
                                   'exit-code',
                                   'scheduler-id',]

        self.lastPerformanceWrite = datetime.datetime.now()

        # Used to prevent threads interacting with self.process ivar while its being created
        self.processLock = threading.RLock()
        self.lastLaunchedLock = threading.RLock()

        # VV: Used make sure that calls to run() are serialized
        self.runLock = threading.RLock()

        self._clock_periodic = reactivex.interval(5.0)
        self._manual_emitter = reactivex.subject.Subject()

        # VV: _manual_emitter will use whichever scheduler invokes its on_next() (i.e. manualEmissions in emit_now)
        self._detailedState = reactivex.merge(
            self._clock_periodic.pipe(
                # VV: The Clock periodic blindly echoes what's in stateDictionary at the moment of trigger
                op.map(lambda x: self.stateDictionary)
            ),
            # VV: The manual emitter produces a dictionary with the same format as self.stateDictionary
            self._manual_emitter,
        ).pipe(
                op.observe_on(Engine.enginePoolScheduler),
                op.map(lambda x: (x, self))
        )

        # VV: _state_updates depend on self._detailedState so create them once the former is up and running
        self._state_updates: reactivex.ConnectableObservable | None = None
        self._state_updates = self._create_state_updates() if init_state_updates else None

        # VV: Optimizer state
        self.opt_lock = threading.RLock()
        self.optimizer_disable_cb = None  # type: Callable[[], None]
        self.optimizer = None  # type: experiment.runtime.optimizer.Optimizer
        self._last_cached_repeat_interval_decision = None  # type Optional[float]

    def _create_termination_observable(self):
        """Instantiates a termination observable which emits just once and replays its last emission

        To trigger the emission call self._termination_subject.on_next(<an exitReason string>).
        The current method also on_completes() the termination_subject if it already exists
        """

        if self._termination_subject is not None:
            self.log.debug("TerminationSubject already exists invoking its on_completed() now")
            self._termination_subject.on_completed()

        self._termination_subject = reactivex.subject.ReplaySubject(scheduler=Engine.triggerPoolScheduler)
        self.terminationObservable = self._termination_subject.pipe(
            op.first(),
        )

    def emit_now(self, what: Dict[str, Any] | None =None):
        """Triggers a stateUpdates emission with custom information"""
        # VV: Use a thread out of the manualEmissions pool to schedule the on_next(), we don't want to block the
        # calling thread till the observers consume the emission
        what = what or {}
        current = self.stateDictionary
        current.update(what)

        reactivex.just(current).pipe(
            op.observe_on(Engine.triggerPoolScheduler)
        ).subscribe(on_next=lambda x: self._manual_emitter.on_next(x),
                    on_error=CheckState)

    def _emit_no_more_manual(self):
        self.log.debug("Asking manual emitter to terminate")
        self._manual_emitter.on_completed()

    def optimizer_enable(self, optimizer, disable_optimizer_cb):
        # type: (experiment.runtime.optimizer.Optimizer, Callable[[],None]) -> None
        with self.opt_lock:
            self.optimizer = optimizer
            self.optimizer_disable_cb = disable_optimizer_cb

    def optimizer_disable(self, propagate):
        # type: (bool) -> None
        with self.opt_lock:
            self.optimizer = None

        if propagate:
            self.optimizer_disable_cb()

    def _prime(self):
        """Engines are instantiated as soon as an experiment is created
        """
        if self._lastLaunched is None:
            self._lastLaunched = datetime.datetime.now()

    def run(self, startObservable: reactivex.Observable = reactivex.timer(ENGINE_RUN_START_DELAY_SECONDS)):

        '''Launches the engines task asynchronously

        This method will return immediately.
        The reciever state observable or isAlive property must be observed to check what happens

        Exceptions
        ----------
        None
        '''

        #Set to indicate run() has been called
        self._runCalled = datetime.datetime.now()
        # VV: If run() got called, this means Engine can consume its producer's outputs
        self._consume = True
        self._prime()

        #
        # The following 5 functions break up the Task Launch process to make the steps clear below
        # - Initialising performance information  (InitPerformanceInfo)
        # - Launching the task (LaunchTask)
        # - Setting the _taskLaunched ivar (SetLaunchTime)
        # - Waiting on task exit (Wait)
        # - Finalising performance information (FinalisePerformanceInfo)

        def InitPerformanceInfo(emission):

            '''
            Initialises a new row in the performance matrix

            Columns set: can-consume, output-available, delta-last-task

            Args:
                emission: Not used

            Returns:
                perfData dictionary with the above keys initialised
                All other keys are set to N/A
            '''

            items = list(zip(self.performanceHeaders, len(self.performanceHeaders) * ['N/A']))
            perfData = collections.OrderedDict(items)
            perfData['can-consume'] = "%s" % True
            perfData['output-available'] = "%s" % True
            perfData["delta-last-task"] = 0
            perfData['scheduler-id'] = ''

            #Initialise a new row in the performance matrix
            #We do this here in-case there is an exception as then we will lose track of perfData
            self.performanceMatrix.addElements(perfData)

            return perfData

        def LaunchTask(perfData):

            '''Creates a task

            Task creation is always attempted if this function is called

            Parameters:
                perfData: A dictionary whose keys are columns of self.performanceMatrix ivar

            Returns:
                A dict with following key:values
                process: An experiment.Task subclass or None if the process could not be created
                exitReason: None or a experiment.codes.exitReason key if process could not be created
                perfData: The perfData object passed as a parameter
            '''

            # VV: Prime self.process equal to None before doing anything serious to return the Engine
            #     in an initialized state
            self.process = None
            exitReason = None
            try:
                self.log.info('Starting %s %s' % (self.job.executable, self.job.arguments))
                self.process = self.taskGenerator(self.job)

                # VV: process can report when it started executing (unit tests and the optimizer can use this info)
                self.emit_now()
            except OSError as error:
                # Since directories/executables are validated on Task creation this indicates
                # a file-system inconsistency error. We will wrap it in a JobLaunchError to give further
                # context to anyone trying to handling exceptions above
                self.log.warning('Submission failed: Task raised OSError error %s. Possible FS issue' % error)
                exitReason = experiment.model.codes.exitReasons['SubmissionFailed']
            except experiment.runtime.errors.JobLaunchError as error:
                self.log.warning('Submission failed: Task raised launch error %s' % error)
                exitReason = experiment.model.codes.exitReasons['SubmissionFailed']
            except Exception as error:
                self.log.warning('Unexpected exception on Task launch: %s\nEXCEPTION:\n%s' %
                                  (error, traceback.format_exc())
                                  )
                exitReason = experiment.model.codes.exitReasons['UnknownIssue']

            return {"process": self.process, "exitReason": exitReason, "perfData": perfData}

        def SetLaunchTime(emission):

            '''Sets the _taskLaunched ivar if process is not None

            Parameters:
                emission: A dictionary with three keys - process, exitReason and perfData


            Returns:
                The emission object passed as a parameter
                The emission['perfData'] dict is updated with the launch time ('component-launch-time' key)
            '''

            perfData = emission['perfData']
            if emission['process'] is not None:
                self._taskLaunched = datetime.datetime.now()
                perfData['component-launch-time'] = self._taskLaunched.strftime("%d%m%y-%H%M%S")

            self.emit_now()

            return emission

        def Wait(emission):

            '''Calls wait() on the emission['process'] object

            Returns when wait() exits

            Parameters:
               emission: A dictionary with three keys - process, exitReason and perfData

            Returns:
                The emission object passed as a parameter
            '''

            process = emission["process"]  # type: experiment.runtime.task.Task

            if process is not None:
                process.wait()

            return emission

        def extract_info_from_emission(emission: Dict[str, Any]) -> Dict[str, Any]:
            """Extracts self.stateDictionary compatible information from {process, perfData, exitReason} emission"""
            info = {
                'engineExitReason': emission['exitReason'],
            }

            process = emission['process']
            perfData = emission['perfData']

            if process is not None:
                task_events = self.compute_task_events(process)
                info.update(task_events)
                info['lastTaskRunTime'] = perfData['task-run-time']
                info['schedulerId'] = perfData['scheduler-id']
                info['engineExitCode'] = process.returncode

            return info

        def FinalisePerformanceInfo(emission):

            '''Finalises the entries in the last row of the performance matrix

            Also sets the taskFinished ivar'''

            process = emission['process']
            perfData = emission['perfData']
            exitReason = emission['exitReason']

            if process is not None:
                self._taskFinished = datetime.datetime.now()
                perfData['exit-code'] = process.returncode
                perfData['scheduler-id'] = process.schedulerId
                perfData['task-run-time'] = experiment.runtime.monitor.TotalSeconds(self._taskFinished - self._taskLaunched)

                # TODO: Calculate data produced
                # We have to capture new files and appended files and overwritten files
                perfData['data-produced'] = 0
                perfData['task-production-rate'] = perfData['data-produced'] / perfData['task-run-time']
                perfData['component-production-rate'] = perfData['data-produced'] / (
                        perfData['delta-last-task'] + perfData['task-run-time'])

                perfData.update(process.performanceInfo.getElements())
            else:
                perfData['exit-code'] = 1
                perfData['scheduler-id'] = None
                perfData['task-run-time'] = "N/A"

            #process.performanceInfo will have added new headers so `setElements` will fail
            #Instead remove last row and use addElements to add it back.
            self.performanceMatrix.removeRows(-1)
            self.performanceMatrix.addElements(perfData)

            return emission

        #
        #The following three functions handle task exit, completion, and errors
        #

        def HandleTaskExit(emission):

            '''Performs finalisation operations with the task exits

            1. Writes performance info
            2. Sets exit reason
            3. Emits State'''
            process = emission['process']  # type: experiment.runtime.task.Task
            perfData = emission['perfData']
            exitReason = emission['exitReason']
            self.log.log(19, 'Handling task exit, Process is %s' % process)

            reason = process.exitReason if process is not None else exitReason
            self.log.log(19, "Reason is %s" % reason)

            try:
                with open(os.path.join(self.job.workingDirectory.path, 'component_performance.csv'), 'w') as f:
                    print(self.performanceMatrix.csvRepresentation(), end=' ', file=f)
            except IOError:
                pass

            self._setExitReason(reason)
            info = extract_info_from_emission(emission)
            self.emit_now(info)

        def HandleTaskObservableException(exception):

            '''The function cleans up after an exception in the TaskWait observable
            We won't know exactly where this was raised from so have to handle all eventualities'''
            info: Dict[str, Any] = {}

            if not isinstance(exception, reactivex.internal.exceptions.SequenceContainsNoElementsError):
                self.log.warning('Handling error: %s. Process is %s' % (exception, self.process))

                #FIXME: setExitReason requires process to be dead (process.isAlive == False)
                #However the kill is asynchronous so it may not be dead by the time we get to setExitReason ...
                if self.process is not None and not self.process.isAlive():
                    self.process.kill()

                #The perf data should differ if the exception happened after the task launched or before ...
                if self._taskLaunched:
                    self._taskFinished = datetime.datetime.now()
                    d = {}
                    d['task-run-time'] = experiment.runtime.monitor.TotalSeconds(self._taskFinished - self._taskLaunched)
                    d['exit-code'] = 1
                    self.performanceMatrix.setElements(d)
                    info['lastTaskRunTime'] = d['task-run-time']
                    info['engineExitCode'] = d['exit-code']

                    #Update performance table
                    try:
                        with open(os.path.join(self.job.workingDirectory.path, 'component_performance.csv'), 'w') as f:
                            print(self.performanceMatrix.csvRepresentation(), end=' ', file=f)
                    except IOError:
                        pass

                self._setExitReason(experiment.model.codes.exitReasons['UnknownIssue'])
                info['engineExitReason'] = experiment.model.codes.exitReasons['UnknownIssue']
            else:
                #This error is caused by take_while calling on_complete on first() when the first has seen no emissions
                #This happens when a kill event is sent
                #Note: If the first() wasn't there then on_complete would be called
                self._setExitReason(experiment.model.codes.exitReasons['Killed'])
                info['engineExitReason'] = experiment.model.codes.exitReasons['Killed']

            self._termination_subject.on_completed()

            self.emit_now(info)

        def HandleTaskObservableCompletion():
            '''The function reports the task observable completion'''

            self.log.log(19, 'Handling task observable completion - exitReason: %s' % (
                self._exitReason
            ))

        def Terminate(emission):

            assert emission == experiment.model.codes.exitReasons['Killed']
            if self.process is not None:
                self.process.kill()
            self.emit_now()

        # Terminate before run() is handled by observable set up in init
        #
        # 1. No termination (1A)
        # 2. Termination after startObservable emits (1B below)
        # 3. Termination after run() called but before startObservable emits (3)
        # 4. Some exception during launch process e.g. from TaskGenerator (4)
        #
        # 1. StartObservable is emitted
        #   - TaskLaunchObservable inits perf info, launches and emits state, and emits process
        #   - TaskWaitObservable enters wait on a different thread
        #   - PostLaunchTerminationObservable, sees process and filters it out
        # 2A. StartObservable is not followed by termination
        #   - TaskWaitObservable waits task exit, finalises perfInfo, and HandlesTaskExit (perf info written, emit state), then completes
        #       - Completion stops terminationObservable (with error due to empty sequence)
        #   - TaskLaunchObservable will complete after terminationObservable completes/error
        #   - PostLaunchTerminationObservable will complete after terminationObservable complete (no error as it saw a process)
        # 2B. StartObservable is followed by termination
        #   - PostLaunchTerminationObservable, sees termination -> kill process (causing the wait to exit) -> then completes (first())
        #   - TaskWaitObservable waits task exit, finalises perfInfo, and HandlesTaskExit (perf info written, emit state), then completes
        #       - Completion stops terminationObservable (with error due to empty sequence)
        #   - TaskLaunchObservable will complete after terminationObservable completes
        # 3. Termination after run but before startObservable emits
        #   - TaskLaunchObservable will complete due to take_while
        #       - an EmptySequenceError will be propagated to observers after the take_while
        #   - TaskWaitObservable will exit with an error (EmptySequenceError)
        #            - HandleTaskObservableException will emit state
        #            - No perf info created or written
        #   - PostLaunchTerminationObservable, sees termination, no process to kill -> completes
        # 4. Exception during launch process i.e. after InitPerformanceInfo called (other than EmptySequenceError)
        #    -  TaskWaitObservable will exit with an error (EmptySequenceError)
        #           - HandleTaskObservableException will kill the process if alive, finalise perf-info, and emit state

        #
        # TaskObservable
        # Switch threads after the second take_while so 3 initial launch tasks have to complete and start Wait()
        # once the chain is started - use a dedicated thread pool (taskPoolScheduler) to wait for Tasks
        # i.e. Since they are on same thread none can process isComplete until the last has finished.
        # If the switch is not made then a Kill signal will cause the observable to terminate even with
        # a process in the pipe
        #
        # Note auto_connect arg is "2": This is to ensure both TaskWaitObservable and
        # PostLaunchTerminationObservable start at same time
        taskLaunchObservable: reactivex.ConnectableObservable = reactivex.merge(
            self.terminationObservable,
            startObservable.pipe(op.delay(ENGINE_LAUNCH_DELAY_SECONDS)),
        ).pipe(
            op.subscribe_on(Engine.enginePoolScheduler),
            op.take_while(lambda e: self.exitReason() is None),
            op.take_while(lambda e: e not in experiment.model.codes.exitReasons),
            op.observe_on(Engine.enginePoolScheduler),
            op.first(),
            op.map(InitPerformanceInfo),
            op.map(LaunchTask),
            op.map(SetLaunchTime),
            op.publish(),
        ).auto_connect(2)

        #
        # TaskWaitObservable
        #
        # We switch threads so actions happen on a different thread to the PostLaunchTerminationObservable
        handle_task_exit = experiment.runtime.utilities.rx.report_exceptions(HandleTaskExit, self.log, 'HandleTaskExit')
        handle_task_exit_error = experiment.runtime.utilities.rx.report_exceptions(
            HandleTaskObservableException, self.log, 'HandleTaskExitError')
        task_observable_complete = experiment.runtime.utilities.rx.report_exceptions(
            HandleTaskObservableCompletion, self.log, 'HandleTaskObservableCompletion')

        taskLaunchObservable.pipe(
            op.observe_on(Engine.taskPoolScheduler),
            op.map(Wait),
            op.map(FinalisePerformanceInfo)
        ).subscribe(on_next=handle_task_exit,  # set exit-reason emits state
                       on_error=handle_task_exit_error,  # Cleans-up perfdata, sets exit reason, emits state
                       on_completed=task_observable_complete)  # Prints debug message


        #
        #PostLaunchTerminationObservable
        #
        #This will NEVER get termination signals BEFORE A process is launched
        #If it gets one the process either launched OR will never launch
        terminate = experiment.runtime.utilities.rx.report_exceptions(Terminate, self.log, 'Terminate')
        taskLaunchObservable.pipe(
            op.observe_on(Engine.enginePoolScheduler),
            # VV: The code below expects emissions to be hashable. It checks whether they're an exitReason or not.
            # However, taskLaunchObservable emits dictionaries, so convert those to None
            op.map(lambda x: None),
            op.concat(self.terminationObservable),
            op.filter(lambda x: x in experiment.model.codes.exitReasons),
            op.first(),
        ).subscribe(on_next=terminate, on_error=CheckState)

    def resubmissionAttempts(self):

        '''How many resubmissions have been attempted.

        A resubmission is due to SubmissionFailed'''

        return self._resubmissionAttempts

    def restart(self, reason=None, code=None):
        # type: (str, int) -> str
        """Attempts to restart a job

        This method does not block if a restart is deemed possible: `self.run` wil be called in a separate thread.
        A component can optionally provide a restart hook

        Parameters;
            reason: Optional. A string from experiment.codes.exitReasons indicating the exit reason of the last execution
                If not given the receivers exitReason() method is used
                If this is None the method will return False.
            code: Optional. An integer indicating the return code of the last execution
                If not given the receivers returncode() method is used.
                This is only used as information to provide the restartHook

        An Engine may only restart up to workflowAttributes.maxRestarts times:
            0: cannot restart at all
            -1: may restart an infinite number of times
            N: up to N times where N >= 1

        If this engine has not reached it's maximum restarts, the behaviour of this method depends on the exitReason:
        - SubmissionFailed:
           - Restart hook is not called and engine is started as if for the first time
        - ResourceExhausted:
           - Restart hook is called if there is one.
           - Otherwise a vanilla restart is attempted
           - If the restart hook returns False nothing happens
        - KnownIssue:
           - Restart hook is called if there is one.
           - Otherwise restart fails.
        - Any other exit reason: No restart

        Returns:
             An experiment.codes.restartCode value
             - RestartInitiated
             - RestartNotRequired
             - RestartCouldNotInitiate

        Raises:
             A restart is only possible if engine.isAlive is False OR engine.run() was never called.
             An AssertionError is raised if neither of these is True.
        """

        #To restart either run() has never been called or isAlive is False
        #This means it is possible to restart if kill() is sent before restart with no run()
        assert (self._runCalled is None or self.isAlive() is False)

        #FIXME: NEED TO RESET taskLaunched/taskFinished etc.
        #Recreate terminationObservable if isAlive is False
        if not self.isAlive():
            self._create_termination_observable()

        # VV: Check if restarting the engine would result in the engine restarting one too many times
        max_restarts = self.job.workflowAttributes.get('maxRestarts', None)
        if max_restarts is None:
            if self.job.workflowAttributes.get('restartHookFile'):
                max_restarts = -1
            else:
                max_restarts = 3

        if (max_restarts != -1) and (self.restarts + 1 > max_restarts):
            self.log.info("Already restarted maximum number of times (%d) - will return RestartMaxAttemptsExceeded" %
                          max_restarts)
            return experiment.model.codes.restartCodes['RestartMaxAttemptsExceeded']

        reason = reason if reason is not None else self.exitReason()
        code = code if code is not None else self.returncode()

        restartContext = experiment.model.codes.restartContexts['RestartContextRestartNotPossible']

        # VV: These are experiment.codes.exitReasons
        reasons_for_restart = self.job.workflowAttributes.get('restartHookOn', [])

        if self.job.type == 'simulator' and reason in reasons_for_restart \
                and self.job.customAttributes.get('sim_restart', 'yes').lower() in ['yes', 'true']:
            self.log.log(19, "Can restart because this is a Simulated engine")
            restartContext = experiment.model.codes.restartContexts['RestartContextRestartPossible']
            if reason != experiment.model.codes.exitReasons["SubmissionFailed"]:
                self.restarts += 1
        elif reason == experiment.model.codes.exitReasons["SubmissionFailed"]:
            self.log.log(19, "Can restart because SubmissionFailed")
            restartContext = experiment.model.codes.restartContexts['RestartContextRestartPossible']
        elif reason in reasons_for_restart:
            self.log.log(19, "Try to restart because %s" % reason)
            # Note: This internal ivar is only tracking continuationRestarts - rename
            self.restarts += 1

            RestartHook = None
            # Check does the package provide a restart hook
            restart_hook_file = self.job.workflowAttributes.get('restartHookFile')

            # VV: Users can set restartHookFile to the empty string to signify that they don't want to
            # use a restart-hook, None on the other-hand means they wish to use the default path to the restart
            # hook file (which is "restart.py").
            if restart_hook_file != '':
                module_name = None
                try:
                    # VV: Do not use `import` here; it breaks unit-tests because if the module is found in one unit-test
                    # it's also picked up by the next unit-tests instead of the modules that the subsequent tests define
                    hooks_dir = os.path.join(self.job.workflowGraph.rootStorage.instancePath, 'hooks')
                    module_name = restart_hook_file if restart_hook_file is not None else 'restart.py'
                    new_module = experiment.model.hooks.import_hooks_restart(hooks_dir, module_name)
                    RestartHook = new_module.Restart
                    self.log.log(19, 'Using custom restart hook to restart component on ResourceExhaustion')
                except (ImportError, IOError):
                    self.log.log(19, f"Unable to import hook.restart {module_name} - will use fallback")

            if RestartHook is None:
                if not restart_hook_file:
                    self.log.info('No module hook.restart provided - using fallbacks (deprecated)')
                else:
                    self.log.log(19, 'No module hook.restart found - using fallbacks')
                RestartHook = DLMESORestart

            # FIXME: Hacky restart mechanism to handle DLMESO and other jobs
            # If there was no hook assume DLMESO restart.
            # If the DLMESO restart fails due to IOError (issues with reading CONTROL file)
            # then its not a DLMESO job and we try a vanilla restart
            try:
                #Two RestartHook interfaces
                #Old: Returns bool
                #New: Returns a restart-contex
                #- False, RestartContextHookNotAvailable -> Vanilla restart
                #- False, RestartContextRestartNotRequired -> No restart
                #- False, RestartContextRestartNotPossible
                #- False, RestartContextHookFailed
                #- True, RestartContextRestartPossible
                #For backwards compatibility we will treat a bool as following
                #- True -> RestartContextRestartPossible
                #- False -> RestartContextRestartNotRequired
                restartContext = RestartHook(workingDirectory=self.job.directory,
                                        restarts=self.restarts,
                                        componentName=self.job.name,
                                        log=self.log,
                                        exitReason=reason,
                                        exitCode=code)

                self.log.log(19, "Restart hook %s returned restartContext:%s" % (RestartHook, restartContext))
            except IOError as error:
                self.log.warning("IOError on attempting restart (%s). Assuming vanilla component" % error)
                restartContext = experiment.model.codes.restartContexts['RestartContextHookNotAvailable']
            except Exception as error:
                self.log.critical(
                "The restart hook raised exception: %s. Will consider it RestartContextHookFailed" % str(error))
                restartContext = experiment.model.codes.restartContexts['RestartContextHookFailed']
                self.log.log(18, 'Traceback: %s' % traceback.format_exc())
            finally:
                # Convert bools
                if isinstance(restartContext, bool):
                    # If restartContext is False and no reason is given assume its because its not needed
                    restartContext = experiment.model.codes.restartContexts[
                        'RestartContextRestartPossible'] if restartContext is True else \
                        experiment.model.codes.restartContexts["RestartContextRestartNotRequired"]

                    # Handle other return values
                if (isinstance(restartContext, string_types) is False) \
                        or (restartContext not in experiment.model.codes.restartContexts.keys()):
                    self.log.warning(
                        "The restart hook returned unknown value %s for restartContext, will consider "
                        "it RestartContextHookNotAvailable" % str(restartContext))
                    restartContext = experiment.model.codes.restartContexts['RestartContextHookNotAvailable']

        else:
            self.log.info("Engine does not meet restarting conditions (restartHookOn=%s)" % reasons_for_restart)
            restartContext = experiment.model.codes.restartContexts['RestartContextRestartConditionsNotMet']

        # Move stdout/stderr of previous job
        # If it fails abandon restart
        if restartContext in [experiment.model.codes.restartContexts["RestartContextRestartPossible"],
                              experiment.model.codes.restartContexts["RestartContextHookNotAvailable"]]:
            try:
                name = os.path.join(self.job.directory, "out.stdout")
                basename = os.path.split(name)[1]
                tempname = os.path.join(self.job.directory, '%s%d' % (basename, self.restarts))
                shutil.move(name, tempname)
            except Exception as error:
                self.log.log(18, "Exception while archiving previous Task outputs during restart. Ignoring: %s" %
                             str(error))

        # Try to restart if context allows ...
        if restartContext in [experiment.model.codes.restartContexts["RestartContextRestartPossible"],
                              experiment.model.codes.restartContexts["RestartContextHookNotAvailable"]]:
            try:
                #Reset ivars that determine isAlive (so the retval of isAlive is True)
                self.process = None
                self.lastExecution = True
                self._taskFinished = None
                self._taskLaunched = None
                self._exitReason = None
                self.run()
                restartCode = experiment.model.codes.restartCodes['RestartInitiated']
            except Exception as error:
                self.log.warning("Unable to restart job - exception while attempting to run new job: %s" % str(error))
                restartCode = experiment.model.codes.restartCodes['RestartCouldNotInitiate']
        elif restartContext == experiment.model.codes.restartContexts["RestartContextRestartNotRequired"]:
            restartCode = experiment.model.codes.restartCodes['RestartNotRequired']
        else:
            self.log.warning("Eventually, unable to restart engine")
            restartCode = experiment.model.codes.restartCodes['RestartCouldNotInitiate']

        if restartCode == experiment.model.codes.restartCodes['RestartInitiated'] \
                and reason == experiment.model.codes.exitReasons['SubmissionFailed']:
            # VV: @tag:RestartEngines
            # VV: Increase the resubmissionAttempts counter if the last exit reason is SubmissionFailed
            self._resubmissionAttempts += 1
            self.log.info("Current resubmission attempts: %d" % self._resubmissionAttempts)

        return restartCode

    @property
    def consume(self):
        # type: () -> bool
        """Returns whether Engine was ever able to consume"""
        return self._consume

    def canConsume(self, delay=0, force=False):
        '''Returns True if there is output available for the Job to consume, sets self._consume to True too

        Parameters:
            delay (int): An integer. If greater than 0 only files which have existed for at least delay
                seconds are counted as output
            force (bool): By default, return True without checking the filesystem for new producer outputs if
                at some point in the past it had detected new outputs. Setting this argument to True instructs
                method to check the filesystem for new outputs even if it has found novel outputs before.

        TODO: Perhaps move to Job class.

        Same Stage:
            Delay = 0 and no new files in working dir since it was created = False
            Delay = 0 and new files in working working dir since it was created = True
            Delay = X and no new files older than X present = False
            Delay = X and new files older than X present = True
        Different Stage:
            Always True

        Exceptions:

        Raises experiment.errors.FilesystemInconsistencyError if there is an issue in accessing the producer
        Jobs' output directories'''

        # VV: If could consume at some point then skip checking the filesystem and claim that can consume now too
        if (not force) and self._consume:
            return True

        # FIXME: (Sync-Point) The consumers must be able to consume the newly created files without error
        # e.g. the may be notified of existence of files before the expected data in the files is fully written
        # This is mainly an issue for non-repeating jobs
        consume = True
        # Don't launch unless jobs we consume from have output
        # NOTE: This only matters for producers which are in the same stage
        for producer in self.job.producerInstances:
            if producer.stageIndex == self.job.stageIndex:
                if delay == 0:
                    if len(producer.workingDirectory.output) == 0:
                        consume = False
                        self.log.info("%s: Waiting on output from producer %s (dir: %s)" % (
                        self.job.identification, producer.identification, producer.workingDirectory.path))
                        break
                else:
                    date = datetime.datetime.now() - datetime.timedelta(seconds=delay)
                    if len(producer.workingDirectory.outputBeforeDate(date)) == 0:
                        consume = False
                        self.log.info("%s. Waiting on output from producer %s (delay: %d, dir: %s)" % (
                        self.job.identification, producer.identification, delay, producer.workingDirectory.path))
                        break

        if consume:
            self._consume = True

        return consume

    def returncode(self):

        '''Returns an integer indicating how the engines Task ended

        0 indicates no problem, 1 an issue.

        If the receiver isAlive then this method returns None'''

        return compute_returncode_from_exit_reason(self.exitReason())

    def _setExitReason(self, reason):

        '''
        Sets the exit reason
        '''

        #we should never enter here if self.process is alive or with no reason
        assert reason is not None
        assert (self.process is None or self.process.isAlive() is False)

        #FIXME: If there is a reason we should use that reason ...
        if self.process is not None:
            reason = self.process.exitReason if self.process.exitReason is not None else reason

        self._exitReason = reason
        self.log.log(19, 'Set exit reason to %s' % self._exitReason)

        if reason == experiment.model.codes.exitReasons['Success']:
            # VV: Reset the resubmissionAttempts
            self._resubmissionAttempts = 0

        self.emit_now()

    def exitReason(self):

        '''Returns a string describing why the engine exited

        There are seven possible reasons defined by the experiment.codes.exitReasons module variable.

        If the engine has not exited or started this method returns None
        If the engine received kill() but asynchronous events associated
        with this have not completed this method will return None
        '''

        return self._exitReason

    def isAlive(self):

        '''Returns True if the Engine is alive, false otherwise.

        Returns (bool):
            True if the Task running or is waiting to be run
            False if process is exited or internal exitReason has been set

        Note: Due to asynchronicity of kill() a process can be alive after receiving it'''

        return compute_is_alive_from_exit_reason(self.exitReason())

    def kill(self):

        '''Kills the Engine.

        Notes:
            This method initiates asynchronous events.
            As a result isAlive() will return True after this method is called
            until all these events have finished.

        Does nothing if receiver is already dead OR if kill() already called

        Note: The current process will be killed
        '''
        if self.isAlive():
            self.log.info("Received kill")
            self._termination_subject.on_next(experiment.model.codes.exitReasons['Killed'])
        else:
            self.log.debug('Received kill but isAlive is False')

    @property
    def isShutdown(self):

        '''Returns True if shutdown() has been called externally otherwise False'''

        return self._shutdown

    def shutdown(self):

        '''Used to inform the receiver, which is non-active, that it is in its final state

        Will raise an AssertionError if the engine is not dead (isAlive() == True)

        Usually will be called by a Component instance which owns the receiver.

        Description

        Engines cannot tell if they will be restarted or not on an non-success exitReason.
        Thus an external agent must indicate this state change to them so they can e.g. stop emitting state observables

        Notes:

        (1) This does NOT terminate the engine (use kill())

        (2)  !isAlive() != isShutdown. This is because when isAlive returns False the state may be transient.
        isActive would be a better name.
        '''

        if self.isAlive():
            raise AssertionError('An engine must be non-active (dead) to be shutdown')
        self.log.debug("Engine asked to shutdown")
        self._shutdown = True

        self.emit_now()

    @property
    def lastLaunched(self):
        # type: () -> datetime.datetime
        '''Returns date the repeating job was last run'''

        return self._lastLaunched

    @lastLaunched.setter
    def lastLaunched(self, value):
        with self.lastLaunchedLock:
            if value > self._lastLaunched:
                self._lastLaunched = value
            else:
                self.log.warning("Error: Trying to set last launched to older date")

    def compute_task_events(self, process):

        if process:
            status = process.status
        else:
            status = None

        if process:
            perf_info = process.performanceInfo.getElements()
        else:
            perf_info = {}

        states_pre_exec = [
            experiment.model.codes.INITIALISING_STATE,
            experiment.model.codes.RESOURCE_WAIT_STATE,
        ]

        states_finished = [
            experiment.model.codes.FAILED_STATE,
            experiment.model.codes.FINISHED_STATE,
            experiment.model.codes.SHUTDOWN_STATE,
        ]

        states_running = [
            experiment.model.codes.RUNNING_STATE,
            experiment.model.codes.OUTPUT_DATA_WAIT_STATE,
        ]
        # VV: Always pair a taskDateComplete emission with its returnCode
        #    FIXME This is actually not guaranteed to be correct, there's a chance a new process
        #    was created while the current thread was doing stuff in this method, the chance is
        #    small so we can assume it'll never hurt us

        ret = {
            'taskDateWait': None,
            'taskDateExecute': None,
            'taskDateComplete': None,
        }

        translate = {
            'epoch-submitted': 'taskDateWait',
            'epoch-started': 'taskDateExecute',
            'epoch-finished': 'taskDateComplete'
        }

        # VV: Take into account the current state of the process when considering the potential
        #     updates to task-events. For example, a process which is still waiting cannot supply
        #     its `epoch-started` timestamp

        if status in states_finished:
            lbl_epochs = ['epoch-submitted', 'epoch-started', 'epoch-finished']
        elif status in states_running:
            lbl_epochs = ['epoch-submitted', 'epoch-started']
        elif status in states_pre_exec:
            lbl_epochs = ['epoch-submitted']
        else:
            lbl_epochs = []

        # VV: Start converting timestamps to datetime.datetime() objects, if anything fails
        #     1.) panic, 2.) report error to user, 3.) disable the optimizer for all Engines
        for key in lbl_epochs:
            if key in perf_info:
                # VV: @tag: OPTIMIZER-observe-engine-updates
                # VV: Could it be that this is the cause for duplicated events ? Converting the
                # same epoch string into a datetime.datetime object is deterministic, so is there
                # a chance that a backend (LSF is typically the culprit) returns more than 1
                # timestamp for the *SAME* task-event ?
                # Convert all epochs into integers
                try:
                    epoch = (perf_info[key] or 'none').lower()
                    if epoch not in [None, '', 'none']:
                        ret[translate[key]] = datetime.datetime.strptime(epoch, "%d%m%y-%H%M%S")
                except ValueError:
                    self.log.critical("Epoch-timestamp %s is %s which is not a float. "
                                      "Will disable optimizer. EXCEPTION: %s\n" % (
                        key, perf_info.get(key, '*unknown*'), traceback.format_exc()
                    ))
                    self.optimizer_disable(True)
                except Exception:
                    self.log.critical("Something went wrong when parsing epoch timestamps. "
                                      "Will disable optimizer. perf_info[%s]=%s. EXCEPTION: %s\n" % (
                        key, perf_info.get(key, '*unknown*'), traceback.format_exc()
                    ))
                    self.optimizer_disable(True)

        # self.log.info("Received engine update: %s -- %s (%s)" % (
        #     status, perf_info, ret
        # ))

        return ret

    @property
    def stateDictionary(self):

        '''Returns a dictionary containing detailed information on current state of the receiver'''

        #NOTE: In non-repeating engine lastLaunched really is engine creationTime
        #Need to fix this
        if not self._runCalled:
            waitTime = 'N/A'
        elif not self._taskLaunched:
            #If task isn't launched and we're dead we're not waiting anymore
            #If we keep incrementing waitTime not only is it meaningless
            #but the stateUpdates observable will keep going
            #However we've no access to the last wait time before termination
            #As a fix if we terminated while waiting we set waitTime to N/A
            #We identify this by checking if exitReason is set (+ last hasn't launched)
            if self.exitReason() is None:
                waitTime = datetime.datetime.now() - self._runCalled
            else:
                waitTime = 'N/A'
        else:
            waitTime = self._taskLaunched - self._runCalled

        if not self._taskLaunched:
            runTime = 'N/A'
        elif not self._taskFinished:
            runTime = datetime.datetime.now() - self._runCalled
        else:
            runTime = self._taskFinished - self._taskLaunched

        def generate_update(runTime, waitTime):

            #_taskFinished is only None if a process was never launched
            #If self.process was set  i.e. a process was launched successfully, _taskFinished is always set

            # Cache ivars accessed multiple times
            process = self.process
            runCalled = self._runCalled
            taskFinished = self._taskFinished

            #isAlive() and returncode() are both functions of exitReason().
            #This can lead to inconsistencies if e.g. exitReason() is accessed and then change beforee `returncode` is accessed
            exitReason = self.exitReason()
            isAlive = compute_is_alive_from_exit_reason(exitReason)
            returncode  = compute_returncode_from_exit_reason(exitReason)

            ret = {
                'sourceType': 'engine',
                'reference': self.job.reference,
                'isAlive': isAlive,
                'isShutdown': self.isShutdown,
                'backend': self.job.type,
                'creationDate': self.lastLaunched,
                # VV: Engine does not use this but einspect.py and repeatingEngine do; the latter sets field
                'isWaitingOnOutput': runCalled is not None,
                'outputWaitTime': waitTime,
                'isRunning': True if isAlive and runCalled is not None else False,
                'runDate': runCalled,
                'lastTaskLaunchDate': self._taskLaunched,
                'lastTaskRunTime': runTime,
                'lastTaskRunState': process.status if process is not None else 'N/A',
                'lastTaskFinishedDate': taskFinished,
                'lastTaskExitCode': process.returncode if taskFinished is not None else 'N/A',
                'lastTaskExitReason': process.exitReason if taskFinished is not None else 'N/A',
                'engineExitCode': returncode,
                'engineExitReason': exitReason,
                'schedulerId': process.schedulerId if process is not None else 'N/A',
                # VV: see self.compute_task_events()
                'taskDateWait': None,
                'taskDateExecute': None,
                'taskDateComplete': None,
            }

            task_events = self.compute_task_events(process)

            ret.update(task_events)
            return ret

        ret = generate_update(runTime, waitTime)

        return ret


    @property
    def state(self):

        '''Returns an rx observable that will emit (state, engine) tuples every 10 secs

        Not multicast.

        VERY IMPORTANT: This observable DOES NOT self terminate and will emit forever if subscribed to.
        Callers MUST dispose of the observable once finished with it

        stateUpdates and notifyFinished both have completion criteria and WILL self terminate once
        engine is killed.
        '''

        return self._detailedState

    def _create_state_updates(self) -> reactivex.ConnectableObservable:
        """Creates and publishes state updates and makes sure that it will emit at least once."""
        def StateClosure(state):
            state_change = [
                'lastTaskRunTime', 'lastTaskRunState', 'lastTaskExitCode', 'lastTaskExitReason',
                'engineExitCode', 'engineExitReason', 'isAlive', 'timeToNextKernelLaunch',
                'lastTaskRunTime',
            ]

            def StateFilter(e):
                # Enters as a tuple with two elements (state, component)
                newState = e[0]
                filtered = {
                    k: newState[k] for k in list(newState.keys()) if newState[k] != state[k]
                }
                state.update(newState)

                detected_state_change = any([key in filtered for key in state_change])

                if detected_state_change:
                    # VV: When a task Changes state ensure that meta-information is included in the emission
                    for key in state_change:
                        if key in newState:
                            filtered[key] = newState[key]

                # VV: FIXME Ensure that taskDateComplete always bundles lastTaskExitCode
                if 'taskDateComplete' in filtered and newState['lastTaskExitCode'] in [None, 'N/A']:
                    # VV: It looks like taskDateComplete is emitted, but lastTaskExitCode is not
                    #     delete taskDateComplete and hope that it will be included in the next emission
                    del filtered['taskDateComplete']

                return filtered, newState, e[1]

            return StateFilter

        stateData = self.stateDictionary

        def may_trigger_final_emission_to_cause_complete(x:Tuple[Dict[str, Any], Dict[str, Any], Engine]):
            """This action may trigger an emitNow if isShutdown is part of this emission-delta

            self.shutdown() sets self._shutdown which is then propagated to stateDictionary. stop_emitting()
            causes the observable to terminate when the emission delta is empty AND isShutdown is set to true.

            The may_trigger_final_emission_to_cause_complete() emission triggers an emission which is expected
            to have an empty "state_filtered" which will then trigger stop_emitting() to return true and therefore
            cause the observable to complete.
            """
            state_filtered, state_whole, engine = x
            if state_filtered.get('isShutdown'):
                self.emit_now()

        def stop_emitting(x: Tuple[Dict[str, Any], Dict[str, Any], Engine]) -> bool:
            """An engine stops emitting updates when it has been asked to shutdown AND it has no
            more events to emit
            """
            state_filtered, state_whole, engine = x
            return state_whole['isShutdown'] is True and len(state_filtered) == 0

        def on_terminate():
            self.log.info('Engine state observable got terminate message (isAlive:%s isShutdown:%s exitReason:%s)' % (
                             self.isAlive(), self.isShutdown, self.exitReason()))
            self._emit_no_more_manual()

        def final_map(x):
            # type: (Tuple[Dict[str, Any], Dict[str, Any], Engine]) -> Tuple[Dict[str, Any], Engine]
            state_filtered, state_whole, engine = x
            state_filtered.update({
                'sourceType': stateData['sourceType'],
                'reference': self.job.reference
            })

            return state_filtered, engine

        # First remove keys that aren't the same, then don't emit empty dicts, then add back missing metadata
        # Note since update returns None we can use `or e` to have the lambda return e after the update
        # Finish if dict is empty and condition is met
        #
        #Note: We switch to a specific thread after the take_while to ensure that in case of "Success"
        #the first `isAlive=False` emission gets through to trigger notifyFinished.
        #
        #observe_on is not guaranteed to use the same thread from a thread pool each time
        #(https://github.com/ReactiveX/RxPY/issues/135)
        #Hence we make a thread-pool of size 1 to force this serialisation

        serialiseScheduler = reactivex.scheduler.ThreadPoolScheduler(1)

        # VV: Is this a BehaviorSubject ?
        state_updates: reactivex.ConnectableObservable = self.state.pipe(
            op.observe_on(serialiseScheduler),
            op.map(StateClosure(stateData)),
            op.map(CheckState),
            op.take_while(lambda x: not stop_emitting(x)),
            op.do_action(on_next=may_trigger_final_emission_to_cause_complete),
            op.do_action(on_completed=on_terminate),
            op.filter(lambda x: len(x[0]) != 0),
            op.map(final_map),
            op.publish(),
        ).auto_connect().pipe(
            op.observe_on(Engine.enginePoolScheduler)
        )

        return state_updates

    @property
    def stateUpdates(self) -> reactivex.ConnectableObservable:
        '''Returns an rx observable that will emit changes to stateDictionary when they occur only

        The emission will only contain changed keys that change after subscribing
        i.e. the initial state will not be emitted only updates to this state

        The observables completes after the last update after the engine is killed or exits with success
        '''
        def generate_artificial_observation(which):
            # type: (Engine) -> Tuple[Dict[str, Any], Engine]
            which.log.log(14, "Emit artificial because my Observable is already complete" % which.stateDictionary)

            return which.stateDictionary, which

        # VV: Use the default_if_empty() RX operator to handle the `sequence contains no elements` errors
        # When the state updates are empty we emit an artificial observation (stateDictionary and self)
        return self._state_updates.pipe(
            op.default_if_empty(default_value=None),
            op.map(lambda e: e if e is not None else generate_artificial_observation(self))
        )

    @property
    def notifyFinished(self) -> reactivex.ConnectableObservable:

        '''
        Returns a published auto-connect rx.Observable that emits a single event when the receiver finishes/dies

        The event is a tuple. The first element will be False (indicating death).
        The second will be the exit reason (as returned by exitReason() method)
        The third will be the receiver
        '''
        default_value = ({}, self)
        return self.stateUpdates.pipe(op.first_or_default(lambda x: x[0].get('isAlive') is False, default_value))

class RepeatingEngine(Engine):

    def __init__(self, componentSpecification, taskGenerator):

        Engine.__init__(self, componentSpecification, taskGenerator=taskGenerator, init_state_updates=False)

        # VV: This starts as False, but then gets set to True the first time self.canConsume() returns True.
        # Future canConsume() calls, when self._consume is True, will skip checking the filesystem and immediately
        # return True (unless the `force` parameter is set to True) - canConsume is only called in run()
        # everything else uses self.consume which is a property that returns self._consume
        self._consume = False

        #TODO: Temporary as RepeatingEngine is not using the termination subject set up in Engine init.
        self._termination_subject.on_completed()

        #Indicates that the components the engine is repeating on is finished
        #FIXME: Will be moved - the engine should be ignorant of the graph and decisions
        # based on it e.g. how/when things should be executed
        #This is for separation of concerns. The component layer should handle all this.
        #i.e. the Engine should be just given some observable that says if it should execute or not on each repeat
        self._producers_are_finished = False
        self._suicide = False
        self.lastExecution = False
        #Indicates that the repeating kernel won't execute again.
        self.kernelCompleted = False
        self.log = logging.getLogger('RepEng.%s' % self.job.reference.lower())
        self.producer_recently_finished_successfully = False
        max_retries = self.job.workflowAttributes['repeatRetries']

        if max_retries is None:
            max_retries = 3

        try:
            self._dieAfter = float(self.job.flowir_description['variables']['kill-after-producers-done-delay'])
        except KeyError:
            self._dieAfter = None

        self._stateDict = {
            'sourceType': 'repeatingEngine',
            'reference': self.job.reference,
            'repeatInterval': self.lastCachedRepeatIntervalDecision(),
            'isAlive': self.isAlive(),
            'backend': self.job.type,
            'creationDate': self.lastLaunched,
            'isRunning': False,
            'runDate': None,
            'lastKernelLaunchDate': None,
            'lastKernelFinishedDate': None,
            'timeToNextKernelLaunch': 'N/A',
            'isWaitingOnOutput': False,
            'outputWaitTime': 0,
            'numberTaskLaunches': 0,
            'repeatRetries': max_retries,
            'lastTaskLaunchDate': None,
            'lastTaskRunTime': 0,
            'lastTaskFinishedDate': None,
            'timeSinceLastTaskLaunch': 0,
            'lastTaskRunState': 'N/A',
            'lastTaskExitCode': 'N/A',
            'lastTaskExitReason': 'N/A',
            'engineExitCode': self.returncode(),
            'engineExitReason': self.exitReason()}

        # VV: Create observable now that stateDict is generated (because self.stateDictionary accesses it)
        self._state_updates = self._create_state_updates()

    def notify_producer_successful_run(self, producer_reference):
        # VV: One of our producers has just executed successfully,
        self.log.log(19, "Just found out that %s has executed a Task successfully, "
                      "will now attempt to immediately repeat" % (
            producer_reference
        ))
        self.producer_recently_finished_successfully = True

    def lastCachedRepeatIntervalDecision(self):
        with self.opt_lock:
            if self.may_use_optimizer() is False or self._last_cached_repeat_interval_decision is None:
                return self.job.repeatInterval()

            return self._last_cached_repeat_interval_decision

    def may_use_optimizer(self):
        return not self.job.workflowAttributes['optimizer']['disable']

    def nextRepeatInterval(self):
        with self.opt_lock:
            if self.may_use_optimizer() is False or self.optimizer is None:
                return self.job.repeatInterval()

            if self.producer_recently_finished_successfully:
                return 5.0

            if self.optimizer:
                try:
                    stage = 'stage%d' % self.job.stageIndex
                    next_delay = self.optimizer.suggest_next_delay(stage, self.job.name,
                                                                   datetime.datetime.now())
                    if next_delay is not None:
                        # self.log.info("Optimizer suggests delay %s" % next_delay)
                        self._last_cached_repeat_interval_decision = next_delay
                        return next_delay
                except Exception as e:
                    self.log.critical("Failed to get new repeat interval from Optimizer. EXCEPTION:%s" %
                                      traceback.format_exc())
                    self.optimizer_disable(True)

        return self.job.repeatInterval()

    def _perfData_initialize(self, init_time, isNewOutput):
        # type: (datetime.datetime, bool) -> Dict[str, Any]

        items = list(zip(self.performanceHeaders, len(self.performanceHeaders) * [0]))
        perfData = collections.OrderedDict(items)
        perfData['component-launch-time'] = init_time.strftime("%d%m%y-%H%M%S")
        perfData['can-consume'] = "%s" % self.consume
        perfData['output-available'] = "%s" % isNewOutput
        # VV: Tasks which did not launch (e.g. because .consume is false) have `None` exit-code
        perfData['exit-code'] = None
        perfData['scheduler-id'] = ''

        return perfData

    def _perfData_before_launch(self, perfData, launch_time):
        # type: (Dict[str, Any], datetime.datetime) -> Dict[str, Any]
        if (self.performanceMatrix.numberOfRows() > 0
                # VV: Ensure that this is not a None value (could None be stored as 'none' ?)
                and isinstance(self.performanceMatrix.column("task-run-time")[-1], (float, int ))
        ):
            # First calculate time since last launch
            # then subtract run-time of the last launch
            perfData["delta-last-task"] = experiment.runtime.monitor.TotalSeconds(
                launch_time - self.lastLaunched
            )
            perfData["delta-last-task"] -= self.performanceMatrix.column("task-run-time")[-1]
        else:
            perfData["delta-last-task"] = 0

        return perfData

    def _perfData_launch_failed(self, perfData, finish_time):
        # type: (Dict[str, Any], datetime.datetime) -> Dict[str, Any]
        perfData['task-run-time'] = experiment.runtime.monitor.TotalSeconds(
            finish_time - self.lastLaunched
        )
        perfData['exit-code'] = -127
        perfData['data-produced'] = 0
        perfData['task-production-rate'] = 0
        perfData['component-production-rate'] = 0
        # VV: There was an exception while the launching the task fetch an empty performance info vector
        task_class_type = experiment.runtime.backends.backendTaskMap[self.job.type]
        perfData.update(task_class_type.default_performance_info().getElements())
        return perfData

    def _perfData_launch_succeeded(self, perfData, finish_time, my_process):
        # type: (Dict[str, Any], datetime.datetime, Task) -> Dict[str, Any]
        perfData['task-run-time'] = experiment.runtime.monitor.TotalSeconds(finish_time - self.lastLaunched)
        perfData['exit-code'] = my_process.returncode
        perfData['scheduler-id'] = my_process.schedulerId
        # TODO: Calculate data produced
        # We have to capture new files and appended files and overwritten files
        perfData['data-produced'] = 0
        perfData['task-production-rate'] = perfData['data-produced'] / perfData['task-run-time']
        perfData['component-production-rate'] = perfData['data-produced'] / (
                perfData['delta-last-task'] + perfData['task-run-time']
        )
        perfData.update(my_process.performanceInfo.getElements())
        return perfData

    def _perfData_register(self, perfData, force_write=False):
        # type: (Optional[Dict[str, Any]], bool) -> None
        if perfData is not None:
            self.performanceMatrix.addElements(perfData)
        if force_write or (
                experiment.runtime.monitor.TotalSeconds(datetime.datetime.now() - self.lastPerformanceWrite) > 120):
            if self.performanceMatrix.numberOfRows() > 0:
                performanceMatrix = self.performanceMatrix
                self.lastPerformanceWrite = datetime.datetime.now()
            else:
                self.log.log(19, "PerformanceInformation is empty will just generate an empty CSV file")
                try:
                    task_class_type = experiment.runtime.backends.backendTaskMap[self.job.type]
                except KeyError:
                    self.log.warning("Unknown job-type %s cannot determine task-class out of it" % self.job.type)
                    return
                performanceMatrix = task_class_type.default_performance_info()

            try:
                with open(os.path.join(self.job.workingDirectory.path, 'component_performance.csv'), 'w') as f:
                    print(performanceMatrix.csvRepresentation(), end=' ', file=f)
            except IOError:
                pass

    def run(self):

        '''Over-ride Engine.run() with repeating version'''
        self._prime()

        self._stateDict['runDate'] = datetime.datetime.now()

        # Default value of checkProducerOutput is True
        # VV: FIXME Promote `check-producer-output` to field of `workflowAttributes` section
        map_to_bool = { 'false': False, 'no': False, 'yes': True, 'true': True,}
        checkProducerOutput = map_to_bool[
            self.job.flowir_description['variables'].get('check-producer-output', 'true').lower()
        ]

        self.log.debug('Check for new producer output before launch?: %s' % checkProducerOutput)

        # Closure for running the repeating task
        def EngineTaskController(lastAction):

            '''
            Parameter:
                lastAction (bool): Notifies the receiver it won't be called again by the Monitor

            Exceptions:

            FilesystemInconsistencyError will be raised on first call if
            the producers working dirs can't be acessed

            Raises FilesystemInconsistencyError if stdout cannot be redirected.
            Raises JobLaunchError if Popen fails.'''

            self.producer_recently_finished_successfully = False

            if lastAction or self._suicide:
                self.kernelCompleted = lastAction
                if self.kernelCompleted:
                    self.log.info("I am now considered finished! - No more executions")
            else:
                # By default assume new output - only check if requested
                isNewOutput = True

                if checkProducerOutput:
                    if not self._producers_are_finished:
                        isNewOutput = self.job.producersHaveOutputSinceDate(self.lastLaunched)
                    else:
                        time_waiting = (datetime.datetime.now() - self.lastLaunched).total_seconds()
                        if time_waiting > 20.0:
                            # VV: FIXME We should consult the graph to figure out whether the producers have
                            #     finished rstage-outing their output files.
                            self.log.log(19, "I have waited for too long for my Finished producers to produce output")
                            isNewOutput = True
                        else:
                            self.log.log(19, "All of my producers are done but I am checking their outputs")
                            isNewOutput = self.job.producersHaveOutputSinceDate(self.lastLaunched)

                # VV: Find out whether producers have finished, then record launch-time
                producers_done_when_i_started = self._producers_are_finished
                launch_time = datetime.datetime.now()
                self._stateDict['lastKernelLaunchDate'] = launch_time

                # VV: Try to populate value of self._consume and handle any FileSystem-related errors
                try:
                    if not self._consume:
                        self.canConsume()
                except experiment.model.errors.FilesystemInconsistencyError as e:
                    self.log.info("Unable to test whether I canConsume() because of %s - will assume canConsume=%s" % (
                        e, self._consume))

                self.log.log(19, "Can consume: %s and there isNewOutput: %s lastAction: %s" % (
                    self._consume, isNewOutput, lastAction))

                perfData = self._perfData_initialize(launch_time, isNewOutput)

                did_i_execute = False

                my_process = None

                # VV: We have a unit-test that utilizes the simulator backend to run a RepeatingEngine which has no
                #     running Subject engines
                if self.consume and (isNewOutput or bool(self.job.producerInstances) is False):
                    did_i_execute = True
                    self._stateDict['isWaitingOnOutput'] = False
                    self.log.debug("Launching repeat %s %s\n" % (self.job.executable, self.job.arguments))

                    #Calculate time since last task FINISHED
                    perfData = self._perfData_before_launch(perfData, launch_time)

                    self.lastLaunched = launch_time
                    self._stateDict['lastTaskLaunchDate'] = self.lastLaunched
                    self._stateDict['numberTaskLaunches'] += 1

                    outputFile = 'out.stdout'
                    outputFileErr = 'out.stderr'

                    try:
                        my_process = self.process = self.taskGenerator(
                            self.job, outputFile=outputFile, errorFile=outputFileErr)
                    except Exception as e:
                        import traceback
                        self.log.warning('Exception when generating RepeatingEngineTask!\n%s' % traceback.format_exc())
                        #We need to record something so performance data doesn't become corrupted
                        perfData = self._perfData_launch_failed(perfData, datetime.datetime.now())
                    else:
                        # VV: Emit right after attempting to launch the process
                        self.emit_now()

                        self.log.debug("Will wait till my process is Finished")
                        my_process.wait()
                        launch_finished = datetime.datetime.now()

                        # VV: once the repeating Job is done attempt to archive its stdout and stderr streams
                        work_dir = self.job.workingDirectory.directory
                        archive_stream(
                            os.path.join(work_dir, outputFile),
                            os.path.join(work_dir, 'streams'),
                            'stdout',
                            self.log,
                            5
                        )

                        archive_stream(
                            os.path.join(work_dir, outputFileErr),
                            os.path.join(work_dir, 'streams'),
                            'stderr',
                            self.log,
                            5
                        )

                        self._stateDict['lastTaskFinishedDate'] = launch_finished
                        perfData = self._perfData_launch_succeeded(perfData, launch_finished, my_process)
                elif self.consume and not isNewOutput:
                    timeDelta = datetime.datetime.now() - self.lastLaunched
                    self.log.info("No new output detected from producers for %s" % timeDelta)
                    self._stateDict['isWaitingOnOutput'] = True
                elif self.consume is False:
                    timeDelta = datetime.datetime.now() - self.lastLaunched
                    self.log.info("No initial output detected from producers - have been waiting for %s" % timeDelta)
                    self._stateDict['isWaitingOnOutput'] = True

                # VV: Register data to self.performanceMatrix and potentially store them on disk
                self._perfData_register(perfData)

                self._stateDict['lastKernelFinishedDate'] = datetime.datetime.now()

                # We only transition to kernelCompleted == True if lastAction is == True
                # lastAction is only == True if the Monitor was cancelled
                # The Monitor is cancelled only via kill()
                # Kill can be called in two ways
                #   - Externally - another object calls kill()
                #   - Internally - we called kill() due to the following conditions being met
                #        - Producers have finished
                #        - Kernel has executed a task once successfully after this OR we've no tried left
                #

                if producers_done_when_i_started or self._suicide:
                    # VV: There is weird corner case for a repeating job X
                    # a) `producers_done_when_i_started` is true for this 'last' execution of X
                    # b) Unfortunately, X failed because one of its life-partners hadn't produced
                    #    correct output or the NFS hadn't synced when X attempted to access the files
                    # As a remedy, use up any self._stateDict['repeatRetries'] before considering that
                    # the Engine is really finished when it does not terminate successfully after all of its
                    # producers have finished.
                    if did_i_execute and my_process.returncode == 0 :
                        self.kill()
                    elif self._suicide:
                        self.log.info("Servicing my \"kill-after-producers-done-delay\"")
                        self.kernelCompleted = True
                        self.kill()
                    else:
                        #I didn't execute the kernel or it failed
                        if self._stateDict['repeatRetries'] == 0:
                            self.log.info("There are no more repeatRetries for me")
                            self.kill()
                        else:
                            self._stateDict['repeatRetries'] -= 1
                            if not did_i_execute:
                                self.log.info("No data was present to try execution,"
                                              " I will retry (left:%d)" %
                                              self._stateDict['repeatRetries'])
                            else:
                                self.log.info("My last_execution failed,"
                                              " I will retry (left:%d)" %
                                              self._stateDict['repeatRetries'])

                    # VV: Emit once `self.process` meta-information has been digested
                    self.emit_now()

        # VV: Ensure that the csv file is stored when the RepeatingEngine finishes
        perfdata_register = experiment.runtime.utilities.rx.report_exceptions(lambda: self._perfData_register(None, True),
                                                           self.log, 'PerfDataRegister')
        self.notifyFinished.subscribe(on_completed=perfdata_register, on_error=CheckState)

        def schedule_next_instance(seconds_waiting):
            next_repeat_interval = self.nextRepeatInterval()

            if 'lastTaskFinishedDate' in self._stateDict:
                if self._stateDict['lastTaskFinishedDate'] is not None:
                    seconds_from_last_finished = (
                            datetime.datetime.now() - self._stateDict['lastTaskFinishedDate']
                    ).total_seconds()

                    # VV: Override seconds_waiting with time since last finished task to avoid spamming the backend
                    seconds_waiting = min(seconds_from_last_finished, seconds_waiting)

            # VV: Enforce a minimum 5 second delay between a task finishing and the new task submission
            if seconds_waiting < 5.0:
                return False

            if self.cancelMonitorEvent is not None and self.cancelMonitorEvent.is_set():
                seconds_saved = next_repeat_interval - seconds_waiting

                self.log.log(19, "My last exec was %s ago (interval:%s), saved %s [CANCELLED]" % (
                    seconds_waiting,
                    next_repeat_interval,
                    seconds_saved
                ))
                return True
            elif seconds_waiting >= next_repeat_interval:
                self.log.log(19, "I will execute after a normal interval (%s)" % next_repeat_interval)
                return True
            elif self._producers_are_finished:
                seconds_saved = next_repeat_interval - seconds_waiting

                self.log.log(19, "My last exec was %s ago (interval:%s), saved %s [PRODUCERS]" % (
                    seconds_waiting,
                    next_repeat_interval,
                    seconds_saved
                ))
                return True

            return False

        # This monitor keeps launching the job until the engine is killed
        monitor = experiment.runtime.monitor.CreateMonitor(schedule_next_instance,
                                                           EngineTaskController,
                                                           cancelEvent=self.cancelMonitorEvent,
                                                           lastAction=True,
                                                           name="%s (EngineCore)" % self.job.reference)

        # Start the monitor in a different thread to avoid blocking main thread (in canConsume())
        # Note: errors with job-launch will not be noticed
        # Note: errors with the given life-check function will not kill the monitor but will not be noticed
        #       details about the raised exceptions will be raised to Log
        monitor()

    def notify_all_producers_finished(self):
        self._producers_are_finished = True

        if self._dieAfter is not None and self.isAlive():
            def suicide(err=None):
                self._suicide = True
                self.log.info("Will proceed to terminate because of kill-after-producers-done-delay")
                if self.process is not None:
                    # VV: RepeatingEngine must be currently running, signal it to stop
                    self.process.kill()
                else:
                    # VV: RepeatingEngine must be in-between consecutive invocations
                    #     kill it and mark it as finished
                    self.kill()
                    self.kernelCompleted = True

            cb_kill_after = experiment.runtime.utilities.rx.report_exceptions(suicide, self.log, 'KillAfterProdDone')

            kill_after = self._dieAfter
            self.log.log(19, f"I will self.kill() in {kill_after} seconds")

            reactivex.timer(kill_after).subscribe(on_completed=cb_kill_after, on_error=CheckState)

    def exitReason(self):

        '''Returns the RepeatingEngine's exit reason

        Returns None if still running.

        A repeating engine can only exit if kill() is called on it.
        It cannot discriminate between
           - being stopped because everything is ok - 'cancelled'
           - being killed

        As a result there are only two possible (non-None) exit reasons:
            1. ResourceExhausted - The last task to complete after kill() was called exited due to ResourceExhausted
            2. Success - Everything else'''

        reason = None
        if self.lastExecution is False:
            if self.cancelMonitorEvent.is_set():
                #The task has been flagged to finish
                #Check if it actually has
                #If the process ivar is set we started executing kernels (the EngineTaskController loop)
                if self.process is not None:
                    if self.kernelCompleted:
                        if self.process.exitReason == experiment.model.codes.exitReasons["ResourceExhausted"]:
                            reason = experiment.model.codes.exitReasons["ResourceExhausted"]
                        else:
                            reason = experiment.model.codes.exitReasons['Success']
                else:
                    reason = experiment.model.codes.exitReasons['Success']

        return reason

    def isAlive(self):

        '''Returns True if the Engine is alive, false otherwise.

        The only way a repeating engine can be stopped (this method returns False) is if kill() was called

        For repeating Engine these are the conditions under which the Engine is/isn't Alive

        Is alive
          - Monitor is running or to be run:
                - identified via the state of self.cancelMonitorEvent
          - Restart of final Task execution is being attempted
        Is dead
          - kill() was called '''

        # The state of a repeating engine is dependent on the Monitor state
        # If the Monitor is running, engine running, if it is stopped, engine is stopped.

        return True if self.exitReason() is None else False

    def kill(self):

        '''Stops the engine.

        This includes stopping the monitor

        Note: The monitor will not exit until the wait time is finished.
        Any current task is let finish as normal and the monitor will execute a final time

        If isAlive() is False this method has no effect'''

        #FIXME: Currently kill() acts like a cancel()
        #That is its 'soft' (waits on task exit and allows last execution)
        #Need to rename kill() to cancel() and implement a real kill()

        if self.isAlive() and not self.cancelMonitorEvent.is_set():
            self.log.warning("Received kill")

            #Stop the repeating monitoring
            #If the monitor was not started this does nothing
            self.cancelMonitorEvent.set()
        elif self.cancelMonitorEvent.is_set() is True:
            self.log.debug('Already received kill')
        else:
            self.log.debug('Received kill but isAlive is False')

    def restart(self, reason=None, code=None):

        '''Attempts to restart the Engine


        A RepeatingEngine can only be restarted if the exitReason is ResourceExhausted.
        This can occur if the final task execution failed due to ResourceExhausted.

        The restart consists of a single launch of the components task

        Params:
            reason: The exitReason to use to determine what restart action to take.
                Can be one of experiment.codes.exitReasons.
                If not supplied the value of self.exitReason is used.
                Note: For RepeatingEngines only ResourceExhausted is acted on
            code: Not used

        Returns:
            An experiment.codes.restartCode
              RestartInitiated (restarted)
              RestartCouldNotInitiate (failed to restart
              RestartNotRequired (not required to restart as the conditions are not met)
        '''

        #Note: Have to acquire exitReason before setting self.lastExecution to True
        #This is because exitReason only returns a reason if the Engine is dead and by
        #setting lastExecution to True causes the Engine to appear alive.
        if reason is None:
            reason = self.exitReason()

        retval = False

        #This function will be launched in a separate thread
        def runRestart():
            # VV: The engine is restarting, it should be marked as `alive` till this function terminates
            self.lastExecution = True

            init_time = datetime.datetime.now()
            perfData = self._perfData_initialize(init_time, isNewOutput=True)
            perfData = self._perfData_before_launch(perfData, launch_time=init_time)
            try:
                my_process = self.process = self.taskGenerator(self.job)
            except Exception as e:
                self.log.warning("Encountered error with restart - %s" % e)
                perfData = self._perfData_launch_failed(perfData, datetime.datetime.now())
            else:
                self.emit_now()
                my_process.wait()

                # VV: @tag:EvaluateStateEachTimeTaskTerminates
                # VV: Mark the end of the last finished task
                launch_finished = datetime.datetime.now()
                self._stateDict['lastTaskFinishedDate'] = launch_finished

                self.log.log(19, "Restart finished with exit reason %s" % my_process.exitReason)
                perfData = self._perfData_launch_succeeded(perfData, launch_finished, my_process)
            finally:
                self._perfData_register(perfData, force_write=True)
                # VV: The engine has either succeeded to launch a task, or there was some kind of an exception,
                #     in both cases, the restart() is complete.
                self.lastExecution = False
                self.emit_now()

        # VV: @tag:RestartEngines
        if reason == experiment.model.codes.exitReasons["ResourceExhausted"] and self.restarts == 0:
            # VV: A RepeatingEngine will only restart once and only if its last exit-reason was ResourceExhausted
            self.log.info("Attempting restart of interrupted last task execution")

            try:
                threading.Thread(target=runRestart).start()
            except Exception as error:
                self.log.warning("Encountered error with launching restart thread - %s. Stopping" % error)
                retval = experiment.model.codes.restartCodes["RestartCouldNotInitiate"]
            else:
                retval = experiment.model.codes.restartCodes["RestartInitiated"]
                self.restarts += 1
                self.log.log(19, "Restart success - waiting")
        else:
            self.log.info("Repeating engine restart conditions not met (exit-reason: %s. number-restarts: %s) "
                          "- will not restart" % (reason , self.restarts))
            retval = experiment.model.codes.restartCodes["RestartNotRequired"]

        return retval

    @property
    def stateDictionary(self):

        '''Returns a dictionary containing detailed information on current state of the receiver'''
        now = datetime.datetime.now()

        def isRunning(isAlive, runDate):
            return True if (isAlive and runDate is not None) else False

        def isFinished(isAlive, runDate):
            return True if (isAlive and runDate is not None) else False

        def generate_update(exitReason, runDate, process):
            isAlive = compute_is_alive_from_exit_reason(exitReason)
            returncode = compute_returncode_from_exit_reason(exitReason)

            return {
                'isAlive': isAlive,
                'isRunning': isRunning(isAlive, runDate),
                'isShutdown': self.isShutdown,
                'lastTaskRunState': process.status if process is not None else 'N/A',
                'lastTaskExitCode': process.returncode if process is not None else 'N/A',
                'lastTaskExitReason': process.exitReason if process is not None else 'N/A',
                'schedulerId': process.schedulerId if process is not None else 'N/A',
                'engineExitCode': returncode,
                'engineExitReason': exitReason,
                # VV: see self.compute_task_events()
                'taskDateWait': None,
                'taskDateExecute': None,
                'taskDateComplete': None,
            }

        exitReason = self.exitReason()
        process = self.process
        runDate = self._stateDict['runDate']
        isAlive = compute_is_alive_from_exit_reason(exitReason)
        update = generate_update(exitReason, runDate, process)

        self._stateDict.update(update)

        if isRunning(isAlive, runDate):
            taskRunTime = 'N/A'
            if self._stateDict['lastTaskLaunchDate'] is not None:
                # TODO VV: This looks weird, why do we generate a fake runtime here if the task has not finished?
                if self._stateDict['lastTaskFinishedDate'] is None or self._stateDict['lastTaskFinishedDate'] < \
                        self._stateDict['lastTaskLaunchDate']:
                    taskRunTime = now - self._stateDict['lastTaskLaunchDate']

                else:
                    taskRunTime = self._stateDict['lastTaskFinishedDate'] - self._stateDict['lastTaskLaunchDate']

            nextKernelLaunch = 'N/A'
            #The next launch date can only be computed if there is no KERNEL running
            #as it depends on when the last executed KERNEL exited
            if self._stateDict['lastKernelFinishedDate'] is not None:
                if self._stateDict['lastKernelLaunchDate'] <  self._stateDict['lastKernelFinishedDate']:
                    nextKernelLaunch = self._stateDict['lastKernelFinishedDate'] + datetime.timedelta(
                        seconds=self.nextRepeatInterval()) - now

            timeSinceLastTask = 'N/A'
            if None not in [self._stateDict['lastTaskFinishedDate'], self._stateDict['lastTaskFinishedDate']]:
                timeSinceLastTask = now - self._stateDict['lastTaskFinishedDate']

            waitTime = 0
            if self._stateDict['isWaitingOnOutput']:
                sinceTime = self._stateDict['lastTaskFinishedDate']
                sinceTime = sinceTime if sinceTime is not None else self._stateDict['runDate']
                waitTime = datetime.datetime.now() - sinceTime

            self._stateDict.update({
                'timeToNextKernelLaunch': nextKernelLaunch,
                'outputWaitTime': waitTime,
                'lastTaskRunTime': taskRunTime,
                'timeSinceLastTaskLaunch': timeSinceLastTask
            })
        elif isFinished(isAlive, runDate):
            # VV: Make sure to compute updates when emitting the very last update
            taskRunTime = 'N/A'
            nextKernelLaunch = 'N/A'

            if self._stateDict['lastTaskRunTime'] == 'N/A':
                self.log.debug('This is my one and only UpdateEmission (finished before emitting any lastTaskRuntime)')

            try:
                taskRunTime = self._stateDict['lastTaskFinishedDate'] - self._stateDict['lastTaskLaunchDate']
            except TypeError:
                self.log.log(19, 'I am finished and have run but there is no record of that (lastTaskRunTime)')

            self._stateDict.update({
                'timeToNextKernelLaunch': nextKernelLaunch,
                'lastTaskRunTime': taskRunTime,
            })

        ret = self._stateDict.copy()
        task_events = self.compute_task_events(process)

        ret.update(task_events)

        return ret


def CheckState(emission):
    #Enable when debugging reactive
    #Commented to exclude from checks for stray print statements
    if isinstance(emission, Exception) is False:
        moduleLogger.debug('Check State. Emission: %s. Thread %s' % (emission, threading.currentThread().name))
    else:
        moduleLogger.warning(f"CheckState Exception. Emission {type(emission)}: {emission}. "
                           f"Thread {threading.current_thread().name}. Traceback: {traceback.format_exc()}")
    return emission
