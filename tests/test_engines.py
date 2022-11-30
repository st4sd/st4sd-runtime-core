# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
import threading
import signal
import pytest

import reactivex
import reactivex.scheduler
import reactivex.operators as op

import experiment.runtime.backends
import experiment.model.codes
import experiment.model.data
import experiment.runtime.engine
import experiment.model.executors
import experiment.model.storage

from .reactive_testutils import *
from .utils import kill_engine_after_started

try:
    from typing import Union, List, Callable, cast, Tuple, Any, Dict
    from experiment.runtime.engine import Engine, RepeatingEngine
except ImportError:
    pass

@pytest.fixture(params=["Source0", "Repeating0"], ids=["Source", "Repeating"])
def componentName(request):
    return request.param

#
#Common Assertion operations
#

def AssertNotifyFinishedGather(emissions, returncode, isAlive, exitReason):

    '''Asserts that the emissions gathered from notifyFinished have expected values

    Parameters:
        emissions: A list of emissions obtained from a notifyFinished observable using a closure returned by GatherClosure
            GatherFunction = GatherClosure(emissions)
    '''

    assert len(emissions) == 1
    assert emissions[0][0]['isAlive'] == isAlive
    assert emissions[0][0]['engineExitReason'] == exitReason
    assert emissions[0][0]['engineExitCode'] == returncode

def AssertEngineState(testEngine, returncode, isAlive, exitReason):

    '''Asserts that the state of the testEngine as reported by its methods matches the values passed'''

    #State tests
    assert testEngine.returncode() == returncode
    assert testEngine.exitReason() == exitReason
    assert testEngine.isAlive() is isAlive

def AssertEngineStateConsistency(testEngine):

    '''Asserts the state reported by engines state methods is the same as the state recorded in its stateDictionary'''

    stateDict = testEngine.stateDictionary
    assert stateDict['isAlive'] == testEngine.isAlive()
    assert stateDict['engineExitCode'] == testEngine.returncode()
    assert stateDict['engineExitReason'] == testEngine.exitReason()

def TestShutdown(testEngine: Engine | RepeatingEngine, stateUpdatesCompletion):
    """Tests engines state and observable behaviour before and after shutdown

    stateUpdates observable will only STOP if shutdown is called
    """

    #Test shutdown
    assert testEngine.isShutdown is False

    #VV: Wait to see if stateUpdates observable stops (it should NOT)
    #The emission interval is 5 - wait for two emissions intervals
    WaitOnCompletion(stateUpdatesCompletion, limit=11)

    if testEngine.isShutdown is False:
        assert stateUpdatesCompletion[0] is False

    #Note: stateUpdates DOESN'T complete when notifyFinished signals completion
    #as if it did there would be no notifyFinished!

    testEngine.shutdown()
    assert testEngine.isShutdown is True

    #VV: Ask the engine to shutdown and wait for it to terminate
    WaitOnCompletion(stateUpdatesCompletion, limit=11)
    assert stateUpdatesCompletion[0] is True

    if isinstance(testEngine, experiment.runtime.engine.RepeatingEngine):
        assert testEngine.kernelCompleted


#For each engine type want to test behaviour and notifications for
#-Successful run
#-Failed run
#-Recieving kill before/during/after run
#For each of above test the engine has correct state at various points
#-Check state before run called
#-Check state after run() called
#-Check state when finished

#Also For each engine type want to test restarting
#- Check can restart and finish successfully
#- Check state before during and after restart

#
# LIFE-CYCLE STAGE STATE TESTS
#
# Test the engines report the expected state at various life-cycle stages
# initialised (before run()) - done
# running (before finished) - done
# exited (success) - combined with notify test
# exited (killed) - done
# exited (failed) - combined with notify test
#
# shutdown (from success, kill and failed) =
# restarted (after success, killed, failed) = combined with notify test
#

#Can merge below into other tests
def test_state_initialised(engine):

    '''Tests engines initial state is correct '''

    AssertEngineState(engine, None, True, None)
    AssertEngineStateConsistency(engine)


def test_tests_are_fast():
    """A test to ensure that the tests are running in "fast" mode with no artificial delays"""
    # VV: We set these in conftest.py so that every pytest-process automatically uses no artificial delays
    logger.info(f"ENGINE_LAUNCH_DELAY_SECONDS={experiment.runtime.engine.ENGINE_LAUNCH_DELAY_SECONDS}")
    logger.info(f"ENGINE_RUN_START_DELAY_SECONDS={experiment.runtime.engine.ENGINE_RUN_START_DELAY_SECONDS}")

    assert experiment.runtime.engine.ENGINE_LAUNCH_DELAY_SECONDS == 0.0
    assert experiment.runtime.engine.ENGINE_RUN_START_DELAY_SECONDS == 0.0


def test_state_running(engine):

    '''Tests engines running state is correct'''
    # Also monitor state updates to see shutdown works correctly
    engine.run()

    AssertEngineState(engine, None, True, None)
    AssertEngineStateConsistency(engine)

    engine.kill()


def test_state_exited_killed(engine: experiment.runtime.engine.Engine | experiment.runtime.engine.RepeatingEngine):

    '''Tests engines state after kill is sent is correct'''

    completed = []
    retval = []
    completedFunction = CompletedClosure(completed)
    event_finished = threading.Event()

    a = engine.notifyFinished.subscribe(on_next=lambda x: event_finished.set(),
                                        on_completed=completedFunction,
                                        on_error=completedFunction)

    engine.run()
    if isinstance(engine, experiment.runtime.engine.RepeatingEngine) is False:
        logger.warning("Will kill Engine after it starts so that its exitReason is Cancelled")
        kill_engine_after_started(engine)
    else:
        logger.warning("Killing RepeatingEngine now")
        engine.kill()

    #kill is asynchronous - need a couple of seconds
    logger.warning("Wait for engine to Finish (max 20 seconds)")
    event_finished.wait(20)

    expectedReason = experiment.model.codes.exitReasons['Success'] if isinstance(engine, experiment.runtime.engine.RepeatingEngine) else experiment.model.codes.exitReasons['Cancelled']
    expectedCode = 0 if isinstance(engine, experiment.runtime.engine.RepeatingEngine) else 1
    AssertEngineState(engine, expectedCode, False, expectedReason)
    AssertEngineStateConsistency(engine)


#
# LIFE-CYCLE STAGE TRANSITION TESTS
#
# Test the engines correctly notify observers when the various transitions occur
#
# A) initialised->running = test_notify_launch
# B) initialised->killed = test_notify_kill_before_launch
# C) running->success  = test_notify_success
# D) running->failed   = test_source_notify_failure
# E) running->killed   = test_notify_kill_running
#
# The first two running transitions are internal and the last is external
# FIXME: For the external transition (running->killed) further test it is independent of delay
# i.e. there are no edge conditions due to internal state changes
# This is because run() causes asynchronous actions to occur
#
# Test behaviour on non-allowed transitions
# F) run -> run = test_notify_run_after_run
# G) finished (any) -> killed = test_notify_kill_after_finished
#
# Restart transitions
# TODO: Add tests for non-allowed restarts
# H) KnownIssue->restart = test_restart_after_exhaustion

# Shutdown transitions
# I) ?
#

def test_engine_instantiation(engine):

    if engine.job.name == 'Source0':
        assert engine.job.isRepeat is False
        assert isinstance(engine, experiment.runtime.engine.Engine)
    elif engine.job.name == 'Repeating0':
        assert engine.job.isRepeat is True
        assert isinstance(engine, experiment.runtime.engine.RepeatingEngine)
    else:
        logger.critical('Unexpected component passed to test - %s - expecting Source0 or Repeating0' % engine.job.name)
        assert 0

def test_notify_launch(engine):

    '''Tests engine notifies correctly on run() being called on it

    TEST for (A)'''

    retval = []
    completed = []
    completedFunction = CompletedClosure(completed)

    a = engine.state.subscribe(on_next=GatherClosure(retval, log=True),
                                        on_completed=completedFunction,
                                        on_error=completedFunction)

    assert engine.stateDictionary['isAlive'] == True
    assert engine.stateDictionary['isRunning'] == False

    engine.run()

    WaitOnRun(retval)
    a.dispose()

    assert len(retval) > 0
    assert retval[-1][0]['isAlive'] is True
    assert retval[-1][0]['isRunning'] is True

    engine.kill()

    WaitOnCompletion(completed)

def test_notify_kill_before_launch(engine):

    '''Tests engine notifies correctly on receiving kill before starting

    TEST for (B)'''

    retval = []
    completed = []
    completedFunction = CompletedClosure(completed)

    a = engine.notifyFinished.subscribe(on_next=GatherClosure(retval),
                                        on_completed=completedFunction,
                                        on_error=completedFunction)

    #kill the engine before starting
    logger.warning("Killing engine before starting")
    engine.kill()

    logger.warning("Waiting for engine to complete")
    WaitOnCompletion(completed)

    logger.warning(f"Engine completion is {completed}")

    expectedReason = experiment.model.codes.exitReasons['Success'] if isinstance(engine, experiment.runtime.engine.RepeatingEngine) else experiment.model.codes.exitReasons['Killed']
    expectedCode = 0 if isinstance(engine, experiment.runtime.engine.RepeatingEngine) else 1

    assert completed[0] == True
    AssertNotifyFinishedGather(retval, expectedCode, False, expectedReason)
    AssertEngineState(engine, expectedCode, False, expectedReason)
    AssertEngineStateConsistency(engine)


def test_notify_success(engine):

    '''Tests engine correctly notifies observers of successful completion

    TEST for (C)'''

    retval = []
    stateEmissions = []
    completed = []
    completedFunction = CompletedClosure(completed)

    engine.run()
    a = engine.notifyFinished.subscribe(on_next=GatherClosure(retval),
                                        on_completed=completedFunction,
                                        on_error=completedFunction)

    #Also monitor state updates to see shutdown works correctly
    stateUpdatesCompletion = []
    stateUpdatesCompletedFunction = CompletedClosure(stateUpdatesCompletion)
    b = engine.stateUpdates.subscribe(
        on_next=GatherClosure(stateEmissions),
        on_completed=stateUpdatesCompletedFunction,
                                      on_error=stateUpdatesCompletedFunction)

    if isinstance(engine, experiment.runtime.engine.RepeatingEngine):
        WaitOnFirstTaskFinish(stateEmissions, limit=20)
        assert engine.stateDictionary["numberTaskLaunches"] != 0
        engine.kill()

    WaitOnCompletion(completed)

    assert completed[0] == True
    AssertNotifyFinishedGather(retval, 0, False, experiment.model.codes.exitReasons['Success'])
    AssertEngineState(engine, 0, False, experiment.model.codes.exitReasons['Success'])
    AssertEngineStateConsistency(engine)

    TestShutdown(engine, stateUpdatesCompletion)

def test_source_notify_failure(engine: Engine):

    '''Tests engine notifies failure in Task

    Only applies to source engines

    TEST for (D)'''

    if isinstance(engine, experiment.runtime.engine.RepeatingEngine):
        assert True
        return

    retval = []
    completed = []
    completedFunction = CompletedClosure(completed, f"notifyFinished of engine {engine.job.reference}")

    a = engine.notifyFinished.subscribe(on_next=GatherClosure(retval, log=True),
                                        on_completed=completedFunction,
                                        on_error=completedFunction)

    engine.run()

    #Also monitor state updates to see shutdown works correctly
    stateUpdatesCompletion = []
    stateUpdatesCompletedFunction = CompletedClosure(stateUpdatesCompletion)
    b = engine.stateUpdates.subscribe(on_completed=stateUpdatesCompletedFunction)

    kill_engine_after_started(engine, signal.SIGUSR1)

    WaitOnCompletion(completed, limit=10)

    expectedCode = 1
    #Signals other than Killed/Cancelled/ResourceExausted should be reported as UnknownIssue
    expectedReason = experiment.model.codes.exitReasons['UnknownIssue']

    # We know we sent SIGUSR1 so we can just confirm this is what was reported by the internal process
    # If its not the other tests will not work
    assert engine.process.returncode == -1*signal.SIGUSR1
    assert completed[0] == True
    AssertNotifyFinishedGather(retval, expectedCode, False, expectedReason)
    AssertEngineState(engine, expectedCode, False, expectedReason)
    AssertEngineStateConsistency(engine)

    #Test Shutdown and statesUpdates behaviour
    TestShutdown(engine, stateUpdatesCompletion)


def test_notify_kill_running(engine):

    '''Tests source engine notifies correctly on receiving kill when running

    TEST of (E)'''

    have_killed = [False]

    def try_kill(tuple_state_engine, which=engine, have_killed=have_killed):
        # type: (Tuple[Dict[str,str], experiment.runtime.engine.Engine], experiment.runtime.engine.Engine, List[bool]) -> None
        state, engine = tuple_state_engine
        logger.info("Have already killed? %s and current emission %s" % (have_killed, state))
        try:
            if state.get('taskDateExecute') is not None:
                have_killed[0] = True
                logger.warning("Will kill %s in 1 second" % (engine.job.reference))
                time.sleep(1.0)
                which.kill()
        except Exception:
            import traceback
            logger.critical("Error: %s" % traceback.format_exc())

    expectedReason = (
        experiment.model.codes.exitReasons['Success'] if isinstance(engine, experiment.runtime.engine.RepeatingEngine)
        # VV: Killing an engine BEFORE it starts executing will return an exitReason 'Killed'
        #     killing it WHILE it's running will set the engineExitReason to `Cancelled`
        else experiment.model.codes.exitReasons['Cancelled']
    )
    expectedCode = 0 if isinstance(engine, experiment.runtime.engine.RepeatingEngine) else 1

    retval = []
    completed = []
    completedFunction = CompletedClosure(completed)

    engine.run()
    a = engine.notifyFinished.subscribe(on_next=GatherClosure(retval),
                                        on_completed=completedFunction,
                                        on_error=completedFunction)

    # VV: kill the engine 1 second after it starts executing
    engine.stateUpdates.pipe(
        op.observe_on(reactivex.scheduler.NewThreadScheduler()),
        op.take_while(lambda x: have_killed[0] is False),
    ).subscribe(on_next=try_kill)

    logger.warning("Waiting for completion of Engine")
    WaitOnCompletion(completed)

    logger.warning(f"Engine completed with {completed}")

    assert completed[0] is True

    AssertNotifyFinishedGather(retval, expectedCode, False, expectedReason)
    AssertEngineState(engine, expectedCode, False, expectedReason)
    AssertEngineStateConsistency(engine)

def test_notify_run_after_run():

    '''Tests source engine behaves correctly on run being called after run

    TEST for (F)'''

    #FIXME: At the moment this is not allowed by the API and there is no handler for it occurring

    pass

def test_notify_kill_after_finished(engine):

    '''Tests source engine behaves correctly on receiving kill after finishing

    TEST for (G)'''

    retval = []
    completed = []
    stateEmissions = []
    completedFunction = CompletedClosure(completed)

    engine.run()
    a = engine.notifyFinished.subscribe(on_next=GatherClosure(retval),
                                        on_completed=completedFunction,
                                        on_error=completedFunction)

    #Also monitor state updates to see repeating engines are working correctly
    stateUpdatesCompletion = []
    stateUpdatesCompletedFunction = CompletedClosure(stateUpdatesCompletion)
    b = engine.stateUpdates.subscribe(
                on_next=GatherClosure(stateEmissions),
                on_completed=stateUpdatesCompletedFunction,
                                      on_error=stateUpdatesCompletedFunction)

    if isinstance(engine, experiment.runtime.engine.RepeatingEngine):
        WaitOnFirstTaskFinish(stateEmissions, limit=20)
        assert engine.stateDictionary["numberTaskLaunches"] != 0
        engine.kill()

    WaitOnCompletion(completed)
    assert completed[0] == True

    #Now send kill()
    engine.kill()

    #Since its already finished NOTHING should change
    expectedCode = 0
    expectedReason = experiment.model.codes.exitReasons['Success']

    a.dispose()
    AssertNotifyFinishedGather(retval, expectedCode, False, expectedReason)
    AssertEngineState(engine, expectedCode, False, expectedReason)
    AssertEngineStateConsistency(engine)

def test_cold_restart(engine):
    # type: (experiment.runtime.engine.Engine) -> None
    retval = []
    stateEmissions = []
    completed = []
    completedFunction = CompletedClosure(completed)

    # VV: Use the simulator back-end to speed up the test
    if not isinstance(engine, experiment.runtime.engine.RepeatingEngine):
        engine.job.options_modify("#resourceManager.config.backend", "simulator")
        engine.job.options_modify("sim_range_execution_time", "1.0")
        engine.taskGenerator = experiment.runtime.backends.backendGeneratorMap[engine.job.type]

    a = engine.notifyFinished.subscribe(on_next=GatherClosure(retval),
                                        on_completed=completedFunction,
                                        on_error=completedFunction)

    if not isinstance(engine, experiment.runtime.engine.RepeatingEngine):
        # VV: before restarting, mock the job output files
        # It looks like it's not necessary to have the correct filename for this particular unit-test
        output_filepath = os.path.join(engine.job.directory, "out.stdout")
        open(output_filepath, 'w').close()

        did_restart = engine.restart(reason=experiment.model.codes.exitReasons['ResourceExhausted'],
                                     code=0)
    else:
        did_restart = experiment.model.codes.restartCodes["RestartInitiated"]
        rep_engine = engine  # type: experiment.runtime.engine.RepeatingEngine
        rep_engine.notify_all_producers_finished()
        rep_engine.run()

    assert did_restart == experiment.model.codes.restartCodes["RestartInitiated"]

    WaitOnCompletion(completed)
    a.dispose()
    expected_code = 0
    expected_reason = experiment.model.codes.exitReasons['Success']

    assert completed[0] == True
    AssertNotifyFinishedGather(retval, expected_code, False, expected_reason)
    AssertEngineState(engine, expected_code, False, expected_reason)
    AssertEngineStateConsistency(engine)


def test_restart_after_success(engine):
    # type: (Union[experiment.runtime.engine.Engine, experiment.runtime.engine.RepeatingEngine]) -> None
    """Tests engine behaves correctly when restarted after a success
    
    Complex test
    - Test state on Success exit is correct
    - Test restarting from Success results in correct notifications and new state
    - Test when restart finishes the notifications and final state are correct
    """
    # VV: The engine is invoking sleep, have it sleep for 5 seconds
    orig_arguments = engine.job.componentSpecification.commandDetails['arguments']
    engine.job.options_modify('#command.arguments', "5")

    retval = []
    stateEmissions = []
    completed = []
    completedFunction = CompletedClosure(completed)

    a = engine.notifyFinished.subscribe(on_next=GatherClosure(retval),
                                        on_completed=completedFunction,
                                        on_error=completedFunction)

    # Also monitor state updates to see shutdown works correctly
    stateUpdatesCompletion = []
    stateUpdatesCompletedFunction = CompletedClosure(stateUpdatesCompletion)
    b = engine.stateUpdates.subscribe(
        on_next=GatherClosure(stateEmissions),
        on_completed=stateUpdatesCompletedFunction)
    
    if engine.job.isRepeat:
        # VV: Triggers the RepeatingEngine to stop running tasks after its next task executes
        engine.notify_all_producers_finished()

    engine.run()

    WaitOnCompletion(completed)
    a.dispose()

    AssertEngineStateConsistency(engine)

    WaitOnCompletion(stateUpdatesCompletion, limit=11)
    assert stateUpdatesCompletion[0] is False

    # VV: Restart
    a.dispose()
    retval = []
    completed = []
    completedFunction = CompletedClosure(completed)
    a = engine.notifyFinished.subscribe(on_next=GatherClosure(retval),
                                        on_completed=completedFunction,
                                        on_error=completedFunction)

    restartCode = engine.restart(reason=experiment.model.codes.exitReasons['ResourceExhausted'])
    assert restartCode == experiment.model.codes.restartCodes["RestartInitiated"]
    AssertEngineState(engine, None, True, None)
    AssertEngineStateConsistency(engine)

    WaitOnCompletion(completed)

    expectedCode = 0
    expectedReason = experiment.model.codes.exitReasons['Success']

    assert completed[0] is True
    AssertNotifyFinishedGather(retval, expectedCode, False, expectedReason)
    AssertEngineState(engine, expectedCode, False, expectedReason)
    AssertEngineStateConsistency(engine)

    # Test Shutdown and statesUpdates behaviour
    TestShutdown(engine, stateUpdatesCompletion)
    b.dispose()
    a.dispose()


def test_restart_after_exhaustion(engine):
    # type: (Union[experiment.runtime.engine.Engine, experiment.runtime.engine.RepeatingEngine]) -> None
    """Tests engine behaves correctly when restarted

    Complex test
    - Test interruption with ResourceExhaustion is identified correctly
    - Test state on ResourceExhaustion exit is correct
    - Test restarting from ResourceExhaustion results in correct notifications and new state
    - Test when restart finishes the notifications and final state are correct

    TEST for (H)"""
    # VV: This is how to setup a simulated job that will fail after 1 to 3 seconds of
    #     execution with an exit code of 24 (which translates to the ResourceExhausted
    #     exit reason. All subsequent runs will exit with 0 (Success exit reason)
    engine.job.options_modify('#resourceManager.config.backend', 'simulator')
    engine.job.options_modify("sim_range_execution_time", "1.0:3.0")
    # VV: The simulator will make sure that the Component's producers have produced
    #     outputs, so we need to explicitly clear all DataReferences
    # MJ: Note this doesn't delete the links in the graph
    # It will just appear that this component does not consume data from specific files or directories
    # dataReferences will be empty, resolveArguments won't do anything ...
    # Methods using the graph e.g. Job.producerInstances will still return the original producers
    engine.job.options_modify("#references", [])
    # VV: Exit code 24 -> ResourceExhausted
    engine.job.options_modify("sim_expected_exit_code", "24")

    engine.taskGenerator = experiment.runtime.backends.backendGeneratorMap[engine.job.type]
    retval = []
    stateEmissions = []
    completed = []
    completedFunction = CompletedClosure(completed)

    a = engine.notifyFinished.subscribe(on_next=GatherClosure(retval),
                                        on_completed=completedFunction,
                                        on_error=completedFunction)

    engine.run()

    #Also monitor state updates to see shutdown works correctly
    stateUpdatesCompletion = []
    stateUpdatesCompletedFunction = CompletedClosure(stateUpdatesCompletion)
    b = engine.stateUpdates.subscribe(
        on_next=GatherClosure(stateEmissions),
        on_completed=stateUpdatesCompletedFunction)

    if isinstance(engine, experiment.runtime.engine.RepeatingEngine):
        # VV: The very first non-None lastTaskExitCode emission
        #     of the repeating engine should be 24 (ResourceExhausted)
        #     but a repeating job will ONLY terminate if it's kill()ed
        #     either because all of its producers have finished producing outputs
        #     and the engine has executed once after their completion
        #     or due to some external call to the engine's kill().
        #     As such, a RepeatingEngine will not complete if it runs into
        #     *any* error.
        def until_exit_code(exit_code):
            def check_exit_code(x):
                if ('lastTaskExitCode' not in x[0]) or (x[0]['lastTaskExitCode'] is None):
                    return True
                elif x[0]['lastTaskExitCode'] == exit_code:
                    return False
                else:
                    return True

            return check_exit_code

        rep_engine = cast(RepeatingEngine, engine)

        got_failure = []
        got_failure_returncode = CompletedClosure(got_failure)
        c = rep_engine.stateUpdates.pipe(
            op.take_while(until_exit_code(24))
        ).subscribe(on_completed=got_failure_returncode,
                             on_error=lambda e: logger.warn('Did not get proper lastTaskExitCode') )
        WaitOnCompletion(got_failure)
        c.dispose()

        assert got_failure[0] is True
    else:
        # VV: Non-repeating jobs will finish
        WaitOnCompletion(completed)
        a.dispose()

    #
    # Resource Exhausted Checks
    #

    expectedCode = 1
    expectedReason = experiment.model.codes.exitReasons['ResourceExhausted']

    if not isinstance(engine, experiment.runtime.engine.RepeatingEngine):
        assert completed[0] is True
        AssertNotifyFinishedGather(retval, expectedCode, False, expectedReason)
        AssertEngineState(engine, expectedCode, False, expectedReason)

    AssertEngineStateConsistency(engine)

    #Check stateUpdates is still running
    #Wait to see nothing happens for two emission intervals
    WaitOnCompletion(stateUpdatesCompletion, limit=11)
    assert stateUpdatesCompletion[0] is False

    #
    # Restart
    #

    #The repeating engine will have fired again and will not have exited with ResourceExhausted
    #Manually set the reason to test its restart behaviour
    reason = experiment.model.codes.exitReasons['ResourceExhausted'] if isinstance(engine, experiment.runtime.engine.RepeatingEngine) else None
    restartCode = engine.restart(reason=reason)

    if not isinstance(engine, experiment.runtime.engine.RepeatingEngine):
        #Resubscribe
        retval.pop()
        completed.pop()
        completedFunction = CompletedClosure(completed)
        a = engine.notifyFinished.subscribe(on_next=GatherClosure(retval),
                                            on_completed=completedFunction,
                                            on_error=completedFunction)

    #
    # Restart running checks
    #

    assert restartCode == experiment.model.codes.restartCodes["RestartInitiated"]
    AssertEngineState(engine, None, True, None)
    AssertEngineStateConsistency(engine)

    #Check stateUpdates is still running
    assert stateUpdatesCompletion[0] is False

    if isinstance(engine, experiment.runtime.engine.RepeatingEngine):
        # Repeating engines don't exit
        # Wait until its run once then kill
        time.sleep(6.0)
        engine.kill()

    WaitOnCompletion(completed)

    #
    # Restart finished checks
    #

    expectedCode = 0
    expectedReason = experiment.model.codes.exitReasons['Success']

    assert completed[0] == True
    AssertNotifyFinishedGather(retval, expectedCode, False, expectedReason)
    AssertEngineState(engine, expectedCode, False, expectedReason)
    AssertEngineStateConsistency(engine)

    #Test Shutdown and statesUpdates behaviour
    TestShutdown(engine, stateUpdatesCompletion)

    engine.job.options_modify('#resourceManager.config.backend', "local")

#
# LAST ACTION TEST
#


def test_repeating_engine_performs_last_action_on_kill():

    '''Tests repeating engine notifies of success when receiving kill'''

    #TODO: Add this test
    #Passing now as its not part of the initially required tests which
    #cover state transitions and notifications
    pass


def test_repeating_engine_kill_consumer_after_producers_done(engine):
    # type: (experiment.runtime.engine.Engine) -> None
    """Tests the 'kill-after-producers-done-delay' functionality.

    This option may only be used by repeating-engines. The controller will instruct the engine
    to kill() itself after some user-specified delay (in seconds) once it has detected
    that the repeating-engine's producers have all finished."""

    # VV: This check only makes sense for RepeatingEngine

    if not isinstance(engine, experiment.runtime.engine.RepeatingEngine):
        return

    job = engine.job  # type: experiment.model.data.Job
    job.options_modify('#resourceManager.config.backend', "simulator")
    # VV: Simulate a job that normally takes 10 seconds to run but receives an instruction
    #     to run kill() after 2 seconds once its producers have finished
    job.options_modify("kill-after-producers-done-delay", "1.0")
    job.options_modify("sim_range_execution_time", "1.0")

    engine = experiment.runtime.engine.Engine.engineForComponentSpecification(job)  # type: experiment.runtime.engine.RepeatingEngine

    # VV: Fake the completion of the RepeatingEngine producers

    producers = job.producerInstances

    for prod in producers:
        pre, executor, post = experiment.model.executors.CommandsFromSpecification(prod)
        with open(os.path.join(executor.workingDir, 'started.txt'), "w"):
            pass

        with open(os.path.join(executor.workingDir, 'finished.txt'), "w"):
            pass

    retval = []
    completed = []
    completedFunction = CompletedClosure(completed)

    a = engine.notifyFinished.subscribe(on_next=GatherClosure(retval),
                                        on_completed=completedFunction,
                                        on_error=completedFunction)

    engine.run()

    engine.notify_all_producers_finished()

    WaitOnCompletion(completed, limit=5)
    a.dispose()

    expected_code = 0
    expected_reason = experiment.model.codes.exitReasons["Success"]
    assert completed[0] is True
    AssertNotifyFinishedGather(retval, expected_code, False, expected_reason)
    AssertEngineState(engine, expected_code, False, expected_reason)
    AssertEngineStateConsistency(engine)
