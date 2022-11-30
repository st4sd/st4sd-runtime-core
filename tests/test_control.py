# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import print_function

import tempfile
import shutil
import threading
from typing import TYPE_CHECKING, Dict, List, Tuple, Set, Optional

import networkx
import pytest
import reactivex
import reactivex.scheduler
import reactivex.operators as op

import experiment.model.codes
import experiment.model.data
import experiment.model.frontends.flowir
import experiment.runtime.errors
import experiment.runtime.status
import experiment.runtime.engine
import experiment.runtime.control
import experiment.runtime.workflow

from .utils import experiment_from_flowir, kill_engine_after_started, new_controller, generate_controller_for_flowir
from .reactive_testutils import *

if TYPE_CHECKING:
    import experiment.model.graph
    from experiment.runtime.workflow import ComponentState




@pytest.fixture
def statusDatabase(experimentInstance):

    db = experiment.runtime.status.StatusDB(location=experimentInstance.instanceDirectory.statusDatabaseLocation)
    yield db
    db.close()
    db.event_finished.wait(60)


def KillController(controller, flag):

    logger.critical("Killing Controller")
    controller.killController("KillController()")
    flag[0] = True

@pytest.fixture
def experiment_three_stages():
    flowir = """
    application-dependencies: {}
    blueprint:
      default: {}
    components:
    - command:
        arguments: /tmp
        executable: ls
      name: 0_Source_a
      resourceManager:
        config:
          backend: simulator
      stage: 0
      variables:
        name: stage0
        sim_range_execution_time: '2'
        sim_range_schedule_overhead: '0'
    - command:
        arguments: /tmp
        executable: ls
      name: 1_Source_a
      resourceManager:
        config:
          backend: simulator
      stage: 0
      variables:
        name: stage0
        sim_range_execution_time: '10'
        sim_range_schedule_overhead: '0'
    - command:
        arguments: /tmp stage0.0_Source_a:ref
        executable: ls
      name: 0_Consumer_b_b
      references:
      - stage0.0_Source_a:ref
      resourceManager:
        config:
          backend: simulator
      stage: 1
      variables:
        name: stage1
        sim_range_execution_time: '10'
        sim_range_schedule_overhead: '0'
    - command:
        arguments: /tmp stage1.0_Consumer_b_b:ref
        executable: ls
      name: 0_ConsumerRepeat_b_c
      references:
      - stage1.0_Consumer_b_b:ref
      resourceManager:
        config:
          backend: simulator
      stage: 1
      variables:
        name: stage1
        sim_range_execution_time: '3'
        sim_range_schedule_overhead: '0'
      workflowAttributes:
        isRepeat: true
        repeatInterval: 2.0
    - command:
        arguments: /tmp stage0.0_Source_a:ref
        executable: ls
      name: 0_Consumer_a_b
      references:
      - stage0.0_Source_a:ref
      resourceManager:
        config:
          backend: simulator
      stage: 2
      variables:
        name: stage2
        sim_range_execution_time: '5'
        sim_range_schedule_overhead: '0'
    - command:
        arguments: /tmp stage2.0_Consumer_a_b:ref
        executable: ls
      name: 0_ConsumerRepeat_a_c
      references:
      - stage2.0_Consumer_a_b:ref
      resourceManager:
        config:
          backend: simulator
      stage: 2
      variables:
        name: stage2
        sim_range_execution_time: '3'
        sim_range_schedule_overhead: '0'
      workflowAttributes:
        isRepeat: true
        repeatInterval: 1.0
    environments:
      default: {}
    """

    path_package = tempfile.mkdtemp()

    exp_instance = experiment_from_flowir(flowir, path_package)

    yield exp_instance

    try:
        shutil.rmtree(exp_instance.instanceDirectory.location)
        shutil.rmtree(path_package)
    except:
        pass


def test_sanitycheck_success_to_restarted_once(output_dir):
    flowir ="""
    components:
    - name: target
      workflowAttributes:
        restartHookOn:
        - Success
      command:
        executable: echo
        arguments: world
    """

    controller = generate_controller_for_flowir(flowir, output_dir, extra_files={
        'hooks/__init__.py': '',
        'hooks/restart.py': """
import experiment.model.codes

def Restart(workingDirectory, restarts, componentName, log, exitReason, exitCode):
    log.info("test_sanitycheck_success_to_restarted_once workingDirectory=%s, restarts=%s, componentName=%s, "
              "exitReason=%s, exitCode=%s" % (workingDirectory, restarts, componentName, exitReason, exitCode))
    if restarts < 2:
        return experiment.model.codes.restartContexts["RestartContextRestartPossible"]
    else:
        return experiment.model.codes.restartContexts["RestartContextRestartNotRequired"]"""})

    controller.run()

    comp = controller.get_compstate('stage0.target')
    assert comp.engine.restarts == 2


def test_sanitycheck_failure_restart_failure(output_dir):
    flowir = """
    components:
    - name: failure
      workflowAttributes:
        restartHookOn:
        - KnownIssue
      command:
        executable: sh
        arguments: "-c 'exit 2'"
    """

    controller = generate_controller_for_flowir(flowir, output_dir, extra_files={
        'hooks/__init__.py': '',
        'hooks/restart.py': """
import experiment.model.codes

def Restart(workingDirectory, restarts, componentName, log, exitReason, exitCode):
    log.info("test_sanitycheck_failure_restart_failure workingDirectory=%s, restarts=%s, componentName=%s, "
              "exitReason=%s, exitCode=%s" % (workingDirectory, restarts, componentName, exitReason, exitCode))
    return experiment.model.codes.restartContexts["RestartContextRestartNotPossible"]"""
    })

    with pytest.raises(experiment.runtime.errors.UnexpectedJobFailureError):
        controller.run()

    comp = controller.get_compstate('stage0.failure')
    assert comp.engine.restarts == 1


def test_sanitycheck_failure_no_restart_attempts(output_dir):
    flowir = """
    components:
    - name: failure
      workflowAttributes:
        restartHookOn: [] # disables restarts
      command:
        executable: sh
        arguments: "-c 'exit 2'"
    """

    controller = generate_controller_for_flowir(flowir, output_dir, extra_files={
        'hooks/__init__.py': '',
        'hooks/restart.py': """
import experiment.model.codes

def Restart(workingDirectory, restarts, componentName, log, exitReason, exitCode):
    log.info("test_sanitycheck_failure_no_restart_attempts for workingDirectory=%s, restarts=%s, componentName=%s, "
              "exitReason=%s, exitCode=%s" % (workingDirectory, restarts, componentName, exitReason, exitCode))
    return experiment.model.codes.restartContexts["RestartContextRestartNotRequired"]"""
    })

    with pytest.raises(experiment.runtime.errors.UnexpectedJobFailureError):
        controller.run()

    comp = controller.get_compstate('stage0.failure')
    assert comp.engine.restarts == 0


def test_sanitycheck_success_restart_with_restartHook(output_dir):
    flowir = """
    blueprint:
      default:
        global:
          command:
            executable: echo
            arguments: "`pwd`"
    components:
    - name: restart_this
    - name: run_this
      stage: 1
    """
    # VV: Simulate restarting from stage 1
    controller = generate_controller_for_flowir(flowir, output_dir, initial_stage=0, do_restart_sources={0: True},
                                                extra_files={
        'hooks/__init__.py': '',
        'hooks/restart.py': """
import experiment.model.codes

def Restart(workingDirectory, restarts, componentName, log, exitReason, exitCode):
    log.info("test_sanitycheck_success_restart_with_restartHook for workingDirectory=%s, restarts=%s, componentName=%s, "
              "exitReason=%s, exitCode=%s" % (workingDirectory, restarts, componentName, exitReason, exitCode))
    return experiment.model.codes.restartContexts["RestartContextRestartPossible"]"""})

    # VV: There must be exactly 0 finished components
    assert len(controller.comp_done) == 0
    restart_this = controller.get_compstate('stage0.restart_this')
    run_this = controller.get_compstate('stage1.run_this')

    assert restart_this.state == experiment.model.codes.RUNNING_STATE
    assert run_this.state == experiment.model.codes.RUNNING_STATE

    # VV: Setup a failsafe watchdog, run the stage, and make sure that the failsafe did not kick in
    flag = [False]
    failSafe = reactivex.timer(80).subscribe(on_completed=lambda: KillController(controller, flag))

    # VV: Run stage 0
    controller.run()

    assert restart_this.state == experiment.model.codes.FINISHED_STATE
    assert restart_this.engine.restarts == 1

    # VV: Run stage 1
    controller.experiment.incrementStage()
    controller.initialise(controller.experiment.getStage(1), controller.statusDatabase)
    controller.run()
    assert flag[0] is False
    failSafe.dispose()

    assert run_this.state == experiment.model.codes.FINISHED_STATE
    controller.experiment.incrementStage()
    controller.cleanUp()


def test_sanitycheck_restart_restartHook_weirdRestartContext(output_dir):
    flowir = """
    components:
    - name: restart
      command:
        executable: echo
        arguments: "`pwd`"
    """
    controller = generate_controller_for_flowir(flowir, output_dir, initial_stage=0, do_restart_sources={0: True},
                                                extra_files={
                                                    'hooks/__init__.py': '',
                                                    'hooks/restart.py': """
import experiment.model.codes

def Restart(workingDirectory, restarts, componentName, log, exitReason, exitCode):
    log.info("test_sanitycheck_restart_restartHook_weirdRestartContext for workingDirectory=%s, restarts=%s, " 
              "componentName=%s, exitReason=%s, exitCode=%s" % (
              workingDirectory, restarts, componentName, exitReason, exitCode))
    return {"I'd like to see you deal with this"}"""})

    # VV: There must be exactly 0 finished components
    assert len(controller.comp_done) == 0
    restart = controller.get_compstate('stage0.restart')

    assert restart.state == experiment.model.codes.RUNNING_STATE
    controller.run()

    # VV: Expected behaviour:
    # 1. Controller attempts to restart component
    # 2. Engine invokes restartHook
    # 3. restartHook reports a bogus restartContext
    # 4. Engine understands that the restartContext is invalid and pretends the restartHook does not exist
    # 5. Engine attempts to restart; it succeeds
    # 6. ComponentState terminates successfully

    assert restart.state == experiment.model.codes.FINISHED_STATE
    assert restart.engine.restarts == 1
    assert restart.engine.lastLaunched is not None
    assert restart.engine.exitReason() == experiment.model.codes.exitReasons['Success']

    controller.cleanUp()

def test_sanitycheck_restart_restartHook_notRequired(output_dir):
    flowir = """
    components:
    - name: do_not_restart
      command:
        executable: echo
        arguments: "hello world"
    """
    controller = generate_controller_for_flowir(flowir, output_dir, initial_stage=0, do_restart_sources={0: True},
                                                extra_files={
        'hooks/__init__.py': '',
        'hooks/restart.py': """
import experiment.model.codes

def Restart(workingDirectory, restarts, componentName, log, exitReason, exitCode):
    log.info("test_sanitycheck_restart_restartHook_weirdRestartContext for workingDirectory=%s, restarts=%s, " 
              "componentName=%s, exitReason=%s, exitCode=%s" % (
              workingDirectory, restarts, componentName, exitReason, exitCode))
    return experiment.model.codes.restartContexts["RestartContextRestartNotRequired"]"""})

    # VV: There must be exactly 0 finished components
    assert len(controller.comp_done) == 0
    do_not_restart = controller.get_compstate('stage0.do_not_restart')

    assert do_not_restart.state == experiment.model.codes.RUNNING_STATE
    controller.run()

    # VV: Expected behaviour:
    # 1. Controller attempts to restart component
    # 2. Engine invokes restartHook,
    # 3. restartHook report that the Task does not need to be restarted
    # 4. Controller marks ComponentState as finished without executing it at all
    # 5. The ComponentState kills the underlying Engine

    assert do_not_restart.state == experiment.model.codes.FINISHED_STATE
    assert do_not_restart.engine.restarts == 1
    assert do_not_restart.engine.lastLaunched is None
    assert do_not_restart.engine.exitReason() == experiment.model.codes.exitReasons['Killed']

    controller.cleanUp()


@pytest.mark.timeout(180, method='thread')
def test_controller_promote(experiment_three_stages, statusDatabase):
    '''Tests the job promotion feature.


    In this synthetic package, there are 3 "chains" of dependencies:

    stage0.0_Source_a --> stage2.0_Consumer_a_a -> stage2.0_ConsumerRepeat_a_c
               |-> stage1.0_Consumer_b_b -> stage1.0_ConsumerRepeat_b_c

    additionally the package specifies that WorkTime(0_Source_a) < WorkTime(1_Source_a)
    so there's an implicit chain assuming that 0_Source_a and 1_Source_a are scheduled at
    the same time

    stage0.0_Source_a -> stage0.1_Source_a

    The test should be complete in about 50-60 seconds tearing down the spawned
    threads (done in @statusDatabase) may take up to another might.
    I've set the maximum timeout to 3 minutes (180 seconds)
    '''

    controller, all_comps = new_controller(experiment_three_stages)

    components = []  # type: List[experiment.runtime.workflow.ComponentState]

    for idx in all_comps:
        components += all_comps[idx]
    order_finished = []

    def report_finished(c):
        order_finished.append(c[0]['reference'])

    reactivex.merge(*[c.notifyFinished for c in components]) \
        .subscribe(on_next=report_finished)

    for stage in experiment_three_stages._stages:
        controller.initialise(stage, statusDatabase)
        controller.run()

    chains = [
        ['stage0.0_Source_a', 'stage2.0_Consumer_a_b', 'stage2.0_ConsumerRepeat_a_c'],
        ['stage0.0_Source_a', 'stage1.0_Consumer_b_b', 'stage1.0_ConsumerRepeat_b_c'],
        ['stage0.0_Source_a', 'stage0.1_Source_a'],
    ]

    # VV: Double check that all components finished
    for comp in components:
        assert comp.specification.reference in order_finished

    for chain in chains:
        observed_order = list(map(order_finished.index, chain))

        for v in observed_order:
            assert v is not None

        for i, v in enumerate(observed_order[1:]):
            # VV: Make sure that observed_order[i] < observed_order[i+1]
            assert observed_order[i] < v


def test_controller_normal_completion(experimentInstance, statusDatabase):

    '''Tests the controller can start a stage and identify when it finishes normally'''

    # controller = experiment.control.Controller(experimentInstance.graph)
    controller, all_comps = new_controller(experimentInstance)

    stage = experimentInstance._stages[0]
    flag = [False]

    controller.initialise(stage, statusDatabase)
    failSafe = reactivex.timer(80).subscribe(on_completed=lambda: KillController(controller, flag))

    #Need to handle blocking ...
    #Set up an external condition to signal the controller to exit
    controller.run()

    #If we got here it worked - as long as it wasn't because of a timeout!
    assert flag[0] is False

    for component in controller.components:
        assert component.state == experiment.model.codes.FINISHED_STATE

    failSafe.dispose()

    del all_comps

def test_controller_component_finish_failed(experimentInstance, statusDatabase):

    '''Tests the controller correctly handles a component finish with a FAILED_STATE'''

    # controller = experiment.control.Controller(experimentInstance.graph)
    concrete = experimentInstance.configuration.get_flowir_concrete(False)

    # VV: Increase duration of Source1 by 30 seconds to increase the chances of Source1
    #     being Alive when the Controller discovers that Source0 has been killed
    comp_source1 = concrete.get_component((0, 'Source1'))
    args_source1 = int(comp_source1['command']['arguments'])
    concrete.set_component_option((0, 'Source1'), '#command.arguments', str(args_source1+30))

    controller, all_comps = new_controller(experimentInstance)
    print(controller.graph.nodes['stage0.Source0'])
    print(experiment.model.frontends.flowir.yaml_dump(controller.graph.nodes['stage0.Source0']['getConfiguration'](False)))

    print(controller.graph.nodes['stage0.Source1'])
    print(experiment.model.frontends.flowir.yaml_dump(controller.graph.nodes['stage0.Source1']['getConfiguration'](False)))

    stage = experimentInstance._stages[0]

    controller.initialise(stage, statusDatabase)
    source = controller.graph.nodes['stage0.Source0']['component']()
    have_killed = [False]

    def try_kill(combo, source=source, have_killed=have_killed):
        # type: (Tuple[Dict[str,str], experiment.runtime.engine.Engine], experiment.runtime.workflow.ComponentState, List[bool]) -> None
        state, engine = combo
        logger.info("Have already killed? %s and current emission %s" % (have_killed, state))
        try:
            if state.get('taskDateExecute') is not None:
                have_killed[0] = True
                logger.warning("Will kill %s in 1 second" % (engine.job.reference))
                time.sleep(1.0)
                source.finish(experiment.model.codes.FAILED_STATE)
        except Exception:
            import traceback
            logger.critical("Error: %s"% traceback.format_exc())

    source.engine.stateUpdates \
        .pipe(
            op.observe_on(reactivex.scheduler.NewThreadScheduler()),
            op.take_while(lambda x: have_killed[0] is False),
        ).subscribe(on_next=try_kill)

    #Backup in-case everything hangs
    flag = [False]
    failSafe = reactivex.timer(80).subscribe(on_completed=lambda: KillController(controller, flag))

    with pytest.raises(experiment.runtime.errors.UnexpectedJobFailureError):
        controller.run()

    assert flag[0] is False

    #Since we stopped the component with FAILED, the controller should shutdowning all
    #all other components in notifyFinishedCheck
    for component in controller.components:
        if component != source:
            if component.state != experiment.model.codes.SHUTDOWN_STATE:
                raise ValueError('%s terminated with state %s but was expected to be shutdown' % (
                    component.engine.job.reference, component.state
                ))

    failSafe.dispose()

    del all_comps

def test_controller_component_finish_shutdown(experimentInstance, statusDatabase):

    '''Tests the controller correctly handles a component finish with SHUTDOWN state'''

    # controller = experiment.control.Controller(experimentInstance.graph)
    controller, all_comps = new_controller(experimentInstance)

    print(controller.graph.nodes['stage0.Source0'])

    stage = experimentInstance._stages[0]

    controller.initialise(stage, statusDatabase)
    source = controller.graph.nodes['stage0.Source0']['component']()
    #In 10 seconds send finish to the Source0 component.
    #This should tigger a cascade and end the stage with an exception
    have_killed = [False]

    def try_kill(combo, source=source, have_killed=have_killed):
        # type: (Tuple[Dict[str,str], experiment.runtime.engine.Engine], experiment.runtime.workflow.ComponentState, List[bool]) -> None
        state, engine = combo
        logger.info("Have already killed? %s and current emission %s" % (have_killed, state))
        try:
            if state.get('taskDateExecute') is not None:
                have_killed[0] = True
                logger.warning("Will kill %s in 5 seconds (need to wait for repeating engines to spawn)" %
                               (engine.job.reference))
                time.sleep(5.0)
                source.finish(experiment.model.codes.SHUTDOWN_STATE)
        except Exception:
            import traceback
            logger.critical("Error: %s" % traceback.format_exc())

    source.engine.stateUpdates.pipe(
        op.observe_on(reactivex.scheduler.NewThreadScheduler()),
        op.take_while(lambda x: have_killed[0] is False)
    ).subscribe(on_next=try_kill)

    #Backup in-case everything hangs
    flag = [False]
    failSafe = reactivex.timer(80).subscribe(on_completed=lambda: KillController(controller, flag))

    #Unlike finishing with FAILURE this won't cause an exception
    controller.run()

    assert flag[0] is False

    #Since we stopped the component with SHUTDOWN, the controller should do nothing
    #and all other process should exit with FINISHED_STATE
    for component in controller.components:
        if component != source:
            assert component.state == experiment.model.codes.FINISHED_STATE

    failSafe.dispose()

    del all_comps


def test_controller_component_failure(experimentInstance, statusDatabase):

    '''Tests the controller correctly handles a component engine failure

    This should trigger POSTMORTEM cascade and force-shutdown other jobs'''

    # controller = experiment.control.Controller(experimentInstance.graph)
    controller, all_comps = new_controller(experimentInstance)

    print(controller.graph.nodes['stage0.Source0'])

    stage = experimentInstance._stages[0]

    controller.initialise(stage, statusDatabase)
    source = controller.graph.nodes['stage0.Source0']['component']()
    #VV: Kill Source0 1 second after it reports it started running
    #This should tigger POST_MORTEM for Source0, failure identify

    kill_engine_after_started(source.engine, time_delay=1.0)

    #Backup in-case everything hangs
    flag = [False]
    failSafe = reactivex.timer(80).subscribe(on_completed=lambda: KillController(controller, flag))

    with pytest.raises(experiment.runtime.errors.UnexpectedJobFailureError):
        controller.run()

    assert flag[0] is False

    #Since we killed the component all other components should be SHUTDOWN by controller
    #However due to various intervals some repeating consumers of the shutdown component
    #may notice its stopped before and FINISH as normal before they get SHUTDOWN message
    for component in controller.components:
        if component != source:
            assert component.state in [experiment.model.codes.SHUTDOWN_STATE, experiment.model.codes.FINISHED_STATE]

    failSafe.dispose()
    del all_comps

def test_controller_cleanup(experimentInstance, statusDatabase):

    '''Tests the controller cleanup method works'''

    # controller = experiment.control.Controller(experimentInstance.graph)
    controller, all_comps = new_controller(experimentInstance)

    print(controller.graph.nodes['stage0.Source0'])

    stage = experimentInstance._stages[0]

    controller.initialise(stage, statusDatabase)
    source = controller.graph.nodes['stage0.Source0']['component']()
    #In 10 seconds send cleanup to the Controller.
    #This should tigger a cascade and end the stage with an exception
    reactivex.timer(10).subscribe(on_completed=lambda: controller.cleanUp())

    failsafe_called = [False]

    def failsafe_cb():
        logger.critical("Failsafe invoked")
        KillController(controller, flag)
        failsafe_called[0] = True

    #Backup in-case everything hangs
    flag = [False]
    failSafe = reactivex.timer(80).subscribe(on_completed=failsafe_cb)

    # VV: All components are expected to be shutdown
    with pytest.raises(experiment.runtime.errors.FinalStageNoFinishedLeafComponents):
        controller.run()

    assert flag[0] is False

    errors = []
    for component in controller.components:
        if component.state != experiment.model.codes.SHUTDOWN_STATE:
            errors.append('Component %s is %s instead of %s' % (
                component.specification.reference, component.state, experiment.model.codes.SHUTDOWN_STATE
            ))

    if errors:
        raise ValueError('\n'.join(errors))

    failSafe.dispose()

    del all_comps

    if failsafe_called[0]:
        raise ValueError("Failsafe had to be invoked")

def test_controller_external_completion(experimentInstance, statusDatabase):

    '''Tests the controller correctly handles when an external completion hook returns True'''

    '''Tests the controller cleanup method works'''

    # controller = experiment.control.Controller(experimentInstance.graph)
    controller, all_comps = new_controller(experimentInstance)

    stage = experimentInstance._stages[0]
    controller.initialise(stage, statusDatabase)

    flag = [False]
    def ExternalCompletionClosure(flag):
        def ExternalCompletion(stage, directory):
            return flag[0]

        return ExternalCompletion

    controller.completionCheck = ExternalCompletionClosure(flag)
    #In 10 seconds switch external completion to return True
    #This should tigger a cascade and end the stage with everythini shutdown
    reactivex.timer(10).subscribe(on_completed=lambda: flag.insert(0, True))

    #Backup in-case everything hangs
    backUpFlag = [False]
    failSafe = reactivex.timer(80).subscribe(on_completed=lambda: KillController(controller, backUpFlag))

    # VV: All components are expected to be shutdown
    with pytest.raises(experiment.runtime.errors.FinalStageNoFinishedLeafComponents):
        controller.run()

    assert backUpFlag[0] is False

    #The stage should exit before all jobs are FINISHED
    states = [c.state for c in controller.components if c.state != experiment.model.codes.FINISHED_STATE]
    print(states)
    assert len(states) > 0

    failSafe.dispose()

    del all_comps


def test_controller_restart(experimentInstance, statusDatabase):

    '''Tests the controller correctly handles when a restart of one of the components'''

    import os, signal

    # controller = experiment.control.Controller(experimentInstance.graph)
    controller, all_comps = new_controller(experimentInstance)

    stage = experimentInstance._stages[0]

    controller.initialise(stage, statusDatabase)

    #In 14 seconds send a signal to the process of the Source0 component.
    #This should tigger POST_MORTEM for Source0, RESOURCE_EXHAUSTION identification

    def kill_component():
        source = controller.graph.nodes['stage0.Source0']['component']()
        logger.warning("Will kill %s" % source.specification.reference)
        try:
            os.kill(source.engine.process.pid, signal.SIGXCPU)
        except Exception:
            import traceback
            logger.critical("Could not kill %s" % source.specification.reference)
            logger.critical(traceback.format_exc())
            raise
    # VV: Set this to 14 seconds, because the job is 15 seconds long.
    #     I noticed that sometimes the signal would not actually be sent
    #     because the process was not yet instantiated
    reactivex.timer(14).subscribe(on_completed=kill_component)

    #Backup in-case everything hangs
    flag = [False]
    # VV: It takes ~95 seconds for this test to run, which is 14 + 81 seconds adding 19 more to the
    #     delay of `failSafe`
    failSafe = reactivex.timer(100).subscribe(on_completed=lambda: KillController(controller, flag))

    controller.run()
    assert flag[0] is False

    logger.info("Disposing failsafe")
    failSafe.dispose()

    logger.info("Checking components")
    #Since the components engine exited with RESOURCE_EXHAUSTION
    #It should be restarted and everything should finish as normal
    for component in controller.components:
         assert component.state == experiment.model.codes.FINISHED_STATE

    logger.info("Deleted components")
    del all_comps


def test_controller_restart_resourceExhausted_success(output_dir):
    flowir = """
blueprint:
  default:
    global:
      resourceManager:
        config:
          backend: simulator
      command:
        executable: "fake_executable"
        arguments: "fake_arguments"

components:
- name: subject
  stage: 0
  resourceManager:
    config:
      backend: simulator
  variables:
    sim_expected_exit_code: 0
    sim_range_execution_time: 0
    sim_range_schedule_overhead: 0

- name: leaf
  stage: 0
  command:
    arguments: stage0.subject:ref
  references:
  - stage0.subject:ref
  variables:
    # VV: Simulate a ResourceExhausted task after the producers have finished
    sim_expected_exit_code: 24 0
    sim_range_execution_time: 0
    sim_range_schedule_overhead: 0
"""

    controller = generate_controller_for_flowir(flowir, output_dir)

    flag = [False]
    failSafe = reactivex.timer(1200).subscribe(on_completed=lambda: KillController(controller, flag))

    controller.run()

    assert flag[0] is False
    failSafe.dispose()
    controller.cleanUp()

    errors = []
    expected_states = {
        'stage0.subject': experiment.model.codes.FINISHED_STATE,
        'stage0.leaf': experiment.model.codes.FINISHED_STATE,
    }

    for name in expected_states:
        if name not in controller.graph.nodes:
            errors.append("%s is not contained in controller graph" % name)
            continue
        try:
            comp = controller.graph.nodes[name]['component']()  # type: ComponentState
        except Exception as e:
            errors.append("Could not fetch ComponentState for %s. Error: %s" % (name, e))
            continue

        if expected_states[name] != comp.state:
            errors.append("Component %s is %s but was expected to be %s" % (
                name, comp.state, expected_states[name]
            ))

    try:
        comp = controller.graph.nodes['stage0.leaf']['component']()  # type: ComponentState
        if comp.engine.restarts != 1:
            errors.append("Leaf was expected to restart once, but restarted %d times" % comp.engine.restarts)
    except Exception as e:
        errors.append(f"Could not check how many times the Observer restarted due to {e}")

    if errors:
        raise ValueError('\n'.join(errors))


def validate_controller_states(expected_states, controller):
    # type: (Dict[str, str], experiment.runtime.control.Controller) -> List[str]
    errors = []

    for name in expected_states:
        if name not in controller.graph.nodes:
            errors.append("%s is not contained in controller graph" % name)
            continue
        try:
            comp = controller.graph.nodes[name]['component']()  # type: ComponentState
        except Exception as e:
            errors.append("Could not fetch ComponentState for %s. Error: %s" % (name, e))
            continue
        if isinstance(expected_states[name], tuple):
            if comp.state not in expected_states[name]:
                errors.append("Component %s is %s but was expected to be %s" % (
                    name, comp.state, [s for s in expected_states[name]]
                ))
        else:
            if expected_states[name] != comp.state:
                errors.append("Component %s is %s but was expected to be %s" % (
                    name, comp.state, expected_states[name]
                ))

    return errors


def test_controller_restart_resourceExhausted_failure(output_dir):
    flowir = """
blueprint:
  default:
    global:
      resourceManager:
        config:
          backend: simulator
      command:
        executable: "fake_executable"
        arguments: "fake_arguments"

components:
- name: subject
  stage: 0
  resourceManager:
    config:
      backend: simulator
  variables:
    sim_expected_exit_code: 0
    sim_range_execution_time: 0
    sim_range_schedule_overhead: 0
- name: observer
  stage: 0
  command:
    arguments: stage0.subject:ref
  references:
  - stage0.subject:ref
  workflowAttributes:
    repeatInterval: 1
    repeatRetries: 0
  variables:
    # VV: Simulate too many ResourceExhaustions
    sim_expected_exit_code: %s
    sim_range_execution_time: 0
    sim_range_schedule_overhead: 0
""" % ' '.join(['24' for _ in range(20)])

    controller = generate_controller_for_flowir(flowir, output_dir)

    flag = [False]
    failSafe = reactivex.timer(80).subscribe(on_completed=lambda: KillController(controller, flag))

    # VV: Expecting stage0.observer to fail with ResourceExhausted
    with pytest.raises(experiment.runtime.errors.UnexpectedJobFailureError) as e:
        controller.run()

    failSafe.dispose()
    errors = []

    exc = e.value  # type:experiment.runtime.errors.UnexpectedJobFailureError

    assert len(exc.failed_engines) == 1
    try:
        failed_name = exc.failed_engines[0].job.identification.identifier
        if failed_name != "stage0.observer":
            raise ValueError("Expected only stage0.observer to fail but \"%s\" failed" % (failed_name))

        exit_reason = exc.failed_engines[0].exitReason()
        if exit_reason != experiment.model.codes.exitReasons['ResourceExhausted']:
            errors.append("Expected state0.observer to exit with ResourceExhausted but it exited with %s" % (
                exit_reason
            ))
    except Exception as e:
        errors.append("Could not get exit-reason of state0.observer because %s" % e)

    assert flag[0] is False

    controller.cleanUp()

    expected_states = {
        'stage0.subject': experiment.model.codes.FINISHED_STATE,
        'stage0.observer': experiment.model.codes.FAILED_STATE,
    }

    errors.extend(validate_controller_states(expected_states, controller))

    try:
        comp = controller.graph.nodes['stage0.observer']['component']()  # type: ComponentState
        if comp.engine.restarts != 1:
            errors.append("Observer was expected to restart once, but restarted %d times" % comp.engine.restarts)
    except Exception:
        errors.append("Could not check how many times the Observer restarted")

    if errors:
        raise ValueError('\n'.join(errors))


def generate_flowir_for_shutdown_propagation(replica0_exit, replica1_exit, plain_exit, shutdown_on_fail, replicas=2):
    # type: (int, int, int, bool, int) -> str

    if shutdown_on_fail:
        shutdown_reason = "KnownIssue"
    else:
        shutdown_reason = "SystemIssue"

    flowir = """
    blueprint:
      default:
        global:
          resourceManager:
            config:
              backend: simulator
          command:
            executable: "fake_executable"
          workflowAttributes:
            shutdownOn:
              - %s
    variables:
      default:
        global:
         sim_range_execution_time: 0
         sim_range_schedule_overhead: 0
         sim_restart: 'no'

    components:
    - name: replicated
      workflowAttributes:
        replicate: %d
      variables:
        sim_expected_exit_code: %d %d[%%(replica)s]

    - name: plain
      variables:
        sim_expected_exit_code: %d

    - name: aggregate
      command:
        arguments: replicated:ref plain:ref
      references:
        - replicated:ref
        - plain:ref
      workflowAttributes:
        aggregate: True
    """ % (shutdown_reason, replicas, replica0_exit, replica1_exit, plain_exit)

    return flowir


def test_controller_shutdown_propagate_replica_noshutdown(output_dir):
    flowir = generate_flowir_for_shutdown_propagation(0, 1, 0, shutdown_on_fail=True)
    controller = generate_controller_for_flowir(flowir, output_dir)
    assert len([c for c in controller.graph.nodes]) == 4

    flag = [False]
    failSafe = reactivex.timer(80).subscribe(on_completed=lambda: KillController(controller, flag))

    controller.run()
    failSafe.dispose()

    assert flag[0] is False

    expected_states = {
        'stage0.replicated0': experiment.model.codes.FINISHED_STATE,
        'stage0.replicated1': experiment.model.codes.SHUTDOWN_STATE,
        'stage0.plain': experiment.model.codes.FINISHED_STATE,
        'stage0.aggregate': experiment.model.codes.FINISHED_STATE,
    }

    errors = validate_controller_states(expected_states, controller)

    if errors:
        raise ValueError('\n'.join(errors))


def test_controller_shutdown_propagate_replica_shutdown(output_dir):
    flowir = generate_flowir_for_shutdown_propagation(1, 1, 0, shutdown_on_fail=True)
    controller = generate_controller_for_flowir(flowir, output_dir)
    assert len([c for c in controller.graph.nodes]) == 4
    
    flag = [False]
    failSafe = reactivex.timer(80).subscribe(on_completed=lambda: KillController(controller, flag))

    # VV: All leaf nodes (stage0.aggregate) are expected to shutdown
    with pytest.raises(experiment.runtime.errors.FinalStageNoFinishedLeafComponents):
        controller.run()
    failSafe.dispose()

    assert flag[0] is False

    expected_states = {
        'stage0.replicated0': experiment.model.codes.SHUTDOWN_STATE,
        'stage0.replicated1': experiment.model.codes.SHUTDOWN_STATE,
        'stage0.plain': experiment.model.codes.FINISHED_STATE,
        'stage0.aggregate': experiment.model.codes.SHUTDOWN_STATE,
    }

    errors = validate_controller_states(expected_states, controller)

    if errors:
        raise ValueError('\n'.join(errors))


def test_controller_shutdown_propagate_nonreplica(output_dir):
    flowir = generate_flowir_for_shutdown_propagation(0, 0, 1, shutdown_on_fail=True)
    controller = generate_controller_for_flowir(flowir, output_dir)
    assert len([c for c in controller.graph.nodes]) == 4

    flag = [False]
    failSafe = reactivex.timer(80).subscribe(on_completed=lambda: KillController(controller, flag))

    # VV: All leaf nodes (stage0.aggregate) are expected to shutdown
    with pytest.raises(experiment.runtime.errors.FinalStageNoFinishedLeafComponents):
        controller.run()
    failSafe.dispose()

    assert flag[0] is False


    expected_states = {
        'stage0.replicated0': experiment.model.codes.FINISHED_STATE,
        'stage0.replicated1': experiment.model.codes.FINISHED_STATE,
        'stage0.plain': experiment.model.codes.SHUTDOWN_STATE,
        'stage0.aggregate': experiment.model.codes.SHUTDOWN_STATE,
    }

    errors = validate_controller_states(expected_states, controller)

    if errors:
        raise ValueError('\n'.join(errors))


def test_controller_shutdown_propagate_failure(output_dir):
    flowir = generate_flowir_for_shutdown_propagation(1, 1, 1, shutdown_on_fail=False, replicas=1)
    controller = generate_controller_for_flowir(flowir, output_dir)
    assert len([c for c in controller.graph.nodes]) == 3

    flag = [False]
    failSafe = reactivex.timer(80).subscribe(on_completed=lambda: KillController(controller, flag))

    with pytest.raises(experiment.runtime.errors.UnexpectedJobFailureError):
        controller.run()
    failSafe.dispose()

    assert flag[0] is False

    expected_states = {
        'stage0.replicated0': (experiment.model.codes.FAILED_STATE, experiment.model.codes.SHUTDOWN_STATE),
        'stage0.plain': (experiment.model.codes.FAILED_STATE, experiment.model.codes.SHUTDOWN_STATE),
        'stage0.aggregate': experiment.model.codes.SHUTDOWN_STATE,
    }

    errors = validate_controller_states(expected_states, controller)

    if errors:
        raise ValueError('\n'.join(errors))

    num_failures = 0
    for c in ['stage0.replicated0', 'stage0.plain']:
        comp = controller.graph.nodes[c]['component']()  # type: experiment.runtime.workflow.ComponentState
        if comp.state == experiment.model.codes.FAILED_STATE:
            num_failures += 1

    if num_failures < 1:
        raise ValueError("Was expected to have at least 1 failure")


def test_controller_restart_from_stage(output_dir):
    flowir = """
    blueprint:
      default:
        global:
          resourceManager:
            config:
              backend: simulator
          command:
            executable: "fake_executable"
            arguments: "fake_arguments"
    variables:
      default:
        global:
          sim_expected_exit_code: 0
          sim_range_execution_time: 0
          sim_range_schedule_overhead: 0
    
    components:
    - name: skip_this
    - name: exec_this
      stage: 1
    """
    # VV: Simulate restarting from stage 1
    controller = generate_controller_for_flowir(flowir, output_dir, initial_stage=1)

    # VV: There must be exactly 1 finished component (stage0.skip_this)
    assert len(controller.comp_done) == 1
    assert controller.get_compstate('stage0.skip_this').state == experiment.model.codes.FINISHED_STATE
    assert controller.get_compstate('stage1.exec_this').state == experiment.model.codes.RUNNING_STATE

    # VV: Setup a failsafe watchdog, run the stage, and make sure that the failsafe did not kick in
    flag = [False]
    failSafe = reactivex.timer(80).subscribe(on_completed=lambda: KillController(controller, flag))

    controller.run()
    assert flag[0] is False
    failSafe.dispose()
    controller.cleanUp()

    assert controller.get_compstate('stage1.exec_this').state == experiment.model.codes.FINISHED_STATE
    controller.experiment.incrementStage()


def test_override_restartHookFile(output_dir):
    flowir ="""
    components:
    - name: restarted
      workflowAttributes:
        restartHookFile: overridden-restart.py
        restartHookOn:
        - Success
      command:
        executable: echo
        arguments: world
    """
    # VV: The correct restart hook result in exactly 1 restart attempt, wrong one in 2
    controller = generate_controller_for_flowir(flowir, output_dir, extra_files={
        'hooks/__init__.py': '',
        'hooks/overridden-restart.py': """
import experiment.model.codes

def Restart(workingDirectory, restarts, componentName, log, exitReason, exitCode):
    log.critical("test_override_restartHookFile CORRECT workingDirectory=%s, restarts=%s, componentName=%s, "
              "exitReason=%s, exitCode=%s" % (workingDirectory, restarts, componentName, exitReason, exitCode))
    return experiment.model.codes.restartContexts["RestartContextRestartNotRequired"]""",
        'hooks/restart.py': """
import experiment.model.codes
# VV: This is a red herring
def Restart(workingDirectory, restarts, componentName, log, exitReason, exitCode):
    log.critical("test_override_restartHookFile WRONG workingDirectory=%s, restarts=%s, componentName=%s, "
              "exitReason=%s, exitCode=%s" % (workingDirectory, restarts, componentName, exitReason, exitCode))
    if restarts < 2:
        return experiment.model.codes.restartContexts["RestartContextRestartPossible"]
    else:
        return experiment.model.codes.restartContexts["RestartContextRestartNotRequired"]"""})

    controller.run()

    comp = controller.get_compstate('stage0.restarted')
    assert comp.engine.restarts == 1


def controller_with_max_restarts(output_dir, number_restarts):
    # type: (str, int) -> experiment.runtime.control.Controller
    """Controller for package containing exactly 1 node which keeps asking to restart itself each time it succeeds,
    component configuration contains workflowAttributes.maxRestarts which sets a hard limit to number of times
    the component can be restarted."""
    flowir = """
            components:
            - name: restarted
              workflowAttributes:
                maxRestarts: %d
                restartHookOn:
                - Success
              command:
                executable: echo
                arguments: world
            """ % number_restarts
    return generate_controller_for_flowir(flowir, output_dir, extra_files={
        'hooks/__init__.py': '',
        'hooks/restart.py': """
import experiment.model.codes

def Restart(workingDirectory, restarts, componentName, log, exitReason, exitCode):
    log.critical("controller_with_max_restarts workingDirectory=%s, restarts=%s, componentName=%s, "
              "exitReason=%s, exitCode=%s" % (workingDirectory, restarts, componentName, exitReason, exitCode))
    return experiment.model.codes.restartContexts["RestartContextRestartPossible"]"""})


@pytest.fixture(params=[0, 1, 2], ids=["zero", "one", "two"])
def configurable_component_max_restarts(request):
    return request.param


def test_controller_max_configurable_component_restarts(output_dir, configurable_component_max_restarts):
    # type: (str, int) -> None
    """Tests whether component configuration can dictate number maximum restarts (checks for 0, 1, and 2)"""
    controller = controller_with_max_restarts(output_dir, configurable_component_max_restarts)
    controller.run()
    controller.cleanUp()
    comp = controller.get_compstate('stage0.restarted')
    assert comp.engine.restarts == configurable_component_max_restarts
