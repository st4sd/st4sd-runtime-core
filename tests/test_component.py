# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import print_function

import pytest

import experiment.model.codes
import experiment.runtime.engine
import experiment.model
import experiment.runtime.workflow
from . import utils
from .reactive_testutils import *


@pytest.fixture(params=["stage0.Source0", "stage0.Aggregating"], ids=["Source", "Aggregating"])
def componentReference(request):
    return request.param


#
# Components go from RUNNING -> POSTMORTEM -> FINAL STATE
# POSTMORTEM state is set by controller, when it engine exits
# FINAL STATE is always set by controller
# POSTMORTEM state can be result of internal (success) or external (shutdown)
#

#
# component.finish() Tells a component to stop.
# component.finish() Tells a component to stop.
#   Its engine will be halted (if running) and shutdown.
#   If the engine is running it will finish asynchronously and final state is not set until it does
#

#
#
# Tests
#
# 1. Run starts the engine
# 2. External exit works (stop) (state-change, notification, and expected actions)
# 3. Internal stop works (completion) (state-change, notification, and expected actions)
#       RepeatingEngine == Producers finished. Required testing Producer/Consumer chain
#       Engine == Exited with success

def test_component_shutdown_no_run_source(output_dir):
    # VV: I noticed that when things go bad in reactivex some unit-tests succeed but the teardown of their
    # pytest.fixtures raises exceptions while checking whether the observables of Engines and ComponentStates
    # complete in a reasonable amount of time. This unit test just checks what happens if we finish a componentstate
    # without ever asking it to run. This is the most common reason why the teardown codes raise exceptions
    flowir="""
    components:
    - name: source
      command:
        executable: echo
        arguments: hello world
    """
    comp_exp = utils.experiment_from_flowir(flowir, output_dir)
    stage = next(comp_exp.stages())
    job = stage.jobs()[0]
    comp = experiment.runtime.workflow.ComponentState(job, comp_exp.experimentGraph)

    assert comp.isAlive() is True

    completed = []
    comp.notifyFinished.subscribe(on_completed=CompletedClosure(completed, "notifyFinished"))
    comp.finish(experiment.model.codes.FINISHED_STATE)
    WaitOnCompletion(completed, limit=20)
    assert completed[0] is True


def test_component_engine_instantiation(components, componentReference):

    c = components[0].graph.nodes[componentReference]['component']()

    if c.specification.reference == 'stage0.Aggregating':
        assert isinstance(c.engine, experiment.runtime.engine.RepeatingEngine)
    elif  c.specification.reference == 'stage0.Source0':
        assert isinstance(c.engine, experiment.runtime.engine.Engine)
    else:
        logger.critical('Unexpected component %s' % c.specification.reference)
        assert 0

def test_component_engine_run(components, componentReference):

    '''Test run() starts the engine

    Via notifications of Component state observable'''

    c  = components[0].graph.nodes[componentReference]['component']()

    assert c.isAlive() == True
    assert c.state == experiment.model.codes.RUNNING_STATE
    logger.info('Test passed. Initial state is RUNNING STATE and isAlive is True')

    #Subscribe observable
    emissions = []
    completed = []
    disp = c.combinedStateUpdates.subscribe(on_next=GatherClosure(emissions))
    finishedDisposable = c.notifyFinished.subscribe(on_completed=CompletedClosure(completed))

    #Run
    c.stageIn()
    c.run()

    #Wait on notification
    WaitOnEvent(emissions, lambda e: e[-1][0]['sourceType'] != 'component' and e[-1][0].get('isRunning') is True)

    c.finish(experiment.model.codes.FINISHED_STATE)

    WaitOnCompletion(completed)
    finishedDisposable.dispose()

    assert c.state == experiment.model.codes.FINISHED_STATE
    assert completed[0]
    disp.dispose()


def test_component_internal_finish(components, componentReference):

    '''Test a component exits correctly due to 'success

    Also test port-mortem notification and final state setting

    '''

    c  = components[0].graph.nodes[componentReference]['component']()

    #Subscribe observable
    completed = []
    postMortem = []
    postMortemDisposable = c.notifyPostMortem.subscribe(on_next=CompletedClosure(postMortem))
    finishedDisposable = c.notifyFinished.subscribe(on_completed=CompletedClosure(completed))

    #For components with repeating engines
    def FinishedClosure(a):
        return lambda e: a.finish(experiment.model.codes.FINISHED_STATE)

    #Run
    #For Aggregate we have to start everything for it to finish via internal route
    if isinstance(c.engine, experiment.runtime.engine.RepeatingEngine):
        limit=120
        #For each component we have to subscribe to its post-mortem so we can finish it
        #and trigger the downstream finished cascade
        #Also need to stageIn so the cascade is set up
        for a in components:
            if a != c:
                a.notifyPostMortem.subscribe(on_next=FinishedClosure(a))
            a.stageIn()

        for a in components:
            a.run()
    else:
        limit=30
        c.stageIn()
        c.run()

    #Wait on POSTMORTEM state change
    WaitOnCompletion(postMortem, limit=limit)

    assert postMortem[0] == True
    logger.info('Internal finish path test part 1 successful: notifyPostMortem emitted ')
    assert c.state == experiment.model.codes.POSTMORTEM_STATE
    logger.info('Internal finish path test part 1 successful: POSTMORTEM_STATE set')

    postMortemDisposable.dispose()
    c.finish(experiment.model.codes.FINISHED_STATE)

    WaitOnCompletion(completed)
    finishedDisposable.dispose()

    assert completed[0] == True
    logger.info('Internal finish path test part 2 successful: notifyFinished observable emitted asynchronously ')

    assert c.state == experiment.model.codes.FINISHED_STATE
    logger.info('Internal finish path test part 2 successful: state correctly set asynchronously')


def test_component_external_finish_running(components, componentReference):

    '''Test a component finishes correctly when in RUNNING

    Also should check that unsubscribe works.
    i.e. register for "POSTMORTEM", then remove it and trigger it directly'''

    c  = components[0].graph.nodes[componentReference]['component']()

    #Subscribe observable
    completed = []
    postMortem = []

    #Registr for POST-MORTEM and COMPLETED notifications
    #In this way we can test unsubscribing from one
    postMortemDisposable = c.notifyPostMortem.subscribe(on_next=CompletedClosure(postMortem))
    finishedDisposable = c.notifyFinished.subscribe(on_completed=CompletedClosure(completed))

    #Run
    c.stageIn()
    c.run()

    assert c.finishCalled == False

    #Dispose of post-mortem as we are going to kill it
    postMortemDisposable.dispose()

    #Now finish component
    c.finish(experiment.model.codes.FAILED_STATE)

    assert c.finishCalled == True

    #Wait for it
    WaitOnCompletion(completed)
    finishedDisposable.dispose()

    assert completed[0] == True
    logger.info('External finish path test successful: notifyFinished observable emitted asynchronously ')

    #Check post-mortem on_completion event was never triggered
    assert postMortem[0] == False
    logger.info('Unsubscription test successful: postMortem observable did not emit')
    assert c.state == experiment.model.codes.FAILED_STATE
    logger.info('External finish path test successful: state correctly set asynchronously')
