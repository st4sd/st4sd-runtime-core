#
# coding=UTF-8
# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

'''Defines classes representing state of components in running workflow'''
from __future__ import annotations

import os
import traceback
import logging
import weakref
import pandas
from typing import (TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple,
                    cast, Union)

import reactivex
import reactivex.scheduler
import reactivex.operators as op
import reactivex.internal.exceptions

from future.utils import raise_with_traceback

import threading
import experiment.model.codes
import experiment.model.errors
import experiment.model.graph
import experiment.runtime.engine
import experiment.runtime.errors
import experiment.runtime.utilities.rx

if TYPE_CHECKING:
    from experiment.model.data import Job
    from experiment.model.graph import DataReference
    from experiment.runtime.optimizer import Optimizer


moduleLogger = logging.getLogger('compstate')


def CheckState(emission):
    #Enable when debugging reactive
    #Commented to exclude from checks for stray print statements
    moduleLogger.debug('Check State. Emission: %s. Thread %s' % (emission, threading.currentThread().name))
    return emission

def ComponentStateFromControllerStateAndEngine(controllerState, engine):
    # type: (Union[str, None], Union[experiment.runtime.engine.Engine, None]) -> str

    '''Determines component state based on controllerState and engine isAlive

    The controllerState is used first. If this is None the engine isAlive is used.

       Parameters:
           controllerState: string or None
           engine: An experiment.engine.Engine instance or subclass instance

       Returns:
           One of the experiment.codes state strings

       Exceptions:
            If both parameters are None an AttributeError will be raised'''


    if controllerState is None:
        retval = ComponentStateFromEngineIsAlive(engine.isAlive())
    else:
        retval = controllerState

    return retval

def ComponentStateFromEngineIsAlive(engineIsAlive):
    #type: (bool) -> str

    '''Determines component state based on engineIsAlive value

    Parameters:
        engineIsAlive: A bool

    Returns:
        One of the experiment.codes state strings

    Exceptions: None'''

    return experiment.model.codes.RUNNING_STATE if engineIsAlive is True else experiment.model.codes.POSTMORTEM_STATE

def IsAliveFromState(componentState):

    retval = True
    if componentState in [experiment.model.codes.FINISHED_STATE,
                          experiment.model.codes.FAILED_STATE,
                          experiment.model.codes.SHUTDOWN_STATE]:
        retval = False

    return retval

class ComponentState(object):

    '''Represent state of a component in a workflow

    Component are either RUNNING, POSTMORTEM, or Finished (FAILED/SUCCESS)

    Observable emit events on transition to POSTMORTEM and FAILED/SUCCESS

    The transition to POSTMORTEM can be
    - internal (normal finish)
    - external (finish() was called)

    Subscribers to an observable can identify which via the finishCalled property.

    If the transition was triggered externally, subscribers to POSTMORTEM should not
    try to modify the state of the Component or its Engine in any way.
    '''

    # VV: The poolScheduler will be instantiated by a single ComponentState, all ComponentStates use the same pool
    componentScheduler: reactivex.scheduler.ThreadPoolScheduler | None = None

    def __init__(self,
                 componentSpecification: Job,
                 workflowGraph: experiment.model.graph.WorkflowGraph,
                 create_engine: bool = True):
        """

        Args:
            componentSpecification: An experiment.data.ComponentSpecification instance.
                Defines the static properties of the receiver
            workflowGraph: An experiment.model.graph.experiment.WorkflowGraph instance
            create_engine: bool
                Whether to actually create the engine or not.
                When restarting an instance from stage I we should not create engines for
                the components in any stage K for K < I
        """
        # VV: ThreadPoolGenerator.get_pool() is threadSafe, so if multiple threads call it concurrently they'll all get
        # the same pool. GIL will then guarantee that every thread will read the same value even if multiple threads
        # end up "updating" ComponentState.componentScheduler.
        tpg = experiment.runtime.utilities.rx.ThreadPoolGenerator
        if ComponentState.componentScheduler is None:
            ComponentState.componentScheduler = tpg.get_pool(tpg.Pools.ComponentState)

        self._specification = componentSpecification

        self.controllerState = None
        self.log = logging.getLogger("wf.cs.%s" % self.specification.reference.lower())
        self.repeatingDisposable = None

        self.workflowGraph = workflowGraph  # type: experiment.model.graph.WorkflowGraph
        self.updateGraph(workflowGraph)

        self.repeatingObservable = None
        self._finishedCalled = False

        self._detailedState : reactivex.Observable | None = None
        self._engine: experiment.runtime.engine.Engine | None = None

        if create_engine:
            self._engine = experiment.runtime.engine.Engine.\
                engineForComponentSpecification(self.specification)  # type: experiment.runtime.engine.Engine

            #Initialise State Data
            stateData = self.stateDictionary
            # VV: FIXME this is probably meant to be:
            # serialScheduler = reactivex.scheduler.ThreadPoolScheduler(1)
            serialScheduler = reactivex.scheduler.NewThreadScheduler()
            def UpdateClosure(componentState, log, componentObject):

                '''Returns a function which updates componentState based on engine emission'''

                def UpdateStateBasedOnEngine(emission):

                    log.debug('Updating State based on %s' % str(emission))



                    #The only updatable elements of the componentState are `state` and `isAlive`
                    #These can only change if
                    #1. engine isAlive has changed i.e 'isAlive' is present in the emission
                    #2. self.controllerState has been set
                    #
                    #The second case only happens if `finish()` has been called
                    #Therefore if it is set we ignore the engine and act immediately on it


                    log.debug('Current state is %s' % componentState )

                    #componentObject.controllerState is only set if finish() is called
                    if componentObject.controllerState is not None:
                        componentState['state'] = ComponentStateFromControllerStateAndEngine(componentObject.controllerState,
                                                                          None)
                        componentState['isAlive'] = IsAliveFromState(componentState['state'])
                        log.debug('New state from controllerState update is %s' % componentState)
                    else:
                        try:
                            # There are two emission types - the timer (ints) and the engine (tuple)
                            # If we get an int the below code will raise an TypeError when we try to unpack it
                            # In this case we just return the last state (as the state can't change without engine changing)

                            # This will return None if isAlive hasn't chan  ged as the key won't be there
                            # in this case keep the component State value
                            engineUpdates, engine = emission
                            engineIsAlive = engineUpdates.get('isAlive')

                            if engineIsAlive is not None:
                                #Update the componentState based on new engine isAlive condition
                                componentState['state'] = ComponentStateFromEngineIsAlive(engineIsAlive)
                                componentState['isAlive'] = IsAliveFromState(componentState['state'])
                                log.debug('New state from engine isAlive update is %s' % componentState)
                            else:
                                log.debug('Engine isAlive did not change')
                        except TypeError as error:
                            pass

                    return componentState

                return UpdateStateBasedOnEngine

            #If an engine is present we determine the state based on the engine update emissions
            #Note: This means _detailedState will reflect the state at time of engine emission
            self._detailedState = reactivex.interval(5, scheduler=reactivex.scheduler.NewThreadScheduler())\
                .pipe(
                    op.merge(self.engine.stateUpdates),
                    op.observe_on(serialScheduler),
                    op.map(UpdateClosure(stateData, self.log, self)),
                    op.observe_on(ComponentState.componentScheduler)
                )
        else:
            #If an engine is not present we determine the state from stateDictionary
            self._engine = None
            self._detailedState = reactivex.interval(5, scheduler=reactivex.scheduler.NewThreadScheduler()).pipe(
                op.map(lambda x: self.stateDictionary)
            )

        # VV: This is generated (and cached) within the first invocation of self.stateUpdates
        def StateClosure(state):
            def StateFilter(newState):
                filtered = {k: newState[k] for k in list(newState.keys()) if newState[k] != state[k]}
                state.update(newState)
                return filtered, newState

            return StateFilter

        stateData = self.stateDictionary

        def final_map(x):
            filtered, newstate = x
            filtered.update({'sourceType': stateData['sourceType'], 'reference': self.specification.reference})
            return (filtered, self)

        def on_terminate():
            try:
                self.log.info('Component state observable got terminate message (isAlive: %s)' % self.isAlive())
            except Exception:
                # VV: It's been observed that on_terminate can trigger after the experiment.log file is removed
                #     which can result in an exception being raised here; in turn that breaks unit-tests
                pass

        # First remove keys that aren't the same, then don't emit empty dicts, then add back missing metadata
        # Note since update returns None we can use `or e` to have the lambda return e after the update
        # Finish if dict is empty and condition is met

        check_state = experiment.runtime.utilities.rx.report_exceptions(CheckState, self.log, 'CheckState')

        self._state_updates: reactivex.ConnectableObservable = self._detailedState.pipe(
            op.observe_on(ComponentState.componentScheduler),
            op.map(StateClosure(stateData)),
            op.map(check_state),
            op.take_while(lambda x: x[1]['isAlive'] is True or len(x[0]) != 0),
            op.do_action(on_completed=on_terminate),
            op.filter(lambda x: len(x[0]) != 0),
            op.map(final_map),
            op.publish()
        ).auto_connect()

    def memoization_reset(self):
        return self.specification.componentSpecification.memoization_reset()

    @property
    def memoization_info(self):
        return self.specification.componentSpecification.memoization_info

    @property
    def memoization_hash(self):
        return self.specification.componentSpecification.memoization_hash

    @property
    def memoization_info_fuzzy(self):
        return self.specification.componentSpecification.memoization_info_fuzzy

    @property
    def memoization_hash_fuzzy(self):
        return self.specification.componentSpecification.memoization_hash_fuzzy

    @property
    def graph(self):
        return self.workflowGraph.graph

    def __str__(self):

        return "State of workflow component %s is: %s" % (self.name, self.state)

    def updateGraph(self, workflowGraph):
        # type: (experiment.model.graph.WorkflowGraph) -> None
        self.workflowGraph = workflowGraph
        # Add self to the graph.
        self.graph.nodes[self._specification.reference]['component'] = weakref.ref(self)

    @property
    def name(self):

        return self.specification.name

    @property
    def engine(self):
        # type: () -> Union[experiment.runtime.engine.Engine, experiment.runtime.engine.RepeatingEngine]
        return self._engine

    @engine.setter
    def engine(self, engine):

        '''Sets the engine

        Note: Setting the engine after called stageIn() will result in inconsistencies'''

        self._engine = engine

    @property
    def state(self):

        """A component is either RUNNING or POSTMORTEM unless the controller sets otherwise

        Returns
        -------
        str
            The state of the component (experiment.codes.<>_STATE)
        """

        try:
            retval = ComponentStateFromControllerStateAndEngine(self.controllerState, self.engine)
        except Exception as error:        
            self.log.debug("Encountered error while getting state: %s" % str(error))
            self.log.debug("Defaulting to RUNNING_STATE")
            retval = experiment.model.codes.RUNNING_STATE

        return retval

    @property
    def producer_names(self):
        # type: () -> List[str]
        producers = self.producers
        return sorted(set([prod.specification.reference for prod in producers]))

    @property
    def stateDictionary(self):

        '''Returns a dictionary containing detailed information on CURRENT state of the receiver.

        Note: DO NOT POLL this property to monitor changes to the component state

        The component-state depends on the engine state and changes as it does.
        The processing of these changes by ComponentState may lag the state of the engine when this method is called.

        That is when you call this method you get the state at the moment of calling.
        If the state changed multiple time between two calls to this method it will not be seen

        To track all changes to ComponentState you need to subscribe to the stateUpdates observable
        (or observable based of this)
        '''

        componentStateDictionary = {
            'sourceType': 'component',
            'reference': self.specification.reference,
            'isAlive': self.isAlive(),
            'state': self.state,
            'engine': 'repeatingEngine' if isinstance(self.engine, experiment.runtime.engine.RepeatingEngine) else 'engine',
            #We may need to condense this for aggregating components
            'consumesFrom': ",".join(self.producer_names)}

        return componentStateDictionary

    @property
    def stateUpdates(self) -> reactivex.ConnectableObservable:

        '''Returns an rx observable that emits changes to stateDictionary

        The emission will only contain changed keys that change after subscribing
        i.e. the initial state will not be emitted only updates to this state

        Each emission is a tuple - (stateDictionary, reference)

        The observables completes after last update after isAlive returns False
        (component enters FAILED or FINISHED state)
        '''

        def generate_artificial_observation(which):
            # type: (ComponentState) -> Tuple[Dict[str, Any], ComponentState]
            which.log.log(14, "Emit artificial %s because my Observable is already complete" % which.stateDictionary)

            return which.stateDictionary, which

        # VV: Use the default_if_empty() RX operator to handle the `sequence contains no elements` errors
        # When the state updates are empty we emit an artificial observation (stateDictionary and self)

        return self._state_updates.pipe(
            op.default_if_empty(default_value=None),
            op.map(lambda e: e if e is not None else generate_artificial_observation(self))
        )

    @property
    def combinedStateUpdates(self) -> reactivex.ConnectableObservable:

        '''Observable that emits updates from the component and its engine in a single stream

        Completes when component isAlive returns False.
        There will be a final emission to indicate the state-change'''

        return self.stateUpdates.pipe(
            op.merge(self.engine.stateUpdates)
        )

    @property
    def specification(self):
        # type: () -> Job
        return self._specification

    @property
    def producers(self):
        # type: () -> List[ComponentState]

        '''Returns a list of the components the receiver consumes from

        Elements of the list are instances of experiment.workflow.ComponentState'''
        producer_components = []
        try:
            # Filter on instantiated components -
            # if we restarted a stage components in previous stages won't have been created
            datarefs = self.specification.componentDataReferences

            unfiltered_producer_names = []

            for dr in datarefs:
                component_ids = dr.true_reference_to_component_id(self.workflowGraph)
                for comp_id in component_ids:
                    unfiltered_producer_names.append('stage%d.%s' % comp_id)

            self.log.log(13, 'UnfilteredProducerNames of %s are %s' % (self.specification.reference,
                                                                        unfiltered_producer_names))

            producer_nodes = [(name, self.graph.nodes[name]) for name in unfiltered_producer_names]

            producer_nodes = [nd_tup for nd_tup in producer_nodes if 'component' in nd_tup[1]]
            #VV: Obtain all the ComponentStates first
            producer_components = [nd_tup[1]['component']() for nd_tup in producer_nodes]
        except KeyError as e:
            message = 'Could not construct producer list because of KeyError %s' % e
            self.log.critical(message)
            raise_with_traceback(experiment.model.errors.InternalInconsistencyError(message))
        except Exception:
            message = traceback.print_exc()
            self.log.critical('Could not construct producer list! (Exception):')
            self.log.critical(message)
            raise_with_traceback(experiment.model.errors.InternalInconsistencyError(message))

        return producer_components

    @property
    def consumers(self):

        '''Returns a list of the ComponentState instances that consume from the receiver.

        Note: This only includes consumers in the same stage as the receiver.
        Consumers in downstream stages will not been initialised so can't be returned.

        Returns:
            A list of experiment.workflow.ComponentState instances'''

        #This returns the node names as full references
        consumer_names = experiment.model.graph.ConsumersForNode(self.specification.reference, self.graph)

        try:
            instantiated_consumers_names = [(n, d) for n, d in self.graph.nodes(data=True)
                                            if n in consumer_names and 'component' in d]
            # Now filter on those where the weakref has not been garbage collected
            alive_consumer_names = [nd_tup for nd_tup in instantiated_consumers_names if nd_tup[0] in consumer_names and nd_tup[1]['component']() is not None]

            consumer_components = [nd_tup[1]['component']() for nd_tup in alive_consumer_names]
        except KeyError as e:
            message = 'Could not construct consumer list because of KeyError %s' % e
            self.log.critical(message)
            raise experiment.model.errors.InternalInconsistencyError(message)
        except Exception:
            message = traceback.print_exc()
            self.log.critical('Could not construct consumer list! (Exception):')
            self.log.critical(message)
            raise experiment.model.errors.InternalInconsistencyError(message)

        return consumer_components

    #Note: This is not a property in order to aid in passing the function as an arg
    def isAlive(self):

        return IsAliveFromState(self.state)

    def _notifyProducersFinished(self, error=None):

        '''Note: Only called if engine is repeating'''

        if error is not None:
            reference = self.specification.reference
            self.log.warning('Repeating observable of %s exited with error %s' % (reference, error))
            self.log.critical('EXCEPTION(notify):\n%s' % ''.join(traceback.format_stack()))
            raise SystemError("%s observable exited with %s" % (reference, error))
        else:
            self.log.log(19, 'All producers of %s have completed' % self.specification.reference)

        # VV: Instead of killing the Engine notify it that all of its producers are now Finished
        engine = cast(experiment.runtime.engine.RepeatingEngine, self.engine)
        engine.notify_all_producers_finished()

    def stageIn(self, stageData=True):
        if stageData:
            try:
                self.specification.stageIn()
            except experiment.model.errors.DataReferenceFilesDoNotExistError as e:
                if self.specification.workflowAttributes['isRepeat'] is True:
                    # VV: It's ok for Observers to not match some of their data-references though we should check
                    #     that said references don't have a Producer->Consumer dependency with this Component
                    pass
                else:
                    # VV: Repeating Components are allowed to refer to non-existing files as long as they are
                    #     produced by a Component for which they have a Subject->Observer dependency
                    consumer_producer = []  # type: List[Tuple[DataReference, str]]
                    direct_references = []  # type: List[Tuple[DataReference, str]]
                    subject_observer = []  # type: List[Tuple[DataReference, str]]

                    for ref, path in e.referenceErrors:
                        if ref.isDirectReference(self.specification.workflowGraph):
                            direct_references.append((ref, path))
                        elif ref.producerIdentifier.stageIndex < self.specification.stageIndex:
                            # VV: Input Components of earlier stages indicate a Producer->Consumer dependency
                            consumer_producer.append((ref, path))
                        else:
                            # VV: Input Components in the same stage indicate a Subject->Observer dependency
                            subject_observer.append((ref, path))

                    errors = consumer_producer + direct_references

                    def prettify(ref_errors):
                        # type: (List[Tuple[DataReference, str]]) -> List[Tuple[str, str]]
                        ret = []

                        for reference, path in ref_errors:
                            ret.append((reference.stringRepresentation, path))

                        return ret

                    if consumer_producer:
                        self.log.critical("Found %s consumer->producer errors for %s" % (
                            prettify(consumer_producer), self.specification.reference
                        ))

                    if direct_references:
                        self.log.critical("Found %s direct-reference errors for %s" % (
                            prettify(direct_references), self.specification.reference
                        ))

                    if subject_observer:
                        self.log.warning("Found %s subject-observer warnings for %s" % (
                            prettify(subject_observer), self.specification.reference
                        ))

                    if errors:
                        raise experiment.model.errors.DataReferenceFilesDoNotExistError(errors)

        # VV: If we've already been asked to not execute do not setup any observers
        if self._finishedCalled:
            return

        # If the engine is repeating subscribe to state updates of all producers
        # Starting on stageIn instead of init as
        # there is no guarantee of the external ordering of instantiations

        if isinstance(self.engine, experiment.runtime.engine.RepeatingEngine):
            producerStates = [p.notifyFinished for p in self.producers if p.isAlive()]

            #Switch the notifyFinished emissions to a separate thread so operations here
            #won't block its emission to other subscribers
            if producerStates:
                self.repeatingObservable = reactivex.merge(*producerStates).pipe(
                    op.observe_on(ComponentState.componentScheduler),
                    op.filter(lambda x: x[0].get('isAlive') is False)
                )

                notify_prod_finished = experiment.runtime.utilities.rx.report_exceptions(
                    self._notifyProducersFinished, self.log, 'NotifyProdFinished')

                self.repeatingDisposable = self.repeatingObservable.\
                    subscribe(
                        on_next=lambda n: self.log.log(19, 'Producer %s completed' % n[1].specification.reference),
                        on_error=lambda e: notify_prod_finished(error=e),
                        on_completed=notify_prod_finished
                    )
            else:
                self._notifyProducersFinished()

    def run(self):

        #Refactoring
        #1. Move both engines to take canConsume as an observable (removes canConsume/producerJobs calls)
        if self.state != experiment.model.codes.SHUTDOWN_STATE:
            self.engine.run()
        else:
            self.log.warning("Asked to run() but ComponentState is already in state %s" % self.state)

    def optimizer_enable(self, optimizer, disable_optimizer_callback):
        #  type: (Optimizer, Callable[[ComponentState], None]) -> None
        self.engine.optimizer_enable(optimizer, lambda myself=self: disable_optimizer_callback(myself))

    def optimizer_disable(self, propagate):
        # type: (bool) -> None
        rep_engine = self.engine  # type: experiment.runtime.engine.RepeatingEngine

        rep_engine.optimizer_disable(propagate)

    def restart(self, reason=None, code=None):
        # type: (str, int) -> str

        '''Restarts the receivers engine

        If finish() was called on the receiver this will raise an errors.CannotRestartShutdownEngineError as
        this indicates a coding logic error (trying to restart after shutdown)

        Returns:
            (str) An experiment.codes.restartCode
        '''

        if not self.engine.isShutdown:
            return self.engine.restart(reason=reason, code=code)
        else:
            raise experiment.runtime.errors.CannotRestartShutdownEngineError(self.specification.reference)

    @property
    def isStaged(self):

        return self.specification.isStaged

    @property
    def notifyFinished(self) -> reactivex.ConnectableObservable:

        '''
        Returns a published auto-connect rx.Observable that emits a single event when the receiver finishes

        The event is a tuple. The first element will be a dictionary.
        This dictionary will contain at least the key 'state' whose value will be FINISHED_STATE or FAILED_STATE
        The second will be the receiver
        '''
        default_value = ({}, self)
        return self.stateUpdates.pipe(op.first_or_default(lambda x: x[0].get('isAlive') is False, default_value))

    @property
    def notifyPostMortem(self) -> reactivex.ConnectableObservable:

        '''
        Returns a published auto-connect rx.Observable that emits an event when the receiver enters POSTMORTEM

        Completes when component isAlive returns False (component enter FINISHED or FAILED state).

        The event is a tuple. The first element will be a dictionary.
            This dictionary will contain at least the key 'state' whose value will be POSTMORTEM_STATE
        The second will be the receivers
        '''
        return self.stateUpdates.pipe(op.filter(lambda x: x[0].get('state') == experiment.model.codes.POSTMORTEM_STATE))

    def finish(self, finalState):

        ''''
        If receiver is in POSTMORTEM state it will immediately transition to finalState
        If receiver is in RUNNING state it will transition to finalState asynchronously (via POSTMORTEM temporarily)

        If component is migratable the components engine will not be killed.
        In this situation it should be killed directly if you want it to stop
        '''

        self.log.info("Finish called for component %s with finalState %s" % (
            self.specification.identification, finalState))

        self._finishedCalled = True

        # Protocol
        # If engine is still running send kill() (we are in RUNNING state)
        #   Subscribe to our own state observable so when it exits finalState is set
        # If engine is already finished (we are in POSTMORTEM state)
        #   Set state to finalState

        def FinalStateClosure(state, shutdown):

            def Setter(e):
                if self.repeatingDisposable is not None:
                    self.repeatingDisposable.dispose()

                self.controllerState = state
                if shutdown:
                    self.engine.shutdown()

            return Setter

        stopEngine = not self.specification.isMigratable

        if self.state == experiment.model.codes.RUNNING_STATE:
            def stop_engine():
                if stopEngine:
                    self.engine.kill()
                else:
                    # Trigger POSTMORTEM directly while keeping engine alive
                    self.controllerState = experiment.model.codes.POSTMORTEM_STATE

            def on_error_notifyPostMortem(err):
                if isinstance(err, reactivex.internal.exceptions.SequenceContainsNoElementsError):
                    self.log.warning("I do not emit any updates even though I'm in running state; will kill my engine")
                else:
                    self.log.warning(
                        "I ran into an error while observing notifyPostMortem: %s; will kill my engine" % (err))
                stop_engine()

            # If repeating we will still be observing producers
            # However handle in the method that subscribes to repeatingObservable

            final_state = experiment.runtime.utilities.rx.report_exceptions(
                FinalStateClosure(finalState, stopEngine), self.log, 'NotifyProdFinished')

            on_error_notifyPostMortem = experiment.runtime.utilities.rx.report_exceptions(
                on_error_notifyPostMortem, self.log, 'ErrorNotifyPostMortem')

            self.notifyPostMortem.subscribe(on_next=final_state, on_error=on_error_notifyPostMortem)
            stop_engine()
        else:
            self.controllerState = finalState
            if stopEngine:
                self.engine.shutdown()

    @property
    def finishCalled(self):

        '''Returns True if finish() has been called

        notifyPostMortem subscribers should check to see if the state
        was externally triggered.'''

        return self._finishedCalled

    @property
    def stageIndex(self):
        return self.specification.stageIndex

    def __repr__(self):
        return "ComponentState(%s, %s)" % (self.specification.reference, self.state)

class StageState(object):

    def __init__(self, index):

        self.componentStates = {}  # type: Dict[str, ComponentState]
        self.index = index

        # VV: The controller can dictate the state of the stage
        self.controller_state = None  # type: Optional[str]

    def stateForComponent(self, jobName):    
        """

        Parameters
        ----------
        jobName: str
            Name of the job

        Returns
        -------
        ComponentState
            The state of the component
        """
        return self.componentStates[jobName]

    def addComponentState(self, componentState):

        self.componentStates[componentState.name] = componentState

    @property
    def runningComponents(self):    
        """

        Returns:
        List[ComponentState]
            List of ComponentStates for all running components
        """

        data = [el for el in list(self.componentStates.values()) if el.state not in [experiment.model.codes.FINISHED_STATE,
                                                                                     experiment.model.codes.FAILED_STATE,
                                                                                     experiment.model.codes.SHUTDOWN_STATE]]
        return data

    @property
    def finishedComponents(self):
        """

        Returns:
        List[ComponentState]
            List of ComponentStates for all finished components
        """

        data = [el for el in list(self.componentStates.values()) if el.state in [experiment.model.codes.FINISHED_STATE,
                                                                                 experiment.model.codes.FAILED_STATE,
                                                                                 experiment.model.codes.SHUTDOWN_STATE
                                                                                 ]]
        return data

    @property
    def failedComponents(self):    
        """

        Returns:
        List[ComponentState]
            List of ComponentStates for all failed components
        """

        data = [el for el in list(self.componentStates.values()) if el.state == experiment.model.codes.FAILED_STATE]
        return data

    @property
    def state(self):

        '''Returns the state of the current stage.
        
        This method will be called from various threads mainly
        while the run() method is executing'''

        if self.controller_state is not None:
            return self.controller_state

        #If no managers created stage is initialising
        if len(self.componentStates) == 0:
            retval =  experiment.model.codes.INITIALISING_STATE
        else:
            #If any job in RESOURCE_WAIT_STATE then stage state is RESOURCE_WAIT_STATE
            #If any job FAILED then stage is failed
            #If all jobs are finished, stage is finished
            #If all job suspeneded, stage is suspended
            #If not RESOURCE_WAIT_STATE and a job is in running state, or POSTMORTEM, the stage state is RUNNING_STATE
            componentStates = set([component.state for component in list(self.componentStates.values())])
            if experiment.model.codes.RESOURCE_WAIT_STATE in componentStates:
                retval = experiment.model.codes.RESOURCE_WAIT_STATE
            elif experiment.model.codes.FAILED_STATE in componentStates:
                retval = experiment.model.codes.FAILED_STATE
            elif experiment.model.codes.RUNNING_STATE in componentStates or experiment.model.codes.POSTMORTEM_STATE in componentStates:
                #If a job is in post mortem state i.e. dead and waiting to be checked by controller
                #keep stage in running state
                retval = experiment.model.codes.RUNNING_STATE
            elif experiment.model.codes.SHUTDOWN_STATE in componentStates:
                #If any component was stopped (but nothing Failed) stage was shutdown
                #This can happen if the stage was cancelled via external signal
                #or if any of the components was configured to transition to shutdown on non-SUCCESS
                retval = experiment.model.codes.SHUTDOWN_STATE
            elif len(componentStates) == 1:
                retval = componentStates.pop()
            else:    
                #This is a bug, if nothing is running, failed, or in resource wait
                #Then all jobs must be either FINISHED or SUSPENDED
                errorString = "Inconsistent jobs states (%s) in stage %d" % (componentStates, self.index)
                raise experiment.model.errors.InternalInconsistencyError(errorString)

        return retval       
