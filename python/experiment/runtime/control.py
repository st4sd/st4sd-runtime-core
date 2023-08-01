#
# coding=UTF-8
# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

'''Class/Function which control a package'''

from __future__ import annotations

import datetime
import distutils.dir_util
import logging
import math
import os
import pprint
import threading
import time
import traceback
import uuid
from typing import (TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set,
                    Tuple, Union, cast)

import networkx
import reactivex
import reactivex.scheduler
import reactivex.operators as op
import reactivex.internal.exceptions
from future.utils import raise_with_traceback

import experiment.appenv
import experiment.model.codes
import experiment.model.errors
import experiment.model.storage
import experiment.model.frontends.flowir
import experiment.model.hooks.utils
import experiment.runtime.engine
import experiment.runtime.errors
import experiment.runtime.monitor
import experiment.runtime.optimizer
import experiment.runtime.workflow

import experiment.runtime.utilities.rx

if TYPE_CHECKING:
    from experiment.model.data import Job, Stage, Experiment
    from experiment.runtime.workflow import ComponentState, StageState
    from experiment.runtime.status import StatusDB
    from experiment.model.graph import WorkflowGraph, ComponentSpecification


def TransitionComponentToFinalState(component, exitReason, returncode):
    # type: (ComponentState, Optional[str], Optional[int]) -> None
    '''Based on exitReason will transition component to correct final state

    Note: This function doesn't interrogate the component as to its exit-reason to
    allow over-riding it and handling the case where it doesn't exist'''

    reference = component.specification.reference
    log = logging.getLogger('controller.transitiontofinalstate')

    if exitReason == experiment.model.codes.exitReasons['Success']:
        component.finish(experiment.model.codes.FINISHED_STATE)
        log.info('Component %s completed successfully' % reference)
    elif exitReason in component.specification.workflowAttributes['shutdownOn']:
        component.finish(experiment.model.codes.SHUTDOWN_STATE)
        log.info('Component %s is shutdown because its exitReason is %s' % (reference, exitReason))
    else:
        component.finish(experiment.model.codes.FAILED_STATE)
        log.info('Component %s with exitReason %s and returnCode %s could not be restarted,'
                      ' set to Failed' % (reference, exitReason, returncode))


def WaitOnStability(duration, checkPeriod=30, stableInterval=30):

    '''Waits until system is stable

    If system is stable when called this function returns immediately.
    Otherwise it will wait up to duration seconds for system to become stable.
    
    Params:
        
        duration: How long to wait until system is stable
        checkPeriod: Interval in seconds at which stability is checked
        stableInterval: If there have been no exception in this interval the system is considered stable.
        
    If checkPeriod and stableInterval are the same then the system is stable
    if no new exceptions have occurred in checkPeriod.'''

    log = logging.getLogger("control.controller")
    monitor = experiment.runtime.monitor.MonitorExceptionTracker.defaultTracker()

    limit = int(math.ceil(duration/(1.0*checkPeriod)))
    count = 0

    retval = True

    while count < limit:
        retval = monitor.isSystemStable(stableInterval)
        if retval is False:
            log.info("System is unstable (waited %d secs so far)" % (count*checkPeriod))
            log.info("Will wait %d secs before next stability check" % checkPeriod)
            time.sleep(checkPeriod)
            count += 1
        else:     
            log.info("System appears stable - no new exceptions in last 30 secs")
            break

    log.warning("Waited %d seconds (max %d)" % (count*checkPeriod, limit*checkPeriod))

    return retval

def DummyCompletionCheck(stage, directory):

    return False


class Controller:

    '''Class which runs/coordinates experiment stages'''

    def __init__(self, comp_experiment, do_stage_data=None, do_restart_sources=None, experiment_name=None,
                 enable_optimizer=False, cdb=None, memoization_fuzzy=False):
        # type: (Experiment, Dict[int, bool], Dict[int, bool], str, bool, "ExperimentRestAPI", bool) -> None
        """

        Args:
            comp_experiment: The experiment to orchestrate
            do_stage_data(Dict[int, bool]): Maps stage indices to booleans.
                If True (Default) each components data is staged in.
                None defauls to True.
            do_restart_sources (Dict[int, bool]): Maps stages to bools. Indicates if
                non-repeating engines should be started using restart() instead of run()
                None defaults to False.
        """
        self._memoization_fuzzy = memoization_fuzzy
        if experiment_name is None:
            experiment_name = str(uuid.uuid4())

        tpg = experiment.runtime.utilities.rx.ThreadPoolGenerator
        self.controllerPool = tpg.get_pool(tpg.Pools.Controller)

        # VV: Controller can query the CDB for memoization components of past experimetns; their outputs can be used
        #     in place of executing a component for the current experiment
        self.cdb = cdb  # type: "experiment.service.db.ExperimentRestAPI"

        self.experiment = comp_experiment  # type: Experiment

        self._max_resubmission_attempts = 5
        self.experiment_name = experiment_name
        self.enable_optimizer = enable_optimizer
        self.optimizer_repeat = None  # type: experiment.runtime.optimizer.Optimizer
        self.opt_lock = threading.RLock()

        self.do_stage_data = do_stage_data
        self.do_restart_sources = do_restart_sources

        # VV: Store DoWhile components in this list so that they're not garbage-collected
        #     (ARE THERE ANY OTHER REFS TO THESE OBJECTS?)
        self._instantiated_components = []  # type: List[ComponentState]

        self.currentStage = None  # type: Stage
        self.migratedComponents = []  # type: List[ComponentState]

        self._stageStates = dict()  # type: Dict[int, StageState]
        self.log = logging.getLogger("control.controller")

        self.stop_executing = False

        # VV: These 2 are filled in initialise()
        self.statusDatabase = None
        self._starting_index = None

        # VV: Locks all `comp_` data and makes the Scheduler a critical region
        self.comp_lock = threading.RLock()
        self.comp_staged_in = set()  # type: Set[ComponentState]

        # VV: Finished/Failed/Shutdown components go in here, this lets us only consider a ComponentState to be done
        #     if we have Observed its termination even though the ComponentState might be reporting otherwise.
        #     Note: This set contains the references to the finished components and references to the finished
        #     placeholders
        self.comp_done = set()  # type: Set[str]

        # VV: Identifier of Component producing condition to the DoWhile identifier that's waiting for it
        self.comp_condition_to_dowhile = {}  # type: Dict[str, str]

        # VV: Controller can trigger scheduler when a component resolves its out-deps
        self._event_scheduler = threading.Event()

        #Check does the package provide a completion check hook
        try:
            status = experiment.model.hooks.utils.import_hooks_status(self.experiment.instanceDirectory.hooksDir)
            self.completionCheck = status.IsStageComplete
            self.log.info('Using custom status hook to determine stage completion')
        except (ImportError, FileNotFoundError):
            self.log.log(19, 'No module hook.status found - using internal completion check')
            self.completionCheck = DummyCompletionCheck

        # VV: Book-keeping data to support the Sleep/WakeUp mechanism
        self._scheduler_sleeps = False
        self._start_sleeping = False
        self._component_finished_while_sleeping = []  # type: List[Tuple[Any, ComponentState]]

        self.parse_workflow_graph()

    @property
    def workflowGraph(self):
        # type: () -> WorkflowGraph
        return self.experiment.experimentGraph

    @property
    def graph(self):
        # type: () -> networkx.DiGraph
        return self.workflowGraph.graph

    @property
    def workflowIsComplete(self) -> reactivex.Observable | None:
        """Returns an observable that emits once when the CombinedState of all known Components in the workflow
        completes

        Note:
            This method does not dynamically include components that are created after the method is invoked.

        Returns:
                An observable that emits once after all the CombinedState of all known components complete, if
                there are no observables it returns None
        """

        graph = self.graph
        observables = []
        for name, data in graph.nodes(data=True):
            try:
                component = data['component']()  # type: ComponentState
            except Exception:
                continue
            observables.append(component.combinedStateUpdates)

        if not observables:
            return

        self.log.info(f"Will wait for {len(observables)} observables to complete")
        return reactivex.from_iterable(observables).pipe(
            op.merge_all(),
            op.last_or_default(default_value=None)
        )

    def _is_memoized_candidate_accessible(self, doc):
        # type: (Dict[str, Any]) -> bool
        # VV: For the time being assume that we can only access the outputs of components which have been executed
        #     in the same filesystem as the one that's used by elaunch at the moment. In the future we can ask CDB
        #     if the files are still available.
        doc_location = 'unknown'
        try:
            doc_path = doc['location']
            instance_uri = doc['instance']
            stage_index = doc['stage']
            component_name = doc['name']

            if os.path.isdir(doc_path):
                try:
                    _, any_dirs, any_files = next(os.walk(doc_path, followlinks=False))
                    return (len(any_dirs) > 0) or (len(any_files) > 0)
                except Exception as e:
                    self.log.log(15, "Exception %s while checking whether directory of memoization candidate %s "
                                     "is empty - will assume it's empty" % (e, doc_path))
                    return False

            # VV: Component working directory does not exist locally, check whether we can fetch it via the CDB
            try:
                return self.cdb.cdb_query_component_files_exist(instance_uri, stage_index, component_name)
            except Exception as e:
                # VV: CDB may not have been configured for fetching remote components, don't treat this as an error
                pass

            return False
        except Exception as e:
            self.log.log(15, traceback.format_exc())
            self.log.info("Unable to determine whether %s is accessible due to %s - will assume that it is not." % (
                doc_location, e))
            return False

    def can_memoize(self, component, fuzzy):
        # type: ("experiment.workflow.ComponentState", bool) -> Optional[Dict[str, Any]]
        """Queries centralized database (CDB) for a component whose outputs can be used instead of executing @component.

        Args:
            component(experiment.workflow.ComponentState): Component in current experiment to be memoized

        Returns:
            The matching `component` document description stored on the CDB
        """
        if self.cdb is None:
            return None
        
        memoization = component.specification.componentSpecification.workflowAttributes.get('memoization', {})
        mem_disable = memoization.get('disable', {})
        comp_ref = component.specification.reference

        if fuzzy is False:
            lbl = 'strong'
            field = 'memoization-hash'
        else:
            lbl = 'fuzzy'
            field = 'memoization-hash-fuzzy'

        if mem_disable.get(lbl):
            self.log.info("Component %s forbids %s memoization" % (comp_ref, lbl))
            return None

        if fuzzy is False:
            mem_hash = component.memoization_hash
        else:
            mem_hash = component.memoization_hash_fuzzy

        if mem_hash is not None:
            self.log.info("Looking for %s memoization components for %s:%s=%s" % (
                lbl, comp_ref, mem_hash, pprint.pformat(component.memoization_info)))
            try:
                docs = [doc for doc in self.cdb.cdb_get_document_component(query={field: mem_hash},
                                                 _api_verbose=False)]
            except Exception as e:
                self.log.warning("Unable to fetch %s memoization documents for %s:%s because of %s" % (
                    lbl, comp_ref, mem_hash, e))
                docs = []
            self.log.info("Scanning %d potential candidates for %s memoization of %s:%s" % (
                len(docs), lbl, comp_ref, mem_hash))

            # VV: Pick the most recent component, if we're lucky the instance directories end with a timestamp
            def time_of_instance(doc):
                instance = doc['instance']  # type: str
                if instance.endswith('.instance'):
                    instance = instance[:-9]

                # datetime.datetime.strptime(epoch, "%Y-%m-%dT%H%M%S.%f")
                timestamp_tokens = instance.rsplit('-', 3)
                if len(timestamp_tokens) == 4:
                    timestamp = '-'.join(timestamp_tokens[1:])
                    try:
                        timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H%M%S.%f")
                        return timestamp
                    except Exception:
                        pass

                # VV: can't tell when this instance was generated
                return None

            if docs:
                docs = sorted([d for d in docs if time_of_instance(d) is not None], key=time_of_instance, reverse=True)
                for doc in docs:
                    if self._is_memoized_candidate_accessible(doc):
                        return doc
                self.log.info("Unable to find accessible cached component instance out of %d potential candidates "
                              "for %s memoization of %s:%s" % (len(docs), lbl, comp_ref, mem_hash))
            return None
        else:
            self.log.info("Unable to generate %s memoization hash for %s - will not attempt to memoize" % (
                lbl, comp_ref))
            return None

    def _memoize_populate_component_workdir(self, component, doc):
        # type: ("experiment.workflow.ComponentState", Dict[str, Any]) -> bool
        """Attempts to populate the working directory of a component with the memoized outputs of a past execution of
        some other component.

        Args:
            component(experiment.workflow.ComponentState): Component to replace with memoized outputs of past execution
              of other component
            doc(Dict[str, Any]): Document description of memoized component

        Returns: Success (True), Failure (False)
        """
        try:
            output_dir = component.specification.directory

            if os.path.isdir(doc['location']):
                # VV: Component working directory is found locally
                distutils.dir_util.copy_tree(doc['location'], output_dir, update=0)
                self.log.info("Populated working directory of %s from memoized local component %s" % (
                    component.specification.reference, doc['location']))
            else:
                # VV: Need to fetch the files via the CDB
                instance_uri = doc['instance']
                stage_index = doc['stage']
                component_name = doc['name']
                remote_location = os.path.join(instance_uri, 'stages', 'stage%d' % stage_index, component_name)
                self.cdb.cdb_download_component_files(instance_uri, stage_index, component_name, output_dir)
                self.log.info("Populated working directory of %s from memoized remote component %s" % (
                    component.specification.reference, remote_location))
        except Exception as e:
            self.log.warning("Failed to populate working directory of %s because of %s - will not memoize" % (
                component.specification.reference, e
            ))
            return False

        return True

    def parse_workflow_graph(self):
        # type: () -> None
        # VV: There have been changes to the workflowGraph, need to re-parse documents (i.e. DoWhile)
        with self.comp_lock:
            self.log.info("Updating WorkflowGraph")
            # VV: First, discover any DoWhile documents and register their latest conditions
            self.comp_condition_to_dowhile.clear()

            FlowIR = experiment.model.frontends.flowir.FlowIR
            dowhile_docs = self.workflowGraph._documents.get(FlowIR.LabelDoWhile, {})

            for dw_name in dowhile_docs:
                try:
                    dw_meta = dowhile_docs[dw_name]
                    doc = dw_meta['document']  # type: Dict[str, Any]
                    state = dw_meta['state']  # type: Dict[str, Any]
                    ref_condition = state['currentCondition']  # type: str
                    cond_stage, cond_name, _, _ = FlowIR.ParseDataReferenceFull(ref_condition, doc['stage'])
                    self.comp_condition_to_dowhile['stage%d.%s' % (cond_stage, cond_name)] = dw_name
                except Exception:
                    msg = "Failed to register DoWhile condition for document %s" % dw_name
                    self.log.critical(msg)
                    raise_with_traceback(ValueError(msg))

            try:
                for comp_name in networkx.topological_sort(self.graph):
                    data = self.graph.nodes[comp_name]
                    stage_index = data['stageIndex']
                    if stage_index not in self._stageStates:
                        self._stageStates[stage_index] = experiment.runtime.workflow.StageState(stage_index)

                    if stage_index < (self._starting_index or 0):
                        continue

                    # VV: The node should have a component
                    try:
                        component = data['component']()
                    except Exception as e:
                        msg = ("Node %s was expected to have a valid ComponentState. "
                               "Error: %s" % (comp_name, e))
                        self.log.critical(msg)
                        raise_with_traceback(ValueError(msg))

                    if component not in self._stageStates[stage_index].componentStates:
                        self._stageStates[stage_index].addComponentState(component)
            except networkx.NetworkXUnfeasible as e:
                try:
                    cycle = networkx.find_cycle(self.graph)
                except Exception:
                    pass
                else:
                    self.log.critical("Cycle in graph: %s" % ['->'.join(edge) for edge in cycle])
                raise_with_traceback(e)

    def get_node_state(self, node_name):
        # type: (str) -> str
        """Returns the experiments.code.*_STATE of a node in the graph
        """
        with self.comp_lock:
            if node_name in self.workflowGraph._placeholders:
                return self.get_placeholder_state(node_name)

            data = self.graph.nodes[node_name]
            try:
                component = data['component']()  # type: ComponentState
            except Exception:
                self.log.critical("Node %s was expected to have a valid ComponentState" % node_name)
                raise

            # VV: `done` Component states are *only* taken into account after we've observed them through
            #     finishedCheck() or explicitly marked them as done
            if component.specification.reference not in self.comp_done:
                return experiment.model.codes.RUNNING_STATE

            return component.state

    def get_placeholder_state(self, node_name):
        # type: (str) -> str
        """Return state of placeholder node.

        A placeholder is done when all of the components it represents are done AND the latest condition of the
        enveloping DoWhile has evaluated to False.

        Its state is experiment.codes.RUNNING_STATE when it's not Done.
        If the component which produces the most recent iteration terminated AND all components represented by this
        placeholder have terminated:
        - If the condition component completed successfully AND it generated a valid condition reference
          (i.e. one that resolves to`True`/`False`) then this method echos the state of the represented looped-component
          in the most recent iteration.
        - Otherwise, this method reports that the placeholder has the experiments.codes.FAILED_STATE
        """
        with self.comp_lock:
            try:
                placeholder = self.workflowGraph._placeholders[node_name]
            except KeyError:
                msg = "Cannot find placeholder \"%s\"" % node_name
                raise_with_traceback(ValueError(msg))

            do_while_id = placeholder['DoWhileId']  # type: str
            represents = placeholder['represents']  # type: List[str]
            latest = placeholder['latest']  # type: str

            FlowIR = experiment.model.frontends.flowir.FlowIR
            try:
                dw_metadata = self.workflowGraph.get_document_metadata(FlowIR.LabelDoWhile, do_while_id)
            except Exception:
                msg = "Failed to locate DoWhile document %s" % do_while_id
                raise_with_traceback(ValueError(msg))

            state = dw_metadata.get('state')  # type: Dict[str, Any]

            if state is None:
                msg = "Could not get state of DoWhile %s" % do_while_id
                raise ValueError(msg)

            try:
                current_condition = state['currentCondition']
            except Exception:
                msg = "Could not get currentCondition for DoWhile %s" % (do_while_id)
                raise ValueError(msg)

            condition_dr = experiment.model.graph.DataReference(current_condition, dw_metadata['document']['stage'])
            cond_prod = condition_dr._producerIdentifier
            represents_done = not any(map(self.node_is_active, represents))

            cond = self.get_compstate(cond_prod.identifier)
            if cond.state in [experiment.model.codes.SHUTDOWN_STATE, experiment.model.codes.FAILED_STATE]:
                # VV: The last condition will never produce a valid condition, if all represented looped-components
                # have finished then report that the placeholder is failed, otherwise report that the placeholder
                # is still running
                if represents_done:
                    return experiment.model.codes.FAILED_STATE
                return experiment.model.codes.RUNNING_STATE

            try:
                do_next_iter = condition_dr.resolve(self.workflowGraph)
            except Exception as e:
                # VV: The file has not been generated yet OR there was a problem resolving it, this can mean 2 things:
                # 1. The Condition has not finished i.e. may generate the condition anytime now
                # 2. The Condition Component has terminated and has NOT generated the condition i.e. it will never
                #    generate it
                if cond.state == experiment.model.codes.FINISHED_STATE:
                    if represents_done:
                        return experiment.model.codes.FAILED_STATE
                return experiment.model.codes.RUNNING_STATE
            else:
                do_next_iter = do_next_iter.strip().lower()
                self.log.log(14, "Resolved %s to \"%s\" for placeholder %s" % (
                    current_condition, do_next_iter, node_name
                ))

            if represents_done:
                if do_next_iter == 'false':
                    # VV: DoWhile is done, echo the state of most recent represented component in the loop
                    return self.get_node_state(latest)
                elif do_next_iter not in ['false', 'true']:
                    return experiment.model.codes.FAILED_STATE
        return experiment.model.codes.RUNNING_STATE

    def get_components_in_stage(self, stage_index, return_done_too=False):
        # type: (int, bool) -> List[ComponentState]
        with self.comp_lock:
            try:
                cycle = networkx.find_cycle(self.graph)
            except Exception:
                pass
            else:
                self.log.critical("Cycle in graph: %s" % ['->'.join(edge) for edge in cycle])

            ret = []

            if return_done_too is False and stage_index < self._starting_index:
                return []

            for comp_name, data in self.graph.nodes(data=True):
                if data['stageIndex'] == stage_index:
                    try:
                        component = data['component']()
                    except Exception as e :
                        msg = "Node %s was expected to have a valid ComponentState" % comp_name
                        self.log.info(msg)

                        # VV: We are aware that the Controller has ran into a problem, keep on trying to
                        #     terminate gracefully
                        if self.stop_executing is False:
                            raise ValueError(msg)
                    else:
                        ret.append(component)

        return ret

    @property
    def components(self):
        # type: ()-> List[ComponentState]
        if self.currentStage is not None:
            stage_index = self.currentStage.index
        else:
            stage_index = 0

        return self.get_components_in_stage(stage_index)

    def _true_nodes_from_identifiers(self, names, only_latest_looped=True):
        # type: (List[str], bool) -> Dict[str, List[Dict[str, Any]]]
        """Returns node names associated with Component identifiers (identifiers can even be Placeholders)

        Placeholder identifiers can be resolved to their latest instance, or all of their instances based on the
        parameter `only_latest_looped`.

        The return value is a Dictionary which maps an identifier to a list of node names for self.graph
        """
        ret = {}  # type: Dict[str, List[Dict[str, Any]]]

        with self.comp_lock:
            for name in names:
                if self.graph.has_node(name):
                    data = self.graph.nodes[name]
                    ret[name] = [data]
                elif name in self.workflowGraph._placeholders:
                    data = self.workflowGraph._placeholders[name]

                    if only_latest_looped:
                        refs = [data['latest']]
                    else:
                        refs = list(data['represents'])

                    # VV: Map names of looped nodes to the data that they contain
                    ret[name] = [self.graph.nodes[loop_name] for loop_name in refs]
                else:
                    raise NotImplementedError("Cannot extract true node from networkx node %s:%s" % (
                        name, data
                    ))

        return ret

    @property
    def is_sleeping(self):
        return self._scheduler_sleeps

    def sleep(self):
        # VV: Signal to the scheduler that the controller should transition to the `sleeping` state
        with self.comp_lock:
            self._start_sleeping = True

    def wake_up(self):
        # VV: Wake up the scheduler and take care of any events that it received while it was sleeping before
        #     it transitions to the `awake` state
        #     The self._scheduler() method will update the state of the Controller to `awake` once it runs

        with self.comp_lock:
            finished = list(self._component_finished_while_sleeping)
            self.log.log(19, "%d components finished while I was sleeping" % len(finished))
            self._component_finished_while_sleeping = []
            self._start_sleeping = False

            for state, comp in finished:
                self.finishedCheck(state, comp)

    def disable_optimizer(self, which):
        #  type: (Union[ComponentState, experiment.runtime.engine.Engine, List[str], None]) -> None
        if isinstance(which, experiment.runtime.workflow.ComponentState):
            self.log.warning("Disabling optimizer because of error in ComponentState %s" %
                              which.specification.reference)
        elif isinstance(which, experiment.runtime.engine.Engine):
            self.log.warning("Disabling optimizer because of error in Engine %s" %
                              which.job.reference)
        elif which is not None:
            self.log.warning("Disabling optimizer because %s" % which)
        else:
            self.log.warning("Disabling optimizer because of error(s) in (?)" )

        with self.opt_lock:
            self.enable_optimizer = False

            for node, data in self.graph.nodes(data=True):
                try:
                    comp = data['component']()
                except Exception:
                    continue
                else:
                    if comp.specification.isRepeat:
                        comp.optimizer_disable(False)

    def _stopComponents(self, components, stop_optimizer):
        # type: (List[ComponentState], bool) -> None
        '''Private: Send finish to all 'alive' components

        Does not send if finish() has already been called
        Subsequent calls to this method while the same stage is executing do nothing

        If component.finish() raises an exception its noted but no exception is raised
        All components state are set to experiment.codes.FAILED_STATE'''

        if stop_optimizer and self.enable_optimizer and self.optimizer_repeat:
            self.disable_optimizer(None)

        #Set state of any components which haven't had this done
        with self.comp_lock:
            for component in components:
                if component.isAlive() and not component.finishCalled:
                    self.log.warning('Stopping component %s' % component.specification.reference)
                    try:
                        #This set component.finishCalled to True
                        #Can be used to filter if a POSTMORTEM transition should be handled
                        component.finish(experiment.model.codes.SHUTDOWN_STATE)
                    except Exception as error:
                        self.log.warning('Unexpected exception when stopping componet  %s: %s' % (
                            component.specification.reference, str(error)
                        ))

    def register_component_with_optimizer(self, comp):
        # type: (ComponentState) -> None
        with self.opt_lock:
            if self.enable_optimizer and self.optimizer_repeat:
                walltime = comp.engine.job.resourceManager['config']['walltime'] * 60
                options = comp.engine.job.resourceRequest
                options['walltime'] = walltime

                options_floats = {
                    'exploitChance': None,
                    'exploitTarget': None,
                    'exploreTargetLow': None,
                    'exploreTargetHigh': None,
                }

                actual_options = comp.specification.workflowAttributes['optimizer']

                for key in options:
                    if key in actual_options:
                        options[key] = int(actual_options[key])

                for key in options_floats:
                    if key in actual_options:
                        options[key] = float(actual_options[key])
                    else:
                        options[key] = options_floats[key]
                try:
                    if comp.specification.isRepeat:
                        repeat_interval = comp.specification.repeatInterval()
                    else:
                        repeat_interval = None

                    self.optimizer_repeat.register_component(
                        'stage%d' % comp.stageIndex,
                        comp.name, comp.specification.type,
                        comp.specification.componentSpecification.commandDetails['executable'],
                        repeat_interval,
                        options['ranksPerNode'],
                        options['numberProcesses'],
                        options['numberThreads'],
                        options['walltime'],
                        datetime.datetime.now(),
                        comp.producer_names,
                        options['exploitChance'],
                        options['exploitTarget'],
                        options['exploreTargetLow'],
                        options['exploreTargetHigh']
                    )

                    comp.optimizer_enable(self.optimizer_repeat, self.disable_optimizer)
                except Exception:
                    import traceback
                    self.log.critical("Could not register %s with optimizer. "
                                      "Will disable optimizer for everyone. EXCEPTION:%s" % (
                                          comp.specification.reference,
                                          traceback.format_exc()
                                      ))
                    self.disable_optimizer(comp)

    def node_is_active(self, node_name):
        # type: (str) -> bool
        """A component is considered active till Controller observes it in finishedCheck()
        """
        return node_name not in self.comp_done

    def _get_placeholder_nodes_in_stage(self, stage_idx):
        with self.comp_lock:
            ret = []  # type: List[str]
            placeholders = self.workflowGraph._placeholders
            for name in placeholders:
                p_stage, p_comp, _ = experiment.model.frontends.flowir.FlowIR.ParseProducerReference(name)
                if p_stage == stage_idx:
                    ret.append(name)
            return ret

    def generate_status_report_for_nodes(self, components=None, filter_done=False):
        # type: (Optional[List[str]], bool) -> str
        with self.comp_lock:
            if components is None:
                components = []
                try:
                    for node_name in networkx.topological_sort(self.graph):
                        components.append(node_name)
                except networkx.NetworkXUnfeasible as e:
                    try:
                        cycle = networkx.find_cycle(self.graph)
                    except Exception:
                        pass
                    else:
                        self.log.critical("Cycle in graph: %s" % ['->'.join(edge) for edge in cycle])
                    raise_with_traceback(e)
                for placeholder_name in self.workflowGraph._placeholders:
                    components.append(placeholder_name)

            if filter_done:
                components = list(filter(self.node_is_active, components))

            messages = ['<Component(s) report>']
            for comp_name in components:
                try:
                    active_predecessors = self._comp_get_active_predecessors(comp_name)

                    active_producers = active_predecessors['producers']
                    active_subjects = active_predecessors['subjects']

                    comp_state = self.get_node_state(comp_name)
                except Exception as e:
                    if self.stop_executing:
                        messages.append('  < Incomplete information for %s >' % (comp_name))
                        continue
                    else:
                        raise_with_traceback(e)

                # VV: Prefix subjects with (*) which looks like an eye
                active_subjects = ['(*)%s' % name for name in active_subjects]
                agg_inputs = ''.join(('[', ', '.join(active_producers + active_subjects), ']'))

                if self.graph.has_node(comp_name):
                    if comp_state == experiment.model.codes.RUNNING_STATE:
                        try:
                            # VV: Flow treats alive components as `runnign` which makes it hard to tell which
                            # of the components have actually began their execution
                            comp = self.get_compstate(comp_name)
                            if comp not in self.comp_staged_in:
                                comp_state = 'pending'
                        except Exception:
                            pass

                    if comp_name not in self.comp_condition_to_dowhile:
                        messages.append('  %s[%s] inputs: %s' % (comp_name, comp_state, agg_inputs))
                    else:
                        messages.append('C:%s[%s] inputs: %s' % (comp_name, comp_state, agg_inputs))
                else:
                    messages.append('P:%s[%s] inputs: %s' % (comp_name, comp_state, agg_inputs))

        messages.append('<\\Component(s) report, (*) indicates Subject->Observer dependency, '
                        'P: a placeholder, '
                        'C: the latest condition of a DoWhile>')

        return '\n'.join(messages)


    def get_compstate(self, name):
        # type: (str) -> ComponentState
        return self.graph.nodes[name]['component']()

    def _fake_finish_with_state(self, component, new_state):
        # type: (ComponentState, str) -> None
        """Component is immediately shutdown before ever running because its producers are SHUTDOWN.

        This method also sets up 2 on_error() handlers for notifyFinished and notifyPostMortem which look
        for SequenceContainsNoElementError exceptions. These exceptions are treated as warnings instead of
        errors. When they are raised the handlers simply warn the user and then evoke finishedCheck() or
        postMortemCheck()
        """

        def on_error_notifyFinished(comp):
            # type: (experiment.runtime.workflow.ComponentState) -> Any
            def handler_notifyFinished(err):
                name = comp.specification.reference
                if isinstance(err, reactivex.internal.exceptions.SequenceContainsNoElementsError):
                    self.log.warning("Component %s did not emit updates (fake_finish to %s" % (name, new_state))
                    self.finishedCheck(comp.state, comp)
                else:
                    self.handleError(err, 'fake::notifyFinished(%s, %s)' % (name, new_state))

            name = comp.specification.reference
            handler_notifyFinished = experiment.runtime.utilities.rx.report_exceptions(
                handler_notifyFinished, self.log, 'ErrorOfNotifyFinished(%s)' % name)

            return handler_notifyFinished

        def on_error_notifyPostMortem(comp):
            # type: (experiment.runtime.workflow.ComponentState) -> Any
            def handler_notifyPostMortem(err):
                name = comp.specification.reference
                if isinstance(err, reactivex.internal.exceptions.SequenceContainsNoElementsError):
                    self.log.warning("Component %s did not emit updates (fake_finish to %s" % (name, new_state))
                    self.postMortemCheck(comp.state, comp)
                else:
                    self.handleError(err, 'fake::notifyFinished(%s, %s)' % (name, new_state))

            name = comp.specification.reference
            handler_notifyPostMortem = experiment.runtime.utilities.rx.report_exceptions(
                handler_notifyPostMortem, self.log, 'ErrorOfNotifyPostMortem(%s)' % name)

            return handler_notifyPostMortem

        self.statusDatabase.monitorComponent(component)

        # VV: Mark the component as SHUTDOWN and then trigger the engine and component state
        #     shutdown sequence which will eventually trigger self.finishedCheck()
        name = component.specification.reference
        finished_check = experiment.runtime.utilities.rx.report_exceptions(
            lambda e: self.finishedCheck(*e), log=self.log, label='FakeFinish-Finish(%s)' % name)

        postmortem_check = experiment.runtime.utilities.rx.report_exceptions(
            lambda e: self.postMortemCheck(*e), log=self.log, label='FakeFinish-PostMortem(%s)' % name)

        component.notifyFinished \
            .pipe(
                op.observe_on(self.controllerPool)
            ).subscribe(on_next=finished_check, on_error=on_error_notifyFinished(component))

        component.notifyPostMortem \
            .pipe(
                op.observe_on(self.controllerPool),
                op.filter(lambda e: e[1].finishCalled is False)
            ).subscribe(on_next=postmortem_check, on_error=on_error_notifyPostMortem(component))
        self.comp_staged_in.add(component)
        component.finish(new_state)

    def _comp_get_active_predecessors(self, component):
        # type: (Union[str, ComponentState]) -> Dict[str, List[str]]

        if isinstance(component, experiment.runtime.workflow.ComponentState):
            comp_is_repeat = component.specification.workflowAttributes['isRepeat']
            comp_stage_idx = component.stageIndex
            node_name = component.specification.reference
            predecessors = list(self.graph.predecessors(node_name))
        else:
            try:
                component = self.get_compstate(component)
            except Exception:
                # VV: This node does not come with a ComponentState object, we can
                # only handle placeholders at this point
                FlowIR = experiment.model.frontends.flowir.FlowIR
                try:
                    data = self.workflowGraph._placeholders[component]  # type: Dict[str, Union[str, List[str]]]
                except Exception as e:
                    msg = "Component %s is neither a known component, nor a placeholder." % component
                    if '#' in component:
                        msg += " It looks like a DoWhile instantiation failed, check the logs for more information"

                    self.log.critical(msg)
                    if self.stop_executing is False and '#' not in component:
                        raise_with_traceback(ValueError(msg))
                    else:
                        # VV: Ensure that this stage will not be scheduled
                        self.stop_executing = True
                        # VV: The Controller is currently trying to shutdown due to an error
                        return {
                            'producers': [],
                            'subjects': [],
                        }
                    # VV: makes the linter happy, even though raise will never be called
                    raise
                else:
                    data = experiment.model.frontends.flowir.deep_copy(data)

                node_name = component
                # VV: Placeholders can never be repeating
                comp_is_repeat = False
                comp_stage_idx, _, _ = experiment.model.frontends.flowir.FlowIR.ParseProducerReference(node_name)

                # VV: Placeholders have dependencies to the components that they represent and the component which
                #     produces the `condition` to determine whether the loop terminates or not
                predecessors = data['represents']

                try:
                    dw_doc = self.workflowGraph.get_document_metadata(FlowIR.LabelDoWhile, data['DoWhileId'])
                    current_condition = dw_doc['state']['currentCondition']
                except KeyError:
                    msg = "Could not access DoWhile document %s for placeholder %s" % (data['DoWhileId'], node_name)
                    self.log.critical(msg)
                    raise_with_traceback(ValueError(msg))
                    raise
                FlowIR = experiment.model.frontends.flowir.FlowIR
                application_dependencies = self.experiment.experimentGraph.configuration.get_application_dependencies()
                special_folders = self.experiment.instanceDirectory.top_level_folders
                stage_idx, producer, _, _ = FlowIR.ParseDataReferenceFull(
                    current_condition, application_dependencies=application_dependencies,
                    special_folders=special_folders)

                if stage_idx is None:
                    msg = ("Could not extract stage index from condition reference \"%s\" for placeholder %s with "
                           "DoWhileId %s" % (current_condition, node_name, data['DoWhileId']))
                    self.log.critical(msg)
                    raise_with_traceback(ValueError(msg))
                    raise
                cond_ref = 'stage%d.%s' % (stage_idx, producer)
                if cond_ref not in predecessors:
                    predecessors.append(cond_ref)
            else:
                comp_is_repeat = component.specification.workflowAttributes['isRepeat']
                comp_stage_idx = component.stageIndex
                node_name = component.specification.reference
                predecessors = list(self.graph.predecessors(node_name))

        ret = {}

        active_predecessors = [name for name in predecessors if self.node_is_active(name)]

        FlowIR = experiment.model.frontends.flowir.FlowIR
        ret['producers'] = [p_name for p_name in active_predecessors if comp_is_repeat is False or
                                                 FlowIR.ParseProducerReference(p_name)[0] != comp_stage_idx]
        ret['subjects'] = [p_name for p_name in active_predecessors if comp_is_repeat is True and
                                                 FlowIR.ParseProducerReference(p_name)[0] == comp_stage_idx]

        return ret

    def _input_dependencies_satisfied(self, component):
        # type: (ComponentState) -> bool
        with self.comp_lock:
            active_predecessors = self._comp_get_active_predecessors(component)
            active_producers = active_predecessors['producers']

            if len(active_producers) > 0:
                return False

            active_subjects = active_predecessors['subjects']
            # VV: A Subject->Observer dependency is satisfied IFF the Subject is staged-in
            for subject_name in active_subjects:
                try:
                    comp = self._true_nodes_from_identifiers([subject_name])[subject_name][0]['component']()
                except Exception as e:
                    self.log.critical("Could not get componentState of input %s to %s. Error: %s" % (
                        subject_name, component, str(e)
                    ))
                    raise_with_traceback(e)

                if comp not in self.comp_staged_in:
                    return False

            return True

    def _schedule(self, migrated_components):
        # type: (Set[ComponentState]) -> None
        """Recursively discovers components with 0 pending dependencies and promotes them to Submitted

        If a pending consumer has at least 1 producer, and all of its producers are in the SHUTDOWN_STATE then the
        consumer is put down (SHUTDOWN_STATE) as well.
        """
        done_states = [
            experiment.model.codes.FINISHED_STATE,
            experiment.model.codes.FAILED_STATE,
            experiment.model.codes.SHUTDOWN_STATE
        ]

        with self.comp_lock:
            if self._start_sleeping:
                self._scheduler_sleeps = True

            ready = []

            with self.comp_lock:
                for node_name, data in self.graph.nodes(data=True):
                    if node_name in self.comp_done:
                        continue

                    try:
                        comp = self.get_compstate(node_name)
                        comp_name = comp.specification.reference
                    except ValueError:
                        # VV: it's conceivable that not all of the nodes will come with a ComponentState when
                        #     restarting
                        self.log.info("Skipping %s, no ComponentState" % node_name)
                        continue

                    if comp.state in done_states \
                            or comp in self.comp_staged_in or\
                            self._input_dependencies_satisfied(comp) is False:
                        continue

                    # VV: This component has no pending dependencies to other components; there're 3 scenarios:
                    # 1. Shutdown an aggregating component if any of its non-replicated predecessors is shutdown
                    #    OR all of its replicated predecessors are shutdown
                    # 2. Shutdown non-aggregating if at least one of its predecessors is shutdown
                    # 3. Schedule a component if all of its inputs to predecessors are satisfied if (1) and (2) do
                    #    not apply (checks have already been performed by _input_dependencies_satisfied(comp))

                    producer_names = self.graph.predecessors(comp_name)
                    producer_nodes = self._true_nodes_from_identifiers(producer_names, True)
                    try:
                        dependencies = [producer_nodes[prod][0]['component']() for prod in producer_nodes]
                    except Exception as e:
                        self.log.critical("Failed to get ComponentState of dependencies %s for %s" % (
                            producer_nodes, node_name))
                        raise_with_traceback(e)
                        raise

                    is_aggregate = (comp.specification.componentSpecification.isAggregating
                                    or comp.specification.componentSpecification.isAggregatingLoopedNodes)

                    producers_failed = [pr for pr in dependencies if pr.state == experiment.model.codes.FAILED_STATE]
                    SHUTDOWN_STATE = experiment.model.codes.SHUTDOWN_STATE

                    if producers_failed:
                        self.log.warning("Consumer %s will be moved to SHUTDOWN because of its FAILED producers %s" % (
                                comp_name, [p.specification.reference for p in producers_failed]))
                        self._fake_finish_with_state(comp, SHUTDOWN_STATE)
                    elif is_aggregate:
                        looped_producers = [x.specification.reference for x in dependencies
                                            if x.specification.componentSpecification.isLooping]

                        def producer_is_aggregated(prod):
                            # type: (experiment.runtime.workflow.ComponentState) -> bool
                            """Returns whether a Component is aggregated because it's either a replica OR looping"""
                            if prod.specification.reference in looped_producers:
                                return True
                            return prod.specification.componentSpecification.isReplicating

                        tp = experiment.runtime.workflow.ComponentState
                        replica_inputs = [p for p in dependencies if producer_is_aggregated(p)]
                        non_replica_inputs = [p for p in dependencies if p not in replica_inputs]
                        shutdown_replicas = [p for p in replica_inputs if cast(tp, p).state == SHUTDOWN_STATE]
                        shutdown_non_replicas = [p for p in non_replica_inputs if cast(tp, p).state == SHUTDOWN_STATE]

                        if shutdown_non_replicas:
                            self.log.warning("Aggregating component %s will shutdown because of SHUTDOWN "
                                             "non-replicated inputs %s" % (
                                comp_name, [p.specification.reference for p in shutdown_non_replicas]))
                            self._fake_finish_with_state(comp, SHUTDOWN_STATE)
                        elif replica_inputs and len(shutdown_replicas) == len(replica_inputs):
                            self.log.warning("Aggregating component %s will shutdown because all of its replicated "
                                             "inputs %s are SHUTDOWN" % (
                                comp_name, [p.specification.reference for p in shutdown_replicas]))
                            self._fake_finish_with_state(comp, SHUTDOWN_STATE)
                        else:
                            ready.append(comp)
                    else:
                        producers_shutdown = [pr for pr in dependencies if pr.state == SHUTDOWN_STATE]
                        if producers_shutdown:
                            self.log.warning(
                                "Non Aggregating component %s will shutdown because of SHUTDOWN inputs %s" % (
                                    comp_name, [p.specification.reference for p in producers_shutdown]))
                            self._fake_finish_with_state(comp, SHUTDOWN_STATE)
                        else:
                            ready.append(comp)

            if not self.stop_executing:
                self.finalize_submit_components(ready)

            if self._start_sleeping is False:
                self._scheduler_sleeps = False

    def finalize_submit_components(self, sched_order):
        # type: (List[ComponentState]) -> None
        """"""

        if sched_order:
            # VV: Make sure the system is stable
            WaitOnStability(120)
            msg = ["<Check for promotions>"]
            for comp in sched_order:
                msg.append('READY %s in stage %d' % (comp.specification.reference, self.stage().index))
            msg.append("<\\Check for promotions>")
            self.log.info('\n'.join(msg))
        else:
            return

        try:
            staged_in = []  # type: List[ComponentState]
            remaining = list(sched_order)
            # VV: Stage the components
            while remaining:
                if self._start_sleeping:
                    comp_names = [comp.specification.reference for comp in remaining]
                    self.log.log(19, "Postpone the submission of components %s till after I wake up" % (
                        comp_names
                    ))
                    break

                comp = remaining.pop(0)

                if self.stop_executing:
                    self.log.warning(
                        "Controller will not execute any more, %s will be reported as SHUTDOWN" %
                        comp.specification.reference)
                    self._fake_finish_with_state(comp, experiment.model.codes.SHUTDOWN_STATE)
                    continue

                memoized_doc = self.can_memoize(comp, False)

                if memoized_doc is None and self._memoization_fuzzy:
                    # VV: No hard-memoization component candidates, check if there're any soft ones
                    memoized_doc = self.can_memoize(comp, True)

                if memoized_doc:
                    self.log.info("Can memoize %s:%s using stage%s.%s from experiment %s" % (
                        comp.specification.reference, comp.memoization_hash,
                        memoized_doc['stage'], memoized_doc['name'], memoized_doc['instance']))
                    if self._memoize_populate_component_workdir(comp, memoized_doc):
                        self.log.info("Reporting that %s finished successfully - was actually memoized" % (
                            comp.specification.reference
                        ))
                        self._fake_finish_with_state(comp, experiment.model.codes.FINISHED_STATE)
                        continue

                stage_in = self.do_stage_data is None or self.do_stage_data[comp.stageIndex]
                if comp.isStaged is True:
                    name = comp.specification.reference
                    raise NotImplementedError("Component %s is already staged!" % name)
                try:
                    self.log.info("Staging in component %s" % comp.specification.reference)
                    comp.stageIn(stageData=stage_in)
                except experiment.model.errors.DataReferenceFilesDoNotExistError as e:
                    self.log.warning("Component %s failed to stage in due to DataReference errors %s."
                                     " Will be considered FAILED" % (
                                    comp.specification.reference, str(e.referenceErrors)))
                    self._fake_finish_with_state(comp, experiment.model.codes.FAILED_STATE)
                    continue
                except Exception as e:
                    self.log.warning("Failed to stage in %s because of %s" % (
                        comp.specification.reference, e))
                    raise

                staged_in.append(comp)
                self.comp_staged_in.add(comp)

                if not stage_in:
                    comp.specification.isStaged = True

                self.statusDatabase.monitorComponent(comp)

                # FIXME: HACK - see handleMigration docs of HybridConfiguration class
                experiment.appenv.HybridConfiguration.defaultConfiguration(). \
                    handleMigration(comp.specification, experiment)

            if not staged_in:
                return

            if self.enable_optimizer and self.optimizer_repeat is not None:
                # VV: Start listening for events
                engines = [c.engine for c in staged_in]  # type: List[experiment.runtime.engine.Engine]

                def safe_observe(state, engine):
                    # type: (Dict[str, Any], experiment.runtime.engine.Engine) -> None
                    try:
                        return self.observe_engine_change(state, engine)
                    except Exception:
                        self.log.critical("Could not observe state update of engine %s. "
                                          "Will disable optimizer. EXCEPTION: %s" % (
                                              engine.job.reference, traceback.format_exc()
                                          ))
                        self.disable_optimizer(engine)

                safe_observe = experiment.runtime.utilities.rx.report_exceptions(safe_observe, log=self.log, label='SafeObserve')
                def on_error_stateUpdates(engines):
                    names = [e.job.reference for e in engines]

                    def handle_stateUpdates(err):
                        if isinstance(err, reactivex.internal.exceptions.SequenceContainsNoElementsError):
                            self.log.warning("Engines %s did not emit updates. Will disable optimizer" % names)
                        else:
                            self.log.warning("Error %s when observing updates of %s" % (err, names))

                        # VV: There is something wrong when observing engine updates, disable the optimizer
                        self.disable_optimizer(names)

                    handle_stateUpdates = experiment.runtime.utilities.rx.report_exceptions(handle_stateUpdates, log=self.log,
                                                                         label='HandleStateUpdates')
                    return handle_stateUpdates

                reactivex.merge(*[e.stateUpdates for e in engines]).pipe(
                    op.observe_on(self.controllerPool),
                    op.take_while(lambda x: self.enable_optimizer)
                ).subscribe(
                    on_next=lambda state_engine: safe_observe(state_engine[0], state_engine[1]),
                    on_error=on_error_stateUpdates(engines))

            # VV: When a task of a Subject terminates successfully ping the downstream Observers
            def push_notification(consumer):
                #  type: (ComponentState) -> Callable[[Any], None]
                def check_for_push_notification(msg, consumer=consumer):
                    state_dict, engine = msg

                    # VV: The push-notification system targets Subjects, not Producers
                    if engine.job.stageIndex < consumer.stageIndex:
                        return

                    if ('lastTaskFinishedDate' in state_dict
                            and 'lastTaskExitCode' in state_dict
                            and state_dict['lastTaskExitCode'] == 0):
                        rep_engine = cast(experiment.runtime.engine.RepeatingEngine, consumer.engine)
                        rep_engine.notify_producer_successful_run(engine.job.reference)

                check_for_push_notification = experiment.runtime.utilities.rx.report_exceptions(
                    check_for_push_notification, log=self.log, label='PushNotification')

                return check_for_push_notification

            def on_error_pushNotification(consumer, producers):
                # type: (ComponentState, List[ComponentState]) -> Callable[[Any], Any]
                names = [prod.engine.job.reference for prod in producers]

                def handle_pushNotification_error(err):
                    if isinstance(err, reactivex.internal.exceptions.SequenceContainsNoElementsError):
                        # VV: Even though having an empty sequence error while observing subjects is weird
                        #     there's a chance that said subjects finished before they ever ran so it might be OK

                        self.log.warning("Observer %s will never receive any push notification from its "
                                         "producers %s. Will identify if any Subjects have finished" % (
                            consumer.specification.reference, names
                        ))

                        for prod in producers:
                            if prod.stageIndex == consumer.stageIndex and prod.isAlive() is False:
                                rep_engine = cast(experiment.runtime.engine.RepeatingEngine, consumer.engine)
                                rep_engine.notify_producer_successful_run(prod.specification.reference)
                                self.log.log(19, "At least one of the subjects has finished will not inspect"
                                                 "any other.")
                                break
                    else:
                        # VV: Any error other than empty sequence is definitely an issue
                        self.handleError(err, 'pushNotification(%s, %s)' % (
                            consumer.specification.reference, names
                        ))

                handle_pushNotification_error = experiment.runtime.utilities.rx.report_exceptions(
                    handle_pushNotification_error, log=self.log, label='HandlePushNotificationError')
                return handle_pushNotification_error

            for consumer in staged_in:
                if consumer.specification.isRepeat:
                    producers = [p for p in consumer.producers if p.isAlive() and p.engine is not None]

                    if producers:
                        reactivex.merge(*[prod.engine.stateUpdates for prod in producers])\
                            .pipe(op.observe_on(self.controllerPool))\
                            .subscribe(
                                on_next=push_notification(consumer),
                                on_error=on_error_pushNotification(consumer, producers))

            comp_names = [c.specification.reference for c in staged_in]

            # VV: Staged-in components should emit observables so treat empty sequences as errors

            # VV: Wait till components are finished to resolve their output anti-dependencies
            finished_check = experiment.runtime.utilities.rx.report_exceptions(
                lambda e: self.finishedCheck(*e), log=self.log, label='FinishedCheck')

            postmortem_check = experiment.runtime.utilities.rx.report_exceptions(
                lambda e: self.postMortemCheck(*e), log=self.log, label='PostMortemCheck')

            handle_error_finished = experiment.runtime.utilities.rx.report_exceptions(
                lambda e: self.handleError(
                    e, 'finalize_submit_components:finished_check(%s)' % comp_names),
                self.log, label='HandleErrorFinishedCheck(%s)' % comp_names
            )

            handle_error_postmortem = experiment.runtime.utilities.rx.report_exceptions(
                lambda e: self.handleError(
                    e, 'finalize_submit_components:postMortemCheck(%s)' % comp_names),
                self.log, label='HandleErrorPostMortem(%s)' % comp_names
            )

            # VV: Wait till components are finished to resolve their output anti-dependencies
            reactivex.merge(*[c.notifyFinished for c in staged_in]) \
                .pipe(op.observe_on(self.controllerPool)) \
                .subscribe(on_next=finished_check, on_error=handle_error_finished)

            # VV: Hook up the notifyPostMortem observable
            reactivex.merge(*[c.notifyPostMortem for c in staged_in])\
                .pipe(op.observe_on(self.controllerPool), op.filter(lambda e: e[1].finishCalled is False)) \
                .subscribe(on_next=postmortem_check, on_error=handle_error_postmortem)

            # VV: Finally, execute the components
            for comp in staged_in:
                self.log.info('Submitting component %s' % comp.specification.reference)
                do_run = (
                            (self.do_restart_sources is None)
                            or (self.do_restart_sources.get(comp.stageIndex, False) is False)
                            or isinstance(comp.engine, experiment.runtime.engine.RepeatingEngine)
                    )

                if do_run:
                    comp.run()
                else:
                    self.log.warning('Restarting source engine %s instead of running it' % comp.specification.reference)
                    restartCode = self._restartComponent(comp,
                                                         exitReason=experiment.model.codes.exitReasons['ResourceExhausted'],
                                                         returncode=0)

                    if restartCode == experiment.model.codes.restartCodes["RestartNotRequired"]:
                        TransitionComponentToFinalState(comp,
                                                        experiment.model.codes.exitReasons['Success'],
                                                        returncode=0)
                    elif restartCode == experiment.model.codes.restartCodes["RestartCouldNotInitiate"]:
                        #This is either because the hook failed to prepare restart OR it determined it wasn't possible
                        #In this case we class it as KnownIssue
                        #This allows the components "shutdownOn" spec to be used in a logical fashion
                        #i.e. if a component specifies to shutdownOn KnownIssue, meaning "when the failure reason has
                        #an understood cause let other component still run", then the case where the restart couldn't
                        #initiate fits this criteria.
                        TransitionComponentToFinalState(comp,
                                                        experiment.model.codes.exitReasons['KnownIssue'],
                                                        returncode=1)

        except Exception:
            import traceback
            self.log.critical("EXCEPTION when submitting\n%s" % traceback.format_exc())
            raise

    def get_stage_status(self, stage_index):
        if stage_index not in self._stageStates:
            return None

        with self.comp_lock:
            comps = self.get_components_in_stage(stage_index)

            done_states = [
                experiment.model.codes.FINISHED_STATE,
            ]

            done = [int(comp.state in done_states) for comp in comps]

            total = len(done)
            len_finished = sum(done)

        if total == 0 :
            self.log.critical("Inconsistent Stage %d with 0 jobs" % stage_index)
            return None

        status_report = len_finished/float(total)

        self.log.info("Status report for stage%d %d/%d = %f" % (
            stage_index, len_finished, total, status_report
        ))

        return status_report

    def get_stages_in_transit(self):
        in_transit = set()  # type: Set[int]

        with self.comp_lock:
            for node_name in self.graph.nodes:
                if self.node_is_active(node_name) is False:
                    continue
                comp_state = self.get_compstate(node_name)
                in_transit.add(comp_state.stageIndex)

        return sorted(in_transit)

    def get_stages_finished(self):
        finished = []

        with self.comp_lock:
            for sg_idx in self._stageStates:
                comps = self.get_nodes_in_stage(sg_idx)
                all_done = not any(map(self.node_is_active, comps))
                if all_done:
                    finished.append(sg_idx)

        return sorted(finished)

    def initialise(self, stage, statusDatabase):
        # type: (Stage, StatusDB) -> None
        '''Sets and initialise the current stage

        Parameters:
            statusDatabase: An experiment.runtime.status.StatusDB instance that will track the components that are run
            stage: An Stage instance

        - Creates the run-time Component instances for each node in the current stage
        - Performs all data reference resolution for each Component (stageIn)
        - Sets up database monitoring

        This step must be called before running the stage

            Parameters:
                
                stage: An experiment.data.Stage instance

        Exceptions:

        Raises errors.DataReferenceFilesDoNotExistError if specified reference files do not exist
        Raises errors.DataReferenceCouldNotStageError if there was an error in copying/linking files
        Raises errors.FilesystemInconsistencyError if problem detected with filesystem.'''

        def init_comps():
            with self.comp_lock:
                if self.currentStage is None:
                    self._starting_index = stage.index
                    # VV: If stageIn is None, assume that all stages must be staged
                    self.statusDatabase = statusDatabase

                if self._starting_index == 0:
                    # VV: The starting stage is 0, there's no need to mark any components as finished
                    return

                # VV: Mark all components in skipped stages as FINISHED
                for finished_stage_idx in range(0, self._starting_index):
                    comps = self.get_components_in_stage(finished_stage_idx, True)
                    for comp in comps:
                        comp.controllerState = experiment.model.codes.FINISHED_STATE

                        # VV: This component is marked as done; this lets the controller inspect the actual state of the
                        #     Component instead of assuming it is still in `running` state
                        self.comp_done.add(comp.specification.reference)

                # VV: Mark all placeholders in stages [0, starting_index) as finished
                for p_ref in self.workflowGraph._placeholders:
                    try:
                        c_id = experiment.model.graph.ComponentIdentifier(p_ref)
                    except Exception:
                        msg = "Could not construct ComponentIdentifier for placeholder \"%s\"" % p_ref
                        self.log.critical(msg)
                        raise_with_traceback(ValueError(msg))
                    if c_id.stageIndex < self._starting_index:
                        self.workflowGraph._placeholders[p_ref]['state'] = experiment.model.codes.FINISHED_STATE
                        self.comp_done.add(p_ref)

        try:
            init_comps()
        except Exception as e:
            msg = "Failed to initialise controller. Errror: %s" % e
            self.log.critical(msg)
            raise_with_traceback(ValueError(msg))
        else:
            # VV: reset stop_executing
            self.stop_executing = False

            self.currentStage = stage
            report = self.generate_status_report_for_nodes(components=None, filter_done=False)
            self.log.info("Initial dependency analysis:%s" % report)

    def sourceComponentForComponent(self, component):
        # type: (ComponentState) -> ComponentState
        '''Find the migrated component related to the passed object

        Parameters:
            component (workflow.ComponentState): A ComponentState instance.
                To make sense component.specification.isMigratable should be True but this is not checked

        Returns:
            None|workflow.ComponentState:
                If a related migrated component was found it is returned'''

        #Migrated components only have one producer - the source component that is being migrated
        #Since this component and the source component store the same specification
        #we can match on specification

        retval = None

        # Get the specifications of all this component producers
        producerSpecifications = [c.specification for c in component.producers]
        components = [comp for comp in self.migratedComponents if comp.specification in producerSpecifications]
        if len(components) > 1:
            self.log.warning("Error - More than one migrated manager found for component" % component, components)
        elif len(components) == 1:
           retval = components[0]

        return retval

    def _handleMigration(self, components):
        # type: (List[ComponentState]) -> List[ComponentState]

        '''Tries to assign all migrated engines to new components

        Returns:
            List of engines that were assigned

        Raises:
            InternalInconsistencyError if all engines cannot be assigned'''

        matchedComponents = []

        try:
            for component in components:
                # Check if is migrated
                if component.specification.isMigrated:
                    self.log.info("Migrated component %s" % component)
                    # The component must have a match in migrated components from previous step
                    # This method will raise InternalInconsistencyError if the migrated component doesn't exist
                    source = self.sourceComponentForComponent(component)
                    if source is None:
                        raise experiment.model.errors.InternalInconsistencyError(
                            'Engine for component %s expected to be migrated from previous stage,'
                            ' but no matching component exists' % component.specification.reference)
                    else:
                        self.log.info("Found migrated component engine: %s" % source.engine)
                        # Switch the engine
                        component.engine = source.engine

                        index = self.migratedComponents.index(source)
                        self.migratedComponents.pop(index)
                        matchedComponents.append(component)

        except ValueError as data:
            self.log.critical('Failure when creating component engines for stage (ValueError)')
            import traceback
            self.log.critical('EXCEPTION:\n%s' % traceback.format_exc())
            raise experiment.model.errors.InternalInconsistencyError(data)
        except Exception:
            self.log.critical('Failure when creating component engines for stage')
            import traceback
            self.log.critical('EXCEPTION:\n%s' % traceback.format_exc())
            raise

        # Check that all migrating jobs were actually migrated
        if len(self.migratedComponents) != 0:
            self.log.critical('Migratable components from previous stage have no matching components in current stage!')
            self._stopComponents(self.migratedComponents+matchedComponents, True)
            raise experiment.model.errors.InternalInconsistencyError(
                'Migratable components from previous stage have no matching components in current stage!')

        return matchedComponents

    def get_nodes_in_stage(self, stage_index):
        # type: (int) -> List[str]
        with self.comp_lock:
            ret = []

            try:
                for node_name, data in self.graph.nodes(data=True):
                    if data.get('stageIndex') == stage_index:
                        ret.append(node_name)
            except networkx.NetworkXUnfeasible as e:
                cycle = networkx.find_cycle(self.graph)
                self.log.critical("Cycle in graph: %s" % ['->'.join(edge) for edge in cycle])
                raise_with_traceback(e)

        return ret


    def _observe_completionCheck(self, stage):
        #Create an observable polling the external completion check
        #It emits one emission, True, if the completion check returns True

        def stop_components_in_stage(stage):
            index = stage.index
            
            # VV: at the time of running closure:
            #     - Collect all non-finished components
            #     - Stop them
            
            def closure(x):
                with self.opt_lock:
                    nodes = self.get_nodes_in_stage(index)
                    nodes = self._true_nodes_from_identifiers(nodes, False)
                    current_components = set()
                    for node in nodes:
                        current_components = current_components.union([c['component']() for c in nodes[node]])

                    self.log.info("CompletionCheck for stage %d returned True, stopping components in same stage" % (
                        index
                    ))
                    self._stopComponents(current_components, False)

                return x

            return closure

        # VV: The external completion event will terminate components in a particular stage
        reactivex.interval(
            5, scheduler=reactivex.scheduler.NewThreadScheduler()) \
            .pipe(
            op.map(lambda x: self.completionCheck(stage.index, stage.directory)),
            op.filter(lambda x: x is True),
            op.first(),
            op.map(stop_components_in_stage(stage))
        ).subscribe(on_error=lambda e: self.handleError(e, '_observe_completionCheck(stage=%s (index=%s))' % (
            stage.name, stage.index)))

    def kill_all_components(self, stop_optimizer):
        with self.comp_lock:
            # VV: Iterate graph and for each component:
            #     - finish(SHUTDOWN) it if it's staged (i.e. executing or about to execute)
            #     - fake-finish(SHUTDOWN) it if they have not been staged yet
            # Mark all placeholders of all DoWhile documents as done
            # Inform every part of the Controller that it has stopped scheduling new components and that it's
            # potentially came across some error
            self.stop_executing = True

            # VV: Mark all placeholders as done
            placeholders = self.workflowGraph._placeholders

            for p_name in placeholders:
                if p_name not in self.comp_done:
                    self.log.info("Tag placeholder %s of %s as Done" % (
                        p_name, placeholders[p_name]['DoWhileId']))
                    self.comp_done.add(p_name)

            # VV: iterate all components, the controller may have experienced a critical error so the graph
            #     may be in some invalid state, do not panic if you can't find the ComponentState of a component
            for node, data in self.graph.nodes(data=True):
                if 'component' in data:
                    try:
                        comp = data['component']()  # type: ComponentState

                        if comp.finishCalled is False and comp.isAlive():
                            self.log.warning("Killing active component %s" % comp.specification.reference)

                            if comp in self.comp_staged_in:
                                comp.finish(experiment.model.codes.SHUTDOWN_STATE)
                            else:
                                self._fake_finish_with_state(comp, experiment.model.codes.SHUTDOWN_STATE)
                    except Exception as e:
                        self.log.warning("Could not setup %s to go through POSTMORTEM and FINISHED checks, marking "
                                         "it as done. Error was %s" % (node, e))
                        self.comp_done.add(node)

            if stop_optimizer:
                self.disable_optimizer("done killing all components.")

    def run(self):

        '''Runs the current stage of an experiment - set using stageData()

        Exceptions:

        Note: Existence of all resources necessary to execute a components task
        (inputs, executables, writable working dir) is assumed to have been checked
        prior to calling this method i.e. as part of the normal experiment
        launch process.

	    Raises an errors.UnexpectedJobFailureError if any component exits with FAILED state
        On detecting any errors all active Components are stopped.

        Raises an errors.SystemError if system issues are detected e.g. required processes not running,
        filesystem inconsistent etc.

        Raises an errors.InternalInconsistencyError in the case of a programming bug e.g.
            - This method is called before stageData(stage).
            - A Component has an undefined type
            - A Component identified as migratable has no matching job in current stage

        Raises an errors.UnexpectedExceptionError if an unexpected error occurred during component start

        '''

        stage = self.currentStage
        stage_idx = self.currentStage.index
        this_stage_components = self.components

        matchedComponents = self._handleMigration(this_stage_components)

        #Migration
        try:
            with self.comp_lock:

                comp_names = [comp.specification.reference for comp in this_stage_components]
                self.log.info("Report for stage %d\n%s" % (
                    stage_idx, self.generate_status_report_for_nodes(comp_names)))

            # VV: Subscribe to external completionCheck() for stage before making any scheduling decisions
            self._observe_completionCheck(stage)

            # VV: Schedule all ready components while keeping in mind
            #     that some of them might be migrated from previous stages
            self._schedule(migrated_components=set(matchedComponents))
        except (experiment.model.errors.FilesystemInconsistencyError,
                experiment.runtime.monitor.MonitorTestError, experiment.runtime.errors.JobLaunchError,
                experiment.model.errors.DataReferenceProducerNotFoundError) as error:
            import traceback
            self.log.critical('System level failure detected:\n%s' % traceback.format_exc())
            self.handleError(error, "run()")
            raise experiment.model.errors.SystemError(error)

        inactive_states = [
            experiment.model.codes.FINISHED_STATE, experiment.model.codes.FAILED_STATE, experiment.model.codes.SHUTDOWN_STATE
        ]

        def get_active_components(stage_idx=stage_idx, inactive_states=inactive_states):
            """Discover active components AND placeholder components

            Placeholder components are surrogate components for components that are part of a loop. They cannot resolve
            to finished till the latest `conditionn :output reference` evaluates to False *AND* all the component
            instances that the Placeholder represents terminate, see get_node_state() for more information
            """
            with self.comp_lock:
                names = self.get_nodes_in_stage(stage_idx)

                active_components = list(filter(self.node_is_active, names))

                placeholders = self._get_placeholder_nodes_in_stage(stage_idx)
                active_components.extend([p for p in placeholders
                                          if (p not in self.comp_done) and
                                          (self.get_node_state(p) not in inactive_states)])
                return active_components

        try:
            while True:
                active_names = get_active_components(stage_idx)
                if not active_names:
                    break
                elif self.stop_executing and active_names:
                    self.log.info("Stage %d is waiting for %s to shutdown before it can complete" %(
                        stage_idx, active_names))

                self._schedule(migrated_components=set(matchedComponents))

                # TODO VV: Do I handle migrated jobs properly?
                self._event_scheduler.wait(5)
                self._event_scheduler.clear()

        except KeyboardInterrupt as error:
            self.log.critical('Caught keyboard interrupt/signal - will stop all active components')
            self.handleError(error, "run(KeyboardInterrupt)")
            self.log.critical('Finished stopping active components')
            raise
        except Exception as error:
            import traceback
            self.log.critical("Unexpected error (%s) while starting components " 
                              "- will stop any already started\n%s" % (error,
                                                                     traceback.format_exc()))
            self.handleError(error, "run(Unexpected error)")
            raise
        else:
            self._stopComponents(self.components, False)

        #Check if any components have failed
        #If there are all components
        #TODO: Modify to allow some components to fail

        # VV: Re-evaluate stage-components because DoWhile documents will keep injecting new ones till a loop
        #     is terminated
        this_stage_components = self.get_components_in_stage(stage_idx)
        failed_components = [comp for comp in this_stage_components if comp.state == experiment.model.codes.FAILED_STATE]

        if len(failed_components) > 0:
            self.log.warning('Detected component failure during stage %d '
                              '- will stop remaining active components' % stage.index)
            raise experiment.runtime.errors.UnexpectedJobFailureError([
                el.engine for el in self._stageStates[self.stage().index].failedComponents
            ])

        #Add migratable components to list
        for component in this_stage_components:
            if component.specification.isMigratable:
                self.migratedComponents.append(component)

        # VV: The final stage is special it has failed if *none* of its leaf nodes finished successfully
        if stage_idx == self.experiment.numStages() - 1:
            graph = self.graph
            leaf_nodes = [x for x in this_stage_components
                          if len([s for s in graph.successors(x.specification.reference)]) == 0]
            for comp in leaf_nodes:
                if comp.state == experiment.model.codes.FINISHED_STATE:
                    break
            else:
                # VV: No leaf node succeeded, this is a failed stage!
                self._stageStates[stage_idx].controller_state = experiment.model.codes.FAILED_STATE
                raise experiment.runtime.errors.FinalStageNoFinishedLeafComponents()

        self.log.info('Stage %d complete. Total components %d' % (stage.index, len(stage.jobs())))
        if len(self.migratedComponents) != 0:
            self.log.info('Will migrate %d components to next stage' % (len(self.migratedComponents)))

    #
    # Restarting
    #

    def _unstableSystemRestart(self, component,
                               exitReason=None,
                               returncode=None
                               ):
        # type: (ComponentState, str, int) -> str
        '''Specifically for handling an restart when system is detected as unstable'''

        retval = experiment.model.codes.restartCodes["RestartCouldNotInitiate"]
        #Sleep for 30 in order to let system stability to return
        component.controllerState = experiment.model.codes.SUSPENDED_STATE
        count = 0
        while count < 4:
            self.log.info("Will wait 30 secs before next stability check")
            time.sleep(30)
            count += 1
            self.log.info("Checking stability after waiting 30secs")
            if experiment.runtime.monitor.MonitorExceptionTracker.defaultTracker().isSystemStable(30) is True:
                self.log.info("System appears stable - no new exceptions in last 30 secs")
                break
            else:
                self.log.warning("System still unstable - will wait 30s")

        self.log.warning("Waited %d seconds (max 120) - will attempt restart of %s" % (
            count*30, component.specification.reference))

        component.controllerState = None
        try:
            retval = component.restart(reason=exitReason, code=returncode)
        except Exception as error:
           self.log.warning("Encountered exception when attempting job restart: %s" % error)

        return retval                

    def _restartComponent(self, component, exitReason=None, returncode=None):
        # type: (ComponentState, str, int) -> str
        '''Attempts to restart a component that finished unexpectedly

        Parameters:
            component (workflow.ComponentState): A restart will be attempted for this component
            exitReason: The exit reason to supply to the restart.
                If None the current exitReason of the components ENGINE is used
                is used if it exists. If this is None the engine will fail to restart
            returncode:   The returncode to supply to the engine restart.
                If None the current returncode of the components ENGINE is used
                This parameter is used (optionally) by custom-hooks only
                If this is None and there is no hook it will have no effect
                If this is None and there is a hook the behaviour depends on the hook.

        Returns:
            (str): One of the restart-codes defined by experiment.codes.restartCodes
            - RestartInitiated
            - RestartNotRequired
            - RestartCouldNotInitiate
            - RestartMaxAttemptsExceeded

        '''

        #VV: Components list the exitReasons for which the restartHook can be invoked. They can be
        #    any exit reason other than Killed and Canceled. If a component does not provide a
        #    workflowAttributes.restartHookOn list of exit-reasons then Flow assumes that the
        #    component wants to invoke its restart hook just for ResourceExhausted and KnownIssue

        #Check why components engine exited unexpectedly
        #Four conditions
        #1. System
        #       Not the programs fault - this is where we can restart
        #       Can be broken into system errors and system deliberate (e.g. resource exhausted)
        #2. Known Internal Error/Inconsistency
        #       Some problem with arguments or data passed to program
        #       May be due to corrupted data due to error 1 - can restart this
        #       Requires some hook?
        #3. Calculation Error
        #       Calculation is running but not behaving e.g. convergence - requires some hook
        #       This also requires an known (under our control) kill agent
        #4. Bug
        #

        #Component state objects could id exitReason themselves and choose appropriate restart method
        #The extra layer of control is because we may want to delay the restart due to e.g. system instability

        retval = experiment.model.codes.restartCodes["RestartCouldNotInitiate"]

        exitReason = exitReason if exitReason is not None else component.engine.exitReason()
        returncode = returncode if returncode is not None else component.engine.exitReason()

        # VV: @tag:RestartEngines
        if exitReason in component.specification.workflowAttributes.get('restartHookOn', []):
            try:
                self.log.info("Attempting to restart due to %s %s (times so far: %d)" % (
                    component.specification.reference, exitReason, component.engine.restarts))
                retval = component.restart(reason=exitReason, code=returncode)
            except Exception as error:
                self.log.warning("Encountered exception when attempting engine restart: %s" % error)
        elif exitReason == experiment.model.codes.exitReasons["SubmissionFailed"]:
            #In this particular case we want to avoid continually resubmitting as the failure may
            #be e.g. the system is down
            self.log.info("Attempting to restart SubmissionFailed %s" % component.specification.reference)
            if component.engine.resubmissionAttempts() < self._max_resubmission_attempts:
                try:
                    retval = component.restart(reason=exitReason, code=returncode)
                except Exception as error:    
                    self.log.warning("Encountered exception when attempting component engine restart: %s" % error)
            else:
                retval = experiment.model.codes.restartCodes["RestartMaxAttemptsExceeded"]
                self.log.critical("Maximum resubmission attempts (%d) exceeded (%s) - aborting" % (
                    self._max_resubmission_attempts, retval))
        elif exitReason not in [experiment.model.codes.exitReasons["Killed"], experiment.model.codes.exitReasons["Cancelled"],
                                experiment.model.codes.exitReasons["Success"]]:
            self.log.debug("Checking for system stability")
            #FIXME: May have to wait for threads to register instability - this should be the minimum of the
            # monitor times
            #i.e. a monitor could have entered a read that failed up to monitor-interval after this process
            #Assume there is a OS wide time-out on reads on exceeding which the exceptions are raised
            time.sleep(25.0)
            if experiment.runtime.monitor.MonitorExceptionTracker.defaultTracker().isSystemStable(120) is False:
                self.log.warning("System is not stable - assuming exit due to instability")
                experiment.runtime.monitor.MonitorExceptionTracker.defaultTracker().printStatus(details=True)
                retval = self._unstableSystemRestart(component, exitReason=exitReason, returncode=returncode)
            else:
                retval = experiment.model.codes.restartCodes["RestartCouldNotInitiate"]
                self.log.log(18, "System appears stable - assuming %s exited due to internal error. "
                               "Cannot restart (%s) " % (component.specification.reference, retval))


        return retval

    #
    # Reactive: Subscribed actions
    #


    def postMortemCheck(self,
                        state,
                        component  # type: ComponentState
                        ):

        '''Triggered on a post-mortem notification

        Parameters:
            state (str): The components state
            component (experiment.workflow.ComponentState): The component instance which has entered portMortem

        The purpose of the call back is 
            - To attempt a restart of the component if necessary e.g. it exited due to ResourceExhausted
            - To otherwise set the components state to FINISHED or FAILED

        When this method is called the Controller performs the following actions

        - Obtains the Components state and returncode
        - Checks if FAILED or FINISHED
            - If FINISHED, the components state is set permanently to FINISHED
            - If FAILED - possibility to restart is checked
        - Note: A non-zero return code from a component, or any exception during this method,
         will lead to FAILED state and stage exiting
        
        Note: If there is an exception in this function the status of the calling
        job is set to failed.
        This is because this indicates a programming bug'''

        try:
            returncode = component.engine.returncode()
            exitReason = component.engine.exitReason()
            reference = component.specification.reference
            cur_stage_idx = self.stage().index if self.stage() is not None else None
            self.log.info('Received POSTMORTEM notification from %s in stage %d. '
                          'Returncode: %s. ExitReason: %s. shutdownOn: %s' % (
                            reference, cur_stage_idx, returncode, exitReason,
                            component.specification.workflowAttributes['shutdownOn'])
                          )

            if self._restartComponent(component) == experiment.model.codes.restartCodes["RestartInitiated"]:
                self.log.info("Restarted component with exitReason %s and returnCode %s" % (exitReason, returncode))
            else:
                TransitionComponentToFinalState(component, exitReason, returncode)
        except Exception as error:
            #This indicates a programing error dealing with job exit
            self.log.critical('Unexpected exception handling component exit notification: %s' % str(error))
            self.log.critical('Will abort experiment as this indicates a bug in the program')
            import traceback
            self.log.critical('EXCEPTION:\n%s' % traceback.format_exc())
            component.finish(experiment.model.codes.FAILED_STATE)


    def handleError(self, error, origin='unknown'):

        '''Handles any error in the combined notifyFinished observable of the components

        The action is to stop (finish) all the components and end the stage.
        isComplete() will return true on the next poll of the main loop i.e not asynchronous to this call

        This is the only way run() should exit without all components being finished.
        '''
        if origin is not None:
            self.log.critical('Error, %s, raised while %s' % (error, origin))
        else:
            self.log.critical('Error, %s, raised while (?)' % error)

        import traceback
        self.log.critical('EXCEPTION:\n%s' % ''.join(traceback.format_stack()))
        index = self.currentStage.index if self.currentStage is not None else None
        self.log.critical('Stage %d will stop' % index)
        self.kill_all_components(True)

        # VV: This will let the controller know what something horribly wrong has taken place and
        #     it should not attempt to fire any new Components
        self.stop_executing=True

    def _instantiate_next_dowhile_iteration(self, dw_node):
        # type: (Dict[str, Any]) -> None
        """Instantiates components for the next iteration of a DoWhile.

        VV: Algorithm
        - Use WorkflowGraph API to generate new graph that includes components for next iteration
        - Compute dependencies
        - Create the working directories of the new nodes (and take into account that Flow might be
          operating in Hybrid mode).
        """
        with self.comp_lock:
            do_while = dw_node['document']
            state = dw_node['state']
            next_iteration = state['currentIteration'] + 1

            dw_name = 'stage%d.%s' % (do_while.get('stage', 0), do_while['name'])
            self.log.info("Instantiating DoWhile %s#%s" % (dw_name, next_iteration))

            new_comp_names = self.workflowGraph.instantiate_dowhile_next_iteration(do_while, next_iteration, True)
            new_folders = []

            skip = self._starting_index
            instance_dir = self.experiment.instanceDirectory
            # VV: Instantiate novel components, and create their working dir (hybrid setups supported too)
            for reference in new_comp_names:
                data = self.graph.nodes[reference]

                try:
                    specification = data['componentSpecification']  # type: ComponentSpecification
                except Exception:
                    msg = "Failed to get componentSpecification from %s:%s" % (reference, data)
                    self.log.critical(msg)
                    raise_with_traceback(ValueError(msg))

                # VV: Create Job after creating the node; Job then registers the 'componentInstance' node attribute
                cid = specification.identification
                directory = instance_dir.createJobWorkingDirectory(cid.stageIndex, cid.componentName)
                job = experiment.model.data.Job.jobFromConfiguration(cid, self.workflowGraph, directory)

                # VV: Ensure that executable is valid - also update executable path/Kubernetes image
                job.componentSpecification.checkExecutable(updateSpecification=True)
                self.experiment.getStage(cid.stageIndex).add_job(job)

                comp = experiment.runtime.workflow.ComponentState(job, self.workflowGraph, cid.stageIndex >= skip)
                self._instantiated_components.append(comp)
                new_folders.append(job.directory)

            # VV: Replicate structure on remote platform (for hybrid configurations)
            hybrid_conf = experiment.appenv.HybridConfiguration.defaultConfiguration()
            if hybrid_conf.isHybrid:
                instance_location = self.workflowGraph.rootStorage.location
                hybrid_conf.replicateFolderStructure(instance_location, new_folders)

            self.parse_workflow_graph()

    def _handle_condition_component_finished(self, component, dw_name):
        # type: (ComponentState, str) -> None
        """Triggered when a Component which generates the condition reference of a DoWhile finishes.

        If the component is not Finished OR the conditionReference cannot be resolved OR the ConditionReference
        does not evaluate to `True`/`False` then all placeholders will be marked as experiment.codes.FAILED_STATE once
        the components they represent terminate. In the meantime, any calls to "get_node_state(placeholder)" will
        return experiment.codes.RUNNING_STATE

        If the component is Finished and the Condition Reference evaluates to:
        - True: spawn the next iteration
        - False: do not spawn the next iteration

        In this scenario, calls to get_node_state(placeholder) will return `experiment.codes.RUNNING_STATE` for as
        long as *any* of the components the placeholder represents has not completed. When all of them terminate
        calls to get_node_state(placeholder) will return the state of the represented component of the last iteration.
        """
        with self.comp_lock:
            FlowIR = experiment.model.frontends.flowir.FlowIR
            dw_node = self.workflowGraph.get_document_metadata(FlowIR.LabelDoWhile, dw_name)

            state = dw_node['state']
            doc = dw_node['document']

            self.log.info("Component %s controls whether iteration %s of %s will be instantiated" % (
                component.specification.reference, state['currentIteration']+1, dw_name
            ))

            if component.state == experiment.model.codes.FINISHED_STATE:
                try:
                    condition_dr = experiment.model.graph.DataReference(state['currentCondition'], doc['stage'])
                    do_next_iter = condition_dr.resolve(self.workflowGraph)
                except Exception as e:
                    raise ValueError("Condition reference %s is supposed to evaluate to \"True\" or \"False\"."
                                     " Instead this reference errored with exception %s" % (
                        state['currentCondition'], e
                    ))
                else:
                    original = do_next_iter
                    do_next_iter = do_next_iter.strip().lower()

                    if do_next_iter not in ['true', 'false']:
                        raise ValueError("Condition reference %s is supposed to evaluate to \"True\" or "
                                         "\"False\". Instead this reference resolved to \"%s\"" % (
                                             state['currentCondition'], original
                                         ))
                    elif do_next_iter == 'true':
                        self.log.info("Condition %s resolved to True, will add new iteration %d" % (
                            state['currentCondition'], state['currentIteration'] + 1))
                        try:
                            self._instantiate_next_dowhile_iteration(dw_node)
                        except Exception as e:
                            self.log.critical("Failed to instantiate next iteration for DoWhile %s" % dw_name)
                            self.kill_all_components(True)
                            raise_with_traceback(e)
                    elif do_next_iter == 'false':
                        self.log.info("Condition %s resolved to False, will not create new iteration for DoWhile "
                                      "loop %s" %(state['currentCondition'], dw_name))
            else:
                self.log.info("Component %s did not finish successfully (%s) - will not resolve condition reference %s "
                              "or create a new iteration for DoWhile %s" % (
                    component.specification.reference, component.state, state['currentCondition'], dw_name))


    def finishedCheck(self, state, component):
        # type: (Any, ComponentState) -> None
        '''Called on each notifyFinished from a component

        1. Checks if the component failed and:
           a) If the component belongs to the current stage it kills all components in current stage
           b) If the component belongs to a FUTURE stage it kills all components that are currently active AND
              tags any pending or ready-to-execute components as failed.
        2. Resolves output dependencies of finished component to promote pending jobs into ready-to-run

        Termination is asynchronous

        Note: Each FAILED_STATE component will trigger an attempt to stop the the others.
        However calls to _stopComponents have no effect after the first
        '''
        my_ref = component.specification.reference
        self.log.info('Finished state of %s is %s' % (my_ref, component.state) )
        # VV: Locking this mutex ensures that while we're handling this Finished event no other
        #     component will be scheduled
        try:
            with self.comp_lock:
                if self._start_sleeping:
                    self.log.log(19, "Postpone finishedCheck() of %s till after I wake up" % (my_ref))
                    self._component_finished_while_sleeping.append((state, component))
                    return

                # VV: Check if this component produces a condition for a DoWhile
                dw_name = self.comp_condition_to_dowhile.get(my_ref)
                if dw_name is not None:
                    self._handle_condition_component_finished(component, dw_name)

                if component.state == experiment.model.codes.FAILED_STATE:
                    #TODO: Check if the failure of the component should trigger stage exit

                    if component.stageIndex > self.currentStage.index:
                        msg = "Component %s from future stage failed %d" % (my_ref, component.stageIndex)
                        self.log.warning('%s - stopping experiment!' % msg)
                        # VV: This error comes from a future stage! Panic and kill everything
                        self.kill_all_components(True)
                    else:
                        self.log.warning('Component failed %s - stopping stage %d' % (my_ref, self.stage().index))

                        # VV: Transfer any component that hasn't been submitted yet to the backend to the failed state
                        stage_components = self.get_components_in_stage(component.stageIndex)

                        for comp in stage_components:
                            if comp not in self.comp_staged_in and comp.finishCalled is False:
                                self._fake_finish_with_state(comp, experiment.model.codes.SHUTDOWN_STATE)

                        # VV: Then stop active components in the same stage
                        self._stopComponents(stage_components, False)
                elif component.state == experiment.model.codes.SHUTDOWN_STATE:
                    pass
                elif component.state == experiment.model.codes.FINISHED_STATE:
                    if self.cdb:
                        # VV: Don't go through the trouble of calculating and reporting the memoization hash
                        #    of a component if we're not planning on memoizing any of the components in this workflow
                        self.log.info("Memoization information of finished %s is %s:%s" % (
                            my_ref, component.memoization_hash, pprint.pformat(component.memoization_info)))
                else:
                    msg = ('The state of component %s is not FAILED/SHUTDOWN/FINISHED. '
                           'This indicates an inconsistency. Experiment will stop' % (my_ref))
                    self.handleError(ValueError(msg), 'finishedCheck()')
        except Exception as e:
            msg = ("Exception raised while handling finishedCheck of %s with state %s (will tag "
                   "component as failed). EXCEPTION:%s" % (my_ref, component.state, traceback.format_exc()))
            self.log.warning(msg)
            component.controllerState = experiment.model.codes.FAILED_STATE
            try:
                self.handleError(ValueError(msg), 'finishedCheck(%s)' % e)
            except Exception as e:
                self.log.critical("Exception raised while handling previous exception %s" % e)
                self.log.critical(traceback.format_exc())
        finally:
            # VV: This component is marked as done; this lets the controller inspect the actual state of the
            #     Component instead of assuming it is still in `running` state
            self.comp_done.add(component.specification.reference)

            # VV: Print information about components that are either scheduled, or waiting to be scheduled (i.e. active)
            report = self.generate_status_report_for_nodes(components=None, filter_done=True)
            self.log.log(14, "Status report for active components follows:%s" % report)

            # VV: Trigger attempt to schedule new components
            self._event_scheduler.set()

    def killController(self, reason):
        '''Call to kill controller'''
        self.log.info('Killing controller, reason: %s' % reason)
        self.kill_all_components(True)

    #
    # Properties
    #

    def stage(self):

        return self.currentStage

    def stageState(self, stage=None):
        # type: (Stage) -> str
        '''Returns the state of the current stage.

        This method will be called from various threads mainly
        while the run() method is executing'''
        if stage is None:
            stage = self.stage()

        return self._stageStates[stage.index].state

    def cleanUp(self):

        '''Call on error to ensure all jobs are killed'''

        #Don't let exception escape here as this method is likely to be called in a finally: block
        try:
            with self.comp_lock:
                self.log.warning("Cleanup called - shutting down components")
                self.kill_all_components(True)
                self.stop_executing = True
        except Exception as error:            
            self.log.warning("Exception when killing jobs - %s - ignoring" % error)

    def observe_engine_change(self, state_update, engine):
        # type: (Dict[str, Any], "experiment.engine.Engine") -> None
        # VV: @tag: OPTIMIZER-observe-engine-updates

        transitions = ['taskDateWait', 'taskDateExecute', 'taskDateComplete']
        served = 0
        # VV: Make sure to process the events in the correct order, it's possible
        #     that a single update contains multiple transitions

        # events = {}
        # for key in transitions:
        #     if key in state_update and state_update[key] is not None:
        #         events[key] = state_update[key]
        #
        # self.log.info("Observed events: %s" % pprint.pformat(events))
        with self.opt_lock:
            if self.enable_optimizer is False:
                return

            for key in transitions:
                if key in state_update and state_update[key] is not None:
                    served += 1
                    dt = state_update[key]  # type: datetime.datetime
                    self.log.log(19, "Received engine update %s:%s (timestamp: %s) from %s" % (
                        key, dt, dt.strftime('%s'), engine.job.reference
                    ))
                    stage = 'stage%d' % engine.job.stageIndex
                    event_date = state_update[key]

                    if key == 'taskDateWait':
                        try:
                            self.optimizer_repeat.event_queue_start(stage, engine.job.name, event_date)
                        except Exception:
                            self.log.critical("Failed to register OPTIMIZER QUEUE START for %s. "
                                              "Disabling optimizer. "
                                              "EXCEPTION: %s\n" % (
                                engine.job.reference, traceback.format_exc()
                            ))
                            self.disable_optimizer(None)
                    elif key == 'taskDateExecute':
                        try:
                            self.optimizer_repeat.event_queue_stop(stage, engine.job.name, event_date)
                            self.optimizer_repeat.event_run_start(stage, engine.job.name, event_date)
                        except Exception:
                            self.log.critical("Failed to register OPTIMIZER QUEUE STOP or RUN START for %s. "
                                              "Disabling optimizer. "
                                              "EXCEPTION: %s\n" % (
                                engine.job.reference, traceback.format_exc()
                            ))
                            self.disable_optimizer(None)
                    elif key == 'taskDateComplete':
                        try:
                            # VV: The exit-code ideally should exist in the same update that contains
                            #     a taskDateComplete if that's not the case attempt to fetch the exit-code
                            #     directly from engine.returncode
                            if 'lastTaskExitCode' in state_update:
                                exit_code = state_update['lastTaskExitCode']
                            else:
                                # VV: go directly to the source
                                exit_code = engine.stateDictionary['lastTaskExitCode']

                            if exit_code is None:
                                self.log.critical("Engine emitted EXECUTE COMPLETE but no exit-code. "
                                                  "The optimizer will panic and disable itself")
                                raise ValueError("Missing exit-code in engine state-update containing taskDateExecute")

                            self.optimizer_repeat.event_run_stop(stage, engine.job.name, event_date, exit_code)
                        except Exception:
                            self.log.critical("Failed to register OPTIMIZER RUN STOP for %s. "
                                              "Disabling optimizer. "
                                              "EXCEPTION: %s\n" % (
                                engine.job.reference, traceback.format_exc()
                            ))
                            self.disable_optimizer(None)
                    else:
                        self.log.critical("Got bogus message %s for %s. "
                                          "Disabling optimizer. "
                                          "EXCEPTION: %s\n" % (
                                              key, engine.job.reference, traceback.format_exc()
                                          ))
                        self.disable_optimizer(None)

            try:
                self.optimizer_repeat.optimize(datetime.datetime.now())
            except Exception:
                self.log.critical("Optimizer raised an exception. Disabling optimizer. EXCEPTION: %s\n" % (
                                      traceback.format_exc()
                                  ))
                self.disable_optimizer(None)
