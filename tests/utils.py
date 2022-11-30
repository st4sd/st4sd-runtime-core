# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os

import experiment.model.data
import experiment.model.storage
import experiment.runtime.control
import experiment.runtime.workflow
import experiment.model.graph
import uuid
import logging
import time

import networkx
import reactivex
import reactivex.scheduler
import reactivex.operators as op

from typing import Optional, Dict, Tuple, List, Set, TYPE_CHECKING

if TYPE_CHECKING:
    import experiment.runtime.engine

logger = logging.getLogger("utils")

NewControllerReturnType = Tuple[experiment.runtime.control.Controller, Dict[int, Set[
    experiment.runtime.workflow.ComponentState]]]



def populate_files(location, extra_files):
    # type: (str, Dict[str, str]) -> None
    """Populates files under some root directory
    """

    for path in extra_files:
        sub_folder, name = os.path.split(path)
        try:
            os.makedirs(os.path.join(location, sub_folder))
        except Exception as e:
            pass
        with open(os.path.join(location, path), 'w') as f:
            f.write(extra_files[path])


def experiment_from_flowir(
        flowir: str,
        location: str,
        extra_files: Dict[str, str]|None=None,
        variable_files: List[str] | None = None,
        checkExecutables=True,
        inputs: List[str] | None = None,
        override_data: List[str] | None = None,
) -> experiment.model.data.Experiment:
    package_path = os.path.join(location, '%s.package' % str(uuid.uuid4()))
    dir_conf = os.path.join(package_path, 'conf')
    os.makedirs(dir_conf)
    with open(os.path.join(dir_conf, 'flowir_package.yaml'), 'w') as f:
        f.write(flowir)

    extra_files = extra_files or {}

    populate_files(package_path, extra_files)

    os.chdir(os.path.expanduser('~'))
    experiment_package = experiment.model.storage.ExperimentPackage.packageFromLocation(package_path)
    exp = experiment.model.data.Experiment.experimentFromPackage(
        experiment_package, location=location, variable_files=variable_files, inputs=inputs, data=override_data)
    exp.validateExperiment(checkExecutables=checkExecutables)
    return exp


def kill_engine_after_started(engine: experiment.runtime.engine.Engine, send_signal: int | None = None,
                              time_delay: float=1.0):
    have_killed = [False]

    def try_kill(tuple_state_engine, which=engine, have_killed=have_killed):
        # type: (Tuple[Dict[str,str], experiment.runtime.engine.Engine], experiment.runtime.engine.Engine, List[bool]) -> None
        state, engine = tuple_state_engine
        logger.info("Have already killed? %s and current emission %s" % (have_killed, state))
        try:
            if state.get('taskDateExecute') is not None:
                have_killed[0] = True

                if send_signal is not None:
                    logger.warning(f"Will send signal {send_signal} to "
                                   f"{engine.job.reference}/{engine.process.pid} in {time_delay} seconds")
                    time.sleep(time_delay)
                    os.kill(engine.process.pid, send_signal)
                else:
                    logger.warning(f"Will kill {engine.job.reference} in {time_delay} second")
                    time.sleep(time_delay)
                    which.kill()

        except Exception:
            import traceback
            logger.critical("Error: %s" % traceback.format_exc())
            raise

    engine.stateUpdates.pipe(
        op.observe_on(reactivex.scheduler.NewThreadScheduler()),
        op.take_while(lambda x: have_killed[0] is False),
    ).subscribe(on_next=try_kill)


def new_controller(compExperiment, initial_stage=0, do_restart_sources=None):
    # type: (experiment.model.data.Experiment, int, Optional[Dict[int, bool]]) -> NewControllerReturnType
    components = {}  # type: Dict[int, Set[experiment.runtime.workflow.ComponentState]]
    # VV: Fill graph with all ComponentState but exclude all stages before the initial one when restarting

    assert 0 <= initial_stage < len(compExperiment._stages)
    workflowGraph = compExperiment.experimentGraph

    for i, stage in enumerate(compExperiment._stages):
        components[stage.index] = set()

    for job_name in networkx.topological_sort(compExperiment.graph):
        data = compExperiment.graph.nodes[job_name]

        stage = compExperiment._stages[data['stageIndex']]
        specification = data['componentSpecification']  # type: experiment.model.graph.ComponentSpecification

        job = stage.jobWithName(specification.identification.componentName)

        component = experiment.runtime.workflow.ComponentState(job,
                                                               workflowGraph,
                                                               create_engine=bool(stage.index >= initial_stage))
        components[component.stageIndex].add(component)

    controller = experiment.runtime.control.Controller(compExperiment, do_restart_sources=do_restart_sources)

    return controller, components

def generate_controller_for_flowir(flowir, location, extra_files=None, initial_stage=0, do_restart_sources=None):
    # type: (str, str, Optional[Dict[str, str]], int, Optional[Dict[int, bool]]) -> experiment.runtime.control.Controller
    exp = experiment_from_flowir(flowir, location, extra_files)

    controller, all_comps = new_controller(exp, initial_stage, do_restart_sources)

    class FakeStatus:
        def __init__(self):
            pass

        def monitorComponent(self, *args, **kwargs):
            pass

    controller.initialise(exp._stages[initial_stage], FakeStatus())
    return controller
