# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import print_function

import os
import pprint
from typing import TYPE_CHECKING

import experiment.model.codes
import experiment.model.errors
import experiment.runtime.control
import experiment.model.data
import experiment.model.frontends.flowir
import experiment.model.graph
import experiment.model.storage
import networkx
import pytest

import experiment.runtime.errors
from .reactive_testutils import *
from .test_control import new_controller

if TYPE_CHECKING:
    from experiment.runtime.workflow import ComponentState


def populate_conf_for_dowhile_flowir(contents_main, contents_dowhile, location):
    # type: (str, str, str) -> None
    conf_dir = os.path.join(location, 'conf')
    os.makedirs(conf_dir)

    with open(os.path.join(conf_dir, 'dowhile.yaml'), 'w') as f:
        f.write(contents_dowhile)

    with open(os.path.join(conf_dir, 'flowir_package.yaml'), 'w') as f:
        f.write(contents_main)


def test_import_dowhile_feature():
    wf_template = {
        'type': 'DoWhile',
        'condition': 'george:output',
        'inputBindings': {
            'hello': {
                'type': 'ref',
            }
        },
        'components': [
            {
                'name': 'george',
            },
            {
                'name': 'consumer-of-george',
                'stage': 1,
                'references': [
                    'stage0.george:ref'
                ],
                'command': {
                    'arguments': 'hello:ref stage0.george:ref'
                }
            },
            {
                'name': 'observer-of-george',
                'stage': 0,
                'references': [
                    'george:ref',
                ]
            }
        ]
    }
    instantiate_dowhile = experiment.model.frontends.flowir.instantiate_dowhile
    components, dw_template = instantiate_dowhile(wf_template, bindings={'hello': 'stage4.world:ref'},
                                                  foreign_components=set([(4, 'world')]),
                                                  import_to_stage=5, import_with_name="some-name")

    assert components == [
        {
            'name': '0#george',
            'stage': 5,
            'variables': {'loopIteration': 0},
        },
        {
            'name': '0#consumer-of-george',
            'stage': 6,
            'command': {
                'arguments': 'stage4.world:ref stage5.0#george:ref'
            },
            'references': ['stage5.0#george:ref'],
            'variables': {'loopIteration': 0},
        },
        {
            'name': '0#observer-of-george',
            'stage': 5,
            'references': ['stage5.0#george:ref'],
            'variables': {'loopIteration': 0},
        }
    ]

    assert dw_template == {
        'bindings': {'hello': 'stage4.world:ref'},
        'condition': 'stage0.george:output',
        'components': [{'name': 'george', 'stage': 0},
                       {'command': {'arguments': 'hello:ref stage0.george:ref'},
                        'name': 'consumer-of-george',
                        'references': ['stage0.george:ref'],
                        'stage': 1},
                       {'name': 'observer-of-george',
                        'references': ['george:ref'],
                        'stage': 0}],
        'inputBindings': {'hello': {'type': 'ref'}},
        'loopBindings': {},
        'name': 'some-name',
        'stage': 5,
        'type': 'DoWhile'
    }


def test_rewrite_looped_component():
    aggregate_comp = {'command': {'arguments': 'stage0.replicate:output',
             'executable': 'echo',
             'expandArguments': 'none',
             'interpreter': 'bash'},
             'name': 'aggregate',
             'references': ['stage0.replicate:output'],
             'stage': 1,
             'workflowAttributes': {'aggregate': True}}

    observer_comp = {'command': {'arguments': 'stage1.aggregate:output',
             'executable': 'echo',
             'interpreter': 'bash'},
             'name': 'observer',
             'references': ['stage1.aggregate:output'],
             'stage': 1,
             'workflowAttributes': {'repeatInterval': 1}}

    components = experiment.model.frontends.flowir.rewrite_components(
        [observer_comp],
        bindings={'somethingThatChanges': 'stage0.GenerateInput:output',
                  'somethingThatRemainsTheSame': 'stage0.GenerateInput:output'},
        import_to_stage=1,
        iteration_no=0,
        known_components=set(),
        looped_components=set([(1, 'consumeSomethingThatRemainsTheSame'),
                          (1, 'consumeSomethingThatChanges'),
                          (2, 'replicate'),
                          (2, 'aggregate'),
                          (1, 'observer'),
                          (1, 'LoopCondition')]),
    )

    rewrite_observer = components[0]

    assert rewrite_observer == {'command': {'arguments': 'stage2.0#aggregate:output',
             'executable': 'echo',
             'interpreter': 'bash'},
             'name': '0#observer',
             'references': ['stage2.0#aggregate:output'],
             'stage': 2,
             'variables': {'loopIteration': 0},
             'workflowAttributes': {'repeatInterval': 1}}


    components = experiment.model.frontends.flowir.rewrite_components(
        [aggregate_comp],
        bindings={'somethingThatChanges': 'stage0.GenerateInput:output',
                        'somethingThatRemainsTheSame': 'stage0.GenerateInput:output'},
        import_to_stage=1,
        iteration_no=0,
        known_components=set(),
        looped_components=set([(1, 'consumeSomethingThatRemainsTheSame'),
                              (1, 'consumeSomethingThatChanges'),
                              (1, 'replicate'),
                              (1, 'aggregate'),
                              (1, 'observer'),
                              (1, 'LoopCondition')]),
    )

    rewrite_aggregate = components[0]

    assert rewrite_aggregate == {'command': {'arguments': 'stage1.0#replicate:output',
             'executable': 'echo',
             'expandArguments': 'none',
             'interpreter': 'bash'},
             'name': '0#aggregate',
             'references': ['stage1.0#replicate:output'],
             'stage': 2,
             'variables': {'loopIteration': 0},
             'workflowAttributes': {'aggregate': True}}


@pytest.fixture()
def dw_exp_simple(output_dir):
    # type: (str) -> experiment.model.data.Experiment
    flowir_dowhile = """
    type: DoWhile

    inputBindings:
      number:
        type: output

    loopBindings:
        number: fake_add:output
    
    condition: 'stop/iteration.next:output'
    components:
    - name: add
      command:
        executable: "echo"
        arguments: "$((1+number:output))"
      references:
      - "number:output"
      workflowAttributes:
        replicate: 1
    - name: noDependencies
      command:
        executable: echo
        arguments: hello
      references: []
      workflowAttributes:
        replicate: 1

    - name: fake_add
      command:
        executable: "echo"
        arguments: "add:output"
      references: ["add:output"]
      workflowAttributes:
        aggregate: True

    - name: stop
      command:
        executable: echo
        arguments: $(
          current_iteration=fake_add:output; 
          targetLoops=%(targetLoops)s ; 
          if [ "${current_iteration}" -lt "${targetLoops}" ]; then 
            echo "True" > iteration.next &&
            echo $(( targetLoops + 1 )) > iteration.total &&
            echo "Remaining iteration(s) $((targetLoops-current_iteration))" ;
          else
            echo "False" > iteration.next &&
            echo "${targetLoops}" > iteration.total &&
            echo "Reached ${targetLoops} iterations";
          fi)
        expandArguments: none
      references:
      - fake_add:output
    """
    flowir_main = """

    environments:
      default:
        environment:
          DEFAULTS: PATH
          PATH: $PATH

    variables:
      default:
        global:
          targetLoops: 5

    components: 
    - stage: 0
      name: GenerateInput
      command: 
        executable: "echo"
        arguments: "0"

    - stage: 1
      $import: dowhile.yaml
      name: simple-do-while
      bindings:
        number: stage0.GenerateInput:output

    - stage: 2
      name: report
      command:
        executable: "echo"
        arguments: "stage1.add:output"
      references: ["stage1.add:output"]

    """
    populate_conf_for_dowhile_flowir(flowir_main, flowir_dowhile, output_dir)
    expPackage = experiment.model.storage.ExperimentPackage.packageFromLocation(output_dir)
    instance_dir = experiment.model.storage.ExperimentInstanceDirectory.newInstanceDirectory(
        '/tmp', package=expPackage)

    comp_experiment = experiment.model.data.Experiment(instance_dir, is_instance=True)
    comp_experiment.validateExperiment()

    return comp_experiment


def test_graph_instantiate_next_iter(dw_exp_simple):
    FlowIR = experiment.model.frontends.flowir.FlowIR
    workflowGraph = dw_exp_simple.experimentGraph

    do_while = list(workflowGraph._documents[FlowIR.LabelDoWhile].values())[0]['document']

    next_iteration = 1

    logger.info("DoWhile contents: %s" % pprint.pformat(do_while))

    workflowGraph.instantiate_dowhile_next_iteration(do_while, next_iteration, True)
    graph = workflowGraph.graph

    schema = {
        'stage0.GenerateInput': {
            'predecessors': [],
            'command': {
                'arguments': '0',
                'executable': 'echo',
                'expandArguments': 'double-quote',
                'interpreter': None,
                'resolvePath': True
            }
        },
        'stage1.0#noDependencies0': {
            'predecessors': [],
            'command': {
                'arguments': 'hello',
                'executable': 'echo',
                'expandArguments': 'double-quote',
                'interpreter': None,
                'resolvePath': True
            },
        },
        'stage1.1#noDependencies0': {
            'predecessors': [],
            'command': {
                'arguments': 'hello',
                'executable': 'echo',
                'expandArguments': 'double-quote',
                'interpreter': None,
                'resolvePath': True
            },
        },
        'stage1.0#add0': {
            'predecessors': ['stage0.GenerateInput'],
            'command': {
                'arguments': '$((1+stage0.GenerateInput:output))',
                'executable': 'echo',
                'expandArguments': 'double-quote',
                'interpreter': None,
                'resolvePath': True
            }
        },
        'stage1.1#add0': {
            'predecessors': ['stage1.0#fake_add'],
            'command': {
                'arguments': '$((1+stage1.0#fake_add:output))',
                'executable': 'echo',
                'expandArguments': 'double-quote',
                'interpreter': None,
                'resolvePath': True
            }
        },
        'stage1.0#fake_add': {
            'predecessors': ['stage1.0#add0'],
            'command': {
                'arguments': 'stage1.0#add0:output',
                'executable': 'echo',
                'expandArguments': 'double-quote',
                'interpreter': None,
                'resolvePath': True
            }
        },
        'stage1.1#fake_add': {
            'predecessors': ['stage1.1#add0'],
            'command': {
                'arguments': 'stage1.1#add0:output',
                'executable': 'echo',
                'expandArguments': 'double-quote',
                'interpreter': None,
                'resolvePath': True
            }
        },
        'stage1.0#stop': {
            'predecessors': ['stage1.0#fake_add'],
            'command': {
                'arguments': ('$( current_iteration=stage1.0#fake_add:output; '
                              'targetLoops=%(targetLoops)s ; '
                              'if [ "${current_iteration}" -lt "${targetLoops}" ]; then '
                                'echo "True" > iteration.next && '
                                'echo $(( targetLoops + 1 )) > iteration.total && '
                                'echo "Remaining iteration(s) $((targetLoops-current_iteration))" ; '
                              'else '
                                'echo "False" > iteration.next && '
                                'echo "${targetLoops}" > iteration.total && '
                                'echo "Reached ${targetLoops} iterations"; '
                              'fi)'),
                'executable': 'echo',
                'expandArguments': 'none',
                'interpreter': None,
                'resolvePath': True
            }
        },
        'stage1.1#stop': {
            'predecessors': ['stage1.1#fake_add'],
            'command': {
                'arguments': ('$( current_iteration=stage1.1#fake_add:output; '
                              'targetLoops=%(targetLoops)s ; '
                              'if [ "${current_iteration}" -lt "${targetLoops}" ]; then '
                                'echo "True" > iteration.next && '
                                'echo $(( targetLoops + 1 )) > iteration.total && '
                                'echo "Remaining iteration(s) $((targetLoops-current_iteration))" ; '
                              'else '
                                'echo "False" > iteration.next && '
                                'echo "${targetLoops}" > iteration.total && '
                                'echo "Reached ${targetLoops} iterations"; '
                              'fi)'),
                'executable': 'echo',
                'expandArguments': 'none',
                'interpreter': None,
                'resolvePath': True
            }
        },
        'stage2.report0': {
            'predecessors': ['stage1.0#add0', 'stage1.0#stop', 'stage1.1#add0', 'stage1.1#stop'],
            'command': {
                'arguments': 'stage1.add0:output',
                'executable': 'echo',
                'expandArguments': 'double-quote',
                'interpreter': None,
                'resolvePath': True
            }
        }
    }

    actual = {}

    concrete = workflowGraph._concrete

    for node in networkx.topological_sort(graph):
        cid = experiment.model.graph.ComponentIdentifier(node)

        comp_id = (cid.stageIndex, cid.componentName)
        config = concrete.get_component(comp_id, True)
        actual[node] = {
            'predecessors': sorted(graph.predecessors(node)),
            'command': config.get('command', {})
        }

    errors = experiment.model.frontends.flowir.validate_object_schema(actual, schema, 'DoWhile')

    for e in errors:
        logger.critical(e)
        print(e)

    assert schema == actual

    workflowGraph._flowir_configuration.store_unreplicated_flowir_to_disk()


def test_workflowgraph_loopref_iter0(dw_exp_simple):
    ref = experiment.model.graph.DataReference('stage1.add0:loopref')
    workflowGraph = dw_exp_simple.experimentGraph
    resolved = ref.resolve(workflowGraph)
    logger.info("Resolved %s to %s" % (ref.stringRepresentation, resolved))

    assert os.path.join(workflowGraph.rootStorage.location, 'stages', 'stage1', '0#add0') == resolved


def test_workflowgraph_loopref_iter1(dw_exp_simple):
    ref = experiment.model.graph.DataReference('stage1.add0:loopref')
    FlowIR = experiment.model.frontends.flowir.FlowIR
    workflowGraph = dw_exp_simple.experimentGraph

    do_while = list(workflowGraph._documents[FlowIR.LabelDoWhile].values())[0]['document']

    next_iteration = 1
    workflowGraph.instantiate_dowhile_next_iteration(do_while, next_iteration, True)

    assert 'bindings' in do_while

    resolved = ref.resolve(workflowGraph)
    logger.info("Resolved %s to %s" % (ref.stringRepresentation, resolved))
    resolved = resolved.split()
    stage_dir = os.path.join(workflowGraph.rootStorage.location, 'stages', 'stage1')

    assert os.path.join(stage_dir, '0#add0') == resolved[0]
    assert os.path.join(stage_dir, '1#add0') == resolved[1]


def test_controller_dowhile_0_loops(output_dir):
    flowir_dowhile = """
type: DoWhile

inputBindings:
    dependency:
        type: output

loopBindings:
    dependency: "stage0.loop:output"

condition: "stage0.loop/loop:output"

components:
- name: loop
  references:
  - dependency:output
  command:
    executable: sh
    # VV: Don't loop
    arguments: -c 'echo \"satisfy dependency:output\" && echo \"False\" >loop' 
    expandArguments: none
"""

    flowir_main = """
components:
- name: dummy
  stage: 0
  command:
    executable: echo
    arguments: Dummy output

- name: never-loop
  stage: 1
  $import: dowhile.yaml
  bindings:
    dependency: stage0.dummy:output
"""
    populate_conf_for_dowhile_flowir(flowir_main, flowir_dowhile, output_dir)
    expPackage = experiment.model.storage.ExperimentPackage.packageFromLocation(output_dir)
    instance_dir = experiment.model.storage.ExperimentInstanceDirectory.newInstanceDirectory(
        '/tmp', package=expPackage)

    comp_experiment = experiment.model.data.Experiment(instance_dir, is_instance=True)
    comp_experiment.validateExperiment()

    controller, components = new_controller(comp_experiment)

    class FakeStatus:
        def __init__(self):
            pass

        def monitorComponent(self, *args, **kwargs):
            pass

    for stage in comp_experiment.stages():
        controller.initialise(stage, FakeStatus())
        controller.run()

    controller.cleanUp()
    graph = comp_experiment.experimentGraph.graph

    known = ['stage1.0#loop', 'stage0.dummy']

    for node, data in graph.nodes(data=True):
        if node not in known:
            raise ValueError("Unexpected node %s with data %s" % (node, data))
        try:
            comp = data['component']()  # type: ComponentState
        except Exception as e:
            raise ValueError("Failed to get ComponentState of %s. Error: %s" % (node, e))
        if comp.state != experiment.model.codes.FINISHED_STATE:
            raise ValueError("Node %s was expected to be FINISHED but it's %s instead" % (node, comp.state))

    for expected in known:
        if expected not in graph.nodes:
            raise ValueError("Node %s is missing" % expected)


def test_controller_dowhile_1_loops(output_dir):
    flowir_dowhile = """
type: DoWhile

inputBindings: {}
loopBindings: {}
condition: "loop/iteration.next:output"

components:
- name: loop
  command:
    executable: echo
    expandArguments: none
    arguments: $(
          if [ "%(loopIteration)s" -lt "%(targetLoops)s" ]; then 
            echo "True" > iteration.next &&
            echo "Remaining iteration(s) $((%(targetLoops)s-%(loopIteration)s))" ;
          else
            echo "False" > iteration.next &&
            echo "Reached %(targetLoops)s iterations" ;  
          fi)
  variables:
    targetLoops: 1
"""

    flowir_main = """
components:
- name: never-loop
  $import: dowhile.yaml
  bindings: {}
"""
    populate_conf_for_dowhile_flowir(flowir_main, flowir_dowhile, output_dir)
    expPackage = experiment.model.storage.ExperimentPackage.packageFromLocation(output_dir)
    instance_dir = experiment.model.storage.ExperimentInstanceDirectory.newInstanceDirectory(
        '/tmp', package=expPackage)

    comp_experiment = experiment.model.data.Experiment(instance_dir, is_instance=True)
    comp_experiment.validateExperiment()

    controller, components = new_controller(comp_experiment)

    class FakeStatus:
        def __init__(self):
            pass

        def monitorComponent(self, *args, **kwargs):
            pass

    old_jobs = {}

    for stage in comp_experiment._stages:
        for job in stage.jobs():
            old_jobs[job.identification.identifier] = job

    new_jobs = {}
    for stage in comp_experiment.stages():
        controller.initialise(stage, FakeStatus())
        controller.run()
        for job in stage.jobs():
            new_jobs[job.identification.identifier] = job

    expected_new_jobs = set(['stage0.1#loop'])
    actual_new_jobs = set(new_jobs.keys()) - set(old_jobs.keys())
    assert  expected_new_jobs == actual_new_jobs

    controller.cleanUp()
    graph = comp_experiment.experimentGraph.graph

    known = ['stage0.0#loop', 'stage0.1#loop']

    for node, data in graph.nodes(data=True):
        if node not in known:
            raise ValueError("Unexpected node %s with data %s" % (node, data))
        try:
            comp = data['component']()  # type: ComponentState
        except Exception as e:
            raise ValueError("Failed to get ComponentState of %s. Error: %s" % (node, e))
        if comp.state != experiment.model.codes.FINISHED_STATE:
            raise ValueError("Node %s was expected to be FINISHED but it's %s instead" % (node, comp.state))

    for expected in known:
        if expected not in graph.nodes:
            raise ValueError("Node %s is missing" % expected)


def perform_test_failure_in_loop0_stage1(flowir_main, flowir_dowhile, where):
    populate_conf_for_dowhile_flowir(flowir_main, flowir_dowhile, where)
    expPackage = experiment.model.storage.ExperimentPackage.packageFromLocation(where)
    instance_dir = experiment.model.storage.ExperimentInstanceDirectory.newInstanceDirectory(
        '/tmp', package=expPackage)

    comp_experiment = experiment.model.data.Experiment(instance_dir, is_instance=True)
    comp_experiment.validateExperiment()

    controller, components = new_controller(comp_experiment)

    class FakeStatus:
        def __init__(self):
            pass

        def monitorComponent(self, *args, **kwargs):
            pass

    for stage in comp_experiment.stages():
        controller.initialise(stage, FakeStatus())
        if stage.index == 1:
            with pytest.raises(experiment.runtime.errors.UnexpectedJobFailureError) as e:
                controller.run()
        else:
            controller.run()

    controller.cleanUp()
    graph = comp_experiment.experimentGraph.graph

    known = ['stage1.0#loop', 'stage0.dummy']
    states = {
        'stage1.0#loop': experiment.model.codes.FAILED_STATE,
        'stage0.dummy': experiment.model.codes.FINISHED_STATE,
    }
    for node, data in graph.nodes(data=True):
        if node not in known:
            raise ValueError("Unexpected node %s with data %s" % (node, data))
        try:
            comp = data['component']()  # type: ComponentState
        except Exception as e:
            raise ValueError("Failed to get ComponentState of %s. Error: %s" % (node, e))
        if comp.state != states[node]:
            raise ValueError("Node %s was expected to be %s but it's %s instead" % (node, states[node], comp.state))

    for expected in known:
        if expected not in graph.nodes:
            raise ValueError("Node %s is missing" % expected)


def test_controller_dowhile_invalid_condition_output(output_dir):
    flowir_dowhile = """
type: DoWhile

inputBindings:
  dependency:
    type: output
loopBindings:
  dependency: "stage0.loop/loop:output"
# VV: component "loop" produces "loop" file which contains an invalid string
condition: "stage0.loop/loop:output"

components:
- name: loop
  references:
  - dependency:output
  command:
    executable: echo
    arguments: '"satisfy dependency: dependency:output but completely invalid" >loop'
    expandArguments: none
"""

    flowir_main = """
components:
- name: dummy
  stage: 0
  command:
    executable: echo
    arguments: Dummy output

- name: never-loop
  stage: 1
  $import: dowhile.yaml
  bindings:
    dependency: stage0.dummy:output
"""
    perform_test_failure_in_loop0_stage1(flowir_main, flowir_dowhile, output_dir)


def test_controller_dowhile_terminate_on_looped_failure(output_dir):
    flowir_dowhile = """
type: DoWhile

inputBindings:
  dependency:
    type: output

loopBindings:
  dependency: "stage0.loop/loop:output"

condition: "stage0.loop/will-never-be-checked-because-stage1.loop-will-fail:output"

components:
- name: loop
  references:
  - dependency:output
  command:
    executable: sh
    # VV: Make this fail
    arguments: '-c "echo \"satisfy dependency:output and then\" && exit 1"'
    expandArguments: none
"""

    flowir_main = """
components:
- name: dummy
  stage: 0
  command:
    executable: echo
    arguments: Dummy output

- name: never-loop
  stage: 1
  $import: dowhile.yaml
  bindings:
    dependency: stage0.dummy:output
"""
    perform_test_failure_in_loop0_stage1(flowir_main, flowir_dowhile, output_dir)


def test_controller_dowhile_condition_does_not_exist_output(output_dir):
    flowir_dowhile = """
type: DoWhile

inputBindings:
    dependency:
        type: output

loopBindings:
    dependency: "stage0.loop/loop:output"

# VV: file `missing` does not exist
condition: "stage0.loop/missing:output"

components:
- name: loop
  references:
  - dependency:output
  command:
    executable: echo
    arguments: '"satisfy dependency: dependency:output" >loop'
    expandArguments: none
"""

    flowir_main = """
components:
- name: dummy
  stage: 0
  command:
    executable: echo
    arguments: Dummy output

- name: never-loop
  stage: 1
  $import: dowhile.yaml
  bindings:
    dependency: stage0.dummy:output
"""
    perform_test_failure_in_loop0_stage1(flowir_main, flowir_dowhile, output_dir)


def test_parse_invalid_bindings(output_dir):
    flowir_dowhile = """
type: DoWhile

inputBindings:
    dependency:
        type: output

loopBindings:
    dependency: "stage1.loop/loop:output"

condition: "stage0.loop/loop:output"

components:
- name: loop
  references:
  - dependency:output
  command:
    executable: echo
    arguments: '"satisfy dependency: dependency:output" >loop'
    expandArguments: none
"""

    flowir_main = """
components:
- name: dummy
  stage: 0
  command:
    executable: echo
    arguments: Dummy output

- name: never-loop
  stage: 1
  $import: dowhile.yaml
  bindings:
    # VV: `dummy` component does not exist in stage 1
    dependency: dummy:output
"""

    populate_conf_for_dowhile_flowir(flowir_main, flowir_dowhile, output_dir)

    with pytest.raises(experiment.model.errors.ExperimentInvalidConfigurationError) as e:
        expPackage = experiment.model.storage.ExperimentPackage.packageFromLocation(output_dir)
        instance_dir = experiment.model.storage.ExperimentInstanceDirectory.newInstanceDirectory(
            '/tmp', package=expPackage)
        _ = experiment.model.data.Experiment(instance_dir, is_instance=True)
    exc = e.value.underlyingError  # type: experiment.model.errors.FlowIRConfigurationErrors
    assert len(exc.underlyingErrors) == 1
    exc = exc.underlyingErrors[0]  # type: experiment.model.errors.FlowIRReferenceToUnknownComponent

    assert exc.stage == 1
    assert exc.component == "dummy"
    assert exc.references == ['stage1.dummy:output']


def test_dowhile_modify_filename_valid(output_dir):
    flowir_dowhile = """
    type: DoWhile

    inputBindings:
        dep_as_is:
            type: copy
        dep_modify:
            type: copy

    condition: "stage0.loop:output"

    components:
    - name: loop
      command:
        executable: echo
        arguments: "false"
    - name: use_as_is
      references:
      - dep_as_is:copy
      command:
        executable: cat
        arguments: test.out 
    - name: modify
      references:
      - dep_modify/test.out:copy
      command:
        executable: cat
        arguments: test.out 
    """

    flowir_main = """
    components:
    - name: dummy
      stage: 0
      command:
        executable: echo
        arguments: "hello >test.out"

    - name: dowhile_creator
      stage: 1
      $import: dowhile.yaml
      bindings:
        dep_as_is: stage0.dummy/test.out:copy
        dep_modify: stage0.dummy:copy
    """

    populate_conf_for_dowhile_flowir(flowir_main, flowir_dowhile, output_dir)
    expPackage = experiment.model.storage.ExperimentPackage.packageFromLocation(output_dir)
    instance_dir = experiment.model.storage.ExperimentInstanceDirectory.newInstanceDirectory(
        '/tmp', package=expPackage)
    comp_experiment = experiment.model.data.Experiment(instance_dir, is_instance=True)
    comp_experiment.validateExperiment()


def test_dowhile_modify_filename_invalid(output_dir):
    flowir_dowhile = """
    type: DoWhile

    inputBindings:
        dependency:
            type: copy

    condition: "loop:output"

    components:
    - name: loop
      command:
        executable: echo
        arguments: "false"
    - name: invalid
      references:
      - dependency/modify/if/value/set/by/dowhile_creator/does/not/specify/filename:copy
      command:
        executable: cat
        arguments: filename  
    """

    flowir_main = """
    components:
    - name: dummy
      stage: 0
      command:
        executable: echo
        arguments: "hello >test.out"

    - name: dowhile_creator
      stage: 1
      $import: dowhile.yaml
      bindings:
        dependency: stage0.dummy/test.out:copy
    """

    populate_conf_for_dowhile_flowir(flowir_main, flowir_dowhile, output_dir)
    with pytest.raises(experiment.model.errors.ExperimentInvalidConfigurationError) as e:
        expPackage = experiment.model.storage.ExperimentPackage.packageFromLocation(output_dir)
        instance_dir = experiment.model.storage.ExperimentInstanceDirectory.newInstanceDirectory('/tmp', package=expPackage)
        _ = experiment.model.data.Experiment(instance_dir, is_instance=True)

    exc = e.value.underlyingError  # type: experiment.model.errors.FlowIRConfigurationErrors
    assert len(exc.underlyingErrors) == 1
    exc = exc.underlyingErrors[0]  # type: experiment.model.errors.FlowIRInvalidBindingModifyFilename

    assert exc.binding_ref == "stage0.dummy/test.out:copy"
    assert exc.binding_usage_ref == "dependency/modify/if/value/set/by/dowhile_creator/does/not/specify/filename:copy"


def test_dowhile_loopbindings_stage_offset(output_dir):
    """Instantiates a DoWhile document which loops twice and uses loopBindings

    Ensures that loopBindings are properly evaluated and double checks the references of the resulting
    components
    """
    dowhile="""
type: DoWhile

inputBindings:
  other:
    type: output

loopBindings:
  other: "stage1.echo:output"

components:
- name: echo
  stage: 1
  command:
    executable: "echo"
    arguments: "other:output"
  references:
  - "other:output"

- name: stop
  command:
    executable: sh
    expandArguments: none
    arguments: '-c
      "if [ %(loopIteration)s -gt 0 ] ; then
          echo False;
       else
          echo True;
       fi"'

condition: 'stop:output'
"""

    flowir = """
components:
- name: zero
  command:
    executable: echo
    arguments: zero
- name: one
  stage: 1
  command:
    executable: echo
    arguments: one
- name: imp-one
  stage: 1
  $import: dowhile.yaml
  bindings:
    other: "stage1.one:output"
"""
    populate_conf_for_dowhile_flowir(flowir, dowhile, output_dir)
    expPackage = experiment.model.storage.ExperimentPackage.packageFromLocation(output_dir)
    instance_dir = experiment.model.storage.ExperimentInstanceDirectory.newInstanceDirectory('/tmp', package=expPackage)
    comp_experiment = experiment.model.data.Experiment(instance_dir, is_instance=True)

    controller, components = new_controller(comp_experiment)

    class FakeStatus:
        def __init__(self):
            pass
        def monitorComponent(self, *args, **kwargs):
            pass

    for stage in comp_experiment.stages():
        controller.initialise(stage, FakeStatus())
        controller.run()

    controller.cleanUp()

    # VV: Maps the components (looped and non-looped alike) with their references:
    # stage2.0#echo is expected to have a dependency to a binding which points to stage1.one:output
    # stage2.1#echo is expected to have a dependency to a loopBinding which evaluates to stage2.0#echo:output
    real_components = {}
    for comp_name, data in comp_experiment.graph.nodes(data=True):
        try:
            component = data['component']()  # type: "ComponentState"
        except KeyError:
            pass
        real_components[comp_name] = [x.absoluteReference for x in component.specification.dataReferences]

    assert real_components == {
        'stage0.zero': [],
        'stage1.0#stop': [],
        'stage1.1#stop': [],
        'stage1.one': [],
        'stage2.0#echo': ['stage1.one:output'],
        'stage2.1#echo': ['stage2.0#echo:output']}


def test_dowhile_loop_aggregators_shutdown_logic(output_dir):
    """Ensures that controler treats nodes which contain :loopref and :loopoutput references as aggregating ones
    """
    dowhile="""
type: DoWhile

inputBindings:
  other:
    type: output

loopBindings:
  other: "stage1.echo:output"

components:
- name: echo
  stage: 1
  command:
    executable: sh
    expandArguments: none
    arguments: '-c
      "if [ %(loopIteration)s -gt 0 ] ; then
          exit 1
       else
          echo other:output;
       fi"'
  references:
  - "other:output"
  workflowAttributes:
    shutdownOn:
    - KnownIssue
- name: stop
  command:
    executable: sh
    expandArguments: none
    arguments: '-c
      "if [ %(loopIteration)s -gt 0 ] ; then
          echo False;
       else
          echo True;
       fi"'

condition: 'stop:output'
"""

    flowir = """
components:
- name: zero
  command:
    executable: echo
    arguments: zero
- name: one
  stage: 1
  command:
    executable: echo
    arguments: one
- name: imp-one
  stage: 1
  $import: dowhile.yaml
  bindings:
    other: "stage1.one:output"
- name: loop-aggregator
  stage: 2
  references:
  - stage2.echo:loopref
  command:
    executable: echo
    arguments: stage2.echo:loopref
- name: loop-plain
  stage: 2
  references:
  - stage2.echo:ref
  command:
    executable: echo
    arguments: stage2.echo:ref
"""
    populate_conf_for_dowhile_flowir(flowir, dowhile, output_dir)
    expPackage = experiment.model.storage.ExperimentPackage.packageFromLocation(output_dir)
    instance_dir = experiment.model.storage.ExperimentInstanceDirectory.newInstanceDirectory('/tmp', package=expPackage)
    comp_experiment = experiment.model.data.Experiment(instance_dir, is_instance=True)

    controller, components = new_controller(comp_experiment)

    class FakeStatus:
        def __init__(self):
            pass

        def monitorComponent(self, *args, **kwargs):
            pass

    for stage in comp_experiment.stages():
        controller.initialise(stage, FakeStatus())
        controller.run()

    controller.cleanUp()

    loop_aggregator = comp_experiment.graph.nodes['stage2.loop-aggregator']['component']()  # type: "ComponentState"
    loop_plain = comp_experiment.graph.nodes['stage2.loop-plain']['component']()  # type: "ComponentState"

    assert loop_aggregator.state == experiment.model.codes.FINISHED_STATE
    assert loop_plain.state == experiment.model.codes.SHUTDOWN_STATE
