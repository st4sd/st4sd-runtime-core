# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# coding=UTF-8
#
# IBM Confidential
# OCO Source Materials
# 5747-SM3
# (c) Copyright IBM Corp. 2021
# The source code for this program is not published or otherwise
# divested of its trade secrets, irrespective of what has
# been deposited with the U.S. Copyright Office.
# Author(s): Vassilis Vassiliadis
# Contact: Vassilis.Vassiliadis@ibm.com

from __future__ import print_function

import pytest

from collections import OrderedDict

import experiment.model.conf
import experiment.model.errors
import experiment.model.frontends.flowir
import experiment.model.graph
from . import utils


def test_graph_from_dictionaries():
    flowir = experiment.model.frontends.flowir.yaml_load("""
    components:
    - name: hello
      command:
        executable: ls
        arguments: -lth fromManifest:ref
      references:
      - fromManifest:ref
    """)
    graph = experiment.model.graph.WorkflowGraph.graphFromFlowIR(flowir, {'fromManifest': 'does not matter'})

    assert list(graph.graph.nodes) == ['stage0.hello']


def test_graph_from_dictionaries_and_application_dependencies():
    flowir = experiment.model.frontends.flowir.yaml_load("""
        application-dependencies:
          default:
          - dataset
        components:
        - name: hello
          command:
            executable: ls
            arguments: -lth dataset:ref fromManifest:ref
          references:
          - dataset:ref
          - fromManifest:ref
        """)
    graph = experiment.model.graph.WorkflowGraph.graphFromFlowIR(flowir, {'fromManifest': 'does not matter'})

    assert list(graph.graph.nodes) == ['stage0.hello']


def test_graph_from_dictionaries_missing_dependency():
    flowir = experiment.model.frontends.flowir.yaml_load("""
    components:
    - name: hello
      command:
        executable: ls
        arguments: -lth notInManifest:ref
      references:
      - notInManifest:ref
    """)

    # VV: Reference `notInManifest` is invalid, because it:
    # a) does not point to a component, and
    # b) does not point to an application-dependency source, and
    # c) is is not declared in the manifest
    with pytest.raises(experiment.model.errors.ExperimentInvalidConfigurationError) as e:
        _ = experiment.model.graph.WorkflowGraph.graphFromFlowIR(flowir, {'fromManifest': 'does not matter'})

    exc = e.value.underlyingError.underlyingErrors

    assert isinstance(exc, list) and len(exc) == 2

    in_args = [x for x in exc if isinstance(x, experiment.model.errors.FlowIRUnknownReferenceInArguments)]
    unknown_comp = [x for x in exc if isinstance(x, experiment.model.errors.FlowIRReferenceToUnknownComponent)]

    assert len(in_args) == 1
    assert len(unknown_comp) == 1

    exc: experiment.model.errors.FlowIRUnknownReferenceInArguments = in_args[0]
    assert exc.ref_unknown == 'notInManifest:ref'

    exc: experiment.model.errors.FlowIRReferenceToUnknownComponent = unknown_comp[0]

    assert exc.references == ['stage0.notInManifest']


def test_graph_invalid_reference_to_component_inside_arguments(output_dir):
    flowir = """
    components:
    - name: missing
      command:
        executable: echo
        arguments: hello world
    - name: should-error
      stage: 1
      command:
        executable: echo
        arguments: missing:ref
      references:
      - stage0.missing:ref
    """
    with pytest.raises(experiment.model.errors.ExperimentInvalidConfigurationError) as e:
        _ = utils.experiment_from_flowir(flowir, output_dir)

    exc = e.value.underlyingError.underlyingErrors
    assert isinstance(exc, list) and len(exc) == 1

    exc: experiment.model.errors.FlowIRUnknownReferenceInArguments = exc[0]
    assert exc.ref_unknown == "missing:ref"


def test_graph_generate_new_dsl_component():
    flowir = experiment.model.frontends.flowir.yaml_load("""
        application-dependencies:
          default:
          - dataset
        environments:
          default:
            my-env:
              FOO: bar
        components:
        - name: hello
          command:
            executable: sh
            arguments: -c "hello %(name)s; ls -lth dataset:ref fromManifest:ref"
            expandArguments: "none"
            environment: my-env
          references:
          - dataset:ref
          - fromManifest:ref
          variables:
            name: wisdom
        """)
    graph = experiment.model.graph.WorkflowGraph.graphFromFlowIR(flowir, {'fromManifest': 'does not matter'})

    spec: experiment.model.graph.ComponentSpecification = graph.graph.nodes['stage0.hello']['componentSpecification']

    dsl = spec.dsl_component_blueprint()

    dsl = {
        name: dict(value) if isinstance(value, OrderedDict) else value for (name, value) in dsl.items()
    }

    assert dsl == {
        'signature': {
            'name': 'stage0.hello',
            'parameters': [
                {'name': 'param0'},
                {'name': 'param1'},
                {
                    'name': 'env-vars',
                    'default': {
                        'FOO': 'bar'
                    }
                },
            ]},
        'variables': {
            'name': 'wisdom'
        },
        'command': {'executable': 'sh', 'arguments': '-c "hello %(name)s; ls -lth %(param0)s %(param1)s"',
                    'resolvePath': True, 'expandArguments': 'none', 'interpreter': None, 'environment': '$(env-vars)s'},
        'resourceRequest': {'numberProcesses': 1, 'numberThreads': 1, 'ranksPerNode': 1, 'threadsPerCore': 1,
                            'memory': None, 'gpus': None},
        'workflowAttributes': {'restartHookFile': None, 'aggregate': False, 'replicate': None,
                               'isMigratable': False, 'isMigrated': False, 'repeatInterval': None,
                               'repeatRetries': 3, 'maxRestarts': None, 'shutdownOn': [], 'isRepeat': False,
                               'memoization': {'disable': {'strong': False, 'fuzzy': False},
                                               'embeddingFunction': None},
                               'optimizer': {'disable': False, 'exploitChance': 0.9, 'exploitTarget': 0.75,
                                             'exploitTargetLow': 0.25, 'exploitTargetHigh': 0.5},
                               'restartHookOn': ['ResourceExhausted']},
        'resourceManager': {'config': {'backend': 'local', 'walltime': 60.0},
                            'lsf': {'statusRequestInterval': 20, 'queue': 'normal', 'reservation': None,
                                    'resourceString': None, 'dockerImage': None, 'dockerProfileApp': None,
                                    'dockerOptions': None},
                            'kubernetes': {'image': None, 'qos': None, 'image-pull-secret': None,
                                           'namespace': 'default', 'api-key-var': None,
                                           'host': 'http://localhost:8080', 'cpuUnitsPerCore': None,
                                           'gracePeriod': None, 'podSpec': None}, 'docker': {'image': None}}}


def test_graph_generate_new_dsl_component_platform_variables():
    flowir = experiment.model.frontends.flowir.yaml_load("""
        application-dependencies:
          default:
          - dataset
        environments:
          default:
            my-env:
              FOO: bar
        variables:
          default:
            global:
                from-platform: this should not have a default value in the signature of
                  the component.
              
        components:
        - name: hello
          command:
            executable: sh
            arguments: -c "hello %(name)s and %(from-platform)s; ls -lth dataset:ref fromManifest:ref"
            expandArguments: "none"
            environment: my-env
          references:
          - dataset:ref
          - fromManifest:ref
          variables:
            name: wisdom
        """)
    graph = experiment.model.graph.WorkflowGraph.graphFromFlowIR(flowir, {'fromManifest': 'does not matter'})

    spec: experiment.model.graph.ComponentSpecification = graph.graph.nodes['stage0.hello']['componentSpecification']

    dsl = spec.dsl_component_blueprint()

    dsl = {
        name: dict(value) if isinstance(value, OrderedDict) else value for (name, value) in dsl.items()
    }

    assert dsl['signature']['parameters'] == [
        {'name': 'param0'},
        {'name': 'param1'},
        {'name': 'from-platform'},
        {
            'name': 'env-vars',
            'default': {
                'FOO': 'bar'
            }
        },
    ]

    assert dsl['variables'] == {
        'name': 'wisdom'
    }


def test_graph_generate_new_dsl_workflow_one_component():
    flowir = experiment.model.frontends.flowir.yaml_load("""
        application-dependencies:
          default:
          - dataset
        environments:
          default:
            my-env:
              FOO: bar
        variables:
          default:
            global:
                from-platform: in-entrypoint

        components:
        - name: hello
          command:
            executable: sh
            arguments: -c "hello %(name)s and %(from-platform)s; ls -lth dataset:ref fromManifest:ref"
            expandArguments: "none"
            environment: my-env
          references:
          - dataset:ref
          - fromManifest:ref
          variables:
            name: wisdom
        """)
    graph = experiment.model.graph.WorkflowGraph.graphFromFlowIR(flowir, {'fromManifest': 'does not matter'})

    dsl = graph.to_dsl()

    assert dsl['entrypoint'] == {
        'entry-instance': 'main',
        'execute': [
            {
                'target': '<main>',
                'args': {
                    'from-platform': "in-entrypoint"
                }
            },
        ]
    }

    assert len(dsl['workflows']) == 1

    workflow = dsl['workflows'][0]

    assert workflow['signature'] == {
        'name': 'main',
        'parameters': [
            {'name': 'param0', 'default': 'dataset:ref'},
            {'name': 'param1', 'default': 'fromManifest:ref'},
            {'name': 'from-platform', 'default': 'in-entrypoint'},
        ]
    }

    assert workflow['steps'] == {'stage0.hello': 'stage0.hello'}

    assert workflow['execute'] == [
                {
                    'target': '<stage0.hello>',
                    'args': {
                        'from-platform': '%(from-platform)s',
                        'param0': '%(param0)s',
                        'param1': '%(param1)s'
                    }
                }
            ]


def test_graph_generate_new_dsl_workflow_two_components():
    flowir = experiment.model.frontends.flowir.yaml_load("""
        application-dependencies:
          default:
          - dataset
        environments:
          default:
            my-env:
              FOO: bar
        variables:
          default:
            global:
                from-platform: in-entrypoint

        components:
        - name: hello
          command:
            executable: sh
            arguments: -c "hello %(name)s and %(from-platform)s; ls -lth dataset:ref fromManifest:ref"
            expandArguments: "none"
            environment: my-env
          references:
          - dataset:ref
          - fromManifest:ref
          variables:
            name: wisdom
        
        - name: consume
          command:
            executable: ls
            arguments: stage0.hello:ref
          references:
          - stage0.hello:ref
        """)
    graph = experiment.model.graph.WorkflowGraph.graphFromFlowIR(flowir, {'fromManifest': 'does not matter'})

    dsl = graph.to_dsl()

    assert dsl['entrypoint'] == {
        'entry-instance': 'main',
        'execute': [
            {
                'target': '<main>',
                'args': {
                    'from-platform': "in-entrypoint"
                }
            },
        ]
    }

    assert len(dsl['workflows']) == 1

    workflow = dsl['workflows'][0]

    assert workflow['signature'] == {
        'name': 'main',
        'parameters': [
            {'name': 'param0', 'default': 'dataset:ref'},
            {'name': 'param1', 'default': 'fromManifest:ref'},
            {'name': 'from-platform', 'default': 'in-entrypoint'},
        ]
    }

    assert workflow['steps'] == {
        'stage0.hello': 'stage0.hello',
        'stage0.consume': 'stage0.consume',
    }

    assert workflow['execute'] == [
        {
            'target': '<stage0.hello>',
            'args': {
                'from-platform': '%(from-platform)s',
                'param0': '%(param0)s',
                'param1': '%(param1)s'
            }
        },
        {
            'target': '<stage0.consume>',
            'args': {
                'param0': '<stage0.hello>:ref',
            }
        }
    ]


def test_graph_generate_new_dsl_workflow_one_component_input_file():
    flowir = experiment.model.frontends.flowir.yaml_load("""
        application-dependencies:
          default:
          - dataset
        environments:
          default:
            my-env:
              FOO: bar
        variables:
          default:
            global:
                from-platform: in-entrypoint

        components:
        - name: hello
          command:
            executable: sh
            arguments: -c "hello %(name)s and %(from-platform)s; ls -lth dataset:ref fromManifest:ref; cat input/hi:ref"
            expandArguments: "none"
            environment: my-env
          references:
          - dataset:ref
          - fromManifest:ref
          - input/hi:ref
          variables:
            name: wisdom
        """)
    graph = experiment.model.graph.WorkflowGraph.graphFromFlowIR(flowir, {'fromManifest': 'does not matter'})

    dsl = graph.to_dsl()

    assert len(dsl['workflows']) == 1

    workflow = dsl['workflows'][0]

    workflow = {name: dict(value) if isinstance(value, OrderedDict) else value for (name, value) in workflow.items()}

    assert workflow['signature'] == {
        'name': 'main',
        'parameters': [
            {'name': 'param2'},
            {'name': 'param0', 'default': 'dataset:ref'},
            {'name': 'param1', 'default': 'fromManifest:ref'},
            {'name': 'from-platform', 'default': 'in-entrypoint'},
        ]
    }

    assert workflow['steps'] == {'stage0.hello': 'stage0.hello'}

    assert workflow['execute'] == [
        {
            'target': '<stage0.hello>',
            'args': {
                'from-platform': '%(from-platform)s',
                'param0': '%(param0)s',
                'param1': '%(param1)s',
                'param2': '%(param2)s'
            }
        }
    ]


def test_graph_generate_new_dsl_workflow_one_component_and_platform():
    flowir = experiment.model.frontends.flowir.yaml_load("""
        application-dependencies:
          default:
          - dataset
        environments:
          default:
            my-env:
              FOO: bar
        variables:
          default:
            global:
                from-platform: in-entrypoint
          extra:
            global:
                another-var: in-entrypoint

        components:
        - name: hello
          command:
            executable: sh
            arguments: -c "hello %(name)s and %(from-platform)s and %(another-var)s; ls -lth dataset:ref fromManifest:ref"
            expandArguments: "none"
            environment: my-env
          references:
          - dataset:ref
          - fromManifest:ref
          variables:
            name: wisdom
        """)
    graph = experiment.model.graph.WorkflowGraph.graphFromFlowIR(flowir, {'fromManifest': 'does not matter'},
                                                                 platform="extra")

    dsl = graph.to_dsl()

    assert dsl['entrypoint'] == {
        'entry-instance': 'main',
        'execute': [
            {
                'target': '<main>',
                'args': {
                    'from-platform': "in-entrypoint",
                    'another-var': "in-entrypoint"
                }
            },
        ]
    }

    assert len(dsl['workflows']) == 1

    workflow = dsl['workflows'][0]

    assert workflow['signature'] == {
        'name': 'main',
        'parameters': [
            {'name': 'param0', 'default': 'dataset:ref'},
            {'name': 'param1', 'default': 'fromManifest:ref'},
            {'name': 'another-var', 'default': 'in-entrypoint'},
            {'name': 'from-platform', 'default': 'in-entrypoint'},
        ]
    }

    assert workflow['steps'] == {'stage0.hello': 'stage0.hello'}

    assert workflow['execute'] == [
                {
                    'target': '<stage0.hello>',
                    'args': {
                        'from-platform': '%(from-platform)s',
                        'another-var': '%(another-var)s',
                        'param0': '%(param0)s',
                        'param1': '%(param1)s'
                    }
                }
            ]


def test_graph_generate_new_dsl_workflow_one_component_param_order():
    flowir = experiment.model.frontends.flowir.yaml_load("""
        application-dependencies:
          default:
          - dataset
        environments:
          default:
            my-env:
              FOO: bar
        
        variables:
          default:
            global:
              backend: kubernetes

        components:
        - name: hello
          command:
            executable: sh
            arguments: -c "hello %(backend)s; ls -lth dataset:ref; cat input/msg.txt:ref"
            expandArguments: "none"
            environment: my-env
          references:
          - dataset:ref
          - input/msg.txt:ref
        """)
    graph = experiment.model.graph.WorkflowGraph.graphFromFlowIR(flowir, {})

    dsl = graph.to_dsl()

    assert len(dsl['workflows']) == 1

    workflow = dsl['workflows'][0]

    assert workflow['signature'] == {
        'name': 'main',
        'parameters': [
            {'name': 'param1'},
            {'name': 'param0', 'default': 'dataset:ref'},
            {'name': 'backend', 'default': 'kubernetes'}
        ]
    }
