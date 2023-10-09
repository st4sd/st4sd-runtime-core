# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis


from __future__ import print_function

import typing

import pytest

from collections import OrderedDict

import experiment.model.conf
import experiment.model.errors
import experiment.model.frontends.flowir
import experiment.model.graph
import experiment.model.frontends.dsl
import experiment.test

import os

import yaml

from . import utils

from .test_dsl import (
    dsl_one_workflow_one_component_one_step_no_datareferences,
)


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
                    'resolvePath': True, 'expandArguments': 'none', 'interpreter': None, 'environment': '%(env-vars)s'},
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
                                           'gracePeriod': None, 'podSpec': None},
                            'docker': {'image': None, 'imagePullPolicy': 'Always', 'platform': None}}}


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
                'target': '<entry-instance>',
                'args': {
                    'from-platform': "in-entrypoint",
                    'manifest.fromManifest': 'fromManifest',
                    'manifest.dataset': 'dataset'
                }
            },
        ]
    }

    assert len(dsl['workflows']) == 1

    workflow = dsl['workflows'][0]

    assert workflow['signature'] == {
        'name': 'main',
        'parameters': [
            {'name': 'manifest.dataset', 'default': 'dataset'},
            {'name': 'manifest.fromManifest', 'default': 'fromManifest'},
            {'name': 'from-platform', 'default': 'in-entrypoint'},
        ]
    }

    assert workflow['steps'] == {'stage0.hello': 'stage0.hello'}

    assert workflow['execute'] == [
        {
            'target': '<stage0.hello>',
            'args': {
                'from-platform': '%(from-platform)s',
                'param0': '"%(manifest.dataset)s":ref',
                'param1': '"%(manifest.fromManifest)s":ref'
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
                'target': '<entry-instance>',
                'args': {
                    'from-platform': "in-entrypoint",
                    'manifest.dataset': "dataset",
                    'manifest.fromManifest': "fromManifest",
                }
            },
        ]
    }

    assert len(dsl['workflows']) == 1

    workflow = dsl['workflows'][0]

    assert workflow['signature'] == {
        'name': 'main',
        'parameters': [
            {'name': 'manifest.dataset', 'default': 'dataset'},
            {'name': 'manifest.fromManifest', 'default': 'fromManifest'},
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
                'param0': '"%(manifest.dataset)s":ref',
                'param1': '"%(manifest.fromManifest)s":ref'
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

    assert dsl['entrypoint']['execute'][0]['args'] == {
        'from-platform': 'in-entrypoint',
        'manifest.dataset': 'dataset',
        'manifest.fromManifest': 'fromManifest',
        'input.hi': 'input/hi'
    }

    assert workflow['signature'] == {
        'name': 'main',
        'parameters': [
            {'name': 'input.hi'},
            {'name': 'manifest.dataset', 'default': 'dataset'},
            {'name': 'manifest.fromManifest', 'default': 'fromManifest'},
            {'name': 'from-platform', 'default': 'in-entrypoint'},
        ]
    }

    assert workflow['steps'] == {'stage0.hello': 'stage0.hello'}

    assert workflow['execute'] == [
        {
            'target': '<stage0.hello>',
            'args': {
                'from-platform': '%(from-platform)s',
                'param0': '"%(manifest.dataset)s":ref',
                'param1': '"%(manifest.fromManifest)s":ref',
                'param2': '"%(input.hi)s":ref'
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
                'target': '<entry-instance>',
                'args': {
                    'from-platform': "in-entrypoint",
                    'another-var': "in-entrypoint",
                    'manifest.dataset': "dataset",
                    'manifest.fromManifest': "fromManifest"
                }
            },
        ]
    }

    assert len(dsl['workflows']) == 1

    workflow = dsl['workflows'][0]

    assert workflow['signature'] == {
        'name': 'main',
        'parameters': [
            {'name': 'manifest.dataset', 'default': 'dataset'},
            {'name': 'manifest.fromManifest', 'default': 'fromManifest'},
            {'name': 'another-var', 'default': 'in-entrypoint'},
            {'name': 'from-platform', 'default': 'in-entrypoint'},
        ]
    }

    assert workflow['steps'] == {'stage0.hello': 'stage0.hello'}

    assert len(workflow['execute']) == 1

    step0 = workflow['execute'][0]
    assert step0 == {
        'target': '<stage0.hello>',
        'args': {
            'from-platform': '%(from-platform)s',
            'another-var': '%(another-var)s',
            'param0': '"%(manifest.dataset)s":ref',
            'param1': '"%(manifest.fromManifest)s":ref'
        }
    }


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
            {'name': 'input.msg.txt'},
            {'name': 'manifest.dataset', 'default': 'dataset'},
            {'name': 'backend', 'default': 'kubernetes'}
        ]
    }


def test_graph_generate_new_dsl_workflow_double_reference_workflow_parameter():
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
          - input/msg.txt:copy
          - data/other.txt:copy
        """)
    graph = experiment.model.graph.WorkflowGraph.graphFromFlowIR(flowir, {})

    dsl = graph.to_dsl()

    assert len(dsl['workflows']) == 1

    workflow = dsl['workflows'][0]

    assert workflow['signature'] == {
        'name': 'main',
        'parameters': [
            {'name': 'input.msg.txt'},
            {'name': 'data.other.txt', 'default': 'data/other.txt'},
            {'name': 'manifest.dataset', 'default': 'dataset'},
            {'name': 'backend', 'default': 'kubernetes'}
        ]
    }

    step0 = workflow['execute'][0]
    assert step0 == {
        'target': '<stage0.hello>',
        'args': {
            'backend': '%(backend)s',
            'param0': '"%(manifest.dataset)s":ref',
            'param1': '"%(input.msg.txt)s":ref',
            'param2': '"%(input.msg.txt)s":copy',
            'param3': '"%(data.other.txt)s":copy',
        }
    }

    entrypoint_args = dsl['entrypoint']['execute'][0]['args']

    assert entrypoint_args == {
        'backend': 'kubernetes',
        'input.msg.txt': 'input/msg.txt',
        'data.other.txt': 'data/other.txt',
        'manifest.dataset': 'dataset'
    }


def test_graph_returns_actual_dsl2(
    output_dir: str,
    dsl_one_workflow_one_component_one_step_no_datareferences: typing.Dict[str, typing.Any],
):
    dsl = experiment.model.frontends.dsl.Namespace(**dsl_one_workflow_one_component_one_step_no_datareferences)

    # VV: command.environment poitns to the "environment" parameter. Resolve it fully here to double check that
    # the DSL we get out of the workflowGraph is the actual DSL we stored in the file and not the hallucinated one
    dsl.components[0].command.environment = [
        p for p in dsl.components[0].signature.parameters if p.name == "environment"
    ][0].default

    dsl_raw = dsl.dict(
        by_alias=True, exclude_none=True, exclude_defaults=True, exclude_unset=True
    )

    pkg_dir = os.path.join(output_dir, "workflow.package")
    utils.populate_files(pkg_dir, {"conf/dsl.yaml": yaml.safe_dump(dsl_raw)})

    instance_dir = os.path.join(output_dir, "package.instance")
    os.makedirs(instance_dir)
    isValid, error, compExperiment = experiment.test.ValidatePackage(pkg_dir, location=instance_dir)

    assert isValid

    dsl_loaded = compExperiment.experimentGraph.to_dsl()

    dsl = experiment.model.frontends.dsl.Namespace(**dsl_loaded)

    # VV: The hallucinated environment would have been "%(env-vars)s"
    assert dsl.components[0].command.environment == {
        "DEFAULTS": "PATH:LD_LIBRARY_PATH",
        "AN_ENV_VAR": "ITS_VALUE",
    }
