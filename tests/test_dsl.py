# Copyright IBM Inc. 2015, 2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis

import typing

import pytest
import yaml

import experiment.model.frontends.flowir
import experiment.model.graph
import experiment.model.frontends.dsl


@pytest.fixture()
def simple_flowir() -> experiment.model.frontends.flowir.DictFlowIR:
    return experiment.model.frontends.flowir.yaml_load("""
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


@pytest.fixture()
def dsl_one_workflow_one_component_one_step_no_datareferences() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load("""
    entrypoint:
      entry-instance: main
      execute:
      - args:
          foo: world
    workflows:
    - signature:
        name: main
        parameters:
        - name: foo
      steps:
        greetings: echo
      execute:
      - target: <greetings>
        args:
          message: "hello"
          other: "%(foo)s"
    components:
    - signature:
        name: echo
        parameters:
        - name: message
        - name: other
          default: a default value
        - name: environment
          default:
            DEFAULTS: PATH:LD_LIBRARY_PATH
            AN_ENV_VAR: ITS_VALUE
      command:
        environment: "%(environment)s"
        executable: echo
        arguments: "%(message)s %(other)s"        
    """)


@pytest.fixture()
def dsl_one_workflow_one_component_two_steps_no_edges() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load("""
    entrypoint:
      entry-instance: main
      execute:
      - args:
          foo: world
    workflows:
    - signature:
        name: main
        parameters:
        - name: foo
        - name: unused
          default: value-unused
      steps:
        one: echo
        two: echo
      execute:
      - target: <one>
        args:
          message: "hello from one"
          other: "%(foo)s"
      - target: <two>
        args:
          message: "hello from two"
          other: "%(foo)s"
    components:
    - signature:
        name: echo
        parameters:
        - name: message
        - name: other
          default: a default value
        - name: environment
          default:
            DEFAULTS: PATH:LD_LIBRARY_PATH
            AN_ENV_VAR: ITS_VALUE
      command:
        environment: "%(environment)s"
        executable: echo
        arguments: "%(message)s %(other)s"        
    """)


@pytest.fixture()
def dsl_one_workflow_one_component_two_steps_with_edges() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load("""
    entrypoint:
      entry-instance: main
      execute:
      - args:
          foo: world
    workflows:
    - signature:
        name: main
        parameters:
        - name: foo
        - name: unused
          default: value-unused
      steps:
        one: echo
        two: echo
      execute:
      - target: <one>
        args:
          message: "hello from one"
          other: "%(foo)s"
      - target: <two>
        args:
          message: "hello from two"
          other: "<one>:output"
    components:
    - signature:
        name: echo
        parameters:
        - name: message
        - name: other
          default: a default value
        - name: environment
          default:
            DEFAULTS: PATH:LD_LIBRARY_PATH
            AN_ENV_VAR: ITS_VALUE
      command:
        environment: "%(environment)s"
        executable: echo
        arguments: "%(message)s %(other)s"        
    """)

@pytest.fixture()
def dsl_two_workflows_one_component_one_step() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load("""
    entrypoint:
      entry-instance: main
      execute:
      - args:
          foo: world
    workflows:
    - signature:
        name: main
        parameters:
        - name: foo
      steps:
        wrapper: actual
      execute:
      - target: <wrapper>
        args:
          foo: "%(foo)s"
    - signature:
        name: actual
        parameters:
        - name: foo
      steps:
        greetings: echo
      execute:
      - target: <greetings>
        args:
          message: "hello"
          other: "%(foo)s"
    components:
    - signature:
        name: echo
        parameters:
        - name: message
        - name: other
          default: a default value
        - name: environment
          default:
            DEFAULTS: PATH:LD_LIBRARY_PATH
            AN_ENV_VAR: ITS_VALUE
      command:
        environment: "%(environment)s"
        executable: echo
        arguments: "%(message)s %(other)s"
    """)





def test_parse_hallucinated_simple_dsl2(simple_flowir: experiment.model.frontends.flowir.DictFlowIR):
    graph = experiment.model.graph.WorkflowGraph.graphFromFlowIR(simple_flowir, {})

    dsl = graph.to_dsl()

    namespace = experiment.model.frontends.dsl.Namespace(**dsl)

    print(yaml.safe_dump(namespace.dict(exclude_unset=True, exclude_defaults=True, exclude_none=True), indent=2))

    print("-----------")
    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)
    print(yaml.safe_dump(flowir.raw(), indent=2))


@pytest.mark.parametrize("input_outputs", [
    ("<workflow/component/file>:ref", ["workflow", "component", "file"], "ref", "<workflow/component/file>:ref"),
    ('"<workflow/component>"/file:ref', ["workflow", "component", "file"], "ref", "<workflow/component/file>:ref"),
    ('"<workflow>"/component/file:ref', ["workflow", "component", "file"], "ref", "<workflow/component/file>:ref"),
    ('"<workflow>":ref', ["workflow"], "ref", "<workflow>:ref"),
    ('<workflow>:ref', ["workflow"], "ref", "<workflow>:ref"),
    ('"<workflow>"', ["workflow"], None, "<workflow>"),
    ('<workflow>', ["workflow"], None, "<workflow>"),
    ("<workflow/component/file>", ["workflow", "component", "file"], None, "<workflow/component/file>"),
    ('"<workflow/component>"/file', ["workflow", "component", "file"], None, "<workflow/component/file>"),
    ('<stage0.XYZToGAMESS>/molecule.inp:copy', ["stage0.XYZToGAMESS", "molecule.inp"], "copy",
        "<stage0.XYZToGAMESS/molecule.inp>:copy")
])
def test_parse_step_references(input_outputs: typing.Tuple[
    str, typing.List[str], typing.Optional[str], str,
]):
    ref_str, expected_location, expected_method, expected_rewrite = input_outputs

    ref = experiment.model.frontends.dsl.OutputReference.from_str(ref_str)

    assert ref.location == expected_location
    assert ref.method == expected_method
    assert ref.to_str() == expected_rewrite


def test_replace_many_parameter_references():
    x = experiment.model.frontends.dsl._replace_many_parameter_references(
        what="%(message)s %(other)s",
        loc=[],
        parameters= {
            "message": "hello",
            "other": "world",
            "environment": {"hi": "there"}
        },
        field=[],
    )

    assert x == "hello world"


@pytest.mark.parametrize("fixture_name", [
    "dsl_one_workflow_one_component_one_step_no_datareferences",
    "dsl_two_workflows_one_component_one_step",
])
def test_dsl2_single_workflow_single_component_single_step_no_datareferences(fixture_name: str, request):
    dsl = request.getfixturevalue(argname=fixture_name)
    namespace = experiment.model.frontends.dsl.Namespace(**dsl)

    print(
        yaml.safe_dump(
            namespace.dict(
                exclude_unset=True,
                exclude_none=True,
                exclude_defaults=True,
                by_alias=True
            ),
            sort_keys=False
        )
    )

    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)

    print(yaml.safe_dump(flowir.raw(), sort_keys=False))

    flowir = flowir.raw()
    assert len(flowir["components"]) == 1
    assert flowir["components"][0] == {
        "stage": 0,
        "name": "greetings",
        "references": [],
        "command": {
            "executable": "echo",
            "arguments": "hello world",
            "environment": "env0"
        }
    }

    assert len(flowir["environments"]["default"]) == 1
    assert flowir["environments"]["default"]["env0"] == {
        "DEFAULTS": "PATH:LD_LIBRARY_PATH",
        "AN_ENV_VAR": "ITS_VALUE"
    }

    errors = experiment.model.frontends.flowir.FlowIR.validate(flowir=flowir, documents={})

    assert len(errors) == 0


def test_dsl2_single_workflow_one_component_two_steps_no_edges(
    dsl_one_workflow_one_component_two_steps_no_edges: typing.Dict[str, typing.Any]
):
    namespace = experiment.model.frontends.dsl.Namespace(**dsl_one_workflow_one_component_two_steps_no_edges)

    print(
        yaml.safe_dump(
            namespace.dict(
                exclude_unset=True,
                exclude_none=True,
                exclude_defaults=True,
                by_alias=True
            ),
            sort_keys=False
        )
    )

    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)

    print(yaml.safe_dump(flowir.raw(), sort_keys=False))

    flowir = flowir.raw()
    assert len(flowir["components"]) == 2
    assert [x for x in flowir["components"] if x['name'] == 'one'][0] == {
        "stage": 0,
        "name": "one",
        "references": [],
        "command": {
            "executable": "echo",
            "arguments": "hello from one world",
            "environment": "env0"
        }
    }

    assert [x for x in flowir["components"] if x['name'] == 'two'][0] == {
        "stage": 0,
        "name": "two",
        "references": [],
        "command": {
            "executable": "echo",
            "arguments": "hello from two world",
            "environment": "env0"
        }
    }

    assert len(flowir["environments"]["default"]) == 1
    assert flowir["environments"]["default"]["env0"] == {
        "DEFAULTS": "PATH:LD_LIBRARY_PATH",
        "AN_ENV_VAR": "ITS_VALUE"
    }

    errors = experiment.model.frontends.flowir.FlowIR.validate(flowir=flowir, documents={})

    assert len(errors) == 0

    assert flowir["variables"]["default"]["global"] == {
        "foo": "world",
        "unused": "value-unused"
    }


def test_dsl2_single_workflow_one_component_two_steps_with_edges(
    dsl_one_workflow_one_component_two_steps_with_edges: typing.Dict[str, typing.Any]
):
    namespace = experiment.model.frontends.dsl.Namespace(**dsl_one_workflow_one_component_two_steps_with_edges)

    print(
        yaml.safe_dump(
            namespace.dict(
                exclude_unset=True,
                exclude_none=True,
                exclude_defaults=True,
                by_alias=True
            ),
            sort_keys=False
        )
    )

    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)

    print(yaml.safe_dump(flowir.raw(), sort_keys=False))

    flowir = flowir.raw()
    assert len(flowir["components"]) == 2
    assert [x for x in flowir["components"] if x['name'] == 'one'][0] == {
        "stage": 0,
        "name": "one",
        "references": [],
        "command": {
            "executable": "echo",
            "arguments": "hello from one world",
            "environment": "env0"
        }
    }

    assert [x for x in flowir["components"] if x['name'] == 'two'][0] == {
        "stage": 0,
        "name": "two",
        "references": [
            "stage0.one:output"
        ],
        "command": {
            "executable": "echo",
            "arguments": "hello from two stage0.one:output",
            "environment": "env0"
        }
    }

    assert len(flowir["environments"]["default"]) == 1
    assert flowir["environments"]["default"]["env0"] == {
        "DEFAULTS": "PATH:LD_LIBRARY_PATH",
        "AN_ENV_VAR": "ITS_VALUE"
    }

    errors = experiment.model.frontends.flowir.FlowIR.validate(flowir=flowir, documents={})

    assert len(errors) == 0

    assert flowir["variables"]["default"]["global"] == {
        "foo": "world",
        "unused": "value-unused"
    }