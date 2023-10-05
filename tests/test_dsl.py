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


def test_parse_simple_dsl2(simple_flowir: experiment.model.frontends.flowir.DictFlowIR):
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
])
def test_parse_step_references(input_outputs: typing.Tuple[
    str, typing.List[str], typing.Optional[str]
]):
    ref_str, expected_location, expected_method, expected_rewrite = input_outputs

    ref = experiment.model.frontends.dsl.OutputReference.from_str(ref_str)

    assert ref.location == expected_location
    assert ref.method == expected_method
    assert ref.to_str() == expected_rewrite

