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
