# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import print_function

import logging

import experiment.model.data
from . import utils
import experiment.model.graph
import experiment.model.storage
import os


logger = logging.getLogger("test-data")


def test_job_multiple_refs_to_same_component(output_dir):
    flowir = """
components:
- name: hello
  stage: 0
  command:
    executable: echo
    arguments: 'hello world >file.txt && echo hi>file2.txt'
  
- name: world
  stage: 0
  command:
    executable: "cat"
    arguments: 'file.txt && cat file2.txt'
  references:
    - "hello/file.txt:copy"
    - "hello/file2.txt:copy" 
"""
    cexp = utils.experiment_from_flowir(flowir, output_dir)
    node = cexp.graph.nodes['stage0.world']
    job = node['componentSpecification']  # type: experiment.model.data.ComponentSpecification

    assert sorted([r.stringRepresentation for r in job.componentDataReferences]) == sorted([
        'stage0.hello/file.txt:copy', 'stage0.hello/file2.txt:copy'
    ])


def test_datareferenceinfo_workdir_and_file():
    dri = experiment.model.data.DataReferenceInfo(
        "stage1.GeometryOptimisation77:ref", "/tmp/workdir/workflow.instance",
        1, "-f stage1.GeometryOptimisation77:ref/stdout", [])

    assert dri.pid.identifier == 'stage1.GeometryOptimisation77'

    prod_files = {f.filename for f in dri.files}

    assert prod_files == {'.', 'stdout'}


def test_datareferenceinfo_file():
    dri = experiment.model.data.DataReferenceInfo(
        "stage1.GeometryOptimisation77/stdout:ref", "/tmp/workdir/workflow.instance",
        1, "-f stage1.GeometryOptimisation77/stdout:ref", [])

    prod_files = [f.filename for f in dri.files]

    assert prod_files == ['stdout']


def test_consumes_producers(output_dir):
    flowir = """
    components:
    - name: hello
      stage: 0
      command:
        executable: echo
        arguments: 'hello world >file.txt && echo hi>file2.txt'

    - name: world
      stage: 0
      command:
        executable: "cat"
        arguments: 'file.txt && cat file2.txt'
      references:
        - "hello/file.txt:copy"
        - "hello/file2.txt:copy" 
    """
    cexp = utils.experiment_from_flowir(flowir, output_dir)
    job = cexp.findJob(0, 'world')

    consumes = job.consumesPredecessorPaths()

    assert len(consumes) == 1

    prod_uid, files = consumes.popitem()

    instance, comp_ref = prod_uid.split('&')

    _, location = experiment.model.storage.partition_uri(instance)

    assert location == cexp.experimentGraph.rootStorage.location
    assert comp_ref == experiment.model.graph.ComponentIdentifier('hello', 0).identifier

    assert {'file.txt', 'file2.txt'} == set(files)


def test_read_write_status_file(output_dir: str):
    path = os.path.join(output_dir, 'status.txt')
    status = experiment.model.data.Status(path, data={}, stages=['hello'])
    err="""hello
    world"""
    status.setErrorDescription(err)
    status.update()

    del status

    status = experiment.model.data.Status.statusFromFile(path)
    assert status.data['error-description'] == err


def test_split_path_to_source_path_and_target_name():
    tests = [
        # VV: (input), (expected source path, expected targetName)
        ["/hello/world", ("/hello/world", None)],
        ["/hello/world:other", ("/hello/world", "other")],
        ["\\:escaped", (":escaped", None)],
        ["\\:escaped:renamed", (":escaped", "renamed")],
        ["hi\\:escaped:renamed", ("hi:escaped", "renamed")],
    ]

    for str_input, (path_src, rename_to) in tests:
        logger.info(f"Testing {str_input} -> [{path_src}, {rename_to}]")
        actual_path_src, actual_rename_to = experiment.model.data.split_path_to_source_path_and_target_name(str_input)

        assert actual_path_src == path_src
        assert actual_rename_to == rename_to


def test_rewrite_input_and_data_names(output_dir):
    flowir = """
    components:
    - name: hello
      command:
        executable: "cat"
        arguments: 'file.txt && cat file2.txt'
      references:
        - "input/file.txt:copy"
        - "data/file2.txt:copy" 
    """
    path_data = os.path.join(output_dir, "custom-data")
    path_input = os.path.join(output_dir, "custom-input")

    with open(path_data, "w") as f:
        f.write("custom-data")

    with open(path_input, "w") as f:
        f.write("custom-input")

    cexp = utils.experiment_from_flowir(flowir, output_dir, extra_files={
            "data/file2.txt": "original-data"
        }, inputs=[f"{path_input}:file.txt"], override_data=[f"{path_data}:file2.txt"]
    )

    assert open(os.path.join(cexp.instanceDirectory.inputDir, "file.txt"), "rt").read() == "custom-input"
    assert open(os.path.join(cexp.instanceDirectory.dataDir, "file2.txt"), "rt").read() == "custom-data"
