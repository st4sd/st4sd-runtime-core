# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import print_function

import logging
import os
import stat
import tempfile

import yaml.error

import experiment.model.errors
import experiment.test
import experiment.model.storage
import experiment.model.data
import experiment.model.graph
import uuid

import pytest

from . import utils

logger = logging.getLogger('test')


def test_load_yaml_with_special_folders(output_dir):
    tempdir = tempfile.gettempdir()

    flowir = """
    components:
    - name: many
      command:
        executable: cat
        arguments: special/message.txt:ref
      references:
      - special/message.txt:ref
      workflowAttributes:
        replicate: 2
    - name: hello
      command:
        executable: echo
        arguments: special/message.txt:output many:ref
      references:
      - special/message.txt:output
      - many:ref
    """

    manifest = """
    special: special
    """

    filename = f"wf-{uuid.uuid4()}.yml"

    utils.populate_files(output_dir, {filename: flowir, "manifest.yml": manifest,
                                      os.path.join("special", "message.txt"): "hello world"})

    isValid, error, compExperiment = experiment.test.ValidatePackage(
        os.path.join(output_dir, filename), location=tempdir, manifest=os.path.join(output_dir, "manifest.yml"),)

    assert isValid
    assert error is None

    # VV: Now make sure that the package is valid - perform check_executable checks too
    compExperiment.validateExperiment(ignoreTestExecutablesError=False)


def test_load_package_that_cannot_replicate(output_dir):
    flowir = """
    components:
    - name: many
      command:
        executable: cat
        arguments: special/message.txt:ref
      references:
      - special/message.txt:ref
      workflowAttributes:
        replicate: "%(replicas)s"
    - name: hello
      command:
        executable: echo
        arguments: special/message.txt:output many:ref
      references:
      - special/message.txt:output
      - many:ref
    """

    utils.populate_files(output_dir, {
        "dummy.yml": flowir,
        os.path.join("special", "message.txt"): "hello world",
        "manifest.yml": f"""special: {os.path.join(output_dir, "special")}:copy"""
    })

    pkg = experiment.model.storage.ExperimentPackage.packageFromLocation(
        location=os.path.join(output_dir, "dummy.yml"), manifest=os.path.join(output_dir, "manifest.yml"),
        validate=False
    )

    g = experiment.model.graph.WorkflowGraph.graphFromPackage(
        pkg, validate=False, primitive=True, variable_substitute=False)

    graph = g.graph

    node_names = [x for x in graph.nodes]

    logging.getLogger().info(f"All nodes: {node_names}")
    assert sorted(node_names) == sorted(["stage0.many", "stage0.hello"])


def test_load_package_that_cannot_replicate_and_fail(output_dir):
    flowir = """
    components:
    - name: many
      command:
        executable: cat
        arguments: special/message.txt:ref
      references:
      - special/message.txt:ref
      workflowAttributes:
        replicate: "%(replicas)s"
    - name: hello
      command:
        executable: echo
        arguments: special/message.txt:output many:ref
      references:
      - special/message.txt:output
      - many:ref
    """

    utils.populate_files(output_dir, {
        "dummy.yml": flowir,
        os.path.join("special", "message.txt"): "hello world",
        "manifest.yml": f"""special: {os.path.join(output_dir, "special")}:copy"""
    })

    pkg = experiment.model.storage.ExperimentPackage.packageFromLocation(
        location=os.path.join(output_dir, "dummy.yml"), manifest=os.path.join(output_dir, "manifest.yml"),
        validate=False
    )

    with pytest.raises(experiment.model.errors.ExperimentInvalidConfigurationError) as e:
        experiment.model.graph.WorkflowGraph.graphFromPackage(pkg, primitive=False)

    exc = e.value
    other_error = exc.underlyingError

    assert isinstance(other_error, experiment.model.errors.FlowIRConfigurationErrors)
    actual_errors = other_error.underlyingErrors
    assert len(actual_errors) == 1

    assert isinstance(actual_errors[0], experiment.model.errors.FlowIRVariableUnknown)

    missing_variable: experiment.model.errors.FlowIRVariableUnknown = actual_errors[0]

    assert missing_variable.variable_route == "replicas"


def test_load_yaml_without_manifest_on_disk(output_dir):
    tempdir = tempfile.gettempdir()

    flowir = """
        components:
        - name: many
          command:
            executable: cat
            arguments: special/message.txt:ref
          references:
          - special/message.txt:ref
          workflowAttributes:
            replicate: 2
        - name: hello
          command:
            executable: echo
            arguments: special/message.txt:output many:ref
          references:
          - special/message.txt:output
          - many:ref
        """

    utils.populate_files(output_dir, {"dummy.yml": flowir, os.path.join("special", "message.txt"): "hello world"})
    manifest = {'special': os.path.join(output_dir, 'special')}
    isValid, error, compExperiment = experiment.test.ValidatePackage(os.path.join(output_dir, 'dummy.yml'),
                                                                     location=tempdir, manifest=manifest)

    assert isValid
    assert error is None

    # VV: Now make sure that the package is valid - perform check_executable checks too
    compExperiment.validateExperiment(ignoreTestExecutablesError=False)


def test_load_yaml_with_manifest(output_dir):
    tempdir = tempfile.gettempdir()

    flowir = """
    components:
    - name: hello
      command:
        executable: bin/hello_world.sh
    """

    manifest = """
    bin: scripts
    """

    hello_world = """
    !/bin/bash
    echo hello world
    """
    filename = f"wf-{uuid.uuid4()}.yml"
    utils.populate_files(output_dir, {filename: flowir, "manifest.yml": manifest,
                                      os.path.join("scripts", "hello_world.sh"): hello_world})

    script_path = os.path.join(output_dir, "scripts", "hello_world.sh")
    os.chmod(script_path, os.stat(script_path).st_mode | stat.S_IEXEC)

    isValid, error, compExperiment = experiment.test.ValidatePackage(os.path.join(output_dir, filename),
                                                    location=tempdir,
                                                    manifest=os.path.join(output_dir, "manifest.yml"),)

    assert isValid
    assert error is None

    # VV: Now make sure that the package is valid - perform check_executable checks too
    compExperiment.validateExperiment(ignoreTestExecutablesError=False)


def test_package_validation_from_yaml(output_dir):
    tempdir = tempfile.gettempdir()
    filename = f"wf-{uuid.uuid4()}.yml"
    dummy_file = os.path.join(output_dir, filename)

    flowir = """
        components:
        - name: hello
          command:
            executable: echo
            arguments: 'hello world >file.txt && echo hi>file2.txt'
        """

    with open(dummy_file, 'w') as f:
        f.write(flowir)

    validationRet = experiment.test.ValidatePackage(dummy_file, location=tempdir)

    assert validationRet[0]
    assert validationRet[1] is None


def test_package_validation_from_dir(output_dir):
    tempdir = tempfile.gettempdir()
    flowir = """
        components:
        - name: hello
          command:
            executable: echo
            arguments: 'hello world >file.txt && echo hi>file2.txt'
        """

    package_path = os.path.join(output_dir, '%s.package' % str(uuid.uuid4()))
    dir_conf = os.path.join(package_path, 'conf')
    os.makedirs(dir_conf)
    with open(os.path.join(dir_conf, 'flowir_package.yaml'), 'w') as f:
        f.write(flowir)

    validationRet = experiment.test.ValidatePackage(package_path, location=tempdir)

    assert validationRet[0]
    assert validationRet[1] is None


def test_experiment_from_package_with_dir(output_dir):
    tempdir = tempfile.gettempdir()
    flowir = """
        components:
        - name: hello
          command:
            executable: echo
            arguments: 'hello world >file.txt && echo hi>file2.txt'
        """

    package_path = os.path.join(output_dir, '%s.package' % str(uuid.uuid4()))
    dir_conf = os.path.join(package_path, 'conf')
    os.makedirs(dir_conf)
    with open(os.path.join(dir_conf, 'flowir_package.yaml'), 'w') as f:
        f.write(flowir)

    expPackage = experiment.model.storage.ExperimentPackage.packageFromLocation(package_path)

    experiment.model.data.Experiment.experimentFromPackage(expPackage, location=tempdir)


def test_experiment_from_package_with_yaml(output_dir):
    tempdir = tempfile.gettempdir()
    filename = f"wf-{uuid.uuid4()}.yml"
    dummy_file = os.path.join(output_dir, filename)

    flowir = """
                components:
                - name: hello
                  command:
                    executable: echo
                    arguments: 'hello world >file.txt && echo hi>file2.txt'
                """

    with open(dummy_file, 'w') as f:
        f.write(flowir)

    expPackage = experiment.model.storage.ExperimentPackage.packageFromLocation(dummy_file)

    experiment.model.data.Experiment.experimentFromPackage(expPackage, location=tempdir)


def test_experiment_from_package_with_invalid_yaml(output_dir):
    filename = f"wf-{uuid.uuid4()}.yml"
    dummy_file = os.path.join(output_dir, filename)

    flowir = """
    # intentional typo
    componentsA:
    - name: hello
      command:
        executable: ls
        arguments: -lth .
    """

    with open(dummy_file, 'w') as f:
        f.write(flowir)

    with pytest.raises(experiment.model.errors.ExperimentInvalidConfigurationError) as e:
        expPackage = experiment.model.storage.ExperimentPackage.packageFromLocation(dummy_file)

    exc: experiment.model.errors.FlowIRConfigurationErrors = e.value.underlyingError

    assert len(exc.underlyingErrors) == 1

    unknown_key: experiment.model.errors.FlowIRKeyUnknown = exc.underlyingErrors[0]

    assert unknown_key.key_name == 'FlowIR.componentsA'


def test_load_package_with_malformed_yaml_and_fail(output_dir):
    flowir = """
    interface:
      description: "Measures band-gap and related properties of small molecules in gas-phase using semi-emperical quantum methods
      inputSpec:
        namingScheme: "SMILES"
    """

    utils.populate_files(output_dir, {
        "dummy.yml": flowir,
    })

    with pytest.raises(experiment.model.errors.ExperimentInvalidConfigurationError) as e:
        _ = experiment.model.storage.ExperimentPackage.packageFromLocation(
            location=os.path.join(output_dir, "dummy.yml"), validate=False)

    exc: experiment.model.errors.ExperimentInvalidConfigurationError = e.value
    assert len(exc.underlyingErrors()) == 1

    # VV: The code should identify that the YAML is malformed therefore even if we do not
    # "validate that platform is runable" we should still receive an exception
    # FlowIRExperimentConfiguration._try_report_errors() MAY have collected multiple errors but it's
    # expected to raise a ExperimentInvalidConfigurationError() which expects a SINGLE underlyingError
    # therefore it wraps the many errors inside a FlowIRConfigurationErrors() which can contain multiple errors

    next_wrapper_exc: experiment.model.errors.ExperimentInvalidConfigurationError = exc.underlyingErrors()[0]
    assert isinstance(next_wrapper_exc, experiment.model.errors.FlowIRConfigurationErrors)
    assert len(next_wrapper_exc.underlyingErrors) == 1

    final_wrapper_exc: experiment.model.errors.FlowIRConfigurationErrors = next_wrapper_exc.underlyingErrors[0]
    assert isinstance(final_wrapper_exc, experiment.model.errors.ExperimentInvalidConfigurationError)
    assert len(final_wrapper_exc.underlyingErrors()) == 1

    yaml_exc: yaml.error.YAMLError = final_wrapper_exc.underlyingErrors()[0]
    assert isinstance(yaml_exc, yaml.error.YAMLError)