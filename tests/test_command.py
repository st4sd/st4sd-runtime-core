# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import print_function

import os
import sys
from typing import TYPE_CHECKING

import pytest

import experiment.model.executors
import experiment.model.errors

from .reactive_testutils import *

import experiment.model.data
from .utils import experiment_from_flowir

if TYPE_CHECKING:
    import experiment.model.executors

@pytest.fixture(params=["stage0.Source0", "stage0.Aggregating"], ids=["Source", "Aggregating"])
def componentReference(request):
    return request.param

def test_component_command_before_check(nxGraph, componentReference):

    c  = nxGraph.nodes[componentReference]['componentSpecification']

    if c.identification.identifier == 'stage0.Aggregating':
        assert c.command.executable == 'ls'
    elif  c.identification.identifier == 'stage0.Source0':
        assert c.command.executable == 'sleep'
    else:
        logger.critical('Unexpected component %s' % c.specification.reference)
        assert 0


def test_inherit_environment_path(nxGraph, componentReference):
    c = nxGraph.nodes[componentReference]['componentSpecification']

    command = c.command  # type: experiment.model.executors.Command

    environment = command.environment
    logger.warning('Environment is %s' % command.environment)

    path = environment['PATH']

    env_folders = path.split(':')
    own_env_folders = os.environ['PATH'].split(':')
    to_check = [x for x in ['/bin', '/usr/bin'] if x in own_env_folders]
    assert len(to_check) > 0
    for k in to_check:
        assert k in env_folders


def test_component_command_after_update(nxGraph, componentReference):

    c  = nxGraph.nodes[componentReference]['componentSpecification']
    #NOTE: THis is not an ivar so we need to get a copy for any changes to persist
    command = c.command  # type: experiment.model.executors.Command

    logger.warning('Environment is %s' % command.environment)

    original_executable = command.executable

    # VV: These engines are using the active-shell environment so resolve `ls` and `sleep`
    executable_location, err, process = experiment.model.executors.read_popen_output(
        'which %s' % original_executable, shell=True
    )

    if executable_location and process.returncode == 0:
        executable_location = executable_location.strip('\n')

    command.updatePath()
    assert command.executable == executable_location


def test_component_specification_check_executable_persists_changes_in_command(nxGraph, componentReference):

    #NOTE: This tests that when you call checkExecutable on a ComponentSpecification it persists
    #      the change into the configuration

    c = nxGraph.nodes[componentReference]['componentSpecification']
    originalCommand = c.command
    #This should update the path and store the updated path back to the underlying configuration
    #So further calls to c.command will have the resolved executable
    c.checkExecutable()

    assert c.command.executable != originalCommand.executable

    # VV: These engines are using the active-shell environment so resolve `ls` and `sleep`
    executable_location, err, process = experiment.model.executors.read_popen_output(
        'which %s' % originalCommand.executable, shell=True
    )

    if executable_location and process.returncode == 0:
        executable_location = executable_location.strip('\n')

    assert c.command.executable == executable_location


def test_component_command_changes_not_persisted(nxGraph, componentReference):

    # NOTE: This tests that each time ComponentSpecification.command is called a new object is created
    # and that changes to one command object do not impact others.
    c = nxGraph.nodes[componentReference]['componentSpecification']
    c.command.updatePath()

    if c.identification.identifier == 'stage0.Aggregating':
        assert c.command.executable == 'ls'
    elif c.identification.identifier == 'stage0.Source0':
        assert c.command.executable == 'sleep'
    else:
        logger.critical('Unexpected component %s' % c.specification.reference)
        assert 0

def test_checkExecutable(nxGraph, componentReference):

    # NOTE: This tests that each time c.command is called a new object is created
    # and that changes to one command object do not impact others.
    c = nxGraph.nodes[componentReference]['componentSpecification']

    with pytest.raises(ValueError) as e_info:

        c.command.checkExecutable()

    print('Passed FIRST test', file=sys.stderr)
    command = c.command
    command.updatePath()
    command.checkExecutable()


def test_environment_coerce_strings(output_dir: str):
    flowir="""
    environments:
      default:
        test:
          DEFAULTS: PATH
          INTEGER: 1
          STRING: hello
    components:
    - name: hello
      command:
        environment: test
        executable: echo
        arguments: ${INTEGER} ${STRING}
    """

    exp = experiment_from_flowir(flowir, output_dir)
    conf: experiment.model.data.ComponentSpecification = exp.graph.nodes['stage0.hello']['componentSpecification']

    env = conf.environment

    assert env['INTEGER'] == '1'
    assert env['STRING'] == 'hello'


def test_environment_override_defaults(output_dir: str):
    flowir = """
        environments:
          default:
            test:
              DEFAULTS: BASE_DIR:PATH
              BASE_DIR: /foo/bar
              FORCEFIELD_DIR: ${BASE_DIR}/forcefield
        components:
        - name: hello
          command:
            environment: test
            executable: echo
            arguments: "${FORCEFIELD_DIR}"
        """

    exp = experiment_from_flowir(flowir, output_dir)
    conf: experiment.model.data.ComponentSpecification = exp.graph.nodes['stage0.hello']['componentSpecification']

    env = conf.environment

    assert env['BASE_DIR'] == '/foo/bar'
    assert env['FORCEFIELD_DIR'] == '/foo/bar/forcefield'

    flowir = """
            environments:
              default:
                test:
                  DEFAULTS: PATH
                  BASE_DIR: /foo/bar
                  FORCEFIELD_DIR: ${BASE_DIR}/forcefield
            components:
            - name: hello
              command:
                environment: test
                executable: echo
                arguments: "${FORCEFIELD_DIR}"
            """

    exp = experiment_from_flowir(flowir, output_dir)
    conf: experiment.model.data.ComponentSpecification = exp.graph.nodes['stage0.hello']['componentSpecification']

    env = conf.environment

    assert env['BASE_DIR'] == '/foo/bar'
    assert env['FORCEFIELD_DIR'] == '/foo/bar/forcefield'


def test_inherited_default_environment_in_platforms(output_dir: str):
    import random
    import string
    custom_name = ''.join([random.choice(string.digits + string.ascii_letters) for _ in range(30)])

    assert custom_name not in os.environ

    flowir = f"""
    environments:
      default:
          environment:
            DEFAULTS: PATH:LD_LIBRARY_PATH
            PATH: hello:$PATH
            LD_LIBRARY_PATH: $LD_LIBRARY_PATH
            # VV: Test that "custom" env-vars which we don't even partially build from the active-shell also behave 
            # in the expected way
            {custom_name}: text

    platforms:
    - default
    - empty

    components:
    - stage: 0
      name: env-test
      command:
        executable: sh
        arguments: |
          -c "
          if [[ \"${{PATH}}\" =~ ^hello: ]]; then 
            echo Correct;
          else
            echo Wrong;
            exit 1;
          fi
          "
        environment: "environment"
        expandArguments: none
    """

    # VV: Double check that the `default` platform also behaves right
    exp = experiment_from_flowir(flowir, output_dir, platform="default")
    conf: experiment.model.data.ComponentSpecification = exp.graph.nodes['stage0.env-test']['componentSpecification']

    env = conf.environment

    assert env['PATH'].startswith("hello:")
    assert env[custom_name] == 'text'

    # VV: This is the meat of the test - make sure the "empty" platform receives the right environment
    exp = experiment_from_flowir(flowir, output_dir, platform="empty")
    conf: experiment.model.data.ComponentSpecification = exp.graph.nodes['stage0.env-test']['componentSpecification']

    env = conf.environment

    assert env['PATH'].startswith("hello:")
    assert env[custom_name] == 'text'


def test_auto_generated_default_env(output_dir: str):
    flowir = """
    components:
    - name: hello
      command:
        environment: environment
        executable: echo
        arguments: "${PATH}"
    """

    exp = experiment_from_flowir(flowir, output_dir)
    conf: experiment.model.data.ComponentSpecification = exp.graph.nodes['stage0.hello']['componentSpecification']

    env = conf.environment

    assert env['PATH'] == os.environ['PATH']


def test_env_not_in_default_platform(output_dir: str):
    import random
    import string
    custom_name = ''.join([random.choice(string.digits + string.ascii_letters) for _ in range(30)])

    assert custom_name not in os.environ

    flowir = f"""
    environments:
      custom:
        platform:
            DEFAULTS: PATH:LD_LOBRARY_PATH
            {custom_name}: text

    components:
    - name: hello
      command:
        environment: platform
        executable: echo
        arguments: "${{{custom_name}}}"
    """

    exp = experiment_from_flowir(flowir, output_dir, platform='custom')
    conf: experiment.model.data.ComponentSpecification = exp.graph.nodes['stage0.hello']['componentSpecification']

    env = conf.environment

    assert env[custom_name] == "text"

    # VV: Repeat the above test but for the `default` platform - we should get an exception that the
    # default platform is invalid because one of the components references an environment which the default platform
    # doesn't know about.
    with pytest.raises(experiment.model.errors.ExperimentInvalidConfigurationError) as e:
        _ = experiment_from_flowir(flowir, output_dir, platform='default')

    exc = e.value

    assert len(exc.underlyingErrors()) == 1
    underlying = exc.underlyingErrors()[0]
    assert isinstance(underlying, experiment.model.errors.FlowIRConfigurationErrors)

    assert len(underlying.underlyingErrors) == 1
    exc = underlying.underlyingErrors[0]
    assert isinstance(exc, experiment.model.errors.FlowIREnvironmentUnknown)

    assert exc.platform == "default"
    assert exc.name == "platform"
