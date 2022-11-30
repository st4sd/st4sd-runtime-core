# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import print_function

import os
from typing import TYPE_CHECKING, List

import pytest
import yaml

import experiment.model.frontends.dosini
import experiment.model.frontends.flowir

if TYPE_CHECKING:
    from experiment.model.frontends.flowir import DictFlowIRComponent, DictFlowIR

conf='''
[DEFAULT]
arrayVariableOne=8 9 dlmeso.exe SomeString 2.4
arrayVariableTwo=a b c d e f
normalVariable=normal


[SomeComponent]
executable=%(arrayVariableOne)s[2]
numberProcesses=%(arrayVariableOne)s[10]
ranksPerNode=%(arrayVariableOne)s[3] %(arrayVariableTwo)s[1]
numberThreads=data.txt[8]
job-type=data.yml[2]
environment=This is a %(normalVariable)s variable
arguments=Nothing here
replica=3
rstage-in=%(arrayVariableOne)s[%(replica)s]
'''


def utility_find_component(flow_ir, component_name):
    # type: (DictFlowIR, str) -> List[DictFlowIRComponent]
    components = flow_ir[experiment.model.frontends.flowir.FlowIR.FieldComponents]  # type: List[DictFlowIRComponent]
    components = [e for e in components if e['name'] == component_name]
    return components


@pytest.fixture(scope='function')
def fileArray(output_dir):

    #Must match the plain array filename in the test conf
    original_dir = os.path.abspath(os.path.curdir)
    os.chdir(output_dir)

    filename = 'data.txt'
    fileArray = ["plain-file-data-%d" % el for el in range(10)]
    with open(filename, 'w') as f:
        print("\n".join(fileArray), file=f)

    yield fileArray

    os.remove(filename)
    os.chdir(original_dir)

@pytest.fixture(scope='function')
def yamlArray(output_dir):
    #Must match the yaml filename in the test conf

    yamlArray = ["yaml-data-%d" % el for el in range(10)]
    yamlArray[2] = 'simulator'

    original_dir = os.path.abspath(os.path.curdir)
    os.chdir(output_dir)
    ymlFile = os.path.join('.', 'data.yml')
    with open(ymlFile, 'w') as f:
        print(yaml.dump(yamlArray), file=f)

    yield yamlArray

    os.remove(ymlFile)
    os.chdir(original_dir)

@pytest.fixture(scope='function')
def stageConfig():
    import experiment.model.conf
    import uuid

    filename = "%s.conf" % uuid.uuid4()

    with open(filename, 'w') as f:
        f.write(conf)

    cfg = experiment.model.frontends.dosini.FlowConfigParser()
    cfg.read([filename])
    yield cfg

    os.remove(filename)


@pytest.fixture(scope='function')
def stage_concrete(fileArray, yamlArray, stageConfig):
    dosini = experiment.model.frontends.dosini.Dosini()

    stage_dict = experiment.model.frontends.dosini.dosini_to_dict(stageConfig)

    flowir = dosini.parse_stage(
        flowir={},
        stage_index=0,
        stage_dict=stage_dict,
        user_variables={}
    )

    concrete = experiment.model.frontends.flowir.FlowIRConcrete(
        flowir, experiment.model.frontends.flowir.FlowIR.LabelDefault, {}
    )

    return concrete


def test_flowir_parse_array_variable(stage_concrete):
    comp_id = (0, 'SomeComponent')

    comp = stage_concrete.get_component(comp_id)
    comp_variables = stage_concrete.get_component_variables(comp_id)

    command = comp['command']

    flowir = experiment.model.frontends.flowir.FlowIR()

    command = flowir.fill_in(command, comp_variables, is_primitive=True)

    assert command['executable'] == 'dlmeso.exe'


def test_flowir_parse_multiple_array_variable(stage_concrete):
    comp_id = (0, 'SomeComponent')

    comp = stage_concrete.get_component(comp_id)
    comp_variables = stage_concrete.get_component_variables(comp_id)

    resourceRequest = comp['resourceRequest']

    flowir = experiment.model.frontends.flowir.FlowIR()

    ranksPerNode = resourceRequest['ranksPerNode']

    ranksPerNode = flowir.fill_in(ranksPerNode, comp_variables, is_primitive=True)

    assert ranksPerNode == "SomeString b"


def test_flowir_parse_yaml_array_variable(yamlArray, stage_concrete):
    comp_id = (0, 'SomeComponent')

    comp = stage_concrete.get_component(comp_id)
    comp_variables = stage_concrete.get_component_variables(comp_id)

    resourceManager = comp['resourceManager']

    flowir = experiment.model.frontends.flowir.FlowIR()

    backend = resourceManager['config']['backend']

    backend = flowir.fill_in(backend, comp_variables, is_primitive=True)

    assert backend == yamlArray[2]


def test_flowir_parse_plain_file_array_variable(fileArray, stage_concrete):
    comp_id = (0, 'SomeComponent')

    comp = stage_concrete.get_component(comp_id)
    comp_variables = stage_concrete.get_component_variables(comp_id)

    resourceRequest = comp['resourceRequest']

    flowir = experiment.model.frontends.flowir.FlowIR()

    numberThreads = resourceRequest['numberThreads']

    numberThreads = flowir.fill_in(numberThreads, comp_variables, is_primitive=True)

    assert numberThreads == fileArray[8]


def test_flowir_array_outof_bounds(stage_concrete):
    comp_id = (0, 'SomeComponent')

    comp = stage_concrete.get_component(comp_id)
    comp_variables = stage_concrete.get_component_variables(comp_id)

    resourceRequest = comp['resourceRequest']

    flowir = experiment.model.frontends.flowir.FlowIR()

    numberProcesses = resourceRequest['numberProcesses']

    with pytest.raises(IndexError):
        numberProcesses = flowir.fill_in(numberProcesses, comp_variables, is_primitive=True)


def test_flowir_normal_variable(stage_concrete):
    comp_id = (0, 'SomeComponent')

    comp = stage_concrete.get_component(comp_id)
    comp_variables = stage_concrete.get_component_variables(comp_id)

    environment = comp['command']['environment']

    flowir = experiment.model.frontends.flowir.FlowIR()

    environment = flowir.fill_in(environment, comp_variables, is_primitive=True)

    assert environment == 'This is a normal variable'


def test_flowir_no_variable(stage_concrete):
    comp_id = (0, 'SomeComponent')

    comp = stage_concrete.get_component(comp_id)
    comp_variables = stage_concrete.get_component_variables(comp_id)

    arguments = comp['command']['arguments']

    flowir = experiment.model.frontends.flowir.FlowIR()

    arguments = flowir.fill_in(arguments, comp_variables, is_primitive=True)

    assert arguments == 'Nothing here'


def test_flowir_replica(stage_concrete):
    comp_id = (0, 'SomeComponent')

    comp = stage_concrete.get_component(comp_id)
    comp_variables = stage_concrete.get_component_variables(comp_id)

    rstage_in = [executor for executor in comp['executors']['pre'] if executor['name'] == 'lsf-dm-in'][0]['payload']

    flowir = experiment.model.frontends.flowir.FlowIR()

    arguments = flowir.fill_in(rstage_in, comp_variables, is_primitive=True)

    assert arguments == 'SomeString'


def test_flowir_expand_array_index_many_ways():
    variables = {
        'testMolecules': "h4t4 h6t5 h8t5 h6t6",
        'myMolecule': '%(testMolecules)s[%(replica)s]',
        'myOtherMolecule': '%(testMolecules)s[1]',
        'myRecursiveOtherMolecule': '%(testMolecules)s[%(index)s]',
        'myExtraRecursiveOtherMolecule': '%(testMolecules)s[%(indices)s[%(replica)s]]',
        'indices': '0 1 2 3 4',
        'index': '%(indices)s[%(replica)s]',
        'other_index': '%(replica)s',
    }

    arguments = experiment.model.frontends.flowir.FlowIR.fill_in(
        "-c data/CONTROL:ref -m %(testMolecules)s[%(replica)s] data/molecules.csv:ref", variables, is_primitive=True)

    assert arguments == "-c data/CONTROL:ref -m %(testMolecules)s[%(replica)s] data/molecules.csv:ref"

    variables['replica'] = '0'

    arguments = experiment.model.frontends.flowir.FlowIR.fill_in(
        "-c data/CONTROL:ref -m %(testMolecules)s[%(replica)s] data/molecules.csv:ref", variables)

    assert arguments == "-c data/CONTROL:ref -m h4t4 data/molecules.csv:ref"

    arguments = experiment.model.frontends.flowir.FlowIR.fill_in(
        "-c data/CONTROL:ref -m %(testMolecules)s[%(other_index)s] data/molecules.csv:ref", variables)

    assert arguments == "-c data/CONTROL:ref -m h4t4 data/molecules.csv:ref"

    arguments = experiment.model.frontends.flowir.FlowIR.fill_in(
        "-c data/CONTROL:ref -m %(myMolecule)s data/molecules.csv:ref", variables)

    assert arguments == "-c data/CONTROL:ref -m h4t4 data/molecules.csv:ref"

    arguments = experiment.model.frontends.flowir.FlowIR.fill_in(
        "-c data/CONTROL:ref -m %(myOtherMolecule)s data/molecules.csv:ref", variables)

    assert arguments == "-c data/CONTROL:ref -m h6t5 data/molecules.csv:ref"

    arguments = experiment.model.frontends.flowir.FlowIR.fill_in(
        "-c data/CONTROL:ref -m %(myRecursiveOtherMolecule)s data/molecules.csv:ref", variables)

    assert arguments == "-c data/CONTROL:ref -m h4t4 data/molecules.csv:ref"

    arguments = experiment.model.frontends.flowir.FlowIR.fill_in(
        "-c data/CONTROL:ref -m %(myExtraRecursiveOtherMolecule)s data/molecules.csv:ref", variables)

    assert arguments == "-c data/CONTROL:ref -m h4t4 data/molecules.csv:ref"

    arguments = experiment.model.frontends.flowir.FlowIR.fill_in(
        "-c data/CONTROL:ref -m data/do_not_expand_this:ref[%(replica)s] %(myExtraRecursiveOtherMolecule)s "
        "data/molecules.csv:ref", variables)

    assert arguments == "-c data/CONTROL:ref -m data/do_not_expand_this:ref[0] h4t4 data/molecules.csv:ref"


def test_expand_wrong_use_of_array_access():
    variables = {
        'experiments-location': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction',
        'hybrid': '--hybrid=paragon',
        'my_molecule': 'h4t4 h6t5 h8t5 h6t6[%(replica)s]',
        'numberTestMolecules': '4',
        'platform': 'paragon',
        'replica': 0,
        'stage-name': 'Setup',
        'testMolecules': 'h4t4 h6t5 h8t5 h6t6'
    }

    command = experiment.model.frontends.flowir.FlowIR.interpolate(
        '-c data/CONTROL:ref -m %(my_molecule)s data/molecules.csv:ref',
        variables, flowir={}, label='component', is_primitive=False
    )

    assert command == '-c data/CONTROL:ref -m h4t4 data/molecules.csv:ref'

    # VV: We're requesting the first entry of the `testMolecules` array and then inserting that into the string
    command_alternative = experiment.model.frontends.flowir.FlowIR.interpolate(
        '-c data/CONTROL:ref -m %(testMolecules)s[0] data/molecules.csv:ref',
        variables, flowir={}, label='component', is_primitive=False
    )

    assert command_alternative == '-c data/CONTROL:ref -m h4t4 data/molecules.csv:ref'

    # VV: Here we're asking Flow to expand the array
    # `-c data/CONTROL:ref -m h4t4 h6t5 h8t5 h6t6` and select the first item which is `-c`
    command_wrong_use_of_arrays = experiment.model.frontends.flowir.FlowIR.interpolate(
        '-c data/CONTROL:ref -m h4t4 h6t5 h8t5 h6t6[0] data/molecules.csv:ref',
        variables, flowir={}, label='component', is_primitive=False
    )

    assert command_wrong_use_of_arrays == '-c data/molecules.csv:ref'


def test_flowir_expand_array_not_at_beginning(fileArray):
    resolved = experiment.model.frontends.flowir.FlowIR.interpolate(
        'Hello data.txt[%(which)s]', context={'which': 0}, flowir={}, label='component', is_primitive=False)

    assert resolved == "Hello plain-file-data-0"
