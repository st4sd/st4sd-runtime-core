# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# coding=UTF-8
#
# IBM Confidential
# OCO Source Materials
# 5747-SM3
# (c) Copyright IBM Corp. 2019
# The source code for this program is not published or otherwise
# divested of its trade secrets, irrespective of what has
# been deposited with the U.S. Copyright Office.
# Author(s): Vassilis Vassiliadis
# Contact: Vassilis.Vassiliadis@ibm.com

from __future__ import print_function

import copy
import json
import logging
import os
import shutil
import tempfile
from typing import TYPE_CHECKING, Dict, cast

import pytest

import experiment
import experiment.model.codes
import experiment.model.frontends.dosini
import experiment.model.frontends.flowir

from .saltcurve_paragon import SaltCurveGenerator, description_process_inputs

logger = logging.getLogger()
logger.setLevel(20)

if TYPE_CHECKING:
    from experiment.model.frontends.flowir import DictFlowIRComponent, DictFlowIR


@pytest.fixture(scope="function")
def salt_curve_conf():
    with SaltCurveGenerator(is_instance=False, path_name='conf', delete_after=True) as path_to_conf_dir:
        yield path_to_conf_dir


@pytest.fixture(scope="function")
def saltcurve_flowir(salt_curve_conf):
    # type: (str) -> DictFlowIR
    return load_flowir(salt_curve_conf)


@pytest.fixture(scope='function')
def saltcurve_flowir_override_input(salt_curve_conf):
    # type: (str) -> DictFlowIR
    return load_flowir_override_input(salt_curve_conf)


@pytest.fixture(scope="function")
def salt_curve_whole_package():
    tmp_package = tempfile.mkdtemp()

    with SaltCurveGenerator(False, tmp_package, path_name='conf') as path_to_conf_dir:
        yield tmp_package

    shutil.rmtree(tmp_package)


def load_flowir_override_input(from_dir):
    input_variable = os.path.join(from_dir, '..', 'input', 'variables.conf')
    input_variable = os.path.normpath(input_variable)
    dos = experiment.model.frontends.dosini.Dosini()
    flowir = dos.load_from_directory(from_dir, [input_variable], {}, is_instance=False)

    return flowir


def load_flowir_instance(from_dir):
    dos = experiment.model.frontends.dosini.Dosini()
    flowir = dos.load_from_directory(from_dir, [], {}, is_instance=True)

    return flowir


def load_flowir(from_dir):
    dos = experiment.model.frontends.dosini.Dosini()
    flowir = dos.load_from_directory(from_dir, [], {})

    return flowir


def utility_plain_component(flowir, stage_index, component_name, ensure_one=True):
    search = [comp for comp in flowir.get('components', [])
              if comp['stage'] == stage_index and comp['name'] == component_name]

    if ensure_one:
        assert len(search) == 1
        return search[0]
    return search

def utility_find_component(flow_ir, stage_index, component_name, raw=False, platform='paragon', include_default=True):
    # type: (DictFlowIR, int, str, bool, str, bool) -> DictFlowIRComponent
    concrete = experiment.model.frontends.flowir.FlowIRConcrete(flow_ir, platform, {})

    comp_id = (stage_index, component_name)

    return concrete.get_component_configuration(comp_id, raw=raw, include_default=include_default)


def raw_section_to_options(dosini_description):
    # type: (str) -> Dict[str, str]
    lines = dosini_description.split('\n')
    lines = [e for e in lines if len(e) > 0]

    options = dict([e.split('=', 1) for e in lines])  # type: Dict[str, str]

    for key in options:
        options[key] = options[key].strip()

    return options


def pretty_json(entry):
    return json.dumps(entry, sort_keys=True, indent=4, separators=(',', ': '))


def validate_process_inputs_raw(process_inputs):
    # type: (DictFlowIRComponent) -> None

    assert set(process_inputs.keys()) == set([
        'command', 'executors', 'name', 'references', 'resourceManager', 'stage', 'variables'
    ])

    assert process_inputs['command'] == {
        "arguments": "-n %(numberPoints)s input/field.conf:ref > concentration_curve_points.csv",
        "environment": "%(pythonenv)s",
        "executable": "concentration_curve_points.py",
        'expandArguments': 'none',
    }

    assert process_inputs['executors'] == {
        "post": [
            {
                'name': 'lsf-dm-out',
                "payload": "all",
            }
        ]
    }

    assert set(process_inputs['references']) == set([
        "input/field.conf:ref"
    ])

    assert process_inputs['resourceManager'] == {
        'config': {
            'backend': 'lsf'
        },
        'lsf': {
            'queue': '%(defaultq)s'
        }
    }

    assert process_inputs['variables'] == {'george': 'of the jungle'}


def test_virtual_environments(saltcurve_flowir):
    virtual_environments = saltcurve_flowir[
        experiment.model.frontends.flowir.FlowIR.FieldVirtualEnvironments]

    assert virtual_environments == {
        'paragon': ['/gpfs/cds/local/HCRI003/rla09/shared/virtualenvs/%(hpc-venv)s']
    }


def test_application_dependencies(saltcurve_flowir):
    app_deps = saltcurve_flowir[experiment.model.frontends.flowir.FlowIR.FieldApplicationDependencies]

    assert app_deps == {
        'default': ['CAF.application', 'DPD.application', 'Viz.application']
    }


def test_stage0_process_inputs(saltcurve_flowir):
    process_inputs = utility_find_component(saltcurve_flowir, 0, 'ProcessInputs', raw=True)

    assert set(process_inputs.keys()) == set([
        'command', 'executors', 'name', 'references', 'resourceManager', 'stage', 'variables',
        'resourceRequest', 'workflowAttributes'
    ])

    assert process_inputs['resourceRequest'] == {
        'gpus': None,
        'memory': None,
        'numberProcesses': 1,
        'numberThreads': 1,
        'ranksPerNode': 1,
        'threadsPerCore': 1
    }

    assert process_inputs['workflowAttributes'] == {
        'restartHookFile': None,
        'repeatRetries': 3,
        'maxRestarts': None,
        'aggregate': False,
        'isMigratable': False,
        'isMigrated': False,
        'isRepeat': False,
        'repeatInterval': None,
        'replicate': None,
        'shutdownOn': [],
        'restartHookOn': [experiment.model.codes.exitReasons['ResourceExhausted']],
        'memoization': {'disable': {'strong': False, 'fuzzy': False}, 'embeddingFunction': None},
        'optimizer': {'disable': False,
                      'exploitChance': 0.9,
                      'exploitTarget': 0.75,
                      'exploitTargetHigh': 0.5,
                      'exploitTargetLow': 0.25},
    }

    assert process_inputs['command'] == {
        "arguments": "-n %(numberPoints)s input/field.conf:ref > concentration_curve_points.csv",
        "environment": "%(pythonenv)s",
        "executable": "concentration_curve_points.py",
        "resolvePath": True,
        'expandArguments': 'none',
        'interpreter': None,
    }

    assert process_inputs['executors'] == {
        "post": [
            {
                'name': 'lsf-dm-out',
                "payload": "all",
            }
        ],
        'pre': [],
        'main': []
    }

    assert set(process_inputs['references']) == set([
        "input/field.conf:ref"
    ])

    assert process_inputs['resourceManager']['config'] == {
            'backend': 'lsf',
            'walltime': 480.0
        }
    assert process_inputs['resourceManager']['lsf'] == {
            'queue': '%(defaultq)s',
            'reservation': None,
            'resourceString': None,
            'statusRequestInterval': 60.0,
            'dockerImage': None,
            'dockerOptions': None,
            'dockerProfileApp': None,
        }

    assert process_inputs['variables'] == {
        # VV: This is a component defined variable
        'george': 'of the jungle',
        # VV: These are all inherited variables
        'OverrideFromInput': 'Hello world',
        'stage-mame': 'CheckInputs',
        'VIZuserClaimToken': '',
        'arch': 'power8',
        'backend': 'lsf',
        'backendLocal': 'local',
        'defaultq': 'paragon',
        'dlmeso-number-processes': '64',
        'dlmeso-ranks-per-node': '16',
        'dpdexe': '/gpfs/cds/local/HCRI003/rla09/shared/bin/dpd_xl_spectrum_omp.exe',
        'exclusiveq': 'paragon',
        'ffdir': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/ForceFieldUnilever',
        'hpc-venv': 'pycaf-fen',
        'moleculeLibrary': '%(ffdir)s/MOLECULE_LIBRARY',
        'mpienv': 'mpi',
        'numberPoints': '3',
        'parameterList': '%(ffdir)s/PARAMETER_LIST',
        'pythonenv': 'pythonlsf',
        'resourceSelection': '',
        'ummapexe': '/gpfs/cds/local/HCRI003/rla09/shared/bin/ummap'
    }


def test_stage2_cafsimulation_raw(saltcurve_flowir):
    caf_sim = utility_find_component(saltcurve_flowir, 2, 'CAFSimulation', raw=True)

    assert set(caf_sim.keys()) == set([
        'command', 'executors', 'name', 'references', 'resourceManager', 'resourceRequest',
        'stage', 'workflowAttributes', 'variables'
    ])

    assert caf_sim['command'] == {
        'arguments': '',
        'environment': '%(mpienv)s',
        'executable': '%(dpdexe)s',
        'expandArguments': 'double-quote',
        'interpreter': None,
        'resolvePath': True
    }

    assert caf_sim['executors'] == {
        "post": [
            {
                'name': 'lsf-dm-out',
                "payload": "OUTPUT",
            }
        ],
        "pre": [
            {
                'name': 'lsf-dm-in',
                "payload": "stage1.FIELD/FIELD:copy stage1.CONTROL/CONTROL:copy",
            }
        ],
        'main': []
    }

    assert set(caf_sim['references']) == set([
        "stage1.FIELD/FIELD:copy",
        "stage1.CONTROL/CONTROL:copy"
    ])

    assert caf_sim['resourceManager']['config'] == {
        'backend': '%(backend)s',
        'walltime': 240.0
    }

    assert caf_sim['resourceManager']['lsf'] == {
        'dockerImage': 'Dummy_a_1',
        'dockerOptions': 'Dummy_a_0',
        'dockerProfileApp': 'Dummy_a_2',
        'queue': '%(exclusiveq)s',
        'reservation': None,
        'resourceString': None,
        'statusRequestInterval': 60.0
    }

    assert caf_sim['resourceRequest'] == {
        'memory': '1Mi',
        'numberProcesses': '%(dlmeso-number-processes)s',
        'numberThreads': 2,
        'ranksPerNode': '%(dlmeso-ranks-per-node)s',
        'threadsPerCore': 2,
        'gpus': None
    }

    assert caf_sim['workflowAttributes'] == {
        'restartHookFile': 'custom-restart-hook.py',
        'repeatRetries': 3,
        'maxRestarts': None,
        'aggregate': False,
        'isMigratable': False,
        'isMigrated': False,
        'isRepeat': False,
        'repeatInterval': None,
        'replicate': None,
        'shutdownOn': ['KnownIssue', 'SystemIssue'],
        'restartHookOn': ['UnknownIssue'],
        'memoization': {'disable': {'strong': True, 'fuzzy': True}, 'embeddingFunction': None},
        'optimizer': {'disable': True,
                      'exploitChance': 0.99,
                      'exploitTarget': 0.98,
                      'exploitTargetHigh': 0.97,
                      'exploitTargetLow': 0.96},
    }

    paragon_variables = saltcurve_flowir[experiment.model.frontends.flowir.FlowIR.FieldVariables]['paragon']

    global_variables = paragon_variables[experiment.model.frontends.flowir.FlowIR.LabelGlobal]

    # VV: Validate that we've read the correct values for the paragon-defined variables
    mpienv = []
    dpdexe = []
    dlmeso_number_processes = []
    dlmeso_ranks_per_node = []

    lookup = {
        'dlmeso-number-processes': dlmeso_number_processes,
        'dlmeso-ranks-per-node': dlmeso_ranks_per_node,
        'mpienv': mpienv,
        'dpdexe': dpdexe
    }

    for key in global_variables:
        if key in lookup:
            lookup[key].append(global_variables[key])

    assert tuple(mpienv) == ('mpi',)
    assert tuple(dpdexe) == ('/gpfs/cds/local/HCRI003/rla09/shared/bin/dpd_xl_spectrum_omp.exe', )
    assert tuple(dlmeso_number_processes) == ('64', )
    assert tuple(dlmeso_ranks_per_node) == ('16',)

    # VV: Validate the %(mpienv)s environment
    paragon_environments = saltcurve_flowir[experiment.model.frontends.flowir.FlowIR.FieldEnvironments]['paragon']

    assert 'MPI' in paragon_environments

    env_mpi = paragon_environments['MPI']

    assert env_mpi == {
        'DEFAULTS': 'PATH:LD_LIBRARY_PATH',
        'LD_LIBRARY_PATH': '/gpfs/paragon/local/apps/cuda/8.0/lib64:'
                           '/opt/ibm/lib:'
                           '/gpfs/paragon/local/apps/ibm/spectrum_mpi/10.1.0/lib/:'
                           '$LD_LIBRARY_PATH',
        'LD_RUN_PATH': '/gpfs/paragon/local/apps/cuda/8.0/lib64:'
                       '/gpfs/paragon/local/apps/ibm/spectrum_mpi/10.1.0/lib/:'
                       '$LD_RUN_PATH',
        'LSF_MPIRUN': '/gpfs/paragon/local/apps/ibm/spectrum_mpi/10.1.0/bin/mpirun',
        'PATH': '/gpfs/paragon/local/apps/ibm/spectrum_mpi/10.1.0/bin/:'
                '$PATH',
    }

    assert caf_sim['variables'] == {
        'OverrideFromInput': 'Hello world',
        'VIZuserClaimToken': '',
        'arch': 'power8',
        'backend': 'lsf',
        'backendLocal': 'local',
        'defaultq': 'paragon',
        'dlmeso-number-processes': '64',
        'dlmeso-ranks-per-node': '16',
        'dpdexe': '/gpfs/cds/local/HCRI003/rla09/shared/bin/dpd_xl_spectrum_omp.exe',
        'exclusiveq': 'paragon',
        'ffdir': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/ForceFieldUnilever',
        'hpc-venv': 'pycaf-fen',
        'moleculeLibrary': '%(ffdir)s/MOLECULE_LIBRARY',
        'mpienv': 'mpi',
        'numberPoints': '3',
        'parameterList': '%(ffdir)s/PARAMETER_LIST',
        'pythonenv': 'pythonlsf',
        'resourceSelection': 'rusage[ngpus_physical=4.00]',
        'stage-name': 'Generating',
        'ummapexe': '/gpfs/cds/local/HCRI003/rla09/shared/bin/ummap'
    }


def test_stage2_cafsimulation(saltcurve_flowir):
    caf_sim = utility_find_component(saltcurve_flowir, 2, 'CAFSimulation', raw=False)

    assert set(caf_sim.keys()) == set([
        'command', 'executors', 'name', 'references', 'resourceManager', 'resourceRequest',
        'stage', 'workflowAttributes', 'variables'
    ])

    assert caf_sim['command'] == {
        'arguments': '',
         'environment': 'mpi',
         'executable': '/gpfs/cds/local/HCRI003/rla09/shared/bin/dpd_xl_spectrum_omp.exe',
         'expandArguments': 'double-quote',
         'interpreter': None,
         'resolvePath': True
    }

    assert caf_sim['executors'] == {
        "post": [
            {
                'name': 'lsf-dm-out',
                "payload": "OUTPUT",
            }
        ],
        "pre": [
            {
                'name': 'lsf-dm-in',
                "payload": "stage1.FIELD/FIELD:copy stage1.CONTROL/CONTROL:copy",
            }
        ],
        'main': []
    }

    assert set(caf_sim['references']) == set([
        "stage1.FIELD/FIELD:copy",
        "stage1.CONTROL/CONTROL:copy"
    ])

    assert caf_sim['resourceManager']['config'] == {
            'backend': 'lsf',
            'walltime': 240.0
    }

    assert caf_sim['resourceManager']['lsf'] == {
        'dockerImage': 'Dummy_a_1',
        'dockerOptions': 'Dummy_a_0',
        'dockerProfileApp': 'Dummy_a_2',
        'queue': 'paragon',
        'reservation': None,
        'resourceString': None,
        'statusRequestInterval': 60.0
    }

    assert caf_sim['resourceRequest'] == {
        # VV: Fully resolved memory is always in bytes
        'memory': 1048576,
        'numberProcesses': 64,
        'numberThreads': 2,
        'ranksPerNode': 16,
        'threadsPerCore': 2,
        'gpus': None
    }

    assert caf_sim['workflowAttributes'] == {
        'restartHookFile': 'custom-restart-hook.py',
        'repeatRetries': 3,
        'maxRestarts': None,
        'aggregate': False,
        'isMigratable': False,
        'isMigrated': False,
        'isRepeat': False,
        'repeatInterval': None,
        'replicate': None,
        'shutdownOn': ['KnownIssue', 'SystemIssue'],
        'restartHookOn': ['UnknownIssue'],
        'memoization': {'disable': {'strong': True, 'fuzzy': True}, 'embeddingFunction': None},
        'optimizer': {'disable': True,
               'exploitChance': 0.99,
               'exploitTarget': 0.98,
               'exploitTargetHigh': 0.97,
               'exploitTargetLow': 0.96},
    }

    paragon_variables = saltcurve_flowir[experiment.model.frontends.flowir.FlowIR.FieldVariables]['paragon']

    global_variables = paragon_variables[experiment.model.frontends.flowir.FlowIR.LabelGlobal]

    # VV: Validate that we've read the correct values for the paragon-defined variables
    mpienv = []
    dpdexe = []
    dlmeso_number_processes = []
    dlmeso_ranks_per_node = []

    lookup = {
        'dlmeso-number-processes': dlmeso_number_processes,
        'dlmeso-ranks-per-node': dlmeso_ranks_per_node,
        'mpienv': mpienv,
        'dpdexe': dpdexe
    }

    for key in global_variables:
        if key in lookup:
            lookup[key].append(global_variables[key])

    assert tuple(mpienv) == ('mpi',)
    assert tuple(dpdexe) == ('/gpfs/cds/local/HCRI003/rla09/shared/bin/dpd_xl_spectrum_omp.exe', )
    assert tuple(dlmeso_number_processes) == ('64', )
    assert tuple(dlmeso_ranks_per_node) == ('16',)

    # VV: Validate the %(mpienv)s environment
    paragon_environments = saltcurve_flowir[experiment.model.frontends.flowir.FlowIR.FieldEnvironments]['paragon']

    assert 'MPI' in paragon_environments

    env_mpi = paragon_environments['MPI']

    assert env_mpi == {
        'DEFAULTS': 'PATH:LD_LIBRARY_PATH',
        'LD_LIBRARY_PATH': '/gpfs/paragon/local/apps/cuda/8.0/lib64:'
                           '/opt/ibm/lib:'
                           '/gpfs/paragon/local/apps/ibm/spectrum_mpi/10.1.0/lib/:'
                           '$LD_LIBRARY_PATH',
        'LD_RUN_PATH': '/gpfs/paragon/local/apps/cuda/8.0/lib64:'
                       '/gpfs/paragon/local/apps/ibm/spectrum_mpi/10.1.0/lib/:'
                       '$LD_RUN_PATH',
        'LSF_MPIRUN': '/gpfs/paragon/local/apps/ibm/spectrum_mpi/10.1.0/bin/mpirun',
        'PATH': '/gpfs/paragon/local/apps/ibm/spectrum_mpi/10.1.0/bin/:'
                '$PATH',
    }

    assert caf_sim['variables'] == {
        'OverrideFromInput': 'Hello world',
        'stage-name': 'Generating',
        'VIZuserClaimToken': '',
        'arch': 'power8',
        'backend': 'lsf',
        'backendLocal': 'local',
        'defaultq': 'paragon',
        'dlmeso-number-processes': '64',
        'dlmeso-ranks-per-node': '16',
        'dpdexe': '/gpfs/cds/local/HCRI003/rla09/shared/bin/dpd_xl_spectrum_omp.exe',
        'exclusiveq': 'paragon',
        'ffdir': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/ForceFieldUnilever',
        'hpc-venv': 'pycaf-fen',
        'moleculeLibrary': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction'
                           '/ForceFieldUnilever/MOLECULE_LIBRARY',
        'mpienv': 'mpi',
        'numberPoints': '3',
        'parameterList': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/'
                         'ForceFieldUnilever/PARAMETER_LIST',
        'pythonenv': 'pythonlsf',
        'resourceSelection': 'rusage[ngpus_physical=4.00]',
        'ummapexe': '/gpfs/cds/local/HCRI003/rla09/shared/bin/ummap'
    }


def test_stage2_vizstream_raw(saltcurve_flowir):
    viz_stream = utility_find_component(saltcurve_flowir, 2, 'VIZStream', raw=True)

    assert set(viz_stream.keys()) == set([
        'command', 'name', 'references', 'resourceManager', 'stage', 'variables', 'workflowAttributes',
        'executors', 'resourceRequest'
    ])

    assert viz_stream['command'] == {
                "arguments": "-i VIZRender:ref/ -b $FFMPEG_PATH -e png -o rtmp://127.0.0.1:10004/live_4k "
                             "-l rtmp://127.0.0.1:10004/live_720p -w http://127.0.0.1:10003/web "
                             "-c VIZRender:ref -p $FLOW_RUN_ID -u \"%(VIZuserClaimToken)s\"",
                "environment": "viz",
                "executable": "bin/viz_streamer.py",
                'expandArguments': 'double-quote',
                'resolvePath': True,
                'interpreter': None,
            }

    assert set(viz_stream['references'],) == set([
         "VIZRender:ref"
    ])

    assert viz_stream['resourceManager']['config'] == {
        'backend': '%(backendLocal)s',
        'walltime': 480.0
    }
    assert viz_stream['resourceManager']['lsf'] == {
        'queue': 'normal',
        'reservation': None,
        'resourceString': None,
        'statusRequestInterval': 60.0,
        'dockerImage': None,
        'dockerOptions': None,
        'dockerProfileApp': None,
    }

    assert viz_stream['variables'] == {
        'OverrideFromInput': 'Hello world',
        'stage-name': 'Generating',
        'VIZuserClaimToken': '',
        'arch': 'power8',
        'backend': 'lsf',
        'backendLocal': 'local',
        'defaultq': 'paragon',
        'dlmeso-number-processes': '64',
        'dlmeso-ranks-per-node': '16',
        'dpdexe': '/gpfs/cds/local/HCRI003/rla09/shared/bin/dpd_xl_spectrum_omp.exe',
        'exclusiveq': 'paragon',
        'ffdir': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/ForceFieldUnilever',
        'hpc-venv': 'pycaf-fen',
        'kill-after-producers-done-delay': '60.0',
        'moleculeLibrary': '%(ffdir)s/MOLECULE_LIBRARY',
        'mpienv': 'mpi',
        'numberPoints': '3',
        'parameterList': '%(ffdir)s/PARAMETER_LIST',
        'pythonenv': 'pythonlsf',
        'resourceSelection': 'rusage[ngpus_physical=4.00]',
        'ummapexe': '/gpfs/cds/local/HCRI003/rla09/shared/bin/ummap'
    }

    assert viz_stream['workflowAttributes'] == {
        'restartHookFile': None,
        'repeatRetries': 3,
        'maxRestarts': None,
        'aggregate': False,
        'isMigratable': False,
        'isMigrated': False,
        'isRepeat': True,
        'repeatInterval': 600.0,
        'replicate': None,
        'shutdownOn': [],
        'restartHookOn': [experiment.model.codes.exitReasons['ResourceExhausted']],
        'memoization': {'disable': {'strong': False, 'fuzzy': False}, 'embeddingFunction': None},
        'optimizer': {'disable': False,
                      'exploitChance': 0.9,
                      'exploitTarget': 0.75,
                      'exploitTargetHigh': 0.5,
                      'exploitTargetLow': 0.25},
    }
    paragon_variables = saltcurve_flowir[experiment.model.frontends.flowir.FlowIR.FieldVariables]['paragon']

    global_variables = paragon_variables[experiment.model.frontends.flowir.FlowIR.LabelGlobal]

    # VV: Validate that we've read the correct values for the paragon-defined variables
    backendLocal = []

    lookup = {
        'backendLocal': backendLocal,
    }

    for key in global_variables:
        if key in lookup:
            lookup[key].append(global_variables[key])

    assert tuple(backendLocal) == ('local',)


def test_stage2_vizstream_resolved(saltcurve_flowir):
    viz_stream = utility_find_component(saltcurve_flowir, 2, 'VIZStream', raw=False)

    assert set(viz_stream.keys()) == set([
        'command', 'name', 'references', 'resourceManager', 'stage', 'variables', 'workflowAttributes',
        'executors', 'resourceRequest'
    ])

    assert viz_stream['command'] == {
                "arguments": "-i VIZRender:ref/ -b $FFMPEG_PATH -e png -o rtmp://127.0.0.1:10004/live_4k "
                             "-l rtmp://127.0.0.1:10004/live_720p -w http://127.0.0.1:10003/web "
                             "-c VIZRender:ref -p $FLOW_RUN_ID -u \"\"",
                "environment": "viz",
                "executable": "bin/viz_streamer.py",
                'resolvePath': True,
                'expandArguments': 'double-quote',
                'interpreter': None,
            }

    assert set(viz_stream['references'],) == set([
         "VIZRender:ref"
    ])

    assert viz_stream['resourceManager']['config'] == {
        'backend': 'local',
        'walltime': 480.0
    }
    assert viz_stream['resourceManager']['lsf'] == {
        'queue': 'normal',
        'reservation': None,
        'resourceString': None,
        'statusRequestInterval': 60.0,
        'dockerImage': None,
        'dockerOptions': None,
        'dockerProfileApp': None,
    }


    assert viz_stream['variables'] == {
        'OverrideFromInput': 'Hello world',
        'stage-name': 'Generating',
        'VIZuserClaimToken': '',
        'arch': 'power8',
        'backend': 'lsf',
        'backendLocal': 'local',
        'defaultq': 'paragon',
        'dlmeso-number-processes': '64',
        'dlmeso-ranks-per-node': '16',
        'dpdexe': '/gpfs/cds/local/HCRI003/rla09/shared/bin/dpd_xl_spectrum_omp.exe',
        'exclusiveq': 'paragon',
        'ffdir': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/ForceFieldUnilever',
        'hpc-venv': 'pycaf-fen',
        'kill-after-producers-done-delay': '60.0',
        'moleculeLibrary': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/'
                           'ForceFieldUnilever/MOLECULE_LIBRARY',
        'mpienv': 'mpi',
        'numberPoints': '3',
        'parameterList': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/'
                         'ForceFieldUnilever/PARAMETER_LIST',
        'pythonenv': 'pythonlsf',
        'resourceSelection': 'rusage[ngpus_physical=4.00]',
        'ummapexe': '/gpfs/cds/local/HCRI003/rla09/shared/bin/ummap'
    }

    assert viz_stream['workflowAttributes'] == {
        'restartHookFile': None,
        'repeatRetries': 3,
        'maxRestarts': None,
        'aggregate': False,
        'isMigratable': False,
        'isMigrated': False,
        'isRepeat': True,
        'repeatInterval': 600.0,
        'replicate': None,
        'shutdownOn': [],
        'restartHookOn': [experiment.model.codes.exitReasons['ResourceExhausted']],
        'memoization': {'disable': {'strong': False, 'fuzzy': False}, 'embeddingFunction': None},
        'optimizer': {'disable': False,
                      'exploitChance': 0.9,
                      'exploitTarget': 0.75,
                      'exploitTargetHigh': 0.5,
                      'exploitTargetLow': 0.25},
    }
    paragon_variables = saltcurve_flowir[experiment.model.frontends.flowir.FlowIR.FieldVariables]['paragon']

    global_variables = paragon_variables[experiment.model.frontends.flowir.FlowIR.LabelGlobal]

    # VV: Validate that we've read the correct values for the paragon-defined variables
    backendLocal = []

    lookup = {
        'backendLocal': backendLocal,
    }

    for key in global_variables:
        if key in lookup:
            lookup[key].append(global_variables[key])

    assert tuple(backendLocal) == ('local',)


def test_component_parse_raw_stage0_process_inputs():
    options = raw_section_to_options(description_process_inputs)

    dos = experiment.model.frontends.dosini.Dosini()

    component = dos.parse_component(options, 'ProcessInputs', 0)

    validate_process_inputs_raw(component)


def test_dosini_typos():
    """
    [QuantumCalculation]
    executable: run_qEOM.py
    environment: qeom
    arguments: ExtractConfiguration:ref/0_molecule.conf
    reference: ExtractConfiguration:ref
    resourceManager: %(backend)s
    k8-image=res-drl-hpc-docker-local.artifactory.swg-devops.com/qeom:0.14-x.ppc64le

    There are two errors
        reference > references
        k8-image -> k8s-image
    """

    dos = experiment.model.frontends.dosini.Dosini()
    errors, possible_errors = dos.validate_component(
        {
            'executable': 'run_qEOM.py',
            'environment': 'qeom',
            "arguments": "ExtractConfiguration:ref/0_molecule.conf",
            "reference": "ExtractConfiguration:ref",
            "resourceManager": "%(backend)s",
            "k8-image": "res-drl-hpc-docker-local.artifactory.swg-devops.com/qeom:0.14-x.ppc64le",
        }, "QuantumCalculation", 0, [], [], dosini_section="stage0.QuantumCalculation"
    )

    errors = cast("List[experiment.errors.DOSINITypographicError]", errors)

    assert len(errors) == 2
    expecting = [('stage0.QuantumCalculation', 'reference', 'references'),
                 ('stage0.QuantumCalculation', 'k8-image', 'k8s-image')]

    print("Errors:")
    for err in errors:
        print("  %s" % err)

    for loc, typo, keyword in expecting:
        for err in errors:
            if err.location == loc and err.typo == typo and err.keyword == keyword:
                break
        else:
            raise ValueError("Expected error for (%s, %s, %s)" %(loc, typo, keyword))


def test_reconstruct(saltcurve_flowir, output_dir):
    dos = experiment.model.frontends.dosini.Dosini()
    dos.dump(saltcurve_flowir, output_dir, update_existing=True)

    new_flowir = load_flowir(output_dir)

    original = experiment.model.frontends.flowir.FlowIR.from_dict(saltcurve_flowir)

    reconstructed = experiment.model.frontends.flowir.FlowIR.from_dict(new_flowir)

    diff = original.diff(reconstructed)

    assert diff == {}


def test_dump_instance(saltcurve_flowir, output_dir):
    dos = experiment.model.frontends.dosini.Dosini()

    concrete = experiment.model.frontends.flowir.FlowIRConcrete(saltcurve_flowir, 'paragon', {})

    saltcurve_flowir_instance = concrete.instance(ignore_errors=True, fill_in_all=False)

    plain_processinputs = utility_plain_component(saltcurve_flowir_instance, 0, 'ProcessInputs')

    assert plain_processinputs == {
        'command': {'arguments': '-n %(numberPoints)s input/field.conf:ref > concentration_curve_points.csv',
                    'environment': '%(pythonenv)s',
                    'executable': 'concentration_curve_points.py',
                    'resolvePath': True,
                    'expandArguments': 'none',
                    'interpreter': None,
                    },
        'executors': {'main': [],
                      'post': [{'name': 'lsf-dm-out', 'payload': 'all'}],
                      'pre': []},
        'name': 'ProcessInputs',
        'references': ['input/field.conf:ref'],
        'resourceManager': {'config': {'backend': 'lsf', 'walltime': 480.0},
                            'kubernetes': {'api-key-var': None,
                                           'host': 'http://localhost:8080',
                                           'image': None,
                                           'image-pull-secret': None,
                                           'namespace': 'default',
                                           'cpuUnitsPerCore': None,
                                           'gracePeriod': None,
                                           'qos': None,
                                           'podSpec': None,
                                           },
                            'lsf': {'dockerImage': None,
                                    'dockerOptions': None,
                                    'dockerProfileApp': None,
                                    'queue': '%(defaultq)s',
                                    'reservation': None,
                                    'resourceString': None,
                                    'statusRequestInterval': 60.0}},
        'resourceRequest': {
            'memory': None,
            'numberProcesses': 1,
            'numberThreads': 1,
            'ranksPerNode': 1,
            'threadsPerCore': 1,
            'gpus': None,
        },
        'stage': 0,
        'variables': {'george': 'of the jungle'},
        'workflowAttributes': {'aggregate': False,
                               'restartHookFile': None,
                               'repeatRetries': 3,
                               'maxRestarts': None,
                               'isMigratable': False,
                               'isMigrated': False,
                               'isRepeat': False,
                               'repeatInterval': None,
                               'replicate': None,
                               'shutdownOn': [],
                               'restartHookOn': [experiment.model.codes.exitReasons['ResourceExhausted']],
                               'memoization': {'disable': {'fuzzy': False, 'strong': False}, 'embeddingFunction': None},
                               'optimizer': {'disable': False,
                                             'exploitChance': 0.9,
                                             'exploitTarget': 0.75,
                                             'exploitTargetHigh': 0.5,
                                             'exploitTargetLow': 0.25},
                               }
    }

    dos.dump(saltcurve_flowir_instance, output_dir, update_existing=True, is_instance=True)

    # VV: Need to also dump status and output
    dos._dump_status(saltcurve_flowir_instance, output_dir)
    dos._dump_output(saltcurve_flowir_instance, output_dir)

    # VV: Load DOSINI and generate raw FlowIR
    new_flowir = load_flowir_instance(output_dir)
    # VV: Populate with default FlowIR fields

    FlowIR = experiment.model.frontends.flowir.FlowIR
    new_concrete = experiment.model.frontends.flowir.FlowIRConcrete(new_flowir, FlowIR.LabelDefault, {})
    new_flowir = new_concrete.instance(ignore_errors=True, fill_in_all=False)

    # VV: At this the 2 instances contain the same information BUT because of the way DOSINI stores information on the
    # disk the information is in slightly different places.
    patched_new_instance = copy.deepcopy(new_flowir)

    variables = saltcurve_flowir_instance[experiment.model.frontends.flowir.FlowIR.FieldVariables]

    assert patched_new_instance[
               experiment.model.frontends.flowir.FlowIR.FieldVariables
           ][
               experiment.model.frontends.flowir.FlowIR.LabelDefault
           ][
               experiment.model.frontends.flowir.FlowIR.LabelGlobal
           ] == {}

    # VV: new instances don't have any global variables
    patched_new_instance[
        experiment.model.frontends.flowir.FlowIR.FieldVariables
    ][
        experiment.model.frontends.flowir.FlowIR.LabelDefault
    ][
        experiment.model.frontends.flowir.FlowIR.LabelGlobal
    ] = variables[
        experiment.model.frontends.flowir.FlowIR.LabelDefault
    ][
        experiment.model.frontends.flowir.FlowIR.LabelGlobal
    ]

    # VV: Old instances have global variables so we need to migrate them to the stage variables

    patched_old_instance = copy.deepcopy(saltcurve_flowir_instance)

    new_stage_variables = patched_new_instance[
        experiment.model.frontends.flowir.FlowIR.FieldVariables
    ][
        experiment.model.frontends.flowir.FlowIR.LabelDefault
    ][
        experiment.model.frontends.flowir.FlowIR.LabelStages
    ]

    for stage_index in new_stage_variables:
        old_stage_vars = patched_old_instance[
            experiment.model.frontends.flowir.FlowIR.FieldVariables
        ][
            experiment.model.frontends.flowir.FlowIR.LabelDefault
        ][
            experiment.model.frontends.flowir.FlowIR.LabelStages
        ].get(stage_index, {})

        global_variables = copy.deepcopy(patched_old_instance[
            experiment.model.frontends.flowir.FlowIR.FieldVariables
        ][
            experiment.model.frontends.flowir.FlowIR.LabelDefault
        ][
            experiment.model.frontends.flowir.FlowIR.LabelGlobal
        ])

        merged_variables = global_variables
        merged_variables.update(old_stage_vars)

        patched_old_instance[
            experiment.model.frontends.flowir.FlowIR.FieldVariables
        ][
            experiment.model.frontends.flowir.FlowIR.LabelDefault
        ][
            experiment.model.frontends.flowir.FlowIR.LabelStages
        ][stage_index] = merged_variables

    FlowIR = experiment.model.frontends.flowir.FlowIR
    instance = FlowIR.from_dict(patched_old_instance)

    # VV: DOSINI does not dump the `platforms` field of FlowIR, since this is an instance
    #     it will only contain the `default` platform
    assert patched_new_instance[FlowIR.FieldPlatforms] == [FlowIR.LabelDefault]
    patched_new_instance[FlowIR.FieldPlatforms] = ['default', 'paragon']
    reconstructed = FlowIR.from_dict(patched_new_instance)

    diff = instance.diff(reconstructed)

    # VV: Now the only differences are the Blueprints! (instance variables do not have a global blueprint)
    assert diff == {
        'blueprint.default.global.resourceManager': ({
            'config': {'walltime': 480.0},
            'lsf': {'statusRequestInterval': 60.0}
        },
        None
    )}


def test_override_input_files(saltcurve_flowir_override_input, output_dir):
    concrete = experiment.model.frontends.flowir.FlowIRConcrete(saltcurve_flowir_override_input, 'paragon', {})

    instance = concrete.instance(ignore_errors=True)

    dos = experiment.model.frontends.dosini.Dosini()
    dos.dump(instance, output_dir, is_instance=True)

    def test_viz_stream(viz_stream):
        assert set(viz_stream.keys()) == set([
            'command', 'name', 'references', 'resourceManager', 'stage', 'variables', 'workflowAttributes',
            'executors', 'resourceRequest'
        ])

        assert viz_stream['command'] == {
            "arguments": "-i VIZRender:ref/ -b $FFMPEG_PATH -e png -o rtmp://127.0.0.1:10004/live_4k "
                         "-l rtmp://127.0.0.1:10004/live_720p -w http://127.0.0.1:10003/web "
                         "-c VIZRender:ref -p $FLOW_RUN_ID -u \"\"",
            "environment": "viz",
            "executable": "bin/viz_streamer.py",
            'resolvePath': True,
            'expandArguments': 'double-quote',
            'interpreter': None,
        }

        assert set(viz_stream['references'], ) == set([
            "VIZRender:ref"
        ])

        assert viz_stream['resourceManager']['config'] == {
            'backend': 'local',
            'walltime': 480.0
        }

        assert viz_stream['resourceManager']['lsf'] == {
            'queue': 'normal',
            'reservation': None,
            'resourceString': None,
            'statusRequestInterval': 60.0,
            'dockerImage': None,
            'dockerOptions': None,
            'dockerProfileApp': None,
        }

        assert viz_stream['variables'] == {
            # VV: This is set to '' within the `input/variables.conf` file
            'OverrideFromInput': '',
            'stage-name': 'Generating',
            'VIZuserClaimToken': '',
            'arch': 'power8',
            'backend': 'lsf',
            'backendLocal': 'local',
            'defaultq': 'paragon',
            'dlmeso-number-processes': '64',
            'dlmeso-ranks-per-node': '16',
            'dpdexe': '/gpfs/cds/local/HCRI003/rla09/shared/bin/dpd_xl_spectrum_omp.exe',
            'exclusiveq': 'paragon',
            'ffdir': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/ForceFieldUnilever',
            'hpc-venv': 'pycaf-fen',
            'kill-after-producers-done-delay': '60.0',
            'moleculeLibrary': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/'
                               'ForceFieldUnilever/MOLECULE_LIBRARY',
            'mpienv': 'mpi',
            'numberPoints': '3',
            'parameterList': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/'
                             'ForceFieldUnilever/PARAMETER_LIST',
            'pythonenv': 'pythonlsf',
            'resourceSelection': 'rusage[ngpus_physical=4.00]',
            'ummapexe': '/gpfs/cds/local/HCRI003/rla09/shared/bin/ummap'
        }

    component = utility_find_component(
        instance, 2, 'VIZStream', raw=False, platform=experiment.model.frontends.flowir.FlowIR.LabelDefault
    )

    test_viz_stream(component)

    # VV: Now load the INSTANCE configuration and ensure that it's the same as the one that was dumped on the disk
    dos = experiment.model.frontends.dosini.Dosini()
    new_flowir = dos.load_from_directory(output_dir, [], {}, is_instance=True)

    component = utility_find_component(
        new_flowir, 2, 'VIZStream', raw=False, platform=experiment.model.frontends.flowir.FlowIR.LabelDefault,
        include_default=True
    )
    test_viz_stream(component)
