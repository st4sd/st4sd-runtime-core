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

import json
import logging
import os

import pytest

import experiment.model.codes
import experiment.model.errors
import experiment.model.frontends.dosini
import experiment.model.storage
from experiment.model.conf import ExperimentConfigurationFactory
from experiment.model.frontends.flowir import (FlowIR, FlowIRCache, FlowIRConcrete,
                                               instantiate_workflow,
                                               validate_object_schema)
from .utils import  experiment_from_flowir
from .saltcurve_paragon import SaltCurveGenerator
from .test_dosini import (salt_curve_conf, salt_curve_whole_package,
                          saltcurve_flowir)

logger = logging.getLogger()

def pretty_json(entry):
    return json.dumps(entry, sort_keys=True, indent=4, separators=(',', ': '))


def test_cpuUnitsPerCore():
    def_cpu_unis = experiment.model.frontends.flowir.FlowIR.default_cpuUnitsPerCore()
    logger.info("Cpu Units per core %s" % def_cpu_unis)
    assert def_cpu_unis is not None


def test_variable_resolution_primitive_with_replica_variable():
    val = experiment.model.frontends.flowir.FlowIR.interpolate('%(replica)s %(hello)s %(replica)s', {'replica': 10,
                                                                                                'hello': 5},
                                                                              is_primitive=True)
    assert val == "10 5 10"

    val2 = experiment.model.frontends.flowir.FlowIR.interpolate('%(replica)s %(hello)s %(replica)s', {'hello': 5},
                                                                               is_primitive=True)
    assert val2 == "%(replica)s 5 %(replica)s"


def test_variable_resolution():
    platform = 'paragon'

    flowir = {
        FlowIR.FieldApplicationDependencies: {},
        FlowIR.FieldVariables: {
            FlowIR.LabelDefault: {
                FlowIR.LabelGlobal: {
                    'hello': 'hello_default_global',
                    'world': 'world_default_global',
                    'hello-p_g': 'hello_default_global_2',
                    'hello-d_g': 'hello_default_global_3',
                },
                FlowIR.LabelStages: {
                    0: {
                        'hello': 'hello_default_stages_0',
                        'world': 'world_default_stages_0',
                        'hello-p_g': 'hello_default_stages_0_2',
                        'hello-p_s_0': 'hello_default_stages_0_3',
                        'hello-d_s_0': 'hello_default_stages_0_4',
                    }
                }
            },
            platform: {
                FlowIR.LabelGlobal: {
                    'hello': 'hello_paragon_global',
                    'world': 'world_paragon_global',
                    'hello-p_g': 'hello_paragon_global2',
                },
                FlowIR.LabelStages: {
                    0: {
                        'hello': 'hello_paragon_stages_0',
                        'world': 'world_paragon_stages_0',
                        'hello-p_s_0': 'hello_paragon_stages_0_2',
                    }
                }
            },
        },

        FlowIR.FieldEnvironments: {
            FlowIR.LabelDefault: {
                FlowIR.LabelGlobal: {
                },
                platform: {

                },
            },
        },

        FlowIR.FieldOutput: {},
        FlowIR.FieldStatusReport: {},
        FlowIR.FieldComponents: [
            {
                "command": {
                    "arguments": "-n %(numberPoints)s input/field.conf:ref > concentration_curve_points.csv",
                    "environment": "%(pythonenv)s",
                    "executable": "concentration_curve_points.py"
                },
                "executors": {
                    "post": {
                        "lsf-dm-out": {
                            "payload": "all",
                        }
                    }
                },
                "name": "ProcessInputs",
                "references": [
                    "input/field.conf:ref"
                ],
                "resourceManager": {
                    "config": {
                        "backend": "lsf"
                    }
                },
                "stage": 0,
                "variables": {
                    "hello": "component_hello",
                    "world": "component_world",
                }
            },
        ]
    }

    raw = FlowIR.from_dict(flowir)
    concrete = FlowIRConcrete(flowir, platform, {})

    format = (
        '%(hello)s %(world)s! '
        '%(hello)s '
        '%(hello-p_g)s '
        '%(hello-p_s_0)s! '
        '%(hello-d_g)s '
        '%(hello-d_s_0)s'
    )

    context = concrete.get_component_variables((0, 'ProcessInputs'))
    val = raw.interpolate(format, context)

    assert val == (
        'component_hello '
        'component_world! '
        'component_hello '
        'hello_paragon_global2 '
        'hello_paragon_stages_0_2! '
        'hello_default_global_3 '
        'hello_default_stages_0_4'
    )

    concrete.configure_platform(None)

    context = concrete.get_component_variables((0, 'ProcessInputs'))
    val = raw.interpolate(format, context)

    assert val == (
        'component_hello component_world! '
        'component_hello '
        'hello_default_stages_0_2 '
        'hello_default_stages_0_3! '
        'hello_default_global_3 '
        'hello_default_stages_0_4'
    )


@pytest.fixture
def concrete_with_overridable_component():
    flowir = {
        FlowIR.FieldPlatforms: ['one', 'two',],
        FlowIR.FieldComponents: [
            {
                'stage': 0,
                'name': 'overrideMe',
                'command': {
                    'executable': 'the-default'
                },
                'variables': {
                    'override-me': 'var-default',
                },
                'override': {
                    'one': {
                        'command': {
                            'executable': 'from-one'
                        },
                        'variables': {
                            'override-me': 'var-from-one',
                        },
                    },
                    'two': {
                        'command': {
                            'executable': 'from-two',
                        },
                        'variables': {
                            'override-me': 'var-from-two',
                        },
                    },
                }
            }
        ]
    }

    concrete = FlowIRConcrete(flowir, None, {})

    return concrete


def test_override_component_options_success(concrete_with_overridable_component):
    concrete = concrete_with_overridable_component
    comp_id = (0, 'overrideMe')

    default_exec = concrete.get_component(comp_id)['command']['executable']
    default_var = concrete.get_component(comp_id)['variables']['override-me']
    comp_override = concrete.get_component(comp_id)['override']
    for platform in ['one', 'two', 'default']:
        if platform != 'default':
            expected_exec = 'from-%s' % platform
            expected_var = 'var-from-%s' % platform
        else:
            expected_exec = default_exec
            expected_var = default_var
        comp = concrete.get_component_configuration(
            comp_id, include_default=False, ignore_convert_errors=False, platform=platform, is_primitive=False,
            inject_missing_fields=False
        )
        expected_dict = {
            'command': {'executable': expected_exec},
            'name': 'overrideMe',
            'override': comp_override,
            'stage': 0,
            'variables': {
                'override-me': expected_var,
            },
        }

        if comp != expected_dict:
            print("Override is unexpected (actual vs expected):")
            print((pretty_json(comp)))
            print((pretty_json(expected_dict)))
            errors = validate_object_schema(comp, expected_dict, 'component')
            for err in errors:
                print(err)
            assert comp == expected_dict


def test_override_component_options_failure(concrete_with_overridable_component):
    concrete = concrete_with_overridable_component
    comp_id = (0, 'overrideMe')

    with pytest.raises(experiment.model.errors.FlowIRPlatformUnknown):
        comp = concrete.get_component_configuration(
            comp_id, include_default=False, ignore_convert_errors=False, platform='unknown platform',
            is_primitive=False, inject_missing_fields=False
        )
        logger.critical("This was not supposed to return %s" % comp)


def test_instantiate_flowir(salt_curve_conf):
    dos = experiment.model.frontends.dosini.Dosini()
    flowir = dos.load_from_directory(salt_curve_conf, [], {})

    concrete = FlowIRConcrete(flowir, 'paragon', {})

    global_blueprint = concrete.get_default_global_blueprint()
    assert global_blueprint == {}

    platform_blueprint = concrete.get_platform_blueprint()
    assert platform_blueprint == {
        'resourceManager': {
            'config': {
                'walltime': 480.0
            },
            'lsf': {
                'statusRequestInterval': 60.0
            }
        },
    }

    platform_stage_blueprint = concrete.get_platform_stage_blueprint(0)
    assert platform_stage_blueprint == {}

    platform_stage_blueprint = concrete.get_platform_stage_blueprint(1)
    assert platform_stage_blueprint == {}

    platform_stage_blueprint = concrete.get_platform_stage_blueprint(2)
    assert platform_stage_blueprint == {}

    global_stage_blueprint = concrete.get_default_stage_blueprint(0)
    assert global_stage_blueprint == {}

    global_stage_blueprint = concrete.get_default_stage_blueprint(1)
    assert global_stage_blueprint == {}

    global_stage_blueprint = concrete.get_default_stage_blueprint(2)
    assert global_stage_blueprint == {}

    component = concrete.get_component((2, 'CAFSimulation'))
    assert component == {
        'command': {
            'environment': '%(mpienv)s',
            'executable': '%(dpdexe)s'
        },
        'executors': {
            'post': [
                {
                    'name': 'lsf-dm-out',
                    'payload': 'OUTPUT'
                }
            ],
            'pre': [
                {
                    'name': 'lsf-dm-in',
                    'payload': 'stage1.FIELD/FIELD:copy stage1.CONTROL/CONTROL:copy'
                }
            ]
        },
        'name': 'CAFSimulation',
        'references': ['stage1.FIELD/FIELD:copy', 'stage1.CONTROL/CONTROL:copy'],
        'resourceManager': {
            'kubernetes': {'api-key-var': 'Dummy_b_2',
                           'host': 'Dummy_b_1',
                           'image': 'Dummy_b_0',
                           'image-pull-secret': 'Dummy_b_3',
                           'namespace': 'Dummy_b_4',
                           'cpuUnitsPerCore': 9999,
                           'gracePeriod': 4,
                           },
            'config': {
                'backend': '%(backend)s',
                'walltime': 240.0
            },
            'lsf': {
                'dockerImage': 'Dummy_a_1',
                'dockerOptions': 'Dummy_a_0',
                'dockerProfileApp': 'Dummy_a_2',
                'queue': '%(exclusiveq)s'
            }
        },
        'resourceRequest': {
            'memory': '1Mi',
            'numberProcesses': '%(dlmeso-number-processes)s',
            'numberThreads': 2,
            'ranksPerNode': '%(dlmeso-ranks-per-node)s',
            'threadsPerCore': 2
        },
        'stage': 2,
        'workflowAttributes': {
            'restartHookFile': 'custom-restart-hook.py',
            'restartHookOn': ['UnknownIssue'],
            'shutdownOn': ['KnownIssue', 'SystemIssue'],
            'memoization': {'disable': {'fuzzy': True, 'strong': True,}},
            'optimizer': {'disable': True,
                          'exploitChance': 0.99,
                          'exploitTarget': 0.98,
                          'exploitTargetHigh': 0.97,
                          'exploitTargetLow': 0.96},
        }
    }

    global_variables = concrete.get_default_global_variables()
    assert global_variables == {
        'OverrideFromInput': 'Hello world',
        'VIZuserClaimToken': '',
        'moleculeLibrary': 'caf/data/MOLECULE_LIBRARY_trumbull',
        'numberPoints': '3',
        'parameterList': 'caf/data/PARAMETER_LIST_trumbull',
        'resourceSelection': '',
        'ummapexe': '/gpfs/cds/local/HCRI003/rla09/shared/bin/ummap'
    }

    platform_variables = concrete.get_platform_global_variables()
    assert platform_variables == {
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
        'parameterList': '%(ffdir)s/PARAMETER_LIST',
        'pythonenv': 'pythonlsf'
    }

    global_stage_variables = concrete.get_default_stage_variables(0)
    assert global_stage_variables == {
        'stage-mame': 'CheckInputs'
    }

    global_stage_variables = concrete.get_default_stage_variables(1)
    assert global_stage_variables == {
        'stage-name': 'Setup'
    }

    global_stage_variables = concrete.get_default_stage_variables(2)
    assert global_stage_variables == {
        'stage-name': 'Generating'
    }

    platform_stage_variables = concrete.get_platform_stage_variables(0)
    assert platform_stage_variables == {}

    platform_stage_variables = concrete.get_platform_stage_variables(1)
    assert platform_stage_variables == {}

    platform_stage_variables = concrete.get_platform_stage_variables(2)
    assert platform_stage_variables == {
        'resourceSelection': 'rusage[ngpus_physical=4.00]'
    }

    component_variables = concrete.get_component_variables(
        (2, 'CAFSimulation'),
        include_platform_stage=False,
        include_platform_global=False,
        include_default_stage=False,
        include_default_global=False,
    )

    assert component_variables == {}

    instance = concrete.instance(fill_in_all=True, is_primitive=True)

    dos.dump(instance, salt_curve_conf, update_existing=True, is_instance=True)

    instance_flowir = dos.load_from_directory(
        salt_curve_conf,
        [os.path.join(salt_curve_conf, '..', 'input', 'variables.conf')], {}, is_instance=True)

    assert instance_flowir[FlowIR.FieldVirtualEnvironments] == {
        'default': ['/gpfs/cds/local/HCRI003/rla09/shared/virtualenvs/pycaf-fen']
    }

    assert instance_flowir[FlowIR.FieldApplicationDependencies] == {
        'default': ['CAF.application', 'DPD.application', 'Viz.application']
    }

    assert instance_flowir[FlowIR.FieldVariables] == {
        'default': {
            # VV: There should be no global variables because instance files do not read `variables.conf`

            # VV: Stage variables should now contain all platform variables
            'stages': {
                0: {
                    # VV: This value is defined in variables.conf (components should have '' instead of 'Hello world'
                    #     because they're using the variables defined in the `input/variables.conf` file)
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
                    'moleculeLibrary': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/'
                                       'ForceFieldUnilever/MOLECULE_LIBRARY',
                    'mpienv': 'mpi',
                    'numberPoints': '3',
                    'parameterList': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/'
                                     'ForceFieldUnilever/PARAMETER_LIST',
                    'pythonenv': 'pythonlsf',
                    'resourceSelection': '',
                    'stage-mame': 'CheckInputs',
                    'ummapexe': '/gpfs/cds/local/HCRI003/rla09/shared/bin/ummap'
                },
                1: {
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
                    'moleculeLibrary': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/'
                                       'ForceFieldUnilever/MOLECULE_LIBRARY',
                    'mpienv': 'mpi',
                    'numberPoints': '3',
                    'parameterList': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/'
                                     'ForceFieldUnilever/PARAMETER_LIST',
                    'pythonenv': 'pythonlsf',
                    'resourceSelection': '',
                    'stage-name': 'Setup',
                    'ummapexe': '/gpfs/cds/local/HCRI003/rla09/shared/bin/ummap'
                },
                2: {
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
                    'moleculeLibrary': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/'
                                       'ForceFieldUnilever/MOLECULE_LIBRARY',
                    'mpienv': 'mpi',
                    'numberPoints': '3',
                    'parameterList': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/'
                                     'ForceFieldUnilever/PARAMETER_LIST',
                    'pythonenv': 'pythonlsf',
                    'resourceSelection': 'rusage[ngpus_physical=4.00]',
                    'stage-name': 'Generating',
                    'ummapexe': '/gpfs/cds/local/HCRI003/rla09/shared/bin/ummap'
                }
            }
        }
    }

    assert set(instance_flowir[FlowIR.FieldPlatforms]) == set(['default'])

    assert instance_flowir[FlowIR.FieldEnvironments] == {
        'default': {
            'ENVIRONMENT': {
                'OMP_NUM_THREADS': '4'
            },
            'GNUPLOT': {
                'DEFAULTS': 'PATH:LD_LIBRARY_PATH',
                'LD_LIBRARY_PATH': '/gpfs/paragon/local/apps/gcc/utilities/lib/:'
                                   'LD_LIBRARY_PATH',
                'PATH': '/gpfs/paragon/local/apps/gcc-7.1.0/gnuplot/5.2.2/bin/:'
                        '$PATH',
                'PYTHONPATH': '/gpfs/cds/local/HCRI003/rla09/shared/virtualenvs/pycaf-fen/lib/python2.7/site-packages:'
                              '/gpfs/paragon/local/apps/python2/2.7.8/lib/python2.7/site-packages'
            },
            'MPI': {
                'DEFAULTS': 'PATH:LD_LIBRARY_PATH',
                'LD_LIBRARY_PATH': '/gpfs/paragon/local/apps/cuda/8.0/lib64:'
                                   '/opt/ibm/lib:/gpfs/paragon/local/apps/ibm/spectrum_mpi/10.1.0/lib/:'
                                   '$LD_LIBRARY_PATH',
                'LD_RUN_PATH': '/gpfs/paragon/local/apps/cuda/8.0/lib64:'
                               '/gpfs/paragon/local/apps/ibm/spectrum_mpi/10.1.0/lib/:'
                               '$LD_RUN_PATH',
                'LSF_MPIRUN': '/gpfs/paragon/local/apps/ibm/spectrum_mpi/10.1.0/bin/mpirun',
                'PATH': '/gpfs/paragon/local/apps/ibm/spectrum_mpi/10.1.0/bin/:'
                        '$PATH'
            },
            'PYTHONLSF': {
                'LD_LIBRARY_PATH': '/gpfs/paragon/local/apps/gcc/lapack/3.7.0/lib64:'
                                   '$LD_LIBRARY_PATH',
                'OMP_NUM_THREADS': '4',
                'PATH': '/gpfs/cds/local/HCRI003/rla09/shared/virtualenvs/pycaf-fen/bin:'
                        '$PATH',
                'PYTHONPATH': '/gpfs/cds/local/HCRI003/rla09/shared/virtualenvs/pycaf-fen/lib/python2.7/site-packages:'
                              '/gpfs/paragon/local/apps/python2/2.7.8/lib/python2.7/site-packages',
                'PYTHON_EGG_CACHE': '$HOME'
            },
            'UMMAP': {
                'DEFAULTS': 'PATH',
                'LD_LIBRARY_PATH': '/opt/ibm/lib:$LD_LIBRARY_PATH'
            },
            'VIZ': {
                'DEFAULTS': 'PATH:LD_LIBRARY_PATH',
                'FFMPEG_PATH': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/'
                               'Viz.application/bin/ffmpeg-3.4.4/bin/ffmpeg',
                'LD_LIBRARY_PATH': '/opt/ibm/lib:'
                                   '/gpfs/paragon/local/HCRI003/rla09/shared/src/spack/opt/spack/linux-rhel7-ppc64le/'
                                   'gcc-4.8.5/image-magick-7.0.2-7-mpwuezn65ybc7fv4caxmkxhkirivlfwy/lib:'
                                   '$LD_LIBRARY_PATH',
                'PATH': '/gpfs/paragon/local/HCRI003/rla09/shared/src/spack/opt/spack/linux-rhel7-ppc64le/gcc-4.8.5/'
                        'image-magick-7.0.2-7-mpwuezn65ybc7fv4caxmkxhkirivlfwy/bin:'
                        '$PATH',
                'PYTHONPATH': '/gpfs/cds/local/HCRI003/rla09/shared/virtualenvs/pycaf-fen/lib/python2.7/site-packages:'
                              '/gpfs/paragon/local/apps/python2/2.7.8/lib/python2.7/site-packages',
                'TACHYON_PATH': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/Viz.application/'
                                'bin/power8/tachyon',
                'VMD_PATH': '/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/Viz.application/'
                            'bin/power8/vmd'
            }
        }
    }

    concrete = FlowIRConcrete(instance_flowir, experiment.model.frontends.flowir.FlowIR.LabelDefault, {})
    comp = concrete.get_component_configuration((0, "ProcessInputs"), include_default=False)

    assert set(comp) == set([
        'name', 'workflowAttributes', 'executors', 'variables', 'references', 'resourceRequest',
        'command', 'resourceManager', 'stage'
    ])

    assert comp['name'] == 'ProcessInputs'
    assert comp['workflowAttributes'] == {
        'restartHookFile': None,
        'repeatRetries': 3,
        'shutdownOn': [],
        'maxRestarts': None,
        'restartHookOn': [experiment.model.codes.exitReasons['ResourceExhausted']],
        'repeatInterval': None,
        'isRepeat': False,
        'aggregate': False,
        'replicate': None,
        'isMigrated': False,
        'isMigratable': False,
        'memoization': {'disable': {'fuzzy': False, 'strong': False}, 'embeddingFunction': None},
        'optimizer': {'disable': False,
                      'exploitChance': 0.9,
                      'exploitTarget': 0.75,
                      'exploitTargetHigh': 0.5,
                      'exploitTargetLow': 0.25},
    }
    assert comp['executors'] == {
        'pre': [],
        'post': [
            {
                'name': 'lsf-dm-out',
                'payload': 'all'
            }
        ],
        'main': []
    }
    assert comp['variables'] == {
        # VV: This value is fetched from the user variables (input/variables.conf)
        'OverrideFromInput': '',
        # VV: This is defined in the component
        'george': 'of the jungle'
    }
    assert comp['references'] == [
        'input/field.conf:ref'
    ]

    assert comp['resourceRequest'] == {
        'memory': None,
        'numberThreads': 1,
        'threadsPerCore': 1,
        'ranksPerNode': 1,
        'numberProcesses': 1,
        'gpus': None
    }
    assert comp['command'] == {
        'environment': 'pythonlsf',
        'executable': 'concentration_curve_points.py',
        'arguments': '-n 3 input/field.conf:ref > concentration_curve_points.csv',
        'resolvePath': True,
        'expandArguments': 'none',
        'interpreter': None,
    }
    assert comp['resourceManager'] == {
        'config': {
            'backend': 'lsf',
            'walltime': 480.0,
        },
        'kubernetes': {
            'api-key-var': None,
            'host': 'http://localhost:8080',
            'image': None,
            'image-pull-secret': None,
            'namespace': 'default',
            'cpuUnitsPerCore': None,
            'gracePeriod': None,
            'qos': None,
            'podSpec': None,
        },
        'lsf': {
            'queue': 'paragon',
            'reservation': None,
            'resourceString': None,
            'statusRequestInterval': 60.0,
            'dockerImage': None,
            'dockerOptions': None,
            'dockerProfileApp': None,
        }
    }
    assert comp['stage'] == 0


def test_application_dependencies_override():
    default_platform = experiment.model.frontends.flowir.FlowIR.LabelDefault
    flowir = {
        experiment.model.frontends.flowir.FlowIR.FieldApplicationDependencies: {
            default_platform: ['/this/is/a/default/application/dependency'],
            'override': ['this is not'],
        }
    }

    concrete = experiment.model.frontends.flowir.FlowIRConcrete(flowir, 'override', {})

    assert concrete.get_application_dependencies('override') == ['this is not']
    assert concrete.get_application_dependencies(default_platform) == ['/this/is/a/default/application/dependency']
    assert concrete.get_application_dependencies('platform with no application-dependencies key') == [
        '/this/is/a/default/application/dependency'
    ]


def test_flowir_experiment_configuration(salt_curve_whole_package):

    econf = ExperimentConfigurationFactory.configurationForExperiment(
        salt_curve_whole_package,
        platform='paragon',
        primitive=False,
        createInstanceFiles=True,
    )
    pythonlsf = econf.environmentForNode('stage0.ProcessInputs', expand=False)

    assert pythonlsf == {
        "LD_LIBRARY_PATH": "/gpfs/paragon/local/apps/gcc/lapack/3.7.0/lib64:$LD_LIBRARY_PATH",
        "OMP_NUM_THREADS": "4",
        "PATH": "/gpfs/cds/local/HCRI003/rla09/shared/virtualenvs/pycaf-fen/bin:"
                "$PATH",
        "PYTHONPATH": "/gpfs/cds/local/HCRI003/rla09/shared/virtualenvs/pycaf-fen/lib/python2.7/site-packages:"
                      "/gpfs/paragon/local/apps/python2/2.7.8/lib/python2.7/site-packages",
        "PYTHON_EGG_CACHE": "$HOME",
    }

    viz_stream = econf.configurationForNode('stage2.VIZStream0')

    assert viz_stream['resourceManager'] == {
        'config': {
            'backend': 'local',
            'walltime': 480.0
        },
        'kubernetes': {
            'api-key-var': None,
            'host': 'http://localhost:8080',
            'image': None,
            'image-pull-secret': None,
            'namespace': 'default',
            'cpuUnitsPerCore': None,
            'gracePeriod': None,
            'qos': None,
            'podSpec': None,
        },
        'lsf': {
            'queue': 'normal',
            'reservation': None,
            'resourceString': None,
            'statusRequestInterval': 60.0,
            'dockerImage': None,
            'dockerOptions': None,
            'dockerProfileApp': None,
        }
    }


def test_incomplete_variables_load():
    raw = {
        'variables': {
            'default': {
                'global': {
                    'world0': '0',
                    'world1': '1',
                    'world2': '2',
                    'world3': '3',
                    'world4': '4'
                }
            }
        },
        'components': [
            {'command': {
                'executable': '%(world0)%(world0)hello %(world1)s %(world2) '
                               '%(world3)s %(world4) %(replica)s %(replica)'
            }}
        ]
    }

    flowir = experiment.model.frontends.flowir.FlowIR.from_dict(raw)
    flowir = flowir.inject_default_values()

    errors = experiment.model.frontends.flowir.FlowIR.validate(flowir, {})
    assert len(errors) == 1

    exc = errors[0]  # type: experiment.model.errors.FlowIRVariablesIncomplete
    print(exc)
    assert exc.reference_strings == sorted(['%(replica)', '%(world0)', '%(world0)', '%(world2)', '%(world4)'])


def test_incomplete_variables_interpolate():
    with pytest.raises(experiment.model.errors.FlowIRVariablesIncomplete) as e:
        ret = FlowIR.interpolate("%(world0)%(world0)hello %(world1)s %(world2) "
                                 "%(world3)s %(world4) %(replica)s %(replica)", {
            'world0': 0, 'world1': '1', 'world2': 2, 'world3': 3, 'world4': '4'
        }, is_primitive=True,)
        print(("Resolved: %s" % ret))
    exc = e.value
    assert exc.reference_strings == sorted(['%(replica)', '%(world0)', '%(world0)', '%(world2)', '%(world4)'])


def test_flowir_diff_resourceManager():
    rma = {
        'config': {
            'backend': 'local',
            'walltime': 480.0
        },
        'lsf': {
            'queue': 'normal',
            'reservation': None,
            'resourceString': None,
            'statusRequestInterval': 60.0
        }
    }

    rmb = {
        'config': {
            'backend': 'local',
            'walltime': 480.0
        },
        'lsf': {
            'queue': 'normal',
            'statusRequestInterval': 60.0,
            'dummy': 3,
        }
    }

    diff = FlowIR._diff_objects(rma, rmb, 'resourceManager')

    assert diff == {
        'resourceManager.lsf.reservation(missing rhs)': (None, None),
        'resourceManager.lsf.resourceString(missing rhs)': (None, None),
        'resourceManager.lsf.dummy(missing lhs)': (None, 3)
    }


def test_modify_replicated_and_aggregating_component_arguments(output_dir):
    flowir="""
components:
- name: root
  command:
    executable: echo
    arguments: world
  workflowAttributes:
    replicate: 1
- name: propagate
  command:
    executable: echo
    arguments: root:ref
  references:
    - stage0.root:ref
- name: terminate
  command:
    executable: echo
    arguments: propagate:ref
  references:
  - stage0.propagate:ref
  workflowAttributes:
    aggregate: True 
"""

    exp = experiment_from_flowir(flowir, output_dir)
    concrete = exp.configuration.get_flowir_concrete()

    root = concrete.get_component_configuration((0, "root0"))
    propagate = concrete.get_component_configuration((0, "propagate0"))
    terminate = concrete.get_component_configuration((0, "terminate"))

    assert root['references'] == []
    assert propagate['references'] == ['stage0.root0:ref']
    assert terminate['references'] == ['stage0.propagate0:ref']


def test_generate_replicate(saltcurve_flowir):
    concrete = experiment.model.frontends.flowir.FlowIRConcrete(saltcurve_flowir, 'paragon', {})
    replicated = concrete.replicate(ignore_errors=True)

    def extract_components(stage, name):
        components = replicated.get('components', [])

        extracted = []

        for comp in components:
            if comp['stage'] == stage and comp['name'].startswith(name):
                extracted.append(comp)

        return extracted

    vizrender = extract_components(2, 'VIZRender')
    replica_names = [comp['name'] for comp in vizrender]

    assert sorted(replica_names) == sorted(['VIZRender0', 'VIZRender1', 'VIZRender2'])

    replica_ids = [comp['variables']['replica'] for comp in vizrender]

    assert replica_ids == [0, 1, 2]

    all_replicas = [comp for comp in replicated.get('components', [])
                    if comp.get('variables', {}).get('replica', None) is not None]

    all_replica_names = sorted([comp['name'] for comp in all_replicas])

    assert all_replica_names == sorted([
        'CONTROL0',
        'CONTROL1',
        'CONTROL2',
        'FIELD0',
        'FIELD1',
        'FIELD2',
        'CAFObservableCalculation0',
        'CAFObservableCalculation1',
        'CAFObservableCalculation2',
        'CAFSimulation0',
        'CAFSimulation1',
        'CAFSimulation2',
        'DetermineShape0',
        'DetermineShape1',
        'DetermineShape2',
        'Render0',
        'Render1',
        'Render2',
        'VIZStream0',
        'VIZStream1',
        'VIZStream2',
        'Convert0',
        'Convert1',
        'Convert2',
        'ClusterAnalysis0',
        'ClusterAnalysis1',
        'ClusterAnalysis2',
        'VIZRender0',
        'VIZRender1',
        'VIZRender2',
    ])

    all_aggregate = [comp for comp in replicated.get('components', [])
                     if comp.get('workflowAttributes', {}).get('aggregate', None) is True]

    all_aggregate_names = [comp['name'] for comp in all_aggregate]

    assert sorted(all_aggregate_names) == sorted(['Montage', 'AggregateCAFObservables', 'AggregateShapeAnalysis'])

    determine_shape = extract_components(2, 'DetermineShape0')[0]

    assert determine_shape['references'] == ['stage2.ClusterAnalysis0:ref']

    assert determine_shape['command']['arguments'] == (
        "--new -w 200 stage2.ClusterAnalysis0:ref/UMMAPOUT/Data/Micelle_Cluster/TIME_Cluster_stats_[Tail].dat"
    )

    assert determine_shape['command']['executable'] == '%(hpc-venv)s/bin/calculate_shape.py'

    montage = extract_components(2, 'Montage')[0]

    assert sorted(montage['references']) == sorted([
        'stage2.Convert0:ref',
        'stage2.Convert1:ref',
        'stage2.Convert2:ref',
        'data/Key.png:ref'
    ])

    assert montage['command']['arguments'] == (
        'stage2.Convert0:ref/lastframe.png '
        'stage2.Convert1:ref/lastframe.png '
        'stage2.Convert2:ref/lastframe.png '
        'data/Key.png:ref -mode Concatenate '
        '-tile "$((1+%(numberPoints)s))"x1 -gravity center SaltCurve.jpg'
    )


def test_reference_extraction():
    flowir = experiment.model.frontends.flowir.FlowIR()

    assert flowir.discover_reference_strings(
        "-f CAFSimulation:ref/HISTORY -t ALL -g {Tail} RESATOMID 1 2 3 4 "
        "AND NOT RESNAME NACL -m C [+d 1 +t 1  +pm 5 +hist COUNT"
        " +c +o FIXED +b N +printconnect Y ] S [ +snap 1 ] -par "
        "data/ummap_parameters.inp:ref -o UMMAPOUT -continue",
        implied_stage=0, component_ids=[(0, 'CAFSimulation')]
    ) == sorted(['stage0.CAFSimulation:ref', 'data/ummap_parameters.inp:ref'])

    assert flowir.discover_reference_strings(
        "-f CAFSimulation:ref/HISTORY -t ALL -g {Tail} RESATOMID 1 2 3 4 "
        "AND NOT RESNAME NACL -m C [+d 1 +t 1  +pm 5 +hist COUNT"
        " +c +o FIXED +b N +printconnect Y ] S [ +snap 1 ] -par "
        "data/ummap_parameters.inp:ref -o UMMAPOUT -continue",
        implied_stage=1, component_ids=[(0, 'CAFSimulation')]
    ) == sorted(['CAFSimulation:ref', 'data/ummap_parameters.inp:ref'])


def test_flowir_cache():
    cache = FlowIRCache()

    cache['component:default:stage0:hello'] = {
        'hello': 'world'
    }

    assert cache._cache == {'component:default:stage0:hello': {'hello': 'world'}}

    # VV: Erase everything in the cache
    removed = cache.invalidate_selector(lambda *x: True)

    assert removed == {'component:default:stage0:hello': {'hello': 'world'}}

    assert cache._cache == {}

    cache['component:default:stage0:hello'] = {
        'hello': 'world'
    }

    del cache['component:default:stage0:hello']
    assert cache._cache == {}

    # VV: This should not raise errors even though the entry doesn't exist in the cache
    del cache['component:default:stage0:hello']

    with pytest.raises(KeyError):
        this_should_raise_exception = cache['component:default:stage0:hello']

    cache['component:default:stage0:hello'] = {
        'hello': 'world'
    }

    removed = cache.invalidate_reg_expression(r"component:.*:stage0:hello*")

    assert removed == {'component:default:stage0:hello': {'hello': 'world'}}

    cache['component:default:stage0:hello'] = {
        'hello': 'world'
    }

    removed = cache.invalidate_reg_expression(r"component:.*:stage0:not-hello*")

    assert removed == {}

    del cache['does not exist']

    assert cache._cache == {'component:default:stage0:hello': {'hello': 'world'}}

    cache.clear()

    cache.disable()

    cache['component:default:stage0:hello'] = {
        'hello': 'world'
    }

    assert cache._cache == {'component:default:stage0:hello': {'hello': 'world'}}

    with pytest.raises(ValueError):
        _ = cache['component:default:stage0:hello']

    del cache['component:default:stage0:hello']

    assert cache._cache == {}

    cache.enable()

    cache['component:default:stage0:hello'] = {
        'hello': 'world'
    }

    assert cache['component:default:stage0:hello'] == {'hello': 'world'}


def test_import_workflow_feature():
    wf_template = {
        'type': 'Workflow',
        'inputBindings': {
            'hello': {
                'type': 'ref',
            }
        },
        'components': [
            {
                'name': 'george',
            },
            {
                'name': 'consumer-of-george',
                'stage': 1,
                'references': [
                    'stage0.george:ref'
                ],
                'command': {
                    'arguments': 'hello:ref stage0.george:ref'
                }
            },
            {
                'name': 'observer-of-george',
                'stage': 0,
                'references': [
                    'george:ref',
                ]
            }
        ]
    }

    components = instantiate_workflow(wf_template, bindings={'hello': 'stage4.world:ref'}, import_to_stage=5,
                                      foreign_components=set([(4, 'world')]))

    assert components == [
        {
            'name': 'george',
            'stage': 5
        },
        {
            'command': {
                'arguments': 'stage4.world:ref stage5.george:ref'
            },
            'name': 'consumer-of-george',
            'references': ['stage5.george:ref'],
            'stage': 6},
        {
            'name': 'observer-of-george',
            'references': ['stage5.george:ref'],
            'stage': 5
        }
    ]



def test_propagate_replicate():
    components = [
        {
            'name': 'add',
            'references': [
                'number:output'
            ],
            'workflowAttributes': {
                'replicate': '%(numberPoints)s'
            }
        },
        {
            'name': 'no-deps-rep',
            'workflowAttributes':{
                'replicate': 1,
            }
        },
        {
            'name': 'no-deps-aggregate',
            'workflowAttributes': {
                'aggregate': 'True'
            }
        },
        {
            'name': 'no-deps-plain',
        },
        {
            'name': 'propagated-replicate',
            'references': [
                'add:output'
            ]
        },
        {
            'name': 'propagated-replicate-absolute-ref',
            'references': [
                'stage0.add:ref'
            ]
        },
    ]

    # VV: This method can propagate the replicate field of workflowAttributes on a subgraph of a node (useful for
    #     reasoning about whether a components in DoWhile/Workflow documents need to be replicated before actually
    #     performing the FlowIRConcrete replicate step)
    ret = FlowIR.propagate_replicate(components, True)

    assert ret == {
        (0, 'add'): ('%(numberPoints)s', False),
        (0, 'no-deps-aggregate'): (None, True),
        (0, 'no-deps-plain'): (None, False),
        (0, 'no-deps-rep'): (1, False),
        (0, 'propagated-replicate'): ('%(numberPoints)s', False),
        (0, 'propagated-replicate-absolute-ref'): ('%(numberPoints)s', False),
    }

    variables = {
        FlowIR.LabelGlobal: {
            'numberPoints': "3"
        },
        FlowIR.LabelStages: {
            0: {
                'numberPoints': "1"
            }
        }
    }

    components.append({
            'name': 'usual-aggregate',
            'references': [
                'stage0.add:ref',
                'no-deps-plain:ref',
                'propagated-replicate:ref',
                'no-deps-rep:ref',
            ],
            'workflowAttributes': {
                'aggregate': 'true',
            }
        })

    # VV: use variables to understand that numberPoints = 1 in stage 0 therefore the aggregate component
    #     `usual-aggregate` is valid
    ret = FlowIR.propagate_replicate(components, True, variables)

    assert ret == {
        (0, 'add'): (1, False),
        (0, 'no-deps-aggregate'): (None, True),
        (0, 'no-deps-plain'): (None, False),
        (0, 'no-deps-rep'): (1, False),
        (0, 'propagated-replicate'): (1, False),
        (0, 'propagated-replicate-absolute-ref'): (1, False),
        (0, 'usual-aggregate'): (1, True)
    }

    # VV: Now raise an exception because Flow can't tell what the value of numberPoints is
    with pytest.raises(ValueError) as e:
        _ = FlowIR.propagate_replicate(components, True)


def test_replicate_subgraph():
    components = [
        {
            'name': 'add',
            'references': [
                'number:output'
            ],
            'workflowAttributes': {
                'replicate': '%(numberPoints)s'
            }
        },
        {
            'name': 'no-deps-rep',
            'workflowAttributes': {
                'replicate': 1,
            }
        },
        {
            'name': 'no-deps-aggregate',
            'workflowAttributes': {
                'aggregate': 'True'
            }
        },
        {
            'name': 'no-deps-plain',
        },
        {
            'name': 'propagated-replicate',
            'references': [
                'add:output'
            ]
        },
        {
            'name': 'propagated-replicate-absolute-ref',
            'references': [
                'stage0.add:ref'
            ]
        }
    ]

    variables = {
        FlowIR.LabelGlobal: {
            'numberPoints': "3"
        },
        FlowIR.LabelStages: {
            0: {
                'numberPoints': "1"
            }
        }
    }

    rep_components = FlowIR.apply_replicate(components, variables, True, [], [])

    rep_components = sorted(rep_components, key=lambda comp: comp['name'])

    assert rep_components == [
        {'name': 'add0',
         'references': ['number:output'],
         'variables': {'replica': 0},
         'workflowAttributes': {'replicate': 1}},
        {'name': 'no-deps-aggregate',
         'references': [],
         'workflowAttributes': {'aggregate': 'True'}},
        {'name': 'no-deps-plain'},
        {'name': 'no-deps-rep0',
         'variables': {'replica': 0},
         'workflowAttributes': {'replicate': 1}},
        {'name': 'propagated-replicate-absolute-ref0',
         'references': ['stage0.add0:ref'],
         'variables': {'replica': 0},
         'workflowAttributes': {'replicate': 1}},
        {'name': 'propagated-replicate0',
         'references': ['stage0.add0:output'],
         'variables': {'replica': 0},
         'workflowAttributes': {'replicate': 1}}
    ]

    # VV: Should fail because there're missing references (specifically there's no `number` component)
    with pytest.raises(experiment.model.errors.FlowIRReferenceToUnknownComponent) as e:
        _ = FlowIR.apply_replicate(components, variables, False, [], [])
    exc = e.value  # type: experiment.model.errors.FlowIRReferenceToUnknownComponent

    assert exc.references == ['stage0.number']

    # VV: Should not be able to resolve the variable `numberPoints`
    with pytest.raises(experiment.model.errors.FlowIRVariableUnknown):
        _ = FlowIR.apply_replicate(components, {}, True, [], [])

    components.append({
        'name': 'number'
    })

    rep_components = FlowIR.apply_replicate(components, variables, False, [], [])
    rep_components = sorted(rep_components, key=lambda comp: comp['name'])

    assert rep_components == [
        {'name': 'add0',
         'references': ['number:output'],
         'variables': {'replica': 0},
         'workflowAttributes': {'replicate': 1}},
        {'name': 'no-deps-aggregate',
         'references': [],
         'workflowAttributes': {'aggregate': 'True'}},
        {'name': 'no-deps-plain'},
        {'name': 'no-deps-rep0',
         'variables': {'replica': 0},
         'workflowAttributes': {'replicate': 1}},
        {'name': 'number'},
        {'name': 'propagated-replicate-absolute-ref0',
         'references': ['stage0.add0:ref'],
         'variables': {'replica': 0},
         'workflowAttributes': {'replicate': 1}},
        {'name': 'propagated-replicate0',
         'references': ['stage0.add0:output'],
         'variables': {'replica': 0},
         'workflowAttributes': {'replicate': 1}}
    ]


def test_parse_reference():
    cond_stage, cond_name, cond_file, cond_method = FlowIR.ParseDataReferenceFull('loop/does/not/exist:output', 0)
    assert cond_stage == 0
    assert cond_name == 'loop'
    assert cond_file == 'does/not/exist'
    assert cond_method == 'output'


def test_override_restart_hook_success(output_dir):
    flowir = """
        components:
        - name: restarted
          workflowAttributes:
            restartHookFile: overridden-restart.py
            restartHookOn:
            - Success
          command:
            executable: echo
            arguments: world
        """
    exp = experiment_from_flowir(flowir, output_dir)


def test_override_restart_hook_fail(output_dir):
    # VV: restartHookFile fields cannot include file separators
    flowir = """
        components:
        - name: restarted
          workflowAttributes:
            restartHookFile: no/folders/please.py
            restartHookOn:
            - Success
          command:
            executable: echo
            arguments: world
        """
    with pytest.raises(experiment.model.errors.ExperimentInvalidConfigurationError) as e:
        experiment_from_flowir(flowir, output_dir)
    exc = e.value   # type: experiment.model.errors.ExperimentInvalidConfigurationError
    assert len(exc.underlyingErrors()) == 1
    str_exc = str(exc.underlyingErrors()[0])

    assert 'no/folders/please.py' in str_exc
    assert 'workflowAttributes.restartHookFile' in str_exc


def test_syntax_error_key_unknown(output_dir):
    flowir = """
    components:
    - name: George
      command:
        executable: ls
      workflowAttributes:
        restart-hook-on:
          - Success
    """

    with pytest.raises(experiment.model.errors.ExperimentInvalidConfigurationError) as e:
        experiment_from_flowir(flowir, output_dir)
    exc = e.value  # type: experiment.model.errors.ExperimentInvalidConfigurationError

    exc = exc.underlyingErrors()[0].underlyingErrors[0]  # type: experiment.model.errors.FlowIRKeyUnknown
    assert exc.key_name == 'stage0.George.workflowAttributes.restart-hook-on'


def test_syntax_error_value_invalid_list(output_dir):
    flowir = """
    components:
    - name: George
      command:
        executable: ls
      workflowAttributes:
        restartHookOn: invalid
    """
    with pytest.raises(experiment.model.errors.ExperimentInvalidConfigurationError) as e:
        experiment_from_flowir(flowir, output_dir)
    exc = e.value  # type: experiment.model.errors.ExperimentInvalidConfigurationError

    exc = exc.underlyingErrors()[0].underlyingErrors[0]  # type: experiment.model.errors.FlowIRValueInvalid
    assert exc.key_name == 'stage0.George.workflowAttributes.restartHookOn'
    assert exc.value == 'invalid'


def test_syntax_error_invalid_constant(output_dir):
    flowir = """
    components:
    - name: George
      command:
        executable: ls
      workflowAttributes:
        restartHookOn:
        - Hello
    """
    with pytest.raises(experiment.model.errors.ExperimentInvalidConfigurationError) as e:
        experiment_from_flowir(flowir, output_dir)
    exc = e.value  # type: experiment.model.errors.ExperimentInvalidConfigurationError

    exc = exc.underlyingErrors()[0].underlyingErrors[0]  # type: experiment.model.errors.FlowIRValueInvalid
    assert exc.key_name == 'stage0.George.workflowAttributes.restartHookOn[0]'
    assert exc.value == 'Hello'


def test_expand_component_references(output_dir):
    flowir = """
        components:
        - name: hi
          command:
            executable: ls
        - name: to_component
          command:
            executable: echo
            arguments: hi:ref
          references:
          - hi:ref
          - /tmp:link
          - conf/flowir_package.yaml:copy
        - name: to_conf
          command:
            executable: echo
            arguments: conf:ref
          references:
          - conf:ref
    """
    wf = experiment_from_flowir(flowir, output_dir)
    conf = wf.configuration.configurationForNode('stage0.to_component')
    assert conf['references'] == ['stage0.hi:ref', '/tmp:link', 'conf/flowir_package.yaml:copy']

    conf = wf.configuration.configurationForNode('stage0.to_conf')
    assert conf['references'] == ['conf:ref']


def test_flowir_interface(output_dir):
    flowir = r"""
    interface:
      description: Computes the band-gap
      inputSpec:
        hasAdditionalData: false
        namingScheme: SMILES
        inputExtractionMethod:
          csvColumn:
            source:
              path: data/smiles.csv
            args:
              column: smiles
    
      propertiesSpec:
        - name: band-gap
          propertyExtractionMethod:
            csvDataFrame:
              source:
                keyOutput: BandGap
                            
    output:
      BandGap:
        data-in: stage0.super-nice-simulation/band-gap.csv:ref
      
    components:
    - name: super-nice-simulation
      command:
        executable: echo
        arguments: -e "SMILES; band-gap\nhi; 3.0">band-gap.csv
        expandArguments: none
    """

    _ = experiment_from_flowir(flowir, output_dir, extra_files={
        'data/smiles.csv': "content does not matter, the file just needs to be there"})


def test_flowir_interface_invalid_extractionMethod(output_dir):
    flowir = r"""
    interface:
      description: Computes the band-gap
      inputSpec:
        hasAdditionalData: false
        namingScheme: SMILES
        inputExtractionMethod:
          # VV: I made a mistake here, it should have been `csvColumn`
          csvcolumn:
            source:
              path: data/smiles.csv
            args:
                column: smiles
    
      propertiesSpec:
        - name: band-gap
          propertyExtractionMethod:
            csvDataFrame:
              # VV: source is optional, however the csvDataFrame *needs* source to be there, therefore
              # if it is missing it should still raise an exception.
              args:
                renameColumns:
                  old: new
                            
    output:
      BandGap:
        data-in: stage0.super-nice-simulation/band-gap.csv:ref
      
    components:
    - name: super-nice-simulation
      command:
        executable: echo
        arguments: -e "SMILES; band-gap\nhi; 3.0">band-gap.csv
        expandArguments: none
    """

    with pytest.raises(experiment.model.errors.ExperimentInvalidConfigurationError) as e:
        _ = experiment_from_flowir(flowir, output_dir)

    exc: experiment.model.errors.ExperimentInvalidConfigurationError = e.value
    assert len(exc.underlyingErrors()) == 1

    exc: experiment.model.errors.FlowIRConfigurationErrors = exc.underlyingErrors()[0]
    assert len(exc.underlyingErrors) == 2
    # VV: Awesome, FlowIR found the mistake and even provided me with some choices
    # actually there are 2 mistakes, an unknown key and a missing key

    print(f"Types: {[type(x) for x in exc.underlyingErrors]}")
    exc_unknown: experiment.model.errors.FlowIRKeyUnknown = [
        x for x in exc.underlyingErrors if isinstance(x, experiment.model.errors.FlowIRKeyUnknown)][0]
    # VV: Validation should identify that there's an issue with hooks and even figure out the correct word
    assert exc_unknown.key_name == "FlowIR.interface.inputSpec.inputExtractionMethod.csvcolumn"
    assert 'csvColumn' in exc_unknown.valid_choices
    assert exc_unknown.most_appropriate == 'csvColumn'

    exc_missing: experiment.model.errors.FlowIRKeyMissing = [
        x for x in exc.underlyingErrors if isinstance(x, experiment.model.errors.FlowIRKeyMissing)][0]
    # VV: Validation should identify that a key is missing
    assert exc_missing.key_name == "FlowIR.interface.propertiesSpec[0].propertyExtractionMethod.csvDataFrame.source"


def test_flowir_interface_invalid_propertySpec_source(output_dir):
    flowir = r"""
    interface:
      description: Computes the band-gap
      inputSpec:
        hasAdditionalData: false
        namingScheme: SMILES
        inputExtractionMethod:
          csvColumn:
            source:
              path: data/smiles.csv
            args:
                column: smiles
    
      propertiesSpec:
        - name: band-gap
          propertyExtractionMethod:
            csvDataFrame:
              source:
                # VV: Oh no, this is a typo the correct output name is BandGap
                keyOutput: BandPag
                            
    output:
      BandGap:
        data-in: stage0.super-nice-simulation/band-gap.csv:ref
      
    components:
    - name: super-nice-simulation
      command:
        executable: echo
        arguments: -e "SMILES; band-gap\nhi; 3.0">band-gap.csv
        expandArguments: none
    """

    with pytest.raises(experiment.model.errors.ExperimentInvalidConfigurationError) as e:
        _ = experiment_from_flowir(flowir, output_dir)

    exc: experiment.model.errors.ExperimentInvalidConfigurationError = e.value
    assert len(exc.underlyingErrors()) == 1

    exc: experiment.model.errors.FlowIRConfigurationErrors = exc.underlyingErrors()[0]
    assert len(exc.underlyingErrors) == 1

    # VV: Awesome, FlowIR found the mistake and even provided me with some choices
    exc: experiment.model.errors.FlowIRValueInvalid = exc.underlyingErrors[0]

    assert isinstance(exc, experiment.model.errors.FlowIRValueInvalid)

    # VV: Validation should identify that there's an issue with the output name and even figure out the correct word
    assert exc.key_name == "FlowIR.interface.propertiesSpec[0].propertyExtractionMethod" \
                           ".csvDataFrame.source.keyOutput"
    assert exc.value == "BandPag"
    assert 'BandGap' in exc.valid_choices
    assert exc.most_appropriate == 'BandGap'


def test_no_interface(output_dir):
    flowir = r"""
    output:
      BandGap:
        data-in: stage0.super-nice-simulation/band-gap.csv:ref

    components:
    - name: super-nice-simulation
      command:
        executable: echo
        arguments: -e "SMILES; band-gap\nhi; 3.0">band-gap.csv
        expandArguments: none
    """

    exp = experiment_from_flowir(flowir, output_dir)

    dict_interface = exp.configuration.get_interface()
    assert dict_interface is None
