#! /usr/bin/env python
# coding=UTF-8

# Copyright IBM Inc. 2021. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis

"""Creates a workflow instance from an existing output dir and FlowIR definition"""

from __future__ import print_function

import glob
import logging
import optparse
import os
import pprint
import shutil
import sys
from typing import List

import experiment.model.data
import experiment.model.codes
import experiment.model.errors
import experiment.runtime.output
import experiment.model.frontends.flowir
import experiment.model.storage
import pkg_resources
import yaml

usage = "usage: %prog [options] [package]"

parser = optparse.OptionParser(usage=usage, version="% 0.1", description=__doc__)
parser.add_option("-l", "--logLevel", dest="logLevel", help="The level of logging. Default %default",
                  type="int", default=20, metavar="LOGGING")
parser.add_option("-f", "--flowir", dest="flowirPath", metavar="PATH_TO_YAML_FILE", default='',
                  help="Path to YAML file containing FlowIR. FlowIR must contain exactly 1 component", )
parser.add_option("-c", "--componentData", dest="componentDataPath", default='',
                  help="Path to folder containing input and output files of component",
                  metavar="PATH_TO_COMPONENT_DATA")
parser.add_option("-o", "--output", dest="outputPath", default="anonymous.instance",
                  help="Path to output instance (will ensure a .instance suffix if missing), default "
                       "is %default", metavar="PATH_TO_OUTPUT_INSTANCE")
parser.add_option("--overwrite", action="store_true", default=False,
                  help="If --output folder already exists this script raises an error. Enabling "
                       "--overwrite triggers this script to delete and overwrite the `--output` "
                       "folder should it already exist.")
options, args = parser.parse_args()

FORMAT = '%(levelname)-9s %(threadName)-30s %(name)-30s: %(funcName)-20s %(asctime)-15s: %(message)s'
logging.basicConfig(format=FORMAT)
log = logging.getLogger()
log.setLevel(options.logLevel)


def main():
    if options.flowirPath == '':
        parser.print_help()
        parser.error("--flowir is required")

    if options.componentDataPath == '':
        parser.print_help()
        parser.error('--componentData is required')

    if os.path.abspath(os.path.normpath(options.outputPath)) \
            == os.path.abspath(os.path.normpath(options.componentDataPath)):
        raise ValueError("--componentData and --output must be different")

    if os.path.isfile(options.flowirPath) is False:
        raise ValueError(f"There is no FlowIR file at {options.flowirPath}")

    if os.path.isdir(options.componentDataPath) is False:
        raise ValueError(f"There is no componentData directory at {options.componentDataPath}")

    try:
        with open(options.flowirPath, 'r') as f:
            contents = experiment.model.frontends.flowir.yaml_load(f)
    except Exception as e:
        log.critical(f"Invalid YAML file at {options.flowirPath}")
        raise

    # VV: Inflate the FlowIR with default values, and then validate the resulting FlowIR
    try:
        concrete = experiment.model.frontends.flowir.FlowIRConcrete(contents, None, {})
        flowir_errors = concrete.validate()
    except Exception:
        log.critical(f"Could not validate FlowIR stored in {options.flowirPath}")
        raise

    if flowir_errors:
        raise experiment.model.errors.ExperimentInvalidConfigurationError("Invalid FlowIR", flowir_errors)

    comp_ids = concrete.get_component_identifiers(True)
    if len(comp_ids) != 1:
        raise ValueError(f"Expected exactly one component definition but found {len(comp_ids)}")

    # VV: Next up detect references to input data
    comp_id = comp_ids.pop()

    used_data = {}

    comp_flowir = concrete.get_component(comp_id)
    app_deps = concrete.get_application_dependencies()
    for ref in comp_flowir.get('references', []):
        (stage_idx, full_path,
         _, method) = experiment.model.frontends.flowir.FlowIR.ParseDataReferenceFull(
            ref, comp_id[0], app_deps)

        if stage_idx is not None:
            raise ValueError(f"Component stage{comp_id[0]}.{comp_id[1]} references a component {ref} "
                             f"but it should only reference input/data files")

        if os.path.isabs(full_path):
            raise ValueError(f"Component stage{comp_id[0]}.{comp_id[1]} references a non input/data "
                             f"{ref} file")
        try:
            producer, str_glob = full_path.split(os.path.sep, 1)
        except ValueError:
            raise ValueError(f"Component stage{comp_id[0]}.{comp_id[1]} references the "
                             f"root path of {full_path} - reference a folder in {full_path} instead")

        if producer not in ['input', 'data']:
            raise ValueError(f"Component stage{comp_id[0]}.{comp_id[1]} references a non input/data "
                             f"{ref} file")

        if producer not in used_data:
            used_data[producer] = []

        used_data[producer].append({'reference': ref, 'method': method, 'glob': str_glob})

    if os.path.isdir(options.outputPath):
        if options.overwrite is False:
            raise ValueError(f"Output directory {options.outputPath} already exists and --overwrite "
                             f"is not set")

        log.warning(f"Deleting {options.outputPath} because it already exists and --overwrite is set")

        shutil.rmtree(options.outputPath)

    log.info(f"Will generate {options.outputPath}")
    os.makedirs(options.outputPath)

    log.info(f"Files used by component: {pprint.pformat(used_data)}")

    # VV: locate files using glob and copy them to their appropriate `prod_dir` (i.e. input, data)
    for prod_dir in used_data:
        full_prod_dir = os.path.join(options.outputPath, prod_dir)
        os.makedirs(full_prod_dir)

        for entry in used_data[prod_dir]:
            matches = glob.glob(os.path.join(options.componentDataPath, entry['glob']))  # type: List[str]
            if not matches:
                raise ValueError(f"Reference {entry['reference']} does not match any "
                                 f"files in {options.componentDataPath}")
            log.info(f"Reference {entry['reference']} matches {pprint.pformat(matches)}")

            # VV: for each match, create a folder in `prod_dir` IFF match is not in the root dir of componentDataPath
            for path in matches:
                _, rel_path = path.split(os.path.sep, 1)

                nested_dirs, match = os.path.split(rel_path)

                if nested_dirs:
                    target_dir = os.path.join(full_prod_dir, nested_dirs)
                    if os.path.isdir(target_dir) is False:
                        log.info(f"Creating nested dir {target_dir}")
                        os.makedirs(target_dir)

                target_path = os.path.join(full_prod_dir, rel_path)
                if os.path.isdir(path):
                    log.info(f"Copying folder {path} to {target_path}")
                    shutil.copytree(path, target_path)
                elif os.path.isfile(path):
                    log.info(f"Copying file {path} to {target_path}")
                    shutil.copyfile(path, target_path)
                else:
                    raise ValueError(f"Path {path} is neither a folder nor a file")

        comp_dir = os.path.join(options.outputPath, 'stages', f'stage{comp_id[0]}', comp_id[1])
        log.info(f"Populating component directory {comp_dir}")

        shutil.copytree(options.componentDataPath, comp_dir)

        log.info(f"Populating {options.outputPath}/conf")
        os.makedirs(os.path.join(options.outputPath, 'conf'))
        shutil.copyfile(options.flowirPath, os.path.join(options.outputPath, 'conf', 'flowir_package.yaml'))
        shutil.copyfile(options.flowirPath, os.path.join(options.outputPath, 'conf', 'flowir_instance.yaml'))


    if 'input' not in used_data:
        log.info(f"Generating empty {options.outputPath}/input directory")
        os.makedirs(os.path.join(options.outputPath, "input"))

    log.info(f"Generating {options.outputPath}/elaunch.yaml")

    elaunch_version = pkg_resources.get_distribution("st4sd-runtime-core").version
    actual_variables = concrete.get_workflow_variables()
    # VV: 'global' userMetadata variables end up becoming `platform-stage` variables (this is the second most
    # high priority scope right after variables defined by components). As a result, we need to backpatch them
    # into the `actual_variables[lbl_global]` dictionary
    lbl_global = experiment.model.frontends.flowir.FlowIR.LabelGlobal
    global_variables = actual_variables.get(lbl_global, {}).copy()
    actual_variables[lbl_global] = global_variables

    elaunch_state = experiment.model.storage.generate_elaunch_metadata_dictionary(
        platform=experiment.model.frontends.flowir.FlowIR.LabelDefault or experiment.model.frontends.flowir.FlowIR.LabelDefault,
        pid=os.getpid(), cmdline=' '.join(sys.argv), inputs=None, data=None, user_variables={},
        variables=actual_variables, version=elaunch_version, hybrid_platform=None,
        user_metadata={}, instance_name=os.path.split(options.outputPath)[1]
    )

    with open(os.path.join(options.outputPath, 'elaunch.yaml'), 'wt') as f:
        yaml.dump(elaunch_state, f, yaml.SafeDumper)

    log.info(f"Generating {options.outputPath}/output/status.txt")

    os.makedirs(os.path.join(options.outputPath, '../output'))
    statusFile = experiment.model.data.Status(os.path.join(options.outputPath, '../output', 'status.txt'),
                                              {'current-stage': 'stage0'}, ['stage0'])
    statusFile.setStageState(experiment.model.codes.FINISHED_STATE)
    statusFile.setExperimentState(experiment.model.codes.FINISHED_STATE)
    statusFile.setExitStatus('Success')
    statusFile.persistentUpdate()

    log.info(f"Generating {options.outputPath}/output/output.json")

    comp_exp = experiment.model.data.Experiment.experimentFromInstance(options.outputPath)
    outputAgent = experiment.runtime.output.OutputAgent(comp_exp)
    outputAgent.checkDataReferences()
    outputAgent.process_stage(0)


if __name__ == '__main__':
    main()
