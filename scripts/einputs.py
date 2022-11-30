#! /usr/bin/env python
# coding=UTF-8

# Copyright IBM Inc. 2021. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis


"""Describe required and optional inputs along with any default values for a workflow definition"""

from __future__ import print_function

import logging
import optparse
import os
from typing import List, Dict, Set

import yaml
import experiment.model.conf
import experiment.model.frontends.flowir

usage = "usage: %prog [options] package"

parser = optparse.OptionParser(usage=usage, version="% 0.1", description=__doc__)

parser.add_option("-l", "--logLevel", dest="logLevel", help="The level of logging. Default %default",
                  type="int", default=20, metavar="LOGGING")
parser.add_option('-o', '--output', dest='output', metavar='PATH_TO_OUTPUT_FILE', default=None,
                  help='(optional) Store stdout output to YAML')

parser.add_option("-p", "--platform", dest="platform", metavar="PLATFORM_NAME", default=None,
                  help="Platform name", )
# VV: Opted not to include a shorthand for `--manifest` because `elaunch.py` doesn't use `-m` for `--manifest`
parser.add_option("", "--manifest", dest="manifest", metavar="PATH_TO_MANIFEST_FILE", default=None,
                  help="Optional path to manifest YAML file to use when setting up package directory from a "
                       "FlowIR YAML file. The manifest should contain a dictionary, with "
                       "targetFolder: sourceFolder entries. Each sourceFolder will be copied or linked to "
                       "populate the respective targetFolder. Source folders can be absolute paths, or "
                       "paths relative to the path of the FlowIR YAML file. SourceFolders may also be "
                       "suffixed with :copy or :link to control whether targetFolder will be copied or "
                       "linked to sourceFolder (default is copy). TargetFolders are interpreted as relative "
                       "paths under the instance directory. They may also be the resulting folder name of "
                       "some applicationDependency. They may include a path-separator (.e.g /) but must not "
                       "be absolute paths.")
options, args = parser.parse_args()

if len(args) != 1:
    parser.error(f'Expected exactly 1 argument (path to workflow definition) but got {args}')

FORMAT = '%(levelname)-9s %(name)-30s: %(funcName)-20s %(asctime)-15s: %(message)s'
logging.basicConfig(format=FORMAT)
log = logging.getLogger()
log.setLevel(options.logLevel)


def main():

    if options.manifest:
        manifest = experiment.model.frontends.flowir.Manifest.fromFile(options.manifest).manifestData
    else:
        manifest = {}

    conf = experiment.model.conf.ExperimentConfigurationFactory.configurationForExperiment(
        args[0], platform=options.platform, manifest=manifest, createInstanceFiles=False, updateInstanceFiles=False)

    concrete = conf.get_flowir_concrete()

    # VV: Required includes references to input files as well as files under application-dependencies
    required = {}

    # VV: First build the expected link names for the application-dependencies
    app_dep_link_names = {x: experiment.model.frontends.flowir.FlowIR.application_dependency_to_name(x)
                          for x in conf.get_application_dependencies()}

    top_level_folders = conf.top_level_folders + [app_dep_link_names[x] for x in app_dep_link_names] + \
                        experiment.model.frontends.flowir.FlowIR.SpecialFolders

    def location_of_app_dep(app_dep: str) -> str:
        """Returns the expected location of an application dependency

        1. If the app_dep is an absolute path return the absolute path
        2. If a manifest is provided look for the app-dep in that
        3. Return the root-folder of the package suffixed with the name of the application dependency

        Arguments:
            app_dep: Name of application dependency
        """
        if os.path.isabs(app_dep):
            return app_dep

        if app_dep in manifest:
            return manifest[app_dep]

        return os.path.join(os.path.abspath(os.path.basename(conf.location)), app_dep)

    app_dep_files = {
        app_dep_link_names[x]: {
            'location': location_of_app_dep(x),
            'required-data': []
        } for x in app_dep_link_names
    }
    host_dependencies = []
    required['external-dependencies'] = app_dep_files
    required['host-dependencies'] = host_dependencies

    variable_refs_in_stage = {}  # type: Dict[int, Set[str]]

    data = []
    comp_ids = concrete.get_component_identifiers(recompute=False)
    for cid in comp_ids:
        comp = concrete.get_component_configuration(cid, include_default=True, raw=False, is_primitive=True)

        # VV: Discover variable references in component definitions, used in a future step
        # to filter out global/stage variables which components never reference
        comp_raw = concrete.get_component_configuration(cid, include_default=True, raw=True, is_primitive=True)

        var_refs = experiment.model.frontends.flowir.FlowIR.discover_references_to_variables(comp_raw)
        stage_index = cid[0]

        if stage_index not in variable_refs_in_stage:
            variable_refs_in_stage[stage_index] = set()

        if var_refs:
            variable_refs_in_stage[stage_index].update(var_refs)

        # VV: places with dependencies: references and command.executable,
        # Note: we don't need to check command.arguments because references covers it
        executable = comp['command']['executable'] or ''  # type: str
        strings_to_check = [executable]  # type: List[str]

        references = comp['references']
        for ref in references:
            stageIndex, jobName, filename, method = experiment.model.frontends.flowir.FlowIR.ParseDataReferenceFull(
                ref, special_folders=top_level_folders)
            if stageIndex is None:
                if filename:
                    new_path = os.path.join(jobName, filename)
                else:
                    new_path = jobName
                strings_to_check.append(new_path)

        for s in strings_to_check:
            folder, sep, rel_path = s.partition(os.path.sep)

            if os.path.isabs(s) and s not in host_dependencies:
                host_dependencies.append(s)
            elif sep and folder in app_dep_files:
                if rel_path not in app_dep_files[folder]['required-data']:
                    app_dep_files[folder]['required-data'].append(rel_path)
            elif sep and folder == 'data' and os.path.sep not in rel_path and rel_path not in data:
                # VV: Only top-level files in data can be overridden
                data.append(rel_path)

    optional = {}

    # VV: Filter out global/stage variables by are not used by any/stage components

    variables = concrete.get_workflow_variables()
    lbl_global = experiment.model.frontends.flowir.FlowIR.LabelGlobal
    lbl_stages = experiment.model.frontends.flowir.FlowIR.LabelStages
    global_variables = variables.get(lbl_global, {})
    stage_variables = variables.get(lbl_stages, {})

    all_var_refs = set()
    for stage_index in variable_refs_in_stage:
        all_var_refs.update(variable_refs_in_stage[stage_index])

    global_variables = {x: global_variables[x] for x in all_var_refs if x in global_variables}

    for stage_index in stage_variables:
        stage_variables[stage_index] = {x: stage_variables[stage_index][x] for x in stage_variables[stage_index]
                                        if x in variable_refs_in_stage.get(stage_index, {})}

    optional['variables'] = {lbl_global: global_variables, lbl_stages: stage_variables}

    optional['data'] = data

    ret = {
        'required': required,
        'optional': optional,
    }

    print(yaml.dump(ret))

    if options.output:
        with open(options.output, 'wt') as f:
            yaml.dump(ret, f, yaml.SafeDumper)


if __name__ == '__main__':
    main()
