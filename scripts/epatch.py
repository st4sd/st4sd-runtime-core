#!/usr/bin/env python
# coding=UTF-8

# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

'''Extracts a patch from a package or instance'''
from __future__ import print_function

import logging
import optparse
import os
import pprint
import shutil
import sys
from functools import reduce
from typing import Dict, Set

import yaml

import experiment.model.conf
import experiment.model.data
import experiment.model.errors
import experiment.model.frontends.flowir
import experiment.model.graph
import experiment.model.storage


def ParseComponentSelection(componentString, stage, workflow):

    g = workflow.graph

    if '...' in componentString:
        # This is an execution path
        # Assume references are full references
        x, y = componentString.split('...')
        print('Finding path between %s and %s' % (x, y), file=sys.stderr)
        path = [p for p in experiment.model.graph.ExecutionPath(x, y, g)]
        selectionType = 'path'
        if len(path) == 0:
            raise ValueError('No path from %s to %s' % (x, y))
    else:
        path = ["stage%d.%s" % (stage, componentString)]

        selectionType = 'component'

    return path, selectionType


def discover_internal_data(concrete, comp_id, platforms, known_component_ids, inputData):
    paths = []
    for plat in platforms:
        try:
            # VV: Fetch and resolve the component variables for the platform then resolve the reference
            comp_variables = concrete.get_component_variables(
                comp_id, platform=plat,
            )
            comp_variables = experiment.model.frontends.flowir.FlowIR.fill_in(
                comp_variables, comp_variables, label='stage%d.%s.variables' % (comp_id[0], comp_id[1]),
                is_primitive=True
            )
        except Exception as e:
            print(("Encountered exception while resolving inputData references for stage%d.%d: %s" % (
                comp_id[0], comp_id[1], e
            )))
        else:

            for r in inputData:
                try:
                    r = experiment.model.frontends.flowir.FlowIR.fill_in(
                        r, comp_variables, label='stage%d.%s.reference.%s' % (comp_id[0], comp_id[1], r)
                    )
                except:
                    pass
                else:
                    (
                        stage_index, ref, filename, method
                    ) = experiment.model.frontends.flowir.FlowIR.ParseDataReferenceFull(
                        r, comp_id[0]
                    )
                    comps_in_stage = known_component_ids.get(stage_index, [])
                    if ref not in comps_in_stage:
                        # VV: The producer is actually a file not a component
                        if filename is not None:
                            path_to_add = os.path.join(ref, filename)
                        else:
                            path_to_add = ref

                        if path_to_add.startswith(os.sep) is False:
                            # VV: Ensure that we're not try to copy a file from the `input` directory
                            file_tokens = path_to_add.split(os.sep)

                            if file_tokens[0] != 'input':
                                if path_to_add not in paths:
                                    paths.append(path_to_add)
                            # VV: This fills the terminal with debug stuff
                            # else:
                            #     print('Skipping input reference %s of component stage%d.%s for platform %s' % (
                            #         orig_ref, comp_id[0], comp_id[1], plat
                            #     ))

    return sorted(set(paths))

if __name__ == "__main__":

    toplevel = experiment.model.storage.FindExperimentTopLevel()

    usage = "usage: %prog [options] [component-name]"

    parser = optparse.OptionParser(usage=usage, version="% 0.1", description=__doc__)

    parser.add_option("-e", "--experiment", dest="experimentDir",
                      help="The experiment instance  or package directory. Default %default",
                      default=toplevel,
                      metavar="EXPERIMENT_DIR")
    parser.add_option('', '--manifest', dest="manifest", metavar="PATH_TO_MANIFEST_FILE", default=None,
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
    parser.add_option("-s", "--stage", dest="stage",
                      help="Temporary: The stage the component is in if only one given. Default %default",
                      type="int",
                      default=0,
                      metavar="STAGE")
    parser.add_option("-l", "--logLevel", dest="logLevel",
                      help="The level of logging. Default %default",
                      type="int",
                      default=40,
                      metavar="LOGGING")
    parser.add_option("-n", "--name", dest="name",
                      help="The name of the output patch dir/archive: Default: %default",
                      default="flow",
                      metavar="NAME")
    parser.add_option("-x", "--expand", dest="expand",
                      help="Selected paths are expanded to full analysis blocks Default: False",
                      action="store_true",
                      metavar="EXPAND")
    parser.add_option("-a", "--analyse", dest="analyse",
                      help="Analyse the provided experiment without extracting a patch. List blocks and their keywords",
                      action="store_true",
                      metavar="ANALYSE")
    parser.add_option("", "--platform", dest="platform",
                      help="The initial platform to use when reading the experiment. Default will be the first detected",
                      default=None,
                      metavar="PLATFORM")
    parser.add_option("", "--repairShadowDir", help="Attempt to repair shadow directory (default: False)",
                      action="store_true",
                      metavar="REPAIR_SHADOW_DIR",
                      default=False)
    # Set up base logger
    FORMAT = '%(levelname)-10s %(threadName)-25s: %(name)-25s %(funcName)-20s %(asctime)-15s: %(message)s'
    logging.basicConfig(format=FORMAT)
    rootLogger = logging.getLogger()

    options, args = parser.parse_args()

    rootLogger.setLevel(options.logLevel)

    if len(args) == 0 and not options.analyse:
        print("No component to extract specified", file=sys.stderr)
        sys.exit(1)
    elif not options.analyse:
        componentString = args[0]

    options.experimentDir = os.path.abspath(options.experimentDir)
    options.experimentDir = os.path.normpath(options.experimentDir)

    if experiment.model.storage.IsExperimentInstanceDirectory(options.experimentDir):
        options.experimentDir = os.path.abspath(options.experimentDir)
        package = experiment.model.storage.ExperimentInstanceDirectory(
            options.experimentDir, attempt_shadowdir_repair=options.repairShadowDir)

        workflowGraph = experiment.model.graph.WorkflowGraph.graphFromExperimentInstanceDirectory(
            package,
            updateInstanceConfiguration=False,
            createInstanceConfiguration=False,
            primitive=True,
            is_instance=True,
        )
        storage_resolver = package
    else:
        options.experimentDir = os.path.abspath(options.experimentDir)
        experimentDir = experiment.model.storage.ExperimentPackage.packageFromLocation(
            options.experimentDir, options.manifest, options.platform)

        workflowGraph = experiment.model.graph.WorkflowGraph.graphFromPackage(
            experimentDir,
            platform=options.platform,
            updateInstanceConfiguration=False,
            createInstanceConfiguration=False,
            primitive=True,
            is_instance=False,
        )
        storage_resolver = experimentDir

    concrete = workflowGraph.configuration.get_flowir_concrete(return_copy=False)
    concrete.configure_platform(experiment.model.frontends.flowir.FlowIR.LabelDefault)

    platforms = [env_platform for env_platform in concrete.platforms if env_platform not in ['bgas', 'instance']]
    platform = platforms[0] if options.platform is None else options.platform
    print('Using %s as initial platform' % platform)
    #FIXME: The initial checking of options checks "job-type".
    #This requires the platform as it may be a variable. Hence we can't create the graph without some value for it

    patch_platform = platform or experiment.model.frontends.flowir.FlowIR.LabelDefault

    #If enter this block the program ends before exiting it
    if options.analyse:
        print('\n=== Analysing Graph of Experiment ===')

        print('\n -----> Block Analysis')

        # Block analysis
        blocks = experiment.model.graph.SubgraphAnalysisBlockModel(workflowGraph.graph)
        blockMap = experiment.model.graph.BlocksToComponentsMap(blocks)
        print('Found %d blocks' % len(blockMap))
        for i in range(len(blockMap)):
            print(i, ", ".join(blockMap[i]))

        try:
            import explore.graph
        except ImportError:
            print('Unable to import explore package - cannot perform NLP analysis', file=sys.stderr)
            sys.exit(0)

        print('\n -----> NLP Analysis')

        print('Ingesting experiment')
        template = explore.graph.ExperimentTemplate(db=None, experimentPackageLocation=options.experimentDir)
        for n in template.nodes:
            temp = reduce(lambda x, y: x + y,
                          [p.tracker for p in n.phrases])

            #print n.name, [w.word for w in temp.allWords]

        print('Building block descriptors')
        blockBOWDict = {}
        blockPhrasesDict = {}
        for i in blockMap:
            blockPhrases = []
            for c in blockMap[i]:
                blockPhrases.extend(template.nodeWithName(c).phrases)

            blockBOW = reduce(lambda x, y: x + y, [p.tracker for p in blockPhrases])

            blockPhrasesDict[i] = blockPhrases
            blockBOWDict[i] = blockBOW

        globalBOW = reduce(lambda x, y: x + y, list(blockBOWDict.values()))

        print('Results')
        import numpy
        totalWeight = globalBOW.totalWeight
        for i in range(len(blockMap)):
            blockBow = blockBOWDict[i]
            print('\nBLOCK', i, ", ".join(blockMap[i]))
            pblock = blockBow.totalWeight/(1.0*totalWeight)
            probs = [blockBow.probabilityForWord(w)*pblock/globalBOW.probabilityForWord(w) for w in blockBow.allWords]
            cutoff = 2 if len(probs) >=3 else len(probs)
            x = numpy.sort(numpy.asarray(probs))[::-1][cutoff]
            print('KEYWORDS:', end=' ')
            keywords = [(w,p) for w,p in zip(blockBOWDict[i].allWords, probs) if p >=x]
            print(", ".join(["%s (%s)" % (w[0].word, w[1]) for w in keywords]))
            keywords = [w[0] for w in keywords]

            print('KEYPHRASES')
            for p in blockPhrasesDict[i]:
                k = [w for w in p.tracker.allWords if w in keywords]
                if len(k) > 2:
                    print('\t', p.phrase)

        sys.exit(0)

    # Converts component string into a set of nodes
    nodes, selectionType = ParseComponentSelection(componentString, options.stage, workflowGraph)
    print('The components selected are:', ",".join(nodes))

    print('\n=== Analysing Graph of Selected Components ===')

    print('\n -----> Block Analysis')

    #Block analysis
    blocks = experiment.model.graph.SubgraphAnalysisBlockModel(workflowGraph.graph)
    #Blocks is a special networkx view object
    #If you iterate it using 'for x in y' you will get a list of tuples
    #However if you access it x[i] you will get the second element of the tuple whose first element is [i]
    blocks = blocks.nodes(data=True)
    blockMembership = [[x in block[1]['graph'].nodes for x in nodes] for block in blocks]
    blockCount = [reduce(lambda x,y: x+y, m) for m in blockMembership]

    print('The path includes components in %d workflow blocks' % len([el for el in blockCount if el != 0]))

    expansionNodes = []
    for i,c in enumerate(blockCount):
        missing = False
        if c != 0:
            print('%d members of the path are in workflow block %d' % (c, i))
            if reduce(lambda x,y:x+y, blockMembership[i]) != blocks[i]['nnodes']:
                print('WARNING: The path does not contain all components in this block')
                missing = True

            print('The components in this workflow block are:')
            print(blocks[i]['graph'].nodes)
            print('The members of this block which are in the selected path are:')
            for j,l in enumerate(blockMembership[i]):
                if l is True:
                    print(nodes[j])

            if missing and options.expand:
                print('Expanding path with missing components:', end=' ')
                mblocks = [m for m in blocks[i]['graph'].nodes if m not in nodes]
                print(",".join(mblocks))
                expansionNodes.extend(mblocks)
            elif missing:
                print('INFO: You can add the missing components using the -x option')

    nodes.extend(expansionNodes)

    print('\n -----> Splicing Analysis')

    #Check some things about the block/being extracted
    #1. How many input edges does it contain
    #1. Does it contain a leaf node - if not note it
    #2. Is it a complete analysis block - if not note and note whats missing
    inputEdges = set()
    rp = workflowGraph.graph.reverse()
    for n in nodes:
        inputs = [e[1] for e in rp.edges(n)]
        producers = set(inputs) - set(nodes)
        inputEdges.update([(e[1],e[0]) for e in rp.edges(n) if e[1] in producers])

    print('The path has %d input edges' % len(inputEdges))
    for e in inputEdges:
        print('%s -> %s' % e)

    producers = set([el[0] for el in inputEdges])
    print('The path consumes from %d producers: %s' % (len(producers), ",".join(producers)))

    splicePoints = set([el[1] for el in inputEdges])
    print('The path has %d splice points: %s' % (len(splicePoints),  ",".join(splicePoints)))


    print('\n====== Creating Patch =======\n')
    #Create output dir

    #Create configuration
    #Sections:
    #-components
    #   - A dictionary whose keys are component names and values are component configuration (another dict)
    #- producers
    #   - A dictionary whose keys are component names and values are reduced component configuration (executable+arguments)
    #-environment
    #   - platform (A dictionary whose keys are platforms and values are dictionary of (environment/dict) pairs
    #-data
    #    - A dictionary with two keys - internal and external
    #-virtual-environments:
    #    - platform (A dictionary whose keys are platforms and values are lists of virtual-environments which
    #                instruct flow to link certain folders in the top-level of the experiment instance)
    #-application-dependencies:
    #    - platform (A dictionary whose keys are platforms and values are lists of applications following the
    #                "<name>.application" format)

    patchConf = {
        'components': {},
        'producers': {},
        'environments': {},
        # 'outputs':{},
        'hooks': [],
        'executables': {'internal': [], 'external': [], 'sandbox': []},
        'data': {'internal': [], 'external': []},
        'virtual-environments': {
            # VV: platform : [ <path to virtual environment> ]
        },
        'application-dependencies': {
            # VV: platform : [ <application dependency> ]
        },
        'variables': {
            # VV: These are the global variables
            # VV: platform: global: {name: value}
            #               stages: {index: {name: value}}
        }
    }

    application_dependencies = {}
    virtual_environments = {}
    environments = []
    raw_flowir = concrete.raw()

    # VV: Record all known component ids
    known_component_ids = {}

    stage_ref_variables = {}  # type: Dict[int, Set[str]]
    all_ref_variables = set()  # type: Set[str]

    for node in nodes:
        nodeData = workflowGraph.graph.nodes[node]
        conf = nodeData['getConfiguration'](True)
        stage = int(conf['stage'])
        if stage not in known_component_ids:
            known_component_ids[stage] = []

        known_component_ids[stage].append(conf['name'])

    for platform in platforms:
        for node in nodes + sorted(producers):
            nodeData = workflowGraph.graph.nodes[node]
            stage, component_name, _ = experiment.model.frontends.flowir.FlowIR.ParseProducerReference(
                node, nodeData['stageIndex']
            )

            if stage not in stage_ref_variables:
               stage_ref_variables[stage] = set()

            conf = concrete.get_component_configuration(
                (stage, component_name), raw=True, platform=platform, is_primitive=True
            )
            comp_ref_vars = experiment.model.frontends.flowir.FlowIR.discover_references_to_variables(conf)
            stage_ref_variables[stage].update(comp_ref_vars)
            all_ref_variables.update(comp_ref_vars)

    patchConf['variables'] = {}

    for platform in platforms:
        platform_global_vars = concrete.get_platform_global_variables(platform)
        platform_global_vars = {
            key: platform_global_vars[key] for key in platform_global_vars if key in all_ref_variables
        }
        platform_stage_vars = {}

        for stage in stage_ref_variables:
            if not stage_ref_variables[stage]:
                continue
            stage_vars = concrete.get_platform_stage_variables(stage, platform)
            platform_stage_vars[stage] = {
                key: stage_vars[key] for key in stage_vars if key in stage_ref_variables[stage]
            }

        patchConf['variables'][platform] = {
            experiment.model.frontends.flowir.FlowIR.LabelGlobal: platform_global_vars,
            experiment.model.frontends.flowir.FlowIR.LabelStages: platform_stage_vars,
        }

    print('Referenced variables')
    pprint.pprint(stage_ref_variables)
    pprint.pprint(all_ref_variables)


    def construct_app_deps(platform):
        default_app_deps = concrete.get_application_dependencies(
            experiment.model.frontends.flowir.FlowIR.LabelDefault)
        app_deps = concrete.get_application_dependencies(platform)
        return sorted(set(default_app_deps + app_deps))

    def construct_venvs(platform):
        all_venvs = concrete.get_virtual_environments(
            experiment.model.frontends.flowir.FlowIR.LabelDefault)
        venvs_platform = concrete.get_virtual_environments(platform)
        return sorted(set(all_venvs + venvs_platform))

    app_deps = construct_app_deps(patch_platform)
    app_folders = [os.path.splitext(app.lower())[0] for app in app_deps]

    all_venvs = construct_venvs(patch_platform)
    venv_folders = [os.path.split(os.path.normpath(venv))[1] for venv in all_venvs]

    for platform in platforms:
        concrete.configure_platform(platform)
        for node in producers:
            nodeData = workflowGraph.graph.nodes[node]
            # Identify input data
            try:
                conf = nodeData['getConfiguration'](True)
                comp_id = (conf['stage'], conf['name'])
                inputData = nodeData['getInputReferences']()
                paths = discover_internal_data(concrete, comp_id, platforms, known_component_ids, inputData)
            except experiment.model.errors.FlowIRVariableUnknown:
                print("Cannot check for internal dependencies for producer %s when using platform %s" % (
                    node, platform
                ))
            else:
                patchConf['data']['internal'].extend(paths)

    patchConf['data']['external'] = sorted(set(patchConf['data']['internal']))
    patchConf['data']['internal'] = sorted(set(patchConf['data']['internal']))
    patchConf['executables']['external'] = sorted(set(patchConf['executables']['external']))
    patchConf['executables']['internal'] = sorted(set(patchConf['executables']['internal']))
    patchConf['executables']['sandbox'] = sorted(set(patchConf['executables']['sandbox']))

    concrete.configure_platform(patch_platform)

    # VV: FIXME This step still uses the patch_platform we should make it more generic
    for node in nodes:
        nodeData = workflowGraph.graph.nodes[node]

        #Get raw configuration for this component from config and add

        stage, component_name, _ = experiment.model.frontends.flowir.FlowIR.ParseProducerReference(
            node, nodeData['stageIndex']
        )
        comp_id = (stage, component_name)
        conf = concrete.get_component_configuration(comp_id, include_default=True, is_primitive=True)

        conf['variables'] = concrete.get_component(comp_id).get('variables', {})

        restart_hook_file = conf['workflowAttributes']['restartHookFile']

        if restart_hook_file is None:
            restart_hook_file = 'restart.py'

        if restart_hook_file:
            if os.path.isfile(os.path.join(options.experimentDir, 'hooks', restart_hook_file)):
                if restart_hook_file not in patchConf['hooks']:
                    patchConf['hooks'].append(restart_hook_file)
            else:
                print("Node %s references restartHookFile %s but the file does not exist - will not be included "
                      "in patch" % (node, restart_hook_file))

        patchConf['components'][node] = conf

        #Fully resolve references (i.e. in stage references that don't have a `stage` part) to make it easier
        patchConf['components'][node]['references'] = nodeData['getReferences']()+nodeData['getInputReferences']()

        #Identify input data
        inputData = nodeData['getInputReferences']()
        #Need to iterate over inputData and get paths

        comp_id = (conf['stage'], conf['name'])

        paths = discover_internal_data(concrete, comp_id, platforms, known_component_ids, inputData)

        patchConf['data']['internal'].extend(paths)

        # Identify executables

        exe = conf['command']['executable']
        if not os.path.isabs(exe) and os.path.split(exe)[0] != "":
            if exe[:3] == 'bin':
                patchConf['executables']['internal'].append(exe)
            elif exe[:5] == 'input':
                patchConf['executables']['external'].append(exe)
            else:

                patchConf['executables']['sandbox'].append(exe)
                # VV: Resolve executable path
                try:
                    resolved_conf = nodeData['componentSpecification']  # type: experiment.model.data.ComponentSpecification
                except:
                    print("WARNING: Could not discover the origins of the SANDBOX executable %s" % exe)
                    pass
                else:
                    real_exe = resolved_conf.commandDetails['executable']

                    norm_path = os.path.normpath(real_exe)
                    root_folder = norm_path.split(os.sep)[0]

                    if root_folder in app_folders:
                        # VV: Match the path to some application-dependency: add app-dep to patchConf
                        index = app_folders.index(root_folder)

                        if experiment.model.frontends.flowir.FlowIR.LabelDefault not in application_dependencies:
                            application_dependencies[
                                experiment.model.frontends.flowir.FlowIR.LabelDefault] = []

                        application_dependencies[experiment.model.frontends.flowir.FlowIR.LabelDefault].append(app_deps[index])
                    elif root_folder in venv_folders:
                        # VV: Match the path to some virtual-environment: add venv to patchConf
                        index = venv_folders.index(root_folder)

                        if patch_platform not in virtual_environments:
                            virtual_environments[patch_platform] = []

                        virtual_environments[patch_platform].append(all_venvs[index])
        else:
            patchConf['executables']['external'].append(os.path.split(exe)[1])

        if 'environment' in conf['command']:
            #The environment may be a variable - so we need to resolve it (passing False for raw parameter)
            #FIXME: This only resolves the value of the variable for the initially active environment
            #FIXME: The "named" environment for node may change on a per-platform basis
            #In both above cases mean the list of environments have to generated per platform
            conf = nodeData['getConfiguration'](False)
            e = conf['command']['environment']
            if e is not None and e not in environments:
                environments.append(e)

    #
    # Process environments
    #

    component_ids = []

    for node in list(workflowGraph.graph.nodes.keys()):
        nodeData = workflowGraph.graph.nodes[node]
        stage, component_name, _ = experiment.model.frontends.flowir.FlowIR.ParseProducerReference(
            node, nodeData['stageIndex']
        )
        component_ids.append((stage, component_name))

    all_platforms = ['default']
    for platform in platforms:
        print('Processing platform', platform)
        try:
            workflowGraph = workflowGraph.changePlatform(platform)
            # VV: Force FlowIR to be fully validated
            instance = workflowGraph.configuration.get_flowir_concrete(return_copy=True).instance(ignore_errors=True)
        except (experiment.model.errors.ExperimentInvalidConfigurationError,
                experiment.model.errors.ComponentSpecificationSyntaxError,
                experiment.model.errors.ComponentDuplicateDataReferenceError,
                experiment.model.errors.FlowIRVariableUnknown) as error:
            print('Error switching to platform %s - skipping' % platform)
            print('Error follows:')
            print(error)
            continue
        all_platforms.append(platform)
        patchConf['environments'][platform] = {}

        try:
            #First environment
            for env in environments:
                try:
                    e = workflowGraph.environmentWithName(env, expand=False)
                    patchConf['environments'][platform][env] = e
                except experiment.model.errors.FlowIREnvironmentUnknown as error:
                    print("Platform %s does not have a definition for environment %s - %s will not be extracted" % (
                        platform, env, env))
        except experiment.model.errors.ConstructedEnvironmentConfigurationError as error:
            patchConf['environments'].pop(platform)
            print('Error creating environments for platform %s - skipping' % platform)
            print('Error follows:')
            print(error)
            continue

    for node in producers:
        print('Processing producer', node)
        nodeData = workflowGraph.graph.nodes[node]

        # Get raw configuration for this component from config and add
        conf = nodeData['getConfiguration'](True)

        comp_id = (conf['stage'], conf['name'])

        default_variables = concrete.get_component_variables(
            comp_id, platform='default',
            include_platform_global=False,
            include_default_stage=False,
            include_platform_stage=False,
            include_default_global=False,
        )

        references = conf.get('references', [])

        absolute_references = []
        for ref in references:
            absolute_references.extend(experiment.model.frontends.flowir.FlowIR.discover_reference_strings(
                ref, conf['stage'], component_ids
            ))

        def filter_platform_variables(platform):
            if platform == 'default':
                return default_variables
            platform_variables = concrete.get_component_variables(comp_id, platform=platform)

            filtered = {
                key: platform_variables[key] \
                    for key in platform_variables if default_variables.get(key, None) != platform_variables[key]
            }

            return filtered

        all_variables = {
            platform: filter_platform_variables(platform) for platform in all_platforms
        }

        total_conf = {
            'command': conf['command'],
            'variables': all_variables,
            'references': absolute_references,
        }
        patchConf['producers'][node] = total_conf

    for platform in application_dependencies:
        application_dependencies[platform] = sorted(set(application_dependencies[platform]))

    # VV: Peek at the virtual environments that are used and collect all of their names
    used_venv_names = []
    for platform in virtual_environments:
        virtual_environments[platform] = sorted(set(virtual_environments[platform]))

        venv_names = [os.path.split(os.path.normpath(venv))[1] for venv in virtual_environments[platform]]
        used_venv_names += venv_names

    used_venv_names = sorted(set(used_venv_names))

    # VV: Match the names of the venvs to paths defined in the platform virtual-environments and include
    #     them in the patch
    for venv in used_venv_names:
        for platform in all_platforms:
            platform_venvs = virtual_environments.get(platform, [])

            venv_names = [os.path.split(os.path.normpath(venv))[1] for venv in platform_venvs]
            novel = set(used_venv_names) - set(venv_names)

            known_venvs = construct_venvs(platform)
            known_venvs = [full for full in known_venvs if os.path.split(os.path.normpath(full))[1] in novel]

            platform_venvs += known_venvs
            platform_venvs = sorted(set(known_venvs))
            if platform_venvs:
                virtual_environments[platform] = platform_venvs

    patchConf['application-dependencies'] = application_dependencies
    patchConf['virtual-environments'] = virtual_environments
    for key in patchConf['executables']:
        patchConf['executables'][key] = sorted(set(patchConf['executables'][key]))

    #FIXME: To be unified with ExperimentPackageDir creation
    patchDir = '%s.patch' % options.name
    try:
        os.mkdir(patchDir)
    except OSError as error:
        print('Cannot create patch dir %s - %s' % (patchDir, error), file=sys.stderr)
        print('Exiting', file=sys.stderr)
        sys.exit(1)

    os.chdir(patchDir)

    with open('conf.yaml', 'w') as f:
        yaml.dump(patchConf, f, default_flow_style=False)

    if len(patchConf['data']['internal']) > 0:
        os.mkdir('data')

    for f in patchConf['data']['internal']:
        experimentPath = storage_resolver.resolvePath(f)
        if os.path.isfile(f):
            print('Skipping internal file %s because it exists under %s' % (
                experimentPath, f
            ))
            continue
        else:
            print(('Copying internal file %s to %s' % (
                experimentPath, f
            )))

        containingDir = os.path.split(f)[0]
        if not os.path.exists(containingDir):
            os.makedirs(containingDir)

        shutil.copyfile(experimentPath, f)

    if len(patchConf['executables']['internal']) > 0:
        os.mkdir('executable')

    for f in patchConf['executables']['internal']:
        f = storage_resolver.resolvePath(f)
        shutil.copy(f, 'executable')
    if patchConf['hooks']:
        os.mkdir('hooks')
        for restart_hook_file in patchConf['hooks']:
            f = os.path.join(options.experimentDir, 'hooks', restart_hook_file)
            shutil.copy(f, 'hooks')

    # Note executables accessing other inbuilt executables are not identified

    #TODO:
    #Identify if any output data add to patch
    #Capture information on producer to help on applying the patch
    #- Need to identify the files being consumed from the producer - requires integration with explore.graph
