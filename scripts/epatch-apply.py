#! /usr/bin/env python
# coding=UTF-8

# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis

'''Applies a patch to an experiment'''
from __future__ import print_function

import logging
import optparse
import os
import pprint
import re
import shutil
import signal
import sys
from distutils.dir_util import copy_tree

import networkx
import numpy
import yaml

import experiment.model.conf
import experiment.model.data
import experiment.model.errors
import experiment.model.frontends.dosini
import experiment.model.frontends.flowir
import experiment.model.graph
import experiment.model.storage
import six.moves
import glob

from typing import List, Dict, Set, Any, Tuple


def FixReferences(referenceList, commandLine, graph, stages_for_patched_components, comp_index):
    # type: (List[str], str, networkx.DiGraph, Dict[str, int], int) -> Tuple[List[str], str]
    """Fix references so they match graph and splicePoints

    Args:
        stages_for_patched_components: Dictionary whose keys are component references and value stage of the component
        referenceList: List of references for component
        commandLine: Commandline of component
        graph: networkx.DiGraph graph with nodes of the target workflow
        comp_index: Stage index of component
    """

    # The components in graph may have been moved to a different stage via their stageIndex key
    # However there reference have not been updated (note their names also have not been updated)

    print('\tFixing following references', referenceList)
    commandLine = str(commandLine or '')
    reference_map = {}
    updateRefs = []
    for r in referenceList:
        component, filename, method = experiment.model.conf.ParseDataReference(r)
        pIndex, producerName, hasIndex = experiment.model.conf.ParseProducerReference(component, comp_index)

        producer_ref = 'stage%d.%s' % (pIndex, producerName)
        actual_producer_index = stages_for_patched_components.get(producer_ref, pIndex)

        long_r = experiment.model.frontends.flowir.FlowIR.compile_reference(producerName, filename, method, pIndex)
        new_long_r = experiment.model.frontends.flowir.FlowIR.compile_reference(producerName, filename, method,
                                                                                               actual_producer_index)
        reference_map[long_r] = new_long_r
        updateRefs.append(new_long_r)

        if actual_producer_index == comp_index:
            short_r = experiment.model.frontends.flowir.FlowIR.compile_reference(producerName, filename, method)
            reference_map[short_r] = new_long_r

    comp_ids = []

    for node in graph:
        comp_ids.append((graph.nodes[node]['flowir']['stage'], graph.nodes[node]['flowir']['name']))

    current_reference_map = {}
    experiment.model.frontends.flowir.FlowIR.discover_reference_strings(commandLine,
                                                                                       implied_stage=None,
                                                                                       component_ids=comp_ids,
                                                                                       out_map=current_reference_map)

    # VV: Rewrite references starting from the longest one
    for match in sorted(current_reference_map, key=lambda x: len(x), reverse=True):
        if match in reference_map and match != reference_map[match]:
            print("\tReplacing reference %s with %s" % (match, reference_map[match]))

            # VV: use regular expression module to rewrite references to ensure that we're not rewriting parts of a
            # different reference!
            pattern = r'\b' + match + r'\b'
            pattern = pattern.replace('(', r'\(')
            pattern = pattern.replace(')', r'\)')
            pattern = pattern.replace('[', r'\[')
            pattern = pattern.replace(']', r'\]')
            try:
                commandLine = re.sub(pattern, reference_map[match], commandLine, 1)
            except Exception:
                print("Failed to re.sub(\"%s\", \"%s\", \"%s\" - ignore" % (
                    pattern, reference_map[match], commandLine))

    return updateRefs, commandLine


def GetOffsets(path, graph):

    '''Returns the stage offsets from the source node for each node in path'''

    indexes = numpy.asarray([graph.nodes[n]['stageIndex'] for n in path])
    print('Stage indexes of path components:', indexes)

    return indexes[0], indexes-indexes[0]

def GraphFromComponentConfigurations(components):

    '''Returns a networkx.Graph object from the given conf

    Parameters:
        components (dict): A dictionary specifying components in YAML format'''

    g = networkx.DiGraph()
    edges = []
    inputSplicePoints = []
    for componentName in components:
        componentData = components[componentName]
        replicate = componentData['workflowAttributes'].get(
            'replicate', None
        )

        isReplicationPoint = replicate is not None
        isAggregate = componentData['workflowAttributes'].get(
            'aggregate', False
        )

        #FIXME: Names from YAML contain stage part
        #print 'Component is:', componentName
        stageIndex, basename, hasIndex = experiment.model.conf.ParseProducerReference(componentName)


        #Can't use graph.CreateNode as it uses WorkflowGraph which assumes StageConfiguration objects
        g.add_node(componentName,
                   flowir=componentData.copy(),
                   stageIndex=stageIndex,
                   isReplicationPoint=isReplicationPoint,
                   isBlueprint=isReplicationPoint,
                   isAggregate=isAggregate,
                   level=stageIndex,
                   rank=stageIndex)

        producers = []
        for r in componentData["references"]:
            producer, filename, method = experiment.model.conf.ParseDataReference(r)
            producers.append(producer)

        edges.append((componentName, producers))

    #FIXME: This will not include the spice point/edge
    for node, producers in edges:
        #print 'Examining Edges from %s to %s' % (producers, node)
        stageIndex, componentName, hasIndex = experiment.model.conf.ParseProducerReference(node)
        for producer in producers:
            #print 'Checking producer', producer
            if g.has_node(producer):
                pIndex, componentName, hasIndex = experiment.model.conf.ParseProducerReference(producer)
                #print 'Adding edge from %s to %s' % (producer, node)
                g.add_edge(producer, node, length=(stageIndex - pIndex + 1))
            else:
                inputSplicePoints.append(producer)

    return g, list(set(inputSplicePoints))


def copy_over_producer_variables_and_apply_rewrite_rules(text, concrete, source_configuration):
    # VV: FIXME why prioritise source variables over target ones?

    source_variables = source_configuration['variables']

    for platform in source_variables:
        missing_variables = []
        ref_vars = experiment.model.frontends.flowir.FlowIR.discover_indirect_dependencies_to_variables(
            text=text, context=source_variables[platform], out_missing_variables=missing_variables
        )

        if missing_variables:
            print("\tProducer component will probably not work for platform %s" % platform)

        for var_name in ref_vars:
            name = var_name
            # VV: Ensure that the variable doesn't already exist
            have_conflict = False
            while have_conflict:
                have_conflict = False

                for name_platform in concrete.platforms:
                    target_global = concrete.get_platform_global_variables(name_platform)

                    while name in target_global:
                        name = '%sz' % name
                        have_conflict = True

            # VV: insert the new global variable
            print('\tUpdating global variables of platform %s, %s=%s' % (
                platform, name, source_variables[platform][var_name]
            ))
            concrete.set_platform_global_variable(name, source_variables[platform][var_name], platform)

            text = text.replace(var_name, name)

    return text


def hook_files_in_target(target_dir):
    # type: (str) -> List[str]
    """Discovers existing names of hook files inside the target directory"""
    return [os.path.split(x)[1] for x in glob.glob(os.path.join(target_dir, 'hooks', '*.py'))]


def detect_and_resolve_hook_conflicts(target_dir, source_conf):
    # type: (str, Dict[str, Any]) -> Dict[str, str]
    """Detects and resolves hook filename conflicts, modifies source_conf in place

    Returns: A dictionary mapping conflicting `workflowAttributes.restartHookFile` values to non-conflicting ones
    """
    resolved = {}
    existing_hook_files = hook_files_in_target(target_dir)

    for hook_file in set(source_conf['hooks']).intersection(existing_hook_files):
        # VV: Just generate a new filename that doesn't conflict with the one under the hooks directory of target_dir
        hfile_name, _ = os.path.splitext(hook_file)
        new_file_name = hook_file
        uid = 0

        while os.path.exists(os.path.join(target_dir, 'hooks', new_file_name)):
            new_file_name = '%s_%d.py' % (hfile_name, uid)
            uid += 1
        resolved[hook_file] = new_file_name

    for comp_ref in source_conf.get('components', {}):
        comp = source_conf['components'][comp_ref]
        workflowAttributes = comp.get('workflowAttributes', {})
        hook_file = workflowAttributes.get('restartHookFile')
        if hook_file is None:
            hook_file = 'restart.py'
        if hook_file in resolved:
            print("  Patching workflowAttributes.restartHookFile of %s - was %s, is now %s" % (
                comp_ref, hook_file, resolved[hook_file]))
            workflowAttributes['restartHookFile'] = resolved[hook_file]
            comp['workflowAttributes'] = workflowAttributes

    return resolved


if __name__ == "__main__":

    toplevel = experiment.model.storage.FindExperimentTopLevel()

    usage = "usage: %prog [options] [patch] [experiment/instance] "

    parser = optparse.OptionParser(usage=usage, version="% 0.1", description=__doc__)

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
    parser.add_option("-p", "--platform", dest="platform",
                      help="This platform will be used for testing. If not given the first found in the target will be used.",
                      default=None,
                      metavar="PLATFORM")
    parser.add_option("-l", "--logLevel", dest="logLevel",
                      help="The level of logging. Default %default",
                      type="int",
                      default=40,
                      metavar="LOGGING")

    parser.add_option('--live',
                      help="Set to indicate that this is a live-patch (default: False)",
                      action="store_true",
                      default=False,)
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

    if len(args) != 2:
        print("Must specify a patch and an experiment/instance to apply it to ", file=sys.stderr)
        sys.exit(1)

    patch, experimentDir = args
    experimentDirPath = os.path.abspath(experimentDir)

    if experiment.model.storage.IsExperimentInstanceDirectory(experimentDir):
        experimentDir = experiment.model.storage.ExperimentInstanceDirectory(experimentDirPath,
                                                                             attempt_shadowdir_repair=options.repairShadowDir)
        workflowGraph = experiment.model.graph.WorkflowGraph.graphFromExperimentInstanceDirectory(
            experimentDir,
            updateInstanceConfiguration=False,
            createInstanceConfiguration=False,
            primitive=True,
            is_instance=True,
        )
    else:
        experimentDir = experiment.model.storage.ExperimentPackage.packageFromLocation(
            experimentDirPath, options.manifest, options.platform, primitive=True)

        workflowGraph = experiment.model.graph.WorkflowGraph.graphFromPackage(
            experimentDir,
            platform=options.platform,
            updateInstanceConfiguration=False,
            createInstanceConfiguration=False,
            primitive=True,
            is_instance=False,
        )

    if options.live:
        with open(os.path.join(experimentDirPath, 'elaunch.yaml')) as f:
            elaunch_metadata = yaml.safe_load(f)

    #Read patch and convert to graph
    with open(os.path.join(patch, 'conf.yaml')) as f:
        conf = yaml.safe_load(f)

    print('\nSTEP 0: IDENTIFY AND RESOLVE HOOK FILE CONFLICTS')
    resolved_hook = detect_and_resolve_hook_conflicts(experimentDirPath, conf)

    if resolved_hook:
        print("Resolved conflicts: %s" % resolved_hook)

    print('\nSTEP 3: CONNECTING PATCH TO TARGET')

    stages_for_patched_components = {}

    print('STEP 1: CREATING PATCH GRAPH')
    g, inputs = GraphFromComponentConfigurations(conf['components'])
    #print 'Graph contents'
    #for n,d in g.nodes(data=True):
    #    print n, d

    print('Checking patch input data')
    allInputs = conf['data']['external']+conf['data']['internal']
    print('Patch input data is:', allInputs)
    allInputDirs = [os.path.split(d)[0] for d in allInputs]

    print('Graph expects following inputs (components AND data directories) ', inputs)

    inputs = [i for i in inputs if i not in allInputDirs]

    print('Graph expects following input components ', inputs)

    print('\nSTEP 2: SPLICE POINT IDENTIFICATION')

    print('NOTE: Using %s as initial platform' % options.platform)
    #FIXME: The initial checking of options checks "job-type".
    #This requires the platform as it may be a variable. Hence we can't create the graph without some value for it

    if options.live:
        options.platform = elaunch_metadata['platform'] or experiment.model.frontends.flowir.FlowIR.LabelDefault

    concrete = workflowGraph.configuration.get_flowir_concrete(return_copy=False)
    concrete.configure_platform(experiment.model.frontends.flowir.FlowIR.LabelDefault)

    platforms = [env_platform for env_platform in concrete.platforms if env_platform not in ['bgas', 'instance']]

    #USE BoW SIMILARITY METRICS
    #If we use advanced similarity metrics more data on the input splice point needs to be in the patch
    #Also we need to be able to create similarity objects from various sets of component data e.g. just name, name+exe etc.
    #And to be able to set what to compare with
    #For example say we just have the name - then we need the SimilarityObject created from the graph to also use just the name.
    #
    #1. Have a SPACE which defines its members and how to measure the distance between them
    #2. The SPACE should be able to map component information to a member
    #         (this is actually how it defines its members, the members of the SPACE are the outputs of transform(comp)
    #3. We can apply a SPACE on a Graph so every node of the graph has its associated member in the SPACE
    #4. We can identify if two members are from same SPACE and can compare them without have the SPACE

    print('Searching for splice points in target matching expected patch input components')
    #Check for splice points
    primitiveGraph = workflowGraph.graph
    nodes = primitiveGraph.nodes()
    matches = {}
    for e in inputs:
        stage, inputBasename = e.split('.')
        print('\tSearching for match to %s. Basename %s' % (e, inputBasename))
        for n in nodes:
            stage, targetBasename = n.split('.')
            if inputBasename == targetBasename:
                print('\t Found potential match to %s in target: %s' % (e, n))
                matches[e] = [stage,targetBasename]

    #FIXME: Just taking top match for now
    #If more than one should provide option

    component_ids = []

    for node, nodeData in workflowGraph.graph.nodes(data=True):
        stage, component_name, _ = experiment.model.frontends.flowir.FlowIR.ParseProducerReference(
            node, nodeData['stageIndex']
        )
        component_ids.append((stage, component_name))

    #Check if the matches command line/executable matches
    print("\nChecking parameters of candidate splice-points")
    splicePointActions = {}

    producer_changes = {}

    for sourceProducer in list(matches.keys()):
        source_configuration = conf['producers'][sourceProducer]
        targetProducer = "%s.%s" % tuple(matches[sourceProducer])
        print('Comparing parameters of patch input %s to potential match %s in target' % (sourceProducer, targetProducer))
        target_configuration = workflowGraph.graph.nodes[targetProducer]['getConfiguration'](True)

        target_args = target_configuration['command'].get('arguments', '')
        source_args = source_configuration['command'].get('arguments', '')

        is_repeating = target_configuration['workflowAttributes'].get('isRepeat', False)

        if is_repeating not in [False, None]:
            is_repeating = True
        else:
            is_repeating = False

        if source_args != target_args:
            print('Command lines do not match')
            print('Source Command Line (o):', source_args)
            print('Target Command Line (k):', target_args)
            choice = six.moves.input("Keep current (k), override (o) or user-defined (u) ? > ")
            if choice not in ['k','o', 'u']:
                choice = six.moves.input("Please type 'k' for keep, 'o' for override or 'u' for user-defined > ")
                if choice not in ['k', 'o']:
                    print('Unable to understand choice - stopping')
                    print(sys.exit(1))

            if choice == "u":
                arguments = six.moves.input("Type desired command line > ")
                action = {'action':'override', 'data':arguments, 'parameter': '#command.arguments'}
            elif choice == "k":
                action = {'action':'keep', 'data': target_args, 'parameter': '#command.arguments'}
            else:
                action = {'action':'override', 'data': source_args, 'parameter': '#command.arguments'}

            splicePointActions[targetProducer] = [action]

            chosen_args = copy_over_producer_variables_and_apply_rewrite_rules(
                action['data'], concrete, source_configuration
            )

            new_references = experiment.model.frontends.flowir.FlowIR.discover_reference_strings(
                chosen_args, target_configuration['stage'], component_ids
            )

            old_references = []
            for ref in target_configuration['references']:
                old_references.extend(experiment.model.frontends.flowir.FlowIR.discover_reference_strings(
                    ref, target_configuration['stage'], component_ids
                ))

            print('New references:', new_references)
            print('Old references:', target_configuration['references'])

            if sorted(new_references) != sorted(old_references):
                print("References do not match")
                new_references.extend(old_references)
                new_references = sorted(set(new_references))
                print("Update references to: %s" % new_references)

                splicePointActions[targetProducer].append(
                    {'action': 'override', 'data': new_references, 'parameter': '#references'}
                )

            if is_repeating is False and chosen_args != target_args:
                producer_changes[targetProducer] = {
                    'arguments': {
                        'before': target_args,
                        'after': chosen_args
                    }
                }

        target_exe = target_configuration['command'].get('executable', '')
        source_exe = source_configuration['command'].get('executable', '')

        if target_exe != source_exe:
            print('Executables do not match')
            print('Source Executable (o):', source_exe)
            print('Target Executable (k):', target_exe)
            choice = six.moves.input("Keep current (k), override (o) or user-defined (u) ? > ")
            if choice not in ['k', 'o', 'u']:
                choice = six.moves.input("Please type 'k' for keep, 'o' for override or 'u' for user-defined > ")
                if choice not in ['k', 'o']:
                    print('Unable to understand choice - stopping')
                    print(sys.exit(1))

            if choice == "u":
                executable = six.moves.input("Type desired executable > ")
                action = {'action': 'override', 'data': executable, 'parameter':'#command.executable'}
            elif choice == "k":
                action = {'action': 'keep', 'data': target_exe, 'parameter': '#command.executable'}
            else:
                action = {'action': 'override', 'data': source_exe, 'parameter': '#command.executable'}

            if targetProducer not in splicePointActions:
                splicePointActions[targetProducer] = []

            executable = action['data']

            pattern = re.compile(experiment.model.frontends.flowir.FlowIR.VariablePattern)

            source_variables = source_configuration['variables']

            comp_id = (target_configuration['stage'], target_configuration['name'])

            new_executable = copy_over_producer_variables_and_apply_rewrite_rules(
                executable, concrete, source_configuration
            )

            if new_executable != executable:
                print('\tUpdated executable to access new variables: %s' % new_executable)

            action['data'] = new_executable
            splicePointActions[targetProducer].append(action)

            if is_repeating is False and new_executable != target_exe:
                if targetProducer not in producer_changes:
                    producer_changes[targetProducer] = {}

                producer_changes[targetProducer]['executable'] = {
                        'before': target_exe,
                        'after': new_executable
                    }

    #Apply the patch to the graph
    #Set vars and environments
    #FIXME: All changes to the graph take place through the files on disk
    #i.e. the graph is read from disk each time its created via the Configuration objects
    #The only way ammend the graph is to ammend the Configuration objects, write them back, and read the graph again

    #There is limited API to do this in code
    #   - Cant ammend the variables live (already resolved)?
    #   - Can't ammend the evironments easily

    #When determining stages there are two constraints
    #1. Each input component must have correct relationship to its target
    #2. Each internal component must have correct relationship to each other
    #These are complicated by the fact that the target components may not have same relationship to each
    #other as in original graph

    #Simplet way seems to be a linear propogation of constraints
    #Take each splice point.
    #   Calculate new stage of input component and modify if necessary
    #   propogate down the patch
    #Since each input must lead through all points in patch all points will be modified
    #Since each modification is only (nothing,1+) then once the stages are modified to satsify an input
    #further modification will not break it


    #Stage Offset
    #For each splice point
    #-Get the destination stage
    #-Get the original offset (from references)

    newg = g.copy()
    for path in experiment.model.graph.AllLeafPaths(g):
        print('Processing patch path:', path)
        source = path[0]
        print('Source component in path is:', source)
        #Get stage offsets from source stage for each component in the path
        #NOTE: Need to compute this for all paths before we start modifying stage Indexes
        sourcePatchStage, baseOffsets = GetOffsets(path, g)
        print('Source component stage index (patch) is:', sourcePatchStage)
        #Now get offset the producers must have from the source
        print('Connecting inputs of %s' % source)
        for ref in g.nodes[source]['flowir']['references']:
            producer, filename, method = experiment.model.conf.ParseDataReference(ref)
            print('\t%s consumes from patch input %s' % (source, producer))
            pIndex, componentName, hasIndex = experiment.model.conf.ParseProducerReference(producer)
            if hasIndex:
                print('\tPatch input stage index is:', pIndex)
                #If the offset is greater > 1 its set to 1
                #If it needs to be greater > 1 in order to incorporate another component
                #It will be shifted later
                producerOffset = 0 if sourcePatchStage-pIndex == 0 else 1
                print('\tThis input should be offset at least %d stages from source component:' % producerOffset)

                connectionStage, connectionComponent = matches[producer]
                print('\tThe match to this patch input in target is', connectionStage, connectionComponent)
                connectionStage = int(connectionStage[5:])
                print('\tThis path must connect to stage %d in target and be offset %d stages' % (connectionStage, producerOffset))
                newStages = [o+producerOffset+connectionStage for o in baseOffsets]
                print('\tThe target stage indexes for all components in this path are:', newStages)
                for newStage, component_full_name in zip(newStages, path):
                    print('\t\tComponent %s in patch stage %d will move to stage %d' % (
                        component_full_name, g.nodes[component_full_name]['stageIndex'], newStage
                    ))
                    g.nodes[component_full_name]['stageIndex'] = newStage
                    stages_for_patched_components[component_full_name] = int(newStage)
            else:
                print('Skipping data ref')

            #NOTE: References are fixed below when written
            #NOTE 2: Names of components in new graph don't match stages

    print('\nSTEP 4: PATCHING TARGET')

    g = newg

    #primitiveGraph = networkx.algorithms.operators.binary.compose(primitiveGraph, g)
    #TODO: Need to add patch stages to the stage configurations

    #FIXME: Due to immaturity of the API we  currently use a workaround to patch/test the experiment
    #Below code includes the skeleton how it should be done (patch the graph in code, test, write-out)
    #and also the workaround (create copy of experiment, write the patched files to it, read, test, keep if works)

    ###### WORKAROUND START ######

    def ignore(dirname, contents):

        basename = os.path.split(dirname)[1]
        if 'deploy' in basename:
            return contents

        return []

    if options.live is False:
        #Create copy of the storage for workaround
        patchedExperimentDir = experimentDirPath+'.patched'
        if isinstance(experimentDir, experiment.model.storage.ExperimentPackage):
            # VV: TODO Do we want to rewrite the manifest so that we *always* copy application-dependencies?
            target_package: experiment.model.storage.ExperimentPackage = experimentDir
            target_package.expandPackageToDirectory(patchedExperimentDir, experimentDir.configuration.file_format)

            patched_package = experiment.model.storage.ExperimentPackage.packageFromLocation(
                patchedExperimentDir, platform=target_package.configuration.platform_name,
                primitive=True)
        else:
            try:
                shutil.copytree(experimentDirPath, patchedExperimentDir, True, ignore)
            except Exception as data:
                print("Error creating copy of directory for patching", file=sys.stderr)
                raise
            patched_package = experiment.model.storage.ExperimentPackage.packageFromLocation(patchedExperimentDir,
                                                                                             primitive=True)
    else:
        # VV: This is a live-patch, copy everything into a subdirectory of the instance
        patchedExperimentDir = os.path.join(experimentDirPath, '../flow.patch')

        if os.path.exists(patchedExperimentDir):
            shutil.rmtree(patchedExperimentDir)

        os.makedirs(patchedExperimentDir)

        root_path, folders, files = next(os.walk(experimentDirPath))

        # VV: Make sure that you skip stages and output
        skip_folders = ['stages', 'output', 'flow.patch', 'input', 'data']
        folders = [folder for folder in folders if folder not in skip_folders]

        symlinks = [path for path in folders if os.path.islink(os.path.join(experimentDirPath, path))]
        skip_folders += [os.path.split(path)[1] for path in symlinks]

        print("Skipping folders %s" % str(skip_folders))

        for folder in folders:
            if folder in skip_folders:
                continue
            print(("Copying folder %s" % folder))

            copy_tree(
                os.path.join(experimentDirPath, folder),
                os.path.join(patchedExperimentDir, folder),
                update=0
            )

        patched_package = experiment.model.storage.ExperimentPackage.packageFromLocation(
            patchedExperimentDir, primitive=True)

    # VV: experiment.storage.ExperimentPackage does not know where conf/bin/data are because there's a chance
    # that the folders are application dependencies and the workflow definition is a YAML file instead of a folder
    # however, patched_package *is* an ExperimentPackage that points to a directory so we can safely use:
    patched_confdir = os.path.join(patched_package.location, 'conf')
    patched_bindir = os.path.join(patched_package.location, 'bin')
    patched_datadir = os.path.join(patched_package.location, 'data')
    patched_hookdir = os.path.join(patched_package.location, 'hooks')

    print('\nSTEP 4.2: ADDING ENVIRONMENTS')

    environment_conflicts = {}
    #Add environments
    #print 'Adding environments for:', options.platform

    platforms_with_env_patch = sorted(set(platforms).intersection(set(conf['environments'])))

    for env_platform in platforms_with_env_patch:
        for envName in conf['environments'][env_platform]:
            print('Adding environment %s to platform %s' % (envName, env_platform))
            env = conf['environments'][env_platform][envName]
            had_conflict = False
            name = envName

            while True:
                try:
                    workflowGraph.configuration.add_environment(name, env, env_platform)
                    print('Added new environment %s with options %s to platform %s' % (name, env, env_platform))
                    break
                except experiment.model.errors.FlowIREnvironmentExists:
                    name = '%sz' % name
                    had_conflict = True

            # VV: The original environment name is already taken, just make a note here and back-patch
            #     the inserted components with the automatically generated environment name
            if had_conflict:
                environment_conflicts[envName] = name

    if environment_conflicts:
        print('WARNING: Environment name conflicts detected. Translation map: %s' % environment_conflicts)

    ###### WORKAROUND END ####

    #Add components
    print('\nSTEP 4.3: ADDING COMPONENTS')

    # VV: { from_source_stage_index: Set[of eventual stage index in the target] }
    source_to_target_stage_index = {}  # type: Dict[int, Set[int]]

    for component_full_name in networkx.topological_sort(g):
        print('Adding', component_full_name)
        pIndex, componentName, hasIndex = experiment.model.conf.ParseProducerReference(component_full_name)

        print('Fixing references')
        data = g.nodes[component_full_name]['flowir']
        data = data.copy()
        data['stage'] = int(stages_for_patched_components[component_full_name])
        stageIndex = data['stage']

        if pIndex not in source_to_target_stage_index:
            source_to_target_stage_index[pIndex] = set()

        source_to_target_stage_index[pIndex].add(stageIndex)

        orig_arguments = data['command'].get('arguments', '')
        orig_references = list(data['references'])
        references, arguments = FixReferences(
            orig_references, orig_arguments, g, stages_for_patched_components, data['stage'])

        if references != orig_references:
            print('Update references from', orig_arguments,'to', references)
        if arguments != orig_arguments:
            print('Updated command line from', orig_arguments, 'to', arguments)

        data['references'] = references
        data['command']['arguments'] = arguments

        comp_env_name = data['command'].get('environment')

        try:
            renamed_comp_env_name = environment_conflicts[comp_env_name]
            print('WARNING: Reconfiguring "%s" to use environment "%s" instead of "%s"' % (
                component_full_name, renamed_comp_env_name, comp_env_name
            ))
            data['command']['environment'] = renamed_comp_env_name
        except KeyError:
            pass

        comp_id = (stageIndex, componentName)

        experiment.model.frontends.flowir.FlowIR.convert_component_types(data)

        concrete.add_component(data)

    # VV: Now discover and fix variable conflicts
    def detect_conflicting_variables(target_vars, source_vars, label):
        print("Detecting conflicting in %s" % label)
        conflicts = [key for key in source_vars if target_vars.get(key, source_vars[key]) != source_vars[key]]
        return conflicts

    human_resolved_conflicts = {}

    def resolve_conflicts(conflict_keys, target_vars, source_vars, label, resolved_conflicts):
        conflicts = {
            key: {'target': target_vars[key], 'source': source_vars[key]} for key in conflict_keys
        }
        print("Global variable conflicts for platform %s" % platform)
        pprint.pprint(conflicts)

        for key in conflicts:
            print('Variable values for variable %s in %s do not match' % (key, label))
            print('Value in source (o):', conflicts[key]['source'])
            print('Value in target (k):', conflicts[key]['target'])
            choices = ['k', 'o', 'u']
            prompt = "Keep current (k), override (o) or user-defined (u) ? > "
            if key in resolved_conflicts:
                conflict_label, conflict_value = resolved_conflicts[key]
                print('Value was resolved in %s (p): %s' % (conflict_label, conflict_value))
                choices.append('p')

            choice = six.moves.input(prompt)

            if choice not in choices:
                choice = six.moves.input("Please type one of the following %s. %s" % (choices, prompt))
                if choice not in choices:
                    print('Unable to understand choice - stopping')
                    print(sys.exit(1))
            if choice == 'u':
                value = six.moves.input("Type desired value > ")
            elif choice == 'k':
                value = conflicts[key]['target']
            elif choice == 'o':
                value = conflicts[key]['source']
            elif choice == 'p':
                _, value = resolved_conflicts[key]

            source_vars[key] = value
            resolved_conflicts[key] = (label, value)

    for platform in platforms:
        # VV: Traverse platforms and make sure that variables which exist in both the source and target have
        #     the exact same value, when this requirement is not met ask the user to resolve the conflict
        target_vars = concrete.get_platform_global_variables(platform)

        source_vars = experiment.model.frontends.flowir.deep_copy(
            conf['variables'].get(platform, {}).get(experiment.model.frontends.flowir.FlowIR.LabelGlobal, {})
        )
        label = 'platform %s' % platform
        conflicts = detect_conflicting_variables(target_vars, source_vars, label)
        if conflicts:
            resolve_conflicts(conflicts, target_vars, source_vars, label, human_resolved_conflicts)

        # VV: Update FlowIR with the resolved conflicts
        for key in source_vars:
            value = source_vars[key]
            concrete.set_platform_global_variable(key, value, platform)

        for source_index in source_to_target_stage_index:
            source_vars = experiment.model.frontends.flowir.deep_copy(
                conf['variables'].get(platform, {}).get(
                    experiment.model.frontends.flowir.FlowIR.LabelGlobal, {})
            )

            if not source_vars:
                continue

            for target_index in source_to_target_stage_index[source_index]:
                target_vars = concrete.get_platform_stage_variables(target_index, platform)

                label = 'platform %s stage %d' % (platform, target_index)
                conflicts = detect_conflicting_variables(target_vars, source_vars, label)

                if conflicts:
                    resolve_conflicts(conflicts, target_vars, source_vars, label, human_resolved_conflicts)

                # VV: Update FlowIR with the resolved conflicts
                for key in source_vars:
                    value = source_vars[key]
                    concrete.set_platform_stage_variable(target_index, key, value, platform)

    #### WORKAROUND START ####
    if len(splicePointActions) > 0:
        print('\nSTEP 4.4: UPDATING SPLICE-POINTS (Based on user-input)')
        for node in splicePointActions:
            print('Splice point %s' % node)
            stageIndex, componentName, hasIndex = experiment.model.conf.ParseProducerReference(node)
            comp_id = (stageIndex, componentName)
            for action in splicePointActions[node]:
                if action['action'] != 'keep':
                    print('Action is to override %s with %s' % (action['parameter'], action['data']))
                    concrete.set_component_option(
                        comp_id, action['parameter'], action['data']
                    )
                else:
                    print('Action is to keep existing value of %s' % action['parameter'])

    print('\nSTEP 4.5: ADDING EXECUTABLES')
    #FIXME: Using `bin` in exeperiment and `executable` in patch ...
    patchExecutableDir = os.path.join(patch, 'executable')
    for exePath in conf['executables']['internal']:
        basename = os.path.basename(exePath)
        print('Moving %s from patch to target' % basename)
        patchPath = os.path.join(patchExecutableDir, basename)
        targetPath = os.path.join(patched_bindir, basename)
        print('Copy %s from patch to target at %s' % (patchPath, targetPath))
        if os.path.exists(targetPath):
            print('WARNING. Patch executable already exists in target - keeping original')
        else:
            shutil.copy(patchPath, targetPath)

    print('\nSTEP 4.6: ADDING DATA')
    for dataPath in conf['data']['internal']:
        print(dataPath)
        dataDir = os.path.split(dataPath)[0]
        print(dataDir)
        patchPath = os.path.join(patch, dataPath)
        #FIXME: Assuming both called "data"
        targetPath = os.path.join(patched_package.location, dataPath)
        targetDir = os.path.join(patched_package.location, dataDir)
        print('Copy %s from patch to target at %s' % (patchPath, targetPath))
        if os.path.exists(targetPath):
            print('WARNING. Patch data file already exists in target - keeping original')
        else:
            if not os.path.exists(targetDir):
                print('WARNING: %s does not exist in target creating it now' % dataDir)
                os.makedirs(targetDir)

            shutil.copy(patchPath, targetPath)

    print('\nSTEP 4.7: ADDING HOOKS')
    if conf['hooks'] and os.path.exists(patched_hookdir) is False:
        os.makedirs(patched_hookdir)

    for hook in conf['hooks']:
        dest_hook_name = resolved_hook.get(hook, hook)
        patch_path = os.path.join(patch, 'hooks', hook)
        target_path = os.path.join(patched_hookdir, dest_hook_name)
        print("  Copy %s hook to %s" % (patch_path, target_path))
        shutil.copy(patch_path, target_path)

    raw_flowir = concrete.raw()
    if options.live is True:
        # VV: In a live-patch do not keep anything other than `flowir_package.yaml`
        shutil.rmtree(patched_confdir, ignore_errors=True)
        os.mkdir(patched_confdir)

    # VV: Always store live-patches in FlowIR format so that we don't have to translate it from DOSINI when
    #     we apply it
    pretty_flowir = experiment.model.frontends.flowir.FlowIR.pretty_flowir_sort(raw_flowir)
    with open(os.path.join(patched_confdir, 'flowir_package.yaml'), 'w') as f:
        experiment.model.frontends.flowir.yaml_dump(pretty_flowir, f, sort_keys=False, default_flow_style=False)

    #Recreate the graph
    primitiveGraph = workflowGraph.graph

    #Write
    print('\nSTEP 5: TESTING PATCHED EXPERIMENT')

    #TODO: Checks
    # Check syntax etc. (whats in validate)
    # Check replication
    #FIXME: Cant do checks at the moment without writing the files - hence above workarounds
    #The checking code is in Experiment and ComponentSpec and they can only be initialised from disk
    #Modifying the configuration directly has no impact
    #Also replication is done on read so can't check replication without writing

    if options.live is False:
        # VV: Temporarily disable checks for live-patches because currently the process fails
        #     when flow is attempting to create instance specific dirs such as input
        e = experiment.model.data.Experiment.experimentFromPackage(patched_package, platform=options.platform)
        e.validateExperiment(checkExecutables=False, excludeInputReferences=True)

        if e is not None:
            e.instanceDirectory.consolidate()
            rootLogger.warning("TEARING DOWN TEST DIRECTORIES")
            try:
                shutil.rmtree(e.instanceDirectory.location)
            except Exception as error:
                print('Error deleting test dir')
                print(error)

    if options.live:

        if producer_changes:
            print(("The patch requires that %d Producer components of the target patch are updated for the patched in "
                  "components to behave 100%% correct. However, elaunch.py will not modify any Producer components."
                  % len(producer_changes)
                  ))
            print("Suggested updates:")
            pprint.pprint(producer_changes)

            print("Do you wish to have elaunch.py (partially) apply the patch?")

            choice = six.moves.input("Yes (y/Y), No (anything else) ? > ")

            if choice not in ['Y', 'y']:
                print(("Will skip signaling elaunch.py (%d) with SIGUSR2. You can modify the patch and then trigger it "
                      "manually. Keep in mind that elaunch will *not* modify any Producer components." % (
                    elaunch_metadata['pid']
                )))
            exit(0)

        print("live-patch has finished, the resulting files are placed under %s" % patched_package.location)
        print("You may now trigger live patch by running 'kill -%d %d'" % (signal.SIGUSR2, elaunch_metadata['pid']))
        # os.kill(elaunch_metadata['pid'], signal.SIGUSR2)

    #For each added component
    #- Read the conf file it was added to
    #- Add it to the conf
    #Write all read files back at the end

    #Copy executables

    #Copy data

    #FIXME: We can't write back the raw files conf directly as the variables have been resolved ....
    #Solution is to re-read each file directly here to control variable substitution and then write back

    #Graph Requirements
    #Decouple graph modification operations from storage.
    #  i.e. currently the class that reads the storage applies the modifications (variables, replication) as it reads
    #Read graph directly from storage - graphFrom... (e.g. graphFromDosIni)
    #Modify the graph (add patch, add variables, switch platform, AND replicate ... currently happens in StageConfiguration) directly
    #Write back the graph to different storage formats
    #
    #wfg = graph.WorkflowGraph.graphFromIniConfiguration([storage method], patches=)
    #wfg.setPlatform(platform)
    #wfg.validate()  (group various validations)
    #wfg.addVariables(...)
    #wfg.addSubgraph(..)
    #wfg.write(instance=, type=)
    #replicatedGraph = wfg.replicate()
