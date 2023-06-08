#! /usr/bin/env python

# Copyright IBM Inc. 2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

'''Executes the command defined by a component-specification in a given workflow.

Note command output will go to a file cexecute.out in the components working dir.

The component-specification can be the name of a component
in which case the matching component in the given stage is executed.
The component-specification can be a glob in which case all matching components in the given stage are executed.
The component-specification can be of the form [start-component-reference]...[end-component-reference]
in which case all components in the path from start-component to end-component are executed.
For example: stage1.GenerateInputs...stage4.TernaryDiagram. Note: the value of the stage option is ignored.
'''

from __future__ import print_function

import logging
import optparse
import os
import sys
import threading

import experiment.settings
import experiment.appenv
import experiment.runtime.backends
import experiment.model.conf
import experiment.model.data
import experiment.model.storage


def ParseComponentSelection(componentString, stage, e):

    g = e.graph

    if '...' in componentString:
        #This is an execution path
        #Assume references are full references
        x, y = componentString.split('...')
        print('Finding path between %s and %s' % (x, y), file=sys.stderr)
        path = e.path(x, y)
        components = []
        for p in path:
            stageIndex, producerName, hasIndex = experiment.model.conf.ParseProducerReference(p)
            components.append(e.findJob(stageIndex, producerName))

        selectionType='path'
        if len(components) == 0:
            raise ValueError('No path from %s to %s' % (x, y))
    else:
        #Find the components matching the string
        if options.exactMatch is True:
            components = [e.findJob(stage, componentString)]
        else:
            #Finds multiple
            components = e.findJobs(stage, componentString)

        selectionType='component'

    return components, selectionType

def ExecuteComponent(specification, fullStack=False):

    '''Executes component defined by specification as in Flow

    - Builds full execution stack for backend
    - Instantiates and returns backed Task'''

    backend = specification.type if fullStack is True else 'local'

    try:
        task = experiment.runtime.backends.backendGeneratorMap[backend](specification, outputFile='cexecute.out')
        task.wait()
    except Exception as error:
        print("Caught exception while replicating command %s in %s" % \
                             (specification.name, specification.directory), file=sys.stderr)
        print(error, file=sys.stderr)


if __name__ == "__main__":


    toplevel = experiment.model.storage.FindExperimentTopLevel()

    usage = "usage: %prog [options] [component-name]"

    parser = optparse.OptionParser(usage=usage, version="% 0.1", description=__doc__)

    parser.add_option("-s", "--stage", dest="stage",
                      help="Stage the component is in. Default %default",
                      type="int",
                      default=0,
                      metavar="STAGE")
    parser.add_option("-e", "--experiment", dest="experimentDir",
                      help="The experiment instance directory. Default %default",
                      default=toplevel,
                      metavar="EXPERIMENT_DIR")
    parser.add_option("-d", "--componentDef", dest="componentDef",
                      help="Dynamically reads the component definitions (patch) from a file and adds them "
                           "to the experiment in the given stage",
                      default=None,
                      metavar="COMPONENT_DEFINITION")
    parser.add_option("-x", "--exactMatch", dest="exactMatch",
                      help="Only execute the component whose name exactly matches the given string",
                      action="store_true",
                      default=False,
                      metavar="EXACT")
    parser.add_option("-p", "--parallel", dest="parallel",
                      help="Run replicated jobs in parallel. "
                           "Each replicated job is run from a separate thread. Note: Use --batchSize to set how many "
                           "jobs are run simultaneously.",
                      action="store_true",
                      default=False,
                      metavar="PARALLEL")
    parser.add_option("-f", "--fullStack", dest='fullStack',
                      help="Execute the component using the backend and full-execution stack defined by the component. "
                           "If the component runs in hybrid mode you must also specify the relevant queue using the "
                           "--hybrid option. The platform specific details are obtained from the instance",
                      action="store_true",
                      default=False,
                      metavar="FULLSTACK")
    parser.add_option('', '--noKubernetesSecurityContext', default=False, action="store_true",
                      dest='no_k8s_security_context',
                      help="By default, flow will configure Pods that are generated by this workflow instance "
                           "to use a spec.securityContext (which among other things, sets a non-root user id, "
                           "non-root group id, and a fs-group identifier). This can be problematic for "
                           "containers which expect to be run as root. Using this parameter will instruct "
                           "flow to leave the securityContext of pods it generates empty. Doing so can be "
                           "considered a security risk and should be avoided.")
    parser.add_option("", "--dockerExecutableOverride", dest="dockerExecutableOverride",
                             help="Specify path to use instead of docker executable. If the executable is already in "
                                  "your $PATH you can just specify the name (e.g. podman). This is only "
                                  "used by Components which launch tasks using the docker backend", default=None)
    parser.add_option("-y", "--hybridQueue", dest='hybridQueue',
                      help="Specify the queue which runs jobs remotely when executing in FULLSTACK mode",
                      default=None,
                      metavar="HYBRID_QUEUE")
    parser.add_option("-l", "--logLevel", dest="logLevel",
                      help="The level of logging. Default %default",
                      type="int",
                      default=40,
                      metavar="LOGGING")
    parser.add_option("-a", "--platform", dest="platform",
                      help="Platform to execute for. Note if specified existing instance configuration is ignored",
                      default=None,
                      metavar="PLATFORM")
    parser.add_option("", "--batchSize", dest="batchSize",
                      help="This determines the number of simultaneous parallel jobs.",
                      type="int",
                      default=-1,
                      metavar="BATCHSIZE")
    parser.add_option("", "--writePatches", dest="storePatches",
                      action="store_true",
                      help="Write patches to disk",
                      default=False,
                      metavar="WRITE_PATCHES")
    parser.add_option("", "--repairShadowDir", help="Attempt to repair shadow directory (default: False)",
                      action="store_true",
                      metavar="REPAIR_SHADOW_DIR",
                      default=False)

    # VV: Load tne environment variables that define the size of worker pools and other Orchestrator settings
    # this may raise an exception explaining which environment-variable is invalid
    experiment.settings.load_settings_orchestrator()

    #Set umask to 0002 to allow group write
    os.umask(0o002)

    #Set up base logger
    FORMAT = '%(levelname)-10s %(threadName)-25s: %(name)-25s %(funcName)-20s %(asctime)-15s: %(message)s'
    logging.basicConfig(format=FORMAT)
    rootLogger = logging.getLogger() 

    options, args = parser.parse_args()

    rootLogger.setLevel(options.logLevel)

    if len(args) == 0:
        print("No component to replicate specified", file=sys.stderr)
        sys.exit(1)

    componentString =  args[0]

    configPatches = {}
    if options.componentDef is not None:
        print('Will apply dynamic component defitions in %s to experiment stage %d' % (
            options.componentDef, options.stage), file=sys.stderr)
        configPatches = {options.stage: [os.path.abspath(options.componentDef)]}

    #Initialise hybrid env if necessary
    if options.hybridQueue is not None:
        experiment.appenv.HybridConfiguration.newDefaultConfiguration(isHybrid=True,
                                                                      remoteQueue=options.hybridQueue)

    runDir = os.getcwd()
    options.experimentDir = os.path.abspath(options.experimentDir)
    instanceDir = experiment.model.storage.ExperimentInstanceDirectory(options.experimentDir,
                                                                       attempt_shadowdir_repair=options.repairShadowDir)

    # If a platform is specified with patches then by default any existing configuration will be overwritten
    if options.platform is not None:
        updateInstanceConfiguration = options.storePatches if options.componentDef is not None else False
        if updateInstanceConfiguration is True:
            rootLogger.warning('New platform and patch writing specified - existing configuration will be overwritten')
    else:
        updateInstanceConfiguration = options.storePatches

    e = experiment.model.data.Experiment(instanceDir,
                                         platform=options.platform,
                                         configPatches=configPatches,
                                         is_instance=True,
                                         updateInstanceConfiguration=updateInstanceConfiguration)
    compExperiment = e
    e.validateExperiment(checkExecutables=False)

    #Converts component string into a set of nodes
    components, selectionType = ParseComponentSelection(componentString, options.stage, e)

    #Initialise backends which require this, expect that elaunch has created a copy of
    #  '/etc/podinfo/flow-k8s-conf.yml' under $INSTANCE_DIR/conf/k8s-config.yaml
    k8s_config_path = os.path.join(e.instanceDirectory.configurationDirectory, 'k8s-config.yaml')
    experiment.runtime.backends.InitialiseBackendsForWorkflow(
        e.experimentGraph,
        # VV: Only for kubernetes backend
        k8s_config_path=k8s_config_path,
        k8s_no_sec_context=options.no_k8s_security_context,
        # VV: only for docker backend
        docker_executable=options.dockerExecutableOverride,
    )

    if len(components) is 0:
        rootLogger.critical("No components matching %s in stage %s" % (componentString, options.stage))
        sys.exit(1)

    for c in components:
        print(c.name)

    if not options.parallel or selectionType == 'path':
        options.batchSize = 1
    elif options.batchSize == -1:
        options.batchSize = len(components)

    #Execute Components
    threads = []
    count = 0
    for start in range(0,len(components),options.batchSize):
        rootLogger.info("Executing batch %d with %d jobs" % (count, options.batchSize))
        for component in components[start:start+options.batchSize]:
            t = threading.Thread(target=ExecuteComponent, args=(component, options.fullStack))
            t.start()
            threads.append(t)

        for t in threads:
            rootLogger.info("Waiting on thread %s" % t.name)
            t.join()

        count += 1
