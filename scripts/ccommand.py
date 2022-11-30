#! /usr/bin/env python

# Copyright IBM Inc. 2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston


'''Outputs the command line defined by a component in an experiment instance'''

from __future__ import print_function

import logging
import optparse
import os
import shutil
import sys
import tempfile

import experiment.model.data
import experiment.model.storage
import experiment.model.executors
import experiment.model.frontends.flowir

toplevel = experiment.model.storage.FindExperimentTopLevel()

usage = "usage: %prog [options] [component name] [component end (optional]"

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
        help="Dynamically reads the component definitions (a.k.a. patches)from a file "
             "and adds them to the experiment in the given stage",
        default=None,
        metavar="COMPONENT_DEFINITION")
parser.add_option("-a", "--platform", dest="platform",
        help="Platform to execute for. If given overrides any stored configuration",
        default=None,
        metavar="PLATFORM")
parser.add_option("", "--writePatches", dest="storePatches",
                  action="store_true",
                  help="Write patches to disk",
                  default=False,
                  metavar="WRITE_PATCHES")
parser.add_option('-f', '--flowirReport', dest='flowirReport',
                  action='store_true',
                  help='Print FlowIR description of component(s)')
parser.add_option("-v", "--verbose", dest="verbose",
        help="Turns on debugging output",
        action="store_true",
        default=False,
        metavar="VERBOSE")
parser.add_option("", "--env", dest="outputEnvironment",
        action="store_true",
        default=False,
        metavar="ENVIRONMENT")
parser.add_option("-l", "--logLevel", dest="logLevel",
        help="The level of logging. Default %default",
        type="int",
        default=50,
        metavar="LOGGING")
parser.add_option("", "--repairShadowDir", help="Attempt to repair shadow directory (default: False)",
                      action="store_true",
                      metavar="REPAIR_SHADOW_DIR",
                      default=False)


options, args = parser.parse_args()

options.experimentDir = os.path.normpath(os.path.abspath(options.experimentDir))

FORMAT = '%(levelname)-9s %(threadName)-30s %(name)-30s: %(funcName)-20s %(asctime)-15s: %(message)s'
logging.basicConfig(format=FORMAT)
rootLogger = logging.getLogger() 
rootLogger.setLevel(options.logLevel)

if len(args) == 0:
    rootLogger.critical("No component specified - exiting")
    sys.exit(1)

configPatches = {}
if options.componentDef is not None:
    print('Will apply dynamic component definitions in %s to experiment stage %d' \
                        % (options.componentDef, options.stage), file=sys.stderr)
    configPatches = {options.stage: [os.path.abspath(options.componentDef)]}

updateInstanceConfiguration = options.storePatches
if updateInstanceConfiguration is True:
    rootLogger.warning('Existing configuration instance files will be overwritten')
else:
    rootLogger.warning('Will not update configuration instance files')

is_package = False
#Check if the directory is an instance directory
if not experiment.model.storage.IsExperimentInstanceDirectory(options.experimentDir):
    rootLogger.info("Using %s as experiment package directory" % options.experimentDir)
    is_package = True
    experimentPackage = experiment.model.storage.ExperimentPackage.packageFromLocation(
        options.experimentDir, platform=options.platform
    )

    e = experiment.model.data.Experiment.experimentFromPackage(
        experimentPackage,
        location=tempfile.gettempdir(),
        timestamp=True,
        platform=options.platform,
    )
else:
    rootLogger.info("Using %s as experiment instance directory" % options.experimentDir)
    instanceDir = experiment.model.storage.ExperimentInstanceDirectory(options.experimentDir,
                                                                       attempt_shadowdir_repair=options.repairShadowDir)

    #This will use the stored configuration if present and only update it if storePatches is True
    e = experiment.model.data.Experiment(
        instanceDir,
        platform=options.platform,
        configPatches=configPatches,
        updateInstanceConfiguration=options.storePatches,
        is_instance=True
    )

try:
    #Don't check executables.
    #This allows returning a command line of a components even if others cannot be resolved
    #TODO: Allow use on packages by not checking direct references
    #This will allows outputing a command line from the package even though e.g. input data may not be present
    e.validateExperiment(checkExecutables=False)

    graph = e.experimentGraph

    for component in args:
        c = e.findJob(options.stage, component)
        if c is None:
            rootLogger.critical("No component called %s in stage %s" % (component, options.stage))
            rootLogger.critical("Available components in stage %d:\n  %s" % (
                options.stage,  '\n  '.join(sorted(e._stages[options.stage]._jobs.keys()))))
            sys.exit(1)

        pre, main, post = experiment.model.executors.CommandsFromSpecification(c)

        if options.flowirReport:
            comp_id = 'stage%d.%s' % (options.stage, component)

            print('Component: %s' % comp_id)
            try:
                # VV: Fetch FlowIR, compress, prettify (i.e. sort fields in some meaningful order), and present
                comp_flowir = graph.configurationForNode(comp_id, inject_missing_fields=False)
                comp_flowir = experiment.model.frontends.flowir.FlowIR.compress_flowir(comp_flowir)
                comp_flowir = experiment.model.frontends.flowir.FlowIR.pretty_flowir_component_soft(comp_flowir)
                print(experiment.model.frontends.flowir.yaml_dump(comp_flowir, default_flow_style=False))
            except Exception as e:
                import traceback

                print('Error: %s', e)
                print(traceback.format_exc())

        if options.outputEnvironment is True:
            env = main.environment
            envstring = ""
            for k in list(env.keys()):
                envstring += "%s=%s " % (k, env[k])

            print(envstring, end=' ')

        print(main.commandLine, '\n')
finally:
    if is_package:
        e.instanceDirectory.consolidate()
        try:
            shutil.rmtree(e.instanceDirectory.location)
        except:
            pass
