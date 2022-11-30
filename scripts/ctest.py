#! /usr/bin/env python

# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s):  Michael Johnston

from __future__ import print_function

import logging
import optparse
import os
import shutil
import sys

import experiment.appenv
import experiment.runtime.backends
import experiment.model.data
import experiment.model.executors
import experiment.model.storage

toplevel = experiment.model.storage.FindExperimentTopLevel()

usage = "usage: %prog [options] [component name]"

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
parser.add_option("-a", "--platform", dest="platform",
        help="Platform to execute for",
        default=None,
        metavar="PLATFORM")
parser.add_option("-l", "--logLevel", dest="logLevel",
        help="The level of logging. Default %default",
        type="int",
        default=50,
        metavar="LOGGING")
parser.add_option("-y", '--hybrid', dest='hybridQueue',
                  help='If you want to test hybrid aspects of the specification specify this with a queue',
                  default=None,
                  metavar='HYBRID')
parser.add_option("", "--repairShadowDir", help="Attempt to repair shadow directory (default: False)",
                      action="store_true",
                      metavar="REPAIR_SHADOW_DIR",
                      default=False)

options, args = parser.parse_args()

FORMAT = '%(levelname)-9s %(threadName)-30s %(name)-30s: %(funcName)-20s %(asctime)-15s: %(message)s'
logging.basicConfig(format=FORMAT)
rootLogger = logging.getLogger() 
rootLogger.setLevel(options.logLevel)

if len(args) == 0:
    rootLogger.critical("No component specified - exiting")
    sys.exit(1)

d = {}
isInstance = True
if  experiment.model.storage.IsExperimentInstanceDirectory(options.experimentDir):
    expMethod = lambda *args, **kwargs: experiment.model.data.Experiment.experimentFromInstance(
        *args, attempt_shadowdir_repair=options.repairShadowDir, **kwargs)
else:
    isInstance = False
    expMethod = experiment.model.data.Experiment.experimentFromPackage
    d = {'platform':options.platform, 'timestamp':False}
    d['createVirtualEnvLinks'] = False
    d['createApplicationLinks'] = False
    #Use /tmp is availale
    if os.path.exists('/tmp'):
        d['location'] = '/tmp'

if options.hybridQueue is not None:
    experiment.appenv.HybridConfiguration.newDefaultConfiguration(isHybrid=True,
                                                                  remoteQueue=options.hybridQueue)

e = None

try:
    if not isInstance:

        experimentPackage = experiment.model.storage.ExperimentPackage.packageFromLocation(
            options.experimentDir, platform=d['platform']
        )
        e = expMethod(experimentPackage, **d)

    else:
        e = expMethod(options.experimentDir, **d)

    specification = e.findJob(options.stage, args[0])
    rootLogger.info(specification)
    pre, main, post = experiment.model.executors.CommandsFromSpecification(specification)

    #Test adding MPI
    main = experiment.model.executors.MPIExecutor.executorFromOptions(main, specification.resourceRequest)

    print('Working dir:', main.workingDir)
    print('\nEnvironment:', main.environment)
    print('\nEnd Chain:', main.endChain.commandLine)

    if len(pre) != 0:
        print("\nPRE-STACK:")

    for el in pre:
        for c in el.commandLine.split(';'):
            print(c)

    print('\nEXECUTION-CHAIN:')
    print(main.commandLine)

    if len(post) != 0:
        print("\nPOST-STACK:")

    for el in post:
        for c in el.commandLine.split(';'):
            print(c)

finally:    
    if e is not None and not isInstance:
        e.instanceDirectory.consolidate()
        rootLogger.warning("TEARING DOWN TEST DIRECTORIES")
        try:
            pass
            shutil.rmtree(e.instanceDirectory.location)
        except:
            pass
