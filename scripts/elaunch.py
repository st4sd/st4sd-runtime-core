#! /usr/bin/env python
# coding=UTF-8

# Copyright IBM Inc. 2015, 2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

'''Deploys and monitors a computational experiment'''
from __future__ import annotations

import base64
import code
import datetime
import errno
import itertools
import json
import logging
import optparse
import os
import os.path
# Temporary
import pprint
import shutil
import signal
import sys
import tempfile
import threading
import time
import traceback
import uuid

import pandas

from distutils.dir_util import copy_tree
from typing import (Any, Dict, List, Optional, Set, Tuple, Union)

import yaml
from future.utils import raise_with_traceback
from six import string_types


import experiment.settings
import experiment.appenv

import experiment.test
import experiment.model.errors
import experiment.model.storage
import experiment.model.codes
import experiment.model.conf
import experiment.model.data
import experiment.model.frontends.flowir
import experiment.model.graph
import experiment.runtime.backends
import experiment.runtime.control
import experiment.runtime.errors
import experiment.runtime.monitor
import experiment.runtime.output
import experiment.runtime.workflow
import experiment.runtime.status
import experiment.runtime.utilities.s3
import experiment.service.db
import experiment.runtime.engine
import experiment.model.hooks

logging.addLevelName(19, 'INFO_MED')
logging.addLevelName(18, 'INFO_LOW')


#Add the python modules to the path if not present
moduleDir = os.path.split(os.path.abspath(__file__))[0]
moduleDir = os.path.join(moduleDir, '../python')
if not moduleDir in sys.path:
    sys.path.insert(0,moduleDir)


# VV: optparse.OptionParser raises a SystemExit(2) on error during parse_args(). However, it internally raises
# exceptions that derive from optparse.OptParserError so we can inherit optparse.OptionParser and just slightly
# modify parse_args() to cause it to raise descriptive exceptions.
# To err on the safe side we should catch BaseException when running parse_args() because I have not reviewed 100%
# of the code in optparse.OptionParser, its developers may also change it to raise SystemExit in a different way
class NoSystemExitOptparseOptionParser(optparse.OptionParser):
    def __init__(self, *args, **kwargs):
        super(NoSystemExitOptparseOptionParser, self).__init__(*args, **kwargs)

    def parse_args(self, args: List[str] = None, values: optparse.Values = None):
        """Parse the command-line options found in 'args' (default: sys.argv[1:]).

        Example::

            parse_args(args : [string] = sys.argv[1:], values : Values = None) -> (values : Values, args : [string])

        Returns:
            (values, args) where 'values' is a Values instance (with all
            your option values) and 'args' is the list of arguments left
            over after parsing options.

        Raises:
            optparse.OptParseError:
                On a parse error
        """
        rargs = self._get_args(args)
        if values is None:
            values = self.get_default_values()

        self.rargs = rargs
        self.largs = largs = []
        self.values = values

        # VV: This can raise optparse.OptParseError. OptionParser.parse_args() calls self.error to raise SystemExit
        _ = self._process_args(largs, rargs, values)

        args = largs + rargs
        return self.check_values(values, args)


def CreateHaltClosure(filename, sendSignal=signal.SIGINT):

    pid = os.getpid()
    log = logging.getLogger()

    log.info("Creating halt closure monitoring %s for pid %d" % (filename, pid))

    def CheckHalt(lastAction):

        halt = False

        try:
            with open(filename) as f:
                lines = f.readlines()
        except IOError:
            pass
        else:
            for line in lines:
                if int(line.strip('\n')) == pid:
                    halt = True

            if halt is True:
                log.warning("Found my pid %d in halt file at %s - terminating using %d" % (pid, filename, sendSignal))
                os.kill(pid, sendSignal)

    return CheckHalt


def point_logger_to_random_file():
    # type: () -> Tuple[logging.Logger, str]
    # Add a FileHandler to logging using a file in the shadow dir
    rootLogger = logging.getLogger()
    logFile = tempfile.NamedTemporaryFile(delete=False).name
    handler = logging.FileHandler(logFile)
    handler.setFormatter(rootLogger.handlers[0].formatter)
    rootLogger.addHandler(handler)

    rootLogger.info("Experiment log at %s" % logFile)
    return rootLogger, logFile


def point_logger_to_logfile(instance_dir):
    # type: (experiment.model.storage.ExperimentInstanceDirectory) -> Tuple[logging.Logger, str]
    """Add a FileHandler to logging using a file in the shadow dir

    If a fileHandler already exists it copies the file it has already created to output/experiment.log
    before creating a new fileHandler
    """
    rootLogger = logging.getLogger()

    old_file = None
    for handler in [x for x in rootLogger.handlers if isinstance(x, logging.FileHandler)]:
        try:
            old_file = handler.baseFilename
        except Exception:
            pass
        rootLogger.removeHandler(handler)

    with instance_dir.mtx_output:
        logFile = os.path.join(instance_dir.outputDir, 'experiment.log')
        try:
            if old_file and os.path.isfile(old_file):
                shutil.copyfile(old_file, logFile)
        except Exception:
            pass

    handler = logging.FileHandler(logFile)
    rootLogger.addHandler(handler)
    handler.setFormatter(rootLogger.handlers[0].formatter)

    rootLogger.info("Experiment log at %s" % logFile)

    return rootLogger, logFile


SetupExceptions = Union[experiment.model.errors.ExperimentInvalidConfigurationError,
                        Optional[experiment.model.errors.ExperimentSetupError]]
SetupReturnType = Tuple[SetupExceptions, Optional[experiment.model.data.Experiment],
                        Optional[experiment.runtime.output.OutputAgent], Optional[
                            experiment.runtime.output.StatusMonitor]]


def extract_application_dependency_source(options):
    # type: (optparse.Values) -> Dict[str, str]
    """Parses --applicationDependencySource commandline arguments (`applicationDependencyEntry:/path/to/new/source`)

    Args:
        options(optparse.Values): commandline arguments to elaunch

    Returns
        A dictionary whose keys are application dependency names and values are absolute paths
    """
    application_dependency_source = {}  # type: Dict[str, str]

    for s in options.application_dependency_source:
        try:
            app_dep, new_source = s.split(':', 1)
        except ValueError:
            raise ValueError('Application dependency source "%s" does not have the format '
                             '`applicationDependencyEntry:/path/to/new/source`' % s)
        application_dependency_source[app_dep] = new_source
    return application_dependency_source


def Setup(setupPath, options):
    # type: (str, optparse.Values) -> SetupReturnType
    '''Wraps setting up the experiment for running.

    Parameters:
        setupPath: if options.restart is none this is interpreted as a package path
        If options.restart is non-none this is interpreted as an instance path
        options: The optparse options object

    Returns:
        A tuple of 4 elements:
        setupErrorException, experimentInstance, outputAgent, statusMonitor
        setupErrorException will be set to a non-none value if there is a problem with the Experiment,
        otherwise it will be set to None
    '''

    #Do some testing
    #Many of the same test are repeat on experiment creation etc.
    #This test is done in order to avoid costs of actually attempting certain copy operations
    #when the experiment will fail to deploy

    if not experiment.test.CheckSystem():
        setup_error = experiment.model.errors.ExperimentSetupError("System check failed - cannot deploy", None, None)
        return setup_error, None, None, None

    cwl_options = {}
    if options.cwlFile is not None:
        cwl_options['cwlFile'] = options.cwlFile

    if options.cwlJobOrderFile is not None:
        cwl_options['cwlJobOrderFile'] = options.cwlJobOrderFile

    compExperiment = None
    rootLogger = None

    custom_application_sources = extract_application_dependency_source(options)

    if options.restart is None:
        packagePath = setupPath
        isValid, error, compExperiment = experiment.test.ValidatePackage(
            packagePath, platform=options.platform, format_priority=options.formatPriority,
            inputs=options.inputs, data=options.data, variables=options.variables, timestamp=options.stamp,
            instance_name=options.instance_name, custom_application_sources=custom_application_sources,
            manifest=options.manifest, **cwl_options)

        if not isValid:
            instance_dir = compExperiment.instanceDirectory if compExperiment else None
            setup_error = experiment.model.errors.ExperimentSetupError("Package validation failed - cannot deploy", error,
                                                                       instance_dir=instance_dir)
            return setup_error, None, None, None
        try:
            rootLogger, logFile = point_logger_to_logfile(compExperiment.instanceDirectory)
        except Exception:
            rootLogger = logging.getLogger()
            rootLogger.log(15, traceback.format_exc())
            setup_error = experiment.model.errors.ExperimentSetupError(
                "Encountered exception while creating experiment instance - cannot deploy", error,
                instance_dir=compExperiment.instanceDirectory)
            return setup_error, compExperiment, None, None
    else:
        #Use existing instance of the experiment
        instancePath = os.path.abspath(setupPath)
        try:
            #Note: Setting ignoreExisting to True so data already present is not treated as input
            #TODO: As it stands an experiment can be restarted from a stage without removing existing data
            #This is actually not what is intended by the restart mechanism but it is very useful behaviour
            #The setting of ignoreExisting to True is to support this, unintended, use of restarting
            #This is inconsistent and may cause problems down the line so the meaing of restart needs to be extended
            #to explicity support the case of restarting with existing data.
            instanceDirectory = experiment.model.storage.ExperimentInstanceDirectory(instancePath, ignoreExisting=True)
            rootLogger, logFile = point_logger_to_logfile(instanceDirectory)
            # VV: @tag:FlowRestartInstanceUpdate
            # VV: When restarting elaunch updates the instance configuration files **only** if the platform has changed
            new_platform = options.platform

            try:
                with open(os.path.join(instancePath, 'elaunch.yaml'), 'r') as old_elaunch:
                    old_elaunch_contents = experiment.model.frontends.flowir.yaml_load(old_elaunch)

                old_platform = old_elaunch_contents['platform'] or experiment.model.frontends.flowir.FlowIR.LabelDefault

                if new_platform is None or (old_platform == new_platform):
                    # VV: If no platform was set OR the set platform matches the one used by the instance, then load
                    #     the instance and don't update its configuration on the disk
                    options.platform = old_platform
                    updateInstanceConfiguration = False
                    is_instance = True
                else:
                    # VV: If the platform has changed, use the new one, load a package (including variable files) then
                    #     store the new instance on the disk
                    options.platform = new_platform
                    updateInstanceConfiguration = True
                    is_instance = False
            except Exception as e:
                if rootLogger is None:
                    rootLogger = logging.getLogger()
                rootLogger.log(15, traceback.format_exc())
                rootLogger.warning("Restarting but failed to fetch last used platform. Will parse package description.")

                # VV: The elaunch.yaml file cannot be trusted, load the instance configuration as if it were
                #     a package and hope for the best (to clarify, the experiment will still run in `restart` mode)
                updateInstanceConfiguration = True
                is_instance = False

            compExperiment = experiment.model.data.Experiment(
                instanceDirectory,
                platform=options.platform,
                updateInstanceConfiguration=updateInstanceConfiguration,
                format_priority=options.formatPriority,
                # VV: parse package when the instance files are to be regenerated
                is_instance=is_instance
            )
            rootLogger, logFile = point_logger_to_logfile(compExperiment.instanceDirectory)
        except Exception as error:
            if rootLogger is None:
                rootLogger = logging.getLogger()

            rootLogger.log(15, traceback.format_exc())
            setup_error = experiment.model.errors.ExperimentSetupError(
                "Encountered exception while creating experiment instance - cannot deploy", error,
                instance_dir=compExperiment.instanceDirectory)
            return setup_error, compExperiment, None, None

    # VV: Initialize backends before performing checkExecutable tests
    try:
        k8s_store_path = os.path.join(compExperiment.instanceDirectory.configurationDirectory, 'k8s-config.yaml')
        k8s_config_path = k8s_etc_path = '/etc/podinfo/flow-k8s-conf.yml'
        if os.path.exists(k8s_etc_path) is False:
            # VV: Looks like we're not running in a pod launched by the workflow operator. This could still be a pod
            # that hopes to use the $INSTANCE_DIR/conf/k8s-config.yaml file (similar to cexecute)
            k8s_config_path = k8s_store_path
            k8s_store_path = None

        experiment.runtime.backends.InitialiseBackendsForWorkflow(
            compExperiment.experimentGraph, ignoreInitializeBackendError=options.ignoreInitializeBackendError,
            logger=rootLogger, k8s_no_sec_context=options.no_k8s_security_context, k8s_config_path=k8s_config_path,
            k8s_store_path=k8s_store_path, k8s_garbage_collect=options.kubernetesGarbageCollect,
            k8s_archive_objects=options.kubernetesArchiveObjects
        )
    except Exception as e:
        rootLogger.log(15, '%s\nException: %s' % (traceback.format_exc(), e))

        if options.ignoreTestExecutablesError:
            rootLogger.warning("Ran into error %s while initializing backends - ignoreTestExecutablesError"
                               " parameter has been set, will not raise an exception" % e)
        else:
            raise_with_traceback(e)

    # VV: Experiment instance dir has been generated, validate the the experiment
    try:
        compExperiment.validateExperiment(ignoreTestExecutablesError=options.ignoreTestExecutablesError)
    except Exception as error:
        rootLogger.log(15, traceback.format_exc())
        setup_error = experiment.model.errors.ExperimentInvalidConfigurationError(
            "Encountered exception while validating experiment instance - cannot deploy", error)
        return setup_error, compExperiment, None, None

    try:
        # VV: Next we need to inspect the interface and potentially invoke extractionMethods for the `inputSpec`
        # (see experiment.model.frontends.flowir.type_flowir_interface_structure() for the schema of `interface.)
        interface = compExperiment.configuration.get_interface()
        if interface:
            # VV: At this point we can only extract the input ids, we have not measured the properties yet
            try:
                do_interface = experiment.model.hooks.interface.Interface(interface, "FlowIR.interface")
                input_ids, additional_data = do_interface.extract_input_ids(exp=compExperiment)

                compExperiment.set_interface_input_ids(input_ids, store_on_disk=True)
                compExperiment.set_interface_additional_input_data(additional_data, store_on_disk=True)
                compExperiment.configuration.store_unreplicated_flowir_to_disk()

                rootLogger.info(f"Extracted {len(input_ids)} input ids, first 5 are {input_ids[:5]}")

                del do_interface
                del input_ids
                del additional_data
            except Exception as e:
                rootLogger.warning(f"Interface could not interpret inputSpec due to {e} - will ignore error, "
                                   f"virtual experiment properties may be corrupted or incomplete.")
                rootLogger.log(15, f"Traceback is ignored: {traceback.format_exc()}")
    except Exception as error:
        rootLogger.log(15, traceback.format_exc())
        if isinstance(error, experiment.model.errors.ExperimentInvalidConfigurationError):
            setup_error = error
        else:
            setup_error = experiment.model.errors.ExperimentInvalidConfigurationError(
                "Encountered exception while running interface.inputSpec instructions - cannot deploy", error)
        return setup_error, compExperiment, None, None

    # VV: Even though S3 upload happens at the end of each stage index we should ensure now (before any
    #     task is generated) that the auth information is correct. If we cannot auth, bail out.
    output_util = None
    try:
        if options.s3StoreToURI:
            try:
                s3_client = s3_instantiate_s3utility(
                    options.s3StoreToURI, options.s3AuthWithEnvVars, options.s3AuthBearer64)
            except Exception as s3_error:
                rootLogger.warning("Issue with S3 parameters: %s - will exit" % s3_error)
                raise_with_traceback(s3_error)
            else:
                s3_bucket, s3_bucket_root = experiment.runtime.utilities.s3.S3Utility.split_s3_uri(options.s3StoreToURI)
                instance_dir_name = os.path.split(compExperiment.instanceDirectory.location)[1]
                s3_bucket_root = experiment.model.frontends.flowir.FlowIR.fill_in(
                    s3_bucket_root, {'instanceDir': instance_dir_name})
                rootLogger.info("Successfully authenticated to S3 - will upload key-outputs to %s" %
                                experiment.runtime.utilities.s3.S3Utility.make_s3_uri(s3_bucket, s3_bucket_root))
                output_util = experiment.runtime.output.OutputToS3(
                    s3_client, s3_bucket, s3_bucket_root, update_existing=True)
    except Exception as error:
        rootLogger.log(15, traceback.format_exc())
        if isinstance(error, experiment.model.errors.ExperimentInvalidConfigurationError):
            setup_error = error
        else:
            setup_error = experiment.model.errors.ExperimentInvalidConfigurationError(
                "Encountered exception while setting up S3 - cannot deploy", error)
        return setup_error, compExperiment, None, None

    #Create output agent and status monitor
    try:
        outputAgent = experiment.runtime.output.OutputAgent(compExperiment, output_to=output_util)
        outputAgent.checkDataReferences()
    except experiment.model.errors.ExperimentInvalidConfigurationError as error:
        if rootLogger is None:
            rootLogger = logging.getLogger()
        rootLogger.log(15, traceback.format_exc())
        setup_error = experiment.model.errors.ExperimentSetupError(
            "Encountered exception while creating output monitor instance - cannot deploy", error,
            instance_dir=compExperiment.instanceDirectory)
        return setup_error, compExperiment, None, None

    try:
        statusMonitor = experiment.runtime.output.StatusMonitor(compExperiment)
    except experiment.model.errors.ExperimentInvalidConfigurationError as error:
        setup_error = experiment.model.errors.ExperimentSetupError(
            "Encountered exception while creating status monitor instance - cannot deploy", error,
            instance_dir=compExperiment.instanceDirectory)
        return setup_error, compExperiment, outputAgent, None

    if options.restart is None:
        concrete = compExperiment.experimentGraph.configuration.get_flowir_concrete()
        try:
            reservation = concrete.get_global_variable('reservation')
        except experiment.model.errors.FlowIRVariableUnknown:
            reservation = None
        #Replicate experiment structure on remote side if in hybrid environment
        experiment.appenv.HybridConfiguration.defaultConfiguration().replicateExperimentStructure(
            reservation, compExperiment.instanceDirectory,
        )
    else:
        rootLogger.info("Not replicating experiment structure as restart has been specified")

    return None, compExperiment, outputAgent, statusMonitor


def signal_handler(signal, frame):
    logging.getLogger().warning('Caught signal %s', str(signal))
    raise KeyboardInterrupt


def trigger_discoverer(discoverer_monitor_dir: str, exp_location: str, wf_report_id: str,
                       trigger_event_id: int, finished_components: List[experiment.service.db.DictMongo],
                       platform: str, upsert_documents: List[experiment.service.db.DictMongo] | None):
    """Emits a file that the discovererMonitor (Centralized Database reporter) parses to asynchronously update the CDB

    Args:
        discoverer_monitor_dir: path to the directory that the reporter expects trigger JSON files
        exp_location: location of experiment
        wf_report_id: a string identifying the experiment (all trigger events will share this)
        trigger_event_id: the reporter respects this number, it ingests trigger events starting with the one
            with the lowest index
        finished_components: list of components which have finished since
            the last emission (dictionary has the format {"stage":int, "name": str}
        platform: the platform the experiment instance uses
        upsert_documents: List of documents that reporter will upsert into the DB

    Raises
        OSError/IOError when unable to write file under discovererMonitor dir
    """

    if discoverer_monitor_dir:
        # VV: Inform experiment-discoverer about newly finished components
        try:
            if os.path.exists(discoverer_monitor_dir) is False:
                # VV: No need to modify the dir permissions because umask()
                os.mkdir(discoverer_monitor_dir)
        except OSError as e:
            rootLogger.warning("Unable to create discovererMonitorDir %s, error %s - will not trigger discoverer" % (
                discoverer_monitor_dir, e))
            raise_with_traceback(e)

        output_file = os.path.join(discoverer_monitor_dir, '%s-%d.json' % (wf_report_id, trigger_event_id))
        temp_file = os.path.join(discoverer_monitor_dir, '%s-%d._temp' % (wf_report_id, trigger_event_id))

        instructions = {
            'experiment-location': exp_location,
            'platform': platform or experiment.model.frontends.flowir.FlowIR.LabelDefault,
            'finished-components': finished_components,
        }

        if upsert_documents:
            instructions['upsert-documents'] = upsert_documents

        # VV: Write to a temp file and then rename it to .json (Discoverer is looking for .json files)
        try:
            with open(os.path.join(temp_file), 'w') as f:
                json.dump(instructions, f)
        except IOError as e:
            rootLogger.warning("Unable to write discoverer trigger file %s, error %s - will not trigger discoverer" % (
                temp_file, e))
            raise_with_traceback(e)
        else:
            try:
                os.rename(temp_file, output_file)
            except OSError as e:
                rootLogger.warning("Unable to rename dicoverer trigger file %s to %s, error %s "
                                   "- will not trigger discoverer" % (temp_file, output_file, e))
                raise_with_traceback(e)


def Run(
        compExperiment,  # type: experiment.model.data.Experiment
        outputAgent,  # type: experiment.runtime.output.OutputAgent
        statusMonitor,  # type: import experiment.runtime.output.StatusMonitor
        controller,  # type: experiment.runtime.control.Controller
        statusDatabase,  # type: import experiment.runtime.status.StatusDB
        restartOptions=None,  # type: Optional[Dict[str, Any]]
        discovererMonitorDir=None,  # type: Optional[str]
        discovererReportId=None,  # type: Optional[str]
        platform_to_use=None,  # type: Optional[str]
):
    # type: (...) -> None
    '''Runs the experiment.

    Parameters:
        compExperiment (experiment.data.Experiment): The experiment object representing the static workflow
        outputAgent (experiment.output.OutputAgent): Updating output of the workflow
        statusMonitor (experiment.output.StatusMonitor): Object monitoring and reporting the progress of the workflow
        controller (experiment.control.Controller): The workflow orchestrator
        restartOptions (dict): A dictionary specifying restart options. Default None
            Valid Keys: startStage, restageData, restartSources
        discovererMonitorDir (str): Directory that the discoverer daemon is monitoring (discoverer adds current
           experiment to Mongodb and acts as a gateway to serve experiment files over a socket)
        discovererReportId (str): Unique ID for this experiment
        platform_to_use (str): The platform that the experiment will target

    Handles Job/Stage failure if possible.
    If it can't recover from failure raises errors.StageFailedError'''

    log = logging.getLogger()

    #First start the status monitors
    #The output monitor is restarted for each stage as the files to monitor change
    statusMonitor.run(controller)

    if restartOptions is None:
        startStage = 0
        restart = False
    else:
        restart = True
        startStage = restartOptions['startStage']
        log.warning("Restarting from stage %d\n" % startStage)
        compExperiment.setCurrentStage(startStage)
        #compExperiment.cleanStage()

    current_stage_index = compExperiment.currentStage()
    while current_stage_index < compExperiment.numStages():
        stage = compExperiment.getStage(current_stage_index)

        stageData = True if (restart is False or current_stage_index != startStage) else restartOptions['restageData']
        restartSources = (
            False if (restart is False or current_stage_index != startStage)
            else restartOptions['restartSources']
        )
        log.info("Running %s" % stage)

        #Running the stage is broken into two steps so we can
        #identify specific errors from each
        #First resolve data references for all the jobs in the stage
        try:
            if not stageData:
                log.warning('Not staging data for first stage, %d, during restart' % startStage)

            controller.initialise(stage, statusDatabase)

        except experiment.model.errors.DataReferenceFilesDoNotExistError as error:
            rootLogger.critical('EXCEPTION %s\n' % traceback.format_exc())
            #Its fatal if file required for a job do not exist
            raise experiment.runtime.errors.StageFailedError(stage, error)
        except (
                experiment.model.errors.FilesystemInconsistencyError, experiment.model.errors.DataReferenceCouldNotStageError) as e:
            #Its possible we could retry the staging if we encountered these errors
            #For now they are fatal
            raise experiment.runtime.errors.StageFailedError(stage, e)

        #Now run
        #Different errors are possible some of which can be solved in different ways
        #Hence the need to separately id related categories

        if restartSources:
            log.warning('Will use available restart hooks for first stage, %d, during restart' % startStage)

        try:
            controller.run()
        except experiment.model.errors.SystemError as error:
            #Errors due to system related problems
            #e.g. job launcher/scheduler not available, job launch on local machine failed
            #file-system crashed e
            #These errors could be recoverable in a number of ways
            #e.g. changing resource used to launch job, restarting scheduler or waiting until it returns
            raise experiment.runtime.errors.StageFailedError(stage, error)
            #if experiment.test.CheckSystem(errorObject):
            #    compExperiment.restage(stage)
            #else:
            #    stopExperiment = True
        except experiment.runtime.errors.FinalStageNoFinishedLeafComponents as error:
            if stage.continueOnError:
                log.warning('Final stage %s does not have any finished leaf nodes but has the '
                            'continueOnError option switched on' % stage.name)
            else:
                raise experiment.runtime.errors.StageFailedError(stage, error)
        except experiment.runtime.errors.UnexpectedJobFailureError as error:
            #Errors when a Job fails unexpectedly but perhaps for a known reason
            #For example time-step too high, restart files corrupted
            #Again some hooks may be required to id if the Job is recoverable
            #Also the user can specify that the experiment continues to next stage even if this errors occur
            #This for example allows a final stage to run which produces results based on the results up to the failure
            #Note: If components in the next stage migrate from the failed stage the next stage will also fail
            if stage.continueOnError:
                failed_components = controller._stageStates[stage.index].failedComponents
                component_names = [component.specification.identification.identifier for component in failed_components]

                log.warning('Stage %s failed: continuing with next as instructed by configuration. '
                         'Will resolve output dependencies of %s' % (stage.name, component_names))
            else:
                raise experiment.runtime.errors.StageFailedError(stage, error)
        except experiment.model.errors.ExperimentError as error:
            #Errors due to the experiment not progress as expected e.g. non-convergence
            #Can we skip/restage ...???
            #This requires some monitoring hook ....
            #TODO: Add a continueOnError ability here -
            #perhaps with new name to discriminate these errors with above
            raise experiment.runtime.errors.StageFailedError(stage, error)
            #if not stage.isSkippable():
            #   if not compExperiment.canRestage(stage, errorObject):
            #       log.warning("Stage failed and cannot recover")
            #       stopExperiment = True
        finally:
            log.info('Stage %d complete' % current_stage_index)

            # VV: We likely do not want to catch exceptions here so that if the key-output mechanism doesn't work
            # or the discoverer Monitor signals don't reach the discoverer these issues are not hidden under the rug
            try:
                outputAgent.process_stage(stage.index)
            except Exception as e:
                rootLogger.log(15, traceback.format_exc())
                rootLogger.warning("OutputAgent ran into error %s while dealing with outputs in %s - will ignore "
                             "exception" % (e, stage.referenceName))
            try:
                if discovererMonitorDir:
                    # VV: Trigger the discoverer to upsert the documents of the just finished components
                    stage_index = stage.index
                    rootLogger.info(f"Generating documentDescriptions for components in stage {stage_index}")

                    docs = stage.documentDescription
                    compExperiment.statusFile.setUpdated(datetime.datetime.now())

                    # VV: This will populate component-state, memoization-hash, and instance
                    # however instance will be overridden by discoverer to include actual gateway-id
                    docs = compExperiment.annotate_component_documents(options.datastoreLabel, docs)

                    exp_doc = compExperiment.generate_experiment_document_description(options.datastoreLabel)
                    docs.append(exp_doc)

                    finished_components = [{'stage': stage_index, 'name': j} for j in stage.jobNames()]
                    trigger_discoverer(discovererMonitorDir, compExperiment.instanceDirectory.location,
                                       discovererReportId, stage_index, finished_components,
                                       platform_to_use, docs)
            except Exception as e:
                rootLogger.log(15, traceback.format_exc())
                rootLogger.warning("Ran into error %s while triggering discoverer after %s completion - will ignore "
                                   "exception" % (e, stage.referenceName))

        # VV: Increment the stage index when :
        # a) Stage finished successfully
        # b) Stage failed but was annotated to continueOnError
        current_stage_index = compExperiment.incrementStage()


class LivePatcher:
    comp_experiment = None  # type: "experiment.data.Experiment"
    controller = None  # type: "experiment.control.Controller"
    output_agent = None  # type: "experiment.output.OutputAgent"
    mon_status = None  # type: "experiment.output.StatusMonitor"
    components = {}  # type: "Dict[int, Set[experiment.workflow.ComponentState]]"
    format_priority = None  # type: List[str]
    platform_name = None  # type: str
    initial_stage = None  # type: int
    is_experiment_paused = False

    @classmethod
    def get_entities_to_pause(cls):
        # entities = [controller, mon_output, mon_status]
        entities = [cls.controller]

        return entities

    @classmethod
    def pause_experiment(cls):
        rootLogger.info("Pausing experiment")

        entities = cls.get_entities_to_pause()

        for entity in entities:
            entity.sleep()

        # VV: Now wait for the entities to actually pause
        for entity in entities:
            rootLogger.info("Waiting for %s to go to sleep" % entity)
            while entity.is_sleeping is False:
                time.sleep(1.0)

        cls.is_experiment_paused = True

    @classmethod
    def wake_up_experiment(cls):
        rootLogger.info("Waking up experiment")

        entities = cls.get_entities_to_pause()
        # VV: Wake up the entities in reverse order, so that the controller wakes up last
        for entity in reversed(entities):
            rootLogger.info("Waking up %s" % entity)
            entity.wake_up()

        # VV: Now wait for the entities to actually wake up
        for entity in entities:
            rootLogger.info("Waiting for %s to wake up" % entity)
            while entity.is_sleeping is True:
                time.sleep(1.0)

        cls.is_experiment_paused = False

    @classmethod
    def live_patch(cls):
        rootLogger.info("Applying patch")

        cls.pause_experiment()

        try:
            old_concrete = cls.comp_experiment.configuration.get_flowir_concrete()

            platform = cls.platform_name

            root_dir = cls.comp_experiment.instanceDirectory.location

            patch_dir = os.path.join(root_dir, '../flow.patch')

            rootLogger.info("Copying files from patch")

            # VV: Contrary to shutil.copy_tree(0, distutils.dir_util.copy_tree can copy over
            #     existing directories.
            copy_tree(patch_dir, root_dir, update=0)

            try:
                shutil.copytree(patch_dir, root_dir)
            except OSError as data:
                if data.errno != errno.EEXIST:
                    raise

            new_workflow_graph = experiment.model.graph.WorkflowGraph.graphFromExperimentInstanceDirectory(
                cls.comp_experiment.instanceDirectory, platform=platform,
                systemvars=cls.comp_experiment.configuration._system_vars,
                createInstanceConfiguration=True,
                updateInstanceConfiguration=True,
                primitive=False,

                # VV: Live-patches are stored in `flowir` format
                format_priority=['flowir'],
                inherit_graph=cls.comp_experiment.experimentGraph,
                # VV: Live-patches are also stored in their `package` flavor
                is_instance=False
            )

            new_concrete = new_workflow_graph.configuration.get_flowir_concrete()

            # VV: Ensure that all application_dependencies and virtual_environments are linked

            application_dependencies = new_concrete.get_application_dependencies()
            experiment.model.data.Experiment._populateApplicationSources(
                application_dependencies, cls.comp_experiment.instanceDirectory.packagePath,
                cls.comp_experiment.instanceDirectory, ignore_existing=True,
                custom_application_sources=cls.custom_application_sources
            )

            virtual_environments = new_concrete.get_virtual_environments()
            experiment.model.data.Experiment._createVirtualEnvLinks(
                virtual_environments, cls.comp_experiment.instanceDirectory.packagePath,
                cls.comp_experiment.instanceDirectory, platform, ignore_existing=True
            )
        except Exception as e:
            rootLogger.critical("Failed to load live-patch description. EXCEPTION %s" % traceback.format_exc())
            rootLogger.critical("Experiment will continue from the point just before the attempt to live-patch.")
            cls.wake_up_experiment()
            return
        earliest_patch_in_stage = None
        # VV: Discover brand new components and create their respective working directories on the local
        #     (and then on the hybrid/remote) filesystem
        instance_location = cls.comp_experiment.instanceDirectory.location

        try:
            old_component_ids = old_concrete.get_component_identifiers(False)
            new_component_ids = new_concrete.get_component_identifiers(False)

            novel_component_ids = new_component_ids.difference(old_component_ids)
            rootLogger.info("Patched-in components")
            rootLogger.info(pprint.pformat(novel_component_ids))

            component_folders = []

            rootLogger.info("Creating component output directories")

            instance_dir = cls.comp_experiment.instanceDirectory
            for novel_comp_id in novel_component_ids:
                stage_index, comp_name = novel_comp_id
                if earliest_patch_in_stage is None or earliest_patch_in_stage > stage_index:
                    earliest_patch_in_stage = stage_index
                new_dir = instance_dir.workingDirectoryForComponent(stage_index, comp_name)
                component_folders.append(new_dir)
        except Exception as e:
            rootLogger.critical("Failed replicate experiment structure on local filesystem. "
                                "EXCEPTION %s" % traceback.format_exc())
            rootLogger.critical("Experiment will continue from the point just before the attempt to live-patch.")
            cls.wake_up_experiment()
            return

        hybrid_conf = experiment.appenv.HybridConfiguration.defaultConfiguration()
        if hybrid_conf.isHybrid:
            try:
                reservation = new_concrete.get_global_variable('reservation')
            except experiment.model.errors.FlowIRVariableUnknown:
                reservation = None

            try:
                hybrid_conf.replicateFolderStructure(cls.comp_experiment.instanceDirectory, component_folders)

                rootLogger.info("Staging in patch-in files")
                hybrid_conf.replicateExperimentStructure(
                    reservation, cls.comp_experiment.instanceDirectory,
                    skip_top_dir_paths=['stages', 'output', '.git'],
                )
            except Exception as e:
                rootLogger.warning("Failed replicate experiment structure on hybrid filesystem. "
                                    "EXCEPTION %s" % traceback.format_exc())
                rootLogger.warning("Experiment will continue from the point just before the attempt to live-patch.")
                cls.wake_up_experiment()
                return

        rootLogger.info("Switching to the new WorkflowGraph")
        cls.comp_experiment.switchWorkflowGraph(new_workflow_graph)

        rootLogger.info("Updating stages and generating patched-in Jobs")
        cls.comp_experiment.updateStages()

        rootLogger.info("Generating patched-in ComponentStates and checking their executables")
        generate_components(components, cls.comp_experiment, cls.initial_stage, check_executables=True, verbose=True)

        starting_stage_index = min(LivePatcher.initial_stage, earliest_patch_in_stage)
        rootLogger.info("Recomputing Component dependencies (starting stage: %s)" % starting_stage_index)
        with controller.comp_lock:
            controller.parse_workflow_graph()

        rootLogger.warning("LivePatch is successful, the experiment will now continue (conf directory will be"
                           "cleared to contain just the FlowIR description of the package and the instance)")
        cls.output_agent.parse_key_outputs()
        conf_dir = cls.comp_experiment.instanceDirectory.configurationDirectory
        archive_dir = os.path.join(conf_dir, 'archive')
        if os.path.exists(archive_dir) is False:
            os.makedirs(archive_dir)

        _, folders, files = next(os.walk(conf_dir))

        for folder in folders:
            if folder != 'archive':
                src = os.path.join(conf_dir, folder)
                copy_tree(src, os.path.join(archive_dir, folder))
                shutil.rmtree(src, ignore_errors=True)

        for file in files:
            if file not in ['flowir_package.yaml', 'flowir_instance.yaml']:
                try:
                    os.rename(os.path.join(conf_dir, file), os.path.join(archive_dir, file))
                except Exception as e:
                    rootLogger.warning("Failed to move file %s to %s" % (
                        os.path.join(conf_dir, file), archive_dir
                    ))

        cls.wake_up_experiment()


def live_patch_trigger():
    threading.Thread(target=LivePatcher.live_patch).start()


def generate_components(
        components,  # type: Dict[int, Set[experiment.runtime.workflow.ComponentState ]]
        compExperiment,  # type: experiment.model.data.Experiment
        initial_stage,   # type: int
        check_executables=True,
        verbose=False,
):
    # type: (...) -> None

    """Generates ComponentState for an experiment, may generate engine object and rewrite executable/k8s-image

    Method may be called multiple times if for example workflow instance is dynamically patched.

    Arguments:
      components(Dict[int, Set[experiment.workflow.ComponentState ]]): A dictionary which holds existing
        component state objects (grouped by their stage_index). Will be updated with any new ones that this
        method generates.
      compExperiment(experiment.data.Experiment): The experiment instance for which to generate ComponentState objects
      initial_stage(int): The index of the very first stage that will be execute. Components whose stage-index
        is at least @initial_stage will also
      check_executables(bool): When True will also check executables of components and update their FlowIR with
        the resolved path to the executable as well as k8s-image (if component uses the the k8s backend)
      verbose(bool): Print info statements
    """
    # VV: TODO Deprecate this and move functionality into Controller and WorkflowGraph:
    #     WorkflowGraph should be performing checkExecutables and wiring Job objects to WorkflowGraph
    #     Controller should be instantiating ComponentState objects

    workflowGraph = compExperiment.experimentGraph

    for i, stage in enumerate(compExperiment._stages):
        if stage.index not in components:
            components[stage.index] = set()

        for job in stage.jobs():
            cid = job.componentSpecification.identification.identifier
            existing_component = [comp for comp in components[stage.index]
                                  if comp.specification.identification.identifier == cid]

            if not existing_component:
                if verbose:
                    rootLogger.info("Generated new ComponentState for %s (%s)" % (cid, workflowGraph))

                component = experiment.runtime.workflow.ComponentState(
                    job, workflowGraph, create_engine=bool(i >= initial_stage))
                components[component.stageIndex].add(component)
            else:
                rootLogger.info("Won't generate new ComponentState for %s because it already exists. Updating "
                                "its graph to %s" % (cid, workflowGraph))
                assert len(existing_component) == 1
                comp = existing_component[0]  # type: experiment.runtime.workflow.ComponentState
                comp.updateGraph(workflowGraph)

            if check_executables:
                # VV: Ensure that component has a valid executable and uses the appropriate Kubernetes image
                job.componentSpecification.checkExecutable(
                    ignoreTestExecutablesError=options.ignoreTestExecutablesError, updateSpecification=True)


def s3_instantiate_s3utility(s3_uri, s3_auth_with_env_vars, s3_auth_bearer_64):
    # type: (str, str, str) -> experiment.runtime.utilities.s3.S3Utility
    """Validates s3 parameters and returns an instance of experiment.s3_utilities.S3Utility

    Note there's no way to check whether folders exist on s3 because there's no concept of folder for s3.

    Args:
        s3_uri(str): a S3 uri pointing to key in bucket which will host key-outputs (s3://<bucket id>/<rel path>)
        s3_auth_with_env_vars(bool): if True will extract S3 authentication credentials from environment variables:
            'S3_ACCESS_KEY_ID', 'S3_SECRET_ACCESS_KEY', and 'S3_END_POINT'
        s3_auth_bearer_64(str): base64 encoded JSON dictionary with S3 authentication credential keys
            'S3_ACCESS_KEY_ID', 'S3_SECRET_ACCESS_KEY', and 'S3_END_POINT'
    """
    s3_keys = ['S3_ACCESS_KEY_ID', 'S3_SECRET_ACCESS_KEY', 'S3_END_POINT']

    # VV: This to ensure that s3 URI is valid
    _ = experiment.runtime.utilities.s3.S3Utility.split_s3_uri(s3_uri)

    if s3_auth_with_env_vars and s3_auth_bearer_64:
        raise ValueError('--s3AuthWithEnvVars and --s3AuthBearer64 are mutually exclusive')
    elif s3_auth_with_env_vars:
        try:
            s3_info = {k: os.environ[k] for k in s3_keys}
        except KeyError as e:
            raise ValueError("S3AuthCredential environment variable %s has not been defined" % str(e))
    elif s3_auth_bearer_64:
        try:
            dec64 = base64.decodebytes(s3_auth_bearer_64.encode('utf-8')).decode('utf-8')
        except AttributeError:
            dec64 = base64.decodestring(s3_auth_bearer_64)

        s3_info = json.loads(dec64)
        if isinstance(s3_info, dict) is False:
            raise ValueError("JSON object for S3AuthCredentials is not a dictionary (%s)" % (type(s3_info)))
    else:
        raise ValueError("Requested to store workflow instance to S3 bucket but failed to provide "
                               "authentication credentials. Need to provide --s3AuthWithEnvVars OR "
                               "--s3AuthBearer64")

    missing_s3 = [k for k in s3_keys if not s3_info.get(k)]
    if missing_s3:
        raise ValueError('Missing S3 credential: %s' % ', '.join(missing_s3))

    # VV: Credentials have been provided, attempt to login to S3 endpoint
    s3_client = experiment.runtime.utilities.s3.S3Utility(
        access_key_id=s3_info['S3_ACCESS_KEY_ID'],
        secret_access_key=s3_info['S3_SECRET_ACCESS_KEY'],
        end_point=s3_info['S3_END_POINT'])

    return s3_client


def cb_parse_bool(option, opt, value, parser, inverse=False):
    """Callback to use with optparse.OptionParser which converts a string to a boolean and updates option in parser

    API: https://docs.python.org/3/library/optparse.html#optparse-option-callbacks

    Arguments:
       option(optparse.Option): The option item that is parsed
       opt(str): cmdline option name (e.g. --help, -h)
       value(str): value of cmdline option (optional)
       parser(optparse.OptionParser): the parser object
       inverse(bool): Whether to return the inverse of the boolean (Default: False)
    """
    try:
        ret = arg_to_bool(opt, value)
    except (TypeError, ValueError) as e:
        parser.error(e)
    else:
        ret = ret if inverse is False else not ret
        setattr(parser.values, option.dest, ret)


def arg_to_bool(name, val):
    """Converts a str value (positive, negative) to a boolean (case insensitive match)

    Arguments:
        name(str): Name of argument
        val(str): Can be one of the following (yes, no, true, false, y, n)

    Returns
        boolean - True if val is "positive", False if val is "negative

    Raises:
        TypeError - if val is not a string
        ValueError - if val.lower() is not one of [yes, no, true, false, y, n]
    """

    options_positive = ['yes', 'true', 'y']
    options_negative = ['no', 'false', 'n']
    if isinstance(val, string_types) is False:
        raise TypeError("Value of argument %s=%s is not a string but a %s" % (name, val, type(val)))

    val = val.lower()
    if val in options_negative:
        return False
    elif val in options_positive:
        return True
    else:
        raise ValueError("Value of argument %s=%s is not in %s" % (name, val, options_positive + options_negative))


def build_parser() -> NoSystemExitOptparseOptionParser:
    import pkg_resources

    # HACK: Daresbury system dependant
    projectDir = os.path.split(os.path.expanduser("~"))[0]
    haltfile = os.path.join(projectDir, 'shared/CHPCBackend/.halt_backend')
    killfile = os.path.join(projectDir, 'shared/CHPCBackend/.kill_backend')

    parser = NoSystemExitOptparseOptionParser(
        usage=usage, version=pkg_resources.get_distribution("st4sd-runtime-core").version, description=__doc__)

    launchOptions = optparse.OptionGroup(parser, "Launch Options")
    parser.add_option_group(launchOptions)

    restartGroup = optparse.OptionGroup(parser, "Restart Options - Use instead of launch options")
    parser.add_option_group(restartGroup)

    cdbGroup = optparse.OptionGroup(parser, "Centralized Database (CDB) Options - (used by memoization)")
    parser.add_option_group(cdbGroup)

    s3Group = optparse.OptionGroup(parser, "Options to store workflow instance to S3 bucket")
    available_package_types = [key for key in list(
        experiment.model.conf.ExperimentConfigurationFactory.format_map.keys())
                               if isinstance(key, string_types)]
    parser.add_option_group(s3Group)

    parser.add_option('', '--flowConfigPath', help=
    """Path to YAML file containing default flow configuration, can be overridden via the FLOWCONFIG_PATH 
    environment variable. The default is ~/.flow/config.yaml.
    The format of the file is: 
    {"default-arguments": [{"--someOption": "some value"}, {"-s": "some value for a short-form option"}]}
    """,
                      default=os.path.join(os.path.expanduser('~'), '.flow', 'config.yaml'), dest="flowConfigPath",
                      metavar="PATH")
    # VV: Leave the default values as None so that we can detect if the user has specified a value
    #     there's probably a smarter way to do this but it's good enough
    parser.add_option('--cwlFile', dest='cwlFile',
                      help='Override cwl main file - default main.cwl',
                      default=None)
    parser.add_option('--cwlJobOrderFile', dest='cwlJobOrderFile',
                      help='Override cwl job-order file. Set the option to '' to disable loading a job-order file '
                           '- default job_order.yml',
                      default=None)

    parser.add_option("--formatPriority", dest="formatPriority",
                      help="Comma separated list of configuration format priorities that "
                           "guides the selection of the Configuration parser which "
                           "will be used to read the Experiment configuration (Available options: %s)."
                           " - default '%%default'" % available_package_types,
                      default=','.join(experiment.model.conf.ExperimentConfigurationFactory.default_priority)
                      )
    parser.add_option("-l", "--logLevel", dest="logLevel",
                      help="The level of logging. Default %default",
                      type="int",
                      default=20,
                      metavar="LOGGING")
    parser.add_option("", "--hybrid", dest="hybrid",
                      help="Indicates the experiment will run in an LSF managed hybrid environment. "
                           "If given the value of this argument must be the default queue for the remote side of the environment. "
                           "NB: Must be specified for launch AND restart.",
                      default=None,
                      metavar="HYBRID")
    parser.add_option("", "--haltFile", dest="haltFile",
                      help="Name of file periodically polled to see if process should halt - default %default.",
                      default=haltfile,
                      metavar="HALTFILE")
    parser.add_option("", "--killFile", dest="killFile",
                      help="Name of file periodically polled to see if process should be killed - default %default.",
                      default=killfile,
                      metavar="KILLFILE")
    parser.add_option("", "--fuzzyMemoization", dest="fuzzyMemoization",
                      help="Set to Yes to enable fallback fuzzy-memoization for cases where hard memoization fails to "
                           "find suitable past components. Fuzzy memoization replaces dependencies to files generated "
                           "by components to dependencies to the producer component + the name of the file. Default "
                           "is No", action="callback", default=False, callback=cb_parse_bool, type=str)

    launchOptions.add_option("-f", "--failSafeDelays", dest="failSafeDelays",
                             help="Set to Yes to enable failsafe delays between a component becoming ready to run and "
                                  "actually running (6 seconds). Default is yes.", default=True, action="callback",
                             callback=cb_parse_bool, type=str)
    launchOptions.add_option("-i", "--inputs", dest="inputs",
                             help="A comma separated list of input files. Can be specified multiple times. "
                                  "Use $sourcePath:$targetName to rename resulting file in $INSTANCE_DIR/input "
                                  "directory. Use \\: to escape : if it is part of $sourcePath and \\\\ to escape \\",
                             action='append',
                             default=[],
                             metavar="INPUTS")
    launchOptions.add_option("-d", "--data", dest="data",
                             help="A path to a file with the same name as a file in the experiments data/ directory. "
                                  "The named file will replace the file in the data directory. "
                                  "Use $sourcePath:$targetName to rename resulting file in $INSTANCE_DIR/data "
                                  "directory. Use \\: to escape : if it is part of $sourcePath and \\\\ to escape \\",
                             action="append",
                             default=[],
                             metavar="DATA")
    launchOptions.add_option("-a", "--variables", dest="variables",
                             help="Path to a file that will be used to define instance specific variables",
                             default=None,
                             metavar="VARIABLES")
    launchOptions.add_option("-p", "--platform", dest="platform",
                             help="The platform the experiment is being deployed on. "
                                  "Use when an experiment support configurations for multiple platforms/systems",
                             # VV: Don't set a default value to tell apart from using the `default` platform and not specifying one
                             default=None,
                             metavar="PLATFORM")
    launchOptions.add_option("", "--instanceName", dest="instance_name",
                             help="Override the base name of the instance dir, can be"
                                  "combined with --nostamp. (default is <package>.instance)",
                             default=None,
                             metavar="INSTANCE_NAME")
    launchOptions.add_option("", "--nostamp", dest="stamp",
                             help="Do not add timestamp to instance names.",
                             action="store_false",
                             default=True,
                             metavar="NOSTAMP")
    launchOptions.add_option("", "--metadata", dest="user_metadata_file",
                             help="Path to YAML file containing user metadata (dictionary with strings as keys)."
                                  "The `user_metadata` dictionary is stored in the generated `elaunch.yaml` file "
                                  "under the field `userMetadata`",
                             default=None, metavar="USER_METADATA_FILE")
    launchOptions.add_option("-m", "", dest="user_metadata_dict",
                             help="Update user_metadata dictionary with `key:value` entry",
                             default=[], metavar="USER_METADATA_ENTRY", action="append")
    launchOptions.add_option('--enableOptimizer', dest='enable_optimizer', metavar="YES/NO",
                             action="callback", default=False, callback=cb_parse_bool, type=str,
                             help='Enable/disable workflowAttributes.repeatInterval optimizer (y/n). Default is No')

    launchOptions.add_option('--registerWorkflow', dest='registerWorkflow', metavar="YES/NO",
                             action="callback", default=False, callback=cb_parse_bool, type=str,
                             help='Enable/disable the registration of the workflow with the CDB - '
                                  'also required a valid --discovererMonitorDir value. Default is No')

    launchOptions.add_option('', '--discovererMonitorDir', dest='discoverer_monitor_dir',
                             help='Point elaunch.py to a directory which is monitored by some ExperimentDiscoverer '
                                  'daemon so that the resulting Experiment Instance is automatically registered '
                                  'to the centralized experiment instance database if --registerWorkflow is Yes',
                             default=None,
                             metavar='DISCOVERER_MONITOR_DIR')
    launchOptions.add_option('', '--ignoreTestExecutablesError', dest='ignoreTestExecutablesError',
                             metavar="YES/NO", action="callback", default=False, callback=cb_parse_bool, type=str,
                             help="Set to Yes to not treat issues during executable checking and resolution as errors. "
                                  "This may be useful in scenarios where a backend is unavailable (e.g. kubernetes, "
                                  "LSF, etc) but there exist cached memoization candidates available which can be used "
                                  "instead of executing a task. Default is No")
    launchOptions.add_option('', '--ignoreInitializeBackendError', dest='ignoreInitializeBackendError',
                             metavar="YES/NO", action="callback", default=False, callback=cb_parse_bool, type=str,
                             help="Set to Yes to not treat issues during backend initialization as errors. "
                                  "This may be useful in scenarios where a backend is unavailable (e.g. kubernetes, "
                                  "LSF, etc) but there exist cached memoization candidates available which can be used "
                                  "instead of executing a task. Default is No.")
    launchOptions.add_option('', '--noKubernetesSecurityContext', dest='no_k8s_security_context',
                             metavar="YES/NO", action="callback", default=False, callback=cb_parse_bool, type=str,
                             help="Set to No for flow to configure Pods that are generated by this workflow instance "
                                  "to use a spec.securityContext (which among other things, sets a non-root user id, "
                                  "non-root group id, and a fs-group identifier). This can be problematic for "
                                  "containers which expect to be run as root. Setting this to Yes will instruct "
                                  "flow to not inject a securityContext to pods it generates. Doing so can be "
                                  "considered a security risk and should be avoided. Default is No.")
    launchOptions.add_option("-s", "--applicationDependencySource", dest="application_dependency_source",
                             help="Point an application dependency to a specific "
                                  "path on the filesystem `applicationDependencyEntry:/path/to/new/source[:link/:copy]`"
                                  "The :link and :copy suffixes determine whether to link or copy the path under the "
                                  "instance directory. They suffix is optional and defaults to :link. If the path "
                                  "contains a ':' character, use '%3A' instead (i.e. url-encode the : character)",
                             default=[], metavar="APPLICATION_DEPENDENCY_SOURCE", action="append")
    launchOptions.add_option('', '--manifest', dest="manifest", metavar="PATH_TO_MANIFEST_FILE", default=None,
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

    launchOptions.add_option('--useMemoization', dest='useMemoization', metavar="YES/NO",
                             action="callback", default=False, callback=cb_parse_bool, type=str,
                             help='Enable/disable memoization of the workflow using other cached workflow instances '
                                  'which have been injected into the CDB in the past. '
                                  'also required a valid --mongoEndPoint value. Default is No')
    launchOptions.add_option('--kubernetesGarbageCollect', dest='kubernetesGarbageCollect',
                             choices=["all", "failed", "successful", "none"], default=None, type="choice",
                             help='Controls how to delete Job and Pod objects after Kubernetes task Completion.')
    launchOptions.add_option('--kubernetesArchiveObjects', dest='kubernetesArchiveObjects',
                             choices=["all", "failed", "successful", "none"], default=None, type="choice",
                             help=' Controls how to store the Job and Pod objects after Kubernetes task Completion.')
    # VV: Overriding means that we can detect whether the user provided a commandline or not, for the time being
    # we can configure --executionMode to just set --kubernetesGarbageCollect and --kubernetesArchiveObjects
    # and then only use --kubernetesArchiveObjects if it's not `None`
    launchOptions.add_option('--executionMode', dest='executionMode',
                             choices=['production', 'development', 'debug'], type="choice", default="debug",
                             help="Configures default options based on intended execution mode. For example, "
                                  "'debug' configures flow to never garbage collect Kubernetes objects and always"
                                  " archive them. "
                                  "'development' only garbage collects kubernetes objects for successful k8s tasks and"
                                  " always archives them. "
                                  "'production' garbage collects all kubernetes objects for k8s tasks and always "
                                  "archives them. "
                                  "Any command-line options you provide override the settings configured by "
                                  "--executionMode.")

    restartGroup.add_option("-r", "--restart", dest="restart",
                            help="Restart at given stage"
                                 "Note this option requires an existing instance is passed to the experiment NOT a package. "
                                 "If specified this option overides any option in the LAUNCH group. ",
                            type="int",
                            default=None,
                            metavar="RESTART")
    restartGroup.add_option("", "--restageData", dest="stageData",
                            help='If set to Yes specified components inputs (copy/links) will be staged on restart. '
                                 'The default is No, i.e. not to stage the existing staged versions of any files are used',
                            metavar="YES/NO", action="callback", default=False, callback=cb_parse_bool, type=str)
    restartGroup.add_option("", "--noRestartHooks", dest="useRestartHooks",
                            help='If set to Yes non-repeating components are started as in a normal run i.e '
                                 'to restart as if they processes exited due to ResourceExhausted. This will cause any '
                                 'restart hooks for the component to be executed. The default is Yes.',
                            metavar="YES/NO", action="callback", default=True, callback=cb_parse_bool, type=str,
                            callback_kwargs={"inverse": True})

    cdbGroup.add_option('--datastoreLabel', dest='datastoreLabel',
                        help='Name of the local datastore can be used to annotate document descriptors. '
                             'Default is None', default=None)
    cdbGroup.add_option('--mongoEndpoint', dest='mongoEndpoint',
                        help='MongoDB endpoint; can either be tcp://<ip>:<port> or http[s]://domain. `tcp://` '
                             'endpoints need also --mongo-user, and --mongo-password (possibly --mongo-database, '
                             '--mongo-collection, and --mongo-authsource)', default=None)
    cdbGroup.add_option('--mongoUser', dest='mongoUser',
                        help='Username to be used for MongoDB authentication (only necessary for tcp:// endpoints)',
                        default=None)
    cdbGroup.add_option('--mongoPassword', dest='mongoPassword',
                        help='Password to be used for MongoDB authentication (only necessary for tcp:// endpoints)',
                        default=None)
    cdbGroup.add_option('--mongoAuthsource', dest='mongoAuthSource',
                        help='Database name to authenticate (may only be specified for for tcp:// endpoints)',
                        default=None)
    cdbGroup.add_option('--mongoDatabase', dest='mongoDatabase',
                        help='Database that hosts MongoDB items (may only be specified for for tcp:// endpoints)',
                        default=None)
    cdbGroup.add_option('--mongoCollection', dest='mongoCollection',
                        help='Collection to be used with MongoDB database '
                             '(may only be specified for for tcp:// endpoints)',
                        default=None)
    cdbGroup.add_option('--cdbRegistry', dest='cdbRegistry', help='Endpoint of CDB registry can either '
                                                                  'be tcp://<ip>:<port> or http[s]://domain',
                        default=None)

    s3Group.add_option('--s3StoreToURI', dest='s3StoreToURI',
                       help="S3 URI (i.e. s3://<bucket-name>/path/to/folder) under which the entire workflow instance "
                            "directory will be updated (with the exception of folders/files that are symbolic links). "
                            "You must also specify a valid --s3AuthWithEnvVars OR --s3AuthBearer64 parameter. "
                            "%(instanceDir)s will be replaced by the name of the instance folder",
                       default=None)
    s3Group.add_option('--s3AuthWithEnvVars', dest='s3AuthWithEnvVars',
                       help='Authenticate to S3 end-point using the information from the environment variables: '
                            'S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_END_POINT', default=False, action='store_true')
    s3Group.add_option('--s3AuthBearer64', dest='s3AuthBearer64',
                       help='Authenticate to S3 end-point using the information provided in this option. This '
                            'value is expected to be a valid JSON dictionary with the keys S3_ACCESS_KEY_ID, '
                            'S3_SECRET_ACCESS_KEY, S3_END_POINT. The string representing the JSON dictionary '
                            'is expected to be base64 encoded.', default=None)

    return parser


def report_error(instance_path: str | None, error_description: str):
    """Generates a Status report in @instance_path/output/status.txt with @error_description

    Use this method when there is no other way to load/create the instance directory at @instance_path

    Args:
        instance_path: Path to the instance directory which will hold the status report
        error_description: A human readable description of what went wrong
    """
    if instance_path is None:
        try:
            instance_path = os.environ["INSTANCE_DIR_NAME"]
        except KeyError as e:
            try:
                logger = rootLogger
            except NameError:
                logger = logging.getLogger("root")
                logger.setLevel(logging.INFO)
            instance_path = tempfile.mktemp(suffix=".instance", dir=".")
            logger.warning("Unable to deduce instance path name and environment variable INSTANCE_DIR_NAME is "
                               f"not set. Will generate an instance directory with the random name {instance_path}")

    # VV: Imitate elaunch.py generating the @instance_path/output/status.txt file then populate it with an error
    output_dir = os.path.join(instance_path, "output")

    try:
        os.makedirs(output_dir)
    except FileExistsError:
        pass

    path = os.path.join(output_dir, "status.txt")
    status_file = experiment.model.data.Status(path, data={}, stages=[])
    # VV: The experiment never actually finished
    status_file.setExperimentState(experiment.model.codes.FAILED_STATE)
    status_file.setExitStatus('Failed')
    status_file.setErrorDescription(str(error_description))
    status_file.update()
    status_file.writeToStream(sys.stderr)


if __name__ == "__main__":

    usage = "usage: %prog [options] [package]"
    parser = build_parser()

    # VV: Group together triggers to the discovererMonitor (CDB reporter) that stem from workflow instance
    report_id = str(uuid.uuid4())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    # VV: In LSF, when a Job exceeds the runtime limit it receives a SIGUSR2 10 minutes before it gets terminated.
    # We want elaunch.py to treat SIGUSR2 as the user asking it to cleanly shutdown so that a user can run it as a LSF
    # job. When elaunch.py terminates due to runtime limit then it will shut down any running jobs and wait for them
    # to complete. At a later time, a user may restart the experiment instance via the `--restart` argument.
    signal.signal(signal.SIGUSR2, signal_handler)
    signal.signal(signal.SIGUSR1, lambda _signal, frame: code.interact())

    cmdline_args = sys.argv.copy()

    try:
        options, args = parser.parse_args()
        opts_only_cmdline = options
    except (BaseException, optparse.OptParseError, SystemExit) as e:
        if isinstance(e, SystemExit):
            # VV: This is triggered when users run `elaunch.py --help`
            if e.code == 0:
                raise
        if isinstance(e, optparse.OptParseError):
            msg = f"During argument parsing of sys-args, ran into {type(e).__name__} {str(e)}"
        else:
            msg = f"During argument parsing of sys-args {sys.argv}, ran into {type(e).__name__} {e}"
        print(f"{msg} - will try to generate error report", file=sys.stderr)
        report_error(None, msg)
        raise

    # VV: Elaunch.py supports the definition of default cli-arguments in a file with the format:
    # default-arguments:
    #  - --argumentLong: some value
    #  - -s: some value for a short-form argument
    # These arguments are treated as if the user typed them right after `elaunch.py`
    # (in the order they appear in the list)
    # also keeps track of eventual cmdline args to record them in elaunch.yaml too
    flow_config_path = os.environ.get('FLOWCONFIG_PATH', options.flowConfigPath)

    try:
        if flow_config_path and os.path.isfile(flow_config_path):
            flow_config = yaml.load(open(flow_config_path, 'r'), yaml.SafeLoader)
            print("Parsing flowConfig at %s:" % flow_config_path)
            print(yaml.dump(flow_config))

            dict_default_args = flow_config.get('default-arguments', [])  # type: List[Dict[str, str]]
            if dict_default_args:
                default_args = []
                for many in dict_default_args:
                    for value in many:
                        default_args.extend((value, str(many[value])))
                cmdline_args = default_args + sys.argv[1:]

                # VV: Need to build a new parser object to get rid of any options it may have parsed the 1st time
                # (e.g. if user provided --inputs the old parser object will append these twice into options.inputs)
                parser = build_parser()

                options, args = parser.parse_args(cmdline_args)
                # VV: finally insert the executable at the beginning of the cmdline
                cmdline_args.insert(0, sys.argv[0])
    except (BaseException, optparse.OptParseError) as e:
        msg = f"During argument parsing of file {flow_config_path}, ran into {type(e).__name__} {str(e)}"
        print(f"{msg} - will try to generate error report", file=sys.stderr)
        instance_path: str | None = opts_only_cmdline.instance_name

        if instance_path is None and len(args) == 1:
            instance_path = args[0]
            instance_path = os.path.splitext(instance_path)[0]
            instance_path = f"{instance_path}.instance"

        if instance_path is not None and os.path.exists(instance_path):
            instance_path = None

        report_error(instance_path, msg)
        raise

    #Set umask to 0002 to allow group write
    os.umask(0o002)

    # Turn of exception raising from logging commands
    # This stops IOErrors/OSErrors during logging e.g. due to fs issues
    # from crashing the program
    logging.raiseExceptions = False

    FORMAT = '%(levelname)-9s %(threadName)-30s %(name)-30s: %(funcName)-20s %(asctime)-15s: %(message)s'
    logging.basicConfig(format=FORMAT)
    logging.getLogger().setLevel(options.logLevel)
    rootLogger, setupLogFile = point_logger_to_random_file()

    rootLogger.info('Using modules in %s' % (os.path.split(experiment.runtime.control.__file__)[0]))

    # VV: Potentially override what --executionMode defines
    orig_k8s_garbage_collect = options.kubernetesGarbageCollect
    orig_k8s_archive_objects = options.kubernetesArchiveObjects

    if options.executionMode == "debug":
        options.kubernetesGarbageCollect = orig_k8s_garbage_collect or "none"
        options.kubernetesArchiveObjects = orig_k8s_archive_objects or "all"
    elif options.executionMode == "development":
        options.kubernetesGarbageCollect = orig_k8s_garbage_collect or "successful"
        options.kubernetesArchiveObjects = orig_k8s_archive_objects or "all"
    elif options.executionMode == "production":
        options.kubernetesGarbageCollect = orig_k8s_garbage_collect or "all"
        options.kubernetesArchiveObjects = orig_k8s_archive_objects or "all"

    rootLogger.info(f"Configured runtime for \"{options.executionMode}\" execution mode: "
                    f"kubernetesGarbageCollect={options.kubernetesGarbageCollect}, "
                    f"kubernetesArchiveObjects={options.kubernetesArchiveObjects}")

    options.formatPriority = options.formatPriority.split(',')

    # VV: If the user asked for the workflow to not be registered then clear --discovererMonitorDir
    if options.registerWorkflow is False:
        options.discoverer_monitor_dir = None
    elif options.discoverer_monitor_dir is None:
        parser.error("--registerWorkflow is set but --discovererMonitorDir is not configured")

    # VV: Ditto but for --useMemoization and --mongoEndpoint
    if options.useMemoization is False:
        options.mongoEndpoint = None
    elif options.mongoEndpoint is None:
        parser.error("--useMemoization is set but --mongoEndpoint is not configured")

    if options.variables and options.restart is not None:
        rootLogger.critical("User variables when restarting is not supported. Please edit the instance files.")
        sys.exit(1)

    # VV: Automatically inject 'cwl' if elaunch is asked to load a cwl file or a job order file
    if options.cwlFile is not None or options.cwlJobOrderFile is not None:
        if 'cwl' not in options.formatPriority:
            options.formatPriority.insert(0, 'cwl')
        if options.hybrid is not None:
            rootLogger.critical('Cannot use --hybrid when launching CWL experiments')
            sys.exit(1)

    if options.failSafeDelays is False:
        rootLogger.info("Disabling artificial failSafe delays (--failSafeDelays=no command-line argument detected)")
        experiment.runtime.engine.ENGINE_RUN_START_DELAY_SECONDS = 0.0
        experiment.runtime.engine.ENGINE_LAUNCH_DELAY_SECONDS = 0.0
    #Disable some loggers
    l = logging.getLogger("utilities.comms")
    l.setLevel(40)

    if len(args) > 1:
        rootLogger.critical("Too many arguments - exiting")
        sys.exit(1)

    if len(args) == 0:
        rootLogger.critical("No package/instance specified - exiting")
        sys.exit(1)

    def should_run(seconds_waiting, cancelEvent, repeat_time):
        if cancelEvent.is_set() or (seconds_waiting >= repeat_time):
            return True

        return False

    #Stop monitor - this is so backend maintainers can kill the process
    cancelHaltMon = threading.Event()
    CheckHalt = CreateHaltClosure(options.haltFile)
    haltMonitor = experiment.runtime.monitor.CreateMonitor(60,
                                                           CheckHalt, cancelHaltMon,
                                                           name="HaltMonitor")

    haltMonitor()

    #Use same event to cancel as halt monitor
    CheckKill = CreateHaltClosure(options.killFile, sendSignal=signal.SIGKILL)
    killMonitor = experiment.runtime.monitor.CreateMonitor(60,
                                                           CheckKill, cancelHaltMon,
                                                           name="KillMonitor")
    killMonitor()

    packagePath = args[0]

    #TEMPORARY: Initialse QCloudConfiguration object with info on config
    #This will be replaced by a generic call to get the resources assigned to the given application type
    #qconf = experiment.appenv.QCloudConfiguration.newDefaultConfiguration(subPort=options.qsubPort,
    #            queryPort=options.qqueryPort)

    #Initialse a HybridConfiguration object if necessary.
    #If not initialised directly then the defaultConfiguration will describe an non-hybrid env
    if options.hybrid is not None:
        experiment.appenv.HybridConfiguration.newDefaultConfiguration(isHybrid=True,
                remoteQueue=options.hybrid)

    options.inputs = [el.split(',') for el in options.inputs]
    options.inputs = [el.strip() for el in itertools.chain(*options.inputs)]
    options.data = [el.strip() for el in options.data]

    #Any error in setup is fatal
    #If the error occurred during instantiation of the Experiment object
    #we can't update the status file e.g. as it may not exist
    compExperiment = None
    s3_client = None  # type: Optional[experiment.runtime.utilities.s3.S3Utility]
    setup_error = None
    statusMonitor = None
    outputAgent = None

    try:
        rootLogger.info("Deploying Experiment at %s " % packagePath)
        setup_error, compExperiment, outputAgent, statusMonitor = Setup(packagePath, options)
    except KeyboardInterrupt as e:
        setup_error = e
        rootLogger.critical('Received keyboard interrupt/signal - exiting')
        rootLogger.log(19, traceback.format_exc())
        initialization_error = "Killed by user signal"
    except Exception as error:
        setup_error = error
        rootLogger.info(traceback.format_exc())
        rootLogger.warning("Error during setup %s" % error)
        initialization_error = error

    if compExperiment:
        # VV: The experiment may be flawed but we've created an instance dir for it, point the rootLogger to the dir
        try:
            rootLogger, logFile = point_logger_to_logfile(compExperiment.instanceDirectory, setupLogFile)
        except Exception:
            pass

    if isinstance(setup_error, experiment.model.errors.ExperimentSetupError):
        rootLogger.warning("Encountered setup error: %s" % str(setup_error))
        initialization_error = setup_error
    elif isinstance(setup_error, experiment.model.errors.ExperimentInvalidConfigurationError):
        underlying = setup_error.underlyingError

        if isinstance(underlying, list):
            rootLogger.warning("Encountered when loading configuration:")
            for err in underlying:
                rootLogger.warning("  %s: %s" % (type(err), str(err)))
            rootLogger.warning("Discovered %d experiment configuration errors" % len(underlying))
        else:
            rootLogger.warning("Encountered error when loading configuration: %s" % underlying)
        initialization_error = underlying
    else:
        initialization_error = setup_error

    if compExperiment is not None:
        rootLogger.info("Status after deployment:")

        with compExperiment.instanceDirectory.mtx_output:
            if initialization_error:
                if isinstance(initialization_error, BaseException):
                    error_description = f"{type(initialization_error).__name__} {initialization_error}"
                else:
                    error_description = str(initialization_error)
                compExperiment.statusFile.setErrorDescription(error_description)
            compExperiment.statusFile.update()

    # VV: Load the environment variables that define the size of worker pools and other Orchestrator settings
    # do this after creating the instance (to preserve the error) but before initializing the components
    if initialization_error is None:
        try:
            experiment.settings.load_settings_orchestrator()
        except experiment.model.errors.EnhancedException as e:
            initialization_error = e

    if initialization_error is not None or compExperiment is None:
        rootLogger.warning("Error detected, will now terminate")
        instance_dir = None  # type: Optional[experiment.model.storage.ExperimentInstanceDirectory]
        if compExperiment is None:
            try:
                expPackage = experiment.model.storage.ExperimentPackage.packageFromLocation(
                    packagePath, manifest=options.manifest, platform=options.platform)
                instance_dir = experiment.model.storage.ExperimentInstanceDirectory.newInstanceDirectory(
                    os.getcwd(), expPackage, options.stamp, options.instance_name)
            except Exception as e:
                rootLogger.warning("Unable to create instance to store status.txt for failed-to-start experiment."
                                   "Error: %s" % e)
        else:
            instance_dir = compExperiment.instanceDirectory

        try:
            if instance_dir:
                _ = point_logger_to_logfile(instance_dir)
            if isinstance(initialization_error, BaseException):
                error_description = f"{type(initialization_error).__name__} {initialization_error}"
            else:
                error_description = str(initialization_error)
            report_error(instance_dir.location if instance_dir else None, error_description)
            if instance_dir:
                instance_dir.consolidate()
        except Exception as e:
            rootLogger.log(15, traceback.format_exc())
            rootLogger.warning("Unable to generate status.txt for failed-to-start experiment."
                               "Error: %s" % e)

        cancelHaltMon.set()
        sys.exit(1)

    statusDatabase = None
    controller = None

    try:
        # VV: Create all components which belong to a stage that's >= initial_stage.
        #     This way we can inspect them and discover their dependencies
        graph = compExperiment.graph

        components = {}  # type: Dict[int, Set[experiment.runtime.workflow.ComponentState]]
        # VV: Fill graph with all ComponentState but exclude all stages before the initial one when restarting
        initial_stage = options.restart
        if initial_stage is None:
            initial_stage = 0

        assert 0 <= initial_stage < len(compExperiment._stages)

        # VV: Executables have already been checked via compExperiment.validateExperiment() during Setup()
        generate_components(components, compExperiment, initial_stage, check_executables=False, verbose=False)

        restartOptions = None
        if options.restart is not None:
            # VV: Before restarting the experiment, clear the error description
            compExperiment.statusFile.removeErrorDescription()

            restartOptions = {'startStage': options.restart,
                              'restageData': options.stageData,
                              'restartSources': options.useRestartHooks}
            restart = True
            startStage = restartOptions['startStage']
        else:
            startStage = 0
            restart = False
        do_stage_data = {}  # type: Dict[int, bool]
        do_restart_sources = {}  # type: Dict[int, bool]
        for s in compExperiment._stages:
            do_stage_data[s.index] = True if (restart is False or s.index != startStage) else restartOptions[
                'restageData']
            do_restart_sources[s.index] = False if (restart is False or s.index != startStage) else restartOptions[
                'restartSources']

        cdb_cli = None

        if options.mongoEndpoint:
            registry = options.cdbRegistry  # type: str
            if registry is not None and registry.startswith('tcp://'):
                registry = registry[6:]

            mongo_params = {
                'cdb_registry_url': registry,
                'consumable_computing_rest_api_url': None,
                'max_retries': 3,
                'secs_between_retries': 5,
            }
            if options.mongoEndpoint.startswith('tcp://'):
                host, port = options.mongoEndpoint.rsplit(':', 1)
                host = host[6:]
                mongo_params['cdb_rest_url'] = None
                mongo_params['mongo_host'] = host
                mongo_params['mongo_port'] = int(port)
                if options.mongoUser:
                    mongo_params['mongo_user'] = options.mongoUser
                if options.mongoPassword:
                    mongo_params['mongo_password'] = options.mongoPassword
                if options.mongoAuthSource:
                    mongo_params['mongo_authsource'] = options.mongoAuthSource
                if options.mongoDatabase:
                    mongo_params['mongo_database'] = options.mongoDatabase
                if options.mongoCollection:
                    mongo_params['mongo_collection'] = options.mongoCollection
            elif options.mongoEndpoint.startswith('http://') or options.mongoEndpoint.startswith('https://'):
                mongo_params['cdb_rest_url'] = options.mongoEndpoint
            else:
                raise ValueError('--mongo-endpoint should start with either tcp:// or http[s]:// instead it was %s' %
                                 options.mongoEndpoint)

            cdb_cli = experiment.service.db.ExperimentRestAPI(**mongo_params)

        controller = experiment.runtime.control.Controller(
            compExperiment, do_stage_data, do_restart_sources, experiment_name=compExperiment.instanceDirectory.location,
            enable_optimizer=options.enable_optimizer, cdb=cdb_cli, memoization_fuzzy=options.fuzzyMemoization
        )

        rootLogger.info('Starting Experiment')
        # Create the component status database
        statusDatabase = experiment.runtime.status.StatusDB(location=compExperiment.instanceDirectory.statusDatabaseLocation)
        # VV: Causes StatusMonitor to periodically generate `output/status_details.json`, this file conntains
        # information which statusDatabase.._pretty_print_status_details_dictionary() can use to generate a string
        # that's identical to what "einspect.py -f all  -c producers -c task -c engine" would print to its output
        statusMonitor.set_status_database(status_database=statusDatabase)

        # VV: Setup the live-patching signal handler and then store the elaunch.py pid on the disk
        LivePatcher.comp_experiment = compExperiment
        LivePatcher.controller = controller
        LivePatcher.output_agent = outputAgent
        LivePatcher.mon_status = statusMonitor
        LivePatcher.components = components
        LivePatcher.initial_stage = initial_stage
        LivePatcher.format_priority = list(options.formatPriority)
        LivePatcher.platform_name = options.platform or experiment.model.frontends.flowir.FlowIR.LabelDefault
        LivePatcher.custom_application_sources = extract_application_dependency_source(options)

        signal.signal(signal.SIGALRM, lambda _signal, frame: live_patch_trigger())

        # VV: Right now we only have 1 variables file, but there's no reason to limit ourselves to just 1
        user_variables = {}
        if options.variables:
            try:
                out_errors = []
                read_user_variables = experiment.model.conf.FlowIRExperimentConfiguration.read_user_variables
                user_variables = read_user_variables(options.variables, out_errors)
                if len(out_errors) != 0:
                    user_variables = {}
                    rootLogger.critical("Failed to fetch user variables. Error(s): %s" % list(map(str, out_errors)))
            except Exception as e:
                rootLogger.critical("Failed to fetch user variables. Error: %s" % e)

        user_metadata = {}

        if options.user_metadata_file:
            with open(options.user_metadata_file, 'r') as f:
                user_metadata = experiment.model.frontends.flowir.yaml_load(f)

        metadata_errors = False
        for s in options.user_metadata_dict:
            try:
                key, value = s.split(':', 1)
            except Exception as e:
                rootLogger.critical("Cannot parse metadata entry \"%s\" "
                                    "(does not follow format label:value). Exception: %s" % (s, e))
                metadata_errors = True
            else:
                user_metadata[key] = value

        if metadata_errors:
            raise ValueError("Invalid metadata entries")

        ValidateOptional = experiment.model.frontends.flowir.ValidateOptional

        def validate_any(obj):
            return True

        user_metadata_schema = {ValidateOptional(str): validate_any}
        errors = experiment.model.frontends.flowir.validate_object_schema(user_metadata, user_metadata_schema, 'user-metadata')

        if errors:
            rootLogger.critical("Invalid user metadata:")
            for err in errors:
                rootLogger.critical("  %s" % err)

            raise ValueError("Invalid user metadata")

        reserved = experiment.model.data.Experiment.ReservedDocumentDescriptorKeys
        illegal_keys = [key for key in user_metadata if key in reserved]

        if illegal_keys:
            msg = "Metadata contents contains forbidden keys %s (keys cannot be %s)." % (illegal_keys, reserved)
            rootLogger.critical(msg)
            raise ValueError(msg)

        # VV: Get a collection of global/stage variables that the workflow instance uses (includes those defined in the
        # package definition as well as those defined by the user - user variables override those from the package).
        # This information will also end up in the `metadata.variables` field of the `experiment` documentDescription
        flowir = compExperiment.experimentGraph.configuration.get_flowir_concrete(return_copy=True)
        actual_variables = flowir.get_workflow_variables()
        # VV: 'global' userMetadata variables end up becoming `platform-stage` variables (this is the second most
        # high priority scope right after variables defined by components). As a result, we need to backpatch them
        # into the `actual_variables[lbl_global]` dictionary
        lbl_global = experiment.model.frontends.flowir.FlowIR.LabelGlobal
        global_variables = actual_variables.get(lbl_global, {}).copy()
        global_user_variables = user_variables.get(lbl_global, {}).copy()
        global_variables.update(global_user_variables)
        actual_variables[lbl_global] = global_variables
        del flowir

        elaunch_state = experiment.model.storage.generate_elaunch_metadata_dictionary(
            platform=options.platform or experiment.model.frontends.flowir.FlowIR.LabelDefault, pid=os.getpid(),
            cmdline=' '.join(cmdline_args), inputs=options.inputs, data=options.data, user_variables=user_variables,
            variables=actual_variables, version=parser.version, hybrid_platform=options.hybrid,
            user_metadata=user_metadata, instance_name=os.path.split(compExperiment.instanceDirectory.location)[1]
        )

        with open(os.path.join(compExperiment.instanceDirectory.location, 'elaunch.yaml'), 'w') as f:
            experiment.model.frontends.flowir.yaml_dump(elaunch_state, f, default_flow_style=False)

        # VV: Ask experimentDiscoverer to register the experiment *after* `elaunch.yaml` is generated so that metadata
        #     variables/etc are picked up by the experimentDiscoverer (aka experiment reporter)
        # VV: We could figure out a way to toggle on/off monitoring of experiments while they're running

        if compExperiment and options.discoverer_monitor_dir:
            if os.path.exists(options.discoverer_monitor_dir) is False:
                # VV: No need to modify the dir permissions because umask()
                os.mkdir(options.discoverer_monitor_dir)

            output_file = os.path.join(options.discoverer_monitor_dir, '%s-begin.json' % report_id)
            temp_file = os.path.join(options.discoverer_monitor_dir, '%s-begin._temp' % report_id)

            # VV: Write to a temp file and then rename it to .json (Discoverer is looking for .json files)
            with open(os.path.join(temp_file), 'w') as f:
                json.dump({'experiment-location': compExperiment.instanceDirectory.location,
                           'platform': options.platform or experiment.model.frontends.flowir.FlowIR.LabelDefault}, f)

            os.rename(temp_file, output_file)

        Run(compExperiment,
            outputAgent,
            statusMonitor,
            controller,
            statusDatabase,
            restartOptions=restartOptions,
            discovererMonitorDir=options.discoverer_monitor_dir,
            discovererReportId=report_id,
            platform_to_use=options.platform)

        # VV: The experiment is over! we can extract and store the measured properties now
        interface = compExperiment.configuration.get_interface()

        if interface is not None:
            rootLogger.info("The experiment has an interface, extracting the measured properties now")
            try:
                do_interface = experiment.model.hooks.interface.Interface(interface, "FlowIR.interface")
                df: pandas.DataFrame | None = do_interface.extract_properties(
                    exp=compExperiment,
                    input_ids=compExperiment.get_input_ids())

                if df is not None:
                    compExperiment.set_measured_properties(df, True)
                    del df
                del do_interface
            except Exception as e:
                rootLogger.warning(f"Interface could not extract properties due to {e} - will ignore error, "
                                   f"virtual experiment properties may be corrupted or incomplete.")
                rootLogger.log(15, f"Traceback is ignored: {traceback.format_exc()}")

        compExperiment.statusFile.setExitStatus('Success')
    except experiment.runtime.errors.StageFailedError as error:
        rootLogger.critical(traceback.format_exc())
        rootLogger.critical('Detected unrecoverable ERROR while running experiment')
        rootLogger.warning(error)
        compExperiment.statusFile.setExitStatus('Failed')
        compExperiment.statusFile.setErrorDescription(str(error))
        sys.exit(1)
    except KeyboardInterrupt:
        rootLogger.critical(traceback.format_exc())
        compExperiment.statusFile.setExitStatus('Stopped')
        compExperiment.statusFile.setErrorDescription('Killed by user signal')
        rootLogger.critical('Received keyboard interrupt/signal - exiting')
        sys.exit(1)
    except Exception as error:
        rootLogger.critical(traceback.format_exc())
        #Should not get an uncategorised exception here
        #we add a handler in case of programming error
        rootLogger.critical('Caught unexpected exception - exiting. Exception data ', error)
        compExperiment.statusFile.setExitStatus('Failed')
        compExperiment.statusFile.setErrorDescription('Unexpected exception reached top level\n.%s' % str(error))
        #Because unexpected reraise error so we get traceback
        raise_with_traceback(error)
    finally:
        time_completed = datetime.datetime.now()

        #NOTE: Stop the halt/kill monitor - Unfortunately this means we can't kill/halt after this point
        rootLogger.info('Clean-up - Stopping halt monitor')
        cancelHaltMon.set()
        if statusMonitor:
            rootLogger.info('Clean-up - Stopping status monitor')
            statusMonitor.kill()
        rootLogger.info('Clean-up - Stopping exception tracker')
        rootLogger.info('Clean-up - Calling controller cleanup')
        if controller is not None:
            controller.cleanUp()

        rootLogger.info('Clean-up - Checking filesystem stability before writing')
        if experiment.runtime.monitor.MonitorExceptionTracker.defaultTracker().isSystemStable(120) is False:
            rootLogger.warning("Clean-Up - Detected file system instability during finalise stage")
            rootLogger.warning("Clean-Up - Will wait 30secs before finishing ...")
            time.sleep(30.0)

        rootLogger.info("Clean-up - Exception info on exit")
        experiment.runtime.monitor.MonitorExceptionTracker.defaultTracker().printStatus()

        if controller:
            rootLogger.info("Clean-up - Waiting for all components to terminate")
            controller_join = threading.Event()

            def controller_term(rx_error=None, event=controller_join):
                try:
                    if rx_error is None:
                        rootLogger.info("All observables completed successfully")
                    else:
                        rootLogger.info(f"Some observables may not have completed successfully, "
                                        f"observed error {rx_error} - elaunch will assume all "
                                        f"Component observables have completed")
                finally:
                    event.set()

            controller.workflowIsComplete.subscribe(on_completed=controller_term, on_error=controller_term)

            controller_join.wait()

        # VV: The statusDB itself has a thread processing transactions, it sets event_finished when the thread stops
        # updating the status database, wait 10 minutes for it to terminate before waiting for daemon threads to stop
        if statusDatabase is not None:
            rootLogger.info("Clean-up - Shutting down component status database")
            statusDatabase.close()
            rootLogger.info("Clean-up - Waiting 10 minutes for async status db shutdown to process")
            statusDatabase.event_finished.wait(10 * 60)
        else:
            rootLogger.warning("Clean-up - Status database was never initialised")

        rootLogger.info('Clean-up - Setting final status')

        # Final status update
        if statusMonitor is not None:
            # VV: Make sure that the satus monitor has finished pushing updates, if there's no Controller
            # there're no updates and the StatusMonitor will never figure out that there's nothing to do
            rootLogger.info("Clean-up - Waiting for StatusMonitor to terminate")
            if controller is not None:
                statusMonitor.join()

        # VV: Don't let output/status monitor write to `output` while we're writing status.txt/consolidating
        with compExperiment.instanceDirectory.mtx_output:
            rootLogger.info("Clean-up - Updating status_details.json file")
            statusMonitor.try_generate_status_details()

            # Final status update
            rootLogger.info('Clean-up - Updating status file')
            compExperiment.statusFile.setCompleted(time_completed)
            compExperiment.statusFile.setUpdated(time_completed)
            compExperiment.statusFile.setExperimentState(experiment.model.codes.FINISHED_STATE)
            compExperiment.statusFile.persistentUpdate()
            rootLogger.info("Clean-up - Final status was")
            try:
                # Change to logToStream and use logging module
                compExperiment.statusFile.writeToStream(sys.stderr)
            except Exception as error:
                rootLogger.critical(traceback.format_exc())
                rootLogger.warning("Unable to write status: %s" % error)

            # Consolidate storage (some things may have been stored in temporary storage)
            rootLogger.info("Clean-up - Consolidating")
            try:
                compExperiment.instanceDirectory.consolidate()
            except Exception as e:
                rootLogger.critical(traceback.format_exc())
                rootLogger.critical("Unable to consolidate shadow-dir files")

        if options.discoverer_monitor_dir:
            # VV: Trigger the CDB daemon to upsert the experiment document *after* shadow dir consolidation
            # this is to ensure that the daemon is able to properly generate `output` and `status` which
            # require access to contents of the shadow dir folder `output`
            try:
                num_stages = compExperiment.numStages()
                exp_docs = [compExperiment.generate_experiment_document_description(options.datastoreLabel)]
            except Exception:
                num_stages = 9999
                exp_docs = None

            try:
                trigger_discoverer(options.discoverer_monitor_dir, compExperiment.instanceDirectory.location,
                                   report_id, num_stages+1, finished_components=[],
                                   platform=options.platform, upsert_documents=exp_docs)
            except Exception as e:
                rootLogger.warning("Cannot generate trigger event for discovererMonitor due to %s - will ignore" % e)

        rootLogger.info("Clean-up - Clean-up complete")
