#
# coding=UTF-8
# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

'''Classes/functions for checking/storing experiment status

Also handle restarting experiment'''
from __future__ import print_function, annotations

import json
import logging
import operator
import os
import threading
import traceback
import uuid
import weakref
from functools import reduce
from threading import RLock

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Tuple


#Experiment modules
import experiment.model.codes
import experiment.model.conf
import experiment.model.errors
import experiment.model.storage
import experiment.model.data
import experiment.model.executors
import experiment.model.graph
import experiment.model.frontends.flowir
import experiment.runtime.control
import experiment.runtime.errors
import experiment.runtime.monitor
import experiment.runtime.utilities.s3


if TYPE_CHECKING:
    from experiment.model.data import Experiment
    from experiment.runtime.workflow import ComponentState
    WeakRefComponentState = Callable[[], experiment.runtime.workflow.ComponentState]


class StatusMonitor:

    '''Class which monitors the experiment status.

    Attributes:
        status jobs: A list of jobs, one per stage, that monitor.'''

    defaults = {'job-type':'local', 'repeat-interval':'1.0'}

    def _initJobs(self, status_report):
        # type: (Dict[str, Dict[str, str]]) -> None
        '''Private methods. Creates the status jobs'''

        for stage in status_report:
            self.log.debug('Creating status monitor for stage %d' % stage)

            if 'executable' in status_report[stage]:
                executable = status_report[stage]['executable']
                try:
                    arguments = status_report[stage]['arguments']
                except KeyError as e:
                    arguments = ''

                references = status_report[stage].get('references', [])

                for str_ref in references:
                    ref = experiment.model.graph.DataReference(str_ref)
                    if ref.method not in [experiment.model.graph.DataReference.Ref]:
                        raise experiment.model.errors.DataReferenceInconsistencyError(
                            ref, 'Status command %s %s uses an unsupported dataReference type %s' % (
                                executable, arguments, ref.method
                        ))
                    location = ref.location(self.weakExperiment().experimentGraph)

                    arguments = arguments.replace(ref.stringRepresentation, location)

                command = experiment.model.executors.Command(
                    executable,
                    arguments=arguments,
                    useCommandEnvironment=False,
                    basePath=self.experiment.instanceDirectory.location
                )

            else:
                command = None
            stage_index = experiment.model.frontends.flowir.FlowIR.stage_identifier_to_stage_index(stage)
            stage_name = 'stage%d' % stage_index
            self.commands[stage_name] = command

    def __init__(self, experimentInstance, report_components=True):
        # type: ("experiment.model.data.Experiment", bool) -> None
        '''Initialises a StatusMonitor for the experiment

        Arguments:
            experimentInstance(experiment.model.data.Experiment): Experiment instance
            report_components(bool): If set to true will also print to stdout report of running/pending/finished
                components

        Exceptions:
           experiment.model.errors.ExperimentInvalidConfigurationError'''

        self.repeatInterval = 30.0

        # VV: If set, will also generate an $INSTANCE_DIR/output/status_details.json file. It contains the dictionary
        # which experiment.runtime.status.StatusDB.get_status_details() produces. This is what einspect.py uses under the hood
        # to print its output.
        self._status_database = None  # type: "Optional[experiment.runtime.status.StatusDB]"
        self.last_status_report = None
        self.report_components = report_components

        #Stage like ivars
        self.weakExperiment = weakref.ref(experimentInstance)  # type: Callable[[], experiment.model.data.Experiment]
        self.log = logging.getLogger("output.statusmonitor")

        directory = self.experiment.instanceDirectory.resolvePath("output")
        #Note: Remove possible symlinks for directory to avoid stale file-handles
        #FIXME: There will be a problem here when consolidate is called ...
        directory = os.path.realpath(directory)
        directory = os.path.abspath(directory)
        self.outputDir = experiment.model.storage.WorkingDirectory(directory)
        self.outputFile = os.path.join(self.outputDir.path, "status.txt")

        # VV: Re-entrant lock to guard both compute_stage_status and CheckStatus
        self.mtx_compute_status = RLock()

        self.updates = {}

        self.commands = {}  # type: Dict[str, experiment.model.executors.Command]

        concrete = experimentInstance.experimentGraph.configuration.get_flowir_concrete(return_copy=False)
        status_report = concrete.get_status()

        try:
            self._initJobs(status_report)
        except (
        experiment.model.errors.DataReferenceFormatError, experiment.model.errors.DataReferenceFilesDoNotExistError) as error:
            raise experiment.model.errors.ExperimentInvalidConfigurationError("Error in status monitor configuration",
                                                                              error)

        #FIXME: Refactor Experiment class for access to stages
        stageNames = [stage.name for stage in self.experiment._stages]

        if len(stageNames) != len(self.commands):
            missing = [x for x in range(len(stageNames)) if ("stage%s" % x) not in list(self.commands.keys())]
            msg = 'Missing status.conf entries for stages %s.' % missing
            msg += "\nThere must be a status.conf entry for each stage (%d stages, %d status conf entries)" % (len(stageNames), len(self.commands))
            raise experiment.model.errors.ExperimentInvalidConfigurationError(msg)

        #Calculate the stage weights from conf file info
        fallbackWeight = 1.0/len(self.commands)
        weights = []

        for stage in status_report:
            try:
                weights.append(float(status_report[stage]['stage-weight']))
            except:
                self.log.warning("No stage weight for stage %s. Default to %lf\n" % (stage, fallbackWeight))
                weights.append(fallbackWeight * 1000)

        # VV: adding floats is hard, let's assume that there're at most 2 decimals
        int_weights = [int(e * 1000) for e in weights]

        if reduce(operator.add, int_weights) != 1000:
            self.log.warning("Stage weights do not add to one: %s = %3.2lf\n" % (weights, reduce(operator.add, weights)))
            self.log.warning("All stage-weights will default to %3.2lf\n" % fallbackWeight)
            weights = [fallbackWeight]*len(self.commands)

        self.stageWeights = weights
        self.statusFile = experimentInstance.statusFile
        self.cancelStatus = None

        self.exceptionTracker = experiment.runtime.monitor.MonitorExceptionTracker.defaultTracker()

        # VV: Is set when it is absolutely certain that statusMonitor will never execute again
        self._condition_stopped = threading.Event()

    def join(self):
        """Wait for the confirmation that StatusMonitor will never execute again (unless a new call to Run is made)"""
        self._condition_stopped.wait()

    @property
    def experiment(self):
        # type: () -> "experiment.model.data.Experiment"
        return self.weakExperiment()

    def set_status_database(self, status_database):
        # type: ("Optional[experiment.runtime.status.StatusDB]") -> None
        """Sets and clears the experiment.runtime.status.StatusDB reference.

        Arguments:
            status_database(Optional[experiment.runtime.status.StatusDB]): Status database (or None) to attach to
            if status_database is not none then StatusMonitor will also generate an
            $INSTANCE_DIR/output/status_details.json file. It contains the dictionary
            which experiment.runtime.status.StatusDB.get_status_details() produces. This is what einspect.py uses under the hood
            to print its output.
        """
        with self.mtx_compute_status:
            self._status_database = status_database


    def _getProgress(
            self,
            command,  # type: experiment.model.executors.Command
            stage
    ):

        progress = 0.0
        returncode = 0

        if command is not None:
            commandLine = 'NOT_SET_YET'
            try:
                commandLine = '%s %s' % (command.executable, command.arguments)

                self.log.debug("Inspecting status of %s command is \"%s\"" % (stage.name, commandLine))

                stdout, stderr, process = experiment.model.executors.read_popen_output(
                    commandLine, cwd=command.workingDir, shell=True
                )
            except OSError as error:
                # OSError will come from Popen. Since directories/executables validated on Job creation this indicates
                # a file-system inconsistency error. We will wrap it in a JobLaunchError to give further
                # context to anyone trying to handling exceptions above
                self.log.warning(
                    "Unable to determine status of %s. Exception while launching status check (%s)" % (
                        stage.name, commandLine
                ))
                exception = experiment.model.errors.FilesystemInconsistencyError("Encountered unexpected OSError while checking status",
                                                                                 error)
                raise experiment.runtime.errors.JobLaunchError("Failed to launch status job %s for stage %s" % (
                    command.commandLine, stage.name
                ), exception)
            else:
                returncode = process.returncode

                if returncode == 0:
                    try:
                        progress = float(stdout.strip())
                    except ValueError:
                        self.log.warning("Status for %s returned \"%s\" instead of a float value, "
                                      "will assume progress is 0" % (
                            stage.name, stdout.strip()
                        ))
                        progress = 0.0
                else:
                    self.log.warning(
                        "Unable to determine status of %s. Status script returned %d" % (stage.name, process.returncode))

        return progress, returncode

    def compute_stage_status(self, stage, controller, completed=False):
        # type: (experiment.model.data.Stage, experiment.runtime.control.Controller, bool) -> float
        with self.mtx_compute_status:
            self.statusFile.setCurrentStage(stage.name)

            command = self.commands[stage.referenceName.lower()]
            progress = 0.0
            returncode = 0

            if command:
                progress, returncode = self._getProgress(command, stage)
            else:
                self.log.info("Stage %d does not define a status script, will take into account "
                              "component completion" % stage.index)
                progress = controller.get_stage_status(stage.index)

                if progress is None:
                    self.log.warning("Controller is not aware of stage %d" % stage.index)
                    progress = 0.0

            # Only update progress if the monitor program was successful
            if returncode == 0:
                self.statusFile.setStageProgress(progress)

        return progress

    def try_generate_status_details(self):
        """Try to generate the output/status_details.json file.

        If self._status_database is set, will generate an $INSTANCE_DIR/output/status_details.json file.
        It contains the dictionary which experiment.runtime.status.StatusD.get_status_details() produces.
        This is what einspect.py uses under the hood to print its output.
        """
        with self.mtx_compute_status:
            if self._status_database is not None:
                try:
                    status_details = self._status_database.getWorkflowStatus(json_friendly=True)
                    if status_details is None:
                        self.log.info("Will not generate an output/status_details.json file because "
                                      "there are no stage data in status database")
                    else:
                        tempname = str(uuid.uuid4())
                        tempname = os.path.join(self.experiment.instanceDirectory.outputDir, tempname)

                        status_details_file = os.path.join(self.experiment.instanceDirectory.outputDir,
                                                           'status_details.json')
                        try:
                            with open(tempname, 'wt') as f:
                                json.dump(status_details, f, sort_keys=True, indent=4, separators=(',', ': '))
                        except IOError as e:
                            self.log.warning("Unable to generate status details file: %s due to %s - "
                                             "StatusMonitor will not update output/status_details.json" % (
                                                 tempname, e))

                        try:
                            os.rename(tempname, status_details_file)
                        except OSError as e:
                            self.log.log(15, traceback.format_exc())
                            self.log.warning("Error updating status details file %s (cannot rename due to %s) - "
                                             "StatusMonitor will not update output/status_details.json" % e)
                except Exception as e:
                    self.log.warning("Unexpected error %s while generating status details file - "
                                     "StatusMonitor will not update output/status_details.json" % e)

    def run(self, controller):
        # type: (experiment.runtime.control.Controller) -> None
        '''Starts the status monitor

        Parameters:
            controller: The control.Controller object which is running the experiment'''

        self.cancelStatus = threading.Event()
        self._condition_stopped.clear()

        def CheckStatus():

            '''Runs the defined status check program for the current stage

            Status check programs write a single float to stdout.
            This is the stages progress.
            On obtaining the progress value this function updates the status file.

            Status check programs should return non-zero exit code if they fail
            to determine status. In this case the status file is not updated.

            If the status check program fails to launch the status file is not updated.'''
            with self.mtx_compute_status:
                self.try_generate_status_details()

                stage = controller.stage()
                if stage is None:
                    self.log.info('No stage running')
                    return

                self.log.debug('Checking status of stage %s' % stage.name)

                try:
                    command = self.commands[stage.referenceName.lower()]
                except KeyError:
                    # FIXME: Handle when no monitor for stage - see TODO above
                    self.log.info('No monitor for stage %s' % stage)
                    return

                if command is None:
                    self.log.debug('No monitor for stage %s' % stage)
                else:
                    self.log.debug("%s status command is %s" % (stage.name, command.commandLine))

                stageState = controller.stageState(stage)
                self.statusFile.setStageState(stageState)
                self.statusFile.setCurrentStage(stage.name)

                active_stages = {}

                with controller.comp_lock:
                    stages_in_transit = [si for si in controller.get_stages_in_transit() if si != stage.index]
                    stages_finished = [si for si in controller.get_stages_finished() if si != stage.index]

                active_stages[stage.index] = self.compute_stage_status(stage, controller)

                if stageState not in [experiment.model.codes.RESOURCE_WAIT_STATE, experiment.model.codes.SUSPENDED_STATE]:
                    self.statusFile.setExperimentState(experiment.model.codes.RUNNING_STATE)
                else:
                    self.statusFile.setExperimentState(stageState)

                for stage_index in stages_in_transit:
                    cur_stage = self.experiment._stages[stage_index]
                    active_stages[stage_index] = self.compute_stage_status(cur_stage, controller)

                stage_status = 0.0

                for stage_index in active_stages:
                    stage_status += active_stages[stage_index] * self.stageWeights[stage_index]

                for stage_index in stages_finished:
                    stage_status += self.stageWeights[stage_index]

                self.statusFile.setTotalProgress(stage_status)
                with self.experiment.instanceDirectory.mtx_output:
                    self.statusFile.update()

                def active_stages_report():
                    num = []

                    for id in sorted(active_stages):
                        num.append('%d:%4.2lf' % (id, active_stages[id]))

                    return ', '.join(num)

                if self.report_components:
                    new_report = controller.generate_status_report_for_nodes(None)

                    if new_report != self.last_status_report:
                        self.log.info(new_report)

                    self.last_status_report = new_report

                self.log.info("Status of %s: %-6.4lf %6.4lf %s (active-stages: {%s})" % (stage.name,
                                        self.statusFile.stageProgress(),
                                        self.statusFile.totalProgress(),
                                        self.statusFile.stageState(),
                                        active_stages_report()))

            self.exceptionTracker.printStatus(details=False)

        def check_status_and_report_finished(last_action):
            """Triggers CheckStatus, on last_action also marks StatusMonitor as done meaning that threads waiting
            to join() it are awaken

            Arguments:
                last_action(bool): Whether this is the final task of the StatusMonitor
            """
            try:
                if last_action is True:
                    self.log.info("About to execute StatusMonitor for the last time")
                CheckStatus()
            finally:
                if last_action is True:
                    self.log.info("Final execution of StatusMonitor is done")
                    self._condition_stopped.set()

        statusMon = experiment.runtime.monitor.CreateMonitor(self.repeatInterval, check_status_and_report_finished,
                                          cancelEvent=self.cancelStatus,
                                          name="StatusMonitor")

        statusMon()

    def kill(self):

        if self.cancelStatus is not None:
            self.cancelStatus.set()

class OutputInterface:
    def __init__(self, label="output"):
        self.log = logging.getLogger(label)

    def output_file(self, key_output_label, file_path):
        """Outputs a file of some key-output

        Args:
            key_output_label(str): Label of key-output
            file_path(str): path to file that makes up key-output
        """
        raise NotImplementedError("output_file() is not implemented")

    def output_folder(self, key_output_label, folder_path):
        """Outputs a folder of some key-output

        Args:
            key_output_label(str): Label of key-output
            folder_path(str): path to folder that makes up key-output
        """
        raise NotImplementedError("output_folder() is not implemented")


class OutputToS3(OutputInterface):
    def __init__(self, s3_client, bucket, root, update_existing=True):
        """Uploads key-outputs to s3

        Args:
            s3_client(experiment.s3_utilities.S3Utility): An S3Utility to carry out the actual file/folder upload
            bucket(str): bucket identifier
            root(str): key in bucket which will serve as the root of output files/folders
            update_existing(bool): Whether to update existing keys in bucket
        """
        super(OutputToS3, self).__init__('OutputToS3')

        self._client = s3_client
        self._bucket = bucket
        self._root = root
        self._update_existing = update_existing

    def output_file(self, key_output_label, file_path):
        """Uploads a file of some key-output to S3

        Args:
            key_output_label(str): Label of key-output
            file_path(str): path to file that makes up key-output
        """
        key = os.path.join(self._root, os.path.split(file_path)[1])
        s3_uri = experiment.runtime.utilities.s3.S3Utility.make_s3_uri(self._bucket, key)
        self.log.info("Uploading file %s to %s for %s" % (file_path, s3_uri, key_output_label))
        self._client.upload_path_to_key(self._bucket, file_path, key, update_existing=self._update_existing)

    def output_folder(self, key_output_label, folder_path):
        """Uploads a folder of some key-output to S3

        Args:
            key_output_label(str): Label of key-output
            folder_path(str): path to folder that makes up key-output
        """
        key = os.path.join(self._root, os.path.split(folder_path)[1])
        s3_uri = experiment.runtime.utilities.s3.S3Utility.make_s3_uri(self._bucket, key)
        self.log.info("Uploading folder %s to %s for %s" % (folder_path, s3_uri, key_output_label))
        self._client.upload_path_to_key(self._bucket, folder_path, key, update_existing=self._update_existing)


class OutputAgent:
    """Class which generates output/output.json description and may also copy workflow key-outputs to external storage

    """

    def __init__(self, experimentInstance, output_to=None):
        """Instantiates an output agent which can be asked to upload key-outputs to S3

        Args:
            experimentInstance(experiment.data.Experiment): An Experiment instance
            output_to(OutputInterface): An instance of a class which implements OutputInterface. It will be used
                to copy key-outputs

        Raises ExperimentInvalidConfigurationError
        """

        #Stage like ivars
        self.weakExperiment = weakref.ref(experimentInstance)
        self.log = logging.getLogger("outputAgent")

        self._output_util = output_to

        directory = self.experiment.instanceDirectory.resolvePath("output")
        #FIXME: There will be a problem here when consolidate is called ...
        directory = os.path.realpath(directory)
        directory = os.path.abspath(directory)
        self.outputDir = experiment.model.storage.WorkingDirectory(directory)
        self.outputFile = os.path.join(self.outputDir.path, "output.txt")

        # VV: In this case dataReference is a dict whose keys are key-output names and values datareferences
        # to components who generate the key-output file and can be in 1 or more stages
        # format: {<key-output>:{"references":{<stage-index>: DataReference}, "status":STATUS} where STATUS is: {
        #  "lastLocation": Optional(str),  # last recorded path (relative path to workflow instance)
        #  "creationTime": Optional(str), # epoch that key-output file/folder was last modified
        #  "lastStage": Optional(int),  # last stage for which a DataReference was copied to external storage
        #  "version": int, # how many times this key-output was copied to external storage (1st copy = 1),
        #  "production": yes/no # set to yes
        #  "description": string with some human-readable description
        #  "type": string
        #  "final": yes/no # set to no when current stage is the last one in the
        #           dataReferences[key-output]['references'] dictionary
        #}
        self.dataReferences = {}  # type: Dict[str, Dict[str, Any]]
        self.parse_key_outputs()

    def parse_key_outputs(self):
        output_section = self.experiment.experimentGraph.configuration.get_key_outputs()
        referenceErrors = []
        self.dataReferences.clear()

        for package in output_section:
            conf_package = output_section[package]
            conf_package['name'] = package
            ref = conf_package['data-in']  # type: str
            stages = conf_package.get('stages', [])
            stages = sorted([experiment.model.frontends.flowir.FlowIR.stage_identifier_to_stage_index(x) for x in stages])

            completeRefs = []
            if len(stages) > 0:
                # Create complete refs for each stage
                for stage in sorted(stages):
                    completeRefs.append("stage%d.%s" % (
                        experiment.model.frontends.flowir.FlowIR.stage_identifier_to_stage_index(stage), ref))
            else:
                completeRefs.append(ref)

            # VV: See declaration of self.dataReferences for more info
            self.dataReferences[package] = {
                'status': {
                    'version': 0,
                    'production': "yes",  # VV: FIXME this is hard-coded to yes
                    'final': 'no',
                    'lastStage': None,
                    'lastLocation': None,
                    'creationTime': None,
                    'description': output_section.get('description', ''),
                    'type': output_section.get('type', '')
                },
                'references': {}
            }

            for ref in completeRefs:
                try:
                    refObject = experiment.model.graph.DataReference(ref)
                    if refObject.stageIndex is None:
                        raise ValueError("Reference %s does not have a stage index" % ref)
                    self.dataReferences[package]['references'][refObject.stageIndex] = refObject
                except ValueError as error:
                    referenceErrors.append((ref, str(error)))

        if len(referenceErrors) != 0:
            underlyingError = experiment.model.errors.DataReferenceFormatError(referenceErrors)
            raise experiment.model.errors.ExperimentInvalidConfigurationError("Error in output monitor configuration",
                                                                              underlyingError)

    @property
    def experiment(self):
        # type: () -> Experiment
        return self.weakExperiment()

    def group_datareferences_in_stages(self) -> Dict[int, List[Tuple[str, experiment.model.graph.DataReference]]]:
        """Returns a mapping of stages to datareferences and the key-output they belong to

        Returns a dictionary with the following format:
            stage_index:
                - (key_output_name, DataReference object)

        """
        ret: Dict[int, List[Tuple[str, experiment.model.graph.DataReference]]] = {}

        for key_output in self.dataReferences:
            references = self.dataReferences[key_output]['references']
            for stage_idx in sorted(references):
                if stage_idx not in ret:
                    ret[stage_idx] = []
                ret[stage_idx].append((key_output, references[stage_idx]))

        return ret

    def checkDataReferences(self):
        data_refs = self.group_datareferences_in_stages()
        for stage in sorted(data_refs):
            for key_output, reference in data_refs[stage]:
                if reference.method != experiment.model.graph.DataReference.Copy:
                    self.log.info('DataReference "%s" uses "%s" method instead of %s - output is the location "%s"' % (
                        reference.stringRepresentation, reference.method, experiment.model.graph.DataReference.Copy,
                        reference.location(self.experiment.experimentGraph)))
                #The monitoring should be monitoring component output
                #Hence if the reference appears direct it is probably an error
                #(a reference appears direct if the producer does not exist in the graph)
                if reference.isDirectReference(self.experiment.experimentGraph):
                    cid = (reference.producerIdentifier.stageIndex, reference.producerIdentifier.componentName)
                    exc = experiment.model.errors.FlowIRComponentUnknown(cid)
                    raise experiment.model.errors.InvalidOutputEntry(key_output, reference.stringRepresentation, exc)
                else:
                    try:
                        path = reference.resolve(self.experiment.experimentGraph)
                    except experiment.model.errors.DataReferenceFilesDoNotExistError as e:
                        # VV: `:output` and `:loopoutput` methods resolve to CONTENTS of files
                        pass

    def process_stage(self, stage):
        # type: (int) -> None
        stage_index = experiment.model.frontends.flowir.FlowIR.stage_identifier_to_stage_index(stage)

        for key_output in self.dataReferences:
            references: Dict[int, experiment.model.graph.DataReference] = \
                self.dataReferences[key_output]['references']
            try:
                dr: experiment.model.graph.DataReference = references[stage_index]
            except KeyError:
                continue
            self.log.info('Key-output "%s" references "%s" in stage %s' % (key_output, dr.stringRepresentation, stage))

            try:
                location = dr.location(self.experiment.experimentGraph)
            except Exception as e:
                self.log.info("Unable to resolve location of %s for key-output %s due to %s- will skip" % (
                    dr.stringRepresentation, key_output, e))
                continue

            if os.path.exists(location) is False:
                self.log.info("Location %s for DataReference %s of key-output %s does not exist - will skip" % (
                    location, dr.stringRepresentation, key_output))
                continue

            if self._output_util:
                try:
                    if os.path.isfile(location):
                        self._output_util.output_file(key_output, location)
                    elif os.path.isdir(location):
                        self._output_util.output_folder(key_output, location)
                except Exception as e:
                    self.log.info("Unable to handle output %s for key-output %s due to %s - will continue" % (
                        location, key_output, e))

            try:
                rel_location = os.path.relpath(location, self.experiment.instanceDirectory.location)
                status = self.dataReferences[key_output]['status']

                status['lastStage'] = stage_index
                status['version'] += 1
                status['final'] = 'yes' if (stage_index == max(references)) else 'no'
                status['lastLocation'] = rel_location
                status['creationTime'] = os.path.getmtime(location)
            except Exception as e:
                self.log.info("Unable to update status of key-output %s for stage %s due to error %s - will continue" %
                              (key_output, stage, e))
                continue

        self.updateLogs()

    def updateLogs(self):

        '''Writes output.json and output.txt based on current contents of availableFiles ivar'''

        with self.experiment.instanceDirectory.mtx_output:
            tempname = str(uuid.uuid4())
            tempname = os.path.join(self.outputDir.path, tempname)
            try:
                with open(tempname, 'w+') as f:
                    for key_output in self.dataReferences:
                        status = self.dataReferences[key_output]['status']

                        # VV: This key-output has not been generated yet, skip it
                        if status['version'] == 0:
                            continue

                        f.write("[%s]\n" % key_output)
                        f.write("filename=%s\n" % os.path.split(status['lastLocation'])[1])

                        # VV: filepath is a relative path to the workflow instance
                        f.write("filepath=%s\n" % status['lastLocation'])
                        f.write("description=%s\n" % status['description'])
                        f.write("type=%s\n" % status['type'])
                        f.write("creationTime=%s\n" % status['creationTime'])
                        f.write("version=%d\n" % status['version'])
                        f.write("production=%s\n" % status['production'])
                        f.write("final=%s\n" % status['final'])
                        f.write("\n")
            except IOError as error:
                self.log.warning("Error updating output filename\n%s" % error)
                self.log.warning("Update failed")
            else:
                try:
                    os.rename(tempname, self.outputFile)
                except OSError as error:
                    os.remove(tempname)
                    self.log.warning("Error updating output filename (cannot rename)\n%s" % error)
                    self.log.warning("Update failed")

            # Write Json format
            try:
                jsonString = experiment.model.conf.ConfigurationFileToJson(self.outputFile)
                with open(tempname, 'w+') as f:
                    print(jsonString, end=' ', file=f)
            except IOError as error:
                self.log.warning("Error updating json output file\n%s" % error)
                self.log.warning("Update failed")
            else:
                basename = os.path.splitext(self.outputFile)[0]
                jsonFile = basename + '.json'
                try:
                    os.rename(tempname, jsonFile)
                except OSError as error:
                    os.remove(tempname)
                    self.log.warning("Error updating output filename (cannot rename)\n%s" % error)
                    self.log.warning("Update failed")


