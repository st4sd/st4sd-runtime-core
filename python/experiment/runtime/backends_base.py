#
# coding=UTF-8
# Copyright IBM Inc. 2017,2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston
#
'''Contains lower-level (non-graph reliant) interfaces from Flow to Task subclasses

Its not necessary that a backend provide such interfaces

i.e. Task factory functions that do not rely on objects with InternalRepresentationAttributes interface

NOTE: Do not import this module using from x => namespace clashes will occur

'''
from __future__ import print_function
from __future__ import annotations

import logging
import os
import tempfile
import uuid
from typing import TYPE_CHECKING, Any, Dict, Tuple, Union, Optional

import experiment.appenv
import experiment.model.codes
import experiment.model.errors
import experiment.model.executors
import experiment.model.frontends.flowir
import experiment.model.interface
import experiment.runtime.backend_interfaces.k8s
import experiment.runtime.errors

import experiment.runtime.utilities.container_image_cache

if TYPE_CHECKING:
    pass


def LightWeightKubernetesTaskGenerator(
        executor:  experiment.model.executors.Command,
        resourceManager: experiment.model.frontends.flowir.DictFlowIRResourceManager,
        outputFile: str,
        pre: experiment.model.executors.Command | None = None,
        post: experiment.model.executors.Command | None = None,
        label: str | None = None,
        flowKubeEnvironment: Dict[str, Any] | None = None,
        splitArgs: bool = True,
        pollingInterval: int = 30,
        archive_path_prefix: str | None = None,
) -> experiment.runtime.backend_interfaces.k8s.NativeScheduledTask:
    # VV: Set resource requirements to very low, 100 Mebibytes for ram, and 1 CPU unit

    resourceManager = experiment.model.frontends.flowir.deep_copy(resourceManager)

    # VV: The expression `walltime*60.0` is treated as `active_deadline_seconds` in V1JobSpec, set it to a ludicrous
    #     10 minutes to account for crowded/slow clusters
    resourceManager['config']['walltime'] = 10
    # VV: We don't really need that much CPU
    resourceManager['kubernetes']['cpuUnitsPerCore'] = 0.1
    # VV: flow-executable-check pods should never be evicted
    resourceManager['kubernetes']['qos'] = experiment.model.frontends.flowir.FlowIR.LabelKubernetesQosGuaranteed

    resourceRequest = {
        'numberProcesses': 1,
        'numberThreads': 1,
        'ranksPerNode': 1,
        'threadsPerCore': 1,
        'memory': experiment.model.frontends.flowir.FlowIR.memory_to_bytes('250Mi'),
    }

    task = KubernetesTaskGenerator(
        executor, resourceManager=resourceManager,
        outputFile=outputFile, pre=pre, post=post,
        flowKubeEnvironment=flowKubeEnvironment, label=label,
        splitArgs=splitArgs, resourceRequest=resourceRequest, pollingInterval=pollingInterval,
        archive_path_prefix=archive_path_prefix,
    )

    return task

def KubernetesTaskGenerator(executor,  # type:  experiment.model.executors.Command
                            resourceManager,  # type:  experiment.model.frontends.flowir.DictFlowIRResourceManager
                            outputFile,  # type:  str
                            resourceRequest=None,  # type:  Dict[str, Any]
                            pre=None,  # type:  experiment.model.executors.Command
                            post=None,  # type:  experiment.model.executors.Command
                            label=None,  # type:  str
                            flowKubeEnvironment=None,  # type:  Dict[str, Any]
                            splitArgs=True,  # type:  bool,
                            pollingInterval=30,  # type: int
                            archive_path_prefix: str | None = None,
                            ):

    '''Creates a k8s task from component specification

    Assumes the k8s options in component-spec are in DOSINI FORMAT

    Note: label cannot exceed 40 chars in order for UUIDS generated from it to stay below k8s limit

    This supports one-container with a persistent volume claim
    Valid Options:

    k8s-host, default: http://localhost:8080
    k8s-api-key-var default None
    k8s-namespace, default "default"
    k8s-image-pull-secret: default: None
    k8s-image, default None

    VV: FIXME `k8s-image` is used but we already have the `docker-image` option

    Set by this function:
    k8s-name, no-default: Set to $COMPONENTNAME
    k8s-flow-id, no-default: Set to experiment FLOWID
    k8s-persistant-volume-claim: default: ....

    Args:
        archive_path_prefix: If k8s is configured to archive objects (appenv.KubernetesConfiguration.archive_objects)
            it generates the objects "${archive_path_prefix}pods.yaml" and "${archive_path_prefix}job.yaml".
            if archive_path_prefix is None then it defaults to "${executor.working_dir}/"
    '''
    #The Tasks name field - generated from the label param - cannot be more than 53 chars in length to meet k8s restrictions
    #This limit is 63 chars but the Task class adds up to 10
    #The prefix 'flow-' is 5 chars
    #The ident is 8 chars
    #This leaves 53-8-5 = 40 chars for the label
    if label is None:
        label = executor.executable

    #We want to keep the numeric id that is usually at the end of the label
    if len(label) > 40:
        label = label[:20] + label[-20:]

    log = logging.getLogger('generator.kubernetes.%s' % label)
    log.debug("Creating task for %s" % label)

    # Task class is independent of the workflow details (classes etc.)
    # Therefore all explicitly specified pre/post chains, including backend specific options
    # are created outside the Task class.

    try:
        appenv = experiment.appenv.KubernetesConfiguration.defaultConfiguration()
    except experiment.model.errors.ExperimentSetupError as error:
        raise experiment.runtime.errors.JobLaunchError('Cannot launch kubernetes task',
                                                       underlyingError=error)

    if flowKubeEnvironment is None:
        flowKubeEnvironment = appenv.options

    #NOTE: WE ASSUME IF USING K8S that flow is running in kubernetes - can change later

    options_k8s = {}
    options_k8s['configuration'] = {}
    #TODO: Allow components to set labels also
    ident = "%s" % uuid.uuid4()

    options_k8s['job'] = {'namespace': flowKubeEnvironment['namespace'],
                          'name': "flow-%s-%s" % (label, (ident[:8])),
                          'flow-id': flowKubeEnvironment['labels']['workflow'],
                          'walltime': resourceManager['config']['walltime'],
                          'labels': flowKubeEnvironment['labels'],
                          'qos': resourceManager['kubernetes']['qos']
                          }
    options_k8s['serviceaccountname'] = flowKubeEnvironment['serviceaccountname']

    pullSecrets = flowKubeEnvironment.get('imagePullSecrets', [])
    securityContext = flowKubeEnvironment.get('securityContext', {})

    config_pullsecret = resourceManager\
        .get('kubernetes', {})\
        .get('image-pull-secret', None)
    if config_pullsecret is not None and config_pullsecret not in pullSecrets:
        pullSecrets.append(config_pullsecret)

    options_k8s['job']['pod'] = {'imagePullSecrets': pullSecrets,
                                 'securityContext': securityContext}
    options_k8s['job']['pod']['restart_policy'] = flowKubeEnvironment['restart_policy']
    options_k8s['job']['pod']['terminationGracePeriodSeconds'] = resourceManager['kubernetes']['gracePeriod']
    #This should be in componentSpecification.command.
    options_k8s['job']['pod']['containers'] = {
        'image': resourceManager['kubernetes']['image'],
    }

    options_k8s['job']['pod']['volumes'] = experiment.model.frontends.flowir.deep_copy(flowKubeEnvironment['volumes'])

    options_k8s['job']['pod']['containers']['volumemounts'] = experiment.model.frontends.flowir.deep_copy(
        flowKubeEnvironment['volumemounts'])

    resourceRequest = experiment.model.frontends.flowir.deep_copy(resourceRequest)
    resourceRequest['cpuUnitsPerCore'] = resourceManager.get('kubernetes', {}).get('cpuUnitsPerCore', None)

    # VV: This is so that Job objects are automatically deleted when a `workflow` object is deleted.
    # Deleting the Job object will then trigger the deletion of the Pod objects.
    # VV: @tag:K8sOwnerReference
    options_k8s['ownerReference'] = flowKubeEnvironment.get('ownerReference')
    #Step 3. Create the task
    #The executor defines environment variables for all sub-executors and the task-manager (Global mode)
    #resources: Defines resource requests and backend-specific options
    with open(outputFile, 'wb+') as stdout:
        task = experiment.runtime.backend_interfaces.k8s.NativeScheduledTask(
            executor,
            preCommands=pre,
            postCommands=post,
            options=options_k8s,
            resourceRequest=resourceRequest,
            stdout=stdout,
            stderr=stdout,
            splitArgs=splitArgs,
            pollingInterval=pollingInterval,
            archive_path_prefix=archive_path_prefix,
            garbage_collect = appenv.garbage_collect,
            archive_objects = appenv.archive_objects,
            template_pod_spec=resourceManager.get('kubernetes', {}).get('podSpec')
        )

    return task


class KubernetesExecutableChecker(experiment.model.interface.ExecutableChecker):
    cache = {}
    # VV: Maps expanded container URIs to the fully resolved, and expanded, image URI which is returned by
    # kubernetes, in the form of pod.status.containerStatuses.imageID. In this context, expanded means that the
    # image URI includes a tag (the default tag is `:latest`).
    image_cache = {}

    @classmethod
    def cache_command(cls, environment, executable, image, resolved_executable, resolved_image):
        # type: (Dict[str, str], str, str, str, str) -> None
        """Associates an environment, executable, and image with a resolved executable path

        Method associates all 4 combinations of executable/image with the resolved executable path. So that
        querying a fully resolved command does not unnecessarily lead to a cache miss
        """
        cls.cache[cls.hash_command(environment, executable, image)] = resolved_executable
        cls.cache[cls.hash_command(environment, resolved_executable, image)] = resolved_executable

        cls.cache[cls.hash_command(environment, executable, resolved_image)] = resolved_executable
        cls.cache[cls.hash_command(environment, resolved_executable, resolved_image)] = resolved_executable

    @classmethod
    def hash_command(cls, environment, executable, image):
        # type: (Dict[str, str], str, str) -> Tuple[Union[str, Tuple[str, str]], ...]
        the_hash = [executable, image]

        sorted_keys = sorted(environment)
        for key in sorted_keys:
            the_hash.append((key, environment[key]))

        # VV: Vectors and dicts are not hash-able but Tuples are!
        return tuple(the_hash)

    @classmethod
    def is_command_hashed(cls, environment, executable, image):
        # type: (Dict[str, str], str, str) -> bool
        return cls.hash_command(environment, executable, image) in cls.cache

    @classmethod
    def get_hashed_command(cls, environment, executable, image):
        # type: (Dict[str, str], str, str) -> str
        return cls.cache[cls.hash_command(environment, executable, image)]

    def __init__(self,  resourceManager: experiment.model.frontends.flowir.DictFlowIRResourceManager):
        '''Initializes the KubernetesExecutableChecker using metadata in component.resourceManager

        Args
            resourceManager:
                    Contains the following key-paths
                        - ['kubernetes']['image']
                        - ['kubernetes']['image-pull-secret']
         '''

        self.log = logging.getLogger('backends.k8scheckexe')
        self.resourceManager = resourceManager

        # VV: If a previous executable check involved the referenced image, make sure to use the exact same image id
        img = self.resourceManager['kubernetes']['image']
        self.resourceManager['kubernetes']['image'] = experiment.runtime.utilities.container_image_cache.cache_lookup(img)

    def _findCommand(self, command):

        return experiment.model.executors.Command(executable='which',
                                                        arguments=command._executable,
                                                        environment=command._environment,
                                                        resolveShellSubstitutions=False,
                                                        resolvePath=False)

    def _findAndResolveCommand(self, command):


        #FIXME:Set environment

        return experiment.model.executors.Command(executable='sh',
                                                        arguments='-c "readlink -f $(which %s)"' % command._executable,
                                                        environment=command._environment,
                                                        resolveShellSubstitutions=False,
                                                        resolvePath=False)

    def findExecutable(self, command, resolvePath):

        '''Finds the Commands executable

        A search is performed if
        - The Commands executable is pathless
        - resolvePath is True

        Note: This method does not check if the executable path exists or is executable.
        Its possible some calls to e.g. follow links, return some meaningless default if they don't work

        Parameters:
            command: An executors.Command instance
            resolvePath: If True the code will attempt to resolve the executable path if it is a link

        Raises:
            ValueError if the executable is:
            - relative

        Returns:
            The updated executable path
                '''

        findCommand = self._findCommand(command) if not resolvePath else self._findAndResolveCommand(command)

        fd, filename = tempfile.mkstemp(dir=command.workingDir)
        archive_path_prefix = os.path.join(command.workingDir, f"executable-check-{os.path.basename(filename)}-")
        task = LightWeightKubernetesTaskGenerator(findCommand, resourceManager=self.resourceManager,
                                       outputFile=filename, label='executable-check',
                                       splitArgs=False, archive_path_prefix=archive_path_prefix)
        task.wait()

        if task.returncode == 0:
            with open(filename) as f:
                executableWithPath = f.read().strip("\n") or None
        else:
            executableWithPath = None
        os.remove(filename)

        if executableWithPath is not None:
            self.log.info("Found executable in command environment - using %s" % executableWithPath)
        else:
            self.log.warning(
                "Unable to find location of executable %s using command environment %s" % (
                    command._executable, command._environment
                ))

        return executableWithPath


    def checkExecutable(self, command):
        # type: ("Command") -> "experiment.runtime.backend_interfaces.k8s.NativeScheduledTask"
        '''Checks if the executable defined by command exists

        Params:
            command: An executors.Command instance

        Raise:
            ValueError if the executable is
            - pathless
            - relative
            - does not exist at absolute path
            - cannot be executed
        '''
        checkCommand = experiment.model.executors.Command(executable='test',
                                                                arguments='-x %s' % command._executable,
                                                                resolveShellSubstitutions=False,
                                                                resolvePath=False)
        fd, filename = tempfile.mkstemp(dir=command.workingDir)
        archive_path_prefix = os.path.join(command.workingDir, f"executable-check-{os.path.basename(filename)}-")
        task = LightWeightKubernetesTaskGenerator(
            checkCommand, resourceManager=self.resourceManager,
            outputFile=filename, label='executable-check', splitArgs=False,
            archive_path_prefix=archive_path_prefix)
        task.wait()
        os.remove(filename)

        if task.returncode != 0:
            msg = "Specified executable at '%s' does not exist or is not executable by user (using environment %s)" % (
                command._executable,
                command._environment)
            self.log.warning(msg)
            raise ValueError(msg)

        return task

    def findAndCheckExecutable(self, command, resolvePath=False):

        '''Finds and checks the Commands executable in a single step

        A search is performed if
        - The Commands executable is pathless
        - resolvePath is True

        Parameters:
            command: An executors.Command instance
            resolvePath: If True the code will attempt to resolve the executable path if it is a link

        Raises:
            ValueError if the executable is:
            - relative
            - does not exist at absolute path
            - cannot be executed

            TaskSubmissionError if unable to submit Kubernetes Job that resolves executable path

        Returns:
            The updated executable path
        '''
        # VV: If the image has been already used before, opt for the exact same hash, else use the task that is about
        # to be spawned to cache the container image url that was used, including its hash
        image = experiment.runtime.utilities.container_image_cache.cache_lookup(self.resourceManager['kubernetes']['image'])  # type: str

        task = None  # type: Optional[experiment.runtime.backend_interfaces.k8s.NativeScheduledTask]

        # VV: Freeze the environment and executable; use this tuple to communicate with the cache
        orig_environment = (command._environment or {}).copy()
        orig_executable = command._executable

        if KubernetesExecutableChecker.is_command_hashed(orig_environment, orig_executable, image):
            return KubernetesExecutableChecker.get_hashed_command(orig_environment, orig_executable, image)

        executablePath = None
        pathless = os.path.split(command._executable)[0] == ""
        if not resolvePath and not pathless:
            task = self.checkExecutable(command)
            executablePath = command._executable
        else:
            if pathless and not resolvePath:
                checkCommand = experiment.model.executors.Command(
                    executable='sh',
                    arguments="-c 'export CMD_PATH=`which %s`; echo ${CMD_PATH}; "
                              "test -x ${CMD_PATH} -a -e ${CMD_PATH}'" % command._executable,
                    environment=command.environment,
                    resolveShellSubstitutions=False,
                    resolvePath=False)
            elif pathless and resolvePath:
                checkCommand = experiment.model.executors.Command(
                    executable='sh',
                    arguments="-c 'export CMD_PATH=`readlink -f $(which %s)`; echo ${CMD_PATH}; "
                              "test -x ${CMD_PATH} -a -e ${CMD_PATH}'" % command._executable,
                    environment=command.environment,
                    resolveShellSubstitutions=False,
                    resolvePath=False)
            else:
                checkCommand = experiment.model.executors.Command(
                    executable='sh',
                    arguments="-c 'export CMD_PATH=`readlink -f %s`; echo ${CMD_PATH}; "
                              "test -x ${CMD_PATH} -a -e ${CMD_PATH}'" % command._executable,
                    environment=command.environment,
                    resolveShellSubstitutions=False,
                    resolvePath=False)

            # VV: Sometimes k8s is quirky and drops submitted jobs. Retry up to 2 times
            max_resubmission_attempts = 2
            resubmission_attempts = max_resubmission_attempts

            k8s_last_reason_failed = None
            while resubmission_attempts > 0:
                fd, filename = tempfile.mkstemp(dir=command.workingDir)
                archive_path_prefix = os.path.join(command.workingDir,
                                                   f"executable-check-{os.path.basename(filename)}-")
                task = LightWeightKubernetesTaskGenerator(
                    checkCommand, resourceManager=self.resourceManager,
                    outputFile=filename, label='executable-check', splitArgs=False,
                    pollingInterval=2.5, archive_path_prefix=archive_path_prefix,
                )

                task.wait()

                if task.returncode == 0:
                    with open(filename) as f:
                        executablePath = f.read().strip("\n")
                else:
                    executablePath = None
                os.remove(filename)

                if task.returncode == 0:
                    self.log.info("Found executable in command environment - using %s" % executablePath)
                    break
                elif task.exitReason == experiment.model.codes.exitReasons['SubmissionFailed']:
                    resubmission_attempts -= 1
                    this_image = self.resourceManager.get('kubernetes', {}).get('image')
                    try:
                        why_failed = '. '.join(task.explain_job_failure())
                    except Exception:
                        why_failed = 'SubmissionFailed'

                    self.log.info("Job to resolve executable %s using image %s failed to Submit due to "
                                  "%s, will retry up to %d times" % (
                        command._executable, this_image, why_failed, resubmission_attempts))

                    k8s_last_reason_failed = ValueError(why_failed)
                else:
                    msg = "No executable file exists at '%s' (resolved path: %s) (using command environment %s)" % (
                        command._executable,  executablePath, command._environment)
                    self.log.warning(msg)
                    raise ValueError(msg)

            if executablePath in ["", None]:
                msg = "Job to resolve executable \"%s\" using image \"%s\" failed to Submit %d times" % (
                      command._executable, self.resourceManager.get('kubernetes', {}).get('image'),
                      max_resubmission_attempts)
                raise experiment.runtime.errors.TaskSubmissionError(msg, k8s_last_reason_failed)

        if task:
            # VV: Update the image cache to include the fully-resolved-image-ids that the task used
            referenced = task.get_referenced_image_ids()
            for img in referenced:
                experiment.runtime.utilities.container_image_cache.cache_register(img, referenced[img])

        # VV: Use the fully resolved image id when caching the executable path
        resolved_image = experiment.runtime.utilities.container_image_cache.cache_lookup(image)
        KubernetesExecutableChecker.cache_command(orig_environment, orig_executable,
                                                  image, executablePath, resolved_image)

        return executablePath
