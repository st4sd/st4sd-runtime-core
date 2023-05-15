#
# coding=UTF-8
# Copyright IBM Inc. 2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

"""Module for creating Tasks on kubernetes"""

from __future__ import print_function
from __future__ import annotations

import json
import datetime
import logging
import os

import pprint
import shlex
import sys
import time
import traceback
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple, cast, TYPE_CHECKING
import urllib3.exceptions
from future.utils import raise_with_traceback
import random
import kubernetes.client
import kubernetes.client.rest
import kubernetes.config
import kubernetes.watch
import reactivex
import reactivex.operators as op
import reactivex.scheduler
import reactivex.subject
import reactivex.internal.exceptions

import experiment.utilities.data
import experiment.runtime.backends_base
import experiment.model.codes
import experiment.model.executors
import experiment.runtime.errors
import experiment.runtime.task
import experiment.runtime.utilities.rx as rx_utilities
import experiment.runtime.utilities.container_image_cache

from experiment.model.frontends.flowir import deep_copy, yaml_dump
import experiment.model.errors
import yaml

if TYPE_CHECKING:
    from kubernetes.client import *

#
# Generic Options
#
# walltime
#

poolK8SNativeTasks = reactivex.scheduler.ThreadPoolScheduler(50)

def instantiate_k8sObject_from_yaml(k8s_class_template, dict_options):
    # type: (Any, Dict[str, Any]) -> Any
    """Generates a kubernetes.client.*VolumeSource instance based on Dictionary with options"""

    # VV: the classes have fields which map parameters to yaml keys, we need to reverse that
    args_to_yaml = k8s_class_template.attribute_map  # type: Dict[str, str]

    # VV: the yaml keys are encoded in lower-case
    parameters = {arg_name: dict_options.get(args_to_yaml[arg_name], dict_options[args_to_yaml[arg_name].lower()])
                  for arg_name in args_to_yaml if (args_to_yaml[arg_name].lower() in dict_options
                                                   or args_to_yaml[arg_name] in dict_options)}

    return k8s_class_template(**parameters)


def generate_persistentVolumeClaimVolumeSource(k8s_obj):
    return instantiate_k8sObject_from_yaml(kubernetes.client.V1PersistentVolumeClaimVolumeSource, k8s_obj)


def generate_configMapVolumeSource(k8s_obj):
    # VV: git-sync-config volume has a `localobjectreference` key in its `configmap` volume source
    name = k8s_obj.get('localObjectReference', k8s_obj.get('localobjectreference', {})).get('name')
    k8s_obj = experiment.model.frontends.flowir.deep_copy(k8s_obj)
    k8s_obj['name'] = name

    return instantiate_k8sObject_from_yaml(kubernetes.client.V1ConfigMapVolumeSource, k8s_obj)


def generate_emptyDirVolumeSource(k8s_obj):
    return instantiate_k8sObject_from_yaml(kubernetes.client.V1EmptyDirVolumeSource, k8s_obj)


def generate_secretVolumeSource(k8s_obj):
    return instantiate_k8sObject_from_yaml(kubernetes.client.V1SecretVolumeSource, k8s_obj)


VolumeInitMap = {
    'persistentvolumeclaim': generate_persistentVolumeClaimVolumeSource,
    'configmap': generate_configMapVolumeSource,
    'emptydir': generate_emptyDirVolumeSource,
    'secret': generate_secretVolumeSource,
}


def instantiate_volumesources_from_definition(k8s_volumes, logger=None):
    # type: (List[Dict[str, Any]], Optional[logging.Logger]) -> List[kubernetes.client.V1Volume]
    if k8s_volumes is None:
        return None

    volumes = []

    logger = logger or logging.getLogger('volumeSources')

    for volume in k8s_volumes:
        if 'volumesource' not in volume:
            logger.log(13, "Skipping volume: %s" % yaml_dump(volume))
            continue

        volumeSource = volume['volumesource']  # type: Dict[str, Any]

        for volume_type in VolumeInitMap:
            if volume_type.lower() == 'configmap' and volumeSource.get('name') == 'git-sync-config':
                # VV: We don't want to mount the git-sync-config configMap as it would break
                # the /etc/shadow of the pod we spawn!
                continue

            if volumeSource.get(volume_type) is not None:
                try:
                    # VV: kubernetes.client.V1Volume receives arguments name + some volume source (i.e. config_map, etc)
                    new_volume_source = VolumeInitMap[volume_type](volumeSource[volume_type])
                    volume_copy = experiment.model.frontends.flowir.deep_copy(volume)
                    del volume_copy['volumesource']
                    volume_copy[volume_type] = new_volume_source
                    filled_volume = instantiate_k8sObject_from_yaml(
                        kubernetes.client.V1Volume, volume_copy
                    )  # type: kubernetes.client.V1Volume
                    logger.log(13, "New volume %s = %s from %s" % (filled_volume.name, filled_volume.to_str(),
                                                                   yaml_dump(volume)))
                    if volume_copy:
                        volumes.append(filled_volume)
                    else:
                        logger.warning("Could not fill in volume: %s" % yaml_dump(volume_copy))
                    break
                except Exception as e:
                    logger.warning("Could not create VolumeSource:\n%s" % yaml_dump(volumeSource))
                    logger.warning(traceback.format_exc())
                    raise_with_traceback(e)
        else:
            logger.log(12, "Do not know how to create a VolumeSource for:\n%s" % yaml_dump(volumeSource))

    return volumes


def instantiate_volumemounts_from_definition(
        dict_volumemounts,  # type: List[Dict[str, Any]]
        volumes,  # type: List[kubernetes.client.V1Volume]
        logger=None,  # type: Optional[logging.Logger]
):
    # type: (...) -> List[kubernetes.client.V1VolumeMount]
    volumeMounts = []
    cache = {}

    logger = logger or logging.getLogger('volumeMounts')

    for volume in volumes:
        cache[volume.name] = volume

    for volumeMount in dict_volumemounts:
        try:
            if volumeMount['name'] not in cache:
                logger.log(13, "Skipping VolumeMount %s" % yaml_dump(volumeMount))
                continue
            new_volume_mount = kubernetes.client.V1VolumeMount(
                mount_path=volumeMount['mountpath'],
                name=volumeMount['name'],
                sub_path=volumeMount.get('subpath')
            )
            volumeMounts.append(new_volume_mount)
            logger.log(13, "Created new volume mount %s=%s" % (
                new_volume_mount.name, new_volume_mount.to_str()
            ))
            # FIXME: Check "name" matches a given volume name (and add this to verifyOptions)
        except KeyError as e:
            logger.warning('Setting task to failed')
            logger.warning('Invalid task configuration. Missing required key for volume-mount: %s' % e)
            logger.warning(traceback.format_exc())
            raise_with_traceback(experiment.runtime.errors.JobLaunchError(
                desc='Invalid task configuration. Missing required key for volume-mount',
                underlyingError=e))

    return volumeMounts


#Need to cache node and image
ImageRegistry = {}


class _MockResponse:
    """We use this in ApiClient.parse_model() to simulate decoding a string that we get from the kubernetes rest-api"""
    def __init__(self, data: Any):
        # ApiClient.deserialize() expects a string here ...
        self.data = json.dumps(data)


class ApiClient(kubernetes.client.ApiClient):
    def parse_model(self, model: Dict[str, Any], of_type: type) -> Any:
        return self.deserialize(_MockResponse(model), of_type)


class K8sJobNotFound(Exception):
    def __init__(self, job_name, exc):
        # type: (str, Optional[kubernetes.client.rest.ApiException]) -> None
        """Raised by methods in NativeScheduledTask whenever the k8s api returns that a Job does not exist"""
        Exception.__init__(self)

        self.job_name = job_name
        self.exc = exc
        self.message = "Job %s was not found, exception: %s" % (job_name, exc)

    def __str__(self):
        return self.message


def parse_k8s_restapi_exception(
        exc,  # type: kubernetes.client.rest.ApiException
        logger=None,  # type: Optional[logging.Logger]
        message=None,  # type: Optional[str]
        log_level=20  # type: int
    ):
    # type: (...) -> Tuple[int,Optional[str]]
    """Extract reason from K8s exception, optionally print exception to logger

    Args:
        exc: Kubernetes REST-API exception
        logger: If provided will log exception body at `log_level` (the statement will be
          `"%s(%d): %s" % (message, exc.status, exc.body)`
        message: Additional text to include, if set to None message will be "K8s REST-API exception"
        log_level: Level of log statement
    Returns:
        int, Optional[String] containing the HTTP status-code and the (optional) reason of the exception
    """
    reason = exc.reason

    if exc.body:
        # VV: Try to extract the reason code from the raw HTTP body
        try:
            dict_form = experiment.model.frontends.flowir.yaml_load(exc.body)
        except Exception:
            reason = exc.reason
        else:
            if isinstance(dict_form, dict):
                reason = dict_form.get('reason', reason)
    if logger:
        if message is None:
            message = "K8s REST-API exception"
        logger.log(log_level, traceback.format_exc())
        logger.log(log_level, "%s(%s):%s" % (message, exc.status, reason))

    return exc.status, reason


class NativeScheduledTask(experiment.runtime.task.Task):
    # VV: The poolScheduler will be instantiated by a single NativeScheduledTask,
    # all NativeScheduledTask use the same pool
    poolK8SNativeTasks: reactivex.scheduler.ThreadPoolScheduler | None = None
    """Starts a Task using the K8s native scheduler"""

    def __init__(self,
                 executor,
                 preCommands=None,
                 postCommands=None,
                 options=None,
                 resourceRequest=None,
                 stdout=sys.stdout,
                 stderr=sys.stderr,
                 splitArgs=True,
                 cacheImage=True,
                 pollingInterval: float = 30,
                 archive_path_prefix: str | None = None,
                 garbage_collect: str | None = "none",
                 archive_objects: str | None = "none",
                 template_pod_spec: Dict[str, Any] | None = None,
                 ):
        '''

        NOTE: The executor.commandLineArguments will be used to get the arguments.
        This will cause resolution of any bash command expansions on the LOCAL machine, not in the container
        environment.

        Note: The `job.name` field cannot be more than 53 characters due to k8s name limits.
        (This class will add up-to 10 chars to name and the limit is 63)
        Names longer than 53 chars will be truncated to 53 chars

        Options Format for k8s is: ::

            configuration (required section even if empty)
              host    (default localhost)
              api-key-var: ( default None)
            job:
              namespace (default "default")
              name  (default UUID)
              flow-id (default None)
              walltime (default 60)
              qos: ["guarantted", "burstable", "besteffort"] # default: guaranteed
              serviceaccountname: name of service account to use # default: None
              terminationGracePeriodSeconds: int - Optional duration in seconds the pod needs to terminate gracefully.
                                                   The value zero indicates delete immediately. Defaults to 30 seconds.
              labels: (default None) Arbitrary key:value pairs to add to job metadata
            pod:
              imagePullSecrets: (default None)
              - name of imagePullSecret
              volumes (default None)
              - name: $name
                # Exactly ONE of persistentvolumeclaim, configmap, emptydir, and secret

                # https://github.com/kubernetes-client/go/blob/master/kubernetes/docs/V1PersistentVolumeClaimVolumeSource.md
                persistentvolumeclaim: # default is None
                  claim_name: the name of PVC
                  read_only: True/False  # False is default

                # https://github.com/kubernetes-client/go/blob/master/kubernetes/docs/V1ConfigMapVolumeSource.md
                configmap:
                  name: the name of the configmap
                  items:
                  - key: name of data key in configmap
                  - mode: the mode of the mounted file - overrides `default_mode`
                  - path: the relative path under the volumeMountPath to mount this specific file
                  # (default of default_mode is 0o600 - to be sure check your Kubernetes environment)
                  default_mode: integer - the mode of the mounted files

                # https://github.com/kubernetes-client/go/blob/master/kubernetes/docs/V1EmptyDirVolumeSource.md
                emptydir:
                  medium: "" (default) or "Memory" - The type of storage medium to back this EmptyDir.
                  size_limit: null (default) or string - Total amount of local storage required for this EmptyDir.
                                                         The size limit is also applicable for memory medium.
                # https://github.com/kubernetes-client/go/blob/master/kubernetes/docs/V1SecretVolumeSource.md
                secret:
                  secret_name: the name of the object
                  optional: True/False Specify whether the Secret or it's keys must be defined
                  items:
                  - key: name of data key in configmap
                  - mode: the mode of the mounted file - overrides `default_mode`
                  - path: the relative path under the volumeMountPath to mount this specific file
                  # (default of default_mode is 0o600 - to be sure check your Kubernetes environment)
                  default_mode: integer - the mode of the mounted files

              containers
                image (required)
                # https://github.com/kubernetes-client/go/blob/master/kubernetes/docs/V1VolumeMount.md
                volumemounts: (default None)
                - name: str - must match the .name of a pod.volumes entry (required)
                  mountpath: Path within the container at which the volume should be mounted.
                             Must not contain ':' (required)
                  subpath: None(default) or str - Path within the volume from which the container's volume should
                                                  be mounted.
                  mountpropagation: str - Determines how mounts are propagated from the host to container and the
                                          other way around. When not set, MountPropagationNone is used.
                                          This field is beta in 1.10.
        Args:
            executor: A Command or subclass defining the command line and environment
                The instance may have to inspect if this object is a Command or one of its subclasses e.g. Executor
            resources: A dictionary containing backend specific options as key:value pairs
                This is an option:value pair related to the backend that is not a resourceRequest
            resourceRequest: A dictionary containing the resource request
                See class docs for syntax
            stdout: The stdout stream for the job
            stderr: The stderr stream for the job
            splitArgs: Parameter is not inspected, the args are created by splitting the executor.CommandLine using
                shlex.split()
            cacheImage: If True if the task finishes successfully the image name so if a
                subsequent Task uses the same image it will only be pulled 'IfNotPresent' regardless of label e.g.
                'latest'
            pollingInterval: Interval to poll status of task (in seconds)
            archive_path_prefix: If k8s is configured to archive objects
                (appenv.KubernetesConfiguration.archive_objects)
                it generates the objects "${archive_path_prefix}pods.yaml" and "${archive_path_prefix}job.yaml".
                if archive_path_prefix is None then it defaults to "${executor.working_dir}/"
            garbage_collect: Controls how to delete Job and Pod objects on task Completion.
                Choices are "all", "failed", "successful", and "none" (or None literal).
            archive_objects: Controls how to store the Job and Pod objects on task Completion.
                Choices are "all", "failed", "successful", and "none" (or None literal).
            template_pod_spec: (optional) A dictionary representation of a Pod.spec structure (fields you see when
                you run kubectl explain pod.spec). This will act as a template for the synthesized
                job.spec.template.spec. Because this is a beta feature, the rules are that NativeScheduledTask may
                discard information you place in @template_pod_spec. Additionally, if you include any templating
                instructions for "containers" NativeScheduledTask will *ONLY* use the 1st entry of the containers array.
                Finally, note that there could potentially be overlap between @template_pod_spec and @options.
                The @template_pod_spec is applied first and NativeScheduledTask may override @template_pod_spec using
                configuration encoded in @options
        '''
        # VV: ThreadPoolGenerator.get_pool() is threadSafe, so if multiple threads call it concurrently they'll all get
        # the same pool. GIL will then guarantee that every thread will read the same value even if multiple threads
        # end up "updating" ComponentState.componentScheduler.
        tpg = experiment.runtime.utilities.rx.ThreadPoolGenerator
        if NativeScheduledTask.poolK8SNativeTasks is None:
            NativeScheduledTask.poolK8SNativeTasks = tpg.get_pool(tpg.Pools.BackendK8s)

        # VV: Convert None to "none" for these 2 options
        if garbage_collect is None:
            garbage_collect = "none"

        if archive_objects is None:
            archive_objects = "none"

        choices = ["all", "failed", "successful", "none"]
        if garbage_collect not in choices:
            raise experiment.model.errors.InternalInconsistencyError(
                f"k8s.NativeScheduledTask(garbage_collect=\"{garbage_collect}\" should be one of {choices}")

        if archive_objects not in choices:
            raise experiment.model.errors.InternalInconsistencyError(
                f"k8s.NativeScheduledTask(archive_objects=\"{archive_objects}\" should be one of {choices}")

        archive_path_prefix = archive_path_prefix or os.path.join(executor.workingDir, '')
        self.archive_path_prefix = archive_path_prefix
        self.garbage_collect = garbage_collect
        self.archive_objects = archive_objects

        # VV: Try to pull the image a number of times before finally bailing out
        # this value is decreased each time the `reason` of the waiting state of a PodStatus is `ErrImagePull`
        self._remaining_image_pull_errors = 5

        # VV: hold last known state of Pods/Jobs object when archive_objects is set to something other than "none"
        self._last_k8s_pods: List[kubernetes.client.V1Pod | None] = [None] # only support 1 for now
        self._last_k8s_job: kubernetes.client.V1Job | None = None

        self.stdout = os.fdopen(os.dup(stdout.fileno()), 'ab') if stdout is not None else None
        options = deep_copy(options or {})
        self.options = options
        options['job'] = options.get('job', {})
        # VV: Job names cannot contain '#' (among other rules, see DNS-1123 subdomain schema for more info)
        options['job']['name'] = options['job'].get('name', 'UnnamedK8s')[:53].replace('#', '.')

        self.log = logging.getLogger(options['job']['name'])
        self.cacheImage = cacheImage
        self.hasCachedImage = False

        # VV: This is set, when the pod/job state cannot be determined. If the k8s api stays unavailable for more than
        #     5 minutes, then the job is considered to have failed with an exit reason SystemIssue. While the API
        #     cannot be reached, the state, exit_reason, and return_code of the task are left unchanged (i.e. they
        #     retain their last known values).
        self.api_unavailable_since = None  # type: Optional[datetime.datetime]

        self.log.log(13, "Before setting defaults options are:\n%s" % pprint.pformat(options))

        self._epoch_submitted = datetime.datetime.now()  # type: datetime.datetime
        self._epoch_started = None  # type: Optional[datetime.datetime]
        self._epoch_finished = None  # type: Optional[datetime.datetime]

        # VV: Total seconds it took for pod to pull its images
        self._dt_image_pull = None  # Optional[float]

        # VV: Tuple contains (State, ExitReason, ReturnCode)
        self.lastReportedState = (
            experiment.model.codes.INITIALISING_STATE, None, None)  # type: Tuple[str, Optional[str], Optional[int]]

        self.terminated = False
        # VV: Set to datetime that Task was successfully terminated
        self._terminate_called_when = None  # type: Optional[datetime.datetime]

        #NOTE: Name cannot be longer than 53 chars
        options['job']['name'] = options['job']['name'][:53]

        #EXTRACT PARAMETERS FROM OPTIONS AND SET DEFAULTS
        options['job']['name'] = options['job'].get('name', "%s-%s" %(executor.executable, uuid.uuid4()))
        #Ensure adherence to naming conventions DNS-1123
        options['job']['name'] = options['job']['name'].lower()
        options['job']['name'] = ''.join([(x if str.isalnum(x) else '-') for x in options['job']['name']])
        options['job']['walltime'] = 60 * 60 if options['job']['walltime'] is None else options['job']['walltime'] * 60
        # str | object name and auth scope, such as for teams and projects
        options['job']['namespace'] = options['job'].get('namespace', 'default')
        options['job']['labels'] = options['job'].get('labels')
        options['configuration']['host'] = options['configuration'].get('host', 'http://localhost:8080')
        options['configuration']['api-key-var'] = options['configuration'].get('api-key-var', None)
        options['job']['pod']['imagePullSecrets'] = [
            kubernetes.client.V1LocalObjectReference(s) for s in options['job']['pod'].get('imagePullSecrets', [])
        ]
        options['job']['pod']['restart_policy'] = options['job']['pod'].get('restart_policy', 'Never')

        volumes = instantiate_volumesources_from_definition(options['job']['pod'].get('volumes'), logger=self.log)
        options['job']['pod']['volumes'] = volumes

        volume_mounts = options['job']['pod']['containers'].get('volumemounts')

        if volumes and volume_mounts:
            try:
                volume_mounts = instantiate_volumemounts_from_definition(volume_mounts, volumes)
                options['job']['pod']['containers']['volumemounts'] = volume_mounts
            except Exception as exc:
                self.lastReportedState = (
                    experiment.model.codes.FAILED_STATE, experiment.model.codes.exitReasons["SubmissionFailed"], None)
                self.log.warning("Failed to instantiate volume mounts from definition for %s" % (
                    self.options['job']['name']
                ))
                raise_with_traceback(exc)

        self.log.log(13, 'After setting defaults options are:\n %s' % pprint.pformat(self.options))
        #FIXME: Check what happens when any of the required keys are missing

        # Connect to API

        try:
            kubernetes.config.load_incluster_config()
            self.configuration = None
        except Exception as e:
            self.log.info('Unable to load in-cluster config (%s) will fall back to options' % e)
            self.configuration = kubernetes.client.Configuration()
            self.configuration.host = options['configuration']['host']
            self.configuration.verify_ssl = False
            self.configuration.api_key['authorization'] = options['configuration']['api-key-var']
            # Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
            # 'Bearer' required for create_namespaced_job
            self.configuration.api_key_prefix['authorization'] = 'Bearer'

        self.options = options

        # create an instance of the API class
        client = ApiClient(self.configuration)
        self.api_instance = kubernetes.client.BatchV1Api(client)

        #Create resource object
        #There is only one type of resource that can be selected which is CPU
        #We want the pods to be "Guaranteed" type
        #   See https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/
        #Note: This requires the kubelets were configured with --cpu-manager-policy=static.
        #We proceed here as if this was the case as it makes no difference
        #In static mode if the pod CPU AND Memory requests == limits the pod is "Guaranteed" and
        #will get exclusive use of the CPUS
        #We can ensure this by setting the CPU Limit to the number requested - then "request" will become
        #the set to limit
        #For the moment memory will default to whatever k8s default is
        if resourceRequest is None:
            resourceRequest = {}

        resourceRequest['numberThreads'] = resourceRequest.get('numberThreads', 1)
        resourceRequest['numberProcesses'] = resourceRequest.get('numberProcesses', 1)
        resourceRequest['threadsPerCore'] = resourceRequest.get('threadsPerCore', 1)

        if resourceRequest.get('cpuUnitsPerCore') is None:
            # VV: If cpuUnitsPerCore is unspecified, automatically detect it for the machine that flow is running on
            resourceRequest['cpuUnitsPerCore'] = experiment.model.frontends.flowir.FlowIR.default_cpuUnitsPerCore()

        #FIXME: k8s sees the HW threads as cores whereas MPI will not
        # VV: Because of https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-cpu
        # We may need to ask K8s for more cores than we need to get the correct number
        # (still no guarantee they will be on same real core)
        numberCPUs = (
            resourceRequest['cpuUnitsPerCore'] * resourceRequest['numberProcesses']
            * float(resourceRequest['numberThreads']) / resourceRequest['threadsPerCore'])
        #FIXME: Need a good way to set default memory were assuming here 512GB node with 16 CPU and dividing up
        #We could inspect the nodes and see what the average memory and CPU is
        #2 is 512/16/8 (16 cores, 8 hw per core due to k8s seeing hw as cores)
        memory = resourceRequest.get('memory')
        if memory is None:
            memory = '%dGi' % (2 * numberCPUs)
        else:
            memory = str(memory)

        limits = {'cpu': numberCPUs, 'memory': memory}

        qos = options['job'].get('qos', experiment.model.frontends.flowir.FlowIR.LabelKubernetesQosGuaranteed)
        qos = experiment.model.frontends.flowir.FlowIR.str_to_kubernetes_qos(qos)

        # VV: For the time being let's assume that there're just nvidia gpus - in the future we can auto-detect
        # the GPU-resource name.
        gpus = resourceRequest.get('gpus')
        if gpus is not None:
            limits['nvidia.com/gpu'] = gpus
            try:
                val_gpu = float(gpus)
            except TypeError:
                # VV: Assume that there's some weird GPU specification here
                val_gpu = 1
            if val_gpu > 0 and qos == experiment.model.frontends.flowir.FlowIR.LabelKubernetesQosBestEffort:
                self.log.info(f"Job specifies the {qos} QOS however it also asks for GPUs {gpus} - changing qos to "
                              f"{experiment.model.frontends.flowir.FlowIR.LabelKubernetesQosBurstable}")
                qos = experiment.model.frontends.flowir.FlowIR.LabelKubernetesQosBurstable

        #FIXME: Create execution chain
        if resourceRequest['numberProcesses'] > 1 and not isinstance(executor, experiment.model.executors.MPIExecutor):
            # FIXME: Need to insert mpiex in correct place
            executor = experiment.model.executors.MPIExecutor.executorFromOptions(executor, resourceRequest)

        #Create container object
        #TODO: Check I think its implied that the shell is active for all the jobs??
        #IF it is we can add a executor to the chain of form /bin/bash -c " " = BUT it would have to quote the substring


        #
        #Args to investigate
        #lifecycle
        #ports

        #START CREATION

        #Temp Fix related to above to-do: Bash would remove single quotations from command line args so here we do same
        # The issue occurs when the bash-expansions output elements, like single quotes, which would be ignored by bash
        env = executor.environment

        if "OMP_NUM_THREADS" in env:
            env.pop("OMP_NUM_THREADS")

        #Add OMP vars
        #Note: Using OpenMP v3.1 envars
        #With OpenMP 4.0 could also use OMP_PLACES and more precise OMP_PROC_BIND options
        #envstring = "OMP_DISPLAY_ENV=TRUE,OMP_PROC_BIND=TRUE,OMP_NUM_THREADS=%d,%s" % (self.numberThreads, envstring)
        env["OMP_DISPLAY_ENV"] = "TRUE"
        env["OMP_NUM_THREADS"] = "%d" % resourceRequest.get('numberThreads',1)
        env["OMP_PROC_BIND"] = "TRUE"

        #TODO the docker CMD docs say
        #The "array form is the preferred format of CMD. Any additional parameters must be individually expressed as
        # strings in the array:"
        #However it doesn't WHY they should be
        #If you want to pass a command to a shell in docker (via sh -c "$MYCMD") you need not to split the commands
        #in quoted string

        # We have to use commandLine not commandArguments as this only returns the args of immediate receiver
        # For example if the executor is MPI it will return MPI options but not the MPI target
        # executable and its options

        args = shlex.split(executor.commandLine)[1:]
        self.log.log(18, "Splitting CommandLine: \"%s\" to \"%s\" and %s" % (
            executor.commandLine, executor.executable, args
        ))

        # VV: To use GDB in containers you must also enable SYS_PTRACE capabilities.
        # Note This has security implication so we are not enabling it by default. Note that it
        # also requires a SecurityContextConstraint with "allowedCapabilities: [SYS_PTRACE]"
        context = kubernetes.client.V1SecurityContext(
            # capabilities=kubernetes.client.V1Capabilities(add=['SYS_PTRACE'])
        )

        jobContainer = kubernetes.client.V1Container(
            command=[executor.executable],
            args=args,
            name="%s-container" % options['job']['name'],
            env=[kubernetes.client.V1EnvVar(name=k, value=env[k]) for k in list(env.keys())],
            image=options['job']['pod']['containers']['image'],
            image_pull_policy='Always',
            working_dir=executor.workingDir,
            security_context=context,
            volume_mounts=options['job']['pod']['containers'].get('volumemounts')
        )
        # VV: Guaranteed pods request equal limits and requests
        #     Burstable pods request 1/10th of their limit
        if qos == experiment.model.frontends.flowir.FlowIR.LabelKubernetesQosGuaranteed:
            resourceRequirements = kubernetes.client.V1ResourceRequirements(limits=limits, requests=limits)
            jobContainer.resources = resourceRequirements
        elif qos == experiment.model.frontends.flowir.FlowIR.LabelKubernetesQosBurstable:
            requests = {k: limits[k] for k in limits if k not in ['cpu', 'memory']}

            memory = experiment.model.frontends.flowir.FlowIR.memory_to_bytes(memory)

            # VV: Basically, 1/10th of the original CPU request and 1/10th of the original Memory request
            #     but other requests (e.g. GPUs) are the same - primarily because I don't know how to ask for less :)
            #     or if it even makes sense to "scale up" the number of utilized GPUs.
            requests['cpu'] = int(numberCPUs * 100) / 1000.0
            requests['memory'] = f"{min(int(memory*0.1), 50*1024*1024)}"

            resourceRequirements = kubernetes.client.V1ResourceRequirements(limits=limits, requests=requests)
            jobContainer.resources = resourceRequirements

        #Create the job template
        # volume
        #Optional args to investigate
        #node_selector - Sets the available nodes the pod can be scheduled to c.f. node_affinity which is a preference
        #affinity (V1Affinity) - Can be node or pod
        #    -node_affinity - Sets a preference of which nodes a pod should go to
        #    -pod_affinity (send the pods where other pods it has affinity to are - can also have anti-affinity_

        #runtime-class-name
        #secduler-name
        #FIXME: restart_policy has to be OnFailure or we won't see failures here due to failed pulls
        # (and jobs will not be deleted)
        #However if restart_policy is OnFailure the failed pod will be deleted so you can't see error messages ...
        #Need to find a way around this

        # VV: All containers in Job should use the same SecurityContext options (fsgroup, user-id, group-id, etc)
        security_context = instantiate_k8sObject_from_yaml(kubernetes.client.V1PodSecurityContext,
                                                           options['job']['pod'].get('securityContext', {}))

        podSpec = kubernetes.client.V1PodSpec(
            active_deadline_seconds=int(options['job']['walltime']),
            containers=[jobContainer],
            image_pull_secrets=options['job']['pod']['imagePullSecrets'],
            #restart_policy='OnFailure',
            restart_policy=options['job']['pod']['restart_policy'],
            service_account_name=options.get('serviceaccountname'),
            termination_grace_period_seconds=options['job']['pod'].get('terminationGracePeriodSeconds', 30),
            volumes=options['job']['pod']['volumes'],
            security_context=security_context,
        )

        if template_pod_spec:
            self.log.log(15, f'Parsing podSpec {template_pod_spec}')
            try:
                # VV: Merge the podSpec above onto the "template_pod_spec" and use the merged result
                from_template = NativeScheduledTask.parse_pod_spec_template(template_pod_spec)
                from_template = NativeScheduledTask.merge_pod_specs(from_template, podSpec)
                podSpec = from_template
            except Exception as e:
                self.log.warning(f"Custom kubernetes template is invalid: {e} - will report that task failed")
                self.lastReportedState = (
                    experiment.model.codes.FAILED_STATE, experiment.model.codes.exitReasons["KnownIssue"], None)
                raise_with_traceback(experiment.runtime.errors.JobLaunchError(
                    desc=f"Cannot use custom kubernetes template because of error \"{e}\"", underlyingError=e))

        template = kubernetes.client.V1PodTemplateSpec(
            spec=podSpec,
            metadata=kubernetes.client.V1ObjectMeta(
            name="%s-pod" % options['job']['name'], labels=options['job']['labels'])
        )

        #Create the job specification
        #FIXME: Only specify ttl_seconds_after_finished when there is cached job state

        jobSpec = kubernetes.client.V1JobSpec(backoff_limit=0,
                                           completions=1,
                                           template=template)
                                           #ttl_seconds_after_finished=0)

        pretty = "pretty_example"  # str | If 'true', then the output is pretty printed. (optional)
        #dry_run = 'dry_run_example'  # str | When present, indicates that modifications should not be persisted.
        # An invalid or unrecognized dryRun directive will result in an error response and no further processing
        # of the request. Valid values are: - All: all dry run stages will be processed (optional)

        owner_references = None

        # VV: @tag:K8sOwnerReference
        if self.options.get('ownerReference'):
            try:
                owner_references = [kubernetes.client.V1OwnerReference(**self.options['ownerReference'])]
            except Exception as e:
                self.log.info("Unable to inject ownerReference=%s (error %s) - Job/Pod objects will not be "
                              "automatically garbage collected by Kubernetes" % (self.options['ownerReference'], e))

        body = kubernetes.client.V1Job(spec=jobSpec,
                                       metadata=kubernetes.client.V1ObjectMeta(name=options['job']['name'],
                                                                               labels=options['job']['labels'],
                                                                               owner_references=owner_references
                                                                               ))  # type: kubernetes.client.V1Job

        self.log.log(13, 'Task configuration: %s' % body.to_str())
        try:
            #fieldManager is a name associated with the actor or entity that is making these changes.
            # The value must be less than or 128 characters long, and only contain printable characters,
            # as defined by https://golang.org/pkg/unicode/#IsPrint. (optional)
            api_response = self._retry_on_restapi_timeout(lambda: self.api_instance.create_namespaced_job(
                    namespace=options['job']['namespace'], body=body, pretty=pretty,
                    field_manager=options['job'].get('flow-id')))  # type: kubernetes.client.V1Job

            self.log.log(13, "Creating the JOB of %s\n%s" % (options['job']['name'], api_response.to_str()))
        except kubernetes.client.rest.ApiException as e:
            #TODO: Can get an exit reason and message from e
            self.log.warning("Exception when calling BatchV1Api->create_namespaced_job: %s\n" % e)
            self.log.warning("Body of request: %s" % body.to_str())
            self.lastReportedState = (
                experiment.model.codes.FAILED_STATE, experiment.model.codes.exitReasons["SubmissionFailed"], None)
            raise_with_traceback(experiment.runtime.errors.JobLaunchError(
                desc="Exception when calling BatchV1Api->create_namespaced_job", underlyingError=e))
        else:
            self.log.log(15, "Created Job object")

        def _get_task_state(x):
            try:
                return self._getTaskState()
            except Exception as e:
                self.log.log(15, traceback.format_exc())
                self.log.warning("Exception while executing getTaskState() : %s" % e)

        # VV: The subscriber (ReportedStateClosure) decides when the task is done which is why we're forced here
        # to look at isAlive() which ReportedStateClosure sets. Without manual_emit, ReportedStateClosure
        # sets isAlive() to false and the next emission (after pollingInterval) discovers that the observable should
        # terminate. The _manual_emit enables ReportedStateClosure to trigger an immediate emission that causes the
        # observable to terminate as soon as ReportedStateClosure sets isAlive() to false.
        self._manual_emit = reactivex.subject.Subject()
        self._state = reactivex.merge(
            reactivex.interval(pollingInterval, scheduler=poolK8SNativeTasks), self._manual_emit
        ).pipe(
            op.take_while(lambda x: self.isAlive()),
            op.map(_get_task_state)
        )

        # VV: Keep a record of consecutive get_state_errors in SetLastReportedState. If we're unable to get the
        # current task state for 3 times in a row mark the task as Failed with an exit reason SystemIssue
        self._consecutive_get_state_errors = 0

        # VV: Maps referenced image(s) to the image-hashes that the containers actually used
        #     (filled in by _get_last_pod_state)
        self._image_map = {}  # type: Dict[str, str]

        def ReportedStateClosure(task):
            # type: (NativeScheduledTask) -> Any

            def SetLastReportedState(x):
                try:
                    try:
                        task.log.debug('Setting state to %s' % str(x))
                        (cur_state, cur_exit_reason, cur_return_code) = x
                    except Exception as e:
                        task._consecutive_get_state_errors += 1
                        if task._consecutive_get_state_errors < 3:
                            task.log.warning("Unable to get status of task (%s) using %s - "
                                             "will try again later" % (e, x))
                            return
                        else:
                            task.log.warning("Unable to get status of task (%s) using %s -"
                                              " SystemIssue detected" % (e, x))
                            cur_state = experiment.model.codes.FAILED_STATE
                            cur_exit_reason = experiment.model.codes.exitReasons['SystemIssue']
                            cur_return_code = None
                    else:
                        task._consecutive_get_state_errors = 0

                    new_state = (cur_state, cur_exit_reason, cur_return_code)

                    if task.lastReportedState != new_state:
                        task.log.log(18, "Updating state to %s exit-reason to %s and return-code to %s" % new_state)

                    task.lastReportedState = (cur_state, cur_exit_reason, cur_return_code)

                    #If this job started or finished and we haven't cached the image, do so now (unless asked not to)
                    if cur_state in [experiment.model.codes.FINISHED_STATE, experiment.model.codes.RUNNING_STATE] \
                            and not self.hasCachedImage \
                            and self.cacheImage:
                        nodes = self._get_nodes()
                        self.log.log(18, 'Job running on %s' % nodes)
                        image = options['job']['pod']['containers']['image']
                        if ImageRegistry.get(image) is None:
                            ImageRegistry[image] = []

                        ImageRegistry[image].extend([n for n in nodes if n not in ImageRegistry[image]])

                        task.log.log(18, 'Registry entry for %s now %s' % (image, ImageRegistry[image]))
                        task.hasCachedImage = True

                    # VV: Finally check if Task has stopped, if that's the case fetch its logs

                    if cur_state in [experiment.model.codes.FAILED_STATE, experiment.model.codes.FINISHED_STATE]:
                        log_retries = 3

                        while log_retries > 0:
                            # VV: Do not fetch stdout for Cancelled and Terminated tasks
                            if self.terminated:
                                self.log.info("Task has been terminated - will not fetch stdout")
                                break
                            if cur_exit_reason in [experiment.model.codes.exitReasons["Cancelled"],
                                                   experiment.model.codes.exitReasons["SubmissionFailed"]]:
                                self.log.info("Task exit reason is %s - will not fetch stdout" % cur_exit_reason)
                                break

                            msg_on_err = "will retry" if log_retries > 0 else "will mark Task as failed"
                            try:
                                task._fetch_stdout()
                            except IOError:
                                task.log.warning("Unable to write logs to stdout fstream - %s" % msg_on_err)
                            except kubernetes.client.rest.ApiException as e:
                                status, reason = parse_k8s_restapi_exception(e)
                                task.log.warning("Unable to get logs due to Kubernetes exception %s:%s - %s" % (
                                    status, reason, msg_on_err))
                            except Exception as e:
                                if self._terminate_called_when is None:
                                    task.log.warning("Unknown exception %s while fetching stdout will ignore for now - "
                                                      "please report incident to workflow orchestrator developers - "
                                                      "%s\n%s" % (e, msg_on_err, traceback.format_exc()))
                                else:
                                    self.log.info("Task has been terminated - will not fetch stdout")
                                    break
                            else:
                                break
                            log_retries -= 1
                            if log_retries > 0:
                                time.sleep(5)

                        if task.stdout:
                            try:
                                task.stdout.close()
                            except Exception as e:
                                task.log.warning("Unable to close stdout fstream due to %s - will ignore" % e)
                                task.stdout = None
                        if log_retries < 0:
                            task.log.warning("Unable to eventually retrieve logs - mark as failed")
                            task.lastReportedState = (
                                experiment.model.codes.FAILED_STATE, experiment.model.codes.exitReasons['SystemIssue'], None)
                        self._manual_emit.on_next("final emission after retrieving logs to trigger task_completed()")
                except kubernetes.client.rest.ApiException as e:
                    task.log.warning("Unexpected Kubernetes exception: %s - will mark task as failed" % e)
                    task.lastReportedState = (
                        experiment.model.codes.FAILED_STATE, experiment.model.codes.exitReasons['SystemIssue'], None)
                except Exception as e:
                    task.log.log(15, traceback.format_exc())
                    task.log.warning("Ran into unexpected error: %s - will mark task as failed" % e)
                    task.lastReportedState = (
                        experiment.model.codes.FAILED_STATE, experiment.model.codes.exitReasons['UnknownIssue'], None)

            return SetLastReportedState

        def ErrorStateClosure(task):
            # type: (NativeScheduledTask) -> Callable[[Any], None]
            def SetErrorJobState(x):
                self.log.warning('Unexpected error in state observation: %s. Will assume job is'
                                ' failed. No further status requests will be made', x)
                # VV: We don't know whether the task ran or not, let's not assume that it did.
                #     einspect and experiment.model.graph will consider None as 1 and the optimizer
                #     will assume that the task never actually run
                task.lastReportedState = (experiment.model.codes.FAILED_STATE, None, None)

            return SetErrorJobState

        def task_completed():
            def eval_state_predicate(rule: str) -> bool:
                """Helper function to return True/False based on predicate rule regarding task status"""
                if rule == "all":
                    return True
                elif rule == "none":
                    return False
                elif rule == "successful":
                    return self.status == experiment.model.codes.FINISHED_STATE

                # VV: This is for @rule == "failed"
                return self.status == experiment.model.codes.FAILED_STATE

            do_archive_objects = eval_state_predicate(self.archive_objects)
            do_garbage_collect = eval_state_predicate(self.garbage_collect)

            self.log.log(18, "State observable halting as task is finished. "
                             f"Final state of {self.options['job']['name']} is {self.lastReportedState}, "
                             f"do_archive_objects={do_archive_objects}, do_garbage_collect={do_garbage_collect}")

            if do_archive_objects:
                self._do_archive_objects()

            if do_garbage_collect:
                self.log.log(15, f"Attempting to garbage collect Job/Pod objects")
                self.terminate()

        reported_state = rx_utilities.report_exceptions(ReportedStateClosure(self), self.log, 'ReportState')
        task_completed = rx_utilities.report_exceptions(task_completed, self.log, 'TaskCompleted')
        error_state = rx_utilities.report_exceptions(ErrorStateClosure(self), self.log, 'ErrorState')

        self._state.subscribe(on_next=reported_state, on_completed=task_completed, on_error=error_state)

    def _do_archive_objects(self):
        try:
            # VV: Here, we're intentionally *not* using os.path.join() because we wish to prefix the paths not
            # place then under a certain directory - e.g. place all flow-executable-check objects in one directory
            p_pods = f"{self.archive_path_prefix}pods.yaml"
            p_job = f"{self.archive_path_prefix}job.yaml"

            # VV: Serialize the k8s objects into Dictionaries, dump them on the disk, and cleanup memory
            pods = [x.to_dict() if x is not None else None for x in self._last_k8s_pods]
            job = self._last_k8s_job.to_dict() if self._last_k8s_job is not None else None

            yaml.dump(pods, open(p_pods, 'wt'))
            yaml.dump(job, open(p_job, 'wt'))

            self._last_k8s_pods = [None]
            self._last_k8s_jobs = None
            del pods
            del job
        except Exception as e:
            self.log.warning(f"Could not archive Job/Pod objects to {self.archive_path_prefix}(job.yaml|pods.yaml) "
                             f" due to {e} - will ignore error")
        else:
            self.log.log(15, f"Archived Job/Pod objects to {self.archive_path_prefix}(job.yaml|pods.yaml)")

    @classmethod
    def parse_pod_spec_template(cls, template_pod_spec: Dict[str, Any]) -> kubernetes.client.V1PodSpec:
        client = ApiClient()
        # VV: a valid v1PodSpec has `containers[0].name` (and possibly other things but for now we just care
        # about this restriction - this is a beta feature)
        if 'containers' not in template_pod_spec:
            template_pod_spec['containers'] = []

        if len(template_pod_spec['containers']) > 1:
            template_pod_spec['containers'] = [template_pod_spec['containers'][0]]

        if template_pod_spec['containers']:
            if 'name' not in template_pod_spec['containers'][0]:
                template_pod_spec['containers'][0]['name'] = 'ignored'

        pod_spec: kubernetes.client.V1PodSpec = client.parse_model(
            template_pod_spec, kubernetes.client.V1PodSpec)
        if isinstance(pod_spec, kubernetes.client.V1PodSpec) is False:
            # VV: I'm pretty sure that this is not possible, but a 3 line check is not bad
            raise ValueError(f"template {type(pod_spec)} is not a valid V1PodSpec")

        return pod_spec

    @classmethod
    def merge_pod_specs(
            cls, template: kubernetes.client.V1PodSpec, layer: kubernetes.client.V1PodSpec
    ) -> kubernetes.client.V1PodSpec:
        """Overrides the settings of @base with those of @layer

        Args:
            template: template V1PodSpec - modified in place
            layer: pod spec to use for overriding @template

        Returns:
            A merged V1PodSpec
        """
        # VV: This is a beta feature, some options are tricky to deal with in a safe way so for now diregard them
        to_force_override = [
            'init_containers', 'ephemeral_containers', 'image_pull_secrets', 'security_context',
            'restart_policy', 'service_account', 'service_account_name', 'termination_grace_period_seconds'
        ]
        to_merge = ['volumes', 'containers']
        to_override = [x for x in kubernetes.client.V1PodSpec.attribute_map if x not in to_merge]
        for attr in to_override:
            value = getattr(layer, attr)
            if value is not None or attr in to_force_override:
                setattr(template, attr, value)

        # VV: For merging Volumes (and other named "things") we just copy over everything from the top layer
        # and make sure that we do not end up with duplicates
        mb: List[kubernetes.client.V1Volume] = template.volumes or []
        ml: List[kubernetes.client.V1Volume] = layer.volumes or []
        base_vm = {x.name: x for x in mb}
        layer_vm = {x.name: x for x in ml}

        if len(base_vm) != len(mb):
            raise ValueError("Template V1PodSpec contains the same volume multiple times")

        if len(layer_vm) != len(ml):
            raise ValueError("Layer V1PodSpec contains the same volume multiple times")

        base_vm.update(layer_vm)
        template.volumes = [base_vm[x] for x in sorted(base_vm)]
        del base_vm, layer_vm, mb, ml

        if len(template.containers) == 0:
            template.containers.extend(layer.containers)
        else:
            for b, l in zip(template.containers, layer.containers):
                cb: kubernetes.client.V1Container = b
                cl: kubernetes.client.V1Container = l

                # VV: For now support "merging" env, and volume_mounts for containers. Everything else we just
                # copy straight from the top-layer if the value is not None
                cb.name = cl.name
                to_merge = ['env', 'volume_mounts']
                to_override = [x for x in cb.attribute_map if x not in to_merge]
                for attr in to_override:
                    value = getattr(cl, attr)
                    if value is not None:
                        setattr(cb, attr, value)

                eb: List[kubernetes.client.V1EnvVar] = cb.env or []
                el: List[kubernetes.client.V1EnvVar] = cl.env or []
                base_env = {x.name: x for x in eb}
                layer_env = {x.name: x for x in el}
                base_env.update(layer_env)
                del eb, el
                cb.env = [base_env[x] for x in sorted(base_env)]
                del base_env, layer_env

                # VV: The "unique" key of a V1VolumeMount is moutPath - the "name" field refers to a spec.Volume entry
                mb: List[kubernetes.client.V1VolumeMount] = cb.volume_mounts or []
                ml: List[kubernetes.client.V1VolumeMount] = cl.volume_mounts or []
                base_vm = {x.mount_path: x for x in mb}
                layer_vm = {y.mount_path: y for y in ml}
                base_vm.update(layer_vm)
                del mb, ml
                cb.volume_mounts = [base_vm[x] for x in sorted(base_vm)]
                del base_vm, layer_vm

        return template


    @property
    def schedulerId(self):
        """Returns the ID of the Kubernetes Job object"""
        return self.options['job']['name']

    def _record_image_ids(self, container_statuses):
        # type: (List[kubernetes.client.V1ContainerStatus]) -> None
        """Records the full image id that was used by the container runtime and maps it to the container image that
        the container requested to execute

        Method suppresses exceptions but prints an Info message about them
        """

        try:
            container_statuses = container_statuses or []
            for c in container_statuses:
                # VV: Expand the url to make sure that it includes a tag. Kubernetes does that automatically but
                # it's best we explicitly do that to account for changes to the behaviour of kubernetes. For example,
                # depending on the container runtime, image_id may be prefixed with `docker-pullable://`, `docker://`,
                # or not prefixed at all (e.g. cri-o).
                img = experiment.runtime.utilities.container_image_cache.expand_image_uri(c.image)

                # VV: Make sure that both img, and image_id are in the same docker-registry. In one instance cri-o
                # reported an imageID that was hosted in a different docker-registry than the one specified by
                # pod.spec.containers.image

                (orig_schema, orig_registry, orig_name, orig_tag
                    ) = experiment.runtime.utilities.container_image_cache.partition_image_uri(img)
                (_, rslvd_registry, rslvd_name, rslvd_tag
                    ) = experiment.runtime.utilities.container_image_cache.partition_image_uri(c.image_id)

                if (orig_registry == rslvd_registry) and rslvd_name and rslvd_tag:
                    # VV: Use the original URI schema (which could be empty) just to make sure that we're not removing
                    # a http/https schemas which will then stop Kubernetes from pulling the image
                    image_id = ''.join((orig_schema, rslvd_registry, rslvd_name, rslvd_tag))
                    self._image_map[img] = image_id
        except Exception as e:
            self.log.log(15, traceback.format_exc())
            self.log.info("Unable to register image IDs for %s (error %s) - future Jobs which reference one of these "
                          "images may not use the same image IDs if the image:tag gets updated in the meantime" % (
                              container_statuses, e))

    def get_referenced_image_ids(self):
        """Returns a copy of the image url to image id map (e.g. {'ibm/drl:latest': 'ibm/drl@sha256:....'}

        This information cannot be known before the container(s) associated with this Job begin their execution.
        """
        return self._image_map.copy()

    def _retry_on_restapi_timeout(self, func, max_retries=3):
        # type: (NativeScheduledTask, Callable[[], Any], int) -> Any
        """Retries function (up to @max_retries) whenever @func raises kubernetes.client.rest.ApiException
        with status 504 (Timeout) OR when kubernetes API raises a urllib3.exception

        Sleeps for 10 to 15 seconds between retries.

        Args:
            func(Callable[[]): 0-argument function to call
            max_retries(int): Number of times to retry on timeout.
        Returns:
            Return value of @func
        Raises:
            kubernetes.client.rest.ApiException: If kubernetes.client.rest.ApiException is not 500, 504 OR have already
              retried @max_max_retries times, OR experienced a urllib exception
            Exception: On any other exception raised by @func
        """
        i = max_retries

        while True:
            try:
                return func()
            except (urllib3.exceptions.HTTPError, kubernetes.client.rest.ApiException) as e:
                if isinstance(e, kubernetes.client.rest.ApiException):
                    status, reason = parse_k8s_restapi_exception(e)
                else:
                    status = 'urllib.exception'
                    reason = "%s:%s" % (type(e), str(e))

                if status in [500, 504, 'urllib.exception']:
                    if i > 0:
                        self.log.warning("Will retry %s %d times (due to %s - %s)" % (func, i, status, reason))
                        i -= 1
                        time.sleep(random.randint(10, 15))
                        continue
                    else:
                        self.log.warning("No more retries left for %s - raising exception (due to %s - %s)" % (
                            func, status, reason))
                raise_with_traceback(e)

    def get_time_to_pull_images(self, pod=None):
        # VV: There're 2 events, one for the beginning of image `Pulling` and another when the image has been `Pulled`
        # both are emitted by the `kubelet` component.

        core_v1_api = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient(self.configuration))

        if pod is None:
            pod = self._fetch_master_pod_for_job()

        eventlist = core_v1_api.list_namespaced_event(self.options['job']['namespace'],
                                                      field_selector='involvedObject.name=%s' % pod.metadata.name)
        eventlist = cast("CoreV1EventList", eventlist)

        if not eventlist:
            return {}

        # VV: There's no way to tell what order Events will come in, use @info to keep pull-start and pull-finish dates
        # and then report the time between these 2 events as the time it took to pull the image for each container in
        # the @pod_name Pod
        info = {}  # type: Dict[str, Dict[str, datetime.datetime]]

        def container_name_for_event(event: "CoreV1Event"):
            obj_ref = cast(kubernetes.client.V1ObjectReference, event.involved_object)
            field_path = obj_ref.field_path

            if field_path.startswith('spec.containers{') is False:
                raise ValueError("Strange field_path %s - cannot extract container name for event" % field_path,
                                 obj_ref)
            return field_path[16:-1]

        for event in cast(List["CoreV1Event"], eventlist.items):
            source = cast("V1EventSource", event.source)
            obj_ref = cast("V1ObjectReference", event.involved_object)

            # VV: Events can be associated with any type of object, to avoid the risk of parsing events for oother kinds
            # of objects which happen to have the same name as the target Pod, skip events for non-Pod objects
            if obj_ref.kind != 'Pod':
                continue

            if event.reason == 'Pulling' and source.component == 'kubelet':
                container_name = container_name_for_event(event)
                if container_name not in info:
                    info[container_name] = {}
                info[container_name]['pull-start'] = event.last_timestamp
            elif event.reason == 'Pulled' and source.component == 'kubelet':
                container_name = container_name_for_event(event)
                if container_name not in info:
                    info[container_name] = {}
                info[container_name]['pull-finish'] = event.last_timestamp

        images_pull_dt = {}  # type: Dict[str, float]
        for x in info:
            if 'pull-start' in info[x] and 'pull-finish' in info[x]:
                images_pull_dt[x] = (info[x]['pull-finish'] - info[x]['pull-start']).total_seconds()

        if images_pull_dt:
            pull_dt = sum(images_pull_dt[x] for x in images_pull_dt)
            self.log.log(15, "Pulled images %s - total seconds to pull containers: %s" % (images_pull_dt, pull_dt))
            return pull_dt

    def _fetch_master_pod_for_job(
            self,  # type: NativeScheduledTask
            report_warnings=True,  # type: bool
    ):
        # type: (...) -> Optional[kubernetes.client.models.V1Pod]
        """Returns V1Pod description of pod for Job.

        Raises K8sJobNotFound when Job does not exist
        """
        configuration = self.configuration

        try:
            core_api_instance = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient(configuration))

            namespace = self.options['job']['namespace']
            job_name = self.options['job']['name']

            podlist = self._retry_on_restapi_timeout(
                lambda: core_api_instance.list_namespaced_pod(
                    namespace=namespace, label_selector="job-name = %s" % job_name, pretty="true"
                ))  # type: kubernetes.client.V1PodList
        except kubernetes.client.rest.ApiException as e:
            status, reason = parse_k8s_restapi_exception(e)

            if reason.lower() in ('NotFound'.lower(), 'Not Found'.lower()):
                raise_with_traceback(K8sJobNotFound(job_name, e))
            raise_with_traceback(e)
        else:
            if len(podlist.items) > 1:
                self.log.log(18, "Job %s in namespace %s has multiple pods. Will assume the first is the master" % (
                    job_name, namespace
                ))
            elif len(podlist.items) < 1:
                msg = "Could not find pod for job %s in namespace %s (last state: %s)" % (
                    job_name, namespace, str(self.lastReportedState))
                if report_warnings:
                    self.log.warning(msg)
                raise_with_traceback(ValueError(msg))

            master_pod = podlist.items[0]

            return master_pod

    def _fetch_stdout(self, dump_to_stdout_fstream=True):
        # type: (bool) -> Optional[bytes]
        """Fetches stdout logs of pod

        Arguments:
            dump_to_stdout_fstream(bool): Whether to also write() logs to self.stdout

        Returns
            The stdout of the pod if available (str) or None if logs are not available
        """

        self.log.log(15, "Getting pod logs")

        configuration = self.configuration
        namespace = self.options['job']['namespace']
        job_name = self.options['job']['name']
        logs = None

        core_api_instance = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient(configuration))
        try:
            pod = self._retry_on_restapi_timeout(lambda: self._fetch_master_pod_for_job(report_warnings=False))
            pod = cast(kubernetes.client.models.V1Pod, pod)
            pod_metadata = pod.metadata  # type: kubernetes.client.models.V1ObjectMeta
            # VV: pods sometimes print weird non utf-8 symbols, ask read_namespaced_pod_log to avoid
            # interpreting the logs as strings (i.e. _preload_content=False)
            logs = core_api_instance.read_namespaced_pod_log(name=pod_metadata.name, namespace=namespace,
                                                             _preload_content=False)
        except K8sJobNotFound:
            self.log.info("Job %s does not exist - will not get its pod's logs" % job_name)
            return None
        except kubernetes.client.rest.ApiException as e:
            status, reason = parse_k8s_restapi_exception(e)

            if reason.lower() in ('NotFound'.lower(), 'Not Found'.lower()):
                self.log.info("Pod for job %s does not exist - will not get its logs" % job_name)
                return None
            raise_with_traceback(e)

        if dump_to_stdout_fstream and self.stdout and logs is not None:
            try:
                self.stdout.seek(0)
                self.stdout.truncate(0)
            except IOError as e:
                self.log.warning("Unable to truncate stdout - will ignore")

            self.log.log(15, "Retrieved %d bytes" % len(logs.data))
            self.stdout.write(logs.data)
            return logs.data

    def _get_nodes(self):

        core_api_instance = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient(self.configuration))

        pods = self._retry_on_restapi_timeout(
            lambda: core_api_instance.list_namespaced_pod(namespace=self.options['job']['namespace'],
                                                          label_selector="job-name = %s" % self.options['job']['name'],
                                                          pretty=True))  # type: kubernetes.client.V1PodList

        nodes = []
        for pod in pods.items:
            status = cast(kubernetes.client.models.V1PodStatus, pod.status)
            if status.host_ip not in nodes:
                nodes.append(status.host_ip)

        return nodes

    @property
    def status(self):

        '''Returns the status of the task (a string)

        This must be one of the states defined in experiment.codes

        e.g. return experiment.codes.FINISHED_STATE'''

        return self.lastReportedState[0]

    def _unavailable_api_to_status(self):
        if self.api_unavailable_since is None:
            self.api_unavailable_since = datetime.datetime.now()
            delta_time = self.api_unavailable_since - self.api_unavailable_since
        else:
            delta_time = datetime.datetime.now() - self.api_unavailable_since

        if delta_time.seconds > 5 * 60:
            # VV: We didn't manage to query Kubernetes for the state of this Job
            cur_state = experiment.model.codes.FAILED_STATE
            cur_exit_reason = experiment.model.codes.exitReasons['SystemIssue']
            cur_return_code = None

            self.log.warning("Have been unable to communicate with the K8s for job %s for too long (%s). "
                             "Will set state:%s, exit_reason: %s, return_code: %s" % (
                                 self.options['job']['name'], delta_time, cur_state, cur_exit_reason, cur_return_code))
        else:
            cur_state, cur_exit_reason, cur_return_code = self.lastReportedState
            self.log.warning("Unable to communicate with K8s for job %s, for %s. Will echo last known "
                             "state:%s, exit_reason: %s, return_code: %s" % (
                                 self.options['job']['name'], delta_time, cur_state, cur_exit_reason, cur_return_code))

        return cur_state, cur_exit_reason, cur_return_code

    def get_job_object(self):
        # type: () -> kubernetes.client.models.V1Job
        """Returns Kubernetes Job object associated with task

        Raises:
            K8sJobNotFound if kubernetes Job object is not found
            kubernetes.client.rest.ApiException on other Kubernetes-related API error
        """
        job_name = self.options['job']['name']
        try:
            job = self._retry_on_restapi_timeout(lambda: self.api_instance.read_namespaced_job(
                job_name, self.options['job']['namespace'], pretty=True))
        except kubernetes.client.rest.ApiException as e:
            http_status, reason = parse_k8s_restapi_exception(e)
            if http_status == 404:
                raise_with_traceback(K8sJobNotFound(job_name, e))
            raise_with_traceback(e)
        else:
            return job

    def explain_job_failure(self):
        # type: () -> List[str]
        """Returns a list of reasons describing why this Task failed"""
        ret = []
        job_name = self.options['job']['name']
        job = None
        try:
            job = self.get_job_object()
        except K8sJobNotFound:
            ret.append("There is no Job object %s" % job_name)
        except kubernetes.client.rest.ApiException as e:
            ret.append("Could not get Job Object because of %s" % str(e))
        except Exception as e:
            ret.append(str(e))

        if job is None:
            return ret

        pod = None
        try:
            # create an instance of the API class
            core_api_instance = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient(self.configuration))

            # FIXME assume one pod per job
            pod_list = self._retry_on_restapi_timeout(
                lambda: core_api_instance.list_namespaced_pod(namespace=self.options['job']['namespace'],
                                                              label_selector="job-name = %s" % job_name,
                                                              pretty="true"))  # type: kubernetes.client.V1PodList
        except kubernetes.client.rest.ApiException as e:
            http_status, reason = parse_k8s_restapi_exception(e)
            if http_status == 404:
                ret.append('There is no Pod object for job %s' % job_name)
            else:
                ret.append('Unable to get PodList from Kubernetes due to %s' % e)
            return ret
        else:
            pods = pod_list.items  # type: List[kubernetes.client.V1Pod]
            if len(pods) > 1:
                ret.append("We expect that a Job has exactly 1 pod - Job has the following pods %s" % (
                    pod_list.to_str()))
            elif len(pods) == 1:
                pod = pods[0]  # type: kubernetes.client.V1Pod

        if pod is None:
            return ret

        pod_status: kubernetes.client.V1PodStatus = pod.status
        if pod_status is None:
            ret.append('Pod does not have a valid status')
            return ret

        if pod_status and pod_status.container_statuses and len(pod_status.container_statuses) > 1:
            ret.append("We expect that a Pod has exactly 1 container - Pod has the following status %s" % (
                pod_status.to_str()))

        # VV: The pod cannot pull its image if we recorded too many ErrImagePull errors OR the pod never started
        # and yet the Job got a DeadLineExceeded exit reason
        cannot_pull = (self._remaining_image_pull_errors < 0) or (
                self._epoch_started is None and pod_status and pod_status.reason == "DeadlineExceeded")

        if cannot_pull:
            ret.append(f"The pod could not pull its image {self.options['job']['pod']['containers']['image']}"
                       f"on time using the imagePullSecrets {self.options['job']['pod']['imagePullSecrets']}. "
                       "Please check that the image exists and that at least one imagePullSecret has permissions "
                       "to pull the image.")

        statuses = pod_status.container_statuses  # type: List[kubernetes.client.V1ContainerStatus]
        for status in statuses:
            waiting = status.state.waiting  # type: kubernetes.client.V1ContainerStateWaiting
            term = status.state.terminated  # type: kubernetes.client.V1ContainerStateTerminated
            if term is not None and term.message and term.exit_code != 0:
                ret.append('The pod has exit code %s and its termination state message was %s' % (term.exit_code, term.message))
            if waiting is not None and waiting.message:
                ret.append('The pod is stuck waiting because of %s' % waiting.message)

        return ret

    def _get_last_pod_state(self):
        """Reads the Pod object associated with this job and returns a Dictionary using only information from
        the pod.

        This method is supposed to be called by _getTaskState() and terminate() only.

        Returns dictionary:
          'state': experiment.codes.*_STATE based on the information in the Pod object (this may NOT be what the actual
             of the task is, we also need information from the associated Job object)
          'return_code': Optional[int] (the return code that kubernetes report for the main container of the pod)
          'exit_reason': best guess for experiment.codes.exitReason[] based on the details of the Pod object (may not
             be the actual exitReason of the task, we need information from the Job object too to make this decision)

        Sets the self._epoch_started ivar. Other methods use this ivar to determine whether the containers(s) of this
        task ever started. This information can only be obtained by inspecting the Pod object and there are scenarios
        where kubernetes/flow/users delete the pod.

        When it sets self._epoch_started ivar it also invokes get_time_to_pull_images() and then sets the
        self._dt_image_pull ivar equal to the sum of seconds it took for pod to pull its containers

        Also invokes self._record_image_ids() to record the actual container images that were used and map them
        to the container image urls that the Job specification asks kubernetes to use.

        On unexpected error the method prints a warning message and returns None.

        Pseudo-algorithm:
        - IF can query pod THEN
            - IF pod exists:
                - record self._epoch_started if containers have transitioned to running
                - record time it took to pull images (if possible)
                - record image ids that pod uses (if possible)
                - if pod is pending:
                    - if the `reason` of `waiting` state is ErrImagePull decrease self._remaining_image_pull_errors by 1
                        - if self._remaining_image_pull_errors <= 0 report
                            state=Failed, exitReason=SubmissionFailed, returnCode=None
                    - else report state=RESOURCE_WAIT_STATE, exitReason=None, returnCode=None
                - if pod is succeeded, look at terminated state of container to decide between
                    [FINISHED_STATE, FAILED_STATE(ResourceExhausted)]
                - if pod is failed, look at terminated state of container to decide between
                    [FAILED_STATE(ResourceExhausted,SubmissionFailed,KnownIssue,Cancelled,Killed)]
            - ELSE pod does not exist:
                - IF pod had known exitReason THEN echo it
                - ELIF pod had started running THEN FAILED_STATE(Cancelled)
                - ELSE report that pod is in RESOURCE_WAIT_STATE
            - return the state, exitReason, and returnCode
        - ELSE return None
        """
        # VV: Assume that there is a single `RESOURCE_WAIT_STATE` pod (i.e. no exit-reason and no return-code
        # and we're waiting for the Kubernetes cluster to create the Pod definition)

        state = experiment.model.codes.RESOURCE_WAIT_STATE
        exit_reason = None
        return_code = None
        job_name = self.options['job']['name']

        try:
            # create an instance of the API class
            core_api_instance = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient(self.configuration))

            # FIXME assume one pod per job
            pod_list = self._retry_on_restapi_timeout(
                lambda: core_api_instance.list_namespaced_pod(namespace=self.options['job']['namespace'],
                    label_selector="job-name = %s" % job_name, pretty="true"))  # type: kubernetes.client.V1PodList
        except kubernetes.client.rest.ApiException:
            state, exit_reason, return_code = self._unavailable_api_to_status()
        else:
            pods = pod_list.items  # type: List[kubernetes.client.V1Pod]

            if len(pods) > 1:
                # VV: Report that we do not deal with this scenario. SetLastReportedState will make a note and try
                # again in the future, if _getTaskState() fails too many times the task is marked as failed with a
                # SystemIssue exit reason (see self._consecutive_get_state_errors and SetLastReportedState in init)
                self.log.warning("We expect that a Job has exactly 1 pod - Job has the following pods %s" % (
                    pod_list.to_str()))
                return None

            if len(pods) == 1:
                if self.archive_objects != "none":
                    # VV: If intending to archive the objects try to remember last known version of object
                    # even if the pod is deleted outside our control we'll still be able to know how it looked like
                    self._last_k8s_pods.clear()
                    self._last_k8s_pods = pods or []

                pod = pods[0]  # type: kubernetes.client.V1Pod

                pod_status = pod.status  # type: kubernetes.client.V1PodStatus

                pod_name = pod.metadata.name
                self.log.log(17, "Status of pod %s is %s" % (pod_name, pod_status.to_str()))

                if pod_status and pod_status.container_statuses and len(pod_status.container_statuses) > 1:
                    self.log.warning(
                        "We expect that a Pod has exactly 1 container - Pod has the following status %s" % (
                            pod_status.to_str()))
                    return None

                # VV: Set to true if any of the containers is unable to pull its image (waiting state with ErrImagePull)
                cannot_pull = False
                # VV: Set to the message in the waiting state of the contianer that cannot pull its image
                cannot_pull_msg = None

                if self._epoch_started is None and pod_status and pod_status.container_statuses:
                    # VV: Discover when *all* containers first started running and whether any of them cannot Pull
                    statuses = pod_status.container_statuses  # type: List[kubernetes.client.V1ContainerStatus]
                    all_running_at = None
                    for status in statuses:
                        running = status.state.running  # type: kubernetes.client.V1ContainerStateRunning
                        term = status.state.terminated  # type: kubernetes.client.V1ContainerStateTerminated
                        waiting: kubernetes.client.V1ContainerStateWaiting = status.state.waiting
                        started_at = None

                        if waiting:
                            if waiting.reason == "ErrImagePull":
                                cannot_pull = True
                                cannot_pull_msg = waiting.message
                                break
                        elif running:
                            started_at = running.started_at
                        elif term:
                            started_at = term.started_at
                        if started_at is None:
                            all_running_at = None
                            break
                        all_running_at = max(all_running_at or started_at, started_at)

                    if all_running_at:
                        self._epoch_started = all_running_at
                        try:
                            self._dt_image_pull = self.get_time_to_pull_images(pod)
                        except Exception as e:
                            self.log.info("Unable to determine duration of image pull due to %s - will not record "
                                          "\"image-pull\" value in performance information" % e)
                        self.log.log(15, "All containers started running at %s" % all_running_at)

                if pod_status and pod_status.phase != 'Unknown':
                    # VV: Reset the recorded time that the k8s started being unavailable
                    self.api_unavailable_since = None

                # VV: Update dictionary mapping image urls to image ids
                if pod_status:
                    self._record_image_ids(pod_status.container_statuses)
                    self._record_image_ids(pod_status.init_container_statuses)

                state_terminated = None  # type: Optional[kubernetes.client.V1ContainerStateTerminated]
                if pod_status and pod_status.container_statuses:
                    c_status = pod_status.container_statuses[0]  # type: kubernetes.client.V1ContainerStatus
                    c_state = c_status.state  # type: kubernetes.client.V1ContainerState
                    if c_state:
                        state_terminated = cast("kubernetes.client.V1ContainerStateTerminated", c_state.terminated)

                if cannot_pull:
                    self._remaining_image_pull_errors -= 1
                    if self._remaining_image_pull_errors < 0:
                        self.log.info(f"Unable to pull image with message {cannot_pull_msg} - SubmissionFailed")
                        state = experiment.model.codes.FAILED_STATE
                        exit_reason = experiment.model.codes.exitReasons['SubmissionFailed']
                        return_code = None
                # VV: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/
                elif pod_status and pod_status.phase == 'Pending':
                    # VV: The Pod has been accepted by the Kubernetes system, but one or more of the Containers
                    # has not been created. This includes time before being scheduled as well as time spent
                    # downloading images over the network, which could take a while.
                    state = experiment.model.codes.RESOURCE_WAIT_STATE
                    exit_reason = None
                    return_code = None
                elif pod_status and pod_status.phase == 'Running':
                    # VV: The Pod has been bound to a node, and all of the Containers have been created. At least
                    # one Container is still running, or is in the process of starting or restarting.
                    state = experiment.model.codes.RUNNING_STATE
                    exit_reason = None
                    return_code = None
                elif pod_status and pod_status.phase == 'Succeeded':
                    # VV: The pod reports it has Succeeded but we also need to look at the state of the container
                    # to determine what actually happened to the task - a "Succeeded" pod just means that
                    # its containers all had the exitCode 0
                    if state_terminated is None or (not state_terminated.reason):
                        self.log.info("Incomplete terminated_state of the Pod container - will assume"
                                      " that exitReason is Success and state is FINISHED_STATE "
                                      f"(state_terminated={state_terminated})")
                        state = experiment.model.codes.FINISHED_STATE
                        exit_reason = experiment.model.codes.exitReasons['Success']
                        return_code = 0
                    elif state_terminated.reason == 'Completed':
                        state = experiment.model.codes.FINISHED_STATE
                        exit_reason = experiment.model.codes.exitReasons['Success']
                        return_code = 0
                    elif state_terminated.reason == 'OOMKilled':
                        self.log.info("Pod reports it was successful but container was OOMKilled - will assume"
                                      " that exitReason is FAILED_STATE and state is ResourceExhausted "
                                      f"(state_terminated={state_terminated})")
                        state = experiment.model.codes.FAILED_STATE
                        exit_reason = experiment.model.codes.exitReasons['ResourceExhausted']
                        return_code = None
                    else:
                        self.log.info("Unknown terminated_state of Completed Pod container - will assume"
                                      " that exitReason is Success and state is FINISHED_STATE "
                                      f"(state_terminated={state_terminated})")
                        state = experiment.model.codes.FINISHED_STATE
                        exit_reason = experiment.model.codes.exitReasons['Success']
                        return_code = 0
                elif pod_status and pod_status.phase == 'Failed' and state_terminated is None:
                    # VV: Containers never ran in this pod, but the pod is considered failed. This can happen if
                    #     the pod failed to get schedule (inadequate CPU/Mem, unpullable image, missing PVC, etc)
                    if pod_status.reason in ["Evicted", "DeadlineExceeded"]:
                        exit_reason = experiment.model.codes.exitReasons['ResourceExhausted']
                    else:
                        exit_reason = experiment.model.codes.exitReasons['SubmissionFailed']
                    return_code = None
                    state = experiment.model.codes.FAILED_STATE
                elif pod_status and pod_status.phase == 'Failed' and state_terminated:
                    # VV: All Containers in the Pod have terminated, and at least one Container has terminated in
                    # failure. That is, the Container either exited with non-zero status or was terminated by
                    # the system.

                    return_code = state_terminated.exit_code

                    state = experiment.model.codes.FAILED_STATE

                    if return_code == 128 + 9:
                        # VV: When docker kills the container with signal 9 it returns the exit code 128+9
                        #     OR the application hit an out-of-memory condition
                        #     https://success.docker.com/article/what-causes-a-container-to-exit-with-code-137
                        #     We'll assume that the container was killed.
                        #     When users delete a pod, K8s sends a SIGTERM(15), followed by a SIGKILL(9) gracePeriod
                        #     time later. However, after a pod is deleted we can't observe these signals. On top of that
                        #     a job will create new pods when the old ones are deleted, this means that a k8s task
                        #     can never reach the `Cancelled` exitReason. At least, not until we figure out a way
                        #     to propagate the signal to flow.
                        # VV: If the container was Killed because of OOM, it still was Killed; our assumption is OK
                        state_terminated.signal = 9

                    if state_terminated.signal is not None:
                        self.log.info("Pod %s received signal %s" % (pod_name, state_terminated.signal))

                    if pod_status.reason in ["Evicted", "DeadlineExceeded"]:
                        exit_reason = experiment.model.codes.exitReasons['ResourceExhausted']
                    elif state_terminated.signal in [2, 15]:
                        exit_reason = experiment.model.codes.exitReasons["Cancelled"]
                    elif state_terminated.signal == 9:
                        exit_reason = experiment.model.codes.exitReasons["Killed"]
                    else:
                        exit_reason = experiment.model.codes.exitReasons["KnownIssue"]
                elif pod_status and pod_status.phase == 'Unknown':
                    # VV: For some reason the state of the Pod could not be obtained, typically due to an error in
                    # communicating with the host of the Pod.
                    self.log.warning("PodStatus of %s is Unknown, cannot communicate with k8s" % pod_name)
                    state, exit_reason, return_code = self._unavailable_api_to_status()
                else:
                    self.log.warning("Unable to tell the state of the pod. ")
            else:
                # VV: This is ok if:
                #  - SubmissionFailed
                #  - the pod has not been created yet
                #  May indicate that pod got deleted (external killer, or even flow via NativeScheduledTask.kill())

                # VV: Assume that the pod has not been created yet (getTaskState() relies on the state to
                #     distinguish between pods that have run and got somehow deleted and pods that never got
                #     to run at all before being deleted)
                state, exit_reason, return_code = self.lastReportedState

                if exit_reason is not None:
                    self.log.info("Pod no longer exists, but exitReason is known - will echo last known status")
                elif self._epoch_started is not None:
                    # VV: This pod at some point was running and now we cannot find it, it must have been deleted
                    self.log.info("Pod is not found but it had started running - will assume it got Cancelled")
                    state = experiment.model.codes.FAILED_STATE
                    exit_reason = experiment.model.codes.exitReasons["Cancelled"]
                    return_code = None
                else:
                    # VV: Pod object has not been created yet - could indicate problem with permissions etc that is
                    # something that the Job status can help us identify - report task is waiting for resources
                    state = experiment.model.codes.RESOURCE_WAIT_STATE
                    return_code = None
                    exit_reason = None

        ret = {'state': state, 'return_code': return_code, 'exit_reason': exit_reason}
        self.log.log(18, "Pod state is %s" % ret)

        return ret

    def _getTaskState(self):
        """Reads the Job and Pod objects associated with the task to report the current status of the task, also
        updates the _epoch_finished_time ivar with the datetime that the container in the task Pod terminated.

        Returns a tuple with
          - Task state (experiment.codes.*_state),
          - exit-reason (experiment.codes.exitReasons[*]),
          - return code

        On unexpected error it prints a warning message and returns None

        Pseudo-algorithm::

            - IF tried too many times to pull image and failed, report SubmissionFailed
            - ELIF can get job:
                - IF tried too many times to pull image and failed, report SubmissionFailed
                - IF job Complete
                    - IF get_last_pod_state() returned something use it (it is going to be Success, with exit code 0)
                    - ELIF there is no pod_state - assume success and print an info message
                - ELIF job Failed
                    - IF pods never run, SubmissionFailed
                    - ELIF pod_state is not None:
                        - IF pod_State has has an exit_reason - return that
                            (the pod_state detects ResourceExhausted by looking at the pod status)
                        - ELSE Job is Failed, Pod is still running, this means that
                            this is a ResourceExhausted task still in GracePeriod
                    - ELSE report UnknownIssue
            - ELIF JobNotFound:
                - IF job had not Finished/Failed before THEN report Task got Cancelled
                - ELSE echo lastReportedState
            - ELSE (failed to query job): report that st4sd cannot contact Kubernetes
            """

        #FIXME: We assume each job has a single replica i.e all pods are the same job
        #So job status and pod status are equivalent

        #We may be called when task has already been Failed (e.g. failed submission), in this case
        if self.status in [experiment.model.codes.FAILED_STATE, experiment.model.codes.FINISHED_STATE]:
            return list(self.lastReportedState)

        self.log.debug('Checking status of job %s' % self.options['job']['name'])

        cur_state = None
        cur_exit_reason = None
        cur_return_code = None
        epoch_finished_time = None

        try:
            #Bizzarely its possible to be able to delete/read/create jobs but NOT be allowed to use
            # read_namespace_job_status ...
            job = self._retry_on_restapi_timeout(lambda: self.api_instance.read_namespaced_job(
                    self.options['job']['name'], self.options['job']['namespace'], pretty=True))
            job = cast(kubernetes.client.models.V1Job, job)

            if self.archive_objects != "none":
                # VV: If intending to archive the objects try to remember last known version of object
                # even if the pod is deleted outside our control we'll still be able to know how it looked like
                self._last_k8s_job = job

            job_status = cast(kubernetes.client.models.V1JobStatus, job.status)

            self.log.debug('Job status:\n%s' % job_status.to_str())
        except kubernetes.client.rest.ApiException as e:
            http_status, reason = parse_k8s_restapi_exception(e)

            # VV: kubectl returns `NotFound` (because it extracts it from the http body)
            # but the exception field `reason` here is `Not Found`. We could check for the HTTP response code 404
            # but that could also be returned for other reasons.
            if reason in ["NotFound", "Not Found"]:
                # VV: Mimic last known state
                cur_state, cur_exit_reason, cur_return_code = self.lastReportedState

                if cur_state not in (experiment.model.codes.FAILED_STATE, experiment.model.codes.FINISHED_STATE):
                    prev_state = cur_state
                    cur_state = experiment.model.codes.FAILED_STATE
                    cur_exit_reason = experiment.model.codes.exitReasons['Cancelled']

                    if self._epoch_started is not None:
                        # VV: Do not report that the JOB terminated if we never observed its pods execute
                        epoch_finished_time = self._terminate_called_when
                    cur_return_code = None

                    if self._terminate_called_when is None:
                        # VV: The job does not exist, but we didn't delete it
                        self.log.warning(
                            "Job was NotFound - Assuming that job record has been deleted. " 
                            "But task was not done (state:%s). Will report state:%s, exitReason:%s, returnCode:%s" %
                            (prev_state, cur_state, cur_exit_reason, cur_return_code))
                else:
                    if self._terminate_called_when is None:
                        self.log.warning(
                            "Job was NotFound - Assuming that job record has been deleted after it completed. "
                            "Will report state:%s, exitReason:%s, returnCode:%s" %
                            (cur_state, cur_exit_reason, cur_return_code))
                    else:
                        self.log.log(15, "Job has been terminated and Task has already observed this event")
            else:
                self.log.warning("Unknown reason \"%s:%s\" - assume that communication with k8s server is interrupted"
                                 % (http_status, reason))

                cur_state, cur_exit_reason, cur_return_code = self._unavailable_api_to_status()
        else:
            # VV: Reset the recorded time that the k8s started being unavailable
            self.api_unavailable_since = None
            try:
                # VV: Fetch pod_state which also populates the self._epoch_started ivar (we can use that to
                # distinguish SubmissionFailed from ResourceExhausted)
                pod_state = self._get_last_pod_state()

                if pod_state is None:
                    # VV: _get_last_pod_state was unable to determine current status of pod,
                    # return None and let SetLastReportedState figure out what to do
                    return None
            except Exception as e:
                self.log.info("Unexpected error \"%s\" while getting state of pod - will try again later" % e)
                return None

            # VV: We've already deduced that the pod cannot pull its images no need for more logic
            if self._remaining_image_pull_errors < 0:
                cur_state = experiment.model.codes.FAILED_STATE
                cur_exit_reason = experiment.model.codes.exitReasons['SubmissionFailed']
                cur_return_code = None

                if self._epoch_finished is None:
                    self._epoch_finished = datetime.datetime.now()
                status = [cur_state, cur_exit_reason, cur_return_code]
                return status

            if job_status.conditions:
                # VV: Sort conditions based on when they took place and pick the most recent one
                sorted_conditions = sorted(
                    job_status.conditions, key=lambda cond: cond.last_transition_time, reverse=True)

                job_cond = cast(kubernetes.client.models.V1JobCondition, sorted_conditions[0])

                self.log.log(18, 'Checking condition %s (type is %s)' % (job_cond, job_cond.type))

                # VV: Condition.type is either Complete or Failed
                if job_cond.type in ['Complete', 'Failed']:
                    # VV: Only Complete jobs include a completion time,
                    # fallback to the transition time of the condition for Failed jobs
                    epoch_finished_time = job_status.completion_time or job_cond.last_transition_time

                if job_cond.type == 'Complete':
                    if pod_state is not None:
                        cur_state = pod_state['state']
                        cur_exit_reason = pod_state['exit_reason']
                        cur_return_code = pod_state['return_code']

                        if cur_return_code != 0:
                            self.log.info(f"Job is Completed but pod_state is {pod_state}")

                    if cur_state is None and job_status.completion_time is not None:
                        self.log.info(f"Job.type is Complete but pod_state={pod_state} is incomplete -"
                                      " will assume success")
                        cur_state = experiment.model.codes.FINISHED_STATE
                        cur_exit_reason = experiment.model.codes.exitReasons['Success']
                        cur_return_code = 0
                elif job_cond.type == 'Failed':
                    # VV: A Job can fail if its pods never got to execute, or if one of them failed
                    #     (we currently only support a single pod per job)
                    self.log.log(19, "Task for failed")

                    if self._epoch_started is None:
                        # VV: IF the pod never ran THEN the task exit reason is SubmissionFailed
                        cur_state = experiment.model.codes.FAILED_STATE
                        cur_exit_reason = experiment.model.codes.exitReasons['SubmissionFailed']
                        cur_return_code = None
                    elif pod_state is not None:
                        if pod_state['exit_reason'] is not None:
                            # VV: The Job failed for some other reason
                            cur_state = pod_state['state']
                            cur_exit_reason = pod_state['exit_reason']
                            cur_return_code = pod_state['return_code']
                        else:
                            # VV: The Job failed, but the pod is still alive. This can happen if
                            # it received a SIGTERM and kubernetes is waiting for the Pod's GracePeriod to run out
                            # before it sends a SIGKILL.
                            cur_exit_reason = None
                            cur_return_code = None
                            cur_state = experiment.model.codes.RUNNING_STATE
                    else:
                        # VV: The Job failed, the Pod has no known exit_reason - we do not know what the problem is
                        self.log.info(f"Cannot infer Task state out of PodState {pod_state} and Job condition - "
                                      f"setting exit_reason to UnknownIssue")
                        cur_state = experiment.model.codes.FAILED_STATE
                        cur_exit_reason = experiment.model.codes.exitReasons['UnknownIssue']
                        cur_return_code = None
            else:
                # VV: The Job is neither Complete nor Failed (no Job conditions yet).
                # NOTE: The Pod(s) may have already completed right after we got the Job object.
                # We should still report that the Task is running/resource_waiting for consistency.
                # The next k8s-poll will mark the Task finished/failed
                if job_status.active not in [0, None]:
                    cur_state = experiment.model.codes.RUNNING_STATE
                else:
                    cur_state = experiment.model.codes.RESOURCE_WAIT_STATE

        # VV: Epoch finished time is the datetime the Job completes (fail/success)
        # OR when it becomes DeadlineExceeded and its pod is done (i.e. not currently Running)
        if epoch_finished_time is not None:
            if self._epoch_finished is None:
                self._epoch_finished = epoch_finished_time
            elif self._epoch_finished != epoch_finished_time:
                self.log.warning("Mismatching completion_time(s) for %s (state:%s). old:%s != new:%s" % (
                    self.options['job']['name'], cur_state, self._epoch_finished, epoch_finished_time))

        status = [cur_state, cur_exit_reason, cur_return_code]

        return status

    def kill(self):

        '''Synonym for terminate'''

        self.terminate()

    def terminate(self):

        '''Stop task with 'soft' kill

        e.g. SIGTERM/SIGINT or equivalent'''

        if self.terminated is True:
            return

        # create an instance of the API class
        pretty = 'pretty_example'  # str | If 'true', then the output is pretty printed. (optional)
        grace_period_seconds = 300  # int | The duration in seconds before the object should be deleted.
        # Value must be non-negative integer. The value zero indicates delete immediately. If this value is nil,
        # the default grace period for the specified type will be used. Defaults to a per object value if not specified.
        # zero means delete immediately. (optional)

        body = kubernetes.client.V1DeleteOptions(propagation_policy='Background',
                                                 grace_period_seconds=grace_period_seconds)

        # VV: Before deleting the JOB Object and if there's no epoch-startime recorded yet, try fetching the pod state
        # to populate the self._epoch_started ivar
        if self._epoch_started is None:
            self.log.info("About to delete Job, first attempt to record epoch-started")
            try:
                _ = self._get_last_pod_state()
            except Exception as e:
                pass

        # VV: if this Job has started and its stdout has not been fetched try fetching it now, this is safe
        # because the only other place that invokes _fetch_stdout() is in
        # NativeScheduledTask().__init__().ReportedStateClosure after the Job transitions to failed/finished state
        # don't close stdout here, leave that to ReportedStateClosure
        if self._epoch_started is not None and self.lastReportedState[0] not in [
                experiment.model.codes.FAILED_STATE, experiment.model.codes.FINISHED_STATE]:
            self.log.info("Attempting to fest stdout before deleting Job")
            try:
                self._fetch_stdout()
            except Exception as e:
                self.log.info("Unable to fetch stdout right before deleting Job due to %s - will ignore" % e)

        try:
            pod_state = self._get_last_pod_state()
            if pod_state is None:
                # VV: _get_last_pod_state was unable to determine current status of pod,
                # return None and let SetLastReportedState figure out what to do
                return None
        except Exception as e:
            self.log.info("Unexpected error \"%s\" while getting state of pod - will try again later" % e)
            try:
                self._manual_emit.on_next('update state right after call to terminate - was unable to pull state here')
            except reactivex.internal.exceptions.DisposedException:
                self.log.info("Terminated() and unable to get state of pod and there are no subscribers")
            return None

        self._terminate_called_when = datetime.datetime.now()

        try:
            #NOTE: This is a "job record delete" - nothing will be left in system to query after grace period
            #NOTE: although the API provides keyword args for deletion parameters these DO NOT WORK
            #You must use `body` and V1DeleteOptions https://github.com/kubernetes-client/python/issues/234
            #https://github.com/kubernetes/kubernetes/issues/43329
            api_response = self._retry_on_restapi_timeout(
                lambda: self.api_instance.delete_namespaced_job(
                    name=self.options['job']['name'],
                    namespace=self.options['job']['namespace'],
                    grace_period_seconds=grace_period_seconds,
                    pretty=pretty, body=body))
            # propagation_policy='Foreground'
            self.log.log(18, 'Delete namespaced job %s=\n%s' % (
                self.options['job']['name'], pprint.pformat(api_response)))
        except kubernetes.client.rest.ApiException as e:
            self._terminate_called_when = None
            self.log.warning("Exception when calling BatchV1Api->delete_namespaced_job: %s\n" % e)
            #FIXME: Should raise something??
        else:
            self.terminated = True

        try:
            self._manual_emit.on_next('update state right after call to terminate')
        except reactivex.internal.exceptions.DisposedException:
            self.log.info("Terminated() and there are no subscribers")

    def isAlive(self):

        retval = True

        # VV: A task is not Alive anymore if it's terminated AND all of its stdout is fetched
        if self.stdout is None:
            stdout_fetched = True
        else:
            stdout_fetched = self.stdout.closed

        if self.status in [experiment.model.codes.FINISHED_STATE, experiment.model.codes.FAILED_STATE] and stdout_fetched:
            retval = False

        return retval

    def wait(self):

        '''This method blocks the calling thread until the task has exited.

        If the task is remote and stages out data it blocks until data is copied by default'''

        try:
            while self.isAlive() is True:
                time.sleep(10.0)
        except KeyboardInterrupt as e:
            self.terminate()
            # Other higher level code may be waiting on KeyboardInterrupt so re-raise
            raise_with_traceback(e)
        finally:
            self.cleanup()

    def verifyOptions(self, options):

        log = logging.getLogger('k8s.NativeScheduledTask')

        retval = 'configuration' in options
        if retval:
            try:
                options['job']['pod']['container']['image']
            except KeyError:
                log.warning('Missing required option job.pod.container.image')
                retval = False
        else:
            log.warning('Missing required configuration section (required even if empty)')

        return retval

    def cleanup(self):
        pass

    @property
    def returncode(self):
        if self.isAlive() is False:
            return self.lastReportedState[2]
        else:
            return None

    @property
    def exitReason(self):

        #FIXME: Move to getTaskStatus so we can map kubernetes reasons to internal reasons
        #FIXME: Need to figure out how to determine ResourceExhausted and SubmissionFailure

        reason = None
        if self.isAlive() is False:
            returncode = self.returncode
            #If there is a returncode we use that to determine exit reason
            #If there is no returncode check if terminate was called
            #If it wasn't it means we died not due to terminate but without container launching

            if self.lastReportedState[1] is not None:
                return self.lastReportedState[1]

            if returncode is not None:
                if returncode == 0:
                    reason = experiment.model.codes.exitReasons['Success']
                elif returncode >= 128:
                    reason = experiment.model.codes.exitReasons['SystemIssue']
                elif returncode < 128:
                    reason = experiment.model.codes.exitReasons['KnownIssue']
            elif self.terminated is True:
                #FIXME: Because of bug with grace-period we cannot get reason/state after
                #terminate as the record will not be there.
                reason = experiment.model.codes.exitReasons['Cancelled']
            else:
                if self.status == experiment.model.codes.FINISHED_STATE:
                    self.log.warning('Note: Task reported exit with success without exit code')
                    reason = experiment.model.codes.exitReasons['Success']
                else:
                    self.log.warning('Note: Task reported failure without exit code - assuming a submission failure')
                    reason = experiment.model.codes.exitReasons['SubmissionFailed']

        return reason

    def poll(self):

        '''For compatibility with Popen interface - same as returncode property'''

        return self.returncode

    @property
    def performanceInfo(self):

        '''Returns details on the time the task spent in various states in the scheduler

        Returns:
            An experiment.utilities.data.Matrix instance with one row and the following columns:
            - epoch-submitted
            - epoch-started
            - epoch-finished
        Info

        wait-time and transfer-out-time-estimate are impacted by the statusRequestInterval

        wait-time will not be accurate if the completion-time < statusRequestInterval
        as it needs to be obtained when job is running

        transfer-out-time is always an estimate as we only know its finish to a statusRequestInterval resolution.

        '''

        schedulingData = self.default_performance_info()

        epoch_map = {
            'epoch-submitted': self._epoch_submitted,
            'epoch-started': self._epoch_started,
            'epoch-finished': self._epoch_finished,
        }

        for name in epoch_map:
            index = schedulingData.indexOfColumnWithHeader(name)
            epoch = epoch_map[name]
            schedulingData.matrix[0][index] = epoch.strftime("%d%m%y-%H%M%S") if epoch else 'None'

        idx = schedulingData.indexOfColumnWithHeader('sec-image-pull')
        schedulingData.matrix[0][idx] = self._dt_image_pull if self._dt_image_pull else 'None'

        return schedulingData

    @classmethod
    def default_performance_info(cls):
        schedulingHeaders = [
            "sec-image-pull",
            "epoch-submitted",
            "epoch-started",
            "epoch-finished",
        ]

        values = ["None"] * (len(schedulingHeaders))
        schedulingData = experiment.utilities.data.Matrix(
            rows=[values],
            headers=schedulingHeaders,
            name='SchedulingData'
        )

        return schedulingData
