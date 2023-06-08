#
# coding=UTF-8
# Copyright IBM Inc. 2015,2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Michael Johnston


'''Temporary module to allow configuration of external resources to be passed to classes that need that info'''
from __future__ import print_function
from __future__ import annotations

import json
import logging
import os
import re
from typing import TYPE_CHECKING, List, Optional, Dict, Any

import experiment.model.errors
import experiment.model.executors
import experiment.model.frontends.flowir
import experiment.model.graph
import experiment.runtime.backend_interfaces.lsf
import kubernetes.config
import kubernetes.client
import kubernetes.client.rest
import traceback

if TYPE_CHECKING:
    from experiment.model.data import Experiment, Job
    from experiment.model.storage import ExperimentInstanceDirectory

class HybridConfiguration(object):           

    '''Single class used to capture information about hybrid environment'''

    defaultConf = None  # type: Optional[HybridConfiguration]

    @classmethod 
    def defaultConfiguration(cls):

        if HybridConfiguration.defaultConf is None:
            HybridConfiguration.defaultConf = HybridConfiguration(isHybrid=False)

        return HybridConfiguration.defaultConf


    @classmethod 
    def newDefaultConfiguration(cls, isHybrid=False, remoteQueue=None, confFiles=[]):

        modulePath = os.path.abspath(os.path.split(__file__)[0])
        filename = modulePath + os.sep + 'rewrite-rules.json'
        confFiles = [filename] + confFiles
        log = logging.getLogger("appenv.hybridconfiguration")

        d = {}

        for f in confFiles:
            if os.path.exists(f):
                try:
                    with open(f) as s:
                        log.info('Reading rewrite rules from %s' % f)
                        #NOTE: According to docs this call doesn't raise exceptions although json may be invalid
                        r = json.load(s)
                        d.update(r)
                except IOError as error:
                    raise experiment.model.errors.ExperimentError('Error reading platform file %s - %s' % (f, error), error)
            else:
                log.critical("Rewrite-rules file '%s' does not exist!" % f)

        HybridConfiguration.defaultConf = HybridConfiguration(isHybrid=isHybrid,
                                                            remoteQueue=remoteQueue,
                                                            rewriteRules=d)

        return HybridConfiguration.defaultConf

    def __init__(self, isHybrid=False, remoteQueue=None, rewriteRules=None):

        '''Environment initialiser

        isHybrid: If True indicates the experiment is running in a hybrid environment.
        remoteQueue: The target queue for running jobs on the remote side
            Currently only one remote queue per-running experiment is allowed as handling inter-remote
            staging is not supported
        rewriteRules: A dictionary of whose keys are queue names and whose values are rules
            The rules are also dictionaries with two keys.
                'pattern': A regular expression that identifies strings to replace
                'replacement: The string to substitute
        '''
        rewriteRules = rewriteRules or {}
        self.isHybrid = isHybrid
        self.rewriteRules = rewriteRules

        if isHybrid is True and remoteQueue is None:
            raise ValueError('Must define a remote queue when running in hybrid environment')
        else:
            self.remoteQueue = remoteQueue

        self.log = logging.getLogger("appenv.hybridconfiguration")

        if self.isHybrid is True:
            self.log.warning('Running in LSF hybrid environment with  remote queue %s' % self.remoteQueue)

            if self.remoteQueue not in list(self.rewriteRules.keys()):
                self.log.warning("The target remote queue (%s) has no rewrite rule (%s) - this may be an error" % \
                      (self.remoteQueue, self.rewriteRules))
            else:
                self.log.info('Rewrite rule for %s: %s' % (self.remoteQueue, self.rewriteRules[self.remoteQueue]))

    def handleMigration(
            self,  # type: HybridConfiguration
            job,   # type: Job
            exp,  # type: Experiment
    ):

        '''Method handling job migration on the remote side.

        FIXME: This is very hacky at the moment. 
        Among other issues (see comments) it assumes all migratable jobs are remote'''

        if self.isHybrid is True:

            import experiment.runtime.backend_interfaces.lsf

            #Migrated jobs replace the hardcoded directory with a link to the migrated jobs dir
            #This won't be done on the remote side
            #Need to submit a program to the remote side which will do the same thing.
            #Issue is we don't know here that `job` is a remote job ...
            #Actually backends.py (task generators) is currently the only place where Flow is aware of the remote nature of a job
            #Below assumes that in a hybrid env all migrating jobs are remote
            if job.isMigrated:

                reservation = None
                try:
                    reservation = exp.get_flow_option('reservation')
                    self.log.info('Using reservation %s for initial DM staging' % reservation)
                except experiment.model.errors.FlowIRVariableUnknown:
                    pass

                #Do the remove in prexec
                preCommands = []
                postCommands = []
                preCommands.append(experiment.model.executors.Command('rm',
                                                                            arguments="-rf %s" % job.workingDirectory.path,
                                                                            useCommandEnvironment=False,
                                                                            resolveShellSubstitutions=False))

                #Migrated jobs (should) have exactly one reference.
                #The dir of the component being migrated in previous stage
                migratingDir = job.dataReferences[0].path
                link = experiment.model.executors.Command('ln',
                                                                arguments='-s %s %s' % (migratingDir, job.workingDirectory.path),
                                                                useCommandEnvironment=False,
                                                                resolveShellSubstitutions=False)

                #post-exec: For debugging write finished indicator to file and stage back
                ref = os.path.join(job.workingDirectory.path, "migrated.txt")
                postCommands.append(experiment.model.executors.Command(executable='echo',
                                                                             arguments="migrated > migrated.text" % ref,
                                                                             resolveShellSubstitutions=False))
                postCommands.append(experiment.runtime.backend_interfaces.lsf.DMStageOutCommand.commandFromOptionsAndRewriteRule({'destinations':[ref]},
                                                                                                                                 self.rewriteRuleForTargetQueue()))

                for c in preCommands+[link]+postCommands:
                    c.setRewriteRule(self.rewriteRuleForTargetQueue())

                with open(os.path.join(exp.instanceDirectory.location, "migrate.txt"), "w") as f:
                    task = experiment.runtime.backend_interfaces.lsf.Task(link,
                                                                          preCommands=preCommands,
                                                                          postCommands=postCommands,
                                                                          options={"reservation": reservation, "queue":self.remoteQueue},
                                                                          stdout=f,
                                                                          isHybrid=True,
                                                                          checkCWD=False)

                task.wait()
                if task.returncode != 0:
                   self.log.critical("Error migrating job %s - status %s. returncode %d. exit-reason %s" % (job.name, task.status, task.returncode, task.exitReason))
                   raise experiment.model.errors.InternalInconsistencyError("Encountered error while migrating remote job")

    def _incremental_stage_in(self,
                              instance_dir,  # type: "ExperimentInstanceDirectory"
                              lsfOptions, skip_top_dir_paths, batch_size):
        instance_location = os.path.normpath(os.path.abspath(instance_dir.location))
        _, folders, filenames = next(os.walk(instance_location))
        paths = folders + filenames
        paths = [path for path in paths if path not in skip_top_dir_paths]
        paths = [os.path.join(instance_location, path) for path in paths]

        # VV: First create the directory
        cmd_mkdir = experiment.model.executors.Command(executable='/bin/mkdir',
                                                             arguments='-p %s' % instance_location,
                                                             resolveShellSubstitutions=False,
                                                             useCommandEnvironment=False)
        cmd_mkdir.setRewriteRule(self.rewriteRuleForTargetQueue())

        with open(os.path.join(instance_location, 'mkdir.txt'), 'w') as f:
            task = experiment.runtime.backend_interfaces.lsf.Task(cmd_mkdir,
                                       options=lsfOptions,
                                       stdout=f,
                                       isHybrid=True,
                                       checkCWD=False)

        task.wait()
        if task.returncode != 0:
            self.log.critical(
                "Unable to mkdir instance on remote hosts - status %s. returncode %d. exit-reason %s" % (
                    task.status, task.returncode, task.exitReason))
            raise experiment.model.errors.ExperimentSetupError(
                "Encountered exception while creating experiment instance - cannot deploy",
                "Unable to create remote root instance directory", instance_dir=instance_dir)

        cmds_stage_in = []

        for start in range(0, len(paths), batch_size):
            batch = paths[start: start + batch_size]
            options = {
                'sources': batch
            }
            cmds_stage_in.append(
                (
                    experiment.runtime.backend_interfaces.lsf.DMStageInCommand.commandFromOptionsAndRewriteRule(
                        options, self.rewriteRuleForTargetQueue()),
                    batch
                )
            )

        # VV: The command is here so that we can bundle it with `bstage` commands
        dummy_command = experiment.model.executors.Command(executable='/bin/ls',
                                                                 arguments="-ltrah %s" % instance_location,
                                                                 resolveShellSubstitutions=False,
                                                                 useCommandEnvironment=False)
        dummy_command.setRewriteRule(self.rewriteRuleForTargetQueue())

        tasks_stage_in = []

        # The experiment directory structure - along with the set data/binaries -
        # must be replicated to the remote host
        # Doing this here for the moment - may move to instance directory e.g for clean?
        for cmd, paths in cmds_stage_in:
            task = experiment.runtime.backend_interfaces.lsf.Task(
                dummy_command,
                preCommands=[cmd], options=lsfOptions, isHybrid=True, checkCWD=False
            )
            tasks_stage_in.append((task, paths))

        for task, paths in tasks_stage_in:
            task.wait()
            if task.returncode != 0:
                self.log.critical(
                    "Unable to setup instance structure on remote hosts "
                    "(%s)- status %s. returncode %d. exit-reason %s" % (
                        paths, task.status, task.returncode, task.exitReason
                    ))
                raise experiment.model.errors.ExperimentSetupError(
                    "Encountered exception while creating experiment instance - cannot deploy",
                    "Unable to stage remote instance directories", instance_dir=instance_dir)

        # VV: Finally, cleanup the .tar.gz files that we've been generating for the staged-in directories
        if folders:
            self.log.critical('Cleaning up .tar.gz files created during incremental stage-in %s' % folders)
            archives = [os.path.join(instance_location, '%s.tar.gz' % folder) for folder in folders]

            for archive in archives:
                os.remove(archive)

    def buildLSFOptions(self, **kwargs):
        lsfOptions = {"queue": self.remoteQueue}
        if self.remoteQueue == 'paragon':
            lsfOptions['resourceString'] = 'select[hname!=pge102]'

        lsfOptions.update(**kwargs)

        return lsfOptions

    def replicateExperimentStructure(
            self,  # type: HybridConfiguration
            reservation,
            instance_dir,  # type: "ExperimentInstanceDirectory"
            skip_top_dir_paths=None,
            batch_size=3,
    ):

        '''Replicates the structure of the experiment on the remote side.

        FIXME: This is another HACK.'''
        instance_location = instance_dir.location

        if self.isHybrid is True:
            instance_location = os.path.normpath(os.path.abspath(instance_location))
            remote_instance_location = os.path.normpath(self.rewritePathForQueue(instance_location))

            if instance_location == remote_instance_location:
                self.log.warning("Replication is not necessary, the remote filesystem is identical to the local one")
                return

            import experiment.runtime.backend_interfaces.lsf

            #FIXME: Temporary hack for issue with paragon queue
            #More permanent solution is to allow options to be added to hybrid config file
            lsfOptions = self.buildLSFOptions(reservation=reservation, statusRequestInterval=5)
            if reservation is not None:
                self.log.info('Using reservation %s for initial DM staging' % reservation)

            # VV: The command is here so that we can bundle it with `bstage` commands
            dummy_command = experiment.model.executors.Command(executable='/bin/ls',
                                                                     arguments="-ltrah %s" % instance_location,
                                                                     resolveShellSubstitutions=False,
                                                                     useCommandEnvironment=False)
            dummy_command.setRewriteRule(self.rewriteRuleForTargetQueue())

            if skip_top_dir_paths:
                # VV: we can copy files with finer granularity but have to break the process in multiple steps
                #     due to constraints to the number of commands that we can pack in a single LSF job
                self._incremental_stage_in(instance_dir, lsfOptions, skip_top_dir_paths, batch_size)
            else:
                # VV: If we just want to replicate the entire instance directory we can do it in a single step
                stageIn = experiment.runtime.backend_interfaces.lsf.DMStageInCommand.commandFromOptionsAndRewriteRule(
                    {'sources': [instance_location]}, self.rewriteRuleForTargetQueue()
                )
                with open(os.path.join(instance_location, "remotestaging.txt"), "w") as f:
                    task = experiment.runtime.backend_interfaces.lsf.Task(
                        dummy_command, preCommands=[stageIn], options=lsfOptions,
                        stdout=f, isHybrid=True,checkCWD=False
                    )

                task.wait()
                if task.returncode != 0:
                    self.log.critical(
                        "Unable to setup instance structure on remote hosts - "
                        "status %s. returncode %d. exit-reason %s" % (
                        task.status, task.returncode, task.exitReason))
                    raise experiment.model.errors.ExperimentSetupError(
                        "Encountered exception while creating experiment instance - cannot deploy. ",
                        "Unable to stage remote instance directories", instance_dir=instance_dir)

    def replicateFolderStructure(self, instance_dir, folders, batch_size=3):
        # type: (ExperimentInstanceDirectory, List[str], int) -> None
        if self.isHybrid is False:
            return
        instance_location = instance_dir.location
        remote_instance_location = self.rewritePathForQueue(instance_location)
        if instance_location == remote_instance_location:
            self.log.warning("Replication of folders is not necessary, "
                             "the remote filesystem is identical to the local one")
            return

        import experiment.runtime.backend_interfaces.lsf

        task_mkdirs = []
        lsfOptions = self.buildLSFOptions(walltime=1, statusRequestInterval=5)

        for start in range(0, len(folders), batch_size):
            batch = folders[start: start+batch_size]
            batch = ['-p %s' % path for path in batch]

            cmd = experiment.model.executors.Command(executable='/bin/mkdir',
                                                           arguments=' '.join(batch),
                                                           resolveShellSubstitutions=False,
                                                           useCommandEnvironment=False)
            cmd.setRewriteRule(self.rewriteRuleForTargetQueue())

            task = experiment.runtime.backend_interfaces.lsf.Task(cmd,
                                                                  options=lsfOptions,
                                                                  isHybrid=True,
                                                                  checkCWD=False)

            task_mkdirs.append((task, folders[start: start+batch_size]))

        for task, batch in task_mkdirs:
            task.wait()

            if task.returncode != 0:
                self.log.critical(
                    "Unable to mkdir %s on remote hosts - status %s. returncode %d. exit-reason %s" % (
                        folders, task.status, task.returncode, task.exitReason))
                raise experiment.model.errors.ExperimentSetupError(
                    "Encountered exception while creating experiment instance - cannot deploy. ",
                    "Unable to create folders", instance_dir=instance_dir)


    def rewriteRuleForTargetQueue(self):

        '''Returns the rewrite rule for the target queue or None if there is none'''

        rule = None
        if self.remoteQueue in list(self.rewriteRules.keys()):
            rule = self.rewriteRules[self.remoteQueue]

        return rule

    def rewriteRuleForQueue(self, queue):

        rule = None
        if queue in list(self.rewriteRules.keys()):
            rule = self.rewriteRules[queue]

        return rule

    def rewritePathForQueue(self, path):
        rule = self.rewriteRuleForTargetQueue()

        if rule is None:
            return path

        return re.sub(rule['pattern'], rule['replacement'], path)


class KubernetesConfiguration:

    defaultConf: KubernetesConfiguration | None = None

    @classmethod
    def defaultConfiguration(
            cls,
            path: Optional[str] = None,
            discard_security_context: bool = False,
            garbage_collect: str | None = None,
            archive_objects: str | None = None,
    ) -> KubernetesConfiguration:
        """Returns the default kubernetes environment - required for flow k8s backend to launch jobs

        Args:
            path: Path to file containing k8s configuration that workflow-operator generated,
              defaults to /etc/podinfo/flow-k8s-conf.yml. The expected format of this yml is explained in the
              docstring of the class constructor.
            discard_security_context: Remove instructions to generate security context, defaults to False
            garbage_collect: Controls how to delete Job and Pod objects on task Completion.
                Choices are "all", "failed", "successful", and (None|"none")
            archive_objects: Controls how to store the Job and Pod objects on task Completion.
                Choices are "all", "failed", "successful", and (None|"none")

        Exceptions:
            Raises experiment.errors.ExperimentSetupError if the required environment cannot be configured for some reason
            The error object will contain further information on the underlying error
        """
        path = path or '/etc/podinfo/flow-k8s-conf.yml'

        if KubernetesConfiguration.defaultConf is None:
            return cls.newDefaultConfigurationFromFile(path, discard_security_context,
                                                       garbage_collect, archive_objects)

        return KubernetesConfiguration.defaultConf

    @classmethod
    def newDefaultConfigurationFromFile(
            cls,
            path: str | None,
            discard_security_context: bool = False,
            garbage_collect: str | None = "none",
            archive_objects: str | None = "none",
    ):
        """Builds the default kubernetes environment - required for flow k8s backend to launch jobs

        Args:
            path: Path to file containing k8s configuration that workflow-operator generated,
              defaults to /etc/podinfo/flow-k8s-conf.yml. The expected format of this yml is explained in the
              docstring of the class constructor.
            discard_security_context(bool): Remove instructions to generate security context, defaults to False
            garbage_collect: Controls how to delete Job and Pod objects on task Completion.
                Choices are "all", "failed", "successful", and (None|"none")
            archive_objects: Controls how to store the Job and Pod objects on task Completion.
                Choices are "all", "failed", "successful", and (None|"none")

        Raises:
            experiment.errors.ExperimentSetupError:
                if the required environment cannot be configured for some reason
                The error object will contain further information on the underlying error
        """
        try:
            with open(path, 'r') as f:
                options = experiment.model.frontends.flowir.yaml_load(f)
        except Exception as e:
            raise experiment.model.errors.ExperimentSetupError(
                'Unable to read kubernetes options file at %s' % path, e, instance_dir=None)
        KubernetesConfiguration.defaultConf = KubernetesConfiguration(options, discard_security_context,
                                                                      garbage_collect, archive_objects)
        return KubernetesConfiguration.defaultConf

    @classmethod
    def newDefaultConfiguration(
            cls,
            options: Dict[str, Any],
            discard_security_context: bool = False,
            garbage_collect: str | None = "none",
            archive_objects: str | None = "none",
    ) -> KubernetesConfiguration:
        """Builds the default kubernetes environment - required for flow k8s backend to launch jobs

        Args:
            options: Kubernetes options that the constructor of this class can parse. See the
              docstring of the constructor for the structure of @options.
            discard_security_context: Remove instructions to generate security context, defaults to False
            garbage_collect: Controls how to delete Job and Pod objects on task Completion.
                Choices are "all", "failed", "successful", and (None|"none").
            archive_objects: Controls how to store the Job and Pod objects on task Completion.
                Choices are "all", "failed", "successful", and (None|"none").
        Returns
            The default KubernetesConfiguration instance
        """
        if garbage_collect is None:
            garbage_collect = "none"

        if archive_objects is None:
            archive_objects = "none"

        KubernetesConfiguration.defaultConf = KubernetesConfiguration(options, discard_security_context, garbage_collect, archive_objects)
        return KubernetesConfiguration.defaultConf

    def __init__(self, options: Dict[str, Any], discard_security_context: bool,
                 garbage_collect: str | None, archive_objects: str | None):

        '''Creates an environment that a flow instance running in k8s to launch k8s jobs

        Options Structure:

            Note: Labels can contain arbitrary extra key:value pairs which will be added to metadata.labels of each
               generated Job object

            objectmeta:
              namespace
              labels:
                workflow: <uid of workflow> (REQUIRED label)
                ... more optional labels ...
            spec:
              serviceaccountname: str
              imagepullsecrets: (optional)
                - name: str
              securitycontext:
                fs_group: int
                run_as_group: int
                run_as_non_root: bool
                run_as_user: int
              volumes: List[kubernetes.model.V1Volume]
              containers:
                - volumemounts: List[kubernetes.model.V1VolumeMount]

        Args:
            options: The options for the environment. Must conform to the structure shown below
            discard_security_context: Whether to discard options to generate security-context of Pods
            garbage_collect: Controls how to delete Job and Pod objects on task Completion.
                Choices are "all", "failed", "successful", and "none" (or None literal).
            archive_objects: Controls how to store the Job and Pod objects on task Completion.
                Choices are "all", "failed", "successful", and "none" (or None literal).

        Raises:
            experiment.errors.ExperimentSetupError:
                if the required environment cannot be configured for some reason
                The error object will contain further information on the underlying error
                e.g. if the required option:values pairs are not present
                The error object will have an underling ExperimentMissingConfigurationError describing what is missing
        '''
        # VV: Convert None to "none" for these 2 options
        if garbage_collect is None:
            garbage_collect = "none"

        if archive_objects is None:
            archive_objects = "none"

        choices = ["all", "failed", "successful", "none"]

        if garbage_collect not in choices:
            raise experiment.model.errors.ExperimentSetupError(
                f'Invalid option for Kubernetes garbage collect',
                ValueError(f"kubernetes garbage collect was set to \"{garbage_collect}\", choices are {choices}"),
                instance_dir=None)

        if archive_objects not in choices:
            raise experiment.model.errors.ExperimentSetupError(
                f'Invalid option for Kubernetes archive objects',
                ValueError(f"kubernetes archive objects was set to \"{garbage_collect}\", choices are {choices}"),
                instance_dir=None)

        self.garbage_collect = garbage_collect
        self.archive_objects = archive_objects
        self.raw_options = experiment.model.frontends.flowir.deep_copy(options)
        self.options = {}
        log = logging.getLogger('appenv.kubernetes')

        try:
            label = options['objectmeta']['labels']['workflow']
        except KeyError as e:
            raise experiment.model.errors.ExperimentSetupError(
                'Unable to initialise kubernetes environment',
                experiment.model.errors.ExperimentMissingConfigurationError(
                    'Required kubernetes option workflow not present in /etc/podinfo/flow-k8s-conf.yml'),
                instance_dir=None)

        self.options['serviceaccountname'] = options['spec'].get('serviceaccountname')

        if discard_security_context is False:
            # VV: See kubernetes.client.V1PodSecurityContext for fields. The yaml we receive has somewhat different
            # format but can be mapped to valid V1PodSecurityContext via experiment.k8s.instantiate_k8sObject_from_yaml
            self.options['securityContext'] = options['spec'].get('securitycontext', {})
        else:
            self.options['securityContext'] = {}

        #FIXME: Temporary - map existing options format to desired options format
        self.options['namespace'] = options['objectmeta']['namespace']
        self.options['labels'] = options['objectmeta']['labels']
        self.options['volumes'] = []
        for volume in options['spec']['volumes']:
            if volume['name'] not in ['config-volume', 'git-secrets-package', 'git-sync-package']:
                self.options['volumes'].append(volume)

        for container in options['spec']['containers']:
            if container['name'] == 'elaunch-primary':
                self.options['volumemounts'] = []
                for mount in container['volumemounts']:
                    if mount['name'] not in ['config-volume', 'git-secrets-package', 'git-sync-package']:
                        self.options['volumemounts'].append(mount)

        self.options['imagePullSecrets'] = [ps['name'] for ps in options['spec'].get('imagepullsecrets', [])]

        if len(self.options['volumes']) == 0:
            raise experiment.model.errors.ExperimentSetupError(
                'Unable to initialise kubernetes environment',
                experiment.model.errors.ExperimentMissingConfigurationError(
                    'No volumes defined in kubernetes option YAML'), instance_dir=None)

        if len(self.options['volumemounts']) == 0:
            raise experiment.model.errors.ExperimentSetupError(
                'Unable to initialise kubernetes environment',
                experiment.model.errors.ExperimentMissingConfigurationError(
                    'No volume mounts defined in kubernetes option YAML'), instance_dir=None)

        if len(self.options['imagePullSecrets']) == 0:
            log.warning('No image pull secrets defined in kubernetes option YAML')

        #Check volume-claims v volumemounts
        try:
            claimNames = [v['name'] for v in self.options['volumes']]
            mountNames = [m['name'] for m in self.options['volumemounts']]

            unmountedVolumes = [v for v in claimNames if v not in mountNames]
            nomatchingVolume = [m for m in mountNames if m not in claimNames]
        except KeyError as e:
            raise experiment.model.errors.ExperimentSetupError(
                'Unable to initialise kubernetes environment',
                experiment.model.errors.ExperimentMissingConfigurationError(
                     'Required kubernetes option %s not present volumes/volume_mounts in '
                     '/etc/podinfo/flow-k8s-conf.yml' % e), instance_dir=None)

        if len(unmountedVolumes) > 0:
            log.warning('Volumes %s specified in kubernetes configuration '
                        'have no matching mount points' % unmountedVolumes)

        if len(nomatchingVolume) > 0:
            raise experiment.model.errors.ExperimentSetupError(
                'Unable to initialise kubernetes environment',
                experiment.model.errors.ExperimentMissingConfigurationError(
                    'Volume mounts %s specified in kubernetes'
                    ' configuration have no matching volumes' % nomatchingVolume), instance_dir=None)

        #Finally set restart policy
        self.options['restart_policy'] = os.environ.get('FLOW_KUBE_POD_RESTART_POLICY', 'Never')

        # VV: For more info https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/
        # long story short, we inject `ownerReferences` into jobs that link them with the Workflow object
        # which created the Job (that is, if the workflow object exists).
        self.options['ownerReference'] = self._prepare_ownerReferences_dictionary(options)

    # VV: @tag:K8sOwnerReference
    def _prepare_ownerReferences_dictionary(self, k8s_config_options: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Returns an ownerReference dictionary that can be used to instantiate a
        kubernetes.client.V1OwnerReference object. The dictionary points to the workflow that owns this experiment.

        Args:
            k8s_config_options: The dictionary that st4sd-runtime-k8s prepares representing the pod yaml of the
                host pod (the pod running this process). This dictionary is expected to be what the Golang module
                "gopkg.in/yaml.v3" produces when it marshals the Pod structure.
        """

        """ VV: The objectmeta.ownerreferences field in the k8s-config.yaml looks like this:
        objectmeta:
            ownerreferences:
            - apiversion: st4sd.ibm.com/v1alpha1
              blockownerdeletion: true
              controller: true
              kind: Workflow
              name: homo-lumo-dft-77i0z83e8u2c95
              uid: 4202b7cf-a881-40a8-9768-fcba4d011f54
        """
        log = logging.getLogger('appenv.kubernetes')

        # VV: Look for the workflow object, extract and extract its uid
        try:
            curr_owner_references: List[Dict[str, Any]] = k8s_config_options\
                .get('objectmeta', {})\
                .get("ownerreferences", [])

            if len(curr_owner_references) != 1:
                raise ValueError(f"Expected 1 OwnerReference for host pod, instead got {curr_owner_references}")

            owner = curr_owner_references[0]

            # VV: These match the arguments to kubernetes.client.V1OwnerReference
            return {
                'api_version': owner['apiversion'], 'uid': owner['uid'],
                'block_owner_deletion': True, 'controller': True,
                'kind': owner['kind'], 'name': owner['name'],
            }
        except Exception as e:
            log.log(15, traceback.format_exc)
            log.warning(f"Run into {e} while extracting current ownerReferences "
                        f"- will not inject ownerreferences to minion kubernetes objects")
            return None

    def _generate_kubernetes_custom_objects_api(self):
        # type: (KubernetesConfiguration) -> kubernetes.client.CustomObjectsApi
        """Loads a kubernetes configuration and returns a kubernetes.client.CustomObjectsAPI that
        can be used to look for Workflow objects (or other CustomResourceDefinition objects).
        """
        try:
            kubernetes.config.load_incluster_config()
        except kubernetes.config.ConfigException:
            # VV: Assumes kubernetes v11.0.0
            kubernetes.config.load_kube_config()

        return kubernetes.client.CustomObjectsApi(kubernetes.client.ApiClient())


class DockerConfiguration:
    defaultConf: DockerConfiguration | None = None

    @classmethod
    def defaultConfiguration(
            cls,
            garbage_collect: str | None = None,
            executable: str | None = None,
    ) -> DockerConfiguration:
        """Returns the default docker environment - required for st4sd Docker backend

        Args:
            executable: The path to the "docker"-like executable or the name of the executable if it is under $PATH
            garbage_collect: Controls how to delete Containers on task Completion.
                Choices are "all", "failed", "successful", and (None|"none")

        Exceptions:
            Raises experiment.errors.ExperimentSetupError if the required environment cannot be configured for some
            reason. The error object will contain further information on the underlying error
        """
        if cls.defaultConf is None:
            return cls.newDefaultConfiguration(garbage_collect=garbage_collect, executable=executable)

        return cls.defaultConf

    @classmethod
    def newDefaultConfiguration(
            cls,
            garbage_collect: str | None = "none",
            executable: str | None = None,
    ) -> DockerConfiguration:
        """Builds the default docker environment - required for st4sd Docker backend

        Arguments:
            executable: The path to the "docker"-like executable or the name of the executable if it is under $PATH.
                If unset, defaults to "docker"
            garbage_collect: Controls how to delete Containers on task Completion.
                Choices are "all", "failed", "successful", and (None|"none")

        Returns
            The default DockerConfiguration instance

        Raises:
            experiment.errors.ExperimentSetupError:  if the required environment cannot be configured for some
                reason. The error object will contain further information on the underlying error
        """
        if garbage_collect is None:
            garbage_collect = "none"

        executable = executable or "docker"

        cls.defaultConf = cls(garbage_collect=garbage_collect, executable=executable)
        return cls.defaultConf

    def __init__(self,  executable: str, garbage_collect: str | None = "none"):
        valid_garbage_collect = ['successful', 'failed', 'all', 'none']
        garbage_collect = garbage_collect or "none"

        if garbage_collect not in valid_garbage_collect:
            raise experiment.model.errors.ExperimentSetupError(
                "Unable to configure Docker backend", underlyingError=KeyError(
                    f"garbage_collect={garbage_collect} not one of {valid_garbage_collect}"),
                instance_dir=None)

        self._garbage_collect = garbage_collect
        self._executable = executable

    @property
    def garbage_collect(self) -> str:
        return self._garbage_collect

    @property
    def executable(self) -> str:
        return self._executable
