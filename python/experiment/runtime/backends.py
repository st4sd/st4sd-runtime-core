#
# coding=UTF-8
# Copyright IBM Inc. 2017,2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

'''Contains the Flow facing interfaces for Task subclasses

The includes
- Configuration management i.e. providing information on valid key/values.
- Flow facing interfaces to exposed backend specific commands
- Task Factory Functions (Launchers)'''

from __future__ import print_function
from __future__ import annotations

from typing import Optional, List, Dict, cast, TYPE_CHECKING, Any

import experiment.model.errors

if TYPE_CHECKING:
    from experiment.model.interface import InternalRepresentationAttributes
    from experiment.model.data import Job

import logging
import os
import pprint
from future.utils import raise_with_traceback

import experiment.appenv

import experiment.model.data
import experiment.model.executors
import experiment.model.frontends.flowir
import experiment.model.graph
import experiment.model.interface
import experiment.runtime.backends_base
import experiment.runtime.task
import experiment.runtime.backend_interfaces.localtask
import experiment.runtime.backend_interfaces.lsf
import experiment.runtime.backend_interfaces.k8s
import experiment.runtime.backend_interfaces.task_simulator


def LocalTaskGenerator(componentSpecification,  # type: InternalRepresentationAttributes
                       outputFile=None, errorFile=None):

    job = componentSpecification  # type: InternalRepresentationAttributes
    log = logging.getLogger('generator.local.%s' % componentSpecification.identification.componentName.lower())

    interpreter = job.componentSpecification.commandDetails.get('interpreter', None)

    pre, executor, post = experiment.model.executors.CommandsFromSpecification(componentSpecification)

    outputFile = "out.stdout" if outputFile is None else outputFile
    errorFile = "out.stderr" if errorFile is None else errorFile
    outputFile = os.path.join(executor.workingDir, outputFile)
    errorFile = os.path.join(executor.workingDir, errorFile)

    if interpreter in [None, 'bash', 'javascript', 'cwl', 'cwlcmdline']:
        commandLine = executor.commandLine
    else:
        raise experiment.model.errors.InternalInconsistencyError(
            'LocalTaskGenerator does not know how to handle the "%s" interpreter' % interpreter
        )

    log.info('Starting %s' % commandLine)

    with open(outputFile, 'w+b') as stdout, open(errorFile, 'w+b') as stderr:
        log.debug(executor.environment)
        process = experiment.runtime.backend_interfaces.localtask.LocalTask(commandLine,
                                                                            cwd=executor.workingDir,
                                                                            env=executor.environment,
                                                                            stdout=stdout,
                                                                            stderr=stderr,
                                                                            shell=True)

    return process


def SimulatorTaskGenerator(componentSpecification,  # type: InternalRepresentationAttributes
                           outputFile=None, errorFile=None):
    # type: (experiment.model.data.Job, Optional[str], Optional[str]) -> experiment.runtime.backend_interfaces.task_simulator.SimulatorTask
    pre, executor, post = experiment.model.executors.CommandsFromSpecification(componentSpecification)

    log = logging.getLogger('gen.sim.%s' % componentSpecification.identification.componentName.lower())

    outputFile = "out.stdout" if outputFile is None else outputFile
    outputFile = os.path.join(executor.workingDir, outputFile)


    with open(outputFile, 'a+b') as stdout:
        process = experiment.runtime.backend_interfaces.task_simulator.SimulatorTask(componentSpecification)
        ret = process.expected_return_code
        dur = process.expected_execution_time
        sched = process.expected_scheduling_time
        if 'sim_expected_exit_code' in componentSpecification.customAttributes:
            tokens = componentSpecification.customAttributes['sim_expected_exit_code'].split()
            exit_codes = ', '.join([s.strip() for s in tokens])

            log_string = "New run duration: %s, exit_code: %s, sched_time: %s," \
                         " next_exit_codes: [%s], inputs: [%s], life_partners: [%s]" % (
                             dur, ret, sched,
                             exit_codes,
                             ', '.join([os.sep.join(x.split(os.sep)[-3:]) for x in process.input_dependencies]),
                             ', '.join(
                                 [os.sep.join(x.split(os.sep)[-3:]) for x in process.lifepartner_dependencies])
                            )
        else:
            log_string = "New run duration: %s, exit_code: %s, sched_time: %s," \
                         " inputs: [%s], life_partners: [%s]" % (
                             dur, ret, sched,
                             ', '.join([os.sep.join(x.split(os.sep)[-3:]) for x in process.input_dependencies]),
                             ', '.join(
                                 [os.sep.join(x.split(os.sep)[-3:]) for x in process.lifepartner_dependencies])
                             )

        stdout.write(('%s\n' % log_string).encode('utf-8', 'ignore'))
        log.info(log_string)

    return process


def KubernetesTaskGenerator(
        componentSpecification: experiment.model.interface.InternalRepresentationAttributes,
        outputFile: str | None = None, errorFile: str | None = None, archive_path_prefix: str | None = None):

    '''Creates a k8s task from component specification

    Assumes the k8s options in component-spec are in DOSINI FORMAT

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
        componentSpecification: Specification of the component
        outputFile: path to hold contents of stdout
        errorFile: path to hold contents of stderr - this is actually not used right now, kubernetes does not
            differentiate between stdout and stderr logs
        archive_path_prefix: If k8s is configured to archive objects (appenv.KubernetesConfiguration.archive_objects)
            it generates the objects "${archive_path_prefix}pods.yaml" and "${archive_path_prefix}job.yaml".
            if archive_path_prefix is None then it defaults to "${executor.working_dir}/"
    '''
    log = logging.getLogger('generator.kubernetes.%s' % componentSpecification.identification.identifier.lower())
    log.debug("Creating task for %s" % componentSpecification.identification.identifier.lower())

    # Step 1. Construct the pre-deployment executors and commands
    # This is specific executors defined in the specification
    # Task class is independent of the workflow details (classes etc.)
    # Therefore all explicitly specified pre/post chains, including backend specific options
    # are created outside the Task class.
    pre, executor, post = experiment.model.executors.CommandsFromSpecification(componentSpecification)

    #Step 2. Create the task
    #The executor defines environment variables for all sub-executors and the task-manager (Global mode)
    #resources: Defines resource requests and backend-specific options
    outputFile = "out.stdout" if outputFile is None else outputFile
    errorFile = "out.stderr" if errorFile is None else errorFile
    outputFile = os.path.join(executor.workingDir, outputFile)
    errorFile = os.path.join(executor.workingDir, errorFile)

    # NOTE: WE ASSUME IF USING K8S that flow is running in kubernetes - can change later
    # Hence don't pass flowKubeOptions to base task generator as it will extract them from appenv
    return experiment.runtime.backends_base.KubernetesTaskGenerator(
        executor,
        componentSpecification.resourceManager,
        outputFile,
        resourceRequest=componentSpecification.resourceRequest,
        pre=pre,
        post=post,
        label=componentSpecification.identification.identifier.lower(),
        archive_path_prefix=archive_path_prefix
    )


def LSFTaskGenerator(componentSpecification, #  type: InternalRepresentationAttributes
                     outputFile=None, errorFile=None):

    log = logging.getLogger('generator.lsf.%s' % componentSpecification.identification.identifier.lower())
    log.debug("Creating task for %s" % componentSpecification.identification.identifier.lower())

    #Task class is independent of the workflow details (classes etc.)
    #Therefore all explicitly specified pre/post chains, including backend specific options
    #are created outside the Task class.

    #Step 1. Construct the pre-deployment executors and commands
    #This is specific executors defined in the specification
    pre, executor, post = experiment.model.executors.CommandsFromSpecification(componentSpecification)

    #Step 2. Acquire meta-information not obtainable from LSF
    #In particular LSF cant work out if a job will execute on cluster with a remote file system
    #unless it has DM requirements
    #This means it can't add custom stage-in/stage-out commands to these jobs
    #unless we tell it they're remote
    hybridConf =  experiment.appenv.HybridConfiguration.defaultConfiguration()
    isHybrid = False
    #FIXME: Probably don't need to be checking this in lsf.Task at all
    checkCWD = True
    resourceManager = componentSpecification.resourceManager
    queue = resourceManager['lsf']["queue"]
    if hybridConf.isHybrid:
        isHybrid = True
        checkCWD = False
        # Set rewrite rules for all executors
        # Executors that don't support rewriting after init will ignore it
        # Any such executors support rewriting by accessing HybridEnvironment in
        # their commandFromOptionsAndSpecification classmethod
        # A rule of None is allowed by the API - this causes nothing to happen
        for j in pre + [executor] + post:
            j.setRewriteRule(hybridConf.rewriteRuleForQueue(queue))

    #Step 3. Create the task
    #The executor defines environment variables for all sub-executors and the task-manager (Global mode)
    #resources: Defines resource requests and backend-specific options
    outputFile = "out.stdout" if outputFile is None else outputFile
    errorFile = "out.stderr" if errorFile is None else errorFile
    outputFile = os.path.join(executor.workingDir, outputFile)
    errorFile = os.path.join(executor.workingDir, errorFile)

    # VV: Walltime is used by at least one more backend (kubernetes)
    options = resourceManager['lsf']
    if 'walltime' in resourceManager['config']:
        options['walltime'] = resourceManager['config']['walltime']

    with open(outputFile, 'w+b') as stdout:
        task = experiment.runtime.backend_interfaces.lsf.Task(executor,
                                                              preCommands=pre,
                                                              postCommands=post,
                                                              options=options,
                                                              resourceRequest=componentSpecification.resourceRequest,
                                                              stdout=stdout,
                                                              stderr=stdout,
                                                              waitOnOutput=True,
                                                              isHybrid=isHybrid,
                                                              checkCWD=checkCWD)

    return task


def InitialiseBackendsForWorkflow(
        workflowGraph: experiment.model.graph.WorkflowGraph,
        ignoreInitializeBackendError: bool = False,
        logger: logging.Logger | None = None, **kwargs):
    '''Initialise backends for the components found in workflowGraph

    Args:
        workflowGraph: The graph of the workflow
        ignoreInitializeBackendError: Report errors during initialization of backend as warnings instead of
          raising exception for them
        logger: Logger to use to print warnings, will be created if set to None
        **kwargs: key:value arguments to propagate to all Backend initializers
    Returns:
        None
    '''

    for backend in workflowGraph.active_backends():
        if backend in backendInitializerMap:
            try:
                backendInitializerMap[backend](workflowGraph, **kwargs)
            except Exception as e:
                if ignoreInitializeBackendError:
                    logger = logger or logging.getLogger('InitBackends')
                    logger.warning("Ran into error %s while initializing backend %s - ignoreInitializeBackendError"
                                   " parameter has been set, will not raise an exception" % (e, backend))
                else:
                    raise_with_traceback(e)


def LSFInitializer(workflowGraph, **kwargs):
    # type: (experiment.model.graph.WorkflowGraph, Dict[str, Any]) -> None
    '''Initialises the LSF backend'''
    # VV: Print information about available LSF Errors and whether the cluster is multi-cluster or not
    log = logging.getLogger("lsf")
    log.debug(f"LSF Version is {experiment.runtime.backend_interfaces.lsf.LSF_VERSION}, "
              f"available LSF codes:\n{pprint.pformat(experiment.runtime.backend_interfaces.lsf.lsfAvailableCodes)}")
    if "TERM_RC_RECALL" not in experiment.runtime.backend_interfaces.lsf.lsfAvailableCodes:
        log.debug("lsf.TERM_RC_RECALL is unknown, this is not a multi-cluster setup")
    else:
        log.debug("lsf.TERM_RC_RECALL is available, this is a multi-cluster setup")

    # Initialise the arbitrator
    experiment.runtime.backend_interfaces.lsf.LSFRequestArbitrator.defaultRequestArbitrator(loggingDir=workflowGraph.rootStorage.location)


def KubernetesInitializer(
        workflowGraph: experiment.model.graph.WorkflowGraph,
        k8s_no_sec_context: bool = False,
        k8s_config_path: str | None = None,
        k8s_store_path: str | None = None,
        k8s_garbage_collect: str | None = None,
        k8s_archive_objects: str | None = None,
        **kwargs):
    """Initialises the k8s backend

    It can be asked to also create a copy of the k8s config file.

    Elaunch can use this feature so that cexecute can access the copy of the file.
    Specifically, elaunch would load /etc/podinfo/flow-k8s-conf.yml and then create a copy of the file under
    $INSTANCE_DIR/conf/k8s-config.yaml

    Args:
        workflowGraph: a WorkflowGraph instance (currently not used but is part of initializer protocol)
        k8s_no_sec_context: Setting this to True will instruct flow not to inject securityContext specs in pods
        k8s_config_path: Path to the k8s configuration to load (
            default value is the default value of the path argument to the
            experiment.appenv.KubernetesConfiguration.defaultConfiguration method)
        k8s_store_path: If set, KubernetesInitializer will copy the contents of @k8s_config_path to
            the new file under @k8s_store_path
        k8s_garbage_collect: Controls how to delete Job and Pod objects on task Completion.
            Choices are "all", "failed", "successful", and (None|"none")
        k8s_archive_objects: Controls how to store the Job and Pod objects on task Completion.
            Choices are "all", "failed", "successful", and (None|"none")
    """
    default_k8s = experiment.appenv.KubernetesConfiguration.defaultConfiguration(
        path=k8s_config_path, discard_security_context=k8s_no_sec_context,
        garbage_collect=k8s_garbage_collect, archive_objects=k8s_archive_objects)

    if k8s_store_path:
        # VV: Now store the dictionary under @k8s_store_path for use in the future (e.g cexecute.py)
        with open(k8s_store_path, 'w') as f:
            experiment.model.frontends.flowir.yaml_dump(default_k8s.raw_options, f)

backendGeneratorMap = {
    'simulator': SimulatorTaskGenerator,
    'lsf': LSFTaskGenerator,
    'local': LocalTaskGenerator,
    'kubernetes': KubernetesTaskGenerator
}

backendInitializerMap = {
    'lsf': LSFInitializer,
    'kubernetes': KubernetesInitializer,
}

backendTaskMap = {
    'simulator': experiment.runtime.backend_interfaces.task_simulator.SimulatorTask,
    'lsf': experiment.runtime.backend_interfaces.lsf.Task,
    'local': experiment.runtime.backend_interfaces.localtask.LocalTask,
    'kubernetes': experiment.runtime.backend_interfaces.k8s.NativeScheduledTask
}


def GenerateTaskFromSpecification(specification  # type: InternalRepresentationAttributes
                                  ):

    '''Creates and launches a last given a component specification

    Note:

    The graph specification comes from must be at L2 (storage specified) to guarantee most functionality.
    This is not checked.

    If specification is of type experiment.data.ComponentInstance this is guaranteed

    Params:
        specification: An object conforming to experiment.interfaces.InternalRepresentationAttributes instance

    Exceptions:
        Raise ValueError if the components backend ('type' parameter) is unknown '''

    backend = specification.resourceManager['config']['backend']

    try:
        task = backendGeneratorMap[backend](specification)
    except KeyError as error:
        raise ValueError('Unknown backend %s' % backend)

    return task


class SimulatorCommandFlow(experiment.runtime.backend_interfaces.lsf.DMStageInCommand):

    '''Flow side interface to DMStageInCommand'''

    classLog = logging.getLogger("backend.SimulatorCommandFLow")

    @classmethod
    def commandFromOptionsAndSpecification(cls, options,
                                           componentSpecification  # type: InternalRepresentationAttributes
                                           ):

       return experiment.model.executors.Command(
           '/usr/bin/echo', arguments='hello world'
       )


class DMStageInCommandFlow(experiment.runtime.backend_interfaces.lsf.DMStageInCommand):

    '''Flow side interface to DMStageInCommand'''

    classLog = logging.getLogger("backend.DMStageInCommandFlow")

    @classmethod
    def commandFromOptionsAndSpecification(cls, options,
                                           componentSpecification  # type: InternalRepresentationAttributes
                                           ):

        '''This method converts Flow lsf-dm-stage-in request into LSF stage in command

        This includes
        - Converting references to paths
        - Setting source and destination paths based on reference type
        - Converting paths to remote system

        Note: It is assumed that any copy refs will be staged locally before the DMStageInCommand
        is executed.

        Raises:
            See DMStageInCommandTester for exceptions raised'''

        import experiment.model

        # FIXME: This also will stage on every launch ...
        # Need to discriminate files within a stage and files outside

        references = cast(List[experiment.model.graph.DataReference], componentSpecification.dataReferences)

        #Although this may have been called on the component specification already
        #e.g. during a component verification stage, this is not ensured
        DMStageInCommandTester(options, componentSpecification)

        stageInString = options["payload"]
        payloads = {'sources':[],
                           'payload':[]}

        cls.classLog.info('rstage-in string: %s' % stageInString)
        # Only copy in reference specified in the rstage-in string
        # Note all potential issues should be detected in DMStageInCommandTester so can proceed here
        # without testing various cases
        for reference in references:
            if stageInString.find(reference.stringRepresentation) != -1:
                cls.classLog.debug('rstage-in ref: %s', reference.stringRepresentation)

                #Ref methods
                #copy - handled
                #ref - handled
                #copyout - handled
                #link - not valid (code won't get here)
                #extract - not handled yet (code won't get here)

                # Direct references will have been staged already as they prexist - skip ':ref' type refs to them
                # NOTE: Still need to handle copy refs to them hence we don't filter out direct references at start.
                # In general we only want to stage-in data for components which aren't on the same system
                # Direct refs and refs to components on that system shouldn't be part of stagein
                # References to components also on remote systems are errors and will be detected above
                if reference.method == "ref" and not reference.isDirectReference(componentSpecification.workflowGraph):
                    payloads['sources'].append(reference.path)
                else:
                    #Method is copy or copyout
                    #For these methods the reference we assume the data is in the component dir
                    #i.e. local stage-in is performed before DMStageInCommand is executed
                    basename = os.path.basename(reference.path)
                    source = os.path.join(componentSpecification.command.workingDir, basename)
                    payloads['payload'].append((source, componentSpecification.command.workingDir))

        hybridConf = experiment.appenv.HybridConfiguration.defaultConfiguration()
        queue = componentSpecification.resourceManager['lsf']["queue"]
        rule = hybridConf.rewriteRuleForQueue(queue)
        if rule is None:
            cls.classLog.critical("No rewrite rule for implied remote queue %s specified by component %s. "
                                  "This could be a potential logical error in the declaration of the Package. "
                                  "Flow will assume that the user intentionally selected a queue for which there is "
                                  "no rewrite rule."
                              % (queue, componentSpecification.identification.identifier))

        payloads['workingDir'] = componentSpecification.command.workingDir
        return DMStageInCommandFlow.commandFromOptionsAndRewriteRule(payloads, rule)

class DMStageOutCommandFlow(experiment.runtime.backend_interfaces.lsf.DMStageOutCommand):

    '''Flow side interface to DMStageInCommand'''

    classLog = logging.getLogger("backend.DMStageOutCommandFlow")

    @classmethod
    def commandFromOptionsAndSpecification(cls, options,
                                           componentSpecification  # type: InternalRepresentationAttributes
                                           ):

        '''This method converts Flow lsf-dm-stage-out options into DMStageOutCommand

        This includes
            - Handling the 'all' keyword
            - Setting src/dst path
        '''

        stageOutString = options["payload"]
        stageOutRefs = stageOutString.split()

        hybridConf = experiment.appenv.HybridConfiguration.defaultConfiguration()
        queue = componentSpecification.resourceManager['lsf']["queue"]
        rule = hybridConf.rewriteRuleForQueue(queue)
        if rule is None:
            cls.classLog.warning('No rewrite rule for implied remote queue %s specified by component %s'
                              % (queue, componentSpecification.identification.identifier))

        command = None
        if stageOutRefs is not None:
            if "all" in stageOutRefs:
               command = experiment.runtime.backend_interfaces.lsf.DMStageOutDirectoryCommand.commandFromOptionsAndRewriteRule(
                                        {'destination':componentSpecification.command.workingDir},
                                        rule)
            else:
                payload = []
                for ref in stageOutRefs:
                    payload.append(ref)

                command = DMStageOutCommandFlow.commandFromOptionsAndRewriteRule(
                                {'destinations':payload,
                                 'outputDir':componentSpecification.command.workingDir},
                                rule=rule)

        return command

def DMStageInCommandTester(options, # type: Dict
                componentSpecification  #  type: experiment.model.interface.InternalRepresentationAttributes
                     ):

    '''Performs the following test and checks on the stage in payload

    - All payload references are in the components reference list
    - Copy references in components reference list are in the payload (otherwise potentially could be missing data)
    - Identifies payload references which would cause other components data to be overwritten
    - Identifies invalid payload references:
            - references to component directories (can't stage a directory)
            - copy references to data in the same stage (data potentially won't exist so staging could fail)
            (This will probably be detected elsewhere)
    '''

    appenv = experiment.appenv.HybridConfiguration.defaultConfiguration()
    dataReferenceStrings = [ref.stringRepresentation for ref in componentSpecification.dataReferences]

    stageInRefs = options['payload'].split()

    log = logging.getLogger("backend.DMStageInCommandTester")

    for ref in stageInRefs:
        # Raise error if payload reference not in reference list (stage-in syntax/specification error)
        if ref not in dataReferenceStrings:
            raise DMStageInPayloadSpecificationError("Payload reference %s not in reference list" % ref)

    #First tests on component references
    producers = componentSpecification.producers
    for ref in componentSpecification.componentDataReferences:
        if ref.stringRepresentation in stageInRefs:

            #Raise error if this reference is of type "ref" to a file in a component that is executing remotely
            #This is an overwrite error
            if ref.fileRef is not None and ref.method == "ref":
                producerRemote = True if producers[ref].resourceManager['lsf']["queue"] == appenv.remoteQueue else False
                if producerRemote:
                    raise DMStageInPotentialOverwriteError(
                        "Payload reference %s would copy local files of a component that is executing remotely to \
                        that components remote directory." % ref.stringRepresentation)

            # Raise Error if payload reference is of :ref type but not to a file
            #  e.g. a component dir (CannotStageDirectory error)'''
            if  ref.fileRef is None and ref.method == "ref":
                raise DMStageInInvalidPayloadReferenceError(
                    "You cannot specify directories of other components (payload reference: %s)" % ref.stringRepresentation)

            # Raise error if payload reference is of type :copy to a component in the same stage
            if ref.method == "copy" and ref.stageIndex == componentSpecification.identification.stageIndex:
                    raise DMStageInInvalidPayloadReferenceError(
                        "You cannot specify copy references to data of components in the same stage (payload reference: %s)" % ref.stringRepresentation)

            # Emit Warning if component contains a copy ref to another remote component.
            #   The first components data will have to be staged out
            #FIXME: It would be better to perform this check on stage-out but we have no-downstream connections in the graph
            if ref.method == "copy":
                producerRemote = True if producers[ref].resourceManager['lsf']["queue"] == appenv.remoteQueue else False
                filename = ref.fileRef
                if producerRemote:
                    try:
                        # VV: Pre/Main/Post executors are Lists of Dictionaries, so search for the unique `lsf-dm-out`
                        #     post-command
                        lsf_dm_out = [executor for executor in producers[ref].executors['post'] if executor.get('name') == 'lsf-dm-out']

                        producerPostPayload = lsf_dm_out[0]['payload']  # type: str

                        if producerPostPayload != 'all' and filename not in producerPostPayload.split():
                            raise DMStageInPayloadChainError(
                                'Producer %s in stage %s does not stage out required data. Ref %s' % \
                                    (producers[ref].identification.identifier, producers[ref].identification.stageIndex, ref.stringRepresentation))
                    except IndexError:
                        raise DMStageInPayloadChainError(
                            'Producer %s in stage %s does not stage out required data. Ref %s' % \
                            (producers[ref].identification.identifier, producers[ref].identification.stageIndex, ref.stringRepresentation))

    #Checks applicable to all reference types
    for ref in componentSpecification.dataReferences:
        if ref.stringRepresentation in stageInRefs:
            if ref.method == "link":
                raise DMStageInInvalidPayloadReferenceError("The link reference method cannot be used with remote components")

            if ref.method == "extract":
                raise DMStageInInvalidPayloadReferenceError("The extract reference method is not yet supported with remote components")
        elif ref.method == "copy":
            # Raise error if stage-in does not contain copy references that are present in the reference list(missing data error)
            raise DMStageInPayloadSpecificationError(
                "Payload missing component copy reference %s. This data will not be on remote host" % ref.stringRepresentation)


def SimulatorCommandTester(options,  # type: Dict
                    componentSpecification  # type: #  type: experiment.interface.InternalRepresentationAttributes
                     ):

    pass


def DMStageOutCommandTester(options,  # type: Dict
                    componentSpecification  # type: #  type: experiment.interface.InternalRepresentationAttributes
                     ):

    pass

class DMStageInPayloadChainError(experiment.model.errors.ExecutorOptionsError):

    '''Raises when a components producer does not specify DM commands required for its own DM commands to work'''

    pass

class DMStageInPayloadSpecificationError(experiment.model.errors.ExecutorOptionsError):

    '''Raised when there are items missing from the payload'''

    pass

class DMStageInInvalidPayloadReferenceError(experiment.model.errors.ExecutorOptionsError):

    '''Raised when there are errors with particular references in the payload'''

    pass

class DMStageInPotentialOverwriteError(experiment.model.errors.ExecutorOptionsError):

    '''Raised when a payload reference could lead to data being overwritten'''

    pass

registry = experiment.model.executors.Registry.registryManager()

registry.addExecutor(DMStageInCommandFlow,
                     'lsf-dm-in',
                     namespace='lsf')
registry.addExecutor(DMStageOutCommandFlow,
                     'lsf-dm-out',
                     namespace='lsf')
registry.addTester(DMStageInCommandTester,
                    'lsf-dm-in',
                     namespace='lsf')
registry.addTester(DMStageOutCommandTester,
                   'lsf-dm-out',
                   namespace='lsf')

registry.addExecutor(SimulatorCommandFlow,
                     'lsf-dm-in',
                     namespace='simulator')
registry.addExecutor(SimulatorCommandFlow,
                     'lsf-dm-out',
                     namespace='simulator')
registry.addTester(SimulatorCommandTester,
                     'lsf-dm-in',
                     namespace='simulator')
registry.addTester(SimulatorCommandTester,
                     'lsf-dm-out',
                     namespace='simulator')
