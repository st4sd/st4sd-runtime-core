# Copyright IBM Inc. 2015, 2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston


"""This is Work In Progress

The DSL v2.0.0 is evolving, below we describe the schema v0.1.0 with hints about v0.2.0 (i.e.
support for using more than 1 namespaces).

The namespace is a collection of Workflow and Component blueprints.

A namespace optionally contains an "entrypoint" that explains to execute one Workflow or one Component instance
whose definition is part of the namespace.

Workflow and Component blueprints have a "signature" which consists of a unique name and a list of parameters.

A Workflow blueprint explains how to instantiate other Workflow or Component blueprints and use them as
"steps". For each step, the workflow describes how to assign arguments to the parameters of the step. The
arguments can be literals, parameters of the parent workflow, or outputs of other steps. Steps cannot form
direct, or indirect, cycles.

The definition of Namespace is:

entrypoint: # describes how to run this namespace
  entry-instance: the-identifier-of-a-class
  execute: # the same as workflow.execute with the difference that there is always 1 step
    # and the name of the step is always <entry-instance>
    - target: "<entry-instance>"
      args: # Instantiate the class
        parameter-name: parameter value # see notes

workflows: # Available Workflow Classes
  - signature: # Identical to the schema of a signature in a Component class
      name: the-identifier-of-the-class
      parameters:
        - name: parameter-name
          default: an optional default value (string)

    # The names of all the steps in this Workflow class
    # Each step is the instance of a Class (either Workflow, or Component)
    steps:
      # The class name may reference a class in this namespace
      # or an imported namespace.
      # Examples:
      # #   the-step-name: foo.bar  # reference bar from namespace foo (when we add support for namespaces)
      #   the-step-name: bar      # reference bar from current namespace
      the-step-name: the-class-name

    # How to execute each step.
    # Step execution order is determined by their dependencies not by the order here.
    execute:
      # reference to the step to execute enclosed in "<" and ">", notice that
      - target: "<the-step-name>"
        args: # The values to pass to the step
          parameter-name: parameter-value # see notes

components: # Available Component Classes
  - signature: # identical to the schema of a signature in a Workflow class
      name: the identifier of the class
      parameters:
        - name: parameter name
          default: an optional default value (string or dictionary)
    # The component contains more top-level fields in addition to `signature`.
    # We discuss most of those fields in our documentation website:
    #     https://st4sd.github.io/overview/workflow-specification/#component
    # of the fields in the above link, the following are NOT part of DSL 2.0:
    #   references
    #   name

# VV: `import` and `identifier` are WIP at the moment
# identifier: the identifier of the current namespace
# import:
#   # Enables the current namespace to use Workflows and Components that
#   # other namespaces define
#   - a list of namespace identifiers
"""

import typing

import pydantic
import experiment.model.codes
import pydantic.typing


ParameterValueType = typing.Optional[typing.Union[str, float, int, typing.Dict[str, typing.Any]]]

TargetReference = pydantic.constr(regex=r"<[\.A-Za-z0-9_-]+>")
ParameterReference = pydantic.constr(regex=r'%\([a-zA-Z0-9_\.-]+\)s')
MaxRestarts = pydantic.conint(gt=-2)
BackendType = pydantic.constr(regex=r'(%\([a-zA-Z0-9_\.-]+\)s|docker|local|lsf|kubernetes)')
K8sQosType = pydantic.constr(regex=r'(%\([a-zA-Z0-9_\.-]+\)s|guaranteed|burstable|besteffort)')
DockerImagePullPolicy = pydantic.constr(regex=r'(%\([a-zA-Z0-9_\.-]+\)s|Always|Never|IfNotPresent)')
ResourceRequestFloat = pydantic.confloat(ge=0)
ResourceRequestInt = pydantic.conint(ge=0)
EnvironmentReference = pydantic.constr(regex=r'(%\([a-zA-Z0-9_\.-]+\)s|none|environment)')


class Parameter(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    name: str = pydantic.Field(
        description="The name of the parameter, must be unique in the parent Signature"
    )
    default: typing.Optional[ParameterValueType] = pydantic.Field(
        description="The default value of the parameter",
    )


class InstantiatedParameter(Parameter):
    value: typing.Optional[ParameterValueType] = pydantic.Field(
        description="The value of the parameter, if unset defaults to @default"
    )

    def get_value(self) -> typing.Optional[ParameterValueType]:
        """The value of the parameter, if unset defaults to @default

        Returns:
            The value of the parameter, if unset defaults to @default
        """
        if 'value' in self.__fields_set__:
            return self.value
        return self.default


class Signature(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    name: str = pydantic.Field(
        description="The name of the blueprint, must be unique in the parent namespace"
    )
    parameters: typing.List[Parameter] = pydantic.Field(
        description="The collection of the parameters to the blueprint"
    )


class InstantiatedSignature(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    name: str = pydantic.Field(
        description="The name of the blueprint, must be unique in the parent namespace"
    )
    parameters: typing.List[InstantiatedParameter] = pydantic.Field(
        description="The collection of the parameters to the blueprint"
    )


class ExecuteStep(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    target: TargetReference = pydantic.Field(
        description="Reference to a step name. A string enclosed in <> e.g. <foo>", min_length=3
    )

    args: typing.Optional[typing.Dict[str, ParameterValueType]] = pydantic.Field(
        None, description="How to assign values to the parameters of the step. "
                          "Each entry is a key: value pair where the key is the name of a step parameter "
                          "and the value the desired value of the step parameter",
    )


class Workflow(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    signature: Signature = pydantic.Field(
        description="The Signature of the Workflow blueprint"
    )

    steps: typing.Optional[typing.Dict[str, str]] = pydantic.Field(
        None,
        description="Instantiated Blueprints that execute as steps of the parent workflow. "
                    "key: value pairs where the key is the name of the Instance and the value is the name "
                    "of the Blueprint from which to create the Instance."
    )

    execute: typing.Optional[typing.List[ExecuteStep]] = pydantic.Field(
        None,
        description="How to populate the parameters of Steps"
    )


class CMemoizationDisable(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    strong: typing.Union[bool, ParameterReference] = pydantic.Field(
        False,
        description="Whether to disable the memoization check for this component during strong-memoization"
    )

    fuzzy: typing.Union[bool, ParameterReference] = pydantic.Field(
        False,
        description="Whether to disable the memoization check for this component during fuzzy-memoization"
    )


class CMemoization(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    embeddingFunction: typing.Optional[str] = pydantic.Field(
        None,
        description="(Beta) A javascript method which returns a custom memoization value that is used during \"fuzzy\" "
                    "memoization (advanced feature)."
    )
    disable: CMemoizationDisable = pydantic.Field(
        default_factory=CMemoizationDisable,
        description="Whether to skip the memoization check for this Component"
    )


class CResourceManagerConfig(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    backend: BackendType = pydantic.Field(
        "local",
        description="Which backend to use"
    )

    walltime: typing.Union[float, ParameterReference] = pydantic.Field(
        60.0,
        description="Maximum execution time (in minutes)"
    )


class CResourceManagerLSF(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    statusRequestInterval: pydantic.conint(ge=20) = pydantic.Field(
        20,
        description="How many seconds to wait between polling the status of LSF tasks"
    )

    queue: str = pydantic.Field(
        "normal",
        description="Name of the LSF queue"
    )

    resourceString: typing.Optional[str] = pydantic.Field(
        None,
        description="Additional options to include in the resourceRequest of the LSF task"
    )

    reservation: typing.Optional[str] = pydantic.Field(
        None,
        description="The reservation identifier to associate with the LSF task"
    )

    @pydantic.root_validator(pre=True)
    def val_hide_keys(cls, model: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
        if not isinstance(model, typing.Dict):
            return model

        for key in ["dockerImage", "dockerProfileApp", "dockerOptions"]:
            try:
                del model[key]
            except KeyError:
                pass

        return model



class CResourceManagerKubernetes(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    image: typing.Optional[str] = pydantic.Field(
        None,
        description="The image to use when executing this Kubernetes task"
    )
    qos: typing.Optional[K8sQosType] = pydantic.Field(
        None,
        description="The desired Quality Of Service target for the Task"
    )

    cpuUnitsPerCore: typing.Optional[typing.Union[float, ParameterReference]] = pydantic.Field(
        None,
        description="A number which when multiplied by "
                    "(resourceRequest.numberProcesses*resourceRequest.numberThreads)/resourceRequest.threadsPerCore to "
                    "generates the CPU Units that will be the CPU request for Tasks of this component"
    )

    # VV: This has an implied default which backends_base.py sets to 30 via `terminationGracePeriodSeconds`
    gracePeriod: typing.Optional[typing.Union[float, ParameterReference]] = pydantic.Field(
        None,
        description="When Kubernetes requests the Task to terminate it will wait for gracePeriod before it forcibly "
                    "terminates it"
    )

    podSpec: typing.Optional[typing.Dict[str, typing.Any]] = pydantic.Field(
        None,
        description="(Beta) A template Pod blueprint to use for generating the pods of Tasks for this component "
                    "(advanced feature)."
    )

    @pydantic.validator("qos", pre=True)
    def val_to_lowercase(cls, value: typing.Optional[str]) -> typing.Optional[str]:
        if isinstance(value, str):
            value = value.lower()

        return value

    @pydantic.root_validator(pre=True)
    def val_hide_keys(cls, model: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
        if not isinstance(model, typing.Dict):
            return model

        for key in ["image-pull-secret", "namespace", "api-key-var", "host"]:
            try:
                del model[key]
            except KeyError:
                pass

        return model


class CResourceManagerDocker(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    image: typing.Optional[typing.Union[ParameterReference, str]] = pydantic.Field(
        None,
        description="The image to use when executing this Task using a Docker-like container runtime"
    )

    imagePullPolicy: typing.Optional[DockerImagePullPolicy] = pydantic.Field(
        None,
        description="Configures the image pull policy for this Task. The logic is similar to the one that Kubernetes "
                    "uses."
    )

    platform: typing.Optional[typing.Union[ParameterReference, str]] = pydantic.Field(
        None,
        description="Configure the docker-like container runtime system to execute the container for a specific "
                    "CPU platform"
    )


class CResourceManager(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    config: CResourceManagerConfig = pydantic.Field(
        default_factory=CResourceManagerConfig,
        description="High level backend configuration"
    )

    lsf: typing.Optional[CResourceManagerLSF] = pydantic.Field(
        None,
        description="Configuration for the LSF backend. In use when config.backend is set to \"lsf\""
    )

    kubernetes: typing.Optional[CResourceManagerKubernetes] = pydantic.Field(
        None,
        description="Configuration for the Kubernetes backend. In use when config.backend is set to \"kubernetes\""
    )

    docker: typing.Optional[CResourceManagerDocker] = pydantic.Field(
        None,
        description="Configuration for the Docker-like backend. In use when config.backend is set to \"docker\""
    )


class CWorkflowAttributes(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    # VV: TODO Do we want to hide stage and repeatInterval in v0.1.0 ? - the `stage` field seems like it should be
    # part of a Signature - likely the Workflow's signature which we then propagate to Steps of the workflow.
    # Nested Workflows add the stage of their parent workflow before propagating down to their steps.
    stage: typing.Optional[int] = pydantic.Field(
        None,
        description="The stage within which to group this component. This is a logical group which only plays a role "
                    "in the scheduling order of \"repeating\" components and Subject->Observer dependencies."
    )

    restartHookFile: typing.Optional[str] = pydantic.Field(
        None,
        description="The name of the python file under the `hooks` directory which contains the "
                    "Restart() python method."
    )

    restartHookOn: typing.List[str] = pydantic.Field(
        default_factory=lambda :[experiment.model.codes.exitReasons['ResourceExhausted']],
        description="On which exitReasons to trigger the execution of the Restart() method in the restartHookFile"
    )

    aggregate: typing.Union[bool, ParameterReference] = pydantic.Field(
        False,
        description="Whether to terminate the replication of upstream \"replicating\" components"
    )

    replicate: typing.Union[None, int, ParameterReference] = pydantic.Field(
        None,
        description="How many times to replicate this component and downstream components which do not \"aggregate\"."
    )

    repeatInterval: typing.Optional[typing.Union[float, ParameterReference]] = pydantic.Field(
        None,
        description="When a number, and greater than zero it turns this component into a Repeating component. "
                    "Upstream Components which are not in the same stage remain Producers of this Consumer component. "
                    "Upstream Components which are in the same stage become Subjects of this Observer component. "
                    "This component will become ready to execute when all its producers *finish* executing and all its "
                    "subjects *start* executing"
    )

    shutdownOn: typing.List[str] = pydantic.Field(
        [],
        description="A collection of exitReasons for tasks of this Component that should trigger the runtime to "
                    "Shutdown this component instead of how it would normally handle it"
    )

    maxRestarts: typing.Optional[typing.Union[MaxRestarts, ParameterReference]] = pydantic.Field(
        None,
        description="Maximum number of restarts for a Component. -1 and None mean infinite restarts",
    )

    repeatRetries: typing.Optional[int] = pydantic.Field(
        None,
        description="How many times to repeat a \"repairing\" Component after all its "
                    "Subjects have completed and before the Component has a successful task"
    )

    memoization: typing.Optional[CMemoization] = pydantic.Field(
        default_factory=CMemoization,
        description="Settings for the memoization subsystem of the runtime"
    )

    @pydantic.root_validator(pre=True)
    def val_hide_keys(cls, model: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
        if not isinstance(model, typing.Dict):
            return model

        for key in ["isMigratable", "isMigrated", "optimizer", "isRepeat", "name"]:
            try:
                del model[key]
            except KeyError:
                pass

        return model


class CResourceRequest(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    numberProcesses: typing.Union[ResourceRequestInt, ParameterReference] = pydantic.Field(
        1,
        description="The number of processes"
    )

    ranksPerNode: typing.Union[ResourceRequestInt, ParameterReference] = pydantic.Field(
        1,
        description="How many processes to fit in 1 compute node"
    )

    numberThreads: typing.Union[ResourceRequestInt, ParameterReference] = pydantic.Field(
        1,
        description="The number of threads"
    )

    threadsPerCore: typing.Union[ResourceRequestInt, ParameterReference] = pydantic.Field(
        1,
        description="The number of threads to fit in 1 core"
    )

    memory: typing.Optional[typing.Union[int, ParameterReference, str]] = pydantic.Field(
        None,
        description="The memory request in bytes. Also accepts subset of Kubernetes-style strings like 2Gi and 512Mi"
    )

    gpus: typing.Optional[typing.Union[int, ParameterReference, str]] = pydantic.Field(
        None,
        description="The number of gpus to request. Note: not all backends support this. "
                    "For best results, please consult the documentation of st4sd"
    )


class CExecutorPre(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    name: pydantic.typing.Literal['lsf-dm-in'] = pydantic.Field(
        'lsf-dm-in',
        description="The name of the pre-executor"
    )
    payload : str = pydantic.Field(
        description="The payload to the pre-executor"
    )


class CExecutorMain(pydantic.BaseModel):
    class Config:
        extra = "forbid"


class CExecutorPost(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    name: pydantic.typing.Literal['lsf-dm-out'] = pydantic.Field(
        'lsf-dm-out',
        description="The name of the post-executor"
    )
    payload : str = pydantic.Field(
        description="The payload to the post-executor"
    )



class CCommand(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    executable: typing.Optional[typing.Union[ParameterReference, str]] = pydantic.Field(
        None,
        description="The executable"
    )

    interpreter: typing.Optional[typing.Union[ParameterReference, str]] = pydantic.Field(
        None,
        description="(Beta) The name of an interpreter to use (advanced feature)"
    )

    arguments: typing.Optional[typing.Union[ParameterReference, str]] = pydantic.Field(
        None,
        description="The arguments to the executable, or interpreter"
    )

    environment: typing.Optional[typing.Union[EnvironmentReference, typing.Dict[str, typing.Any]]] = pydantic.Field(
        None,
        description="The environment to use. If set to the literal  \"none\" then the environment is empty. If set to "
                    "the literal \"environment\" then it receives the default environment. If it references a parameter "
                    "then it must reference a parameter whose value is either one of the 2 literals or a dictionary "
                    "of environment variables and their values. Otherwise, it can be a dictionary of environment "
                    "variables and their values."
    )

    resolvePath: typing.Optional[typing.Union[bool, ParameterReference]] = pydantic.Field(
        True,
        description="When True, instructs the runtime to follow links when resolving the executable path"
    )

    expandArguments: typing.Optional[
        typing.Union[ParameterReference,
        pydantic.typing.Literal["double-quote", "none"]]
    ] = pydantic.Field(
        "double-quote",
        description="When set to \"double-quote\" it instructs the runtime to expand the arguments to tasks using the "
                    "shell command echo. When set to \"none\" the runtime does not expand the arguments at all."
    )




class CExecutors(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    pre: typing.List[CExecutorPre] = pydantic.Field(
        [],
        description="(Beta) pre-executors (advanced feature)"
    )

    main: typing.List[CExecutorMain] = pydantic.Field(
        [],
        description="(Beta) main-executors (advanced feature)"
    )

    post: typing.List[CExecutorPost] = pydantic.Field(
        [],
        description="(Beta) post-executors (advanced feature)"
    )


class Component(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    signature: Signature = pydantic.Field(
        description="The Signature of the Component blueprint"
    )

    command: CCommand = pydantic.Field(
        default_factory=CCommand,
        description="The command that the tasks of this Component blueprint execute"
    )

    workflowAttributes: CWorkflowAttributes = pydantic.Field(
        default_factory=CWorkflowAttributes,
        description="Settings that control the execution of a Component as part of a "
                    "workflow e.g. restart logic, how many times to replicate, how to handle propagation of errors, etc"
    )

    resourceRequest: CResourceRequest = pydantic.Field(
        default_factory=CResourceRequest(),
        description="Resource request options to propagate to the resourceManager of the backend that will execute "
                    "the Tasks of this component"
    )

    resourceManager: CResourceManager = pydantic.Field(
        default_factory=CResourceManager(),
        description="Settings for the backend to use for executing Tasks of this Component"
    )


class Entrypoint(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    execute: typing.List[ExecuteStep] = pydantic.Field(
        [],
        min_items=1,
        max_items=1,
        description="How to execute the instance of the entrypoint blueprint in this namespace"
    )

    # VV: FIXME Rename this into "entryInstance" (i.e. remove the alias)
    entryInstance: str = pydantic.Field(
        description="The identifier of the entrypoint blueprint",
        alias="entry-instance"
    )

    @pydantic.root_validator(pre=False, skip_on_failure=True)
    def val_single_execute_step(cls, model: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
        target_name = model['execute'][0].target
        if target_name != "<entry-instance>":
            raise ValueError("The instance of the entrypoint blueprint is called \"<entry-instance>\" "
                             f"not \"{target_name}\"")
        return model


class Namespace(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    entrypoint: typing.Optional[Entrypoint] = pydantic.Field(
        None, description="How to execute the entrypoint blueprint"
    )

    workflows: typing.Optional[typing.List[Workflow]] = pydantic.Field(
        None, description="The Workflow blueprints of this namespace"
    )

    components: typing.Optional[typing.List[Component]] = pydantic.Field(
        None, description="The Component blueprints of this namespace"
    )
