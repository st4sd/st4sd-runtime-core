# Copyright IBM Inc. 2015, 2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston


"""Support for DSL 2.0 IS A WORK IN PROGRESS - the model and validation/auto-generation rules may change.

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

- Reference a step like so: <stepName>
- Reference a step that is nested in a workflow like this:
  - <parentWorkflow/stepName> or
  - "<parentWorkflow>"/stepName
  - <parentWorkflow>/stepName
  - <"%(parameterContainingOutputReference)s">stepName
- Reference a parameter: %(like-this)s
- Reference an output of a step: <stepName/optional/path> or "<stepName>"/optional/path
  - Outputs of steps can also have reference methods: <stepName/optional/path>:ref
  - supported reference methods are:
    - :ref
    - :output
    - :copy
    - :link
    - :extract

Instantiated Blueprints have outputs:
  Workflow: the outputs are the steps
  Component: the outputs are files (and streams) that the components produce


The definition of Namespace is:

entrypoint: # describes how to run this namespace
  # TODO: Rename this field as entryInstance
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
import copy
import typing

import experiment.model.codes
import pydantic.typing
import pydantic
import re

import experiment.model.frontends.flowir
import experiment.model.errors

ParameterPattern = experiment.model.frontends.flowir.FlowIR.VariablePattern
ParameterValueType = typing.Optional[typing.Union[str, float, int, typing.Dict[str, typing.Any]]]

SignatureNamePattern = r"(stage(?P<stage>([0-9]+))\.)?(?P<name>([A-Za-z0-9._-]*[A-Za-z_-]+))"
StepNamePattern = r"[.A-Za-z0-9_-]+"
# VV: A reference may have a prefix of N step names and a suffix of M paths each separated with a "/"
# N must be greater or equal to 1 and M must be greater or equal to 0
StepNameOrPathPattern = r"[^/:]+"

PatternReferenceMethod = f"(:(?P<method>(ref|copy|output|link|extract)))"

BlueprintPattern = rf"{StepNamePattern}(/({StepNameOrPathPattern}))*"
OutputReferenceVanilla = (rf"(?P<location>(<{BlueprintPattern}/?>))(/(?P<location_nested>{BlueprintPattern}))?"
                          rf"{PatternReferenceMethod}?")
OutputReferenceNested = rf'(?P<location>"<{BlueprintPattern}>")(/(?P<location_nested>{BlueprintPattern}))?'\
                             rf'{PatternReferenceMethod}?'
RevampedReferencePattern = fr'"(?P<reference>([.a-zA-Z0-9_/-])+)"{PatternReferenceMethod}'
LegacyReferencePattern = fr'(?P<reference>([.a-zA-Z0-9_/-])+){PatternReferenceMethod}'

TargetReference = pydantic.constr(regex=fr"<{StepNamePattern}>")
ParameterReference = pydantic.constr(regex=ParameterPattern)
MaxRestarts = pydantic.conint(gt=-2)
BackendType = pydantic.constr(regex=fr'({ParameterPattern}|docker|local|lsf|kubernetes)')
K8sQosType = pydantic.constr(regex=fr'({ParameterPattern}|guaranteed|burstable|besteffort)')
DockerImagePullPolicy = pydantic.constr(regex=fr'({ParameterPattern}|Always|Never|IfNotPresent)')
ResourceRequestFloat = pydantic.confloat(ge=0)
ResourceRequestInt = pydantic.conint(ge=0)
EnvironmentReference = pydantic.constr(regex=fr'({ParameterPattern}|none|environment)')


class OutputReference:
    _pattern_vanilla = re.compile(OutputReferenceVanilla)
    _pattern_nested = re.compile(OutputReferenceNested)

    def __init__(
        self,
        location: typing.List[str],
        method: typing.Optional[str] = None,
    ):
        self.location = list(location)
        self.method = method

    @classmethod
    def from_str(cls, ref_str: str) -> "OutputReference":
        match = cls._pattern_nested.fullmatch(ref_str)
        if match:
            groups = match.groupdict()
            # VV: get rid of `"<` and `>"` then a possibly trailing `/`, whatever is left is the location of the output
            location = groups['location'][2:-2].rstrip("/").split("/")
            # VV: similar for the optional "nested" location
            if  groups.get("location_nested") is not None:
                location_nested = groups['location_nested'].rstrip("/").split("/")
                location.extend(location_nested)

            if groups.get("method") is not None:
                method = groups["method"]
            else:
                method = None

            return cls(location=location, method=method)

        match = cls._pattern_vanilla.fullmatch(ref_str)

        if match:
            groups = match.groupdict()
            # VV: get rid of `<` and `>` then a possibly trailing `/`, whatever is left is the location of the output
            location = groups['location'][1:-1].rstrip("/").split("/")

            if  groups.get("location_nested") is not None:
                location_nested = groups['location_nested'].rstrip("/").split("/")
                location.extend(location_nested)

            if groups.get("method") is not None:
                method = groups["method"]
            else:
                method = None
            return cls(location=location, method=method)

        raise ValueError(f"{ref_str} is not a valid reference to the output of a Blueprint")

    def to_str(self):
        from_location = f"<{'/'.join(self.location)}>"
        if self.method:
            return ":".join((from_location, self.method))

        return from_location


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
        description="The name of the blueprint, must be unique in the parent namespace",
        min_length=1,
        # VV: Names cannot end in digits - FlowIR has a special meaning for digits at the end of component names
        regex=SignatureNamePattern
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
        description="Reference to a step name. A string enclosed in <> e.g. <foo>", min_length=3,
        regex=fr"<{StepNamePattern}>",
    )

    args: typing.Optional[typing.Dict[str, ParameterValueType]] = pydantic.Field(
        None, description="How to assign values to the parameters of the step. "
                          "Each entry is a key: value pair where the key is the name of a step parameter "
                          "and the value the desired value of the step parameter",
    )

    def get_target(self) -> str:
        return self.target[1:-1]


class ExecuteStepEntryInstance(ExecuteStep):
    target: pydantic.typing.Literal["<entry-instance>"] = pydantic.Field(
        "<entry-instance>", description="The entry point step name. Must be <entry-instance>."
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

    dockerImage: typing.Optional[str] = pydantic.Field(
        None,
        description="(deprecated) docker image"
    )

    dockerProfileApp: typing.Optional[str] = pydantic.Field(
        None,
        description="(deprecated) profile app to use with docker"
    )

    dockerOptions: typing.Optional[str] = pydantic.Field(
        None,
        description="(deprecated) docker options"
    )


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

    image_pull_secret: typing.Optional[str] = pydantic.Field(
        None,
        alias="image-pull-secret",
        description="(deprecated) The name of a Secret to use when pulling images"
    )

    namespace: typing.Optional[str] = pydantic.Field(
        None,
        description="(deprecated) The name of the namespace to host the resulting Job"
    )

    api_key_var: typing.Optional[str] = pydantic.Field(
        None,
        alias="api-key-var",
        description="(deprecated)"
    )

    host: typing.Optional[str] = pydantic.Field(
        None,
        description="(deprecated) The url to the Kubernetes REST API"
    )

    @pydantic.validator("qos", pre=True)
    def val_to_lowercase(cls, value: typing.Optional[str]) -> typing.Optional[str]:
        if isinstance(value, str):
            value = value.lower()

        return value


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


class COptimizer(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    disable: typing.Optional[bool] = pydantic.Field(
        None,
        description="(deprecated) Whether to disable the optimizer"
    )

    exploitChance: typing.Optional[float] = pydantic.Field(
        None,
        description="(deprecated)"
    )

    exploitTarget: typing.Optional[float] = pydantic.Field(
        None,
        description="(deprecated)"
    )

    exploitTargetLow: typing.Optional[float] = pydantic.Field(
        None,
        description="(deprecated)"
    )

    exploitTargetHigh: typing.Optional[float] = pydantic.Field(
        None,
        description="(deprecated)"
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

    isMigratable: typing.Optional[bool] = pydantic.Field(
        None,
        description="(deprecated) Whether this component can migrate to later stages"
    )

    isMigrated: typing.Optional[bool] = pydantic.Field(
        None,
        description="(deprecated) Whether this component has migrated from earlier stages"
    )

    isRepeat: typing.Optional[bool] = pydantic.Field(
        None,
        description="(deprecated) Whether this component repeats - tracked internally"
    )

    optimizer: typing.Optional[COptimizer] = pydantic.Field(
        None,
        description="(deprecated) Settings for the optimization of repeat intervals for repeating components"
    )


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

def _replace_many_parameter_references(
        what: str,
        loc: typing.List[str],
        parameters: typing.Dict[str, ParameterValueType],
        field: typing.List[str],
        rg_parameter: typing.Optional[re.Pattern] = None,
) -> ParameterValueType:
    start = 0

    if rg_parameter is None:
        rg_parameter = re.compile(ParameterPattern)

    while True:
        for match in rg_parameter.finditer(what, pos=start):
            if match and match.group(0) != "%(replica)s":
                break
        else:
            return what

        # VV: Get rid of %( and )s
        name = match.group()[2:-2]

        try:
            fillin = parameters[name]
        except KeyError:
            raise ValueError(f"Node {loc} references (field={field}) unknown parameter {name}. "
                             f"Known parameters are {parameters}")

        if isinstance(fillin, dict):
            if not (start == 0 and match.start() == 0 and match.end() == len(what)):
                raise ValueError(f"Node {loc} references (field={field}) a dictionary parameter "
                                 f"{name} but the value contains more characters. The value is \"{what}\"")
            else:
                return fillin

        if not isinstance(fillin, str):
            if not (start == 0 and match.start() == 0 and match.end() == len(what)):
                fillin = str(fillin)
            else:
                return fillin

        what = what[:match.start()] + fillin + what[match.end():]
        # VV: It's correct to ignore what we replaced in. If it includes a parameter reference then
        # it must be pointing to a parameter that exists in the parent scope!
        start = match.start() + len(fillin)

def replace_parameter_references(
        value: ParameterValueType,
        field: typing.List[str],
        all_scopes: typing.Dict[typing.Tuple[str], "ScopeStack.Scope"],
        location: typing.Iterable[str],
) -> ParameterValueType:
    """Resolves a value given the location of the owner blueprint instance and a book of all scopes

    Args:

        value:
            The parameter value
        all_scopes:
            A collection of all known scopes
        field:
            the path to the field whose value is being handled
        location:
            The location of the node that owns the field
    Returns:
        The resolved parameter value
    """
    rg_parameter = re.compile(ParameterPattern)

    def should_resolve_more(what: ParameterValueType) -> bool:
        if isinstance(what, (int, bool, dict)) or what is None:
            return False

        if isinstance(what, str):
            match =rg_parameter.search(what)
            return match is not None and match.group(0) != "%(replica)s"
        return False

    current_location = list(location)
    while should_resolve_more(value):
        current_scope = all_scopes[tuple(current_location[:-1])]
        value = _replace_many_parameter_references(
            what=value,
            loc=current_location,
            parameters=current_scope.parameters,
            field=field,
            rg_parameter=rg_parameter
        )
        current_location.pop(-1)

    return value


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
        default_factory=CResourceRequest,
        description="Resource request options to propagate to the resourceManager of the backend that will execute "
                    "the Tasks of this component"
    )

    resourceManager: CResourceManager = pydantic.Field(
        default_factory=CResourceManager,
        description="Settings for the backend to use for executing Tasks of this Component"
    )

    variables: typing.Dict[str, typing.Union[None, str, float, int, bool]] = pydantic.Field(
        {},
        description="Variables that the component can use internally, cannot be set by caller of Component"
    )


class Entrypoint(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    # VV: FIXME Rename this into "entryInstance" (i.e. remove the alias)
    entryInstance: str = pydantic.Field(
        description="The identifier of the entrypoint blueprint",
        alias="entry-instance"
    )

    execute: typing.List[ExecuteStepEntryInstance] = pydantic.Field(
        [],
        min_items=1,
        max_items=1,
        description="How to execute the instance of the entrypoint blueprint in this namespace"
    )


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

    def get_blueprint(self, name: str) -> typing.Union[Workflow, Component]:
        for t in (self.components or []) + (self.workflows or []):
            if t.signature.name == name:
                return t
        raise KeyError(f"No blueprint with name {name}")


class ScopeStack:
    """A collection of all reachable Scopes starting from the Entrypoint of a Namespace"""
    
    class Scope:
        """Describes how to instantiate a @blueprint at a certain @location with certain @parameters"""
        def __init__(
            self,
            location: typing.List[str],
            parameters: typing.Dict[str, ParameterValueType],
            blueprint: typing.Union[Workflow, Component]
        ):
            self.location = location
            self.parameters = parameters or {}
            self.blueprint = blueprint.copy(deep=True)

        @property
        def name(self) -> str:
            return self.location[-1]

        def dsl_location(self) -> typing.List[str]:
            bp_type = "workflows" if isinstance(self.blueprint, Workflow) else "components"
            return [bp_type, self.blueprint.signature.name]

        def verify_parameters(self, caller_location: experiment.model.errors.DSLLocation):
            """Utility method to verify the parameters of a scope

             Args:
                 caller_location:
                    The location of the YAML field that instantiates this Blueprint

             Raises:
                 experiment.model.errors.DSLInvalidError:
                    If the caller of the Blueprint references unknown parameters
            """
            dsl_error = experiment.model.errors.DSLInvalidError([])

            known_params = [p.name for p in self.blueprint.signature.parameters]

            for name in self.parameters:
                if name not in known_params:
                    dsl_error.underlying_errors.append(
                        experiment.model.errors.DSLInvalidFieldError(
                            location=caller_location,
                            underlying_error=ValueError(f"Unknown parameter {name}")
                        )
                    )

            if dsl_error.underlying_errors:
                raise dsl_error

        def fold_in_defaults_of_parameters(self):
            """Utility method to update the @parameters ivar with default values to the parameters of the Blueprint
            """
            known_params = {
                p.name: p.default for p in self.blueprint.signature.parameters
                if "default" in p.__fields_set__
            }

            for name, value in known_params.items():
                if name not in self.parameters:
                    self.parameters[name] = value

        def resolve_parameter_references_of_instance(
            self: "ScopeStack.Scope",
            all_scopes: typing.Dict[typing.Tuple[str], "ScopeStack.Scope"]
        ) -> typing.List[Exception]:
            errors: typing.List[Exception] = []
            for name, value in self.parameters.items():
                try:
                    self.parameters[name] = replace_parameter_references(
                        location=self.location,
                        value=value,
                        field=["signature", "parameters", name],
                        all_scopes=all_scopes,
                    )
                except ValueError as e:
                    errors.append(
                        experiment.model.errors.DSLInvalidFieldError(
                            location=self.dsl_location(),
                            underlying_error=e
                        )
                    )

            return errors

        def resolve_output_references_of_instance(
            self: "ScopeStack.Scope",
            all_scopes: typing.Dict[typing.Tuple[str], "ScopeStack.Scope"],
            ensure_references_point_to_sibling_steps: bool,
        ) -> typing.List[Exception]:
            errors: typing.List[Exception] = []
            for name, value in self.parameters.items():
                try:
                    self.parameters[name] = self.replace_step_references(
                        value=value,
                        field=["signature", "parameters", name],
                        all_scopes=all_scopes,
                        ensure_references_point_to_sibling_steps=ensure_references_point_to_sibling_steps,
                    )
                except ValueError as e:
                    errors.append(
                        experiment.model.errors.DSLInvalidFieldError(
                            location=self.dsl_location(),
                            underlying_error=e
                        )
                    )

                try:
                    self.parameters[name] = self.replace_revamped_references(value=self.parameters[name])
                except ValueError as e:
                    errors.append(
                        experiment.model.errors.DSLInvalidFieldError(
                            location=self.dsl_location(),
                            underlying_error=e
                        )
                    )

            return errors

        def resolve_legacy_data_references(
            self: "ScopeStack.Scope",
            all_scopes: typing.Dict[typing.Tuple[str], "ScopeStack.Scope"],
        ) -> typing.List[Exception]:
            errors: typing.List[Exception] = []
            for name, value in self.parameters.items():
                try:
                    self.parameters[name] = self.replace_revamped_references(value=value)
                except ValueError as e:
                    errors.append(
                        experiment.model.errors.DSLInvalidFieldError(
                            location=self.dsl_location(),
                            underlying_error=e
                        )
                    )

            return errors

        @classmethod
        def replace_revamped_references(
                cls,
                value: ParameterValueType,
        ) -> ParameterValueType:
            """Rewrites all Revamped DataReferences into Legacy DataReferences

            For example, it turns "input/hello":ref into input/hello:ref

            Args:
                value:
                    The parameter value
            Returns:
                The resolved parameter value
            """
            if not isinstance(value, str):
                return value

            pattern_revamped_ref = re.compile(RevampedReferencePattern)

            partial = ""
            start=0
            for match in pattern_revamped_ref.finditer(value):
                partial += value[start:match.start()]
                start = match.end()
                ref = match.groupdict()["reference"]
                method =  match.groupdict()["method"]
                partial += ":".join((ref, method))
            partial += value[start:]
            value = partial

            return value

        def replace_step_references(
                self,
                value: ParameterValueType,
                field: typing.List[str],
                all_scopes: typing.Dict[typing.Tuple[str], "ScopeStack.Scope"],
                ensure_references_point_to_sibling_steps: bool = True,
        ) -> ParameterValueType:
            """Rewrites all references to steps in a value to their absolute form

            Args:
                value:
                    The parameter value
                all_scopes:
                    A collection of all known scopes
                field:
                    the path to the field whose value is being handled
                ensure_references_point_to_sibling_steps:
                    When True, and the location of an OutputReference does not start from a sibling step to the
                    owner of the field then the method will raise a ValueError()

            Returns:
                The resolved parameter value

            Raises:
                ValueError:
                    On an invalid OutputReference
            """

            if not isinstance(value, str):
                return value

            sibling_steps = []
            if len(self.location) > 1:
                uid_parent = tuple(self.location[:-1])
                parent_workflow_name = uid_parent[-1] if len(uid_parent) > 0 else "**missing**"

                try:
                    parent_scope = all_scopes[uid_parent]
                except KeyError:
                    raise experiment.model.errors.DSLInvalidFieldError(
                        location=["workflows", parent_workflow_name],
                        underlying_error=ValueError(f"Unable to identify the parent Workflow of {self.location}")
                    )

                if not isinstance(parent_scope.blueprint, Workflow):
                    raise experiment.model.errors.DSLInvalidFieldError(
                        location=["workflows", parent_workflow_name],
                        underlying_error=ValueError(
                            f"The parent of {self.location} is not a Workflow but a {type(parent_scope)}")
                    )

                sibling_steps=[x for x in parent_scope.blueprint.steps if x != self.name]
            else:
                # VV: this can only happen in entrypoint.execute
                uid_parent = []

            pattern_vanilla = re.compile(OutputReferenceVanilla)
            pattern_nested = re.compile(OutputReferenceNested)

            if ensure_references_point_to_sibling_steps:
                # VV: First ensure that references to outputs all begin with a sibling name
                for pattern in [pattern_vanilla, pattern_nested]:
                    for match in pattern.finditer(value):
                        # VV: This works even if the OutputReference contains a reference to a parameter
                        ref = OutputReference.from_str(match.group(0))
                        if not ref.location or ref.location[0] not in sibling_steps:

                            raise experiment.model.errors.DSLInvalidFieldError(
                                location=self.dsl_location() + (field or []),
                                underlying_error=ValueError(
                                    f"OutputReference {match.group(0)} does not reference any of the known siblings "
                                    f"{sibling_steps}")
                            )

            for pattern in [pattern_vanilla, pattern_nested]:
                partial = ""
                start=0
                for match in pattern.finditer(value):
                    ref = OutputReference.from_str(match.group(0))
                    partial += value[start:match.start()]
                    start = match.end()
                    abs_ref = OutputReference(list(uid_parent) + ref.location, method=ref.method)
                    partial += abs_ref.to_str()
                partial += value[start:]
                value = partial

            return value

    def __init__(self):
        # VV: The current collection of Scopes. The scope at the end of the list is the "current" scope.
        self.stack: typing.List[ScopeStack.Scope] = []

        # VV: Keys are "locations" of blueprint instances i.e. ["entry-instance", "parentWorkflow", "component"]
        # think of this as the full path an instance of a blueprint starting from the root of the namespace.
        # The root of the namespace is what the entrypoint invokes, it's name is always `entry-instance`.
        # The values are the ScopeEntries which are effectively instances of a Blueprint i.e.
        # The instance name, definition, and parameters of a Blueprint
        self.scopes: typing.Dict[typing.Tuple[str], ScopeStack.Scope] = {}

    def enter(
            self,
            name: str,
            parameters: typing.Dict[str, ParameterValueType],
            blueprint: typing.Union[Workflow, Component]
    ):
        parameters = copy.deepcopy(parameters)
        errors = []
        location = self.get_location() + [name]

        uid = tuple(location)

        if uid in self.scopes:
            errors.append(ValueError(f"Node has already been visited", location))

        scope_entry = ScopeStack.Scope(location=location, parameters=parameters, blueprint=blueprint)

        for p in blueprint.signature.parameters:
            if p.name in parameters:
                continue
            if "default" not in p.__fields_set__:
                errors.append(ValueError(f"The step {p.name} in location {'/'.join(location)} "
                                         f"does not have a value or default"))
        if errors:
            dsl_error = experiment.model.errors.DSLInvalidError([])
            for e in errors:
                if isinstance(e, experiment.model.errors.DSLInvalidFieldError):
                    dsl_error.underlying_errors.append(e)
                else:
                    dsl_error.underlying_errors.append(
                        experiment.model.errors.DSLInvalidFieldError(
                            location=scope_entry.dsl_location(),
                            underlying_error=e
                        )
                    )
            raise dsl_error

        self.scopes[uid] = scope_entry

        self.stack.append(scope_entry)

        return scope_entry

    def exit(self):
        self.stack.pop(-1)

    def current_scope(self) -> "ScopeStack.Scope":
        return self.stack[-1]

    def depth(self) -> int:
        return len(self.stack)


    def get_location(self) -> typing.List[str]:
        return [s.name for s in self.stack]


    def get_parent_parameter_names(self) -> typing.List[str]:
        if self.depth() > 1:
            uid = tuple(self.get_location()[:-1])
            scope = self.scopes[uid]
            return sorted(scope.parameters)

        return []

    def _seed_all_scopes(self, namespace: Namespace) -> typing.List[Exception]:
        """Utility method to visit all Workflows and Components which are reachable from the Entrypoint and seed
        the ScopeStack with 1 scope for each visited blueprint

        The utility method updates the `all_scopes` ivar.

        Args:
            namespace:
                The namespace to walk

        Returns:
            An array of errors
        """
        import enum
        class Action(str, enum.Enum):
            Enter = "enter"
            Exit = "exit"

        remaining_scopes: typing.List[typing.Tuple[Action, ScopeStack.Scope]] = [
            (
                Action.Enter,
                ScopeStack.Scope(
                    location=["entry-instance"],
                    blueprint=namespace.get_blueprint(namespace.entrypoint.entryInstance),
                    parameters=namespace.entrypoint.execute[0].args,
                )
            )
        ]

        dsl_error = experiment.model.errors.DSLInvalidError([])
        errors: typing.List[Exception] = []

        rg_param = re.compile(ParameterPattern)
        while remaining_scopes:
            action, scope = remaining_scopes.pop(0)
            if action == Action.Exit:
                self.exit()
                continue
            try:
                scope = self.enter(name=scope.name, parameters=scope.parameters, blueprint=scope.blueprint)
            except experiment.model.errors.DSLInvalidError as e:
                dsl_error.underlying_errors += e.underlying_errors
                break

            remaining_scopes.insert(0, (Action.Exit, scope))

            location = self.get_location()
            if location != ["entry-instance"]:
                parent_scope = self.scopes[tuple(location[:-1])]
                parent_scope.dsl_location()
                workflow: Workflow = parent_scope.blueprint
                field_invoke_loc = ["workflows", workflow.signature.name] + ["execute"]
                for idx, step in enumerate(workflow.execute):
                    step: ExecuteStep = step
                    if step.get_target() == scope.name:
                        field_invoke_loc = ["workflows", workflow.signature.name] + ["execute", idx]
            else:
                field_invoke_loc = ["entrypoint", "execute", 0]

            scope.verify_parameters(field_invoke_loc)
            scope.fold_in_defaults_of_parameters()

            # VV: Argument values may ONLY reference parameters of the parent scope
            parent_parameters = self.get_parent_parameter_names()

            for name, value in scope.parameters.items():
                if isinstance(value, str):
                    refs = rg_param.findall(value)
                    for match in refs:
                        # VV: throw away "%(" and ")s"
                        param = match[2:-2]

                        if param not in parent_parameters:
                            if param == "replica":
                                continue
                            errors.append(KeyError(
                                f"Node {location} references the parameter {param} but its parent "
                                f"does not have such a parameter, it has {parent_parameters}"))

            if isinstance(scope.blueprint, Workflow):
                children_scopes = []
                for execute in scope.blueprint.execute:
                    child_location = location + [execute.get_target()]

                    try:
                        blueprint_name = scope.blueprint.steps[execute.get_target()]
                    except KeyError:
                        errors.append(KeyError(f"Node {child_location} has no matching step"))
                        continue

                    try:
                        blueprint = namespace.get_blueprint(blueprint_name)
                    except KeyError as e:
                        errors.append(e)
                        continue

                    parameters = copy.deepcopy(execute.args or {})

                    new_scope = ScopeStack.Scope(
                        location=scope.location + [execute.get_target()],
                        parameters=parameters,
                        blueprint=blueprint,
                    )

                    if isinstance(blueprint, Workflow):
                        children_scopes.append((Action.Enter, new_scope))
                    else:
                        children_scopes.insert(0, (Action.Enter, new_scope))

                remaining_scopes = children_scopes + remaining_scopes
            elif isinstance(scope.blueprint, Component):
                pass
            else:
                location = '\n'.join(location)
                raise NotImplementedError(f"Cannot visit location {location} for Node", scope)

        return errors

    @classmethod
    def from_namespace(cls, namespace: Namespace) -> "ScopeStack":
        scope_stack = cls()

        errors = scope_stack._seed_all_scopes(namespace=namespace)

        if errors:
            exc = experiment.model.errors.DSLInvalidError.from_errors(errors)
            # print(exc)
            raise exc

        # VV: TODO now find references to the env-vars by visiting the fields of the component
        # if any field other than `command.environment` references a dictionary record an error

        def resolve_scope(
            scope: ScopeStack.Scope,
            scope_stack: ScopeStack = scope_stack,
        ):
            scope_errors = []
            # VV: Workflows can use the arguments of their step Workflows to propagate the outputs of their children
            # steps to their grandchildren. This means that it is valid for a Blueprint that is instantiated by a
            # deeply nested workflow to eventually (i.e. via a chain of parameters) receive an OutputReference whose
            # location is a step that is NOT a sibling step.
            # Therefore, we want to ensure that references point to sibling steps before we resolve any parameters.
            # After we resolve parameters, we can go ahead and resolve the OutputReferences again without performing
            # the above sibling-step check.
            errors.extend(scope.resolve_output_references_of_instance(
                    all_scopes=scope_stack.scopes,
                    ensure_references_point_to_sibling_steps=True,
                )
            )
            scope_errors.extend(scope.resolve_parameter_references_of_instance(all_scopes=scope_stack.scopes))
            scope_errors.extend(scope.resolve_legacy_data_references(all_scopes=scope_stack.scopes))
            scope_errors.extend(scope.resolve_output_references_of_instance(
                    all_scopes=scope_stack.scopes,
                    ensure_references_point_to_sibling_steps=False,
                )
            )
            return  scope_errors

        for location, scope in scope_stack.scopes.items():
            if isinstance(scope.blueprint, Workflow):
                errors.extend(resolve_scope(scope=scope, scope_stack=scope_stack))

        for location, scope in scope_stack.scopes.items():
            if isinstance(scope.blueprint, Component):
                errors.extend(resolve_scope(scope=scope, scope_stack=scope_stack))

        # for location, scope in scope_stack.scopes.items():
        #     print(type(scope.blueprint).__name__, scope.location, "parameters", scope.parameters)

        if errors:
            raise experiment.model.errors.DSLInvalidError.from_errors(errors)

        return scope_stack


class ComponentFlowIR:
    def __init__(
        self,
        environment: typing.Optional[typing.Dict[str, typing.Any]],
        scope: ScopeStack.Scope,
        errors: typing.List[Exception],
    ):
        self.environment = environment
        self.scope = scope
        self.errors = errors

        self.flowir = scope.blueprint.dict(
            by_alias=True, exclude_none=True, exclude_defaults=True, exclude_unset=True
        )

        del self.flowir['signature']

        if "command" not in self.flowir:
            # VV: this could be a component with a bunch of errors so we cannot trust its contents
            self.flowir["command"] = {}

    @property
    def step_name(self) -> str:
        # VV: The step name is the last entry in the location of the Blueprint instance
        return self.scope.location[-1]

    def _get_refs_in_param(self, pattern: str) -> typing.Dict[str, typing.List[str]]:
        refs = {}

        pattern = re.compile(pattern)

        for param, param_value in self.scope.parameters.items():
            refs[param] = [
                x.group(0) for x in pattern.finditer(param_value)
            ]

        return {param_name: sorted(set(param_refs)) for param_name, param_refs in refs.items()}

    def discover_output_references(self) -> typing.Dict[str, typing.List[str]]:
        return self._get_refs_in_param(OutputReferenceVanilla)

    def discover_legacy_references(self) -> typing.Dict[str, typing.List[str]]:
        return self._get_refs_in_param(LegacyReferencePattern)


    def convert_outputreferences_to_datareferences(
        self,
        uid_to_name: typing.Dict[typing.Tuple[str], typing.Tuple[int, str]]
    ):
        """Utility method to convert OutputReference instances into Legacy DataReferences

        This method updates the flowir ivar

        Args:
            uid_to_name:
                A key: value dictionary where keys are scope locations and values are the names of the associated
                component instances in the FlowIR domain
        """
        if "references" not in self.flowir:
            self.flowir["references"] = []

        # VV: OutputReferences and Legacy DataReferences that are in parameter values
        parameters_legacy: typing.Set[str] = set()
        parameters_output: typing.Set[str] = set()

        # VV: OutputReferences and Legacy DataReferences that are in the command.arguments field
        arguments_legacy: typing.Set[str] = set()
        arguments_output: typing.Set[str] = set()

        # VV: We can find legacy DataReferences and OutputReferences in 2 places:
        # 1. parameter values
        # 2. command.arguments
        # if they appear in a different field, we hope that something else will identify these errors (FlowIRConcrete?)

        pattern_output = re.compile(OutputReferenceVanilla)
        pattern_legacy = re.compile(LegacyReferencePattern)

        args = self.flowir["command"].get("arguments", "")
        self.flowir["command"]["arguments"] = args

        # VV: TODO Here we'll need to do something about :copy - I'll figure this out in a future update
        for match in pattern_output.finditer(args):
            ref = OutputReference.from_str(match.group(0))
            if not ref.method:
                raise experiment.model.errors.DSLInvalidFieldError(
                    location=["components", self.scope.blueprint.signature.name, "command", "arguments"],
                    underlying_error=ValueError(f"The arguments of {self.scope.location} contain a reference to "
                                 f"the output {match.group(0)} but the OutputReference is partial, it does not "
                                 f"end with a :$method suffix.")
                )

        for name, value in self.scope.parameters.items():
            if not isinstance(value, str):
                continue
            for match in pattern_output.finditer(value):
                ref = OutputReference.from_str(match.group(0))
                if ref.method:
                    parameters_output.add(match.group(0))
                else:
                    # VV: This parameter is "partial" in that it doesn't include a :$method
                    # it **must** appear somewhere in the arguments
                    search = match.group(0) + ":"
                    for x in arguments_output:
                        if x.startswith(search) and x[len(search):] in [
                            "copy", "link", "ref", "output", "extract"
                        ]:
                            break
                    else:
                        raise experiment.model.errors.DSLInvalidFieldError(
                            location=[
                                "components", self.scope.blueprint.signature.name, "signature", "parameters", name
                            ],
                            underlying_error=ValueError(
                                f"The parameter {name}={value} of the Component {self.scope.location} "
                                f"is an OutputReference without a :$method and the :$method suffix "
                                f"cannot be inferred from the field command.arguments={args}"
                            )
                        )

        for match in pattern_legacy.finditer(args):
            arguments_legacy.add(match.group(0))

        for name, value in self.scope.parameters.items():
            if not isinstance(value, str):
                continue
            for match in pattern_legacy.finditer(value):
                parameters_legacy.add(match.group(0))

        # VV: We need to replace OutputReferences with Legacy DataReferences
        # 1. validate that OutputReferences point to outputs of Components
        # 2. partition the location of an OutputReference into "location of producer" and "file ref location"
        # 3. generate a Legacy DataReference which looks like this: $producerReference[/optional/path]:$method
        # 4. if $method is not in [copy, link, extract] then replace occurrences of the OutputReference string
        #    in the arguments string with the Legacy DataReference string
        #    else, if there is a fileref, replace occurrences of the OutputRef string in the arguments string with
        #       the basename of the fileref. If there is no fileref in the OutputReference and it appears in the args
        #       then raise an exception

        def infer_closest_match(comp_location: typing.List[str], from_ref: str) -> typing.Tuple[
            typing.Tuple[str],
            typing.Optional[str],
        ]:
            best = None
            largest_overlap = 0

            for other_loc in uid_to_name:
                overlap = 0
                if len(comp_location) < len(other_loc):
                    continue

                for i in range(min(len(comp_location), len(other_loc))):
                    if other_loc[i] != comp_location[i]:
                        break
                    overlap += 1

                if overlap > largest_overlap:
                    best = other_loc

            if best:
                if len(best) == len(comp_location):
                    fileref = None
                else:
                    fileref = '/'.join(comp_location[len(best)+1:]).lstrip('/')

                return best, fileref

            raise experiment.model.errors.DSLInvalidFieldError(
                location=self.scope.dsl_location(),
                underlying_error=ValueError(
                    f"The Component {self.scope.location} contains the reference {from_ref} which does not "
                    f"point to any known Components", uid_to_name
                )
            )

        for ref_str in parameters_output.union(arguments_output):
            ref = OutputReference.from_str(ref_str)
            producer, fileref = infer_closest_match(comp_location=ref.location, from_ref=ref_str)
            stage, producer = uid_to_name[producer]

            producer = f"stage{stage}.{producer}"

            if fileref:
                producer = '/'.join((producer, fileref))
            new_ref_str = ':'.join((producer, ref.method))

            self.flowir["command"]["arguments"] = self.flowir["command"]["arguments"].replace(ref_str, new_ref_str)
            parameters_legacy.add(new_ref_str)

        self.flowir["references"] = sorted(parameters_legacy.union(arguments_legacy))

    def resolve_parameter_references(self):
        """Utility method to replace all parameter references in @flowir with their values

        The method updates @errors with any problems it identifies
        """

        can_have_data_output_references = [["command", "arguments"]]

        # VV: Algorithm:
        # 1. Walk the FlowIR fields
        # 2. if a field is a string and it is referencing a parameter substitute the parameter reference with the
        #    parameter value
        #    if the parameter doesn't exist, then record an error
        #    if the parameter is an output or data reference and the field is not meant to use either of those,
        #       then record an error

        class Explore:
            def __init__(self, location: typing.List[typing.Union[str, int]], value: typing.Any):
                self.location = location
                self.value = value

            def update_value(self, new_value: typing.Any, flowir: typing.Dict[str, typing.Any]):
                where = flowir

                for loc in self.location[:-1]:
                    where = where[loc]

                where[self.location[-1]] = new_value

            def replace_parameter_references(self, scope: ScopeStack.Scope, flowir: typing.Dict[str, typing.Any]):
                new_value = replace_parameter_references(
                    self.value,
                    field=self.location,
                    location=scope.location + ["inner field"],
                    all_scopes={
                        tuple(scope.location): scope
                    }
                )

                if new_value != self.value:
                    self.update_value(new_value=new_value, flowir=flowir)

        pending: typing.List[Explore] = [Explore([], self.flowir)]

        while pending:
            node = pending.pop(0)

            if isinstance(node.value, dict):
                for key, value in node.value.items():
                    location = node.location + [key]
                    pending.insert(0, Explore(location=location, value=value))
            elif isinstance(node.value, str):
                node.replace_parameter_references(scope=self.scope, flowir=self.flowir)
            elif isinstance(node.value, list):
                for idx, value in enumerate(node.value):
                    location = node.location + [idx]
                    pending.insert(0, Explore(location=location, value=value))


def number_to_roman_like_numeral(value: int) -> str:
    lookup = [
        [1, "I"],
        # VV: If you get past IV then there's likely something fishy going on
        [4, "IV"],
        [5, "V"],
        [9, "IX"],
        [10, "X"],
        # VV: pretend that there are no more numbers past X, if you need 50 you get XXXXX
    ]

    digit = len(lookup) - 1
    rep = ""

    while value:
        while value >= lookup[digit][0]:
            value -= lookup[digit][0]
            rep += lookup[digit][1]

        digit -= 1

    return rep

def namespace_to_flowir(namespace: Namespace) -> experiment.model.frontends.flowir.FlowIRConcrete:
    """Converts a Namespace to flowir

    Algorithm:

    1. visit all reachable nodes starting from the entrypoint (not necessarily in scheduling order)
    2. verify that all reachable nodes reference a blueprint that actually exists
    3. build a mapping of unique node identifiers to the blueprint that they reference
    4. after visiting all nodes if there're any errors raise an exception and stop, else:
    5. revisit all Component nodes (order doesn't matter) and:
       - generate unique names for components (resolve name conflicts by appending roman numerals to component names)
       - generate unique names for environments (if 2 environments contain identical env-vars they're the same)
       - rewrite OutputReferences to Legacy DataReferences
       - resolve all parameters to flowir
    6. Hallucinate global variables. One for each parameter of `<entry-instance>`. These global variables are
       hallucinated in that there are no references to them by any component. Recall that all parameters have been
       fully resolved as part of step 5. The hallucinated global variables are only here so that if something
       (e.g. st4sd-runtime-service) reads the FlowIR it gets some hints about the names of "variables" that this
       DSL 2 Namespace uses as parameters.

    Args:
        namespace: the namespace to convert

    Returns:
        A FlowIRConcrete instance

    Raises:
        experiment.model.errors.DSLInvalidError:
            When there are errors
    """
    scopes =  ScopeStack.from_namespace(namespace)

    components: typing.Dict[typing.Tuple[str], ComponentFlowIR] = {}
    errors = []

    for location, scope in scopes.scopes.items():
        if isinstance(scope.blueprint, Component):
            comp_flowir = digest_dsl_component(
                scope=scope,
                all_scopes=scopes.scopes
            )
            errors.extend(comp_flowir.errors)
            components[tuple(scope.location)] = comp_flowir

    if errors:
        raise experiment.model.errors.DSLInvalidError.from_errors(errors)

    component_names: typing.Dict[str, int] = {}
    uid_to_name: typing.Dict[typing.Tuple[str], typing.Tuple[int, str]] = {}

    pattern_name = re.compile(SignatureNamePattern)

    for _, comp in components.items():
        assert isinstance(comp.scope.blueprint, Component)

        if comp.step_name not in component_names:
            component_names[comp.step_name] = 0
            name = comp.step_name
        else:
            component_names[comp.step_name] += 1
            prior = component_names[comp.step_name]
            name = "-".join((comp.step_name, number_to_roman_like_numeral(prior)))


        match = pattern_name.fullmatch(name)
        match_groups = match.groupdict()

        uid_to_name[tuple(comp.scope.location)] = (int(match_groups.get("stage") or 0), match_groups["name"])

        comp.flowir['name'] = uid_to_name[tuple(comp.scope.location)][1]
        comp.flowir['stage'] = uid_to_name[tuple(comp.scope.location)][0]

    complete = experiment.model.frontends.flowir.FlowIRConcrete(
        flowir_0={},
        platform=experiment.model.frontends.flowir.FlowIR.LabelDefault,
        documents={},
    )

    def hash_environment(environment: typing.Dict[str, typing.Any]) -> typing.Tuple[typing.Tuple[str, str]]:
        hash: typing.List[typing.Tuple[str, str]] = []

        for key in sorted(environment):
            value = environment[key]
            if value is None:
                continue
            hash.append((key, str(value)))

        return tuple(hash)

    known_environments: typing.Dict[typing.Tuple[typing.Tuple[str, str]], str] = {}

    for _, comp in components.items():
        comp.resolve_parameter_references()

        environment = comp.environment
        command = comp.flowir["command"]

        if environment == {}:
            command["environment"] = "none"
            continue
        elif environment is None:
            command["environment"] = None
            continue

        dict_hash = hash_environment(environment)
        try:
            environment_name = known_environments[dict_hash]
        except KeyError:
            environment_name = f"env{len(known_environments)}"
            known_environments[dict_hash] = environment_name

            complete.set_environment(environment_name, environment, platform="default")

        command["environment"] = environment_name

    # VV: After resolving the parameters, handle OutputReferences and Legacy DataReferences. This step **must**
    # happen after resolving the parameters as the OutputReferences and Legacy DataReferences may be
    # constructed inside the arguments by combining the values of 1 or more parameters with 0 or more literal strings
    for _, comp in components.items():
        comp.convert_outputreferences_to_datareferences(uid_to_name)

    all_errors = sum([comp.errors for comp in components.values()], [])
    if all_errors:
        raise experiment.model.errors.DSLInvalidError.from_errors(all_errors)

    # VV: At this point components are ready, start plopping them in the flowir
    for _, comp in components.items():
        complete.add_component(comp.flowir)

    for name, value in scopes.scopes[("entry-instance",)].parameters.items():
        if value is None or isinstance(value, dict):
            continue
        complete.set_platform_global_variable(variable=name, value=value, platform="default")

    return complete


def digest_dsl_component(
    scope: ScopeStack.Scope,
    all_scopes: typing.Dict[typing.Tuple[str], ScopeStack.Scope],
) -> ComponentFlowIR:
    """Utility method to generate information that the caller can use to put together a FlowIRConcrete instance

    At this point, parameters are either e.g. float, int, str, dictionaries or None
    Strings may point to other Blueprint instances, when they to, they look like:
     - <optional/this>[:${method}] (i.e. an optional method)
     - "<optional/this>"[/optional/path][:${method}]
    If such kind of strings appear in arguments, they **must** be followed by a :${method} e.g. :ref, :copy, etc
    Parameters can point to both Workflow and Component instances. Parameters which reference Blueprint instances
    **must not** appear in fields other than command.arguments
     If a Parameter references a Workflow instance,
       the parameter **must** also be referenced "%(like-this)s/location/to/component/optional/path":${method}
          in the field command.arguments
       the parameter's value **must not** be <optional/workflow>:${method} (i.e. no ${method}
     If a Parameter references a Component instance,
       if it has a :${method},
          if method=copy, the parameter may not appear in the arguments

    Args:
        scope:
            the component blueprint, its parameters to instantiate it, and its unique location (uid)
        all_scopes:
            all other instantiated blueprints. This is a key: value dictionary, where each key is the location of a
            scope and the value is the scope of the instantiated blueprint. The scopes may point to either Workflows
            or Components.

    Returns:
        Information necessary to produce FlowIR from a Component in the form of ComponentFlowIR and a collection of
        errors. If there's more than 1 error, the information in the ComponentFlowIR may be incomplete
    """
    if not isinstance(scope.blueprint, Component):
        exc = ValueError(f"Node {scope.location} was expected to be a Component not a {scope.blueprint}")
        exc = experiment.model.errors.DSLInvalidFieldError(location=scope.dsl_location(), underlying_error=exc)

        return ComponentFlowIR(
            errors=[exc],
            environment=None,
            scope=scope,
        )

    environment = None
    errors = []

    pattern_parameter = re.compile(ParameterPattern)

    check_environment = scope.blueprint.command.environment
    if (
        isinstance(check_environment, str)
        and pattern_parameter.fullmatch(check_environment)
    ):
        # VV: get rid of $( and )s
        param_name = check_environment[2:-2]
        if param_name not in scope.parameters:
            exc = ValueError(f"The Component {scope.location} uses the parameter {param_name} to set its"
                                     f"environment, but the component does not have such a parameter")
            exc = experiment.model.errors.DSLInvalidFieldError(
                location=scope.dsl_location() + ["signature", "environment"], underlying_error=exc)
            errors.append(exc)

        check_environment = scope.parameters[param_name]

    if isinstance(check_environment, dict):
        environment = check_environment
    elif check_environment is "none":
        environment = {}
    elif check_environment is None:
        environment = None
    else:
        exc = ValueError(f"Environment of Component {scope.location} must either be a Dictionary of "
                                 f"key: value env-vars, be unset, or the string literal \"none\". However it is "
                                 f"{check_environment}")
        exc = experiment.model.errors.DSLInvalidFieldError(
            location=scope.dsl_location() + ["signature", "environment"], underlying_error=exc)
        errors.append(exc)

    ret = ComponentFlowIR(
        errors=errors,
        environment=environment,
        scope=scope,
    )

    return ret
