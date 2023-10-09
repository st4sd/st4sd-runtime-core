# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

'''Module containing exception definitions'''
from __future__ import print_function, annotations

import typing
from typing import TYPE_CHECKING, Any, List, Optional, Dict, Union, Tuple, Callable
from six import string_types
import configparser

if TYPE_CHECKING:
    import experiment.model.storage
    import experiment.model.graph
    from experiment.model.frontends.flowir import DictFlowIRComponent, FlowIRComponentId, DictFlowIR

    ExperimentSource = Union[
        experiment.model.storage.ExperimentPackage, experiment.model.storage.ExperimentInstanceDirectory]

DSLLocation = List[Union[str, int]]

# VV: FlowIR Errors

class FlowException(Exception):

    def __repr__(self):
        return '%s(%s)' % (type(self), self.__str__())


class FlowIRException(FlowException):
    pass


class FlowExceptionWithMessageError(FlowException):
    def __init__(self, message: str):
        self.message = message

    def __str__(self):
        return self.message


class RuntimeHookError(FlowExceptionWithMessageError):
    pass


class ExtractionMethodSourceInvalidError(RuntimeHookError):
    def __init__(self, message: str, definition: Dict[str, str]):
        self.definition = definition
        message += f". The definition is {definition}"
        super(ExtractionMethodSourceInvalidError, self).__init__(message)


class InputExtractionMethodUnknownError(RuntimeHookError):
    def __init__(self, message: str, definition: Dict[str, str]):
        self.definition = definition
        message += f". The definition is {definition}"
        super(InputExtractionMethodUnknownError, self).__init__(message)


class RuntimeHookManyErrors(RuntimeHookError):
    def __init__(self, underlying_errors: List[Exception]):
        self.underlying_errors = underlying_errors

        msg = f"Ran into {len(underlying_errors)} errors while running hooks.\nUnderlying errors:"
        msg += "\n".join( [f"({type(underlying_error)}) {underlying_error}" for underlying_error in underlying_errors])
        super(RuntimeHookManyErrors, self).__init__(msg)


class UnableToIdentifyInputsError(RuntimeHookError):
    def __init__(self, input_id_file: str):
        self.input_id_file = input_id_file

        super(UnableToIdentifyInputsError, self).__init__(f"Unable to identify input ids in {input_id_file}")


class UnableToAccessWorkingDirError(RuntimeHookError):
    def __init__(self, workdir: str):
        self.workdir = workdir

        super(UnableToAccessWorkingDirError, self).__init__(f"Could not access working directory {workdir}")


class CannotReadInputIdFileError(RuntimeHookError):
    def __init__(self, underlying_error: Exception | None = None):
        self.underlying_error = underlying_error

        msg = f"Cannot read input id file"
        if underlying_error:
            msg += f"\nUnderlying error:\n({type(underlying_error)}) {underlying_error}"

        super(CannotReadInputIdFileError, self).__init__(msg)


class RuntimeHookUnknownError(RuntimeHookError):
    def __init__(self, hook_name: str,  underlying_error: Exception | None = None, other: str | None = None):
        self.hook_name = hook_name
        self.underlying_error = underlying_error
        self.other = other

        msg = f"Unknown hook error for {hook_name}."
        if other:
            msg += f"{other}."
        if underlying_error:
            msg += f"\nUnderlying error:\n({type(underlying_error)}) {underlying_error}"

        super(RuntimeHookUnknownError, self).__init__(msg)

class CannotReadPropertyOutputFileError(RuntimeHookError):
    def __init__(self, property_name: str, output_file: str, underlying_error: Exception | None = None):
        self.underlying_exception = underlying_error
        self.property_name = property_name
        self.output_file = output_file

        msg = f"Cannot read property {property_name} from output file {output_file}."

        if underlying_error:
            msg += f"\nUnderlying error:\n({type(underlying_error)}) {underlying_error}"

        super(CannotReadPropertyOutputFileError, self).__init__(msg)


class UnknownPropertyError(RuntimeHookError):
    def __init__(self, property_name: str, output_file: str):
        self.property_name = property_name
        self.output_file = output_file

        msg = f"Property {property_name} is unknown (for output file {output_file})."

        super(UnknownPropertyError, self).__init__(msg)


class PropertyNotMeasuredError(RuntimeHookError):
    def __init__(self, property_name: str, output_file: str):
        self.property_name = property_name
        self.output_file = output_file

        msg = f"Property {property_name} is not measured in output file {output_file}."

        super(PropertyNotMeasuredError, self).__init__(msg)


class MissingColumnError(RuntimeHookError):
    def __init__(self, property_name: str, column_name: str, property_source_paths: List[str]):
        self.property_name = property_name
        self.property_source_paths = property_source_paths
        self.column_name = column_name

        msg = f"Hook to extract property {property_name} from property source paths {property_source_paths} " \
              f"returned a pandas.DataFrame that did not have the required column `{column_name}`"

        super(MissingColumnError, self).__init__(msg)


class FlowIRSyntaxException(FlowException):
    def __init__(self, message):
        self.message = message

        super(FlowIRSyntaxException, self).__init__()

    def __str__(self):
        return self.message


class DSLInvalidFieldError(Exception):
    def __init__(self, location: typing.List[str], underlying_error: Exception):
        self.location = location
        self.underlying_error = underlying_error

    def __str__(self):
        return "DSL Error at " + "/".join(map(str, self.location)) + ": " + str(self.underlying_error)


class DSLInvalidError(FlowIRException):
    def __init__(self, underlying_errors: typing.List[DSLInvalidFieldError]):
        self.underlying_errors = underlying_errors

    def __str__(self):
        return f"DSL contains {len(self.underlying_errors)} error(s):\n" + "\n".join(map(str, self.underlying_errors))

    def __repr__(self):
        return str(self)

    def errors(self) -> typing.List[typing.Dict[str, typing.Any]]:
        return [
            {
                "location": e.location,
                "error": str(e.underlying_error)
            } for e in self.underlying_errors
        ]

    @classmethod
    def from_errors(cls, errors: List[Union[DSLInvalidFieldError, Exception]]):
        dsl_error = cls([])

        for e in errors:
            if isinstance(e, DSLInvalidFieldError):
                dsl_error.underlying_errors.append(e)
            else:
                dsl_error.underlying_errors.append(
                    DSLInvalidFieldError(
                        location=[],
                        underlying_error=e
                    )
                )
        return dsl_error

class FlowIRNotDictionary(FlowIRSyntaxException):
    def __init__(self, what: Any):
        self.what = what

        super(FlowIRNotDictionary, self).__init__(f"FlowIR object is not a dictionary, it is a {type(what)}")


class FlowIRConfigurationErrors(FlowIRException):
    def __init__(self, errors: List[Exception], location: Optional[str]=None):
        self.underlyingErrors = list(errors)
        self.location = location

        message = f"Found {len(errors)} errors"
        if location:
            message += f" with configuration at {location}. "

        message += 'Errors:\n' + '\n'.join((f"{e}" for e in errors))

        super(FlowIRConfigurationErrors, self).__init__(message)


class FlowIRKeyMissing(FlowIRSyntaxException):
    def __init__(self, key_name):
        self.key_name = key_name

        super(FlowIRKeyMissing, self).__init__("Key %s is missing" % key_name)


class FlowIRManifestException(FlowIRSyntaxException):
    def __init__(self, message: str):
        super(FlowIRManifestException, self).__init__(message)


class FlowIRManifestMissing(FlowIRManifestException):
    def __init__(self, path: str):
        self.path = path
        super(FlowIRManifestMissing, self).__init__(f"Manifest does not exist at path {path}")


class FlowIRManifestSyntaxException(FlowIRManifestException):
    def __init__(self, message: str):
        super(FlowIRManifestSyntaxException, self).__init__(message)


class FlowIRManifestInvalidType(FlowIRManifestSyntaxException):
    def __init__(self, what: Any):
        self.manifest = what
        super(FlowIRManifestInvalidType, self).__init__(
            f"Manifest was expected to be a dictionary but it was {type(what).__name__}")


class FlowIRManifestKeyIsAbsolutePath(FlowIRManifestSyntaxException):
    def __init__(self, target: str):
        self.target = target

        super(FlowIRManifestKeyIsAbsolutePath, self).__init__(
            f'Manifest target "{target}" is invalid because it is an absolute path')


class FlowIRManifestSourceInvalidReferenceMethod(FlowIRManifestSyntaxException):
    def __init__(self, target: str, source: str):
        self.target = target
        self.source = source

        super(FlowIRManifestSourceInvalidReferenceMethod, self).__init__(
            f'Target "{target}" has invalid source {source} - invalid reference method')


class FlowIRKeyUnknown(FlowIRSyntaxException):
    def __init__(self, key_name, valid_choices, most_appropriate):
        self.key_name = key_name
        self.valid_choices = list(valid_choices) if valid_choices else None
        self.most_appropriate = most_appropriate

        if most_appropriate:
            msg = 'Invalid key %s, did you mean %s? - valid choices are %s' % (
                key_name, self.most_appropriate, self.valid_choices)
        else:
            msg = 'Invalid key %s - valid choices are %s' % (key_name, self.valid_choices)

        super(FlowIRKeyUnknown, self).__init__(msg)


class FlowIRValueInvalid(FlowIRSyntaxException):
    def __init__(self, key_name, value, valid_choices, most_appropriate=None):
        self.key_name = key_name
        self.value = value
        self.valid_choices = list(valid_choices) if valid_choices else None
        self.most_appropriate = most_appropriate
        valid_choices_msg = str(self.valid_choices)

        if len(valid_choices_msg) > 200:
            valid_choices_msg = '...'.join((valid_choices_msg[0:98], valid_choices_msg[-99:]))

        if most_appropriate:
            msg = f'Invalid value {key_name}={value}, did you mean {most_appropriate}? ' \
                  f'- valid choices are {valid_choices_msg}'
        else:
            msg = f'Invalid value {key_name}={value} - valid choices are {valid_choices_msg}'

        super(FlowIRValueInvalid, self).__init__(msg)


class FlowIRInvalidDocumentType(FlowIRException):
    def __init__(self, document_type, valid_documents, document=None):
        # type: (str, List[str], Optional[Dict[str, Any]]) -> None
        self.document_type = document_type
        self.valid_documents = list(valid_documents)
        self.document = document

        self.message = "Invalid document type %s (valid options are %s)" % (document_type, valid_documents)

    def __str__(self):
        return self.message


class FlowIRInvalidBindingModifyFilename(FlowIRException):
    def __init__(self, binding_ref, binding_usage_ref):
        self.binding_ref = binding_ref
        self.binding_usage_ref = binding_usage_ref

        self.message = "Cannot modify filename of binding-provided reference %s with the one from %s " \
                       "because the former is not empty." % (binding_ref, binding_usage_ref)

    def __str__(self):
        return self.message


class FlowIRFailedComponentConvertType(FlowIRException):
    def __init__(self, component, errors):
        # type: (DictFlowIRComponent, List[Exception]) -> None
        self.component = component
        self.errors = errors

        self.message = '\n'.join(
            ['Could not convert component stage%s.%s because of %d errors:' % (
                component.get('stage', '*Unknown*'), component.get('name', '*Unknown*'),
                len(errors)
            )] + [str(exc) for exc in errors]
        )

    def __str__(self):
        return self.message


class FlowIRInvalidFieldType(FlowIRException):
    def __init__(self, field_route, expected_type, value):
        # type: (str, Any, Any) -> None
        Exception.__init__(self)

        self.field_route = field_route
        self.expected_type = expected_type
        self.value = value

        self.message = "'%s'='%s' cannot be converted to the expected type %s" % (
            field_route, value, expected_type
        )

    def __str__(self):
        return self.message


class FlowIRUnknownReferenceInArguments(FlowIRException):
    def __init__(self, ref_unknown: str, references: List[str],
                 component: str, stage: int, known_component_ids: List[FlowIRComponentId],
                 top_level_folders: List[str]):

        Exception.__init__(self)
        top_level_folders = top_level_folders or []
        known_component_ids = known_component_ids or []
        references= references or []

        self.top_level_folders = list(top_level_folders)
        self.ref_unknown = ref_unknown
        self.references = list(references)

        self.stage = stage
        self.component = component
        self.kown_component_ids = list(known_component_ids)

        self.message = 'Unknown reference to %s in the command-line arguments of "stage%s.%s". ' \
                       'Component references %s, all known components are, %s. The top-level folders in ' \
                       'workflow root directory are %s' % (
            ref_unknown, stage, component, references, ['stage%s.%s' % x for x in known_component_ids],
            top_level_folders,
        )

    def __str__(self):
        return self.message


class FlowIRReferenceToUnknownComponent(FlowIRException):
    def __init__(self, references, component, stage, known_component_ids):
        # type: (Union[str, List[str]], str, int, List[FlowIRComponentId]) -> None
        Exception.__init__(self)

        if isinstance(references, string_types):
            references = [references]

        self.references = references
        self.stage = stage
        self.component = component

        self.message = 'Unknown reference(s) %s by "stage%s.%s". Known components: %s' % (
            references, stage, component, ['stage%s.%s' % x for x in known_component_ids or []]
        )

    def __str__(self):
        return self.message


class FlowIRInvalidComponent(FlowIRException):
    def __init__(self, missing, extra, invalid, componentName, stage_index, message=None):
        # type: (Dict[str, Any], Dict[str, Any], Dict[str, Tuple[Any, Any]], str, int, Optional[str]) -> None
        Exception.__init__(self)

        # VV: `invalid` keys point to tuples containing (actual_value, expected_value(s))
        self.invalid = invalid
        self.missing = missing
        self.extra = extra
        self.componentName = componentName
        self.stage_index = stage_index

        message = message or ''

        self.message = "stage%s.%s is invalid. Missing fields: %s. Extra fields: %s. Invalid fields: %s.%s" % (
            stage_index, componentName, missing, extra, invalid, message
        )

    def __str__(self):
        return self.message


class FlowIRInvalidOverride(FlowIRException):
    def __init__(self, step_num, pending_override):
        # type: (int, List[Tuple[Any, Any, Callable[[Any], None]]]) -> None
        self.step_num = step_num
        self.pending_override = pending_override
        self.message = "Override requires more than the maximum number of steps (%d). " \
                       "It would take at least %d for the override method to terminate." % (
            self.step_num, len(self.pending_override)
        )

    def __str__(self):
        return self.message


class FlowIREnvironmentUnknown(FlowIRException):
    def __init__(self, name, platform, flowir):
        # type: (str, str, DictFlowIR) -> None
        Exception.__init__(self)

        self.flowir = flowir
        self.name = name
        self.platform = platform

        self.message = 'Unknown environment "%s" for platform "%s".' % (
            name, platform
        )

    def __str__(self):
        return self.message


class FlowIREnvironmentExists(FlowIRException):
    def __init__(self, name, platform, flowir):
        # type: (str, str, DictFlowIR) -> None
        Exception.__init__(self)

        self.flowir = flowir
        self.name = name
        self.platform = platform

        self.message = 'Environment "%s" for platform "%s" already exists.' % (
            name, platform
        )

    def __str__(self):
        return self.message


class FlowIRPlatformUnknown(FlowIRException):
    def __init__(self, platform, flowir):
        # type: (str, DictFlowIR) -> None
        Exception.__init__(self)

        self.platform = platform
        self.flowir = flowir

        self.message = 'Unknown platform "%s"' % (
            platform
        )

        if flowir:
            self.message += ' known platforms %s' % flowir.get('platforms', '*Unknown*')

    def __str__(self):
        return self.message


class FlowIRPlatformExists(FlowIRException):
    def __init__(self, platform, available_platforms):
        # type: (str, List[str]) -> None
        Exception.__init__(self)

        available_platforms = available_platforms or []
        self.platform = platform
        self.available_platforms = available_platforms[:]

        self.message = 'Platform "%s" already exists (available: %s)' % (
            platform, available_platforms
        )

    def __str__(self):
        return self.message


class FlowIRInconsistency(FlowIRException):
    def __init__(self, reason, flowir, exception=None, extra=None):
        # type: (str, DictFlowIR, Optional[Exception], Any) -> None
        Exception.__init__(self)

        self.reason = reason
        self.flowir = flowir
        self.exception = exception
        self.extra = extra

        self.message = 'Inconsistent FlowIR: %s' % self.reason

        if exception is not None:
            self.message = '%s. Underlying exception: %s' % (self.message, exception)

    def __str__(self):
        return self.message


class FlowIRVariablesIncomplete(FlowIRException):
    def __init__(self, reference_strings, label):
        # type: (List[str], str) -> None
        FlowIRException.__init__(self)
        self.reference_strings = sorted(reference_strings)
        self.label = label
        self.message = 'Incomplete variable substitution: %s' % str(reference_strings)

        if label is not None:
            self.message = '%s. Referenced by %s' % (self.message, label)

    def __str__(self):
        return self.message


class FlowIRVariableUnknown(FlowIRException):
    def __init__(self, variable_route, context, flowir, label=None):
        # type: (str, Dict[str, Any], DictFlowIR, Optional[str]) -> None
        FlowIRException.__init__(self)

        # VV: variable 'flow.isHybrid' has the route ['flow', 'isHybrid']
        self.variable_route = variable_route
        self.context = context
        self.flowir = flowir
        self.label = label

        if label is None:
            self.message = 'Attempted to resolve "%s". Context: %s' % (
                variable_route, [str(x) for x in context]
            )
        else:
            self.message = 'Attempted to resolve "%s" for "%s". Context: %s' % (
                variable_route, label, [str(x) for x in context]
            )

    def __str__(self):
        return self.message


class FlowIRVariableInvalid(FlowIRException):
    def __init__(self, format, variable_route, context, flowir, label=None):
        # type: (str, str, Dict[str, Any], DictFlowIR, Optional[str]) -> None
        Exception.__init__(self)

        # VV: variable 'flow.isHybrid' has the route ['flow', 'isHybrid']
        self.format = format
        self.variable_route = variable_route
        self.context = context
        self.flowir = flowir
        self.label = label

        if label is None:
            self.message = 'Attempted to resolve invalid typed "%s" from "%s". Context: %s' % (
                variable_route, format, context
            )
        else:
            self.message = 'Attempted to resolve invalid typed "%s" from "%s" for "%s". Context: %s' % (
                variable_route, format, label, context
            )

    def __str__(self):
        return self.message


class FlowIRComponentUnknown(FlowIRException):
    def __init__(self, comp_id, flowir=None):
        # type: (Tuple[int, str], Optional[DictFlowIR]) -> None
        Exception.__init__(self)

        self.comp_id = comp_id
        self.name = comp_id[1]
        self.stage_index = comp_id[0]
        self.flowir = flowir

        self.message = "Unknown component stage%s.%s" % comp_id

    def __str__(self):
        return self.message


class FlowIRComponentExists(FlowIRException):
    def __init__(self, comp_id, comp_desc, flowir):
        # type: (Tuple[int, str], DictFlowIRComponent, DictFlowIR) -> None
        Exception.__init__(self)

        self.comp_id = comp_id
        self.name = comp_id[1]
        self.stage_index = comp_id[0]
        self.flowir = flowir
        self.comp_desc = comp_desc

        self.message = "comp_id: %s already exists" % str(comp_id)
    def __str__(self):
        return self.message


class FLowIRSymbolTableNotImplemented(FlowIRException):
    def __init__(self):
        Exception.__init__(self)

        self.message = 'Have not implemented Flow Symbol Table yet'

    def __str__(self):
        return self.message


# VV: DOSINI Errors

class DOSINIDuplicateOption(Exception):
    def __init__(self, name, cfg, section):
        # type: (str, Union[configparser.ConfigParser, str], str) -> None
        Exception.__init__(self)
        self.name = name

        self.cfg = cfg
        self.section = section

        msg = 'Option "%s" supplied multiple times in section "%s"' % (
            name, section
        )

        if isinstance(cfg, string_types):
            msg += ' of file %s' % cfg

        self.message = msg

    def __str__(self):
        return self.message


class DOSINIForbiddenOption(Exception):
    def __init__(self, name, value, component_name, stage):
        # type: (str, str, str, int) -> None
        Exception.__init__(self)
        self.name = name
        self.value = value
        self.component_name = component_name
        self.stage = stage

        self.message = 'Forbidden option "%s=%s" for component stage%d.%s' % (
            name, value, stage, component_name
        )

    def __str__(self):
        return self.message


class DOSINIMissingRequiredOption(Exception):
    def __init__(self, name, component_name, stage):
        # type: (str, str, int) -> None
        Exception.__init__(self)
        self.name = name
        self.component_name = component_name
        self.stage = stage

        self.message = 'Component stage%d.%s is missing option "%s"' % (
            stage, component_name, name
        )

    def __str__(self):
        return self.message


class DOSINIInvalidSection(Exception):
    def __init__(self, filename, section):
        # type: (str, str) -> None
        self.filename = filename
        self.section = section

        self.message = 'Invalid section %s in DOSINI file %s' % (
            self.section, self.filename
        )

    def __str__(self):
        return self.message


class DOSINITypographicError(Exception):
    """Raised when a DOSINI section contains a typographical error of a keyword.

    Any option which is not a keyword AND is very similar to a keyword is considered at typo"""
    def __init__(self, location, typo, keyword):
        self.location = location
        self.typo = typo
        self.keyword = keyword
        self.message = 'DOSINI option in %s uses "%s" instead of "%s"' % (location, typo, keyword)
    def __str__(self):
        return self.message


class InvalidFileError(FlowException):
    def __init__(self, path, underlyingError):
        # type: (str, Exception) -> None
        self.path = path
        self.underlyingError = underlyingError
        self.message = "%s is invalid, underlying error is %s" % (path, underlyingError)

    def __str__(self):
        return self.message


class EnhancedException(FlowException):

    def __init__(self, desc, underlyingError):

        self.underlyingError = underlyingError

        super(EnhancedException, self).__init__(desc)

    def underlyingErrors(self):

        underlyingErrors = [self.underlyingError]
        if self.underlyingError is not None and isinstance(self.underlyingError, EnhancedException):
            underlyingErrors.extend(self.underlyingError.underlyingErrors())

        return underlyingErrors


class ComponentExecutableCannotBeFoundError(EnhancedException):

    def __init__(self, desc, underlyingError=None):

        EnhancedException.__init__(self,
                '%s\nUnderlying error:\n(%s) %s' % (desc, type(underlyingError), str(underlyingError)),
                underlyingError)


class ComponentSpecificationSyntaxError(EnhancedException):

    def __init__(self, desc, underlyingError=None):

        EnhancedException.__init__(self,
                '%s\nUnderlying error:\n(%s) %s' % (desc, type(underlyingError), str(underlyingError)),
                underlyingError)


class ExecutorOptionsError(EnhancedException):

    def __init__(self, desc, underlyingError=None):

        ue = ""
        if underlyingError is not None:
            ue = 'Underlying error:\n(%s) %s' % (
                                   underlyingError.__class__.__name__,
                                   str(underlyingError))

        EnhancedException.__init__(self,
                                   '%s\n%s' % (desc, ue),
                                   underlyingError)


class BackendExecutorMismatchError(Exception):

    '''Exception raised when a pre/post executor is specified but its unknown by the backend'''

    pass


class DataReferenceProducerNotFoundError(Exception):

    '''Exception for cases where the producer in a data reference cannot be found'''

    def __init__(self, referenceString):

        '''
        Parameters:
            referenceString: The data reference string defining the producer'''

        Exception.__init__(self,
            'Could not find producer with reference %s' % referenceString)


class ComponentNotFoundError(Exception):

    '''Exception for cases where the component referred to be a component-identifier cannot be found in a graph'''

    def __init__(self, componentIdentifier):

        '''
        Parameters:
            componentIdentifier (str): The string identfiying the component'''

        Exception.__init__(self,
            'Could not find component with identifier %s' % componentIdentifier)


class ComponentSpecificationNotFoundError(Exception):

    '''Exception for cases where there is no specification attached to the node in the graph called componentIdentifier'''

    def __init__(self, componentIdentifier):

        '''
        Parameters:
            componentIdentifier (str): The string identfiying the component'''

        Exception.__init__(self,
            'Component specification for node %s requested but none has been attached' % componentIdentifier)


class CircularComponentReferenceError(Exception):

    '''Exception for cases where two components have each other as repeat dependencies

    This can lead to a deadlock, while as each waits on the other to output data.
    It can also lead to an infinite loop - as neither process will ever finish'''

    def __init__(self, outReference, inReference):

        '''
        Parameters:
            outReference: The outgoing reference in the circular loop
            inReference: The incompong reference in the circular loop'''

        Exception.__init__(self,
            'Workflow graphs must be acyclic: '
            'component references %s which in-turn references this component (%s)' %
                           (outReference.stringRepresentation, inReference.stringRepresentation))


class DataReferenceFormatError(Exception):

    '''Exception for cases where there is an error in the format of a data reference'''

    def __init__(self, referenceErrors=None):

        '''
        Parameters:
            referenceErrors: A list of (reference,error desc) tuples'''

        Exception.__init__(self)
        referenceErrors = referenceErrors or []
        desc = ""
        for error in referenceErrors:
            desc += "Reference: %s. Error: %s\n" % error

        self.message = 'Encountered %d references with errors:\n%s' % (len(referenceErrors), desc)

    def __str__(self):

        return self.message


class InvalidComponentReferenceError(Exception):

    '''Exception when a component reference is not valid.

    This can only currently happen if the type qualifier is given but
    is not one of comp or component'''

    def __init__(self, reference):

        '''
        Parameters:
            reference: The offending reference'''

        Exception.__init__(self)

        self.message = 'Component reference %s is invalid - check type qualifier is "comp" or "component"' % reference

    def __str__(self):

        return self.message


class ComponentDuplicateDataReferenceError(EnhancedException):

    '''Error raised when the files a data reference contain duplicates'''

    def __init__(self, msg, referenceErrors=None):

        '''
        Parameters:
            referenceErrors: A list of references tuples'''

        Exception.__init__(self)
        referenceErrors = referenceErrors or None
        referenceErrors = referenceErrors or []
        desc = ""
        for error in referenceErrors:
            desc += "Reference already specified: %s\n" % error

        desc = 'Found %d duplicate references:\n%s' % (len(referenceErrors), desc)

        EnhancedException.__init__(self,
                                   '%s\nUnderlying error:\n%s' % (msg, str(desc)),
                                   desc)

class DataReferenceFilesDoNotExistError(Exception):

    '''Error raised when the files a data reference refers to do not exists'''

    def __init__(self, referenceErrors=None):

        '''
        Parameters:
            referenceErrors: A list of (reference,path) tuples'''

        Exception.__init__(self)
        referenceErrors = referenceErrors or []
        self.referenceErrors = referenceErrors  # type: List[Tuple[experiment.model.graph.DataReference, str]]

        desc = ""
        for ref, path in referenceErrors:
            desc += "Reference %s. File: %s\n" % (ref.stringRepresentation, path)

        self.referenceErrors = referenceErrors  # type: List[Tuple[experiment.model.graph.DataReference, str]]

        self.message = 'Found %d references to non-existant files:\n%s' % (len(referenceErrors), desc)

    def __str__(self):

        return self.message


class DataReferenceInconsistencyError(Exception):

    def __init__(self, reference, error):

        '''
        Parameters:
            error - An exception object describing why the data could not be staged'''

        Exception.__init__(self,

            'Inconsistency with reference %s. Reason:\n%s' % (reference.stringRepresentation, str(error)))


class DataReferenceCouldNotStageError(Exception):

    def __init__(self, reference, error):

        '''
        Parameters:
            error - An exception object describing why the data could not be staged'''
        msg = 'Could not stage reference %s. Reason follows:\n%s' % (reference.stringRepresentation, str(error))
        Exception.__init__(self, msg)

        self.message =msg

    def __str__(self):
        return self.message


class PackageLocationError(Exception):

    '''Raised when a specified package cannot be found'''

    def __init__(self, location):
        # type: (str) -> None
        self.location = location
        Exception.__init__(self,
             'Unable to find package at specified location: %s' % location)


class PackageStructureError(Exception):

    '''Raised when the structure of a package in invalid'''

    def __init__(self, location):
        # type: (str) -> None
        self.location = location
        Exception.__init__(self,
             'Invalid package structure: %s' % location)


class PackageCreateError(EnhancedException):

    '''Raised if unable to create a package directory out of a package file'''

    def __init__(self, underlyingError, location_package_directory, location_package_file):
        # type: (Exception, str, str) -> None
        self.location_package_directory = location_package_directory
        self.location_package_file = location_package_file

        EnhancedException.__init__(self,
                'Unable to create package directory at specified location %s for package-file %s.'
                '\nUnderlying error: %s' % (location_package_directory, location_package_file,
                                            str(underlyingError)), underlyingError)


class PackageUnknownFormatError(PackageStructureError):
    """Raised when conf.ExperimentConfiguration cannot generate a configuration for a package with unknown format"""

    def __init__(self, location, format_priority, is_instance):
        # type: (str, List[str], bool) -> None
        super(PackageUnknownFormatError, self).__init__(location)
        self.format_priority = list(format_priority or [])
        self.is_instance = is_instance

    def __str__(self):
        return "Could not find Configuration parser for "\
            "format_priority=%s, root_dir=%s, is_instance=%s" % (self.format_priority, self.location, self.is_instance)


class InstanceLocationError(Exception):

    '''Raised if there is a problem with the specified instance location'''

    def __init__(self, location):

        Exception.__init__(self,
             'Specified location, %s, for instance directory invalid' % location)


class InstanceCreateError(EnhancedException):

    '''Raised if the instance directory could not be created'''

    def __init__(self, underlyingError, location):

        self.location = location

        EnhancedException.__init__(self,
                'Unable to create instance directory at specified location.\nUnderlying error: %s' % (str(underlyingError)),
                underlyingError)


class ShadowLocationError(Exception):

    '''Raised if there is a problem with the specified shadow location'''

    def __init__(self, location):

        Exception.__init__(self,
             'Specified location, %s, for shadow directory invalid' % location)


class ShadowCreateError(EnhancedException):

    '''Raised if the shadow directory could not be created'''

    def __init__(self, underlyingError):

        EnhancedException.__init__(self,
                'Unable to create shadow directory at specified location.\nUnderlying error: %s' % (str(underlyingError)),
                underlyingError)


class ExperimentMissingConfigurationError(Exception):

    '''Raised if an expected configuration file is not present'''

    pass


class ExperimentInvalidConfigurationError(EnhancedException):

    '''Raised if the contents of a configuration file are invalid'''

    def __init__(self, desc, underlyingError=None):
        if underlyingError is not None:
            EnhancedException.__init__(self,
                    '%s\nUnderlying error:\n (%s) %s' % (desc,
                                                    underlyingError.__class__.__name__,
                                                    str(underlyingError)),
                    underlyingError)
        else:
            EnhancedException.__init__(self, '%s' % desc, underlyingError)


class InterfaceInputSpecExtractionError(ExperimentInvalidConfigurationError):
    def __init__(self, underlyingError: Exception, interface_label: str):
        """Raised when there is an error while processing the interface to extract the input ids

        Related FlowIR key: interface.inputSpec

        Args:
            underlyingError: Describes why extracting the input-ids failed.
        """
        self.interface_label = interface_label

        super(InterfaceInputSpecExtractionError, self).__init__(
            f'{interface_label}.inputSpec error: {underlyingError}', underlyingError)


class InterfacePropertySpecExtractionError(ExperimentInvalidConfigurationError):
    def __init__(self, underlyingError: Exception, interface_label: str, property_name: str):
        """Raised when there is an error while processing the interface to extract properties

        Related FlowIR key: interface.propertiesSpec

        Args:
            underlyingError: Describes why extracting the properties failed.
        """
        self.interface_label = interface_label
        self.propety_name = property_name

        super(InterfacePropertySpecExtractionError, self).__init__(
            f'{interface_label}.propertiesSpec[{property_name}] error: {underlyingError}', underlyingError)


class InvalidOutputEntry(ExperimentInvalidConfigurationError):
    """Raised when an OutputEntry is invalid"""

    def __init__(self, output_name, output_reference_str, underlyingError):
        desc = "Output \"%s\" has an invalid DataReference \"%s\"\n Underlying error:\n(%s) %s" % (
            output_name, output_reference_str, type(underlyingError), str(underlyingError))
        super(InvalidOutputEntry, self).__init__(desc, underlyingError)


class ExperimentTooManyVariableFilesError(ExperimentInvalidConfigurationError):
    """Raised if instructed to load an experiment and provided more than 1 variable files"""
    def __init__(self, exp_directory, variable_files):
        # type: ("ExperimentSource", List[str]) -> None
        self.exp_directory = exp_directory
        self.variable_files = list(variable_files or [])
        self.message = "Support up to 1 variable files but instructed to load %s for experiment directory %s" % (
            variable_files, exp_directory.location)
        super(ExperimentTooManyVariableFilesError, self).__init__(self.message, self)

    def __str__(self):
        return self.message


class UnusedDataReferenceError(Exception):

    '''Raised when a reference appears in a component specification thats not used'''

    def __init__(self, name, reference, msg):

        '''
        Parameters:
            name: The name of the component
            reference: The reference that's unused
            msg - A string explaining the specific error'''

        Exception.__init__(self,
                           'Reference %s declared by component %s is not used. Reason:\n%s' % (
                           reference.stringRepresentation,name, msg))


class UndeclaredDataReferenceError(Exception):

    '''Raised when one or more undeclared references are detected in the components command line'''

    def __init__(self, name, undeclared):

        '''
          Parameters:
              name (string): The name of the component whose command includes unresovled references
              undeclared (list) - A list of strings, one per unresolved reference, explaining where in the
                command line it occurs
            '''

        Exception.__init__(self,

                           'Undeclared references detected in component %s command line. Details:\n%s' % (
                           name, "\n".join(undeclared)))


class UndefinedPlatformError(ExperimentMissingConfigurationError):

    '''Raised if there an experiment does not define a platform with a given name'''

    def __init__(self, platform, searchErrors):

        '''
        Parameters:
            platform - Name of the platform for which no files exist
            errors - Errors raised for each failed check for configuration files for this platform'''

        Exception.__init__(self)

        self.message = "No configuration or variables are defined for the given platform, %s, in this experiment\n" % platform

        if searchErrors is not None:
            for e in searchErrors:
                print(e)
                self.message += "\t%s\n" % e

    def __str__(self):

        return self.message


class ConstructedEnvironmentConfigurationError(Exception)  :

    '''Raised if there is a problem with a constructed environment'''

    def __init__(self, msg, name):

        '''
        Parameters:
            msg - Message describing the error
            name - The constructed environments name'''

        Exception.__init__(self,
            "Error in constructed environment %s: %s" % (name, msg))


class PathNotRelativeError(Exception):

    def __init__(self, path):

        '''
        Parameters:
            error - An absolute path was given when a relative was required'''

        Exception.__init__(self,
            'Absolute path %s provided when a relative path required' % path)


class JobNotFoundError(Exception):

    '''Error raised when a Job object could not be found when expected'''

    def __init__(self, jobName, stage):

        '''
        Parameters:
            error - A Job with the given name could not be found'''

        Exception.__init__(self,
            'No Job called %s could be found in stage %d' % (jobName, stage))


class ExperimentSetupError(EnhancedException):

    '''Raised if there was an issue with experiment setup'''

    def __init__(self, desc, underlyingError, instance_dir: experiment.model.storage.ExperimentInstanceDirectory | None):
        self.instance_dir = instance_dir

        EnhancedException.__init__(self,
                'Error encountered while setting up experiment.\n%s\nUnderlying error: %s' % (desc,str(underlyingError)),
                underlyingError)


class InternalInconsistencyError(Exception):

    '''Raised when an inconsistency in the state of the program is noticed'''

    def __init__(self, string):

        Exception.__init__(self,
                'Internal inconsistency detected: %s. This indicates a programming bug' % string)


class ExperimentError(EnhancedException):

    '''A generic error occurred with the experiment

    Note: This error should be used specifically for 'experimental' errors
    e.g. convergence, experiment jobs crashing.
    Errors due to the system the experiments is running on e.g. filesystem errors,
    should not handled differently'''


class DataFileUpdateError(Exception):

    '''Error raised when attempting to update a data file that doesn't exist'''

    def __init__(self, name):

        Exception.__init__(self)

        self.message = "No file called %s exists in data directory - cannot update" % name

    def __str__(self):

        return self.message


class InvalidSimulatorModel(Exception):
    def __init__(self, model_path, exc):
        # type: (str, Exception) -> None
        self.model_path = model_path
        self.underlying_exception = exc

        self.message = 'Invalid simulator model %s (exception: %s)' % (
            model_path, str(exc)
        )


class MixingSimulatorWithRealBackend(Exception):
    def __init__(self, job_types):
        Exception.__init__(self)

        self.job_types = job_types

        non_simulator = { }

        for key in job_types:
            if key != 'simulator':
                non_simulator[key] = ', '.join([j.reference for j in job_types[key]])

        self.message = "You are mixing Simulator-type jobs with "\
               "something else, are you sure about that? Different job types: %s" % (
                   non_simulator
               )

    def __str__(self):
        return self.message


class InvalidValueForConstant(Exception):
    def __init__(self, section_name, attribute_name, expected_type, received_value):
        # type: (str, str, str, str) -> None

        Exception.__init__(self)
        self.expected_type = expected_type
        self.received_value = received_value
        self.section_name = section_name
        self.attribute_name = attribute_name

        self.message = "Expected a %s for attribute %s of %s but received %s" % (
            expected_type, attribute_name, section_name, received_value
        )

    def __str__(self):
        return self.message


class FilesystemInconsistencyError(EnhancedException):

    '''Raised when an inconsistency is detected with the filesystem.

    This is usually when the expected state of a filesystem object has changed.
    For example a directory that was validated as readable, and that must be readable for
    the program to function, becomes unreadable.
    Another example is if an executable which was validated as present then disappears'''

    def __init__(self, desc, underlyingError):

        EnhancedException.__init__(self,
                'Filesystem inconsistency detected  %s\nUnderlying error %s' % (desc, str(underlyingError)),
                                   underlyingError)

class SystemError(EnhancedException):

    '''Raised when an system level issue was detected'''

    def __init__(self, underlyingError):

        EnhancedException.__init__(self,
                'Problem encountered with system.\n %s' % str(underlyingError),
                                   underlyingError)