# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# coding=UTF-8
#
# IBM Confidential
# OCO Source Materials
# 5747-SM3
# (c) Copyright IBM Corp. 2022
# The source code for this program is not published or otherwise
# divested of its trade secrets, irrespective of what has
# been deposited with the U.S. Copyright Office.
# Author(s): Vassilis Vassiliadis
# Contact: Vassilis.Vassiliadis@ibm.com

from __future__ import annotations

import os.path
import traceback
import typing
import copy
import errno
import inspect

import experiment.model.hooks.utils
import experiment.model.frontends.flowir
import experiment.model.errors
import experiment.model.data
import experiment.model.errors
import pandas


class ExtractionMethodSource:
    # This how OpenAPI model classes work
    types = {
        'key_output': 'str',
        'path': 'str',
        'path_list': 'typing.List[str]'
    }

    attribute_map = {
        'key_output': 'keyOutput',
        'path': 'path',
        'path_list': 'pathList'
    }

    def __init__(self, key_output: str | None, path: str | None, path_list: typing.List[str] | None):
        """Provides input to an ExtractionMethod. Only one of the fields must be non-None

        Args:
            key_output: name of key output
            path: relative path to the instance directory
            path_list: list of relative paths to the instance directory
        """
        self.key_output = key_output
        self.path_list = path_list
        self.path = path

    def to_dict(self):
        """The dictionary definition of the object

        Returns:
            The dictionary definition of the object
        """
        result = {}

        for attr in self.attribute_map:
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(map(
                    lambda x: x.to_dict() if hasattr(x, "to_dict") else x,
                    value
                ))
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(map(
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            else:
                result[attr] = value

        return result

    def validate(self):
        """Validates the definition

        Returns:
            None

        Raises:
            experiment.model.errors.hooks.ExtractionMethodSourceInvalidError: if definition is invalid
        """

        many = [x for x in [self.key_output, self.path, self.path_list] if x is not None]
        if len(many) != 1:
            msg = f'ExtractionMethodSource should specify exactly 1 of {", ".join(self.attribute_map.values())}'
            raise experiment.model.errors.ExtractionMethodSourceInvalidError(msg, self.to_dict())

        if self.path and os.path.isabs(self.path) is True:
            msg = f'ExtractionMethodSource.path is absolute path, it should be relative to the instance directory'
            raise experiment.model.errors.ExtractionMethodSourceInvalidError(msg, self.to_dict())

        if self.path_list:
            for i, path in enumerate(self.path_list):
                if os.path.isabs(path):
                    msg = f'ExtractionMethodSource.path_list[i] is absolute path, it should be relative to the instance directory'
                    raise experiment.model.errors.ExtractionMethodSourceInvalidError(msg, self.to_dict())

    def resolve(self, exp: experiment.model.data.Experiment, absolute=True) -> typing.List[str]:
        """Interprets the object definition to return a list of paths.

        Args:
            exp: An experiment  to use to resolve paths
            absolute: Whether to return absolute paths or relative paths to the instance directory. Default is True

        Returns:
            A list of paths

        Raises:
            experiment.model.errors.hooks.ExtractionMethodSourceInvalidError: if definition is invalid
        """
        self.validate()

        # VV: We record the absolute path. If @absolute is False we convert the paths to relative ones wrt instance dir
        paths: typing.List[str] = []

        if self.key_output is not None:
            outputs = exp.resolve_key_outputs()

            if self.key_output not in outputs:
                msg = f'ExtractionMethodSource.key_output is unknown, known keyOutputs are {", ".join(outputs)}'
                raise experiment.model.errors.ExtractionMethodSourceInvalidError(msg, self.to_dict())

            paths.append(outputs[self.key_output])
        elif self.path:
            paths.append(os.path.join(exp.instanceDirectory.location, self.path))
        else:
            root_instance_dir = exp.instanceDirectory.location
            paths.extend(map(lambda x: os.path.join(root_instance_dir, x), self.path_list))

        if absolute is False:
            root_instance = exp.instanceDirectory.location
            paths = [os.path.relpath(path, root_instance) for path in paths]

        return paths

# VV: Arguments to InputExtractionMethods are (source, experiment, kwargs)
TypeInputExtractionMethod = typing.Callable[
    [ExtractionMethodSource, "experiment.model.data.Experiment", typing.Dict[str, typing.Any]], typing.List[str]]


class InputExtractionMethod:
    supported_methods: typing.Dict[str, str] = {
        "csvColumn": "method_csv_column",
        "hookGetInputIds": "method_hook_get_input_ids",
    }

    def __init__(self, source: ExtractionMethodSource, method_name: str, extra_args: typing.Dict[str, typing.Any]):
        self.method_name = method_name
        self.source = source
        self.kwargs = extra_args.copy()

    def to_dict(self):
        return {
            'method_name': self.method_name,
            'source': self.source.to_dict() if self.source is not None else None,
            'kwargs': self.kwargs
        }

    def validate(self, exp: experiment.model.data.Experiment, interface_label):
        """Validates the definition

        Args:
            exp: Experiment to use for validating this input extraction method
            interface_label: the label of the owner interface

        Returns:
            None

        Raises:
            experiment.model.errors.hooks.InputExtractionMethod: if definition is invalid
        """

        if self.method_name not in self.supported_methods:
            msg = f"InputExtractionMethod.method_name is unknown, known methods are {', '.join(self.supported_methods)}"
            raise experiment.model.errors.InputExtractionMethodUnknownError(msg, self.to_dict())

        item_method = self.supported_methods[self.method_name]

        # VV: replace method_ with validate_
        item_method = f"validate_{item_method[7:]}"
        validate = getattr(self, item_method)
        validate(self.source, exp, self.kwargs, interface_label)

    @classmethod
    def validate_hook_get_additional_input_data(
            cls,
            exp: experiment.model.data.Experiment,
            interface_label: str,
    ):
        me = f"{interface_label}.inputSpec.impliedHookGetAdditionalInputData"

        try:
            hooks_dir = exp.instanceDirectory.hooksDir
            mod_interface = experiment.model.hooks.utils.import_hooks_interface(hooks_dir)
            signature = inspect.signature(mod_interface.get_additional_input_data)
            if list(signature.parameters) != ["input_id", "instance_dir"]:
                raise experiment.model.errors.RuntimeHookError(
                    f"{me} does not implement get_additional_input_data(input_id: str, instance_dir: str), "
                    f"its signature is get_additional_input_data({', '.join(signature.parameters)})")
        except experiment.model.errors.RuntimeHookError as e:
            raise e from e
        except Exception as e:
            raise experiment.model.errors.RuntimeHookError(
                f"Unable to inspect get_additional_input_data() for {me} because {e}") from e


    @classmethod
    def validate_hook_get_input_ids(
            cls,
            source: ExtractionMethodSource,
            exp: experiment.model.data.Experiment,
            kwargs: typing.Dict[str, typing.Any],
            inteface_label: str,
    ):
        """Validates whether the python interface of hook get_input_ids() is Correct

        Raises:
            experiment.model.errors.RuntimeHookError: if interface is invalid
        """
        me = f"{inteface_label}.inputSpec.hookGetInputIds"

        if source.path is None:
            raise experiment.model.errors.RuntimeHookError(f"{me} must have a source.path")

        if kwargs:
            raise experiment.model.errors.RuntimeHookError(f"{me} does not support args")

        hooks_dir = exp.instanceDirectory.hooksDir
        try:
            mod_interface = experiment.model.hooks.utils.import_hooks_interface(hooks_dir)
        except (ImportError, FileNotFoundError) as e:
            raise experiment.model.errors.RuntimeHookError(
                f"Unable to import interface hooks for {me} because {e}") from e

        try:
            signature = inspect.signature(mod_interface.get_input_ids)
            params = list(signature.parameters)

            if params != ["input_id_file", "variables"] and params != ["input_id_file"]:
                raise experiment.model.errors.RuntimeHookError(
                    f"{me} does not implement "
                    f"get_input_ids(input_id_file: str, variables: typing.Dict[str, str]), or "
                    f"get_input_ids(input_id_file: str), "
                    f"its signature is get_input_ids({', '.join(signature.parameters)})")
        except experiment.model.errors.RuntimeHookError as e:
            raise e from e
        except Exception as e:
            raise experiment.model.errors.RuntimeHookError(
                f"Unable to inspect get_input_ids() for {me} because {e}") from e

    @classmethod
    def validate_csv_column(
            cls,
            source: ExtractionMethodSource,
            exp: experiment.model.data.Experiment,
            kwargs: typing.Dict[str, typing.Any],
            interface_label: str,
    ):
        """Validates whether the python interface of hook get_input_ids() is Correct

        Raises:
            experiment.model.errors.RuntimeHookError: if interface is invalid
        """
        me = f"{interface_label}.inputSpec.csvColumn"

        if source.path is None and (source.path_list is None or len(source.path_list) != 1):
            raise experiment.model.errors.RuntimeHookError(f"{me} must have a source.path "
                                                             f"or source.pathList with 1 item")

        if list(kwargs or {}) != ["column"]:
            raise experiment.model.errors.RuntimeHookError(f"{me} expects args.column")

    @classmethod
    def method_hook_get_input_ids(
            cls,
            source: ExtractionMethodSource,
            exp: experiment.model.data.Experiment,
            kwargs: typing.Dict[str, typing.Any],
    ) -> typing.List[str]:
        paths = source.resolve(exp, absolute=True)

        if len(paths) != 1:
            raise experiment.model.errors.RuntimeHookError(
                "get_input_ids() hook expects an ExtractionMethodSource that resolves to exactly 1 path, however "
                f"source resolved to {len(paths)} paths")
        try:
            exp.instanceDirectory.attempt_fix_hooks_directory()
            hooks_dir = exp.instanceDirectory.hooksDir
            mod_interface = experiment.model.hooks.utils.import_hooks_interface(hooks_dir)
        except Exception as e:
            raise experiment.model.errors.RuntimeHookError(
                f"unable to import hooks/interface.py containing "
                f"get_input_ids(input_id_file, variables) hook due to {e}") from e

        try:
            signature = inspect.signature(mod_interface.get_input_ids)
            params = list(signature.parameters)
            if 'variables' in params:
                variables = exp.configuration.get_global_variables(include_user_variables=True)
                return copy.deepcopy(mod_interface.get_input_ids(paths[0], variables))
            else:
                return copy.deepcopy(mod_interface.get_input_ids(paths[0]))
        except Exception as e:
            raise experiment.model.errors.RuntimeHookError(
                f"get_input_ids(input_id_file, variables) failed with {type(e).__name__}: {e}") from e

    @classmethod
    def method_csv_column(
            cls,
            source: ExtractionMethodSource,
            exp: experiment.model.data.Experiment,
            kwargs: typing.Dict[str, typing.Any],
    ) -> typing.List[str]:
        paths = source.resolve(exp, absolute=True)
        column: str = kwargs['column']

        if len(paths) != 1:
            raise experiment.model.errors.RuntimeHookError(
                "csvColumn hook expects an ExtractionMethodSource that resolves to exactly 1 path, however "
                f"source resolved to {len(paths)} paths")
        panda_args = {x: kwargs[x] for x in kwargs if x not in ['column']}

        # VV: Try configure pandas to sniff header
        if 'engine' not in panda_args:
            panda_args['engine'] = 'python'
        if 'sep' not in panda_args:
            panda_args['sep'] = None

        df: pandas.DataFrame = pandas.read_csv(filepath_or_buffer=paths[0], **panda_args)
        try:
            input_ids: typing.List[str] = df[column].to_list()
        except KeyError as e:
            raise experiment.model.errors.RuntimeHookError(
                f"Unknown column {column} in {paths[0]}, known columns are {df.columns}") from e

        start_index = int(kwargs.get('startIndex', 0))
        number_rows = int(kwargs.get('numberRows', len(input_ids) - start_index))
        return input_ids[start_index: (start_index + number_rows)]

    def resolve(self, exp: experiment.model.data.Experiment) -> typing.List[str]:
        """Processes the inputExtractionMethod to return a list of inputIds

        Args:
            exp: The experiment for which to discover which inputIds it processed

        Returns:
            A list of strings, each string is an input id
        Raises:
            experiment.model.errors.hooks.InputExtractionMethod: if definition is invalid
            experiment.model.errors.RuntimeHookError: If the hook detects a problem
            OSError: if a required file does not exist
        """
        try:
            item_method = self.supported_methods[self.method_name]
            method: TypeInputExtractionMethod = getattr(self, item_method)
            input_ids = method(self.source, exp, self.kwargs)
            return input_ids
        except Exception as e:
            exp.log.log(15, f"While running inputExtractionMethod {traceback.format_exc()}")
            raise experiment.model.errors.RuntimeHookError(f"{self.method_name} failed with {e}") from e


# VV: Arguments to InputExtractionMethods are (source, experiment, property_name, path_input_ids, kwargs)
TypePropertyExtractionMethod = typing.Callable[
    [ExtractionMethodSource, "experiment.model.data.Experiment", str, str, typing.Dict[str, typing.Any]],
    pandas.DataFrame]


class PropertyExtractionMethod:
    supported_methods: typing.Dict[str, str] = {
        "hookGetProperties": "method_hook_get_properties",
        "csvDataFrame": "method_csv_readDataFrame",
    }

    def __init__(
            self,
            source: ExtractionMethodSource, method_name: str,
            property_name: str,
            extra_args: typing.Dict[str, typing.Any]
    ):
        self.method_name = method_name
        self.source = source
        self.property_name = property_name

        self.kwargs = extra_args

    def to_dict(self):
        return {
            'method_name': self.method_name,
            'source': self.source.to_dict() if self.source is not None else None,
            'kwargs': self.kwargs
        }

    def validate(self, exp: experiment.model.data.Experiment, interface_label: str):
        """Validates the definition

        Args:
            exp: Experiment to use for testing the definition of property extraction method
            interface_label: Label of the owner interface

        Returns:
            None

        Raises:
            experiment.model.errors.RuntimeHookError: if definition is invalid
            experiment.model.errors.InputExtractionMethodUnknownError: if extraction method is unknown
        """

        if self.method_name not in self.supported_methods:
            msg = f"InputExtractionMethod.method_name is unknown, known methods are {', '.join(self.supported_methods)}"
            raise experiment.model.errors.InputExtractionMethodUnknownError(msg, self.to_dict())

        item_method = self.supported_methods[self.method_name]

        # VV: replace method_ with validate_
        item_method = f"validate_{item_method[7:]}"
        validate = getattr(self, item_method)
        validate(self.source, exp, self.property_name, self.kwargs, interface_label)

    @classmethod
    def validate_hook_get_properties(
            cls,
            source: ExtractionMethodSource,
            exp: experiment.model.data.Experiment,
            property_name: str,
            kwargs: typing.Dict[str, typing.Any],
            interface_label: str,
    ):
        """Validates whether the python interface of hook get_properties() is Correct

        Raises:
            experiment.model.errors.RuntimeHookError: if interface is invalid
        """
        me = f"{interface_label}.propertiesSpec[{property_name}].hookGetProperties"
        if source.path is None and source.path_list is None and source.key_output is None:
            raise experiment.model.errors.RuntimeHookError(f"{me} must have a source")

        hooks_dir = exp.instanceDirectory.hooksDir
        try:
            mod_interface = experiment.model.hooks.utils.import_hooks_interface(hooks_dir)
        except (ImportError, FileNotFoundError) as e:
            raise experiment.model.errors.RuntimeHookError(
                f"Unable to import interface hooks for {me} because {e}") from e

        try:
            signature = inspect.signature(mod_interface.get_properties)
            params = list(signature.parameters)

            if params != ["property_name", "property_output_file", "input_id_file"] and \
                    params != ["property_name", "property_output_file", "input_id_file", "variables"]:
                raise experiment.model.errors.RuntimeHookError(
                    f"{me} does not implement "
                    f"get_properties(property_name: str, property_output_file: str, "
                    f"input_id_file: str) -> pandas.DataFrame, or "
                    f"get_properties(property_name: str, property_output_file: str, input_id_file: str, "
                    f"variables: Dict[str, str]) -> pandas.DataFrame, "
                    f"its signature is get_properties({', '.join(signature.parameters)})")
        except experiment.model.errors.RuntimeHookError as e:
            raise e from e
        except Exception as e:
            raise experiment.model.errors.RuntimeHookError(
                f"Unable to inspect get_properties() for {me} because {e}") from e

    @classmethod
    def validate_csv_readDataFrame(
            cls,
            source: ExtractionMethodSource,
            exp: experiment.model.data.Experiment,
            property_name: str,
            kwargs: typing.Dict[str, typing.Any],
            interface_label: str,
    ):
        """Validates whether the definition of csvDataFrame() is Correct

        Raises:
            experiment.model.errors.RuntimeHookError: if interface is invalid
        """
        me = f"{interface_label}.propertiesSpec[{property_name}].csvDataFrame"

        if source.path is None and source.path_list is None and source.key_output is None:
            raise experiment.model.errors.RuntimeHookError(f"{me} must have a source")

    @classmethod
    def method_hook_get_properties(
            cls,
            source: ExtractionMethodSource,
            exp: experiment.model.data.Experiment,
            property_name: str,
            path_input_ids: str,
            kwargs: typing.Dict[str, typing.Any],
    ) -> pandas.DataFrame:
        paths = source.resolve(exp, absolute=True)

        if len(paths) != 1:
            raise experiment.model.errors.RuntimeHookError(
                "get_properties() hook expects an ExtractionMethodSource that resolves to exactly 1 path, however "
                f"source resolved to {len(paths)} paths")
        try:
            exp.instanceDirectory.attempt_fix_hooks_directory()
            hooks_dir = exp.instanceDirectory.hooksDir
            mod_interface = experiment.model.hooks.utils.import_hooks_interface(hooks_dir)

            signature = inspect.signature(mod_interface.get_properties)
            params = list(signature.parameters)
            if 'variables' in params:
                variables = exp.configuration.get_global_variables(include_user_variables=True)
                return mod_interface.get_properties(property_name, paths[0], path_input_ids, variables)
            else:
                return mod_interface.get_properties(property_name, paths[0], path_input_ids)
        except experiment.model.errors.RuntimeHookError as e:
            raise e from e
        except Exception as e:
            raise experiment.model.errors.RuntimeHookError(
                f"Unable to run get_properties() for {property_name} because {e}") from e

    @classmethod
    def method_csv_readDataFrame(
            cls,
            source: ExtractionMethodSource,
            exp: experiment.model.data.Experiment,
            property_name: str,
            path_input_ids: str,
            kwargs: typing.Dict[str, typing.Any],
    ) -> pandas.DataFrame:
        """

        ExtractionMethod arguments ::

            - renameColumns(Dict[str, str]): Rename column `key` to `value`

        Args:
            source:
            exp:
            property_name:
            path_input_ids:
            kwargs: See section about ExtractionMethod arguments


        Returns:

        """
        paths = source.resolve(exp, absolute=True)

        if len(paths) != 1:
            raise experiment.model.errors.RuntimeHookError(
                "csvColumn hook expects an ExtractionMethodSource that resolves to exactly 1 path, however "
                f"source resolved to {len(paths)} paths")

        panda_args = {x: kwargs[x] for x in kwargs if x not in ['renameColumns']}

        # VV: Try configure pandas to sniff header
        if 'engine' not in panda_args:
            panda_args['engine'] = 'python'
        if 'sep' not in panda_args:
            panda_args['sep'] = None

        df: pandas.DataFrame = pandas.read_csv(filepath_or_buffer=paths[0], **panda_args)

        rename_columns = kwargs.get('renameColumns')
        if rename_columns:
            df.rename(columns=rename_columns, inplace=True)

        return df

    def resolve(self, exp: experiment.model.data.Experiment, path_inputids: str) -> pandas.DataFrame:
        """Processes the PropertyExtractionMethod to return a Dataframe

        Args:
            exp: The experiment for which to discover which inputIds it processed
            path_inputids: Path to inputIds, if path is not absolute it is considered relative to the instance directory

        Returns:
            A list of strings, each string is an input id
        Raises:
            experiment.model.errors.hooks.InputExtractionMethod: if definition is invalid
            experiment.model.errors.RuntimeHookError: If the hook detects a problem
            OSError: if a required file does not exist
        """
        try:
            item_method = self.supported_methods[self.method_name]
            method: TypePropertyExtractionMethod = getattr(self, item_method)

            if os.path.abspath(path_inputids) is False:
                path_inputids = os.path.join(exp.instanceDirectory.location, path_inputids)

            return method(self.source, exp, self.property_name, path_inputids, self.kwargs)
        except Exception as e:
            exp.log.log(15, f"While running propertyExtractionMehtod {self.method_name} "
                            f"for {self.property_name} {traceback.format_exc()}")
            raise experiment.model.errors.RuntimeHookError(f"{self.method_name} failed with {e}") from e



class Interface:
    @classmethod
    def _extract_one_method(
            cls,
            name: str,
            methods: typing.Dict[str, typing.Dict[str, typing.Any]]
    ) -> typing.Tuple[str, typing.Dict[str, typing.Any]]:
        """Extracts 1 method out of many

        This works similar to how you would define VolumeMounts in kubernetes, there is 1 huge struct and you must
        define just 1 of the fields.

        Args:
            name: name of container of methods (e.g. interface.inputSpec, or interface.propertiesSpec[3]) - for debugging
            methods: A dictionary whose keys are names of methods and values are dictionaries (or None) containing the
                configuration of the method. ONLY 1 method_name must be populated

        Returns:
            The method name, and method configuration

        Raises:
            experiment.model.errors.FlowIRSyntaxException: If there is not exactly 1 definition of a method
        """
        methods = {x: methods[x] for x in methods if methods[x] is not None}
        if len(methods) != 1:
            raise experiment.model.errors.FlowIRSyntaxException(
                f"{name} does not contain a single extraction method definition. it contains {methods}")
        return methods.popitem()

    @classmethod
    def _parse_extraction_method(
            cls,
            methods: typing.Dict[str, typing.Any],
            label: str,
    ) -> typing.Tuple[ExtractionMethodSource, str, typing.Dict[str, typing.Any]]:
        method_name, method = cls._extract_one_method(label, methods)

        source = ExtractionMethodSource(
            key_output=method.get('source', {}).get('keyOutput'),
            path=method.get('source', {}).get('path'),
            path_list=method.get('source', {}).get('pathList'),
        )

        method_args: typing.Dict[str, typing.Any] = method.get('args', {})
        return source, method_name, method_args

    def __init__(self, interface: experiment.model.frontends.flowir.DictFlowIRInterface, label: str):
        self.interface = copy.deepcopy(interface)
        self.has_additional_data = interface['inputSpec']['hasAdditionalData']
        self.label = label

        self.input_extraction_method = self._get_inputspec_extraction_method()
        self.properties_extraction_methods = self._get_propertiesspec_extraction_methods()

    def validate(self, exp: experiment.model.data.Experiment):
        """Validates the Interface

        Method does not use input_id_file to test interface signatures

        Args:
            exp: Experiment to use for validating this input extraction method

        Raises:
            experiment.model.errors.RuntimeHookError: Interface is invalid with underlying exception
        """
        self.input_extraction_method.validate(exp, self.label)

        if self.has_additional_data:
            InputExtractionMethod.validate_hook_get_additional_input_data(exp, self.label)

        for prop in self.properties_extraction_methods:
            prop.validate(exp, self.label)

    def _get_inputspec_extraction_method(self) -> InputExtractionMethod:
        """Generates the InputExtractionMethod object for interface.inputSpec

        Returns:
            An input extraction method for the interface.inputSpec
        """
        input_spec: typing.Dict[str, typing.Any] = self.interface['inputSpec']
        source, method_name, method_args = self._parse_extraction_method(
            input_spec["inputExtractionMethod"],
            label=f"{self.label}.inputSpec")

        return InputExtractionMethod(source, method_name, method_args)

    def _get_propertiesspec_extraction_methods(self) -> typing.List[PropertyExtractionMethod]:
        """Generates the PropertyExtractionMethod objects 1 for each interface.propertiesSpec

        Returns:
            A list of PropertyExtractionMethod objects
        """
        properties_spec: typing.List[typing.Dict[str, typing.Any]] = self.interface['propertiesSpec']

        ret = []
        for idx, prop in enumerate(properties_spec):
            source, method_name, method_args = self._parse_extraction_method(
                prop["propertyExtractionMethod"],
                label=f"{self.label}.propertiesSpec[{idx}]")

            ret.append(PropertyExtractionMethod(source, method_name, prop['name'], method_args))

        return ret

    def extract_input_ids(
            self,
            exp: experiment.model.data.Experiment,
    ) -> typing.Tuple[typing.List[str], typing.Dict[str, typing.List[str]]]:
        """Executes the inputSpec instructions to extract the input ids amd addotional data

        Args:
            exp: Virtual experiment instance to process

        Returns:
            A tuple with 2 entries.
            The first entry is a list of input ids.
            The second entry is a dictionary with key: value pairs. The key is an inputId the value is an array of
            paths to additional data files associated with the input id. Each path is relative to the instance directory.

        Raises:
            NotImplementedError: Functionality is not implemented yet
            experiment.model.errors.InterfaceInputSpecExtractionError: Interface is invalid with underlying exception
        """
        input_ids: typing.List[str] = []
        additional_input_data: typing.Dict[str, typing.List[str]] = {}

        exp.log.info(f"Processing {self.label}.inputSpec")
        try:
            paths = self.input_extraction_method.source.resolve(exp)

            if len(paths) != 1:
                raise experiment.model.errors.RuntimeHookError(
                    f"{self.label} must point to exactly 1 path containing input ids, however it points to {paths}")

            path_inputids = os.path.join(exp.instanceDirectory.location, paths[0])
            if os.path.exists(path_inputids) is False:
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path_inputids)

            input_ids = self.input_extraction_method.resolve(exp)

            if self.has_additional_data is True:
                exp.instanceDirectory.attempt_fix_hooks_directory()
                hooks_dir = exp.instanceDirectory.hooksDir
                mod_interface = experiment.model.hooks.utils.import_hooks_interface(hooks_dir)
                instance_dir = exp.instanceDirectory.location

                for input_id in input_ids:
                    try:
                        paths: typing.List[str] = mod_interface.get_additional_input_data(input_id, instance_dir)
                    except experiment.model.errors.RuntimeHookError as e:
                        # VV: in python 3< 3.9 raising an exception inside except does not auto chain exceptions
                        raise e from e
                    except Exception as e:
                        raise experiment.model.errors.EnhancedException(
                            f"Unable to extract the additional input data files for input id {input_id}, "
                            f"{self.label}.inputSpec is invalid.", e) from e
                    else:
                        for p in paths:
                            if os.path.exists(p) is False:
                                exp.log.warning(f"AdditionalInputFile {p} for input id \"{input_id}\" "
                                                f"does not exist - {self.label}.inputSpec is invalid")
                                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), p)

                        additional_input_data[input_id] = paths
        except Exception as e:
            raise experiment.model.errors.InterfaceInputSpecExtractionError(e, self.label) from e
        exp.log.info(f"Finished processing {self.label}.inputSpec")

        return input_ids, additional_input_data

    def extract_properties(
            self,
            exp: experiment.model.data.Experiment,
            input_ids: typing.List[str],
    ) -> pandas.DataFrame | None:
        """Executes the propertiesSpec instructions of an experiment's interface

        Args:
            exp: Experiment to process
            input_ids: List of input ids for interface
            self.label: label of interface (E.g. FlowIR.interface, FlowIR.runtimePropertiesInterface[0], etc)
                - for debugging and generation of Exceptions

        Raises:
            NotImplementedError: Functionality is not implemented yet
            experiment.model.errors.RuntimeHookError: Interface is invalid with underlying exception
        """
        try:
            if not self.properties_extraction_methods:
                exp.log.info(f"Experiment does not have any {self.label}.propertiesSpec - will not measure properties")
                return None

            if input_ids is None:
                raise experiment.model.errors.RuntimeHookError(
                    f"missing inputIds - check whether {self.label}.inputSpec is problematic")

            exp.log.info(f"Processing {self.label}.propertiesSpec")

            # VV: We will join() all the pandas.DataFrames that the propertiesSpec instructions generate
            all_properties = pandas.DataFrame.from_dict({'input-id': input_ids})

            paths = self.input_extraction_method.source.resolve(exp)

            if len(paths) != 1:
                raise experiment.model.errors.RuntimeHookError(
                    f"{self.label} must point to exactly 1 path containing input ids, however it points to {paths}")

            path_inputids = os.path.join(exp.instanceDirectory.location, paths[0])
            if os.path.exists(path_inputids) is False:
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path_inputids)
        except Exception as e:
            raise experiment.model.errors.InterfacePropertySpecExtractionError(
                e, interface_label=self.label, property_name="all properties") from e

        for idx, prop_extract in enumerate(self.properties_extraction_methods):
            try:
                exp.log.info(f"Using propertyExtraction {prop_extract.method_name} with args {prop_extract.kwargs} "
                             f"for property {prop_extract.property_name}")

                pd_prop = prop_extract.resolve(exp, path_inputids)
                pd_prop.rename(columns={x: x.lower() for x in pd_prop.columns}, inplace=True)

                for x in ['input-id', prop_extract.property_name.lower()]:
                    if x not in pd_prop.columns:
                        raise experiment.model.errors.MissingColumnError(
                            prop_extract.property_name.lower(), x, prop_extract.source.resolve(exp, absolute=False))

                # AP - Merge on the intersection of the columns
                # ref: https://github.ibm.com/st4sd/st4sd-runtime-core/issues/70
                shared_columns = all_properties.columns.intersection(pd_prop.columns)
                all_properties = all_properties.merge(pd_prop, on=shared_columns.tolist(), how="outer")
            except Exception as e:
                raise experiment.model.errors.InterfacePropertySpecExtractionError(
                    e, interface_label=self.label, property_name=prop_extract.property_name) from e

        exp.log.info(f"Finished processing {self.label}.propertiesSpec")
        return all_properties
