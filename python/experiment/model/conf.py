# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

"""Class/Functions for working with experiment configuration.

This includes the files and the structure of elements in those conf files"""

from __future__ import annotations
from __future__ import print_function

import configparser
import copy
import difflib
import logging
import os
import pydantic.typing
import re
import traceback
from typing import (TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type,
                    Union)

import yaml
from future.utils import raise_with_traceback

import experiment.model.errors
import experiment.model.frontends.dosini
import experiment.model.frontends.flowir

if TYPE_CHECKING:
    from experiment.model.frontends.flowir import (FlowIRConcrete, DictFlowIRComponent, DictFlowIR, DictManyDocuments,
                                                   DictManifest, Manifest)


def ConfigurationToDict(cfg):

    '''Converts the data in a ConfigParser instance to a python dict'''

    config = {}

    for section in cfg.sections():
        config[section] = {}
        for name, value in cfg.items(section):
            config[section][name] = value
            if len(config[section][name]) == 1:
                config[section][name] = config[section][name][0]
            elif len(config[section][name]) == 0:
                config[section][name] = ''

    return config


def ConfigurationFileToJson(filename):

    '''Reads dosini format configuration file and returns it as json string'''

    cfg = configparser.ConfigParser()
    cfg.read([filename])
    return ConfigurationToJson(cfg)


def ConfigurationToJson(cfg, pretty=True):

    '''Converts the data in a ConfigParser instance to json string

    Parameters:
        cfg: A ConfigParser.ConfigParser instance
        pretty: If True (Default) json string is prettified. Otherwise default rep is used.

    Returns:
        A json formatted string'''

    import json

    config = ConfigurationToDict(cfg)

    if pretty:
        jsonString = json.dumps(config, sort_keys=True,
                         indent=4, separators=(',', ': '))

    else:
        jsonString = json.dumps(config)

    return jsonString


def SplitComponentName(componentName):

    '''Given the name of a component returns the basename and replica index

    Parameters:
        componentName: The name of a replicated experiment component

    Returns:
        A tuple with two elements.

    Raises ValueError if no replica index can be determined'''
    return experiment.model.frontends.flowir.FlowIR.SplitReplicatedComponentName(componentName)

def ParseDataReference(value):
    # type: (str) -> Tuple[str, Optional[str], str]
    '''Breaks the data reference into parts

    Parameters:
        value - A data reference.
        
    Returns:
        A tuple (producerReference, file, method)
        File may be None
        If method is not given it defaults to ref

    Exceptions:
        Raises ValueError if reference string contains no method, and invalid method, or has two many ':'
     '''

    return experiment.model.frontends.flowir.FlowIR.ParseDataReference(value)


def ParseProducerReference(reference, index=None):
    """Parses a producer reference string returning the producer name and the stage Index.

    A producer reference string has form ($stageName).$producerName
    $stageName is optional.

    Parameters:
        reference - The reference string
        index - (Optional) The index of the stage the reference is relative to if it is not
            specifed in the reference
    Returns:
        A tuple (stageIndex, producerName, hasIndex)
        If reference contains no stageIndex and job is not provided stageIndex==None
        hasIndex is True if reference contained a stageIndex false otherwise

    Note: In the case where the producer is a directory the stageIndex has no meaning

    Errors:
        Raises a ValueError if $stageName has incorrect format. Must be stage$i.
        Raises a ValueError if the producer reference contains more than one '.
    """
    return experiment.model.frontends.flowir.FlowIR.ParseProducerReference(reference, index)


def CreateDataReference(producer, filename, method):

    '''Returns a data reference string given components.

    Note: This function does not create a producer reference string.
    This must have been created previously.

    Parameters:
        producer: The producer reference
        filename: The file under the producer if any
        method: The reference method/type'''
    return experiment.model.frontends.flowir.FlowIR.compile_reference(producer, filename, method)


def suggest_alternative(value, valid_values, option_name, componentName, defaults, out_errors, cutoff=0.8):
    # type: (str, List[str], str, str, Dict[str, str], List[str], float) -> None
    # VV: Validates an option against a list of valid_values
    possibilities = difflib.get_close_matches(value, valid_values, cutoff=cutoff)
    if len(possibilities):
        if option_name not in defaults:
            out_errors.append('%s: unknown %s %s. Did you mean: %s?' % (
                componentName, option_name, value, possibilities[0]
            ))
        else:
            out_errors.append(
                '%s: unknown %s %s (inherited from defaults). Did you mean: %s?' % (
                    componentName, option_name, value, possibilities[0]
                ))
    else:
        out_errors.append('%s: unknown %s %s. No close match found' % (
            componentName, option_name, value
        ))


class FlowIRExperimentConfiguration:
    # VV: Making this a Class object enables us to load multiple packages and keep the record of the reported warnings
    _suppressed_warnings = set()
    _NoFlowIR = object()

    def suppressed_warning(self, warning_message):
        # type: (str) -> None
        if warning_message not in self._suppressed_warnings:
            self._suppressed_warnings.add(warning_message)
            if len(self._suppressed_warnings) > 100:
                self._suppressed_warnings.clear()

            self.log.log(15, '%s. Future identical warnings will be suppressed' % warning_message)

    def __init__(
        self,
        path: Optional[str],
        platform: Optional[str],
        variable_files: Optional[List[str]],
        system_vars: Optional[Dict[str, str]],
        is_instance: bool,
        createInstanceFiles: bool,
        primitive: bool,
        concrete: "Optional[FlowIRConcrete]" = None,
        updateInstanceFiles: bool = True,
        variable_substitute: bool = True,
        manifest: "Optional[DictManifest]" = None,
        validate: bool = True,
        config_patches: Optional[Dict[int, List[str]]] = None,  # VV: deprecate config_patches
        expand_references: bool = True,
        file_format: Optional[pydantic.typing.Literal["flowir", "dsl"]] = None,
        **kwargs
    ):
        """Initializes and then optionally validates a package/Instance configuration.

        Steps:
        0? Load FlowIR from file (only when `concrete` is None)
        1. Parse user supplied variable files and patch the description
        2. expand references to absolute string representation
        3. Store instance description + manifest
        4. Replicate
        5. Validate

        Args:
            platform:
                Name of platform (defaults to `default` which is mapped to `experiment.conf`)
            path:
                Path to workflow definition. Can be a directory, a file, or None if workflow definition
                is stored entirely in memory
            variable_files:
                Instance specific variable file(s). If multiple files are provided then they are layered
                starting from the first and working towards the last. This means that the value of a variable that
                 exists in multiple layers will be the one that the last layer defines.
            system_vars:
                A dictionary of environment variables.
                These variables are added to every environment - even environments with no vars/name
                The purpose of system vars is for the experiment controller to be able to set
                non-application specific variables
            config_patches:
                A dictionary whose keys are stage indexes and whose values are a list
                of configuration files. These configuration files will be layered on-top of the
                default configuration file for stage index. For example, allowing addition of new
                components, or dynamic option reconfig.
            is_instance:
                Indicates whether to load the Instance flavour of the Experiment instead of the Package one
            createInstanceFiles:
                If set to True will auto-generate instance files provided that they do not
                already exist. Set updateInstanceFiles to True too to update existing files.
            concrete:
                The FlowIRConcrete object which holds the FlowIR configuration, if it's none then
                 the class will load the configuration from conf/flowir_package.yaml for Packages and
                 conf/flowir_instance.yaml for Instances
            primitive:
                If true will not perform replication, otherwise will replicate components following the
                workflowAttributes.replicate rules
            updateInstanceFiles:
                Set to true to update instance files if they already exist
            variable_substitute:
                Whether to perform variable substitution, optional for a primitive graph
                but required for a replicated one
            manifest:
                The manifest is a dictionary, with targetFolder: sourceFolder entries. Each
                sourceFolder will be copied or linked to populate the respective targetFolder. Source folders can be
                absolute paths, or paths relative to the path of the FlowIR YAML file. SourceFolders may also be
                suffixed with :copy or :link to control whether targetFolder will be copied or linked to sourceFolder
                (default is copy). TargetFolders are interpreted as relative paths under the instance directory. They
                may also be the resulting folder name of some applicationDependency. They may include a path-separator
                (.e.g /) but must not be absolute paths.
            validate:
                When True this method will raise exception if configuration is invalid or missing
                expand_references: Whether to expand component references to their absolute string representation
            file_format:
                The file format of a package which consists of a single file. Defaults to "flowir"
        Raises:
            experiment.errors.ExperimentInvalidConfigurationError:  If the configuration fails the validation checks
            experiments.errors.ExperimentMissingConfigurationError: If configuration does not exist and validate is True
        """
        self.log = logging.getLogger('FlowIRConf')

        if platform is None:
            platform = experiment.model.frontends.flowir.FlowIR.LabelDefault

        system_vars = system_vars or {}
        config_patches = config_patches or {}
        variable_files = list(set(variable_files or []))

        out_errors = []
        self.file_format = file_format
        self._is_instance = is_instance
        self._manifest = FlowIRExperimentConfiguration._NoFlowIR  # type: Union[object, Manifest]

        try:
            self._manifest = experiment.model.frontends.flowir.Manifest(manifest)
        except Exception as e:
            out_errors.append(e)

        # VV: Path to conf/flowir_package.yaml file *only* set for packages in the form of a directory. Exclusively
        # used to generate a `flowir_package.yaml` file for Workflow Packages that are defined in a schema other than
        # FlowIR (e.g. DOSINI)
        self._path_package_yaml = None

        if path is None:
            # VV: This configuration is stored in Memory
            self._location = "in-memory://"
            self._conf_dir = self._location
            self._dsl_file_path = self._location
            self._is_package = None
        elif os.path.isfile(path):
            self._location = os.path.dirname(os.path.abspath(path))
            self._conf_dir = self._location
            self._dsl_file_path = path
            self._is_package = False
        else:
            # NOTE: This is for packages which derive from an existing directory
            self._location = path
            self._conf_dir = os.path.join(self._location, 'conf')
            self._path_package_yaml = os.path.join(self._conf_dir, 'flowir_package.yaml')
            if is_instance is False:
                self._dsl_file_path = self._path_package_yaml
            else:
                self._dsl_file_path = os.path.join(self._conf_dir, 'flowir_instance.yaml')
            self._is_package = True

        self._create_instance_files = createInstanceFiles
        self._platform = platform
        self._config_patches = config_patches.copy()
        self._update_instance_files = updateInstanceFiles
        self._system_vars = system_vars.copy()
        self._variable_files = variable_files
        self._user_variables = {}
        self._is_primitive = primitive
        self._variable_substitute = variable_substitute
        self._expand_references = expand_references

        # VV: Maintain a copy of the original FlowIR so that we can parametrize the configuration later
        self._original_flowir_0 = FlowIRExperimentConfiguration._NoFlowIR  # type: Union[object, DictFlowIR]
        self._unreplicated = FlowIRExperimentConfiguration._NoFlowIR  # type: Union[object, FlowIRConcrete]

        # VV: Collections of documents: outer key is the type of the collection (e.g. DoWhile). Nested to outer key
        #     is the id of the document. The id then points to the contents of the document
        self._documents = {}  # type: DictManyDocuments
        # self.log.info('Load FlowIR, the default platform is %s' % platform)

        if validate:
            if path is None and concrete is None:
                if concrete is None and validate:
                    raise experiment.model.errors.ExperimentMissingConfigurationError(
                        f"Missing configuration at {self._location}")
            elif path is not None and os.path.exists(path) is False:
                raise experiment.model.errors.ExperimentMissingConfigurationError(f"Missing configuration at {path}")

        # VV: We're optionally validating the Workflow definition and the Manifest, just record any unexpected problems
        # and then report them at the end
        try:
            if concrete is None:
                self.log.log(19, "No FlowIRConcrete object - will now attempt to load from %s" % path)

                if self.isExperimentPackageDirectory:
                    path = self._path_to_main_file(path, is_instance)
                else:
                    path = self._dsl_file_path
                # VV: This method also populates self._original_flowir_0
                concrete = self._load_concrete(path, self._platform, out_errors)
            else:
                self._original_flowir_0 = concrete.raw()

            # VV: Store the package *before* polluting FlowIR with any user-variables
            if self._path_package_yaml and os.path.exists(self._path_package_yaml) is False and updateInstanceFiles:
                flowir_primitive = concrete.raw()
                # experiment.model.frontends.flowir.FlowIR.compress_flowir(flowir_primitive)
                pretty_primitive = experiment.model.frontends.flowir.FlowIR.pretty_flowir_sort(flowir_primitive)
                with open(self._path_package_yaml, 'w') as f:
                    experiment.model.frontends.flowir.yaml_dump(pretty_primitive, f, sort_keys=False, default_flow_style=False)

            self._concrete: experiment.model.frontends.flowir.FlowIRConcrete = (
                    concrete or FlowIRExperimentConfiguration._NoFlowIR
            )
            self._initialize(out_errors)
        except (experiment.model.errors.ExperimentMissingConfigurationError,
                experiment.model.errors.ExperimentInvalidConfigurationError) as e:
            if validate:
                raise_with_traceback(e)
            else:
                out_errors.append(e)
        except Exception as e:
            self.log.debug(f"Unexpected error while initializing {e} -- traceback:\n{traceback.format_exc()}")
            out_errors.append(e)

        self._try_report_errors(path, validate, out_errors)

    def get_platform_name(self) -> str:
        return self._concrete.active_platform

    def get_unreplicated_flowir(self, return_copy=True) -> experiment.model.frontends.flowir.FlowIRConcrete | None:
        """Returns the unreplicated version of FlowIR (if it exists)

        Args:
            return_copy: When True returns the copy of the unreplicated FlowIR. Default is True

        Returns:
            An instance of experiment.model.frontends.flowir.FlowIRConcrete. If there is no unreplicated version it
                returns None
        """

        if self._unreplicated ==  FlowIRExperimentConfiguration._NoFlowIR:
            return None

        if return_copy:
            return self._unreplicated.copy()
        return self._unreplicated

    def parametrize(
        self,  # type: FlowIRExperimentConfiguration
        platform: Optional[str],
        variable_files: Optional[List[str]],
        systemvars: Optional[Dict[str, str]],
        is_instance: bool,
        createInstanceFiles: bool,
        primitive: bool,
        updateInstanceFiles: bool = True,
        variable_substitute: bool = True,
        manifest: "Optional[DictManifest]" = None,
        validate: bool = True,
        expand_references: bool = True,
        config_patches: Optional[Dict[int, List[str]]] = None,  # deprecate this
    ) -> "FlowIRExperimentConfiguration":
        """Parametrizes and optionally validates the configuration

        Steps:
        0. Use self._original_flowir_0, and self._documents as a basis for the FlowIR and apply manifest to it
        1. Parse user supplied variable files and patch the description
        2. expand references
        3. Store instance description + manifest
        4. Replicate
        5. Validate

        Args:
            platform: Name of platform (defaults to `default` which is mapped to `experiment.conf`)
            variable_files: Instance specific variable file(s). If multiple files are provided then they are layered
                starting from the first and working towards the last. This means that the value of a variable that
                exists in multiple layers will be the one that the last layer defines.
            systemvars: A dictionary of environment variables.
                These variables are added to every environment - even environments with no vars/name
                The purpose of system vars is for the experiment controller to be able to set
                non-application specific variables
            is_instance: Indicates whether to load configuration files from an existing instance directory.
            createInstanceFiles: If set to True will auto-generate instance files provided that they do not
                already exist. Set updateInstanceFiles to True too to update existing files.
            primitive: If true will not perform replication, otherwise will replicate components following the
                workflowAttributes.replicate rules
            updateInstanceFiles: Set to true to update instance files if they already exist
            variable_substitute: Whether to perform variable substitution, optional for a primitive graph
                but required for a replicated one
            manifest: The manifest is a dictionary, with targetFolder: sourceFolder entries. Each
                sourceFolder will be copied or linked to populate the respective targetFolder. Source folders can be
                absolute paths, or paths relative to the path of the FlowIR YAML file. SourceFolders may also be
                suffixed with :copy or :link to control whether targetFolder will be copied or linked to sourceFolder
                (default is copy). TargetFolders are interpreted as relative paths under the instance directory. They
                may also be the resulting folder name of some applicationDependency. They may include a path-separator
                (.e.g /) but must not be absolute paths.
            validate: Whether to perform validation checks on Load
            expand_references: Whether to expand component references to their absolute string representation
            config_patches: A dictionary whose keys are stage indexes and whose values are a list
                of configuration files. These configuration files will be layered on-top of the
                default configuration file for stage index. For example, allowing addition of new
                components, or dynamic option reconfig (deprecated).

        Returns:
            FlowIRExperimentConfiguration: Self - this method makes IN-place parametrization

        Raises:
            experiment.errors.ExperimentInvalidConfigurationError:  If the configuration fails the validation checks
            experiments.errors.ExperimentMissingConfigurationError: If configuration does not exist and validate is True
        """
        if platform is None:
            platform = experiment.model.frontends.flowir.FlowIR.LabelDefault

        self.log.info(f"Parametrizing FlowIR for platform {platform}, primitive={primitive}, "
                      f"variable_files={variable_files}, variable_substitute={variable_substitute}")

        if validate and self._original_flowir_0 == FlowIRExperimentConfiguration._NoFlowIR:
            experiment.model.errors.ExperimentMissingConfigurationError(f"Missing configuration")

        systemvars = systemvars or {}
        config_patches = config_patches or {}
        variable_files = list(set(variable_files or []))

        out_errors = []

        # VV: _load_concrete, and _initialize use these ivars; we can use these 2 methods in __init__() too
        self._is_instance = is_instance
        self._create_instance_files = createInstanceFiles
        self._platform = platform
        self._config_patches = config_patches.copy()
        self._update_instance_files = updateInstanceFiles
        self._system_vars = systemvars.copy()
        self._variable_files = variable_files
        self._is_primitive = primitive
        self._variable_substitute = variable_substitute
        self._expand_references = expand_references

        self._manifest = FlowIRExperimentConfiguration._NoFlowIR
        try:
            self._manifest = experiment.model.frontends.flowir.Manifest(manifest)
        except Exception as e:
            out_errors.append(e)

        self._concrete = self._load_concrete(None, platform, out_errors)
        self._initialize(out_errors)

        self._try_report_errors(self.location, validate, out_errors)

        return self

    def get_global_variables(self, include_user_variables=True) -> Dict[str, str]:
        """Returns global, platform, and (optionally) user variables (layered global -> platform -> user with
        user having the highest priority)

        Args:
            include_user_variables: Whether to layer user variables on top of global/platform variables

        Returns:
            Dictionary with name: value pairs of variables
        """
        if include_user_variables:
            user_vars = self.get_user_variables().get(experiment.model.frontends.flowir.FlowIR.LabelGlobal, {})
        else:
            user_vars = {}

        global_vars = self._concrete.get_default_global_variables()
        platform_vars = self._concrete.get_platform_global_variables()

        variables = global_vars
        experiment.model.frontends.flowir.FlowIR.override_object(variables, platform_vars)
        experiment.model.frontends.flowir.FlowIR.override_object(variables, user_vars)

        return variables

    def get_user_variables(self, return_copy=True):
        """Returns a read only view of user defined variables

        The format of the returned user variables is ::

            {
                'global': {
                    <name: str>: <value: int/float/bool/str>
                },
                'stages': {
                    <stage index: int> : {
                        <name: str>: <value: int/float/bool/str>
                    },
                }
            }

        Args:
            return_copy: Whether to return a deep copy of the read-only view, or the read-only view as is
        Returns:
            A "variables" dictionary with.
        """
        if return_copy:
            return copy.deepcopy(self._user_variables)
        else:
            return self._user_variables

    def _initialize(self, out_errors: List[Exception]):
        try:
            if (self._concrete != FlowIRExperimentConfiguration._NoFlowIR) or (len(out_errors) == 0):
                self._user_variables = self.layer_many_variable_files(self._variable_files)
                self._patch_in_variable_files(self._variable_files, self._concrete, out_errors)
                self._unreplicated = self._concrete.copy()

                self._generate_instance_files(self._create_instance_files, self._update_instance_files, out_errors)

                if self._is_primitive is False:
                    self.replicate()

                if self._expand_references:
                    try:
                        app_deps = self.get_application_dependencies()
                    except Exception as e:
                        exc = experiment.model.errors.EnhancedException(
                            "Unable to get application dependencies due to %s" % e, e)
                        out_errors.append(exc)
                    else:
                        # VV: Get a direct reference to the components in FlowIR to modify them in place
                        components = self._concrete.get_components(return_copy=False)
                        for comp in components:
                            comp_ref = '*missing name*'
                            try:
                                comp_stage = comp.get('stage', 0)
                                comp_id = (comp_stage, comp['name'])
                                comp_ref = 'stage%s.%s' % (comp_stage, comp['name'])

                                # VV: If a component has references, try to expand them and if the resulting
                                # references differ those that component originally had update the component dictionary
                                if 'references' in comp:
                                    old_refs = comp['references']
                                    new_refs = experiment.model.frontends.flowir.FlowIR.expand_component_references(
                                        old_refs, comp_stage, None, app_deps, self.top_level_folders)

                                    if old_refs != new_refs:
                                        # VV: Invalidate FlowIR cache so that lookup returns the new references
                                        comp['references'] = new_refs
                                        self._concrete.invalidate_cache_for_component(comp_id)
                            except experiment.model.errors.FlowException as e:
                                out_errors.append(e)
                            except Exception as e:
                                # VV: If a component contains invalid references causing an error make a note of it
                                # and try to expand the references of the remaining components
                                exc = experiment.model.errors.EnhancedException(
                                    "Unable to expand references of component %s, due to %s" % (comp_ref, e), e)
                                out_errors.append(exc)

            self.validate(out_errors)
        except experiment.model.errors.FlowException as e:
            out_errors.append(e)
        except Exception as e:
            self.log.debug(f"Unexpected error while initializing {e} -- traceback:\n{traceback.format_exc()}")
            out_errors.append(experiment.model.errors.EnhancedException(f'Unable to initialize because {e}', e))

    def _generate_instance_files(self, create_instance_files: bool,
                                 update_instance_files: bool, out_errors: List[Exception]):
        instance_file = os.path.join(self._conf_dir, 'flowir_instance.yaml')
        manifest_file = os.path.join(self._conf_dir, 'manifest.yaml')

        exists_flowir = os.path.exists(instance_file)
        exists_manifest = os.path.exists(manifest_file)

        if create_instance_files and (exists_flowir is False or update_instance_files is True):
            try:
                self.store_unreplicated_flowir_to_disk()
            except Exception as e:
                out_errors.append(e)

        if create_instance_files and (exists_manifest is False or update_instance_files is True):
            try:
                with open(manifest_file, 'w') as f:
                    experiment.model.frontends.flowir.yaml_dump(self.manifestData, f)
            except Exception as e:
                out_errors.append(e)

    @property
    def manifestData(self) -> Dict[str, str]:
        return self._manifest.manifestData

    @property
    def update_instance_files(self):
        return self._update_instance_files

    @property
    def system_vars(self):
        return self._system_vars

    @property
    def config_patches(self):
        return self._config_patches

    @property
    def is_instance(self):
        return self._is_instance

    @property
    def top_level_folders(self):
        # type: () -> List[str]
        """A list of top level folders in the package/instance directory"""

        return self._manifest.top_level_folders

    @property
    def is_raw(self):
        """A Raw configuration is one which doesn't substitute variables.

        Returns True when variables are not substituted, False when variables are substituted
        """
        return not self._variable_substitute

    def store_unreplicated_flowir_to_disk(self):
        """Stores the FlowIR definition of the experiment under conf/flowir_instance.yaml

        This is version of FlowIR without any component replication
        """
        instance_file = os.path.join(self._conf_dir, 'flowir_instance.yaml')
        with open(instance_file, 'w') as f:
            primitive = self._unreplicated.instance(ignore_errors=True, inject_missing_fields=False,
                                                    fill_in_all=False, is_primitive=True)
            # primitive = experiment.model.frontends.flowir.FlowIR.compress_flowir(primitive)
            pretty_primitive = experiment.model.frontends.flowir.FlowIR.pretty_flowir_sort(primitive)
            experiment.model.frontends.flowir.yaml_dump(
                pretty_primitive, f, sort_keys=False, default_flow_style=False
            )

    @property
    def configurationDirectory(self):
        return self._conf_dir

    @property
    def platform_name(self):
        return self._platform

    @property
    def location(self):
        if self._is_package:
            return self._location
        else:
            return self._dsl_file_path

    def flowir_path(self):
        return self._dsl_file_path

    def get_application_dependencies(self):
        return self._concrete.get_application_dependencies()

    def get_key_outputs(self) -> experiment.model.frontends.flowir.DictFlowIROutput:
        """Returns a dictionary with the specification of the key-outputs

        The format of the dictionary is:

        <keyOutputName>:
            data-in: a reference to a file that a component produces (reference method :ref/:copy)
            description (optional): human readable text
            stages (optional but required data-in does not contain a "stage%d." prefix):
            - stage0
            - 1

        Notice that:

        1. IF data-in contains a "stage%d." prefix then then "stages" MUST NOT be set
        2. IF data-in does not contain a "stage%d." prefix, then "stages" MUST be set

        Returns:
            A dictionary of dictionaries
        """
        return self._concrete.get_output()

    @classmethod
    def _fetch_user_variables(cls, path, out_errors):
        # type: (str, List[Exception]) -> Dict[str, Dict[str, str]]
        """Returns variables from a file grouped into sections.

        There can be a `global` section, and multiple 'stage' sections that are grouped under
        {
           'global': {
             <str: name>: <int/float/bool/str: value>
           },
          'stages': {
            <int: stage index> : {
              <str: name>: <int/float/bool/str: value>
            },
          }
        }
        """
        user_variables = {}
        try:
            with open(path, 'r') as f:
                user_variables = experiment.model.frontends.flowir.yaml_load(f)
        except Exception as e:
            out_errors.append(e)

        return user_variables

    @classmethod
    def layer_many_variable_files(cls, variable_files: List[str]):
        """Generates the contents of a single Variables dictionary after layering the many variable dictionaries

        Args:
            variable_files: Instance specific variable file(s). If multiple files are provided then they are layered
                starting from the first and working towards the last. This means that the value of a variable that
                exists in multiple layers will be the one that the last layer defines.

        Notes:
          The schema of a variable dictionary is
            {
                'global': {
                    <str: name>: <int/float/bool/str: value>
                },
                'stages': {
                    <int: stage index> : {
                        <str: name>: <int/float/bool/str: value>
                    },
                }
            }

        Raises:
            experiment.errors.FlowIRConfigurationErrors: if there're problems while loading the variable files

        Returns:
            A Dictionary containing the layered variables
        """
        agg_user_variables = dict()
        user_vars_errors = []

        for path in variable_files:
            try:
                user_vars = cls.read_user_variables(path, user_vars_errors, True)
            except (NotImplementedError, experiment.model.errors.FlowIRConfigurationErrors) as e:
                user_vars_errors.append(e)
            else:
                try:
                    experiment.model.frontends.flowir.FlowIR.override_object(agg_user_variables, user_vars)
                except Exception as e:
                    user_vars_errors.append(experiment.model.errors.FlowIRConfigurationErrors(
                        [experiment.model.errors.EnhancedException(
                            f'Could not merge aggregate variables with user variables at {path}'
                            f' because of {e}', e)], path))

        if user_vars_errors:
            raise experiment.model.errors.FlowIRConfigurationErrors(user_vars_errors)

        return agg_user_variables

    @classmethod
    def read_user_variables(cls, path: str, out_errors: List[Exception], validate: bool = True) \
            -> Dict[str, Union[Dict[int, Dict[str, str]], Dict[str, str]]]:
        """Returns variables from a file grouped into sections.

        There can be a `global` section, and multiple 'stage' sections that are grouped under
        {
           'global': {
             <str: name>: <int/float/bool/str: value>
           },
          'stages': {
            <int: stage index> : {
              <str: name>: <int/float/bool/str: value>
            },
          }
        }
        """
        _, ext = os.path.splitext(path)

        if ext == '.conf':
            user_variables = DOSINIExperimentConfiguration._fetch_user_variables(path, out_errors)
        elif ext in ['.yaml', '.yml']:
            user_variables = FlowIRExperimentConfiguration._fetch_user_variables(path, out_errors)
        else:
            raise NotImplementedError('Unknown file type for user variables "%s"' % path)

        cls._validate_user_variables(path, user_variables, validate)

        return user_variables

    @classmethod
    def _validate_user_variables(cls, path: str,
                                 user_variables: Dict[str, Union[Dict[int, Dict[str, str]], Dict[str, str]]],
                                 validate=True) -> Optional[List[Exception]]:
        """Validates user variables and logs errors to stderr

        Args:
            path: path to file containing variables (for including in error messages)
            user_variables: user variables
            validate: when set to True will raise an experiment.errors.FlowIRConfigurationErrors instead of returning
                the list of errors

        Raises:
            experiment.errors.FlowIRConfigurationErrors: when validate is set to True and user variables are invalid

        Returns:
            List[Exception]: A list of all errors
        """
        # VV: Validate user-variables after reading them
        try:
            FlowIR = experiment.model.frontends.flowir.FlowIR
            ValidateOptional = experiment.model.frontends.flowir.ValidateOptional
            PrimitiveTypes = experiment.model.frontends.flowir.PrimitiveTypes
            string_types = experiment.model.frontends.flowir.string_types
            validate_object_schema = experiment.model.frontends.flowir.validate_object_schema

            variable_collection = {ValidateOptional(string_types): PrimitiveTypes}
            variables_schema = {
                ValidateOptional(FlowIR.LabelGlobal): variable_collection,
                ValidateOptional(FlowIR.LabelStages): {ValidateOptional(int): variable_collection}
            }

            errors = validate_object_schema(user_variables, variables_schema, 'File[%s]' % path)
        except Exception as e:
            errors = [experiment.model.errors.EnhancedException(
                f"Unable to validate user variables -- underlying error {e}", e)]

        if errors:
            log = logging.getLogger('UserVars')
            log.warning(f"Failed to validate user variables file {path}, "
                        f"user variables are {experiment.model.frontends.flowir.yaml_dump(user_variables)}"
                        f"{'- will ignore' if validate is False else ''}")
            log.warning("Error(s):")
            for i, e in enumerate(errors):
                log.warning(f'{i}) {str(e)}')
            if validate:
                raise experiment.model.errors.FlowIRConfigurationErrors(errors, path)
        return errors

    def _load_concrete(self, path: Optional[str], platform: str, out_errors: List[Exception]):
        """Generates an experiment.model.frontends.flowir.FlowIRConcrete instance

        Does not raise exceptions but inserts them to the @out_esrrors array

        Args:
            path: path to load the YAML, when None method uses the YAML stored in self._original_flowir_0 ivar
            platform: platform to use when loading flowIR defaults to 'default'
            out_errors:

        Returns:
            experiment.model.frontends.flowir.FlowIRConcrete object OR @FlowIRExperimentConfiguration._NoFlowIR
                if its unable to instantiate the object. The method can return an object *and* populate out_errors
                with exceptions.
        """
        concrete = FlowIRExperimentConfiguration._NoFlowIR
        try:
            if path is not None:
                flowir, self._documents = experiment.model.frontends.flowir.package_document_load(path, self._is_instance)
                self._original_flowir_0 = flowir
            else:
                flowir = self._original_flowir_0

            if platform is None:
                platform = experiment.model.frontends.flowir.FlowIR.LabelDefault

            concrete = experiment.model.frontends.flowir.FlowIRConcrete(flowir, platform, self._documents)
        except Exception as e:
            self.log.log(15, f"Unable to load FlowIRConcrete() due to {type(e)}:{e} "
                             f"traceback is {traceback.format_exc()} -- will report error")
            out_errors.append(e)
        return concrete

    def _try_report_errors(self, path: str, validate: bool,  out_errors: List[Exception]):
        if not out_errors:
            return

        self.log.warning(f'{len(out_errors)} errors when loading configuration'
                         f'{" - ignore issues that make platform un-runable" if validate is False else ""}')

        for err in out_errors:
            self.log.log(18, err)

        if validate:
            raise experiment.model.errors.ExperimentInvalidConfigurationError(
                'Errors when loading configuration',
                experiment.model.errors.FlowIRConfigurationErrors(out_errors, path))

        def is_important(exc: Exception) -> bool:
            return (
                isinstance(exc, experiment.model.errors.ExperimentMissingConfigurationError) or
                isinstance(exc, experiment.model.errors.ExperimentInvalidConfigurationError)
            )

        # VV: Some errors are too important not to raise even if we do not plan on validating whether the platform
        # we meant to load is runable. An example "important" error is a malformed YAML, or a missing configuration
        important_errors = [ x for x in out_errors if is_important(x)]

        if important_errors:
            raise experiment.model.errors.ExperimentInvalidConfigurationError(
                'Errors when loading configuration',
                experiment.model.errors.FlowIRConfigurationErrors(important_errors, path))

    @classmethod
    def _patch_in_variable_files(cls, variable_files, concrete, out_errors):
        # type: (List[str], FlowIRConcrete, List[Exception]) -> None
        # VV: This method, or parts of it, should be moved to experiment.model.frontends.flowir.FlowIRConcrete
        try:
            user_variables = cls.layer_many_variable_files(variable_files)

            num_stages = concrete.get_stage_number()
            global_variables = user_variables.get(experiment.model.frontends.flowir.FlowIR.LabelGlobal, {})
            stage_variables = user_variables.get(experiment.model.frontends.flowir.FlowIR.LabelStages, {})

            for plat in concrete.platforms:
                for stage_index in range(num_stages):
                    inject_variables_to_stage: Dict[str, str] = copy.deepcopy(global_variables)
                    inject_variables_to_stage.update(stage_variables.get(stage_index, {}))

                    for name in inject_variables_to_stage:
                        concrete.set_platform_stage_variable(
                            stage_index, name, inject_variables_to_stage[name], platform=plat)
        except Exception as e:
            out_errors.append(e)

    @classmethod
    def _path_to_main_file(cls, root_directory, is_instance):
        """Returns path to main configuration file in workflow package/instance root directory

        Args:
            root_directory: Path to root directory of workflow package/instance (contains `conf`, `data`, etc)
            is_instance: Look for instance vs package configuration
        """

        exp_file = {
            True: 'flowir_instance.yaml',
            False: 'flowir_package.yaml'
        }[is_instance]

        return os.path.join(root_directory, 'conf', exp_file)

    @classmethod
    def format_found_in_directory(cls, root_directory, is_instance, **kwargs):
        # type: (str, bool, Dict[str, Any]) -> bool
        """Returns whether format exists in the configuration files of a package/instance

        Args:
            root_directory: Path to root directory of workflow package/instance (contains `conf`, `data`, etc)
            is_instance: Look for instance vs package configuration
        """
        return os.path.isfile(cls._path_to_main_file(root_directory, is_instance))

    def replicate(self):
        # VV: Do not check for errors because components could be using `replica` to access arrays
        try:
            replicated = self._unreplicated.replicate(ignore_errors=True, platform=self._platform,
                                                      top_level_folders=self.top_level_folders)
        except Exception as e:
            self.log.critical("Failed to replicate, known components:")
            for cid in self._concrete.get_component_identifiers(True):
                self.log.info("  %s" % str(cid))
            raise_with_traceback(e)

        self._concrete = experiment.model.frontends.flowir.FlowIRConcrete(replicated, self._platform, self._documents)

    def validate(self, out_errors):
        # type: (List["experiment.errors.FlowIRException"]) -> None
        """Validates FlowIR and populates out_error with exceptions describing the issues with the workflow FlowIR"""
        # VV: Validate after replicating because of `replica` variables
        try:
            if (self._concrete != FlowIRExperimentConfiguration._NoFlowIR) or (len(out_errors) == 0):
                out_errors.extend(self._concrete.validate(top_level_folders=self.top_level_folders))
        except Exception as e:
            self.log.debug(f"Unexpected error while validating {e} -- traceback:\n{traceback.format_exc()}")
            out_errors.append(e)

    def add_environment(self, name, environment, platform=None):
        # type: (str, Dict[str, str], Optional[str]) -> None
        self._concrete.add_environment(name, environment, platform)

    @property
    def constructedEnvironments(self):
        """Returns a list of the constructed environments
        """
        return list(self._concrete.get_environments().keys())

    @property
    def isExperimentPackageDirectory(self):
        # type: () -> bool
        """Returns whether this FlowIRExperimentConfiguration is for a directory package or not"""
        return self._is_package

    def get_flowir_concrete(self, return_copy=True):
        # type: (bool) -> FlowIRConcrete
        if return_copy:
            return self._concrete.copy()
        else:
            return self._concrete

    def has_platform(self, platform):
        # type: (str) -> bool
        return platform in self._concrete.platforms

    @property
    def numberStageConfigurations(self):
        return self._concrete.numberStageConfigurations

    def configurationForStage(self, stage_index, raw=None):
        # type: (int, bool) ->configparser.SafeConfigParser
        # VV: @tag:DeprecateThis
        if raw is None:
            raw = self.is_raw
        self.log.critical('DOSINI CODE WILL BE DEPRECATED (configurationsForStage)')
        flowir = self._concrete.instance(fill_in_all=True, is_primitive=self._is_primitive) \
            if raw is False else self._concrete.raw()

        return experiment.model.frontends.dosini.Dosini.configuration_for_stage(flowir, stage_index, is_instance=True)

    def configurationForNode(self, nodeName, raw=None, omitDefault=False,
                             is_primitive=False, inject_missing_fields=True):
        # type: (str, bool, bool, bool, bool) -> DictFlowIRComponent
        """Returns FlowIR description of component"""
        stage_index, comp_name, _ = ParseProducerReference(nodeName)
        comp_id = (stage_index, comp_name)
        if raw is None:
            raw = self.is_raw
        return self._concrete.get_component_configuration(
            comp_id, raw, include_default=not omitDefault, is_primitive=is_primitive,
            inject_missing_fields=inject_missing_fields
        )

    def getOptionForNode(
            self,  # type: FlowIRExperimentConfiguration
            nodeName,  # type: str
            key,  # type: str
            raw=None,  # type: bool
            include_default=True,  # type: bool
            is_primitive=None,   # type: Optional[bool]
    ):
        is_primitive = is_primitive if is_primitive is not None else self._is_primitive
        stage_index, comp_name, _ = ParseProducerReference(nodeName)
        comp_id = (stage_index, comp_name)
        if raw is None:
            raw = self.is_raw

        if '#' not in key:
            self._concrete.get_component_variable(comp_id, key)
        else:
            key = key[1:]
            comp_flowir = self._concrete.get_component_configuration(comp_id, raw=True, include_default=include_default)

            route = key.split('.')
            context = comp_flowir

            for point in route[:-1]:
                context = context[point]

            variable_name = route[-1]
            option = context[variable_name]
            if raw is False:
                option = experiment.model.frontends.flowir.FlowIR.fill_in(
                    option, context=comp_flowir.get('variables', {}), flowir=comp_flowir,
                    label='components.stage%s.%s.variables' % (
                        comp_flowir.get('stage', '*Unknown*'),
                        comp_flowir.get('name', '*Unknown*')
                    ), is_primitive=is_primitive
                )
            return option

    def get_interface(self) -> "Optional[experiment.model.frontends.flowir.DictFlowIRInterface]":
        """Returns a copy of the interface (see FlowIR.type_flowir_interface_structure for spec).

        The interface is optional, a None value indicates that the definition does not have an interface.

        Returns:
            A dictionary representation of the interface
              (See experiment.model.frontends.FlowIR.type_flowir_interface_structure() for spec).
              Or `None` if the definition does not have an interface.
        """
        return self._concrete.get_interface(return_copy=True)

    def setOptionForNode(self, nodeName, key, value):
        stage_index, comp_name, _ = ParseProducerReference(nodeName)
        comp_id = (stage_index, comp_name)

        self._concrete.set_component_option(comp_id, key, value)

    def removeOptionForNode(self, nodeName, key):
        stage_index, comp_name, _ = ParseProducerReference(nodeName)
        comp_id = (stage_index, comp_name)

        self._concrete.remove_component_option(comp_id, key)

    def dataReferencesForNode(self, nodeName, raw=None):
        # type: (str, bool) -> List[str]

        """Returns the raw data-reference strings for nodeName
        """
        stage_index, comp_name, _ = ParseProducerReference(nodeName)
        comp_id = (stage_index, comp_name)

        if raw is None:
            raw = self.is_raw

        comp = self._concrete.get_component_configuration(
            comp_id, include_default=True, is_primitive=self._is_primitive, raw=raw)

        return comp.get('references', [])

    def variablesForNode(self, nodeName):
        stage_index, comp_name, _ = ParseProducerReference(nodeName)
        comp_id = (stage_index, comp_name)

        return self._concrete.get_component_variable_references(comp_id)

    def environmentForNode(
        self,
        nodeName: str,
        expand : bool = True,
        include_default : bool = True,
        is_primitive: Optional[bool] = None,
        remove_defaults_key: bool = True,
    ) -> Optional[Dict[str, str]]:
        """Returns the environment for a node

        This method finds the name of the environment that this component uses and then invokes
        returns the output of environmentWithName(). Next it uses string-interpolation to replace references
        to %(component-variables)s with their values.

        Finally, if the component is an "interpreter" then this method ensures that the interpreter has the
        following env-vars (uses the active shell of the runtime as reference to resolve the environment variables):
        - PATH
        - PYTHONPATH
        - PYTHONHOME
        - LD_LIBRARY_PATH

        Args:
            nodeName: the absolute reference of the node (e.g. stage0.helloWorld)
            expand: If true, expand the values of the environment variables using other variables
                in the environment right before returning the dictionary and then use the env-vars of the active
                shell environment to potentially expand more variables. References to env-vars which are neither in the
                environment, nor the active shell environment are left as is.
            include_default: use default component variables to string-interpolate the contents of the environment
                variables
            is_primitive: whether the graph is pritimitive (i.e. no replicas) or not.
            remove_defaults_key: If True, remove the DEFAULTS special key from the environment after processing it

        Returns: A dictionary containing the environment variables
        """
        # VV: @tag:FlowIR:Component
        #     @tag:FlowIR:Environment

        is_primitive = is_primitive if is_primitive is not None else self._is_primitive

        stage_index, comp_name, _ = ParseProducerReference(nodeName)
        comp_id = (stage_index, comp_name)
        comp = self._concrete.get_component_configuration(comp_id, raw=self.is_raw, include_default=include_default)

        command = comp.get('command', {})
        environment_name = command.get('environment', None)
        context = copy.deepcopy(comp['variables'])

        environment_name = experiment.model.frontends.flowir.FlowIR.fill_in(
            environment_name, context, flowir={},
            label='components.stage%s.%s.command.environment' % (
                comp.get('stage', '*Unknown*'), comp.get('name', '*Unknown*')
            ), is_primitive=is_primitive
        )

        # VV: Fetch the environment and then fill it in using global default and platform variables
        env = self.environmentWithName(
            environment_name,
            expand=expand,
            remove_defaults_key=remove_defaults_key
        )

        global_variables = self._concrete.get_default_global_variables()
        platform_vars = self._concrete.get_platform_global_variables()

        context = {}
        context.update(global_variables)
        context.update(platform_vars)
        context.update(env)

        if not self.is_raw:
            env = experiment.model.frontends.flowir.FlowIR.fill_in(env, context, {}, is_primitive=is_primitive)

        # VV: If an interpreter is active copy PATH, PYTHONPATH, LD_LIBRARY_PATH, PYTHONHOME
        #     from the active shell environment if they are missing from the built environment
        if command.get('interpreter'):
            copy_from = ['PATH', 'PYTHONPATH', 'PYTHONHOME', 'LD_LIBRARY_PATH']
            active_shell = os.environ.copy()
            fake_environment = {
                key: active_shell[key] for key in copy_from if key in active_shell and key not in env
            }
            env.update(fake_environment)
            self.suppressed_warning('Generated an environment for interpreter %s = %s' % (
                nodeName, env
            ))

        return env

    def environmentWithName(
        self,
        environment_name: Optional[str],
        expand: bool = True,
        remove_defaults_key: bool = True,
    ) -> Dict[str, str]:
        """Build environment with a specific name.

        Notes:

            A) If the environment is empty (None or '') (i.e. no environment is selected) then the environment contains
            the default environment. The default environment is the environment called "environment". If that is unset
            then the default environment defaults to the environment variables in the active shell of the runtime.

            B) If the name is "none" then the environment contains {}

            In all cases we layer the environment on top of the system environment variables generated by the runtime.

            If an environment defines a `DEFAULTS` key then that key is expected to have the format `VAR1:VAR2:VAR3...`.
            Other options in the environment could reference the aforementioned vars using the $VAR and ${VAR} notation.
            These references will be expanded to the values of the respective environment variables in the active shell
            of the runtime.

            Any $VAR and ${VAR} references not matched by `DEFAULTS` keys will be resolved using the active shell
            (workflow launch environment).

            If a variable is defined in `DEFAULTS` but there is no value for it in the default environment then treat it
            as if it was never in the `DEFAULTS` option in the first place i.e. just leave it as is.

        Args:
            environment_name: The environment_name, special values are None, "none", and "experiment".
             The method converts the environment name to lowercase letters. Read the notes for more information.
            expand: If True, expand the values of the environment variables using other variables
                in the environment right before returning the dictionary and then use the env-vars of the active
                shell environment to potentially expand more variables. References to env-vars which are neither in the
                environment, nor the active shell environment are left as is.
            remove_defaults_key: If True, remove the DEFAULTS special key from the environment after processing it
        """
        default_env = self.defaultEnvironment()
        environment = (self._system_vars or {}).copy()

        # VV: The default environment is called "environment"
        if not environment_name:
            environment_name = 'environment'
        environment_name = environment_name.lower()

        # VV: A "none" environment is just the system variables
        if environment_name in ['', 'environment']:
            # VV: This is the default environment
            environment.update(default_env)
        elif environment_name == 'none':
            # VV: Special environment which doesn't contain any environment variables
            pass
        else:
            # VV: This is some named environment
            try:
                try:
                    flowir_env_vars = self._concrete.get_environment(environment_name)
                except experiment.model.errors.FlowIREnvironmentUnknown as e:
                    # VV: if this is not the default platform then attempt to find the environment
                    #     in the default platform too
                    default_platform = experiment.model.frontends.flowir.FlowIR.LabelDefault
                    if self._platform != default_platform:
                        flowir_env_vars = self._concrete.get_environment(
                            environment_name, platform=default_platform
                        )
                    else:
                        raise
            except experiment.model.errors.FlowIREnvironmentUnknown as e:
                def pretty_json(entry):
                    import json
                    return json.dumps(entry, sort_keys=True, indent=4, separators=(',', ': '))

                log = logging.getLogger('Environment')
                log.log(15, 'Could not find environment %s in %s' % (
                    e.name, pretty_json(e.flowir[experiment.model.frontends.flowir.FlowIR.FieldEnvironments])))
                raise
            environment = (self._system_vars or {}).copy()
            environment = environment.copy()

            environment.update(flowir_env_vars)

        # VV: First add any env-vars from DEFAULTS for which there're no keys already in the environment
        lbl_defaults = experiment.model.frontends.flowir.FlowIR.LabelEnvironmentDefaults

        if lbl_defaults in environment:
            default_env_vars = environment[lbl_defaults].split(':')
            default_environment = {}
            # VV: Expand env-vars in the environment which are also part of DEFAULTS
            #    e.g. PATH: my/custom/path:$PATH,
            # if the env-var is not in the environment already, then just add it
            # if expand is True the code will then auto-expand references to the DEFAULTS env-vars because
            # they'll already be inside the environment
            for def_env_var in default_env_vars:
                if def_env_var not in os.environ:
                    continue
                default_environment[def_env_var] = os.environ[def_env_var]

                if def_env_var not in environment:
                    environment[def_env_var] = default_environment[def_env_var]
                else:
                    environment[def_env_var] = experiment.model.frontends.flowir.expand_vars(
                        environment[def_env_var],
                        {def_env_var: os.environ[def_env_var]}
                    )

            if remove_defaults_key:
                environment.pop(lbl_defaults)

        # VV: Finally replace any references to env variables of the environment with their values and then
        #     use the active-shell environment to expand any remaining environment variables (this final step
        #     will expand variables such as $RANDOM).
        if expand:
            environment = {
                key: os.path.expandvars(experiment.model.frontends.flowir.expand_vars(
                    environment[key], environment)
                ) for key in environment if environment[key]
            }

        return environment

    def defaultEnvironment(self, fill_when_unset: bool = True) -> Dict[str, str]:
        try:
            config_env = self._concrete.get_environment('environment')
        except experiment.model.errors.FlowIREnvironmentUnknown:
            if fill_when_unset:
                log = logging.getLogger('Environment')
                log.warning('No default environment defined, will assume it contains the environment variables '
                            'in os.environ')
                config_env = copy.deepcopy(os.environ)
            else:
                raise

        return config_env


class DSLExperimentConfiguration(FlowIRExperimentConfiguration):
    @classmethod
    def format_found_in_directory(cls, root_directory, is_instance, **kwargs):
        # type: (str, bool, Dict[str, Any]) -> bool
        # VV: TODO implement loading instances
        if is_instance is True:
            return False

        path_to_main = cls._path_to_main_file(root_directory, is_instance, **kwargs)

        if os.path.isfile(path_to_main):
            with open(path_to_main, 'r') as f:
                try:
                    dictionary = yaml.safe_load(f)
                    return isinstance(dictionary, dict) and "entrypoint" in dictionary and "components" in dictionary
                except Exception:
                    return False

        return False


    @classmethod
    def _path_to_main_file(cls, root_directory, is_instance, **kwargs):
        if is_instance is True:
            return None
        # VV: Technically the instance file should contain the Namespace of the package file but with its
        # entrypoint updated with the global variables from the user variables file(s).
        main_file = kwargs.get('dsl_file', 'dsl.yaml')
        return os.path.join(root_directory, 'conf', main_file)

    def __init__(
            self,
            path: Optional[str],
            variable_files: Optional[List[str]],
            is_instance: bool,
            createInstanceFiles: bool,
            primitive: bool,
            updateInstanceFiles: bool = True,
            variable_substitute: bool = True,
            manifest: "Optional[DictManifest]" = None,
            validate: bool = True,
            expand_references: bool = True,
            **kwargs):
        """Initializes and then optionally validates a package configuration from a DSL 2.0 definition


        Args:
            path:
                Path to workflow definition. Can be a directory, a file, or None if workflow definition
                is stored entirely in memory
            variable_files:
                Instance specific variable file(s). If multiple files are provided then they are layered
                starting from the first and working towards the last. This means that the value of a variable that
                 exists in multiple layers will be the one that the last layer defines.
            is_instance:
                Indicates whether to load the Instance flavour of the Experiment instead of the Package one
            createInstanceFiles:
                If set to True will auto-generate instance files provided that they do not
                already exist. Set updateInstanceFiles to True too to update existing files.
            primitive:
                If true will not perform replication, otherwise will replicate components following the
                workflowAttributes.replicate rules
            updateInstanceFiles:
                Set to true to update instance files if they already exist
            variable_substitute:
                Whether to perform variable substitution, optional for a primitive graph
                but required for a replicated one
            manifest:
                The manifest is a dictionary, with targetFolder: sourceFolder entries. Each
                sourceFolder will be copied or linked to populate the respective targetFolder. Source folders can be
                absolute paths, or paths relative to the path of the FlowIR YAML file. SourceFolders may also be
                suffixed with :copy or :link to control whether targetFolder will be copied or linked to sourceFolder
                (default is copy). TargetFolders are interpreted as relative paths under the instance directory. They
                may also be the resulting folder name of some applicationDependency. They may include a path-separator
                (.e.g /) but must not be absolute paths.
            validate:
                When True this method will raise exception if configuration is invalid or missing
                expand_references: Whether to expand component references to their absolute string representation

        Raises:
            experiment.errors.ExperimentInvalidConfigurationError:  If the configuration fails the validation checks
            experiments.errors.ExperimentMissingConfigurationError: If configuration does not exist and validate is True
        """

        import experiment.model.frontends.dsl
        dsl_path = path
        if os.path.isdir(path):
            dsl_path = os.path.join(path, "conf", "dsl.yaml")

        with open(dsl_path, 'r') as f:
            dictionary = yaml.safe_load(f)

        self.dsl_namespace = experiment.model.frontends.dsl.Namespace(**dictionary)

        override_entrypoint_args = None

        if variable_files:
            # VV: If there're variable files then load them, and then use them to override the parameters
            # of the entry-instance step in the entrypoint
            # We need to do this step before we executing namespace_to_flowir() because that method replaces all
            # variable references with the values of said variables.
            variables = self.layer_many_variable_files(variable_files)

            # VV: There's no guarantee that this DSL is correct so be careful accessing the entrypoint
            try:
                initial_template = self.dsl_namespace.get_template(self.dsl_namespace.entrypoint.entryInstance)
            except Exception as e:
                if validate:
                    raise experiment.model.errors.DSLInvalidError([
                        experiment.model.errors.DSLInvalidFieldError(
                            location=["entrypoint", "entry-instance"],
                            underlying_error=e
                        )
                    ])
            else:
                if isinstance(variables.get("global"), dict):
                    # VV: Start with arguments in the entrypoint, then fill in those from the user variables
                    override_entrypoint_args = (self.dsl_namespace.entrypoint.execute[0].args or {}).copy()
                    override_entrypoint_args.update({name: value for name, value in variables["global"].items()})

        concrete = experiment.model.frontends.dsl.namespace_to_flowir(
            namespace=self.dsl_namespace,
            override_entrypoint_args=override_entrypoint_args
        )

        super().__init__(
            path=path,
            variable_files=variable_files,
            is_instance=is_instance,
            createInstanceFiles=createInstanceFiles,
            primitive=primitive,
            updateInstanceFiles=updateInstanceFiles,
            variable_substitute=variable_substitute,
            manifest=manifest,
            validate=validate,
            expand_references=expand_references,
            concrete=concrete,
            file_format="dsl",
            **kwargs
        )


class CWLExperimentConfiguration(FlowIRExperimentConfiguration):
    @classmethod
    def format_found_in_directory(cls, root_directory, is_instance, **kwargs):
        # type: (str, bool, Dict[str, Any]) -> bool
        if is_instance is True:
            return False
        return os.path.isfile(cls._path_to_main_file(root_directory, is_instance, **kwargs))

    @classmethod
    def _path_to_main_file(cls, root_directory, is_instance, **kwargs):
        if is_instance is True:
            return None

        main_file = kwargs.get('cwlFile', 'main.cwl')
        return os.path.join(root_directory, 'conf', 'cwl', main_file)

    def __init__(
            self,  # type: CWLExperimentConfiguration
            path,  # type: str
            platform,  # type: Optional[str]
            variable_files,  # type: Optional[List[str]]
            system_vars,  # type: Optional[Dict[str, str]]
            is_instance,  # type: bool
            createInstanceFiles,  # type: bool
            primitive=True,  # type: bool
            updateInstanceFiles=True,  # type: bool
            manifest=None,  # type: Dict[str, str]
            config_patches=None,  # type: Optional[Dict[int, List[str]]]
            **kwargs
    ):
        """Initializes and then validates a package/Instance configuration.

        Args:
            platform: Name of platform (defaults to `default` which is mapped to `experiment.conf`)
            path: Path to root directory of workflow package (contains `conf`, `data`, etc)
            variable_files: A list containing paths to variables.conf files which will be loaded right after
                the `variable.conf` files that are defined under `conf_dir/variables.conf`
            system_vars: A dictionary of environment variables.
                These variables are added to every environment - even environments with no vars/name
                The purpose of system vars is for the experiment controller to be able to set
                non-application specific variables
            config_patches: A dictionary whose keys are stage indexes and whose values are a list
                of configuration files. These configuration files will be layered on-top of the
                default configuration file for stage index. For example, allowing addition of new
                components, or dynamic option reconfig.
            is_instance: Indicates whether to load configuration files from an existing instance directory.
            manifest: The manifest is a dictionary, with targetFolder: sourceFolder entries. Each
                sourceFolder will be copied or linked to populate the respective targetFolder. Source folders can be
                absolute paths, or paths relative to the path of the FlowIR YAML file. SourceFolders may also be
                suffixed with :copy or :link to control whether targetFolder will be copied or linked to sourceFolder
                (default is copy). TargetFolders are interpreted as relative paths under the instance directory. They
                may also be the resulting folder name of some applicationDependency. They may include a path-separator
                (.e.g /) but must not be absolute paths.
        """
        # VV: Lazily import frontend_cwl because it imports `cwltool` which in turns warns users that
        #     python 2 is out of fashion
        import experiment.model.frontends.cwl

        logger = logging.getLogger('CWLConf')
        cwl_dir = os.path.join(path, 'conf', 'cwl')
        platform = platform or experiment.model.frontends.flowir.FlowIR.LabelDefault
        main_file = self._path_to_main_file(path, is_instance, **kwargs)
        job_order_file = kwargs.get('cwlJobOrderFile', 'job_order.yml')
        if job_order_file != '':
            job_order_files = [os.path.join(cwl_dir, job_order_file)]
        else:
            job_order_files = []

        logger.info('Load CWL (%s job_order:%s), the default platform is %s' % (main_file, job_order_files, platform))

        config_patches = config_patches or []

        cwl = experiment.model.frontends.cwl.Cwl.process_cwl_doc(main_file, job_order_files)
        concrete, new_input_files, new_data_files = cwl.generate_component_descriptions()

        FlowIRExperimentConfiguration.__init__(
            self, path, platform, variable_files, system_vars, is_instance,
            createInstanceFiles, primitive, concrete, updateInstanceFiles=updateInstanceFiles,
            manifest=manifest, config_patches=config_patches,
        )

        self.new_input_files = new_input_files
        self.new_data_files = new_data_files


class DOSINIExperimentConfiguration(FlowIRExperimentConfiguration):
    @classmethod
    def format_found_in_directory(cls, root_directory, is_instance, **kwargs):
        # type: (str, bool, Dict[str, Any]) -> bool
        """Returns whether DOSINI configuration files are available for a given package/instance.

        Args:
            root_directory: Path to root directory of workflow package/instance (contains `conf`, `data`, etc)
            is_instance: Look for instance vs package configuration
        """
        return os.path.isfile(cls._path_to_main_file(root_directory, is_instance))

    @classmethod
    def _path_to_main_file(cls, root_directory, is_instance):
        """Returns path to main configuration file in workflow package/instance root directory

        Args:
            root_directory: Path to root directory of workflow package/instance (contains `conf`, `data`, etc)
            is_instance: Look for instance vs package configuration
        """

        exp_file = {
            True: 'experiment.instance.conf',
            False: 'experiment.conf'
        }[is_instance]

        return os.path.join(root_directory, 'conf', exp_file)

    @classmethod
    def _fetch_user_variables(cls, path, out_errors):
        """Returns variables from a file grouped into sections.

        There can be a `global` section, and multiple 'stage' sections that are grouped under
        {
           'global': {
             <str: name>: <int/float/bool/str: value>
           },
          'stages': {
            <int: stage index> : {
              <str: name>: <int/float/bool/str: value>
            },
          }
        }
        """
        user_variables = experiment.model.frontends.dosini.Dosini().fetch_user_variables([path], out_errors)

        ret = {}
        default_section = experiment.model.frontends.dosini.STAGE_DEFAULT_SECTION
        if default_section in user_variables:
            ret[experiment.model.frontends.flowir.FlowIR.LabelGlobal] = user_variables[default_section]
            del user_variables[default_section]

        if user_variables:
            ret[experiment.model.frontends.flowir.FlowIR.LabelStages] = {}

        for stage_name in user_variables:
            if stage_name.lower().startswith('stage'):
                index = int(stage_name[5:])
            else:
                raise ValueError("Section name was expected to be STAGE<%d> but it's %s" % stage_name)
            ret[experiment.model.frontends.flowir.FlowIR.LabelStages][index] = user_variables[stage_name]

        return ret

    def __init__(
            self,  # type: DOSINIExperimentConfiguration
            path,  # type: str
            platform,  # type: Optional[str]
            variable_files,  # type: Optional[List[str]]
            system_vars,  # type: Optional[Dict[str, str]]
            is_instance,  # type: bool
            createInstanceFiles,  # type: bool
            primitive=True,  # type: bool
            updateInstanceFiles=True,  # type: bool
            manifest=None,  # type: Optional[Dict[str, str]]
            config_patches=None,  # type: Optional[Dict[int, List[str]]]
            **kwargs
    ):
        """Initializes and then validates a package/Instance configuration.

        Args:
            platform: Name of platform (defaults to `default` which is mapped to `experiment.conf`)
            path: Path to root directory of workflow package (contains `conf`, `data`, etc)
            variable_files: A list containing paths to variables.conf files which will be loaded right after
                the `variable.conf` files that are defined under `conf_dir/variables.conf`
            system_vars: A dictionary of environment variables.
                These variables are added to every environment - even environments with no vars/name
                The purpose of system vars is for the experiment controller to be able to set
                non-application specific variables
            config_patches: A dictionary whose keys are stage indexes and whose values are a list
                of configuration files. These configuration files will be layered on-top of the
                default configuration file for stage index. For example, allowing addition of new
                components, or dynamic option reconfig.
            is_instance: Indicates whether to load configuration files from an existing instance directory.
            manifest: The manifest is a dictionary, with targetFolder: sourceFolder entries. Each
                sourceFolder will be copied or linked to populate the respective targetFolder. Source folders can be
                absolute paths, or paths relative to the path of the FlowIR YAML file. SourceFolders may also be
                suffixed with :copy or :link to control whether targetFolder will be copied or linked to sourceFolder
                (default is copy). TargetFolders are interpreted as relative paths under the instance directory. They
                may also be the resulting folder name of some applicationDependency. They may include a path-separator
                (.e.g /) but must not be absolute paths.

            **kwargs: are inherited from FlowIRExperimentConfiguration
        """
        self.log = logging.getLogger('DosiniConf')

        platform = platform or experiment.model.frontends.flowir.FlowIR.LabelDefault

        self.log.info('Load DOSINI, the default platform is %s' % platform)
        config_patches = config_patches or []
        conf_dir = os.path.join(path, 'conf')
        self._dosini = experiment.model.frontends.dosini.Dosini()

        out_errors = []
        flowir = self._dosini.load_from_directory(
            conf_dir, [], config_patches, is_instance, out_errors=out_errors
        )

        concrete = experiment.model.frontends.flowir.FlowIRConcrete(flowir, platform, {})

        try:
            FlowIRExperimentConfiguration.__init__(
                self, path, platform, variable_files, system_vars, is_instance,
                createInstanceFiles, primitive, concrete, updateInstanceFiles=updateInstanceFiles,
                manifest=manifest, config_patches=config_patches, **kwargs
            )
        except Exception as e:
            self.log.critical("Failed to instantiate experiment configuration: %s" % e)
            raise_with_traceback(e)

        if createInstanceFiles:
            if is_instance and updateInstanceFiles is False and self.format_found_in_directory(
                    path, is_instance):
                # VV: Don't update if updateInstanceFiles is not set
                return

            unreplicated = self._unreplicated.instance(
                ignore_errors=True, inject_missing_fields=False, fill_in_all=False,
                is_primitive=True,
            )
            self._dosini.dump(unreplicated, conf_dir, is_instance=True, update_existing=updateInstanceFiles)


class ExperimentConfigurationFactory(object):
    format_map = {
        'flowir': FlowIRExperimentConfiguration,
        'dosini': DOSINIExperimentConfiguration,
        'cwl': CWLExperimentConfiguration,
        'dsl': DSLExperimentConfiguration,

        CWLExperimentConfiguration: 'cwl',
        FlowIRExperimentConfiguration: 'flowir',
        DOSINIExperimentConfiguration: 'dosini',
        DSLExperimentConfiguration: 'dsl',
    }

    default_priority = ['cwl', 'dosini', 'dsl', 'flowir']

    @classmethod
    def get_config_parser(
            cls,
            path: str,
            is_instance: bool,
            format_priority: Optional[List[str]] = None,
            **kwargs
    ) -> Type[FlowIRExperimentConfiguration]:
        """Return the available configuration type under `root_directory` with the highest priority.

        Args:
            path: Path to the definition of a workflow, it can point to a file or a folder
            is_instance: Look for instance vs package configuration
            format_priority: Indicates priorities of formats (setting this parameter to None will prioritize
              DOSINI over flowir i.e. ['dosini', 'flowir'])

        Raises:
            experiment.errors.PackageUnknownFormatError: If @path contains an unknown workflow definition schema

        Returns:
            Type[FlowIRExperimentConfiguration] which is a Class that can parse the format at @path
        """
        if os.path.isfile(path):
            with open(path, 'r') as f:
                try:
                    dictionary = yaml.safe_load(f)
                    if isinstance(dictionary, dict) and "entrypoint" in dictionary and "components" in dictionary:
                        return cls.format_map["dsl"]
                except Exception:
                    pass
            return cls.format_map['flowir']

        if format_priority and 'cwl' in format_priority and 'flowir' not in format_priority:
            # VV: if the user has specified `cwl` but not `flowir` make sure to automatically add
            #     `flowir` because there is no `instance` version of FlowIR
            logging.getLogger('ExpFactory').log(15,
                'Inserting "flowir" to list of format priorities %s' % format_priority)
            format_priority.append('flowir')

        format_priority = format_priority or cls.default_priority

        for name in format_priority:
            factory = cls.format_map[name.lower()]
            if factory.format_found_in_directory(path, is_instance, **kwargs):
                return factory

        raise experiment.model.errors.PackageUnknownFormatError(path, format_priority, is_instance)

    @classmethod
    def configurationForExperiment(
            cls,
            packagePath: str,
            platform: Optional[str] = None,
            systemvars: Optional[Dict[str, str]] = None,
            createInstanceFiles: bool = True,
            primitive: bool = True,
            variable_files: Optional[List[str]] = None,
            format_priority: Optional[List[str]] = None,
            out_chosen_format: Optional[Dict[str, Any]] = None,
            updateInstanceFiles:bool = True,
            is_instance: bool = False,
            manifest: Optional[Union[str, Dict[str, str]]] = None,
            validate: bool = True,
            variable_substitute: bool = True,
            **kwargs
    ) -> FlowIRExperimentConfiguration:
        '''Return an FlowIRExperimentConfiguration object for an experiment (instance or package)

        Args:
            packagePath: Path to the package definition
            platform: Name of platform (defaults to `default` which is mapped to `experiment.conf`)
            systemvars: A dictionary of environment variables.
                These variables are added to every environment - even environments with no vars/name
                The purpose of system vars is for the experiment controller to be able to set
                non-application specific variables
            is_instance: Indicates whether to load the Instance flavour of the Experiment instead of the Package one
            createInstanceFiles: If set to True will auto-generate instance files provided that they do not
                already exist. Set updateInstanceFiles to True too to update existing files.
            primitive: If true will not perform replication, otherwise will replicate components following the
                workflowAttributes.replicate rules
            variable_files: A list containing paths to variables.conf files which will be loaded right after
                the `variable.conf` files that are defined under `conf_dir/variables.conf`
            format_priority: Indicates priorities of formats (setting this parameter to None will prioritize
              DOSINI over flowir i.e. ['dosini', 'flowir'])
            updateInstanceFiles(bool): Set to true to update instance files if they already exist
            manifest: Optional path to YAML file containing manifest OR contents of the manifest.
                The manifest is a dictionary, with targetFolder: sourceFolder entries. Each
                sourceFolder will be copied or linked to populate the respective targetFolder. Source folders can be
                absolute paths, or paths relative to the path of the FlowIR YAML file. SourceFolders may also be
                suffixed with :copy or :link to control whether targetFolder will be copied or linked to sourceFolder
                (default is copy). TargetFolders are interpreted as relative paths under the instance directory. They
                may also be the resulting folder name of some applicationDependency. They may include a path-separator
                (.e.g /) but must not be absolute paths.
            validate: When set to True test whether platform contains enough information to build a non-primitive
                WorkflowGraph.
            variable_substitute: Whether to perform variable substitution, optional for a primitive graph
                but required for a replicated one
            kwargs: Arguments to FlowIRExperimentConfiguration constructor and classes that inherit it depending on
                the `format` of the Workflow that this method ends up loading.

        Raises:
            experiment.errors.ExperimentUndefinedPlatformError: if the experiment does not have the requested platform
            experiment.errors.ExperimentInvalidConfigurationError:  If the configuration is invalid
        '''
        out_chosen_format = out_chosen_format if out_chosen_format is not None else {}

        try:
            factory = cls.get_config_parser(
                path=packagePath, is_instance=is_instance, format_priority=format_priority, **kwargs)
        except experiment.model.errors.PackageUnknownFormatError as e:
            if is_instance is True:
                logger = logging.getLogger('ConfigFactory')
                logger.info(f"Path {packagePath} is not an instance - will try to load a package definition")
                factory = cls.get_config_parser(
                    path=packagePath, is_instance=False, format_priority=format_priority, **kwargs)
                # logger.info("This is indeed an instance directory containing a package configuration")
                is_instance = False
            else:
                raise_with_traceback(e)
                raise  # VV: keep linter happy

        implied_manifest = {}

        if os.path.isdir(packagePath):
            implied_manifest = experiment.model.frontends.flowir.Manifest.fromDirectory(packagePath).manifestData

        if manifest is not None and isinstance(manifest, str):
            try:
                manifest = experiment.model.frontends.flowir.Manifest.fromFile(manifest, validate=False).manifestData
            except experiment.model.errors.FlowIRManifestException as e:
                raise_with_traceback(e)
            except Exception as e:
                raise_with_traceback(experiment.model.errors.FlowIRManifestException(
                    f"Unknown error {e} while parsing manifest {manifest}"))

        if manifest is not None:
            manifest.update(implied_manifest)
        else:
            manifest = implied_manifest

        ret = factory(
            path=packagePath, platform=platform, variable_files=variable_files, system_vars=systemvars,
            is_instance=is_instance, createInstanceFiles=createInstanceFiles, validate=validate,
            primitive=primitive, updateInstanceFiles=updateInstanceFiles, manifest=manifest,
            variable_substitute=variable_substitute, **kwargs)

        out_chosen_format['format'] = cls.format_map[factory]
        out_chosen_format['is-instance'] = is_instance
        out_chosen_format['factory'] = factory
        out_chosen_format['manifest'] = manifest.copy()

        return ret
