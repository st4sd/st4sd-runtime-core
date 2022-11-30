#
# coding=UTF-8
# Copyright IBM Inc. 2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis


from __future__ import print_function


import configparser
import copy
import glob
import logging
import os
import pprint
import re
import shutil
import tempfile
from collections import OrderedDict
from typing import (Any, Callable, Dict, List, Optional, Tuple, Type, Union,
                    cast)
from six import string_types

import experiment.model.errors
from experiment.model.frontends.flowir import DictFlowIR, DictFlowIRComponent, FlowIR

VariableFormat = re.compile("%\([.A-Za-z_0-9-]+\)s")
IndexAccess = re.compile(".+\[(\d+|%\(\S+\)s)\]")

STAGE_META_SECTION = "META"
STAGE_DEFAULT_SECTION = "DEFAULT"

logger = logging.getLogger('Dosini')


def pretty_json(entry):
    import json
    return json.dumps(entry, sort_keys=True, indent=4, separators=(',', ': '))


def dict_to_dosini(dictionary):
    # type: (Dict[str, Dict[str, str]]) -> configparser.ConfigParser
    cfg = FlowConfigParser()

    for key_section in dictionary:
        section = dictionary[key_section]
        cfg.add_section(key_section)
        for option_name in section:
            option_value = section[option_name]
            cfg.set(key_section, option_name, str(option_value))

    return cfg


def dosini_to_dict(
        cfg,  # type: Union[configparser.ConfigParser.SafeConfigParser, str]
        out_errors=None,  # type: Optional[List[Exception]]
        user_variables=None,  # type: Optional[Dict[str, str]]
        consider_meta_as_section=False,
):
    # type: (...) -> Dict[str, Dict[str, str]]
    ret = {}

    user_variables = user_variables or {}
    out_errors = out_errors if out_errors is not None else []

    if isinstance(cfg, string_types):
        path = cast(str, cfg)
        cfg = FlowConfigParser()
        cfg.read([path])

    default_values = {}

    if consider_meta_as_section:
        non_sections = [STAGE_META_SECTION, STAGE_DEFAULT_SECTION]
    else:
        non_sections = [STAGE_DEFAULT_SECTION]

    for default_section in non_sections:
        if cfg.has_section(default_section):
            for key in cfg.options(default_section):
                default_values[key] = cfg.get(default_section, key)
                cfg.remove_option(default_section, key)

    if cfg.has_section(STAGE_META_SECTION):
        for option in cfg.options(STAGE_META_SECTION):
            default_values[option] = cfg.get(STAGE_META_SECTION, option, raw=True)

    non_default = [
        section for section in cfg.sections() if section.upper() not in non_sections
    ]

    ret[STAGE_DEFAULT_SECTION] = default_values

    for section in non_default:
        component = {}

        for name in cfg.options(section):
            if name in component:
                out_errors.append(
                    experiment.model.errors.DOSINIDuplicateOption(name, cfg, section)
                )
            value = cfg.get(section, name, raw=True)
            component[name] = value

        if section.upper() in non_sections:
            section = STAGE_DEFAULT_SECTION

            ret[section].update(component)
        else:
            ret[section] = component

    return ret


def environment_to_dict(cfg):
    # type: (Union[configparser.ConfigParser.SafeConfigParser, str]) -> Dict[str, Dict[str, str]]
    """Parses a DOSINI environment and converts it into a dictionary.

    Replaces all references to variables defined in the `DEFAULT` section with their values (it
    attempts to remove indirect references too)."""
    ret = {}

    if isinstance(cfg, string_types):
        path = cast(str, cfg)
        cfg = FlowConfigParser()
        cfg.read([path])

    default_values = cfg.defaults()

    for section in cfg.sections():
        if section == 'DEFAULT':
            continue

        ret[section] = {}

        for name in cfg.options(section):
            if name in default_values:
                continue

            value = cfg.get(section, name, raw=True)

            dirty = True
            while dirty:
                # VV: Search for %(<variable>)s references which point to variables defined in the
                #     DEFAULT section. Stop when there's no more references to DEFAULT variables
                #     in the value (there can be references to variables not defined in this file).
                for global_var in default_values:
                    pattern = '%%(%s)s' % global_var
                    if pattern in value:
                        value = value.replace(pattern, default_values[global_var])
                        break
                else:
                    dirty = False

            ret[section][name] = value

    return ret


class GenerateDosini(object):
    def __init__(
            self,  # type: GenerateDosini
            is_instance,  # type: bool
            stages=None,  # type: Optional[Dict[int, str]]
            status=None,  # type: Optional[str]
            output=None,  # type: Optional[str]
            inputs=None,  # type: Optional[Dict[str, str]]
            environments=None,  # type: Optional[Dict[Optional[str], str]]
            variables=None,  # type: Optional[Dict[Optional[str], str]]
            root_path=None,  # type: Optional[str]
            path_name=None,  # type: Optional[str]
            delete_after=True,  # type: bool
    ):
        self.is_instance = is_instance
        self.stages = stages or {}
        self.status = status or {}
        self.output = output or {}
        self.environments = environments or {}
        self.inputs= inputs or {}
        self.variables = variables or {}
        self.path_directory = None  # type: Optional[str]
        self.root_path = root_path
        self.path_name = path_name
        self.delete_after = delete_after
    def __enter__(self):
        assert self.path_directory is None

        root_path = self.root_path or tempfile.mkdtemp()

        if self.path_name is None:
            self.path_directory = tempfile.mkdtemp(dir=root_path)
        else:
            self.path_directory = os.path.join(root_path, self.path_name)
            os.makedirs(self.path_directory)

        dir_stages = os.path.join(self.path_directory, 'stages.d')
        dir_variables = os.path.join(self.path_directory, 'variables.d')

        directories = [dir_stages, dir_variables,]

        if self.inputs:
            dir_input = os.path.join(root_path, 'input')
            directories.append(dir_input)

        for p in directories:
            os.mkdir(p)

        for input_name in self.inputs:
            path = os.path.join(dir_input, input_name)
            with open(path, 'w') as f:
                f.write(self.inputs[input_name])

        for stage_index in self.stages:
            if self.is_instance:
                path = os.path.join(dir_stages, 'stage%d.instance.conf' % stage_index)
            else:
                path = os.path.join(dir_stages, 'stage%d.conf' % stage_index)
            stage = self.stages[stage_index]

            with open(path, 'w') as f:
                f.write(stage)

        for platform_name in self.environments:
            platform_environments = self.environments[platform_name]

            if platform_name is not None:
                path = os.path.join(self.path_directory, 'experiment.%s.conf' % platform_name)
            else:
                if self.is_instance is False:
                    path = os.path.join(self.path_directory, 'experiment.conf')
                else:
                    path = os.path.join(self.path_directory, 'experiment.instance.conf')

            with open(path, 'w') as f:
                f.write(platform_environments)

        for platform_name in self.variables:
            platform_variables = self.variables[platform_name]

            if platform_name is not None:
                path = os.path.join(dir_variables, '%s.conf' % platform_name)
            else:
                path = os.path.join(self.path_directory, 'variables.conf')

            with open(path, 'w') as f:
                f.write(platform_variables)

        for file_name, configuration in [('status.conf', self.status), ('output.conf', self.output)]:
            if configuration:
                path = os.path.join(self.path_directory, file_name)
                with open(path, 'w') as f:
                    f.write(configuration)

        print('Configuration is stored under %s' % self.path_directory)

        return self.path_directory

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.path_directory is not None:
            if self.delete_after:
                shutil.rmtree(self.path_directory)


class FlowConfigParser(configparser.ConfigParser):
    def __init__(self, defaults=None, dict_type=None, allow_no_value=False, **kwargs):
        dict_type = dict_type or OrderedDict
        args = kwargs.copy()
        if 'strict' not in args:
            args['strict'] = False
        super(FlowConfigParser, self).__init__(defaults, dict_type, allow_no_value=allow_no_value, **args)

    def get(self, section, option, raw=True, vars=None):
        return super(FlowConfigParser, self).get(section, option, raw=raw, vars=vars)

    def read(self, paths, encoding=None):
        encoding = encoding or u'utf-8'
        return super(FlowConfigParser, self).read(paths, encoding=encoding)

    # VV: Maintain lower/upper-case
    def optionxform(self, optionstr):
        return optionstr


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


class Dosini(object):
    # VV: Making this a Class object enables us to load multiple packages and keep the record of the reported warnings
    _suppressed_warnings = set()
    _known_flowir = set([
        'resolvePath', 'executable', 'arguments', 'environment', 'interpreter', 'expandArguments',
        'references', 'aggregate',
        'isMigratable', 'replicate', 'repeatRetries',
        'queue', 'reservation', 'resourceString',
        'statusRequestInterval', 'walltime', 'docker-args', 'docker-image', 'rstage-in', 'rstage-out',
        'numberProcesses', 'numberThreads', 'ranksPerNode', 'threadsPerCore', 'memory',
    ])

    _translate_map = {
        'lsf-docker-image': 'dockerImage',
        'lsf-docker-profile-app': 'dockerProfileApp',
        'lsf-docker-options': 'dockerOptions',
        'shutdown-on': 'shutdownOn',
        'restart-hook-on': 'restartHookOn',
        'restart-hook-file': 'restartHookFile',
        'job-type': 'backend',
        'repeat-interval': 'repeatInterval',
        'max-restarts': 'maxRestarts',
        'k8s-image': 'image',
        'k8s-grace-period': 'gracePeriod',
        'k8s-image-pull-secret': 'image-pull-secret',
        'k8s-api-key-var': 'api-key-var',
        'k8s-host': 'host',
        'k8s-namespace': 'namespace',
        'k8s-cpu-units-per-core': 'cpuUnitsPerCore',
        'optimizerDisable': 'disable',
        'optimizerExploitChance': 'exploitChance',
        'optimizerExploitTarget': 'exploitTarget',
        'optimizerExploitTargetLow': 'exploitTargetLow',
        'optimizerExploitTargetHigh': 'exploitTargetHigh',
        'memoization-disable-strong': 'strong',
        'memoization-disable-fuzzy': 'fuzzy',
        'memoization-embedding-function': 'embeddingFunction',
    }

    @classmethod
    def known_flowir_options(cls):
        ret = cls._known_flowir.copy()
        ret.update(cls._translate_map.keys())
        return list(ret)

    @classmethod
    def dosini_to_flowir_translate_map(cls):
        return cls._translate_map.copy()

    @classmethod
    def suppressed_warning(self, warning_message):
        # type: (str) -> None
        if warning_message not in self._suppressed_warnings:
            self._suppressed_warnings.add(warning_message)
            if len(self._suppressed_warnings) > 100:
                self._suppressed_warnings.clear()

            logger.warning('%s. Future identical warnings will be suppressed' % warning_message)

    def __init__(self):
        """Frontend to load/store packages in DOSINI format.
        """
        pass

    @classmethod
    def _dosini_environments_to_dicts(cls, directory, is_instance):
        # type: (str, bool) -> Dict[str, Dict[str, Dict[str, str]]]
        """Read environment files and return Dictionary containing their raw contents.

        Return format is:
        {
            platform_name(str) : {
                section_name(str): {
                    variable_name(str): variable_value(str)
                }
            }
        }

        Args:
            directory:
            is_instance:

        Returns:

        """
        platform_files = glob.glob(os.path.join(directory, 'experiment.*.conf'))

        if is_instance is False:
            default_platform_path = os.path.join(directory, 'experiment.conf')
        else:
            default_platform_path = os.path.join(directory, 'experiment.instance.conf')

        platform_files = [e for e in platform_files if os.path.split(e)[1] != 'experiment.instance.conf']

        platforms_to_paths = {}

        if is_instance is False:
            for path in platform_files:
                # VV: naming scheme: experiment.<platform name>.conf

                # VV: remove path to file and .conf extension
                filename = os.path.splitext(os.path.split(path)[1])[0]

                # VV: Skip past 'experiment.'
                platform_name = filename[11:]

                platforms_to_paths[platform_name] = path

        if 'default' in platforms_to_paths:
            raise experiment.model.errors.ExperimentInvalidConfigurationError("Default experiment should be defined within "
                                                                        "'experiment.conf' not 'experiment.default.conf")

        if os.path.exists(default_platform_path):
            platforms_to_paths['default'] = default_platform_path

        for platform_name in platforms_to_paths:
            logger.log(19, "Discovered platform %s in %s" % (
                platform_name, platforms_to_paths[platform_name]
            ))

        dosini_dict = {
            name: environment_to_dict(platforms_to_paths[name]) for name in platforms_to_paths
        }

        return dosini_dict

    @classmethod
    def parse_environment_dicts(
            cls,  # type: Type[Dosini]
            flowir,  # type: DictFlowIR
            dosini_dict,  # type: Dict[str, Dict[str, Dict[str, str]]]
            is_instance=False,  # type: bool
    ):
        # type: (...) -> DictFlowIR

        flowir = flowir.copy()
        environments = {
            'default': {}
        }

        application_dependencies = {
        }
        virtual_environments = {
        }

        virtual_envs = ['SANDBOX', 'ENVIRONMENT']
        for platform_name in dosini_dict:
            # VV: We don't care about the order of the Environments because they are independent
            #     from each other, we DO care about the order within each environment
            platform_environments = {}  # type: Dict[str, Dict[str, str]]

            if is_instance and platform_name != FlowIR.LabelDefault:
                logger.log(19, "Skipping platform %s because I am loading an instance (which only has "
                              "the default platform)" % platform_name)
                continue

            config = dosini_dict[platform_name]
            for environment_name in config:
                environment = {}
                section = config[environment_name]
                for variable_name in section:
                    environment[variable_name] = section[variable_name]

                if environment_name.upper().startswith('ENV-') is False and environment_name.upper() not in virtual_envs:
                    raise Exception(
                        'Invalid name for environment: %s' % environment_name
                    )

                if environment_name.upper() not in virtual_envs:
                    environment_name = environment_name[4:]

                platform_environments[environment_name] = environment

            if platform_name not in environments:
                environments[platform_name] = {}

            environments[platform_name].update(platform_environments)

            if 'SANDBOX' in environments[platform_name]:
                sandbox = environments[platform_name]['SANDBOX']
                if 'applications' in sandbox:
                    platform_dependencies = sandbox['applications'].split(',')

                    # VV: application-dependencies entries cannot contain empty strings
                    application_dependencies[platform_name] = [e for e in [s.strip() for s in platform_dependencies] if bool(e)]

                    del sandbox['applications']

                if 'virtualenvs' in sandbox:
                    platform_venvs = sandbox['virtualenvs'].split(',')
                    virtual_environments[platform_name] = [s.strip() for s in platform_venvs]

                    del sandbox['virtualenvs']

                del environments[platform_name]['SANDBOX']

        if FlowIR.FieldEnvironments not in flowir:
            flowir[FlowIR.FieldEnvironments] = {}
        if FlowIR.FieldApplicationDependencies not in flowir:
            flowir[FlowIR.FieldApplicationDependencies] = {}
        if FlowIR.FieldVirtualEnvironments not in flowir:
            flowir[FlowIR.FieldVirtualEnvironments] = {}

        flowir[FlowIR.FieldEnvironments].update(environments)

        for platform_name in virtual_environments:
            venvs = virtual_environments[platform_name]
            if platform_name not in flowir[FlowIR.FieldVirtualEnvironments]:
                flowir[FlowIR.FieldVirtualEnvironments][platform_name] = venvs
            else:
                for venv in venvs:
                    if venv not in flowir[FlowIR.FieldVirtualEnvironments][platform_name]:
                        flowir[FlowIR.FieldVirtualEnvironments][platform_name].append(venv)

        for platform_name in application_dependencies:
            app_deps = application_dependencies[platform_name]
            if platform_name not in flowir[FlowIR.FieldApplicationDependencies]:
                flowir[FlowIR.FieldApplicationDependencies][platform_name] = app_deps
            else:
                for app in app_deps:
                    if app not in flowir[FlowIR.FieldApplicationDependencies][platform_name]:
                        flowir[FlowIR.FieldApplicationDependencies][platform_name].append(app)
        return flowir

    @classmethod
    def options_for_backend(cls, backend):

        '''Returns options specific to backend

        Returns:
            A dict with two keys whose values are lists of options.
            The keys are 'required' and 'optional'

        If no backend specific options are available returns empty lists'''

        # Temporary - later implement better way of adding backend specific options

        options = {'required': ["executable"], 'optional': []}
        if backend == "lsf":
            options['optional'].extend(['queue', 'walltime', 'reservation', 'resourceString'])
            ## Alex: Add optional options for running containers
            options['optional'].extend(['lsf-docker-image', 'lsf-docker-profile-app', 'lsf-docker-options'])
        elif backend in ["slurm", 'loadleveler']:
            options['optional'].extend(['queue'])
        elif backend in ['simulator']:
            options['optional'].extend(
                [
                    # Simulator options
                    # VV: Ranges are in the form of low:high (can be just low) (seconds in float)
                    'sim_range_schedule_overhead',
                    'sim_range_execution_time',
                    # VV: default is 0, can be set to fail
                    'sim_expected_exit_code',
                    # VV: Accept all lsf arguments so that we can easily switch between
                    #     production and simulation flows
                    'queue', 'walltime', 'reservation', 'resourceString'
                ]
            )
        elif backend == 'kubernetes':
            options['required'].extend(['k8s-image',])
            options['optional'].extend(['k8s-image-pull-secret', 'k8s-api-key-var', 'k8s-host', 'k8s-namespace',
                                        'k8s-grace-period'])

        return options

    @classmethod
    def validate_component(
            cls,  # type: Dosini
            options,  # type: Dict[str, str]
            component_name,  # type: str
            stage_index,  # type: int
            forbidden_options,  # type: List[str]
            safe_missing,  # type: List[str]
            dosini_section,  # type: str
    ):
        # type: (...) -> Tuple[List[Exception], List[Exception]]
        errors = []  # type: List[Exception]
        possible_errors = []  # type: List[Exception]

        # VV: Interpreters don't need an executable, the 'executable' keyword will be tagged as
        #     required if `backend` is fully resolved. That's alright because FlowIR can perform
        #     more in-depth error checking.
        required = []

        optional = cls.known_flowir_options()

        full_name = 'stage%d.%s' % (stage_index, component_name)
        # Migrated components don't need executable
        try:
            backend = options['job-type']
            backend = FlowIR.fill_in(
                backend, context=options, label='dosini.%s' % full_name, is_primitive=True
            )
        except experiment.model.errors.FlowIRVariableUnknown:
            logger.debug('dosini.%s.job-type="%s" cannot be statically resolved. '
                           'Will assume that backend is setup correctly' % (
                full_name, options['job-type']
            ))
        except KeyError:
            pass
        else:
            backendOpts = cls.options_for_backend(backend)
            required.extend(backendOpts['required'])
            optional.extend(backendOpts['optional'])

            # VV: Ensure that there're no duplicates
            required = sorted(set(required))
            optional = sorted(set(optional))

            if backend == "migrated":
                required.remove("executable")

        if options.get('interpreter') is not None:
            # VV: An interpreter component may not define an `executable`
            if 'executable' in required:
                required.remove('executable')

        required = sorted(set(required) - set(safe_missing))

        missing_options = set(required) - set(options.keys())
        errors.extend([experiment.model.errors.DOSINIMissingRequiredOption(name, component_name, stage_index) for name in missing_options])

        forbidden_options = set(options).intersection(set(forbidden_options))
        errors.extend([experiment.model.errors.DOSINIForbiddenOption(name, options[name], component_name, stage_index) for name in forbidden_options])

        # VV: FIXME Check for duplicate keywords (that calls for some revamping)

        possible_variables = set(options.keys()) - set(required) - set(optional)

        for typo, kw in FlowIR.discover_typos({key: options[key] for key in possible_variables},
                                list(set(required).union(set(optional)))):
            errors.append(experiment.model.errors.DOSINITypographicError(dosini_section, typo, kw))

        for err in errors:
            logger.critical(err)

        return errors, possible_errors

    @classmethod
    def parse_component(
            cls,  # type: Dosini
            options,  # type: Dict[str, str]
            component_name,  # type: str
            stage_index,  # type: int
            forbidden_options=None,  # type: Optional[List[str]]
            safe_missing=None,  # type: Optional[List[str]]
            out_errors=None,  # type: Optional[List[Exception]]
            dosini_section=None  # type: Optional[str]
    ):
        # type: (...) -> DictFlowIRComponent

        # VV: `errors` will be populated by DOSINI errors
        out_errors = out_errors if out_errors is not None else []
        forbidden_options = forbidden_options or []
        safe_missing = safe_missing or []

        # VV: First translate DOSINI names to FlowIR names and then figure out which FlowIR field the
        #     DOSINI name maps to

        variables = options.copy()

        def value_to_bool(value, attribute_name):
            try:
                return {
                    'yes': True,
                    'true': True,
                    'false': False,
                    'no': False,
                }[value.lower()]
            except KeyError:
                if VariableFormat.match(value) is None and IndexAccess.search(value) is None:
                    comp_id = "stage%d.%s" % (stage_index, component_name)
                    raise experiment.model.errors.InvalidValueForConstant(
                        comp_id, attribute_name, 'boolean', value
                    )
                return value

        def value_to_int(value, attribute_name):
            try:
                return int(value)
            except ValueError:
                if VariableFormat.match(value) is None and IndexAccess.search(value) is None:
                    comp_id = "stage%d.%s" % (stage_index, component_name)
                    raise experiment.model.errors.InvalidValueForConstant(
                        comp_id, attribute_name, 'int', value
                    )

                return value

        def value_to_float(value, attribute_name):
            try:
                return float(value)
            except ValueError:
                if VariableFormat.match(value) is None and IndexAccess.search(value) is None:
                    comp_id = "stage%d.%s" % (stage_index, component_name)
                    raise experiment.model.errors.InvalidValueForConstant(
                        comp_id, attribute_name, 'float', value
                    )
                return value

        def value_to_memorybytes(value, attribute_name):
            try:
                # VV: Attempt to parse value into memory-bytes form
                _ = experiment.model.frontends.flowir.FlowIR.memory_to_bytes(value)
            except:
                if VariableFormat.match(value) is None and IndexAccess.search(value) is None:
                    comp_id = "stage%d.%s" % (stage_index, component_name)
                    raise experiment.model.errors.InvalidValueForConstant(
                        comp_id, attribute_name, 'memory-bytes', value
                    )
                return value
            else:
                return value
        command = {}
        references = []
        workflowAttributes = {}
        resourceManager = {}
        resourceRequest = {}
        executors = {}

        v_errors, v_possible_errors = cls.validate_component(
            variables, component_name, stage_index, forbidden_options, safe_missing, dosini_section=dosini_section
        )

        full_name = 'stage%s.%s' % (stage_index, component_name)
        out_errors.extend(v_errors)

        for option_name, similar_keyword in v_possible_errors:
            cls.suppressed_warning('%s: Option "%s=%s" looks like a typo. Did you mean: "%s" ?' % (
                full_name, option_name, variables[option_name], similar_keyword
            ))

        executors_main = []
        executors_pre = []
        executors_post = []

        translate_map = cls.dosini_to_flowir_translate_map()
        # VV: Macaroni code that handles all known FlowIR values
        # The way it works is that we check for the DOSINI key tranlate it to the FlowIR name and use that to
        # store the information in the appropriate FlowIR dictionary
        for key in set(variables.keys()).intersection(cls.known_flowir_options()):
            value = variables[key]
            del variables[key]

            try:
                translated_key = translate_map[key]
            except KeyError:
                translated_key = key

            if key == 'resolvePath':
                command[translated_key] = value_to_bool(value, key)
            elif key in ['executable', 'arguments', 'environment', 'interpreter', 'expandArguments']:
                command[translated_key] = value
            elif key == 'references':
                references = value.split()
            elif key in ['restart-hook-file']:
                workflowAttributes[translated_key] = value
            elif key in ['shutdown-on', 'restart-hook-on']:
                workflowAttributes[translated_key] = value.split()
            elif key in ['aggregate', 'isMigratable']:
                workflowAttributes[translated_key] = value_to_bool(value, key)
            elif key in ['memoization-disable-strong', 'memoization-disable-fuzzy']:
                memoization = workflowAttributes.get('memoization', {})
                if 'disable' not in memoization:
                    memoization['disable'] = {}
                memoization['disable'][translated_key] = value_to_bool(value, key)
                workflowAttributes['memoization'] = memoization
            elif key in ['memoization-embedding-function']:
                memoization = workflowAttributes.get('memoization', {})
                memoization[translated_key] = value
                workflowAttributes['memoization'] = memoization
            elif key in [
                'optimizerDisable', 'optimizerExploitChance', 'optimizerExploitTarget',
                'optimizerExploitTargetLow', 'optimizerExploitTargetHigh']:
                optimizer = workflowAttributes.get('optimizer', {})

                if key != 'optimizerDisable':
                    optimizer[translated_key] = value_to_float(value, key)
                else:
                    optimizer[translated_key] = value_to_bool(value, key)
                workflowAttributes['optimizer'] = optimizer
            elif key in ['replicate', 'repeatRetries', 'maxRestarts']:
                workflowAttributes[translated_key] = value_to_int(value, key)
            elif key == 'repeat-interval':
                if value is not None:
                    repeat_interval = value_to_float(value, key)
                    is_repeat = True
                else:
                    is_repeat = False
                    repeat_interval = None

                workflowAttributes['repeatInterval'] = repeat_interval
                workflowAttributes['isRepeat'] = is_repeat
            elif key == 'job-type':
                if 'config' not in resourceManager:
                    resourceManager['config'] = {}

                resourceManager['config'][translated_key] = value
            elif key == 'walltime':
                if 'config' not in resourceManager:
                    resourceManager['config'] = {}

                resourceManager['config'][translated_key] = value_to_float(value, key)
            elif key in ['queue', 'reservation', 'resourceString', 'lsf-docker-image', 'lsf-docker-profile-app', 'lsf-docker-options']:
                if 'lsf' not in resourceManager:
                    resourceManager['lsf'] = {}

                resourceManager['lsf'][translated_key] = value
            elif key == 'statusRequestInterval':
                if 'lsf' not in resourceManager:
                    resourceManager['lsf'] = {}

                resourceManager['lsf'][translated_key] = value_to_float(value, key)
            elif key in ['k8s-image-pull-secret', 'k8s-host', 'k8s-namespace', 'k8s-api-key-var', 'k8s-image']:
                if 'kubernetes' not in resourceManager:
                    resourceManager['kubernetes'] = {}

                resourceManager['kubernetes'][translated_key] = value
            elif key == 'k8s-cpu-units-per-core':
                if 'kubernetes' not in resourceManager:
                    resourceManager['kubernetes'] = {}
                resourceManager['kubernetes'][translated_key] = value_to_float(value, key)
            elif key in ['k8s-grace-period']:
                if 'kubernetes' not in resourceManager:
                    resourceManager['kubernetes'] = {}
                resourceManager['kubernetes'][translated_key] = value_to_int(value, key)
            elif key == 'rstage-in':
                executors_pre.append({
                    'name': 'lsf-dm-in',
                    'payload': value,
                })
            elif key == 'rstage-out':
                executors_post.append({
                    'name': 'lsf-dm-out',
                    'payload': value,
                })
            elif key == 'docker-args':
                docker_dict = [dictionary for dictionary in executors_main if dictionary['name'] == 'docker']

                update = {'docker-args': value, 'name': 'docker'}

                if len(docker_dict) == 0:
                    executors_main.append(update)
                else:
                    assert len(docker_dict) == 1
                    docker_dict[0].update(update)
            elif key == 'docker-image':
                docker_dict = [dictionary for dictionary in executors_main if dictionary['name'] == 'docker']

                update = {'docker-image': value, 'name': 'docker'}

                if len(docker_dict) == 0:
                    executors_main.append(update)
                else:
                    assert len(docker_dict) == 1
                    docker_dict[0].update(update)

            elif key in ['numberProcesses', 'numberThreads', 'ranksPerNode', 'threadsPerCore']:
                resourceRequest[translated_key] = value_to_int(value, key)
            elif key == 'memory':
                resourceRequest[translated_key] = value_to_memorybytes(value, key)

        component = {
            'name': component_name,
            'stage': stage_index,
        }

        if command:
            component['command'] = command
        if references:
            component['references'] = references
        if workflowAttributes:
            component['workflowAttributes'] = workflowAttributes
        if resourceManager:
            component['resourceManager'] = resourceManager
        if resourceRequest:
            component['resourceRequest'] = resourceRequest
        if executors_main:
            executors['main'] = executors_main
        if executors_pre:
            executors['pre'] = executors_pre
        if executors_post:
            executors['post'] = executors_post

        if executors:
            component['executors'] = executors

        component['variables'] = variables

        return component

    @classmethod
    def parse_output(cls, flowir, dict_output):
        # type: (DictFlowIR, Dict[str, Dict[str, str]]) -> DictFlowIR
        flowir = flowir.copy()
        packages = {}

        stage_sections = [e for e in list(dict_output.keys()) if e not in [STAGE_DEFAULT_SECTION, STAGE_META_SECTION]]

        for package in stage_sections:
            def safe_get(option, cfg):
                if option in cfg:
                    return cfg[option]

            stages = (safe_get('stages', dict_output[package]) or '').split(',')

            def extract_stage_index(stage):
                assert stage.lower().startswith('stage')
                return int(stage[5:])

            # VV: Remove whitespace between comma separated words, and keep non-empty stage-descriptions
            stages = [e for e in [s.strip() for s in stages] if e]
            stages = list(map(extract_stage_index, stages))

            data_in = safe_get('data-in', dict_output[package])
            description = safe_get('description', dict_output[package])
            file_type = safe_get('type', dict_output[package])

            packages[package] = {
                'stages': stages,
                'data-in': data_in,
                'description': description,
                'type': file_type,
            }

        if packages:
            if FlowIR.FieldOutput not in flowir:
                flowir[FlowIR.FieldOutput] = {}

            for package in packages:
                if package not in flowir[FlowIR.FieldOutput]:
                    flowir[FlowIR.FieldOutput][package] = {}

                flowir[FlowIR.FieldOutput][package].update(packages[package])

        return flowir

    @classmethod
    def parse_status(cls, flowir, dict_status):
        # type: (DictFlowIR, Dict[str, Dict[str, str]]) -> DictFlowIR
        status = {}

        # VV: A stage may define an executable along with arguments and references.
        #     this binary will be periodically invoked by Flow to infer the progress of the
        #     stage towards its completion. If a binary is not supplied then the progress of
        #     a stage is the percentage of its components which have finished successfully.
        #     Each stage may also define a `stage-weight` which will be used as a component
        #     of a weighted sum to calculate the progress of the whole experiment.
        #     The sum of stage-weights for all stages should be equal to 1.0 if it's not
        #     Flow will assign a stage-weight to the stages (typically 1.0/number_of_stages)
        #     The current method does not perform any checks other than making sure that
        #     stage-weights are indeed floating point numbers

        def safe_get(option, cfg):
            if option in cfg:
                return cfg[option]

        flowir = flowir.copy()

        stage_sections = [e for e in list(dict_status.keys()) if e not in [STAGE_DEFAULT_SECTION, STAGE_META_SECTION]]

        for stage in stage_sections:
            assert stage.startswith('STAGE')

            stage_index = int(stage[5:])

            stage_weight = safe_get('stage-weight', dict_status[stage])

            if stage_weight is not None:
                try:
                    stage_weight = float(stage_weight)
                except ValueError:
                    raise experiment.model.errors.ExperimentInvalidConfigurationError(
                        "status.conf: Stage %d defines invalid stage-weight (%s)" % (
                            stage_index, stage_weight
                        ))

            executable = safe_get('executable', dict_status[stage])
            if executable:
                arguments = safe_get('arguments', dict_status[stage]) or ''
                references = safe_get('references', dict_status[stage]) or ''

                references = references.split()

                status[stage_index] = {
                    'stage-weight': stage_weight,
                    'executable': executable,
                    'arguments': arguments,
                    'references': references,
                }
            else:
                status[stage_index] = {
                    'stage-weight': stage_weight
                }
        if FlowIR.FieldStatusReport not in flowir:
            flowir[FlowIR.FieldStatusReport] = {}

        for stage_index in status:
            if stage_index not in flowir[FlowIR.FieldStatusReport]:
                flowir[FlowIR.FieldStatusReport][stage_index] = {}
            flowir[FlowIR.FieldStatusReport][stage_index].update(status[stage_index])

        return flowir

    @classmethod
    def _discover_stages(cls, directory, is_instance):
        #  type: (str, bool) -> Dict[int, str]
        stages_dir = os.path.join(directory, 'stages.d')
        if is_instance is False:
            stage_files = glob.glob(os.path.join(stages_dir, 'stage*.conf'))
            # VV: Support parsing the `non-instance` configuration files from some
            #     existing package instance
            stage_files = [path for path in stage_files if path.endswith('.instance.conf') is False]
        else:
            stage_files = glob.glob(os.path.join(stages_dir, 'stage*.instance.conf'))

        stage_to_paths = {}

        for path in stage_files:
            # VV: remove DirPath, (.instance if present), and .conf extension and convert to lowercase
            stage_id = os.path.split(os.path.split(path)[1])[1]
            stage_id = stage_id.split('.')[0]

            # VV: skip the 'stage' part
            stage_index = int(stage_id[5:])

            stage_to_paths[stage_index] = path

        num_stages = len(stage_to_paths)

        if set(stage_to_paths.keys()) != set(range(num_stages)):
            raise experiment.model.errors.ExperimentInvalidConfigurationError(
                "Missing stage files (present: %s)" % list(stage_to_paths.keys())
            )

        logger.info("Discovered %d stages" % num_stages)

        return stage_to_paths

    @classmethod
    def parse_stage(cls, flowir, stage_index, stage_dict, user_variables, out_errors=None):
        # type: (DictFlowIR, int, Dict[str, Dict[str, str]], Dict[str, str], Optional[List[Exception]]) -> DictFlowIR
        """

        global_variables: The format is
            {
              'global': {
                variable_name:str : value:str
              },
              'stages': {
                stage_index:int: {
                  variable_name:str : value:str
                  }
                }
            }

        """
        flowir = flowir.copy()

        out_errors = out_errors if out_errors is not None else []
        user_variables = user_variables or {}

        components = []

        stage_options = {}

        if STAGE_DEFAULT_SECTION in stage_dict:
            stage_options.update(stage_dict[STAGE_DEFAULT_SECTION])

        stage_blueprint = cls.parse_component(
            stage_options, 'StageBlueprint', stage_index, safe_missing=['executable'],
            out_errors=out_errors, dosini_section='[STAGE%s]' % stage_index
        )
        stage_variables = stage_blueprint.get('variables', {})

        # VV: Remove dummy keys from blueprint
        del stage_blueprint['name']
        del stage_blueprint['stage']

        if 'variables' in stage_blueprint:
            # VV: Remove variables from the blueprint since they will be placed under the FieldVariables
            #     top-level FlowIR field
            del stage_blueprint['variables']

        comp_sections = [section for section in stage_dict if section.upper() not in [STAGE_DEFAULT_SECTION, STAGE_META_SECTION]]

        for component_name in comp_sections:
            options = copy.deepcopy(user_variables)
            options.update(stage_dict[component_name])
            component = cls.parse_component(options, component_name, stage_index, out_errors=out_errors,
                                            dosini_section='[stage%s.%s]' % (stage_index, component_name))

            components.append(component)

        if components:
            if FlowIR.FieldComponents not in flowir:
                flowir[FlowIR.FieldComponents] = []

            flowir[FlowIR.FieldComponents].extend(components)

        if stage_variables:
            if FlowIR.FieldVariables not in flowir:
                flowir[FlowIR.FieldVariables] = {}

            if FlowIR.LabelDefault not in flowir[FlowIR.FieldVariables]:
                flowir[FlowIR.FieldVariables][FlowIR.LabelDefault] = {}

            if FlowIR.LabelStages not in flowir[FlowIR.FieldVariables][FlowIR.LabelDefault]:
                flowir[FlowIR.FieldVariables][FlowIR.LabelDefault][FlowIR.LabelStages] = {}

            if stage_index not in flowir[FlowIR.FieldVariables][FlowIR.LabelDefault][FlowIR.LabelStages]:
                flowir[FlowIR.FieldVariables][FlowIR.LabelDefault][FlowIR.LabelStages][stage_index] = {}

            flowir[FlowIR.FieldVariables][FlowIR.LabelDefault][FlowIR.LabelStages][stage_index].update(stage_variables)

        if stage_blueprint:
            if FlowIR.FieldBlueprint not in flowir:
                flowir[FlowIR.FieldBlueprint] = {}

            if FlowIR.LabelDefault not in flowir[FlowIR.FieldBlueprint]:
                flowir[FlowIR.FieldBlueprint][FlowIR.LabelDefault] = {}

            if FlowIR.LabelStages not in flowir[FlowIR.FieldBlueprint][FlowIR.LabelDefault]:
                flowir[FlowIR.FieldBlueprint][FlowIR.LabelDefault][FlowIR.LabelStages] = {}

            if stage_index not in flowir[FlowIR.FieldBlueprint][FlowIR.LabelDefault][FlowIR.LabelStages]:
                flowir[FlowIR.FieldBlueprint][FlowIR.LabelDefault][FlowIR.LabelStages][stage_index] = {}

            flowir[FlowIR.FieldBlueprint][FlowIR.LabelDefault][FlowIR.LabelStages][stage_index].update(stage_blueprint)

        return flowir

    @classmethod
    def _merge_variable_dicts(cls, dest, source):
        dest = dest.copy()

        for scope_name in source:
            if scope_name in dest:
                dest[scope_name].update(source[scope_name])
            else:
                dest[scope_name] = source[scope_name]

        return dest.copy()

    @classmethod
    def _merge_objects(cls, base, override):
        if base is None:
            return override
        if override is None:
            return base

        if type(base) != type(override):
            raise TypeError('Cannot merge %s and %s (types differ: %s vs %s)' % (
                base, override, type(base), type(override)
            ))

        not_for_recursion = string_types + (int, float, bool)

        for_recursion = (
            dict, list
        )

        if isinstance(base, not_for_recursion):
            return override
        elif isinstance(base, dict):
            base = base.copy()
            override = override.copy()

            ret = base

            for key in override:
                value_over = override[key]

                if key not in base or isinstance(value_over, not_for_recursion):
                    ret[key] = value_over
                elif isinstance(value_over, for_recursion):
                    ret[key] = cls._merge_objects(base[key], value_over)
                else:
                    raise NotImplementedError("Don't know how to merge %s with %s" % (ret[key], value_over))

            return ret
        elif isinstance(base, list):
            return base + override
        else:
            raise NotImplementedError("Don't know how to merge toplevel %s with %s" % (base, override))

    @classmethod
    def _dosini_variables_to_dicts(cls, directory, is_instance, out_errors=None):
        # type: (str, bool, Optional[List[Exception]]) -> Dict[str, Dict[Union[int, str], Dict[str, str]]]

        out_errors = out_errors if out_errors is not None else []
        variables_dir = os.path.join(directory, 'variables.d')

        variables_files = []

        default_platform_path = os.path.join(directory, 'variables.conf')
        variables_files.insert(0, default_platform_path)
        variables_files = list(filter(os.path.isfile, variables_files))

        platform_files = glob.glob(os.path.join(variables_dir, '*.conf'))

        dict_variables = {}

        if is_instance is False:
            for path in platform_files:
                # VV: remove path to file and .conf extension
                platform_name = os.path.splitext(os.path.split(path)[1])[0]

                logger.log(19, 'Loading platform %s from %s' % (
                    platform_name, path
                ))

                if platform_name == FlowIR.LabelDefault:
                    raise experiment.model.errors.ExperimentInvalidConfigurationError(
                        "Default variables should be defined within 'variables.conf' not 'variables.d/default.conf"
                    )

                var_scopes = dosini_to_dict(path, out_errors)

                if platform_name not in dict_variables:
                    dict_variables[platform_name] = var_scopes
                else:
                    dict_variables[platform_name] = cls._merge_variable_dicts(
                        dict_variables[platform_name], var_scopes
                    )
            logger.info("Loaded platforms %s" % list(dict_variables))

        if variables_files:
            default = {}

            for path in variables_files:
                logger.info('Loading variable files from %s' % path)
                override = dosini_to_dict(path, out_errors)

                for section in override:
                    if section in default:
                        default[section].update(override[section])
                    else:
                        default[section] = override[section]

            dict_variables[FlowIR.LabelDefault] = default

        return dict_variables

    @classmethod
    def parse_variables(
            cls,
            flowir,  # type: DictFlowIR
            dict_variables,  # type: Dict[str, Dict[str, Dict[str, str]]]
            out_errors=None,  # type: Optional[List[Exception]]
            root_conf=None,  # type: Optional[str]
    ):
        # type: (...) -> DictFlowIR

        out_errors = out_errors if out_errors is not None else []
        root_conf = root_conf or 'conf'
        flowir = flowir.copy()
        variables = {
            # VV: Format:
            # FlowIR.FieldDefault | platform_name (str): {
            #     FlowIR.FieldGlobal: {},  # type: Dict[str, str]
            #     FlowIR.FieldStages: {
            #           stage_index(int): {
            #               name(str): value(str)
            #           }
            #     },  # type: Dict[int, Dict[str, str]]
            # }
        }

        for key in [FlowIR.FieldVariables, FlowIR.FieldBlueprint]:
            if key not in flowir:
                flowir[key] = {}

        blueprint = {}

        for collection_name in dict_variables:
            logger.log(19, "Processing variables in %s" % collection_name)
            variables[collection_name] = {
                FlowIR.LabelGlobal: {},
                FlowIR.LabelStages: {},
            }

            blueprint[collection_name] = {
                FlowIR.LabelGlobal: {},
                FlowIR.LabelStages: {},
            }

            for key in [FlowIR.FieldVariables, FlowIR.FieldBlueprint]:
                if collection_name not in flowir[key]:
                    flowir[key][collection_name] = {}

                for inner_key in [FlowIR.LabelGlobal, FlowIR.LabelStages]:
                    if inner_key not in flowir[key][collection_name]:
                        flowir[key][collection_name][inner_key] = {}

            var_file_dict = dict_variables[collection_name]

            for section in var_file_dict:
                section_id = section.upper()  # type: str
                options = var_file_dict[section]

                if not options:
                    continue

                if section_id not in [FlowIR.LabelGlobal.upper(), FlowIR.LabelDefault.upper()]:
                    if section_id.upper().startswith('STAGE') is False:
                        filename = '%s/variables.d/%s.conf' % (root_conf, collection_name)

                        out_errors.append(
                            experiment.model.errors.DOSINIInvalidSection(filename, section_id.upper())
                        )
                        cls.suppressed_warning("Skipping section %s of %s (invalid section)" % (
                            section_id.upper(), filename
                        ))
                        continue

                    stage_index = int(section_id[5:])

                    stage_blueprint = cls.parse_component(options, 'GlobalStageBlueprint', stage_index)
                    # VV: Remove dummy keys from blueprint
                    del stage_blueprint['name']
                    del stage_blueprint['stage']

                    stage_variables = stage_blueprint.get('variables', {})
                    if 'variables' in stage_blueprint:
                        del stage_blueprint['variables']

                    blueprint[collection_name][FlowIR.LabelStages][stage_index] = stage_blueprint
                    variables[collection_name][FlowIR.LabelStages][stage_index] = stage_variables
                else:
                    global_blueprint = cls.parse_component(
                        options, 'GlobalBlueprint', -1, safe_missing=['executable'],
                        forbidden_options=['replicate', 'aggregate'],
                        dosini_section='blueprint for %s platform' % collection_name)
                    global_variables = global_blueprint.get('variables', {})

                    # VV: Remove dummy keys from blueprint
                    del global_blueprint['name']
                    del global_blueprint['stage']

                    if 'variables' in global_blueprint:
                        del global_blueprint['variables']

                    blueprint[collection_name][FlowIR.LabelGlobal] = global_blueprint
                    variables[collection_name][FlowIR.LabelGlobal] = global_variables

            flowir[FlowIR.FieldVariables] = cls._merge_objects(flowir[FlowIR.FieldVariables], variables)
            flowir[FlowIR.FieldBlueprint] = cls._merge_objects(flowir[FlowIR.FieldBlueprint], blueprint)
        return flowir

    @classmethod
    def fetch_user_variables(cls, variable_files, out_errors=None):
        out_errors = out_errors if out_errors is not None else []

        user_variables = {}

        for path in variable_files:
            logger.info('Extracting user variables from file %s' % path)
            user_variables = dosini_to_dict(path, out_errors)
            if 'GLOBAL' in user_variables:
                user_variables[STAGE_DEFAULT_SECTION] = user_variables.get(STAGE_DEFAULT_SECTION, {})
                user_variables[STAGE_DEFAULT_SECTION].update(user_variables.get('GLOBAL', {}))
                del user_variables['GLOBAL']

            logger.log(19, 'User variables: %s' % pprint.pformat(user_variables))

        return user_variables

    @classmethod
    def load_from_directory(
            cls,  # type: Dosini
            directory,  # type: str
            variable_files,  # type: List[str]
            component_definitions,  # type: Dict[int, List[str]]
            is_instance=False,  # type: bool
            out_errors=None,  # type: Optional[List[Exception]]
    ):
        # type: (...) -> DictFlowIR
        out_errors = out_errors if out_errors is not None else []

        logger.info('Loading DOSINI from %s. Extra variable files: %s' % (
            directory, variable_files
        ))

        user_variables = cls.fetch_user_variables(variable_files, out_errors)
        flowir = {}

        dict_environments = cls._dosini_environments_to_dicts(directory, is_instance)
        flowir = cls.parse_environment_dicts(flowir, dict_environments)
        if is_instance is False:
            dict_variables = cls._dosini_variables_to_dicts(directory, is_instance, out_errors=out_errors)
        else:
            # VV: DOSINI instances fetch their variables straight from the stage%d.instance.conf files
            logger.log(19, "Will not parse variables.conf because we're loading an instance")
            dict_variables = {
                FlowIR.LabelDefault: {}
            }
        flowir = cls.parse_variables(flowir, dict_variables, out_errors, directory)

        stage_to_path = cls._discover_stages(directory, is_instance)

        stage_files_sorted = sorted(stage_to_path.keys())

        # VV: Stages declare components and Variables,
        #     variables can be found under FlowIR['variables'][platform | 'default']['stages'][<index:int>]
        #     and
        #     FlowIR['variables'][platform | 'default']['global']

        for stage_index in stage_files_sorted:
            stage_file = stage_to_path[stage_index]
            logger.log(19, "Loading stage %d from %s" % (stage_index, stage_file))
            stage_dict = dosini_to_dict(stage_file, out_errors)

            global_vars = copy.deepcopy(user_variables.get(STAGE_DEFAULT_SECTION, {}))
            stage_vars = copy.deepcopy(user_variables.get('STAGE%d' % stage_index, {}))

            stage_user_variables = global_vars
            stage_user_variables.update(stage_vars)

            flowir = cls.parse_stage(
                flowir, stage_index, stage_dict, out_errors=out_errors, user_variables=stage_user_variables
            )

        for stage_index in component_definitions:
            for stage_file in component_definitions[stage_index]:
                stage_dict = dosini_to_dict(stage_file, out_errors)
                global_vars = copy.deepcopy(user_variables.get(STAGE_DEFAULT_SECTION, {}))
                stage_vars = copy.deepcopy(user_variables.get('STAGE%d' % stage_index, {}))

                stage_user_variables = global_vars
                stage_user_variables.update(stage_vars)

                flowir = cls.parse_stage(
                    flowir, stage_index, stage_dict, user_variables=stage_user_variables, out_errors=out_errors
                )

        # variables[FlowIR.FieldDefault][FlowIR.FieldStages] = stage_variables
        output_path = os.path.join(directory, 'output.conf')
        if os.path.isfile(output_path):
            dict_output = dosini_to_dict(output_path, out_errors, consider_meta_as_section=True)
            flowir = cls.parse_output(flowir, dict_output)

        status_path = os.path.join(directory, 'status.conf')
        if os.path.isfile(status_path):
            dict_status = dosini_to_dict(status_path, out_errors, consider_meta_as_section=True)
            flowir = cls.parse_status(flowir, dict_status)

        flowir[FlowIR.FieldPlatforms] = sorted(set(flowir[FlowIR.FieldEnvironments].keys())\
            .union(list(flowir[FlowIR.FieldVariables].keys())))

        return experiment.model.frontends.flowir.FlowIR.compress_flowir(flowir)

    @classmethod
    def dump(cls, flowir, output_dir, update_existing=True, is_instance=False):
        # type: (DictFlowIR, str, bool, bool) -> None
        logger.info('Storing DOSINI configuration (instance=%s, update_existing=%s) to path %s' % (
            is_instance, update_existing, output_dir
        ))
        flowir = {} or flowir

        flowir = copy.deepcopy(flowir)

        if update_existing:
            # VV: Do a cleanup first
            stage_files = glob.glob(os.path.join(
                output_dir, 'stages.d', 'stage*.conf'
            ))

            stage_files = [e for e in stage_files if e.endswith('.instance.conf') is is_instance]

            if is_instance is False:
                platform_files = glob.glob(
                    os.path.join(output_dir, 'experiment*.conf')
                )
            else:
                platform_files = []

            if is_instance is False:
                variable_files = [os.path.join(output_dir, 'variables.d', '%s.conf' % platform) for platform in flowir[FlowIR.FieldPlatforms]]
            else:
                variable_files = []

            remove_files = sorted(stage_files + platform_files + variable_files)

            if remove_files:
                logger.info("Will remove:\n  %s" % '\n  '.join(remove_files))
            else:
                logger.log(19, "Will not remove any files.")

            for remove_file in remove_files:

                if os.path.exists(remove_file):
                    os.remove(remove_file)

        if os.path.exists(output_dir) is False:
            os.makedirs(output_dir)

        if os.path.exists(os.path.join(output_dir, 'stages.d')) is False:
            os.makedirs(os.path.join(output_dir, 'stages.d'))

        if os.path.exists(os.path.join(output_dir, 'variables.d')) is False:
            os.makedirs(os.path.join(output_dir, 'variables.d'))

        if is_instance:
            exp_conf_path = os.path.join(output_dir, 'experiment.instance.conf')
        else:
            exp_conf_path = os.path.join(output_dir, 'experiment.conf')

        cls._dump_experiment_root_conf(flowir, exp_conf_path, update_existing)

        cls._dump_variables(flowir, output_dir, is_instance, update_existing)

        if is_instance is False:
            cls._dump_platforms(flowir, output_dir, update_existing)
            cls._dump_output(flowir, output_dir, update_existing)
            cls._dump_status(flowir, output_dir, update_existing)

        cls._dump_components(flowir, output_dir, is_instance, update_existing)

    @classmethod
    def _dump_output(cls, flowir, output_dir, force_generate=True):
        # type: (DictFlowIR, str, bool) -> None
        if FlowIR.FieldOutput not in flowir:
            return

        output = flowir[FlowIR.FieldOutput]
        cfg = FlowConfigParser()

        for name in output:
            cfg.add_section(name)

            entry = output[name]

            for key in ['description', 'type', 'data-in']:
                if key in entry:
                    cfg.set(name, key, str(entry[key]))

            if 'stages' in entry:
                stage_strings= ['stage%d' % idx for idx in entry['stages']]
                cfg.set(name, 'stages', ','.join(stage_strings))
        if output or force_generate:
            with open(os.path.join(output_dir, 'output.conf'), 'w') as f:
                cfg.write(f)

    @classmethod
    def _dump_status(cls, flowir, output_dir, force_generate=True):
        # type: (DictFlowIR, str, bool) -> None
        cfg = FlowConfigParser()

        if FlowIR.FieldStatusReport in flowir:
            status = flowir[FlowIR.FieldStatusReport]

            for stage_index in status:
                pretty_name = 'STAGE%d' % stage_index
                cfg.add_section(pretty_name)

                for key in status[stage_index]:
                    assert isinstance(key, string_types + (float, int, bool,))
                    value = status[stage_index][key]

                    if isinstance(value, string_types) is False and isinstance(value, list):
                        value = ' '.join(value)
                    elif isinstance(value, (float, int, bool )):
                        value = str(value)

                    assert isinstance(value, string_types)

                    cfg.set(pretty_name, key, str(value))
        elif force_generate:
            # VV: First discover how many stages there're
            stages = set()
            for comp in flowir[FlowIR.FieldComponents]:
                stages.add(comp['stage'])
            stages = sorted(list(stages))

            default_stage_weight = 1.0 / float(len(stages))

            for stage_index in stages:
                pretty_name = 'STAGE%d' % stage_index
                cfg.add_section(pretty_name)

                cfg.set(pretty_name, 'stage-weight', '%.2f' % default_stage_weight)
        else:
            return

        with open(os.path.join(output_dir, 'status.conf'), 'w') as f:
            cfg.write(f)

    @classmethod
    def _dump_experiment_root_conf(cls, flowir, output_file_name=None, update_existing=False):
        # type: (DictFlowIR, Optional[str], bool) -> None
        # VV: TODO Maintain whether a variable is defined within the DEFAULT section ?

        if update_existing is False and os.path.exists(output_file_name):
            return

        environments = flowir[FlowIR.FieldEnvironments][FlowIR.LabelDefault]

        application_dependencies = flowir[FlowIR.FieldApplicationDependencies]  # type: Dict[str, List[str]]
        virtual_environments = flowir[FlowIR.FieldVirtualEnvironments]  # type: Dict[str, List[str]]

        cfg = FlowConfigParser()

        sandbox_settings = {}

        if FlowIR.LabelDefault in application_dependencies:
            app_deps = application_dependencies[FlowIR.LabelDefault]

            if app_deps:
                sandbox_settings['applications'] = ','.join(app_deps)

        if FlowIR.LabelDefault in virtual_environments:
            venvs = virtual_environments[FlowIR.LabelDefault]

            if venvs:
                sandbox_settings['virtualenvs'] = ','.join(venvs)

        if sandbox_settings or 'SANDBOX' in environments:
            cfg.add_section('SANDBOX')

        for key in sandbox_settings:
            cfg.set('SANDBOX', key, str(sandbox_settings[key]))

        if 'SANDBOX' in environments:
            for val in environments['SANDBOX']:
                cfg.set('SANDBOX', val, str(environments['SANDBOX'][val]))

        for section_name in environments:
            if section_name == 'SANDBOX':
                continue
            section_name_pretty = 'ENV-%s' % section_name.upper()
            cfg.add_section(section_name_pretty)
            for val in environments[section_name]:
                cfg.set(section_name_pretty, val, str(environments[section_name][val]))

        with open(output_file_name, 'w') as f:
            cfg.write(f)

    @classmethod
    def _flowir_component_to_dict(cls, flowir_component):
        comp_dict = flowir_component.get('variables', {})

        comp_dict = copy.deepcopy(comp_dict)

        if 'references' in flowir_component:
            comp_dict['references'] = ' '.join(flowir_component['references'])

        extract_functions = [
            cls._comp_command_to_dict,
            cls._comp_executors_to_str,
            cls._comp_resource_manager_to_str,
            cls._comp_resource_request_to_dict,
            cls._comp_workflow_attributes_to_dict,
        ]

        for extract in extract_functions:
            extracted = extract(flowir_component)

            conflict = set(extracted.keys()).intersection(set(comp_dict.keys()))

            if conflict:
                logger.critical("Conflicting keys: %s" % conflict)

            assert bool(conflict) is False

            comp_dict.update(extracted)

        return comp_dict

    @classmethod
    def _dump_variables(cls, flowir, output_dir, is_instance, update_existing):
        # type: (DictFlowIR, str, bool, bool) -> None
        variables_dir = os.path.join(output_dir, 'variables.d')
        default_platform_path = os.path.join(output_dir, 'variables.conf')

        def platform_to_cfg(platform_name):
            global_blueprint = flowir.get(FlowIR.FieldBlueprint, {}) \
                .get(platform_name, {}) \
                .get(FlowIR.LabelGlobal, {}).copy()

            stages_blueprint = flowir.get(FlowIR.FieldBlueprint, {}) \
                .get(platform_name, {}) \
                .get(FlowIR.LabelStages, {}).copy()

            variables_for_global_flowir_fields = cls._flowir_component_to_dict(global_blueprint)

            global_variables = flowir.get(FlowIR.FieldVariables, {}) \
                .get(platform_name, {}) \
                .get(FlowIR.LabelGlobal, {}).copy()

            stage_variables = flowir.get(FlowIR.FieldVariables, {}) \
                .get(platform_name, {}) \
                .get(FlowIR.LabelStages, {}).copy()

            cfg = FlowConfigParser()
            global_variables.update(variables_for_global_flowir_fields)

            if global_variables:
                pretty_name = FlowIR.LabelGlobal.upper()
                cfg.add_section(pretty_name)

                for key in global_variables:
                    cfg.set(pretty_name, key, str(global_variables[key]))

            for stage_index in set(stage_variables).union(set(stages_blueprint)):
                stage_blueprint = stages_blueprint.get(stage_index, {})
                variables_for_stage_flowir_fields = cls._flowir_component_to_dict(stage_blueprint)
                this_stage = stage_variables.get(stage_index, {})
                this_stage.update(variables_for_stage_flowir_fields)

                pretty_name = 'STAGE%d'% stage_index
                cfg.add_section(pretty_name)
                for key in this_stage:
                    cfg.set(pretty_name, key, str(this_stage[key]))

            return cfg

        platform_files = dict([
            (platform, os.path.join(variables_dir, '%s.conf' % platform)) for platform in flowir[
                FlowIR.FieldPlatforms
            ]
        ])

        platform_files[FlowIR.LabelDefault] = default_platform_path

        for platform in platform_files:
            if update_existing is False and os.path.exists(platform_files[platform]):
                continue

            if is_instance:
                if os.path.exists(platform_files[platform]):
                    logger.log(19, "DOSINI instance dump will skip overwriting variables file %s"
                                % platform_files[platform])
                    continue
                else:
                    logger.info("DOSINI instance dump will generate missing variables file %s"
                                % platform_files[platform])

            cfg = platform_to_cfg(platform)
            logger.log(19, 'Generating variables file %s' % platform_files[platform])
            with open(platform_files[platform], 'w') as f:
                cfg.write(f)

    @classmethod
    def _dump_platforms(cls, flowir, output_dir, update_existing):
        # type: (DictFlowIR, str, bool) -> None
        # VV: TODO What about the [DEFAULT] section ? is it ok to spill DEFAULT variables into other sections ?
        platform_names = flowir[FlowIR.FieldPlatforms]

        for platform_name in platform_names:
            if platform_name == FlowIR.LabelDefault:
                continue

            output_file = os.path.join(output_dir, 'experiment.%s.conf' % platform_name)

            if update_existing is False and os.path.exists(output_file):
                continue
            
            cfg = FlowConfigParser()
            sandbox = {}

            if platform_name in flowir[FlowIR.FieldApplicationDependencies]:
                if flowir[FlowIR.FieldApplicationDependencies][platform_name]:
                    app_deps = ','.join(flowir[FlowIR.FieldApplicationDependencies][platform_name])
                    sandbox['application-dependencies'] = app_deps

            if platform_name in flowir[FlowIR.FieldVirtualEnvironments]:
                if flowir[FlowIR.FieldVirtualEnvironments][platform_name]:
                    venvs = ','.join(flowir[FlowIR.FieldVirtualEnvironments][platform_name])
                    sandbox['virtualenvs'] = venvs

            environments = flowir[FlowIR.FieldEnvironments][platform_name]  # type: Dict[str, Any]

            env_names = list(environments.keys())  # type: List[str]
            # VV: Make sure that sandbox ends up at the very top

            if 'SANDBOX' in env_names:
                assert False

            if sandbox:
                cfg.add_section('SANDBOX')
                for key in sandbox:
                    cfg.set('SANDBOX', key, str(sandbox[key]))

            for environment in env_names:
                pretty_name = 'ENV-%s' % environment.upper()

                cfg.add_section(pretty_name)
                for key in environments[environment]:
                    cfg.set(pretty_name, key, str(environments[environment][key]))

            with open(output_file, 'w') as f:
                cfg.write(f)

    @classmethod
    def _translate_dict_to_dict(
            cls,  # type: Type[Dosini]
            field,  # type: Dict[str, Any]
            required,  # type: Dict[str, Callable[[str, Any], Dict[str, str]]]
            optional,  # type: Dict[str, Callable[[str, Any], Dict[str, str]]]
    ):
        # type: (...) -> Dict[str, str]

        ret = {}

        field = {
            key: field[key] for key in field if field[key] is not None
        }

        def apply_conversion(conversion_function, key, value, store_in):
            # type: (Callable[[str, Any], Dict[str, str]], str, Any, Dict[str, str]) -> None
            # VV: filter out NONE keys
            converted = conversion_function(key, value)
            store_in.update(converted)

        for key in set(optional.keys()).intersection(set(field.keys())):
            value = field[key]
            apply_conversion(optional[key], key, value, ret)
            del field[key]

        for key in required:
            value = field[key]
            apply_conversion(required[key], key, value, ret)
            del field[key]

        return ret

    @classmethod
    def _comp_workflow_attributes_to_dict(cls, comp):
        # type: (DictFlowIRComponent) -> Dict[str, str]
        key = 'workflowAttributes'

        if key not in comp:
            return {}

        flat = cls._translate_dict_to_dict(
            comp[key],
            required={
            },
            optional={
                'shutdownOn': lambda key, value: {'shutdown-on': ' '.join(value)},
                'restartHookOn': lambda key, value: {'restart-hook-on': ' '.join(value)},
                'restartHookFile': lambda key, value: {'restart-hook-file': value},
                'repeatRetries': lambda key, value: {key: str(value)},
                'maxRestarts': lambda key, value: ({'max-restarts': str(value)} if value is not None else {}),
                'replicate': lambda key, value: {key: str(value)},
                'aggregate': lambda key, value: {key: str(value).lower()},
                'repeatInterval': lambda key, value: {'repeat-interval': str(value)},
                'isMigratable': lambda key, value: {key: str(value).lower()},
            }
        )

        optimizer = comp['workflowAttributes'].get('optimizer', {})
        flat.update(
            cls._translate_dict_to_dict(
                optimizer,
                required={
                },
                optional={
                    'disable': lambda key, value: {'optimizerDisable': str(value).lower()},
                    'exploitChance': lambda key, value: {'optimizerExploitChance': str(value)},
                    'exploitTarget': lambda key, value: {'optimizerExploitTarget': str(value)},
                    'exploitTargetLow': lambda key, value: {'optimizerExploitTargetLow': str(value)},
                    'exploitTargetHigh': lambda key, value: {'optimizerExploitTargetHigh': str(value)},
                }
            )
        )

        flat.update(
            cls._translate_dict_to_dict(
                comp['workflowAttributes'].get('memoization', {}).get('disable', {}),
                required={},
                optional={
                    'strong': lambda key, value: {'memoization-disable-strong': str(value).lower()},
                    'fuzzy': lambda key, value: {'memoization-disable-fuzzy': str(value).lower()},}))
        flat.update(
            cls._translate_dict_to_dict(
                comp['workflowAttributes'].get('memoization', {}),
                required={},
                optional={'embeddingFunction': lambda key, value: {'memoization-embedding-function': value},}))

        return flat


    @classmethod
    def _comp_resource_request_to_dict(cls, comp):
        # type: (DictFlowIRComponent) -> Dict[str, str]
        def str_to_str(key, value):
            # type: (str, str) -> Dict[str, str]
            return {key: value}

        def int_to_str_pair(key, value):
            # type: (str, int) -> Dict[str, str]
            return {key: str(value)}

        try:
            resource_request = comp['resourceRequest']
        except KeyError:
            return {}

        return cls._translate_dict_to_dict(
            resource_request,
            required={
            },
            optional={
                'numberProcesses': int_to_str_pair,
                'numberThreads': int_to_str_pair,
                'ranksPerNode': int_to_str_pair,
                'threadsPerCore': int_to_str_pair,
                'memory': str_to_str,
            }
        )

    @classmethod
    def _comp_resource_manager_to_str(cls, comp):
        # type: (DictFlowIRComponent) -> Dict[str, str]
        translate_map = {
            'backend': 'job-type',
            'image': 'k8s-image',
            'image-pull-secret': 'k8s-image-pull-secret',
            'api-key-var': 'k8s-api-key-var',
            'cpuUnitsPerCore': 'k8s-cpu-units-per-core',
            'namespace': 'k8s-namespace',
            'host': 'k8s-host',
            'dockerOptions': 'lsf-docker-options',
            'dockerImage': 'lsf-docker-image',
            'dockerProfileApp': 'lsf-docker-profile-app',
            'gracePeriod': 'k8s-grace-period',
        }

        def str_to_str(key, value):
            # type: (str, str) -> Dict[str, str]
            try:
                key = translate_map[key]
            except KeyError:
                pass
            return {key: value}

        def float_to_str(key, value):
            # type: (str, float) -> Dict[str, str]
            try:
                key = translate_map[key]
            except KeyError:
                pass

            return {key: str(value)}

        def int_to_str(key, value):
            # type: (str, float) -> Dict[str, str]
            try:
                key = translate_map[key]
            except KeyError:
                pass

            return {key: str(value)}

        groups = ['config', 'lsf', 'kubernetes']
        required = {
            'config': {},
            'lsf': {},
            'kubernetes': {},
        }

        optional = {
            'config': {
                'backend': str_to_str,
                'walltime': float_to_str,
            },
            'lsf': {
                'queue': str_to_str,
                'reservation': str_to_str,
                'resourceString': str_to_str,
                'statusRequestInterval': float_to_str,
                'dockerProfileApp': str_to_str,
                'dockerOptions': str_to_str,
                'dockerImage': str_to_str,

            },
            'kubernetes': {
                'image': str_to_str,
                'image-pull-secret': str_to_str,
                'api-key-var': str_to_str,
                'host': str_to_str,
                'namespace': str_to_str,
                'cpuUnitsPerCore': str_to_str,
                'gracePeriod': int_to_str,
            }
        }

        flat_dict = {}

        errors = []

        resource_manager = comp.get('resourceManager', {})

        for group in set(groups).intersection(set(resource_manager)):
            group_dict = cls._translate_dict_to_dict(
                resource_manager[group],
                required=required[group],
                optional=optional[group]
            )

            # VV: Ensure that no 2 options have the SAME name
            conflicts = set(group_dict).intersection(set(flat_dict))

            if conflicts:
                errors.append((
                    group,
                    conflicts,
                    { key: group_dict[key] for key in conflicts},
                    {key: flat_dict[key] for key in conflicts},
                ))

            flat_dict.update(group_dict)

        if errors:
            raise Exception('Cannot dump resourceManager to disk because of conflicts! %s' % str(errors))

        return flat_dict

    @classmethod
    def _comp_executors_to_str(cls, comp):
        # type: (DictFlowIRComponent) -> Dict[str, str]
        if 'executors' not in comp:
            return {}

        executors = comp['executors']

        ret = {}

        try:
            ret['rstage-in'] = [ex for ex in executors['pre'] if ex.get('name') == 'lsf-dm-in'][0]['payload']
        except (KeyError, IndexError) as e:
            pass

        try:
            ret['rstage-out'] = [ex for ex in executors['post'] if ex.get('name') == 'lsf-dm-out'][0]['payload']
        except (KeyError, IndexError) as e:
            pass

        try:
            main = [ex for ex in executors['main'] if ex.get('name') == 'docker'][0]
            # VV: Keep everything but the `name` key because that's automatically generated by Flow
            ret.update({
                key: main[key] for key in main if key != 'name'
            })
        except (KeyError, IndexError) as e:
            pass

        return ret

    @classmethod
    def _comp_command_to_dict(cls, comp):
        # type: (DictFlowIRComponent) -> Dict[str, str]
        def str_to_str(key, value):
            # type: (str, str) -> Dict[str, str]
            return {key: value}

        def bool_to_str(key, value):
            # type: (str, bool) -> Dict[str, str]
            return {key: str(value).lower()}

        key = 'command'

        if key not in comp:
            return {}

        return cls._translate_dict_to_dict(
            comp[key],
            required={
            },
            optional={
                'arguments': str_to_str,
                'environment': str_to_str,
                'resolvePath': bool_to_str,
                'executable': str_to_str,
                'interpreter': str_to_str,
                'expandArguments': str_to_str,
            },
        )

    @classmethod
    def configuration_for_stage(
            cls,  # type: Type[Dosini]
            flowir,  # type: DictFlowIR
            stage_index,  # type: int
            stage_variables=None,  # type: Optional[Dict[str, str]]
            stage_components=None,  # type: Optional[List[DictFlowIRComponent]]
            is_instance=False  # type: False
    ):
        # type: (...) -> FlowConfigParser
        if stage_components is None:
            stage_components = []  # type: List[DictFlowIRComponent]

            components = flowir[FlowIR.FieldComponents]  # type: List[DictFlowIRComponent]

            for comp in components:
                if comp['stage'] == stage_index:
                    stage_components.append(comp)

        if stage_variables is None:
            try:
                stage_variables = cast(
                    Dict[int, Dict[str, Any]],
                    flowir[FlowIR.FieldVariables][FlowIR.LabelDefault][FlowIR.LabelStages]
                )[stage_index]
            except KeyError:
                stage_variables = {}
        cfg = FlowConfigParser()

        type_label = {
            True: 'Instance',
            False: 'Package'
        }[is_instance]

        logger.log(19, "Generating %s stage %d" % (type_label, stage_index))

        global_variables = flowir[FlowIR.FieldVariables].get(FlowIR.LabelDefault, {}).get(FlowIR.LabelGlobal, {})
        logger.debug('Global variables: %s' % pprint.pformat(global_variables))
        logger.debug('Stage variables: %s' % pprint.pformat(stage_variables))

        if is_instance:
            store_variables = copy.deepcopy(global_variables)
            store_variables.update(stage_variables)
        else:
            store_variables = stage_variables

        if store_variables:
            cfg.add_section(STAGE_META_SECTION)

            for variable in store_variables:
                cfg.set(STAGE_META_SECTION, variable, str(store_variables[variable]))

        for comp in stage_components:
            name = comp['name']
            cfg.add_section(name)

            try:
                comp_dict = cls._flowir_component_to_dict(comp)
            except:
                logger.critical("Error when dumping stage%d.%s --> %s" % (
                    comp['stage'], comp['name'], pretty_json(comp)
                ))
                raise

            for key in comp_dict:
                if comp_dict[key] is not None:
                    cfg.set(name, key, str(comp_dict[key]))

        return cfg

    @classmethod
    def _dump_components(cls, flowir, output_dir, is_instance=False, update_existing=True):
        # type: (DictFlowIR, str, bool, bool) -> None
        # VV: Tag:@DOSINI-Components@
        stage_components = {}  # type: Dict[int, List[DictFlowIRComponent]]

        components = flowir[FlowIR.FieldComponents]  # type: List[DictFlowIRComponent]

        for comp in components:
            if comp['stage'] not in stage_components:
                stage_components[comp['stage']] = []

            stage_components[comp['stage']].append(comp)

        try:
            stage_variables = cast(
                Dict[int, Dict[str, Any]],
                flowir[FlowIR.FieldVariables][FlowIR.LabelDefault][FlowIR.LabelStages]
            )
        except KeyError:
            stage_variables = {}

        try:
            global_variables = cast(
                Dict[int, Dict[str, Any]],
                flowir[FlowIR.FieldVariables][FlowIR.LabelDefault][FlowIR.LabelGlobal]
            )
        except KeyError:
            global_variables = {}

        for stage_index in sorted(stage_components):
            if is_instance:
                stage_path = os.path.join(output_dir, 'stages.d', 'stage%d.instance.conf' % stage_index)
            else:
                stage_path = os.path.join(output_dir, 'stages.d', 'stage%d.conf' % stage_index)

            if update_existing is False and os.path.exists(stage_path):
                continue

            if is_instance:
                this_stage_variables = global_variables.copy()
            else:
                this_stage_variables = {}

            this_stage_variables.update(stage_variables.get(stage_index, {}))

            cfg = cls.configuration_for_stage(
                flowir, stage_index,
                stage_variables=this_stage_variables,
                stage_components=stage_components.get(stage_index, None),
                is_instance=is_instance
            )

            with open(stage_path, 'w') as f:
                cfg.write(f)
