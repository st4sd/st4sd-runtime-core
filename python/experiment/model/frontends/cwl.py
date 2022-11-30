#
# coding=UTF-8
# Copyright IBM Inc. 2019 All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis

from __future__ import print_function

import argparse
import copy
import hashlib
import json
import logging
import os
import pprint
import re
import sys
try:
    import urllib.request, urllib.parse, urllib.error
except ImportError:
    import urllib

from collections import OrderedDict
from typing import IO, Any, Dict, List, Optional, Set, Tuple, Union, cast

import yaml
from networkx import DiGraph, topological_sort
from six import string_types

import cwltool.load_tool as load
import cwltool.loghandler
from cwltool.argparser import get_default_args
from cwltool.command_line_tool import CommandLineTool
from cwltool.context import LoadingContext, RuntimeContext, getdefault
from cwltool.main import load_job_order
from cwltool.process import Process
from cwltool.resolver import tool_resolver
from cwltool.workflow import Workflow, WorkflowStep, default_make_tool

from experiment.model.frontends.flowir import (DictFlowIR, DictFlowIRComponent, FlowIR,
                                               FlowIRConcrete)


class RejectFilter(logging.Filter):
    def filter(self, record):
        return False


# VV: Reduce amount of CWLtool logs
cwltool.loghandler._logger.addFilter(RejectFilter())
cwltool.loghandler._logger.setLevel(10000)


IDCwl = str
IDFlow = str
IDVariable = str

moduleLogger = logging.getLogger('frontend_cwl')


class UnsupportedCWLRequirement(Exception):
    def __init__(self, requirement_class, requested_by_uri):
        #  type: (str, str) -> None
        self.requirement_class = requirement_class
        self.requested_by = requested_by_uri

        self.message = '%s requested unsupported requirement %s' % (
            self.requested_by, self.requirement_class
        )

    def __str__(self):
        return self.message


def convert_to_str(obj):
    if isinstance(obj, string_types):
        return str(obj)
    elif isinstance(obj, list):
        return list(map(convert_to_str, obj))
    elif isinstance(obj, dict):
        return {convert_to_str(key): convert_to_str(obj[key]) for key in obj}
    return obj


def convert_schema(schema):
    # VV: Utility to convert OrderedDict to plain Dict
    if isinstance(schema, string_types):
        return schema
    elif isinstance(schema, dict):
        return {
            key: convert_schema(schema[key]) for key in schema
        }
    elif isinstance(schema, list):
        return list(map(convert_schema, schema))
    else:
        return schema


def preprocess_and_validate_flow_dependency_file(location):
    # type: (str) -> Dict[str, Any]
    """

    VV: Schema:

    envDef: Union[List[Dict[str, str]], Dict[str, str]] ( will be converted to List[Dist[str, str]]

    applications: List[str] # Flow-style (i.e `<name>.application`) string

    copy: Dict[str(dest), str(source)]  # Copy source to data/dest where dest is a path and source a location.
                                        # source is either an absolute path or a path relative to @location

    """
    moduleLogger.info("Loading flow dependency file from %s" % location)

    if location.startswith('file://'):
        file_path = location[7:]
        with open(file_path, 'rt') as f:
            dep = yaml.safe_load(f)
    elif location.startswith('http://') or location.startswith('https://'):
        fetch_file = urllib.request.urlopen(location).read()
        dep = yaml.safe_load(fetch_file)
    else:
        raise Exception(
            "Don't know how to fetch description from %s "
            "(supported protocols: [https://, http://, file:// "
            "(currently does not support `host` field)]" % location
        )

    ret = {}

    def preprocess_applications(applications):
        assert isinstance(applications, list) and isinstance(applications, str) is False

        errors = [e for e in applications if (
                    (isinstance(e, str) is False)
                    or (e.endswith('.application') is False)
                    or ('/' in e)
            )]

        if errors:
            raise Exception(
                'Invalid applications field for %s (errors: %s)' % (
                    location, dumps_json(errors)
                )
            )

        return applications

    def preprocess_env_def(env_def):
        pattern = re.compile('[a-zA-Z_][a-zA-Z0-9_]*')

        # VV: Convert a dict to a 1-entry list of dicts
        if isinstance(env_def, dict):
            env_def = [env_def]

        errors = []

        for group in env_def:
            for key in group:
                if isinstance(key, str) is False:
                    errors.append(
                        'Invalid key: %s (not a string) (accepted pattern: [a-zA-Z_][a-zA-Z0-9_]*)' % key
                    )
                elif pattern.match(key) is False:
                    errors.append(
                        'Invalid key: %s (invalid characters) (accepted pattern: [a-zA-Z_][a-zA-Z0-9_]*)' % key
                    )
                if isinstance(group[key], (int, str, float)) is False:
                    errors.append(
                        'Invalid value for %s=%s (expected int, str, float)' % (
                            key, dumps_json(group[key])
                        )
                    )

        return env_def

    fn_preprocess = {
        'applications': preprocess_applications,
        'envDef': preprocess_env_def,
    }

    moduleLogger.info("Loaded dependency:\n%s" % dumps_json(dep))

    for key in dep:
        ret[key] = fn_preprocess[key](dep[key])

    return ret


class ZWorkflow(object):
    def __init__(
            self,
            flow_uid,  # type: str
            cwl_id,  # type: str
            tool_blueprint,  # type: Workflow
            dynamic_vars,  # type: Dict[str, str]
            hints,  # type: List[Dict[str, Any]]
            requirements,  # type: List[Dict[str, Any]]
            input_schemas=None,  # type: Optional[List[Dict[str, Any]]]
            output_schemas=None,  # type: Optional[List[Dict[str, Any]]]
            replica_idx=None,  # type: Optional[int]
    ):
        input_schemas = input_schemas or []
        output_schemas = output_schemas or []

        input_schemas = convert_schema(input_schemas)
        output_schemas = convert_schema(output_schemas)

        self.tool_blueprint = tool_blueprint
        self.flow_uid = flow_uid
        self.cwl_id = cwl_id
        self.dynamic_vars = dynamic_vars

        self.hints = hints
        self.requirements = requirements
        self.input_schemas = input_schemas
        self.output_schemas = output_schemas
        self.replica_idx = replica_idx


class ZWorkflowStep(object):
    pass


def dump_json(entry, fp):
    json.dump(entry, fp, sort_keys=True, indent=4, separators=(',', ': '))


def dumps_json(entry):
    return json.dumps(entry, sort_keys=True, indent=4, separators=(',', ': '))


def unroll_cwl_env_def(description):
    # type: (List[Tuple[List[Dict[str, str]], Dict[str, str]]]) -> (List[Dict[str, str]])

    # VV: the CWL preprocessor turns list of Dictionaries into lists of lists of dictionaries
    #     the dictionaries contain 2 keys: envName and envValue

    actual_description = []
    try:
        # VV: CWL EnvVarRequirement is NOT a list of dicts, explicitly convert description
        unrolled_group = {}
        for group in description:
            unrolled_group.clear()
            group = cast(List[List[Dict[str, str]]], group)
            for pair in group:
                unrolled_group[str(pair['envName'])] = str(pair['envValue'])

            actual_description.append(unrolled_group.copy())
    except TypeError:
        actual_description = []
        for group in description:
            unrolled_group = dict(group)
            actual_description.append(unrolled_group)

    return actual_description


def replace_env_var(string, pattern, value):
    # VV: re.finditer() returns matches in the order found when scanning the string from left to right
    #     apply the replace() instructions using the reverse order so that the indices reported by
    #     finditer() are correct
    iter = list(re.compile(pattern).finditer(string)) or []
    for i in reversed(iter):
        string = '%s%s%s' % (
            string[:i.start()], value, string[i.end():]
        )
    return string


def extract_refs_to_env_var(string):
    refs = set()

    pattern = re.compile('\$\w+')
    for match in pattern.finditer(string):
        refs.add(match.group()[1:])

    pattern = re.compile('\${\w+}')
    for match in pattern.finditer(string):
        refs.add(match.group()[2:-1])

    return list(refs)


def apply_env_vars_to_string(string, vault):
    env_var_refs = extract_refs_to_env_var(string)
    found = set()

    for ref in env_var_refs:
        for group in vault:
            if ref in group:
                found.add(ref)
                break

    missing = [e for e in env_var_refs if e not in found]

    if missing:
        moduleLogger.critical(
            '%s references unknown environment variables %s' % (
                string, missing
            ))

    # VV: All references are present in the Vault which means it's safe to blindly
    #     replace them with their expanded value as long as we make sure to apply them
    #     from the longest to the shortest (so that it's safe to use env-vars whose name
    #     is the beginning of some other env var's name)
    env_var_refs = sorted(env_var_refs, key=lambda e: len(e), reverse=True)

    if isinstance(vault, dict):
        vault = [vault]

    for ref in env_var_refs:
        for group in vault:
            if ref in group:
                string = replace_env_var(string, r'\$%s\b' % ref, group[ref])
                string = replace_env_var(string, r'\${%s}\b' % ref, group[ref])

    return string


def apply_hint_and_validate(
        hint_name,  # type: str
        valid_keys,  # type: List[str]
        required_keys,  # type: List[str]
        parse_into,  # type: Dict[str, Any]
        hint,  # type: Dict[str, Any]
        flow_uid,  # type: str
):
    parse_into.update(hint)
    del parse_into['class']
    invalid_keys = [key for key in parse_into if key not in valid_keys]
    invalid_keys.extend([key for key in required_keys if key not in hint])

    if invalid_keys:
        raise Exception(
            'Invalid/missing %s keys: %s for %s' % (
                hint_name, invalid_keys, flow_uid
            )
        )


def hash_uri(uri):
    # type: (str) -> str
    md5 = hashlib.md5()
    if isinstance(uri, str):
        uri = uri.encode()
    md5.update(uri)
    return str(md5.hexdigest())


def hash_environment(
        description,  # type: List[Dict[str, str]]
):  # type: (...) -> Tuple[Tuple[str, str], ...]
    aggregated = []

    for group in description:
        group = dict(group)
        ordered_keys = sorted(group.keys())
        aggregated.extend([(e, group[e]) for e in ordered_keys])

    return tuple(aggregated)


def split_cwl_output_uid(uid):
    # type: (str) -> Optional[Tuple[str, str]]
    id_begin = uid.rfind('#')
    assert id_begin >= 0

    output_begin = uid.rfind('/', id_begin + 1)

    if output_begin != -1:
        return uid[0:id_begin], uid[id_begin + 1:]

    # VV: This is not a cwl-uid for a tool output
    return None


def override_dict(
        child,  # type: List[Dict[str, str]]
        parent,  # type: List[Dict[str, str]]
        key_name='class',  # type: str
        keys_allow_many=None,  # type: List[str]
        normalize_key_names=True,
):  # type: (...) -> List[Dict[str, str]]
    """Parent overrides entries that are defined in the Child or insert new ones for entries that don't exist.

    A Parent's entry is considered to exist in the Child list if the child contains an entry which shares the
    same @key_name with the Parent's entry.

    If entries are expected to contain multiple instances with the same @key_name then any matching Parent
    entries are appended to the END of the returned list
    """
    if not parent:
        return child

    if not child:
        return parent

    keys_allow_many = keys_allow_many or []

    child_map = {}
    ret = []

    # VV: Allow keys to be declared multiple times (while preserving their order) if their name is
    #     within keys_allow_may
    #     For keys which cannot exist multiple times, each subsequent occurence patches (dict.update() behaviour)
    #     the resulting_hit[key_name] entry
    for entry in child:
        key = entry[key_name]

        if normalize_key_names and (key.startswith('file://') or key.startswith('http://')):
            key = os.path.split(key)[1]
            entry = entry.copy()
            entry[key_name] = key

        if key in keys_allow_many:
            ret.append(entry)
        elif key in child_map:
            child_map[key].update(entry)
        else:
            child_map[key] = entry

    ret.extend(list(child_map.values()))

    # VV: Parent entries overwrite those of the children
    #    (Consumers overwrite the hints/requirements of their Producers because it is the Consumers who instantiate
    #    the Producers)
    for entry in parent:
        key = entry[key_name]

        if normalize_key_names and (key.startswith('file://') or key.startswith('http://')):
            key = os.path.split(key)[1]
            entry = entry.copy()
            entry[key_name] = key

        if key in child_map:
            child_map[key].clear()
            child_map[key].update(entry)
        else:
            ret.append(entry)

    return ret


def sort_workflow_steps(workflow):
    # type: (Workflow) -> List[DependencyInfo]
    fake_dependencies = {}  # type: Dict[str, DependencyInfo]
    ordered_steps = []  # type: List[DependencyInfo]

    # VV: Use DependencyInfo to figure out in which order to process the steps
    for step in workflow.steps:
        fakedep = DependencyInfo(uid=step.tool['id'], instance=step)
        fake_dependencies[step.tool['id']] = fakedep

    for step in workflow.steps:
        # VV: Filter out inputs whose sources are not some other component
        #     that's defined under this workflow

        filtered = [p for p in step.inputs_record_schema['fields'] if 'source' in p]
        sources = [p['source'] for p in filtered]
        sources = list(filter(split_cwl_output_uid, sources))

        producer_outputs = [p.rsplit('/', 1)[0] for p in sources]
        producer_depinfo = [fake_dependencies[p] for p in producer_outputs]  # type: List[DependencyInfo]

        fakedep = fake_dependencies[step.tool['id']]

        for prod in producer_depinfo:
            fakedep.new_producer(prod)

    groups = DependencyInfo.compute_schedule(set(fake_dependencies.values()))

    for g in groups:
        ordered_steps.extend(list(g))

    return ordered_steps


class RomanNumeral:
    # VV: http://code.activestate.com/recipes/81611-roman-numerals/
    numeral_map = tuple(zip(
        (1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1),
        ('M', 'CM', 'D', 'CD', 'C', 'XC', 'L', 'XL', 'X', 'IX', 'V', 'IV', 'I')
    ))

    @staticmethod
    def int_to_roman(i):
        result = []
        for integer, numeral in RomanNumeral.numeral_map:
            count = i // integer
            result.append(numeral * count)
            i -= integer * count
        return ''.join(result)

    @staticmethod
    def roman_to_int(n):
        i = result = 0
        for integer, numeral in RomanNumeral.numeral_map:
            while n[i:i + len(numeral)] == numeral:
                result += integer
                i += len(numeral)
        return result


class OutputInfo(object):
    def __init__(
            self,
            output_name,  # type: str
            output_record_schema,  # type: Dict[str, Any]
            tool_instance,  # type: Optional[ZCommandLineTool]
    ):
        self.output_name = output_name
        self.output_record_schema = output_record_schema
        self.instance = tool_instance

    @property
    def producer_uid(self):
        # type: () -> IDFlow
        return self.instance.flow_uid


class DependencyInfo(object):
    """Keeps track of dependency information for a single Component.

    VV: @tag FlowLikeDependency

    I like to think that this class implicitly builds 4 graphs:
       Producer to Consumer
       Consumer to Producer
       Subject to Observer (Source engine to Repeating engine)
       Observer to Subject (Repeating engine to Source engine)

       Rules:
         1: A Consumer MUST be scheduled AFTER its producerS TERMINATES
         2: An Observer SHOULD be scheduled AFTER its subjectS START-EXECUTING

       If the Workflow engine does not support the Observer mechanism it falls back to
       treating Subjects as Producers and Observers as Consumers.
         - I.E When converting from FLowIR to CWL insert HINTS that Observers are
               observing and let the Workflow execution Engine figure out what to
               do with this information.

    These graphs can be used to partition the components into groups of Components where
    each group contains components that should be Scheduled in parallel.
    """

    def __init__(self,
                 uid,  # type: str
                 instance,  # type: ZCommandLineTool
                 producers=None,  # type: Set[DependencyInfo]
                 subjects=None,  # type: Set[DependencyInfo]
                 consumers=None,  # type: Set[DependencyInfo]
                 observers=None  # type: Set[DependencyInfo]
                 ):
        self.flow_uid = uid
        self.instance = instance
        self.producers = producers or set()
        self.subjects = subjects or set()
        self.consumers = consumers or set()
        self.observers = observers or set()

        for producer in self.producers:
            producer.new_consumer(self)

        for subject in self.subjects:
            subject.new_observer(self)

    def new_consumer(
            self,
            consumer,  # type: DependencyInfo
    ):
        if consumer not in self.consumers:
            self.consumers.add(consumer)

        if self not in consumer.producers:
            consumer.new_producer(self)

        moduleLogger.debug("Register Consumer %s to %s" % (
            consumer.flow_uid, self.flow_uid
        ))

    def new_observer(
            self,
            observer,  # type: DependencyInfo
    ):
        if observer not in self.observers:
            self.observers.add(observer)

        if self not in observer.subjects:
            observer.new_subject(self)

        if observer in self.consumers:
            moduleLogger.debug("Promoted consumer %s of %s to Observer" % (
                observer.flow_uid, self.flow_uid
            ))
            self.consumers.remove(observer)

        if self in observer.producers:
            moduleLogger.debug("Promoted producer %s of %s to Subject" % (
                observer.flow_uid, self.flow_uid
            ))
            observer.producers.remove(self)

        moduleLogger.debug("Register Observer %s to %s" % (
            observer.flow_uid, self.flow_uid
        ))

    def new_producer(
            self,
            producer,  # type: DependencyInfo
    ):
        if producer not in self.producers:
            self.producers.add(producer)

        if self not in producer.consumers:
            producer.new_consumer(self)

        moduleLogger.debug("Register Producer %s to %s" % (
            producer.flow_uid, self.flow_uid
        ))

    def new_subject(
            self,
            subject,  # type: DependencyInfo
    ):
        if subject not in self.subjects:
            self.subjects.add(subject)

        if self not in subject.observers:
            subject.new_observer(self)

        if subject in self.producers:
            moduleLogger.debug("Promoted producer %s of %s to Subject" % (
                subject.flow_uid, self.flow_uid
            ))
            self.producers.remove(subject)

        if self in subject.consumers:
            moduleLogger.debug("Promoted consumer %s of %s to Observer" % (
                self.flow_uid, subject.flow_uid
            ))
            subject.consumers.remove(self)

        moduleLogger.debug("Register Subject %s to %s" % (
            subject.flow_uid, self.flow_uid
        ))

    @classmethod
    def compute_schedule(cls, components):
        # type: (Set[DependencyInfo]) -> List[Set[DependencyInfo]]
        """Partition a list of components into ordered groups of Components which can be executed concurrently.
        """
        ret = []  # type: List[Set[DependencyInfo]]

        pending = list(components)  # type: List[DependencyInfo]
        # VV: Contains uids of scheduled components
        scheduled = set()  # type: Set[str]

        # VV: Follow a basic 2-phase approach. For each group, first determine which components are safe to
        #     execute because all of their Producer->Consumer dependencies are satisfied
        #     In a second discover those Components whose Obsever->Subject dependencies are met (that's safe to
        #     do because all ready-to-execute components from the current group will have been considered
        #     ready to schedule).
        #     The second step needs to be repeated till no Component is considered ready
        while pending:
            group = set()  # type: Set[DependencyInfo]
            for comp in pending[:]:
                # VV: Only inspect non-repeating components
                if not comp.subjects:
                    unmet = [e for e in comp.producers if e.flow_uid not in scheduled]
                    if not unmet:
                        group.add(comp)
                        pending.remove(comp)

            scheduled.update([e.flow_uid for e in group])

            dirty = True

            while dirty and pending:
                dirty = False
                for comp in pending:
                    # VV: Make sure that there's at least 1 Subject->Consumer dependency
                    if comp.subjects:
                        # VV: Check that ALL dependencies are met
                        unmet = [e for e in comp.producers.union(comp.subjects) if e.flow_uid not in scheduled]
                        if not unmet:
                            scheduled.add(comp.flow_uid)
                            group.add(comp)
                            pending.remove(comp)
                            dirty = True

            assert group

            ret.append(group)

        return ret


class ZCommandLineTool(object):
    def __init__(
            self,  # type: ZCommandLineTool
            flow_uid,  # type: str
            cwl_id,  # type: str
            tool_blueprint,  # type: CommandLineTool
            dynamic_vars,  # type: Dict[str, str]
            hints,  # type: List[Dict[str, Any]]
            requirements,  # type: List[Dict[str, Any]]
            dependency_info,  # type: DependencyInfo
            component_outputs,  # type: Set[OutputInfo]
            stage_index=None,  # type: Optional[int]
            component_name=None,  # type: Optional[str]
            repeat_interval=None,  # type: Optional[float]
            replica_idx=None,  # type: Optional[int]
            input_schemas=None,  # type: Optional[List[Dict[str, Any]]]
            output_schemas=None,  # type: Optional[List[Dict[str, Any]]]
            step_name=None
    ):
        input_schemas = input_schemas or []
        output_schemas = output_schemas or []

        input_schemas = convert_schema(input_schemas)
        output_schemas = convert_schema(output_schemas)

        self.tool_blueprint = tool_blueprint
        self.flow_uid = flow_uid
        self.cwl_id = cwl_id
        self.dynamic_vars = {
            key: dynamic_vars[key] for key in dynamic_vars if dynamic_vars[key] is not None
        }
        self.dependency_info = dependency_info
        self.hints = hints
        self.requirements = requirements
        self.input_schemas = input_schemas
        self.output_schemas = output_schemas
        self.arguments = self.tool_blueprint.metadata.get('arguments', [])  # type: List[Dict[str, Any]]

        # VV: output_name -> OutputInfo map (this ZCommandLineTool produces these outputs)
        self.component_outputs = component_outputs

        self.stage_index = stage_index
        self.component_name = component_name

        self.repeat_interval = repeat_interval

        self.resource_request = {}
        self.resource_manager = {}

        self.replica_idx = replica_idx
        self.stage_in = {}

        self.variables = { }

        self.config_kubernetes = None
        self.resolve_path = True
        
        self.step_name = step_name

        for requirement in requirements:
            if 'class' not in requirement:
                continue

            class_type = requirement['class']

            if class_type.startswith('file://') or class_type.startswith('http://'):
                class_type = os.path.split(class_type)[1]

            if class_type not in Cwl.supported_requirements:
                moduleLogger.info("Unsupported CommandLineTool.requirement %s" % requirement)
                continue

            if class_type == Cwl.featureInitialWorkDir:
                for entry in requirement['listing']:
                    entry = cast(str, entry)

                    if entry.startswith('$(') is False or entry.endswith(')') is False:
                        raise Exception(
                            'Only support listings which contain SIMPLE expressions $(...) which do not '
                            'reference arrays but got %s' % (entry)
                        )
                    # VV: Throw away "$(" prefix and ")" suffix
                    input_name = entry[2:-1]
                    if input_name.startswith('inputs.') is False:
                        raise Exception(
                            'Only support listings which contain simple expressions $(inputs. ..) which do not '
                            'reference arrays but got an expression of $("%s")' % input_name
                        )
                    # VV: Trim 'inputs.' prefix
                    input_name = input_name[7:]

                    if '[' in input_name or '.' in input_name:
                        raise Exception(
                            'Only support listings which contain simple expressions $(inputs. ..) which do not '
                            'reference arrays but got an expression of $(inputs."%s")' % input_name
                        )

                    if input_name in self.dynamic_vars:
                        assert input_name not in self.stage_in

                        self.stage_in[input_name] = {
                            'input-name': input_name,
                            'var-uid': self.dynamic_vars[input_name]
                        }

        for hint in hints:
            if 'class' not in hint:
                continue
            class_type = hint['class']  # type: str

            if class_type.startswith('file://') or class_type.startswith('http://'):
                class_type = os.path.split(class_type)[1]

            if class_type not in Cwl.supported_hints:
                moduleLogger.info("Unsupported CommandLineTool.hint %s" % hint)
                continue

            if class_type == Cwl.extResolvePath:
                self.resolve_path = hint.get('resolve', True)
            elif class_type == Cwl.extDisableCheckProducerOutput:
                self.variables['check-producer-output'] = False
            elif class_type == Cwl.extKubernetes:
                if 'kubernetes' not in self.resource_manager:
                    self.resource_manager['kubernetes'] = {}

                apply_hint_and_validate(
                    Cwl.extKubernetes,
                    # VV: Valid keys
                    ['image', 'image-pull-secret', 'namespace', 'api-key-var', 'host'],
                    # VV: Required keys
                    [],
                    self.resource_manager['kubernetes'], hint, self.flow_uid
                )
                if 'config' not in self.resource_manager:
                    self.resource_manager['config'] = {}

                self.resource_manager['config']['backend'] = 'kubernetes'

            elif class_type == Cwl.extShutdownOn:
                parsed = {}
                apply_hint_and_validate(
                    Cwl.extShutdownOn,
                    ['codes'],
                    ['codes'],
                    parsed, hint, self.flow_uid
                )
                self.variables['shutdown-on'] = ' '.join(parsed['codes'])
            elif class_type == Cwl.extLSFBackend:
                lsf_vars = {}
                lsf_keys = ['queue', 'resourceString', 'statusRequestInterval', 'dockerImage', 'dockerProfileApp']
                config_keys = ['walltime']
                apply_hint_and_validate(
                    Cwl.extLSFBackend,
                    # VV: Valid keys
                    config_keys + lsf_keys,
                    # VV: Required keys
                    [],
                    lsf_vars, hint, self.flow_uid
                )
                if 'lsf' not in self.resource_manager:
                    self.resource_manager['lsf'] = {}

                if 'config' not in self.resource_manager:
                    self.resource_manager['config'] = {}

                self.resource_manager['config']['backend'] = 'lsf'

                for key in lsf_keys:
                    if key in lsf_vars:
                        self.resource_manager['lsf'][key] = lsf_vars[key]
                if 'resourceString' in lsf_vars:
                    self.resource_manager['lsf']['resourceString'] = lsf_vars['resourceString']
            elif class_type == Cwl.extResourceManager:
                if 'config' not in self.resource_manager:
                    self.resource_manager['config'] = {}

                apply_hint_and_validate(
                    Cwl.extResourceManager,
                    # VV: Valid keys
                    ['walltime', 'backend'],
                    # VV: Required keys
                    [],
                    self.resource_manager['config'], hint, self.flow_uid
                )
            elif class_type == Cwl.extResourceRequest:
                apply_hint_and_validate(
                    Cwl.extResourceManager,
                    # VV: Valid keys
                    ['numberProcesses', 'numberThreads', 'ranksPerNode', 'threadsPerCore'],
                    # VV: Required keys
                    [],
                    self.resource_request, hint, self.flow_uid
                )

    @property
    def is_repeat(self):
        return self.repeat_interval is not None and self.repeat_interval > 0.0

    @property
    def resolved_reference(self):
        return 'stage%d.%s' % (self.stage_index, self.component_name)

    def preprocess_cwlcmdline(
            self,  # type: ZCommandLineTool
            cwl,  # type: Cwl
    ):
        # type: (...) -> Dict[str, Any]
        """Generate arguments to `interpreter_cwl_commandline.py` (cwlcmdline interpreter).

        Algorithm:
        1. Discover the source for each input
        2. Create a `binding` dictionary for all arguments and input-record-schema entries.
        """
        metadata = self.tool_blueprint.metadata

        bindings_arguments = metadata.get('arguments', [])

        unnamed_id = 0
        all_bindings = {}

        binding_template = {
            'position': 0,
            'separate': True,
            'shellQuote': True,
        }

        # VV: Generate bindings for all Arguments
        for binding in bindings_arguments:
            name_arg = 'zz_arg%d' % unnamed_id
            unnamed_id += 1
            build = copy.deepcopy(binding_template)

            if isinstance(binding, (dict, OrderedDict)):
                build.update(dict(binding))
                binding = build
            elif isinstance(binding, string_types):
                binding = {
                    'valueFrom': binding,
                    'position': 0,
                    'separate': True,
                }
                build.update(binding)
                binding = build
            else:
                raise NotImplementedError('Cannot extract argument binding from %s for %s (%s)' % (
                    pprint.pformat(binding), self.flow_uid, self.tool_blueprint.metadata['id']
                ))
            binding = binding.copy()
            binding['name'] = name_arg
            all_bindings[name_arg] = binding

        # VV: Generate bindings for all fields in the input-schema
        bindings_inputs = [schema for schema in self.input_schemas if 'inputBinding' in schema]
        for schema in bindings_inputs:
            name = schema['name']
            if name not in self.dynamic_vars:
                cwl.log.info("Skipping Null-typed input %s for stage%s.%s" % (
                    name, self.stage_index, self.component_name
                ))
                continue

            build = copy.deepcopy(binding_template)

            binding = schema['inputBinding']
            build.update(dict(binding))
            build['name'] = name
            all_bindings[name] = build

        return convert_to_str(all_bindings)

    def compile_arguments(self, cwl):
        cwl_cmdline_blueprint = self.preprocess_cwlcmdline(cwl)

        all_objects = [(key, cwl_cmdline_blueprint[key]) for key in cwl_cmdline_blueprint]
        all_objects = sorted(all_objects, key=lambda e: e[1]['position'])

        cwl_cmdline_reference = 'stage%s.cwlcmdline-%s' % (self.stage_index, self.component_name,)

        args = ['%s/%s.txt:output' % (cwl_cmdline_reference, name_value[0]) for name_value in all_objects]

        return ' '.join(args)

    def compile_references(self, cwl):
        cwl_cmdline_blueprint = self.preprocess_cwlcmdline(cwl)

        all_objects = [(key, cwl_cmdline_blueprint[key]) for key in cwl_cmdline_blueprint]
        all_objects = sorted(all_objects, key=lambda e: e[1]['position'])

        cwl_cmdline_reference = 'stage%s.cwlcmdline-%s' % (self.stage_index, self.component_name,)

        references = ['%s/%s.txt:output' % (cwl_cmdline_reference, name_value[0]) for name_value in all_objects]
        return references

    def produce_flowir_mine(
        self,  # type: ZCommandLineTool
        cwl,  # type: Cwl
    ):
        environment_name = cwl.try_parse_env_vars_and_dependencies(
            self.requirements, self.hints, self.tool_blueprint.metadata['id']
        )

        if self.is_repeat:
            workflowAttributes = {
                'isRepeat': True,
                'repeatInterval': self.repeat_interval
            }
        else:
            workflowAttributes = {}

        # VV: The arguments are :output references wrapped in quotes and joined by a space
        arguments = self.compile_arguments(cwl)
        references = self.compile_references(cwl)

        flowir = {
            'command': {
                'executable': self.tool_blueprint.metadata['baseCommand'],
                'arguments': arguments,
                'environment': environment_name,
                'resolvePath': self.resolve_path,
            },
            'stage': self.stage_index,
            'name': self.component_name,
            'references': references,
            'workflowAttributes': workflowAttributes,
            'variables': self.variables,
            'resourceManager': self.resource_manager,
            'resourceRequest': self.resource_request,
        }

        # if len(references):
        #     flowir['executors'] = {
        #         'pre': [
        #             {
        #                 'name': 'lsf-dm-in',
        #                 'payload': ' '.join(references),
        #             }
        #         ]
        #     }

        return flowir

    def produce_stage_in(
        self,  # type: ZCommandLineTool
        cwl,  # type: Cwl
    ):
        # type: (...) -> Tuple[Dict[str, DictFlowIRComponent], Dict[str, Tuple[str, Dict[str, Any]]]]
        """Generates one CWL interpreter per fake-stage in.

        Returns:
            A dictionary that maps input object names to FlowIR descriptions of components and a dictionary
            that maps input object names to a Tuple that has the path inside the `data` directory of the file
            and the second entry in the tuple is the contents of the file itself.
        """
        input_sources = {
            input_name: cwl.track_down_var_uid_producer_info(
                self.dynamic_vars[input_name]
            ) for input_name in self.stage_in
        }  # type: Dict[str, Dict[str, str]]

        components = {}
        data_files = {}

        interpreter_environment = cwl.register_environment([{
            'DEFAULTS': 'PATH:PYTHONPATH:LD_LIBRARY_PATH',
            'PATH': '$PATH',
            'PYTHONPATH': '$PYTHONPATH',
            'LD_LIBRARY_PATH': '$LD_LIBRARY_PATH',
        }], 'cwl-interpreter'
        )

        for input_name in input_sources:
            cwl.log.info("Generating fake lsf-dm-in CWL interpreter")
            info = input_sources[input_name]

            interpreter_name = 'cwl-stage-in-%s-%s' % (self.component_name, input_name)
            blueprint_name = 'stage%d.%s' % (self.stage_index, interpreter_name)

            # VV: input_blueprint_ref is what we insert in `references` input_blueprint_path is what we insert
            #     in arguments. The difference is that in references we may want to have a reference to some producer
            #     component work-directory but the input_blueprint_path is actually the path to the blueprint itself
            #     we may want to keep them separate because a blueprint may be generated on the fly!
            if 'job-order-object-name' in info:
                job_order_obj_name = info['job-order-object-name']
                input_blueprint = os.path.join('input', '%s.yaml' % job_order_obj_name)
                input_blueprint_path = input_blueprint_ref = '%s:ref' % input_blueprint
            elif 'cwl-interpreter-uid' in info:
                interpreter_uid = info['cwl-interpreter-uid']

                try:
                    stage_index, component_name = cwl.pretty_names[interpreter_uid]
                except KeyError:
                    interpreter_info = cwl.track_down_var_uid_producer_info(info['cwl-interpreter-uid'])
                    moduleLogger.critical('Failed to fetch pretty name of %s' % interpreter_uid)
                    moduleLogger.critical('Transformation info is %s' % pprint.pformat(interpreter_info))
                    raise
                input_blueprint_ref = 'stage%d.%s:ref' % (stage_index, component_name)
                input_blueprint_path = "%s/cwl_output.yaml" % input_blueprint_ref
            else:
                raise NotImplementedError("Cannot handle input_source %s=%s" % (input_name, pprint.pformat(info)))

            blueprint_ref = '%s.yaml:ref' % os.path.join('data', 'cwl', blueprint_name)
            references = [blueprint_ref, input_blueprint_ref]

            arguments = ' '.join([
                '--blueprint %s' % blueprint_ref,
                '--input self %s' % input_blueprint_path
            ])

            blueprint = {
                # VV: FIXME this is a hack to copy files inside the working directory of the component
                'copy-to': os.path.join('$INSTANCE_DIR', 'stages', 'stage%d' % self.stage_index, self.component_name),
                'self': 'self',
            }  # type: Dict[str, str]

            blueprint = convert_to_str(blueprint)

            comp_flowir = {
                'command': {
                    'interpreter': 'cwl',
                    'arguments': arguments,
                    'environment': interpreter_environment,
                },
                'references': references,
                'resourceManager': {
                    'config': {
                        'backend': 'local'
                    }
                },
                'stage': self.stage_index,
                'name': interpreter_name,
            }

            components[input_name] = comp_flowir
            data_files[input_name] = (blueprint_name, blueprint)

            cwl.log.critical('Component stage in %s' % pprint.pformat(comp_flowir))

        return components, data_files

    def produce_flowir_cwl_cmdline(self, cwl, inject_deps=None):
        # type:(Cwl, Optional[List[str]]) -> DictFlowIRComponent

        inject_deps = inject_deps or []

        inject_deps = sorted(set(inject_deps))

        input_sources = {
            input_name: cwl.track_down_var_uid_producer_info(
                self.dynamic_vars[input_name]
            ) for input_name in self.dynamic_vars
        }  # type: Dict[str, Dict[str, str]]

        blueprint_reference = os.path.join('data', 'cwl', 'stage%s.cwlcmdline-%s.yaml:ref' % (
            self.stage_index, self.component_name
        ))

        cwl_cmdline_args = [
            '--blueprint "%s"' % blueprint_reference
        ]

        cwl_cmdline_refs = [blueprint_reference]

        for input_name in input_sources:
            info = input_sources[input_name]
            moduleLogger.debug('Handling input of cwlcmdline: %s' % pprint.pformat(info))
            try:
                if 'job-order-object-name' in info:
                    job_order_obj_name = info['job-order-object-name']
                    input_path = os.path.join('input', '%s.yaml' % job_order_obj_name)
                    cwl_cmdline_args.append('--constant_input %s "%s:ref"' % (input_name, input_path))
                    cwl_cmdline_refs.append('%s:ref' % input_path)
                elif 'cwl-interpreter-uid' in info:
                    interpreter_uid = info['cwl-interpreter-uid']

                    try:
                        stage_index, component_name = cwl.pretty_names[interpreter_uid]
                    except KeyError:
                        interpreter_info = cwl.track_down_var_uid_producer_info(info['cwl-interpreter-uid'])
                        moduleLogger.critical('Failed to fetch pretty name of %s' % interpreter_uid)
                        moduleLogger.critical('Transformation info is %s' % pprint.pformat(interpreter_info))
                        raise
                    ref_path = 'stage%d.%s:ref' % (stage_index, component_name)
                    cwl_cmdline_args.append('--input %s "%s/cwl_output.yaml"' % (input_name, ref_path))
                    cwl_cmdline_refs.append(ref_path)
                else:
                    raise NotImplementedError("Cannot handle input_source %s=%s" % (input_name, pprint.pformat(info)))
            except:
                moduleLogger.critical("Error when processing %s = %s" %(input_name, pprint.pformat(info)))
                raise

        cwl_cmdline_args += ['--fake %s' % ref for ref in inject_deps]
        cwl_cmdline_refs += inject_deps

        cwl_cmdline_refs = sorted(set(cwl_cmdline_refs))
        cwl_cmdline_args = ' '.join(cwl_cmdline_args)

        if self.is_repeat:
            workflowAttributes = {
                'isRepeat': True,
                'repeatInterval': self.repeat_interval
            }
        else:
            workflowAttributes = {}

        interpreter_environment = cwl.register_environment([{
            'DEFAULTS': 'PATH:PYTHONPATH:LD_LIBRARY_PATH',
            'PATH': '$PATH',
            'PYTHONPATH': '$PYTHONPATH',
            'LD_LIBRARY_PATH': '$LD_LIBRARY_PATH',
        }], 'cwl-interpreter'
        )

        return {
            'command': {
                'interpreter': 'cwlcmdline',
                'arguments': cwl_cmdline_args,
                'environment': interpreter_environment,
            },
            'references': cwl_cmdline_refs,
            'resourceManager': {
                'config': {
                    'backend': 'local'
                }
            },
            'stage': self.stage_index,
            'name': 'cwlcmdline-%s' % self.component_name,
            'workflowAttributes': workflowAttributes,
            'executors': {
                'post': [
                    {
                        'name': 'lsf-dm-out',
                        'payload': 'all'
                    }
                ]
            }
        }

    def preprocess_output_cwl(
            self,  # type: ZCommandLineTool
            cwl,  # type: Cwl
    ):
        # type: (...) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, str]]]
        """Generates CWL interpreters which postprocess the outputs of the tool.
        """
        blueprints = {}
        input_sources = {}
        tracked_so_far = {}
        for original in self.output_schemas:
            output = dict(original)
            output_name = output['name']
            blueprints[output_name] = output

            referenced_inputs = cwl.discover_references_to_variables(
                output.get('outputBinding', {}), ['outputEval', 'glob']
            )

            input_sources[output_name] = {}

            for input_name in referenced_inputs:
                if input_name not in tracked_so_far:
                    tracked_so_far[input_name] = cwl.track_down_var_uid_producer_info(
                        self.dynamic_vars[input_name]
                    )

                input_sources[output_name][input_name] = tracked_so_far[input_name]

        return convert_to_str(blueprints), convert_to_str(input_sources)

    def insert_to_concrete_flowir(self, cwl, concrete=None, inject_deps_to_cwlcmdline=None):
        # type: (Cwl, Optional[FlowIRConcrete], Optional[List[str]]) -> FlowIRConcrete
        concrete = concrete or FlowIRConcrete({}, FlowIR.LabelDefault, {})

        # VV: First inject the cwl-cmdline interpreter
        concrete.add_component(self.produce_flowir_cwl_cmdline(cwl, inject_deps=inject_deps_to_cwlcmdline))

        # VV: then inject the actual Component
        concrete.add_component(self.produce_flowir_mine(cwl))

        return concrete


class Cwl(object):
    (
        # VV: schema: {'resolve': bool (default=True if not specified)}
        extResolvePath,
        # VV: schema: {queue}
        extLSFBackend,

        # VV: schema: {repeatInterval: <int>, subjects=[names of inputs]}
        extObserveInput,

        # VV: schema: {numberProcesses, numberThreaads, ranksPerNode, threadsPerCore}
        extResourceRequest,

        # VV: schema: {backend: <str>, walltime: <float>}
        extResourceManager,

        # VV: schema: {}
        extDisableCheckProducerOutput,

        # VV: schema: {codes: List[str]}
        extShutdownOn,

        extSoftwareVault,
        extKubernetes,


        featureInitialWorkDir,
        featureSoftware,
        featureEnvVar,
        featureSubworkflow,
        featureStepInputExpression,
        featureInlineJavascript,
        featureScatter,
    ) = [
        'FlowResolvePath',
        'FlowLSFBackend',
        'FlowObserveInput',
        'FlowResources',
        'FlowResourcesManager',
        'FlowDisableCheckProducerOutput',
        'FlowShutdownOn',
        'FlowSoftwareVault',
        'FlowKubernetes',

        'InitialWorkDirRequirement',
        'SoftwareRequirement',
        'EnvVarRequirement',
        'SubworkflowFeatureRequirement',
        'StepInputExpressionRequirement',
        'InlineJavascriptRequirement',
        'ScatterFeatureRequirement',
    ]

    # VV: populate these with Requirements/Hints that Flow supports
    supported_requirements = [
        featureEnvVar,
        featureInitialWorkDir,
        featureSoftware,

        # VV: Support nested workflows
        featureSubworkflow,
        # featureInlineJavascript,
        featureStepInputExpression,
        featureScatter,
        featureInlineJavascript,
    ]

    supported_hints = [
        extResolvePath,
        extKubernetes,
        extShutdownOn,
        extDisableCheckProducerOutput,

        # VV: Strictly speaking this is a requirement but it's not supported by cwltool (which we use
        #    to parse and validate the CWL description of workflows) so we're currently masking it
        #    as a hint
        featureSoftware,
        extSoftwareVault,

        extLSFBackend,
        extObserveInput,
        extResourceRequest,
        extResourceManager,
    ]

    def __init__(self):
        """Frontend to load/store packages in CWL format.

        # VV: @tag FlowUID
        FlowUID = UNIQUE string that identifies ZCommandLineTool (ONLY contains letters and underscores)
        FlowReference = UNIQUE reference in the form of stage<number>.Name (good old flow references)
        """
        self.log = logging.getLogger('CWL')

        self.tool_cmd = {}  # type: Dict[IDCwl, CommandLineTool]
        self.tool_workflow = {}  # type: Dict[IDCwl, Workflow]

        self.inst_cmd = {}  # type: Dict[IDFlow, ZCommandLineTool]
        self.inst_workflow = {}  # type: Dict[IDFlow, ZWorkflow]
        self.step_dependencies = {}  # type: Dict[IDFlow, DependencyInfo]

        # VV: Map Variables to the components that are building them
        self.var_uid_to_process = {}  # type: Dict[IDVariable, Union[ZCommandLineTool, ZWorkflow, ZWorkflowStep]]

        self.num_tool_instances = {
            # VV: Keeps track of number of instances per Process (CommandLineTool, Workflow)
            #    (this info is used to generate unique names for the Flow components)
            #    (key is the URI that points to the Tool description)
        }  # type: Dict[IDCwl, int]

        # VV: Format: Built value flow-uid: [From value flow-uid: {Transformation details}]
        self.var_uid_built_from = {}  # type: Dict[IDFlow, Tuple[Optional[IDFlow], Dict[str, Any]]]

        self.component_outputs = {
            # VV: Maps INSTANCES of CommandLineTools to their registered outputs
        }  # type: Dict[IDFlow, Set[OutputInfo]]

        self.flow_application_dependencies = []  # type: List[str]

        self.names_to_environments = {}  # type: Dict[str, Dict[str, str]]
        self.environments = {}  # type: Dict[Tuple[Tuple[str, str], ...], Any]

        self.flowir = {}  # type: DictFlowIR

        self.pretty_names = {}  # type: Dict[str, Tuple[int, str]]

        self.vault_remote_software_files = {}

        load.loaders = {}

        # VV: Generate the default loading Context (imitates cwltool.main)
        args = get_default_args()

        self.args = argparse.Namespace()

        for key in args:
            setattr(self.args, key, args[key])

        loadingContext = LoadingContext(args)
        runtimeContext = RuntimeContext(args)

        loadingContext.loader = load.default_loader(loadingContext.fetcher_constructor)
        loadingContext.research_obj = runtimeContext.research_obj
        loadingContext.disable_js_validation = (
            self.args.disable_js_validation or (not self.args.do_validate)
        )
        loadingContext.construct_tool_object = getdefault(
            loadingContext.construct_tool_object, default_make_tool
        )
        loadingContext.resolver = getdefault(loadingContext.resolver, tool_resolver)
        loadingContext.do_update = not (self.args.pack or self.args.print_subgraph)

        self.loading_context = loadingContext
        self.loading_context.disable_js_validation = True

    def flow_uid_to_component_name(self, flow_uid, generate_unique_in_stage=None):
        orig_flow_uid = flow_uid
        try:
            flow_uid, metadata = flow_uid.rsplit('#', 1)

            # VV: Metadata format:
            #     (input|output)/object-name for CommandLineTools
            #     (input|output)/step-name/object-name for Workflows/WorkflowSteps

            tokens = metadata.split('/')

            method = tokens[0]
            object_name = tokens[-1]
            # VV: Objects that are scattered are suffixed with ':%d' % scatter_index
            object_name = object_name.rsplit(':', 1)[0]
            if method == 'input':
                if len(tokens) == 2:
                    stage, comp_name = self.pretty_names[flow_uid]

                    cwl_interp_name = os.path.join(
                        'cwl-%s-%s-%s' % (
                            comp_name, method, object_name
                        ))
                else:
                    step_name = tokens[1]
                    cwl_interp_name = os.path.join(
                        'cwl-workflow-%s-%s-%s-%s' % (
                            flow_uid, step_name, method, object_name
                        )
                    )
            elif method == 'output':
                # VV: This is an `output`
                if len(tokens) == 2:
                    source_info = self.track_down_var_uid_producer_info(orig_flow_uid)
                    if 'job-order-object-name' in source_info:
                        stage = 0
                        comp_name = 'from-input'
                    elif 'producer-uid-component' in source_info:
                        stage, comp_name = self.pretty_names[source_info['producer-uid-component']]
                    else:
                        raise NotImplementedError("Cannot generate component name for %s with source info %s" %(
                            orig_flow_uid, pprint.pformat(source_info)
                        ))

                    cwl_interp_name = os.path.join(
                        'cwl-%s-%s-%s' % (
                            comp_name, method, object_name
                        ))
                else:
                    step_name = tokens[1]
                    cwl_interp_name = os.path.join(
                        'cwl-workflow-%s-%s-%s-%s' % (
                            flow_uid, step_name, method, object_name
                        )
                    )
            else:
                raise NotImplementedError('Have not implemented %s yet' % method)

            name = cwl_interp_name

            if generate_unique_in_stage is not None:
                extra = 1
                names_in_stage = list(
                    [index_name[1] for index_name in [index_name for index_name in list(self.pretty_names.values()) if index_name[0] == generate_unique_in_stage]]
                )

                while cwl_interp_name in names_in_stage:
                    cwl_interp_name = name + str(RomanNumeral.int_to_roman(extra))
                    extra += 1
        except:
            moduleLogger.critical("Failed to generate component name for %s (%s)" % (
                orig_flow_uid, generate_unique_in_stage
            ))
            raise
        
        moduleLogger.info("Component name for %s (%s) is %s" % (
            orig_flow_uid, generate_unique_in_stage, cwl_interp_name
        ))

        return cwl_interp_name

    @staticmethod
    def hash_environment(
            description,  # type: List[Dict[str, str]]
    ):  # type: (...) -> Tuple[Tuple[str, str], ...]
        aggregated = []

        for group in description:
            group = dict(group)
            ordered_keys = sorted(group.keys())
            aggregated.extend([(e, group[e]) for e in ordered_keys])

        return tuple(aggregated)

    def register_environment(
            self,  # type: Cwl
            description,  # type: List[Dict[str, str]]
            name_suggestion,  # type: str
    ):  # type: (...) -> str
        # VV: Perform string interpolation to remove all references to environment variables

        self.log.debug("Trying to register :%s" % dumps_json(description))

        vault = {}

        for group in description:
            self.log.debug("Group: %s" % dumps_json(group))
            for key in group:
                self.log.debug("Key: %s" % key)
                group[key] = apply_env_vars_to_string(group[key], vault)
                vault[key] = group[key]

        env_hash = self.hash_environment([vault])

        if env_hash in self.environments:
            return self.environments[env_hash]['name']

        # VV: Make sure to pick a unique name for the environment
        next_number = 0
        base = name_suggestion or 'gen'
        while name_suggestion is None or name_suggestion in self.names_to_environments:
            next_number += 1
            name_suggestion = '%s%s' % (base, RomanNumeral.int_to_roman(next_number))

        self.environments[env_hash] = {
            'name': name_suggestion,
            'environment': [vault],
        }

        self.names_to_environments[name_suggestion] = vault

        return name_suggestion

    def _generate_uid_for_tool(
            self,  # type: Cwl
            tool_id,  # type: str
            replica_idx,  # type: Optional[int]
    ):  # type: (...) -> str
        # VV: This can be used to generate unique human-readable names that can be used as Flow component names
        self.num_tool_instances[tool_id] = self.num_tool_instances[tool_id] + 1

        self.log.debug("Generated %d for %s" % (
            self.num_tool_instances[tool_id], tool_id
        ))
        instance_num = self.num_tool_instances[tool_id]

        file_name = os.path.split(tool_id)[1]

        if '#' in file_name:
            file_name = file_name.split('#')[1]

        if '.' in file_name:
            file_name = file_name.rsplit('.', 1)[0]

        if replica_idx is not None:
            # VV: Flow doesn't like it when there's numbers at the end of a component name
            file_name = '%s%dz' % (file_name, replica_idx)

        # VV: @tag FlowUID
        flow_uid = '%s_%s_%s' % (
            hash_uri(tool_id),
            file_name,
            instance_num,
        )

        return flow_uid

    def _load_cwl_document(self, cwl_file):
        # type: (str) -> Process
        """Loads A CWL document, and registers it under the workflows/command-line-tools library"""

        ret = load.load_tool(cwl_file, self.loading_context)

        if isinstance(ret, Workflow):
            ret = cast(Workflow, ret)
            cwl_id = ret.metadata['id']

            if cwl_id in self.tool_workflow:
                return self.tool_workflow[cwl_id]

            self.tool_workflow[cwl_id] = ret
        elif isinstance(ret, CommandLineTool):
            ret = cast(CommandLineTool, ret)
            cwl_id = ret.metadata['id']

            if cwl_id in self.tool_cmd:
                return self.tool_cmd[cwl_id]

            self.tool_cmd[cwl_id] = ret
        else:
            raise NotImplementedError("Unexpected cwl-document of type %s" % type(ret))

        self.num_tool_instances[cwl_id] = 0
        return ret

    @classmethod
    def process_cwl_doc(
            cls,  # type: Cwl
            cwl_file,  # type: str
            input_object_paths=None,  # type: Optional[List[str]]
            stdin=sys.stdin,  # type: IO[Any]
    ):
        # type: (...) -> Cwl
        """Process a CWL document and its accompanying Input Object file to generate FlowIR.

        CWL-file can be a path to either a CommandLineTool or a WorkFlow

        InputObjectPath may point to a JSON/YAML file containing inputs that will feed the CWL object
                        (dictionary with `input-name: value` structure CWL documentation refers to
                        this thing as a `job order` or `input object`)
        """
        cwl_obj = Cwl()
        cwl_root = cwl_obj._load_cwl_document(cwl_file)

        if isinstance(input_object_paths, string_types):
            input_object_paths = [input_object_paths]

        if input_object_paths:
            # VV: CWL expects a Namespace object to load the input object path/job-order
            args = argparse.Namespace()

            dict_args = vars(cwl_obj.args)
            for key in dict_args:
                setattr(args, key, dict_args[key])
            setattr(args, 'job_order', input_object_paths)
            uri, tool_file_uri = load.resolve_tool_uri(
                cwl_file, resolver=cwl_obj.loading_context.resolver,
                fetcher_constructor=cwl_obj.loading_context.fetcher_constructor)
            job_order_object, input_basedir, jobloader = load_job_order(
                args, stdin, cwl_obj.loading_context.fetcher_constructor,
                cwl_obj.loading_context.overrides_list, tool_file_uri)
        else:
            job_order_object = {}

        if 'id' in job_order_object:
            del job_order_object['id']

        assert not cwl_obj.var_uid_built_from

        for key in job_order_object:
            transformation = {
                'job-order': (key, job_order_object[key]),
            }
            # VV: Include both the job-order name and value
            cwl_obj.var_uid_built_from['*#%s' % key] = (None, transformation)

        job_order_object = {key: '*#%s' % key for key in job_order_object}

        if isinstance(cwl_root, CommandLineTool):
            cwl_obj.digest_commandline_tool(cwl_root, dynamic_vars=job_order_object)
        elif isinstance(cwl_root, Workflow):
            cwl_obj.digest_workflow(cwl_root, dynamic_vars=job_order_object)

        return cwl_obj

    def generate_schedule(self):
        # type: () -> List[Set[DependencyInfo]]
        # VV: @tag FlowUID
        # VV: Map flow-uid to Flow references and backpatch self.var_uid_built_from[]
        all_step_dependencies = set([cmd.dependency_info for cmd in list(self.inst_cmd.values())])

        schedule = DependencyInfo.compute_schedule(all_step_dependencies)

        return schedule

    def prettify_flow_component_names(
            self,  # type: Cwl
            schedule,  # type: List[Set[DependencyInfo]]
    ):
        # type: (...) -> Dict[int, Dict[str, str]]
        """Generates human readable names for components and ensures that components in same stage have
         unique names.
         """
        name_instances = {
            # type: Dict[str, Dict[str, int]]
        }

        pretty_names = {}  # type: Dict[int, Dict[str, str]]

        for stage_index, stage_schedule in enumerate(schedule):
            pretty_names[stage_index] = {}
            name_instances[stage_index] = {}
            for dep_info in stage_schedule:
                name = dep_info.instance.flow_uid
                tokens = name.split('_')
                meat = '_'.join(tokens[1:-1])

                if dep_info.instance.step_name:
                    meat += '_' + dep_info.instance.step_name

                if dep_info.instance.replica_idx is not None:
                    meat += RomanNumeral.int_to_roman(dep_info.instance.replica_idx+1)

                if meat not in name_instances[stage_index]:
                    name_instances[stage_index][meat] = 0
                else:
                    name_instances[stage_index][meat] += 1

                if name_instances[stage_index][meat] > 0:
                    new_name = '%s_%s' % (
                        meat, RomanNumeral.int_to_roman(name_instances[stage_index][meat])
                    )
                else:
                    new_name = meat

                pretty_names[stage_index][name] = new_name

        self.pretty_names = {}

        for i, stage in enumerate(schedule):
            stage = cast(Set[DependencyInfo], stage)
            for comp in stage:
                comp.instance.stage_index = i
                comp.instance.component_name = pretty_names[i][comp.flow_uid]
                self.pretty_names[comp.flow_uid] = (comp.instance.stage_index, comp.instance.component_name)

        return pretty_names

    def generate_flowir_for_interpreter(
            self,  # type: Cwl
            stage_index,  # type: int
            comp_name,  # type: str
            config,  # type: Dict[str, Any]
            concrete,  # type: FlowIRConcrete
    ):
        # type: (...) -> FlowIRConcrete
        blueprint_path = os.path.join('data', 'cwl', 'stage%d.%s.yaml' % (stage_index, comp_name))
        command_arguments = [
            '--blueprint %s:ref' % blueprint_path
        ]

        references = ['%s:ref' % blueprint_path]
        if 'cwl-gather-from-uids' not in config:
            inputs = config['inputs']
        else:
            inputs = {
                'input%d'%idx: uid for idx, uid in enumerate(config['cwl-gather-from-uids']['from'])
            }

        if config.get('producer-uid-component'):
            stage_index, component_name = self.pretty_names[config['producer-uid-component']]
            ref_path = 'stage%d.%s:ref' % (stage_index, component_name)
            command_arguments.append('--root_dir "%s"' % ref_path)
            references.append(ref_path)

        if config.get('producer-uid-interpreter') is not None:
            source_uid = config['producer-uid-interpreter']
            source_info = self.track_down_var_uid_producer_info(source_uid)
            # VV: A producer can either be another interpreter or a Job Order
            if 'job-order-object-name' not in source_info:
                stage_index, component_name = self.pretty_names[config['producer-uid-interpreter']]
                ref_path = 'stage%d.%s:ref' % (stage_index, component_name)
                file_path = '%s/cwl_output.yaml' % ref_path
            else:
                ref_path = os.path.join('input', '%s.yaml:ref' % source_info['job-order-object-name'])
                file_path = ref_path
            command_arguments.append('--input %s "%s"' % ('__z_self', file_path))
            references.append(ref_path)

        for input_name in inputs:
            info = self.track_down_var_uid_producer_info(inputs[input_name])

            if 'job-order-object-name' in info:
                job_order_obj_name = info['job-order-object-name']
                input_path = os.path.join('input', '%s.yaml' % job_order_obj_name)
                command_arguments.append('--input %s "%s:ref"' % (input_name, input_path))
                references.append('%s:ref' % input_path)
            elif 'cwl-interpreter-uid' in info:
                interpreter_uid = info['cwl-interpreter-uid']
                stage_index, component_name = self.pretty_names[interpreter_uid]
                ref_path = 'stage%d.%s:ref' % (stage_index, component_name)
                command_arguments.append('--input %s "%s/cwl_output.yaml"' % (input_name, ref_path))
                references.append(ref_path)
            else:
                msg = "Cannot handle input_source %s=%s" % (input_name, pprint.pformat(info))
                moduleLogger.critical(msg)
                raise NotImplementedError(msg)

        references = sorted(set(references))

        interpreter_environment = self.register_environment([{
                'DEFAULTS': 'PATH:PYTHONPATH:LD_LIBRARY_PATH',
                'PATH': '$PATH',
                'PYTHONPATH': '$PYTHONPATH',
                'LD_LIBRARY_PATH': '$LD_LIBRARY_PATH',
            }], 'cwl-interpreter'
        )

        command = {
            'interpreter': 'cwl',
            'environment': interpreter_environment,
            'arguments': ' '.join(command_arguments)
        }

        comp = {
            'command': command,
            'references': references,
            'stage': stage_index,
            'name': comp_name,
        }

        concrete.add_component(comp)

        return concrete

    def generate_component_descriptions(self):
        # VV: @tag FlowUID
        # VV: Map flow-uid to Flow references and backpatch self.var_uid_built_from[]
        all_step_dependencies = set([cmd.dependency_info for cmd in list(self.inst_cmd.values())])

        schedule = DependencyInfo.compute_schedule(all_step_dependencies)

        pretty_names = self.prettify_flow_component_names(schedule)

        moduleLogger.info("Prettified names: %s" % pprint.pformat(pretty_names))

        bindings_generated = {}
        input_files_generated = {}
        data_files_generated = {}
        out_errors = []

        concrete = FlowIRConcrete({}, FlowIR.LabelDefault, {})

        moduleLogger.critical('Track uid vault:\n%s' % pprint.pformat(self.var_uid_built_from))

        for i, stage in enumerate(schedule):
            moduleLogger.info("Generating FlowIR for stage %d" % i)
            stage = cast(Set[DependencyInfo], stage)

            for comp in stage:
                moduleLogger.info("  Component: stage%s.%s" % (comp.instance.stage_index, comp.instance.component_name))
                all_tool_chains = []
                for input_name in comp.instance.dynamic_vars:
                    input_var_uid = comp.instance.dynamic_vars[input_name]

                    if input_var_uid is None:
                        moduleLogger.info("Skipping none input %s of stage%s.%s" % (
                            input_name, comp.instance.stage_index, comp.instance.component_name
                        ))
                        continue

                    chain_graph = self.build_transformations_graph(input_var_uid)

                    for chain in topological_sort(chain_graph):
                        if chain not in all_tool_chains:
                            all_tool_chains.append(chain)

                new_tool_chains = [e for e in all_tool_chains if e not in bindings_generated]
                moduleLogger.info("    Has %d input chains (new %d)" % (
                    len(all_tool_chains), len(new_tool_chains)
                ))
                if new_tool_chains:
                    for chain in new_tool_chains:
                        moduleLogger.info("      %s" % chain)

                # VV: Generate CWL interpreters
                for chain_uid in new_tool_chains:
                    info = self.track_down_var_uid_producer_info(chain_uid)

                    moduleLogger.critical('        Generating CWL for %s=%s' % (
                        chain_uid, info
                    ))
                    bindings_generated[chain_uid] = info
                    try:
                        if 'job-order-object-name' in info:
                            # VV: This is a job-order object, it should be placed in the `inputs` directory
                            object_name = info['job-order-object-name']
                            obj = info['job-order-object']
                            if input_files_generated.get(object_name, obj) != obj:
                                raise ValueError('Input file %s has already been generated' % object_name)

                            # VV: TODO yaml.safe_dump() raises an exception when unicode arrays are dumped() so
                            #     we recursively convert all string_types in `obj` to a string (as opposed to unicode)
                            input_files_generated[object_name] = convert_to_str(obj)
                        elif 'compute' in info:
                            # VV: This is CWL interpreter!
                            blueprint = info['compute'].copy()

                            # VV: Place this CWL-interpreter in the appropriate stage
                            if info.get('producer-uid-component') is not None:
                                stage_index, _ = self.pretty_names[info['producer-uid-component']]
                            elif info.get('producer-uid-interpreter') is not None:
                                # VV: If this is produced by an interpreter then place them in the same stage
                                source_uid = info['producer-uid-interpreter']

                                if source_uid in self.pretty_names:
                                    stage_index, _ = self.pretty_names[source_uid]
                                else:
                                    # VV: it's only safe not to refer to a CWL interpreter if you're referencing
                                    #     a job-order object
                                    source_info = self.track_down_var_uid_producer_info(source_uid)
                                    stage_index = comp.instance.stage_index
                                    if 'job-order-object-name' not in source_info:
                                        raise ValueError('Unknown producer-uid-interpreter %s' % source_uid)
                            else:
                                stage_index = comp.instance.stage_index

                            if info.get('producer-uid-interpreter') is not None:
                                blueprint['self'] = '__z_self'

                            cwl_interp_name = self.flow_uid_to_component_name(
                                chain_uid, generate_unique_in_stage=stage_index
                            )
                            self.pretty_names[chain_uid] = (stage_index, cwl_interp_name)

                            moduleLogger.info("Registering %s to stage%s.%s" % (
                                chain_uid, stage_index, cwl_interp_name
                            ))

                            concrete = self.generate_flowir_for_interpreter(
                                stage_index, cwl_interp_name, info, concrete
                            )

                            full_name = 'stage%d.%s' % (stage_index, cwl_interp_name)

                            if data_files_generated.get(full_name, blueprint) != blueprint:
                                raise ValueError('Data file %s has already been generated' % full_name)

                            data_files_generated[full_name] = blueprint
                        elif 'cwl-gather-from-uids' in info:
                            # VV: take the easy way out and place this interpreter in the same stage as the one
                            #     that's consuming it.
                            stage_index = comp.instance.stage_index
                            input_objects = info['cwl-gather-from-uids']['from']
                            blueprint = {
                                'valueFrom': '$([%s])' % ','.join(['inputs.input%d' % idx for idx in range(len(input_objects))])
                            }

                            moduleLogger.info("Blueprint is %s" % pprint.pformat(blueprint))

                            cwl_interp_name = self.flow_uid_to_component_name(
                                chain_uid, generate_unique_in_stage=stage_index
                            )
                            self.pretty_names[chain_uid] = (stage_index, cwl_interp_name)

                            moduleLogger.info("Registering %s to stage%s.%s" % (
                                chain_uid, stage_index, cwl_interp_name
                            ))

                            concrete = self.generate_flowir_for_interpreter(
                                stage_index, cwl_interp_name, info, concrete
                            )

                            full_name = 'stage%d.%s' % (stage_index, cwl_interp_name)

                            if data_files_generated.get(full_name, blueprint) != blueprint:
                                raise ValueError('Data file %s has already been generated' % full_name)

                            data_files_generated[full_name] = blueprint
                        else:
                            raise NotImplementedError(
                                "Cannot generate unknown interpreter with info %s" % pprint.pformat(info)
                            )
                    except Exception as e:
                        moduleLogger.critical('        Failed to generate interpreter %s with info %s' % (
                            chain_uid, pprint.pformat(info)
                        ))
                        out_errors.append(e)
                        raise

                # VV: Generate Component and input-related interpreter FlowIR descriptions
                blueprint = comp.instance.preprocess_cwlcmdline(self)
                blueprint_name = 'stage%s.cwlcmdline-%s' % (comp.instance.stage_index, comp.instance.component_name)
                data_files_generated[blueprint_name] = blueprint

                inject_deps_to_cwlcmdline = []

                # VV: FIXME This is a hack!
                # VV: Finally generate a fake cwl component that simply stages-in files (i.e. copies them in the
                #     working directory of the component)
                if comp.instance.stage_in:
                    stage_in_components, stage_in_data_files = comp.instance.produce_stage_in(self)
                    for input_name in stage_in_components:
                        comp_stage_in = stage_in_components[input_name]
                        concrete.add_component(comp_stage_in)
                        # VV: This will make it so the cwl-commandline interpreter executes between the `stage-in`
                        #     fake CWL interpreters and the actual component
                        ref = 'stage%d.%s:ref' % (comp_stage_in['stage'], comp_stage_in['name'])
                        inject_deps_to_cwlcmdline.append(ref)

                    for input_name in stage_in_data_files:
                        stage_in_bp_name, stage_in_bp_contents = stage_in_data_files[input_name]
                        data_files_generated[stage_in_bp_name] = stage_in_bp_contents

                concrete = comp.instance.insert_to_concrete_flowir(
                    self, concrete, inject_deps_to_cwlcmdline=inject_deps_to_cwlcmdline
                )

        moduleLogger.debug('Dumping environments %s' % list(self.names_to_environments.keys()))
        for name in self.names_to_environments:
            moduleLogger.debug('Environment: %s = %s' % (
                name, pprint.pformat(self.names_to_environments[name])
            ))
            concrete.set_environment(name, self.names_to_environments[name])

        # VV: FIXME This is a hack!
        concrete._flowir[FlowIR.FieldApplicationDependencies] = {
            FlowIR.LabelDefault: sorted(set(self.flow_application_dependencies))
        }

        if out_errors:
            raise ValueError(out_errors)

        return concrete, input_files_generated, data_files_generated

    def fetch_software_dependency(self, location):
        # type: (str) -> Dict[str, Any]

        if location.startswith('http://') or location.startswith('https://'):
            if location in self.vault_remote_software_files:
                dep = self.vault_remote_software_files[location]
            else:
                dep = preprocess_and_validate_flow_dependency_file(location)
                self.vault_remote_software_files[location] = dep
        else:
            dep = preprocess_and_validate_flow_dependency_file(location)

        return dep

    def parse_software_dependency_requirement(self, requirement, user_tool_location, software_vault_dir):
        # type: (Dict[str, Any], str, Optional[str]) -> Tuple[List[Dict[str, str]], List[str]]
        application_names = []  # type: List[str]
        environment_variables = []  # type: List[Dict[str, str]]

        for package in requirement['packages']:
            package = cast(Dict[str, Any], package)
            if 'specs' in package:
                for spec in package['specs']:
                    if spec.startswith('flow+') is False:
                        raise Exception("Cannot satisfy SoftwareRequirement: %s" % dumps_json(package))

                    dep = {}

                    if spec.startswith('flow+application://'):
                        application_name = os.path.split(spec)[1]
                        application_names.append(application_name)
                        moduleLogger.debug("Register dependency to application '%s' from spec %s" % (
                            application_name, spec
                        ))
                    elif spec.startswith('flow+software://'):
                        dep_location = spec[16:]
                        if software_vault_dir is not None:
                            try:
                                dep = self.fetch_software_dependency(
                                    os.path.join(software_vault_dir, dep_location)
                                )
                            except:
                                dep = self.fetch_software_dependency(
                                    os.path.join(user_tool_location, dep_location)
                                )
                        else:
                            dep = self.fetch_software_dependency(os.path.join(user_tool_location, dep_location))
                    elif (spec.startswith('flow+file://')
                          or spec.startswith('flow+http://')
                          or spec.startswith('flow+https://')):
                        dep_location = spec[5:]
                        dep = self.fetch_software_dependency(dep_location)

                    if 'envDef' in dep:
                        environment_variables.extend(dep['envDef'])
            else:
                raise Exception(
                    "Cannot satisfy SoftwareRequirement: %s (no specs defined)" % dumps_json(package)
                )

        # VV: Applications may also have `bin` directories !
        app_path = ':'.join(
            ['$INSTANCE_DIR/%s/bin' % '.'.join(path.split('.')[:-1]).lower() for path in set(application_names)]
        )

        if app_path:
            environment_variables.append({
                'PATH': '%s:$PATH' % app_path
            })

        self.log.info("Read environment variables: %s" % dumps_json(environment_variables))
        return environment_variables, application_names

    def try_parse_env_vars_and_dependencies(self, requirements, hints, cwl_uid, name_for_empty=None):
        # type: (List[Dict[str, str]], List[Dict[str, str]], str, Optional[str]) -> Optional[str]
        environment_variables = []
        environment_name = None

        root_location = os.path.split(cwl_uid)[0]

        for requirement in requirements:
            requirement = cast(Dict[str, Any], requirement)
            if requirement['class'] not in self.supported_requirements:
                raise UnsupportedCWLRequirement(
                    requirement['class'], cwl_uid
                )

            if requirement['class'] == self.featureEnvVar:
                environment_variables.append(requirement['envDef'])

        environment_variables = unroll_cwl_env_def(environment_variables)

        software_requirements = []  # type: List[Dict[str, Any]]
        software_vault_dir = None

        for hint in hints:
            if 'class' not in hint:
                continue
            class_type = hint['class']  # type: str

            if class_type.startswith('file://') or class_type.startswith('http://'):
                class_type = os.path.split(class_type)[1]

            if class_type == Cwl.featureSoftware:
                software_requirements.append(hint)
            elif class_type == Cwl.extSoftwareVault:
                assert software_vault_dir is None

                t = hint['location']  # type: str

                if t.startswith('http://') or t.startswith('https://') or t.startswith('file://'):
                    # VV: Either this is an ALREADY pre-processed hint!!! it was generated by some previous
                    #     call to try_parse_env_vars_and_dependencies() or a human used an absolute URI
                    #     for the SoftwareVault location
                    software_vault_dir = t
                else:
                    # VV: This is the VERY first time we come across a SoftwareLocation hint,
                    #     prefix it with root_location and then modify the hint so that the
                    #     software_location value gets propagated to downstream components (actually upstream)
                    software_vault_dir = os.path.join(root_location, t)
                    hint['location'] = software_vault_dir

        for software_req in software_requirements:
            env_defs, application_names = self.parse_software_dependency_requirement(
                software_req, root_location, software_vault_dir
            )
            self.flow_application_dependencies.extend(application_names)
            environment_variables.extend(env_defs)

        # VV: Attempt to register a new environment, if the environment_variables are identical with some other
        #     environment, the enveloping environment will be reused
        if environment_variables:
            file_name = os.path.split(cwl_uid)[1]

            if '#' in file_name:
                file_name = file_name.split('#')[1]

            if '.' in file_name:
                file_name = file_name.rsplit('.', 1)[0]
            environment_name = self.register_environment(environment_variables, file_name)
        elif name_for_empty:
            environment_name = self.register_environment([], name_for_empty)

        return environment_name

    def get_producers_of_var_uid(
            self,  # type: Cwl
            flow_var_uid,  # type: str
    ):
        # type: (...) -> List[str]

        def split_flow_var_uid(var_uid_reference):  # type: (str) -> Tuple[str, List[str]]
            flow_uid, object_input_uid = var_uid_reference.split('#')
            object_input_uid = object_input_uid.split('/')
            return flow_uid, object_input_uid

        def get_flow_uid_of_closest_producer(src):
            trf = None

            # VV: Look for entries which have None source i.e. they generate information
            #     that can only happen for:
            #     a) Job-order objects (i.e. constant objects)
            #     b) Outputs of Command Line Tools
            #     c) Gather of Scattered inputs
            #     d) valueFrom in WorkflowStep inputs and CommandLineTool inputBinding

            while src in self.var_uid_built_from:
                dest, trf = self.var_uid_built_from[src]
                if dest is None:
                    break

                src = dest

            if 'job-order' in trf:
                return []

            if 'cwl-gather-from-uids' not in trf:
                flow_uid, _ = split_flow_var_uid(src)

                return [flow_uid]
            else:
                gathered_producers = []
                gather = trf['cwl-gather-from-uids']['from']

                for g_var_uid in gather:
                    gathered_producers.extend(get_flow_uid_of_closest_producer(g_var_uid))

                return gathered_producers

        source, transformation = self.var_uid_built_from[flow_var_uid]
        ret = []  # type: List[str]

        if source is not None:
            ret.extend(get_flow_uid_of_closest_producer(source))

        for collection_name in {'inputs'}.intersection(set(transformation.keys())):
            collection = transformation[collection_name]  # type: Dict[str, str]

            for var_uid in collection:
                ret.extend(get_flow_uid_of_closest_producer(var_uid))

        return [e for e in ret if e is not None]

    @classmethod
    def extract_input_types_from_field_record(cls, field):
        # VV: This input has been fed a null-typed object !
        input_types = field['type']

        # VV: The type field can be a dictionary (for arrays/enums/inputRecordSchemas) a CWLType
        #     which is a string, or it can be a List of any of the aforementioned
        #     An Object can ONLY be set to the Null value if it can be of type `null`, and `null`
        #     is a CWLType so we just care for type-descriptions which are strings (if it's just
        #     one we'll create a 1-entry list)
        if isinstance(input_types, string_types):
            input_types = [input_types]
        elif isinstance(input_types, dict):
            # VV: Don't care about this as it can't be a `null` type
            input_types = []
        elif isinstance(input_types, list):
            input_types = [e for e in input_types if isinstance(e, string_types)]
        else:
            msg = ('Expected a type of [string, list, Dict] received this thing: %s' % dumps_json(field))
            moduleLogger.critical(msg)
            raise ValueError(msg)

        return input_types

    def _digest_tool_dynamic_vars(
            self,  # type: Cwl
            flow_uid,  # type: IDFlow
            dynamic_vars,  # type: Dict[str, str]
            tool,  # type: Process
            inputs_record_schema,  # type: Dict[str, Any]
    ):
        may_be_null = []

        errors = []

        fed_vars = [l['name'] for l in inputs_record_schema['fields']]

        moduleLogger.info("Fed vars are %s" % fed_vars)
        moduleLogger.info("Input record: %s" % pprint.pformat(inputs_record_schema['fields']))

        # may_be_null.extend(set(fed_vars).difference(dynamic_vars))
        # dynamic_vars = {
        #     key: dynamic_vars[key] for key in set(fed_vars).intersection(set(dynamic_vars.keys()))
        # }

        for key in fed_vars:
            input_var_uid = '%s#input/%s' % (flow_uid, key)

            field = [l for l in inputs_record_schema['fields'] if l['name'] == key]

            if len(field) != 1:
                msg = 'Could not find exact match for dynamic var %s in inputs_record_schema (found %s) for %s' % (
                    key, pprint.pformat(field), flow_uid,
                )
                moduleLogger.critical(msg)
                errors.append(msg)
                continue

            field = field[0]

            input_types = self.extract_input_types_from_field_record(field)

            if 'null' not in input_types and field['name'] not in dynamic_vars:
                msg = (
                    "Input %s to %s is set to None but it can't be null-typed (input_types: %s from desc: %s)" % (
                        key, flow_uid, input_types, dumps_json(field)
                    )
                )
                moduleLogger.critical(msg)
                errors.append(msg)
                continue
            elif 'null' in input_types and field['name'] not in dynamic_vars:
                may_be_null.append(key)
                continue


            try:
                transformation = {'inputBinding': dict(field['inputBinding'])}
            except KeyError:
                transformation = {}

            self.var_uid_built_from[input_var_uid] = (dynamic_vars[key], transformation)

        if errors:
            raise ValueError(errors)

        # VV: Null objects can be omitted altogether, keep track of them mostly for debugging purposes
        #     there's no need to validate that they are indeed null-typed CWLtool already did that for us
        all_input_names = [l['name'] for l in tool.inputs_record_schema['fields']]

        for input_name in all_input_names:
            if input_name in dynamic_vars and dynamic_vars[input_name] is None and input_name in may_be_null:
                del dynamic_vars[input_name]

        return may_be_null

    def _digest_hint_observe(
            self,
            flow_id,
            cwl_id,
            hints,
            subjects,
            producers
    ):
        filtered_hints = []
        repeat_interval = None

        # VV: Filter out unsupported hints and apply extObserveInput ("FlowObserveInput") to potentially
        #     convert this Producer/Consumer component into an Observer one
        for hint in hints:
            if 'class' not in hint:
                continue
            class_type = hint['class']  # type: str

            if class_type.startswith('file://') or class_type.startswith('http://'):
                class_type = os.path.split(class_type)[1]

            if class_type not in self.supported_hints:
                self.log.info("Unsupported CommandLineTool.hint %s" % hint)
                continue

            if class_type == self.extObserveInput:
                # VV: TODO Consider that there's a single input Object has dependencies to several Components
                #     (perhaps a `valueFrom` is involved) is it safe to promote all of the Producer dependencies
                #     to Subject dependencies ? (because that's what we're doing here)

                repeat_interval = float(hint['repeatInterval'])
                # VV: List of input names
                ext_subjects = cast(List[str], hint['subjects'])

                # VV: Till this point we didn't know that this CommandLineTool is actually an observer
                #     which means that there has to be exactly one Producer for each of the input names
                #     listed within 'subjects'

                for subject in ext_subjects:
                    try:
                        subjects[subject] = producers[subject]
                    except KeyError:
                        self.log.critical('%s (%s) does not have an input %s (%s)' % (
                            flow_id, cwl_id, subject, dumps_json(list(producers.keys()))
                        ))
                        raise
                producers = producers.copy()
                # VV: Now remove the promoted subject entries from the list of producers
                for input_name in ext_subjects:
                    del producers[input_name]

                if bool(subjects) is False:
                    raise NotImplementedError(
                        "%s (%s) is marked as an Obsever but has no Subjects" % (
                            flow_id, cwl_id
                        ))
                continue

            # VV: `hint` is supported and it's not extObserveInput. ZCommandLineTool will handle
            #     it when it's initialized
            filtered_hints.append(hint)

        return filtered_hints, repeat_interval

    @classmethod
    def discover_references_to_variables(cls, schema, fields_with_input_references):
        referenced_inputs = []

        pattern = re.compile(r'\$\(inputs\.(\w+)\b')

        for field_name in fields_with_input_references:
            field = schema.get(field_name, '')
            if isinstance(field, string_types):
                referenced_inputs.extend(
                    [e.groups()[0] for e in pattern.finditer(field)]
                )
            elif isinstance(field, list):
                referenced_inputs.extend(
                    [e.groups()[0] for e in pattern.finditer(' '.join(field))]
                )
            elif isinstance(field, dict):
                referenced_inputs.extend(
                    [e.groups()[0] for e in pattern.finditer(' '.join(list(field.values())))]
                )
            else:
                raise NotImplementedError('Do not know how discover variable references in %s' % field)

        return referenced_inputs

    def digest_workflowstep(
            self,  # type: Cwl
            step,  # type: WorkflowStep
            workflow,  # type: Workflow
            workflow_flow_uid,  # type: str
            workflow_dynamic_vars,  # type: Dict[str, str]
            workflow_hints,  # type: List[Dict[str, Any]]
            workflow_requirements,  # type: List[Dict[str, Any]]
            nested_cmdline,  # type: Dict[str, ZCommandLineTool]
            nested_workflow,  # type: Dict[str, ZWorkflow]
    ):
        tool_path = cast(str, step.tool['run'])
        cwl_uid = step.tool['id']
        moduleLogger.info("Started Processing workflowstep %s" % cwl_uid)
        moduleLogger.info('Dynamic vars: %s' % pprint.pformat(workflow_dynamic_vars))

        self.num_tool_instances[cwl_uid] = 0
        step_name = cwl_uid.split('#')[-1]

        inputs = convert_schema(step.inputs_record_schema)
        outputs = convert_schema(step.outputs_record_schema)

        step_hints = override_dict(step.hints or [], workflow_hints, keys_allow_many=[Cwl.extLSFBackend])
        step_requirements = override_dict(
            step.requirements or [], workflow_requirements, keys_allow_many=[Cwl.featureEnvVar]
        )

        errors = []

        # VV: Divide inputs into those fetched from the Workflow (i.e. dynamic_vars)
        #     and those generated by other steps of the parent workflow.
        #     Group the inputs under their `origin`:
        #       None for `workflow`
        #       `name of step` for steps
        inputs_from = {}

        for field in inputs['fields']:
            if 'source' in field:
                source = field['source']

                _, param_name = source.rsplit('#', 1)

                origin, param_name = os.path.split(param_name)

                if not origin:
                    origin = None
            else:
                origin = None
                param_name = field['name']

            field = copy.deepcopy(field)
            del field['_tool_entry']
            field['param-name'] = param_name

            if origin is not None or param_name in workflow_dynamic_vars:
                # VV: If this input is fed from another step, or it is piped through the parent workflow and it
                #     has been assigned a value then register it

                if origin not in inputs_from:
                    inputs_from[origin] = []
                inputs_from[origin].append(field)
            elif origin is None and param_name not in workflow_dynamic_vars:
                # VV: Else this input is optional and is not provided by the workflow

                input_types = self.extract_input_types_from_field_record(field)
                if 'null' in input_types:
                    moduleLogger.info("Step parameter %s is not provided, will ignore" % param_name)
                else:
                    msg = 'Step parameter %s is not provided and cannot be null for %s (%s)' % (
                        param_name, cwl_uid, pprint.pformat(field)
                    )
                    moduleLogger.critical(msg)
                    raise ValueError(msg)
            else:
                raise NotImplementedError('Cannot handle input %s of %s'% (pprint.pformat(field), cwl_uid))

        moduleLogger.info('Inputs for workflowstep %s are \n%s' % (cwl_uid, pprint.pformat(inputs_from)))

        dynamic_vars = {}

        from_workflow = inputs_from.get(None, {})

        def schema_to_transformation(param_schema):
            track_info = {}
            if 'valueFrom' in param_schema:
                track_info['inputs'] = {
                    ref_input_name: workflow_dynamic_vars[ref_input_name] \
                        for ref_input_name in Cwl.discover_references_to_variables(param_schema, ['valueFrom'])
                }
                track_info['compute'] = {
                    'valueFrom': param_schema['valueFrom'],
                    'object-type': param_schema['type'],
                }
                track_info = {'cwl-interpreter': track_info}
            return track_info

        workflow_step_var_uids = {}

        # VV: Hook step inputs to Workflow inputs
        will_remove = []
        for param_schema in from_workflow:
            param_name = param_schema['param-name']
            tool_param_name = param_schema['name']
            input_var_uid = "%s#input/%s/%s" % (workflow_flow_uid, step_name, tool_param_name)

            if 'source' in param_schema:
                _, workflow_param_name = param_schema['source'].rsplit('#', 1)
                try:
                    origin = workflow_dynamic_vars[param_name]
                except KeyError:
                    will_remove.append(param_schema)
                    continue

            else:
                origin = None

            track_info = schema_to_transformation(param_schema)

            workflow_step_var_uids[input_var_uid] = (origin, track_info)
            dynamic_vars[tool_param_name] = input_var_uid

        # VV: Hook this step inputs to the outputs of other steps of the same Workflow
        for input_step_name in set(inputs_from.keys()).difference({None}):
            from_step = inputs_from[input_step_name]

            for param_schema in from_step:
                param_name = param_schema['param-name']
                tool_param_name = param_schema['name']

                input_var_uid = "%s#input/%s/%s" % (workflow_flow_uid, step_name, tool_param_name)
                origin_uid = '%s#output/%s/%s' % (workflow_flow_uid, input_step_name, param_name)

                track_info = schema_to_transformation(param_schema)

                workflow_step_var_uids[input_var_uid] = (origin_uid, track_info)
                dynamic_vars[tool_param_name] = input_var_uid

        self.var_uid_built_from.update(workflow_step_var_uids)

        moduleLogger.critical('Dynamic vars of step %s: %s' % (
            step_name, pprint.pformat(dynamic_vars)
        ))

        blueprint = self._load_cwl_document(tool_path)

        # VV: At this point we have to distinguish between workflowSteps which have scattered inputs
        #     and those which have plain ones.
        scattered_inputs = step.tool.get('scatter', [])

        # VV: we're expecting a list of strings (Names of WorkflowStep input objects)
        if isinstance(scattered_inputs, string_types):
            scattered_inputs = [scattered_inputs]

        scattered_inputs = sorted(set([os.path.split(step_input_id)[1] for step_input_id in scattered_inputs]))

        workflowstep_var_uid_outputs = {}

        if len(scattered_inputs) == 0:
            # VV: This WorkflowStep does not have any scattered inputs
            self.var_uid_built_from.update(workflow_step_var_uids)

            if isinstance(blueprint, Workflow):
                workflow_instance = self.digest_workflow(
                    blueprint, dynamic_vars=dynamic_vars,
                    inputs_record_schema=inputs, outputs_record_schema=outputs,
                    parent_hints=step_hints, parent_requirements=step_requirements
                )
                nested_workflow[step_name] = workflow_instance
                step_tool_instance_uid = workflow_instance.flow_uid
            elif isinstance(blueprint, CommandLineTool):
                cmdline_instance = self.digest_commandline_tool(
                    blueprint, dynamic_vars=dynamic_vars,
                    inputs_record_schema=inputs, outputs_record_schema=outputs,
                    parent_hints=step_hints, parent_requirements=step_requirements,
                    step_name = step_name
                )
                nested_cmdline[step_name] = cmdline_instance
                step_tool_instance_uid = cmdline_instance.flow_uid
            else:
                # VV: This is probably an ExpressionTool which we currently don't support
                #     it should be easy to support once we properly implement CWLJIT arguments
                raise NotImplementedError(
                    'Unsupported tool for %s' % blueprint.tool
                )

            # VV: Update records for the outputs of the current step
            for output in outputs['fields']:
                output_name = output['name']
                producer_var_uid = '%s#output/%s' % (step_tool_instance_uid, output_name)
                workflowstep_var_uid_outputs[output_name] = (producer_var_uid, {})
        else:
            # VV: This WorkflowStep has scattered inputs, we need to launch multiple copies of it
            scatter_method = step.tool.get('scatterMethod', 'dotproduct').lower()

            if scatter_method == 'dotproduct':
                pass
            else:
                raise NotImplementedError('Have not implemented scatter_method %s that is used in workflow %s' % (
                    scatter_method, workflow.tool['id']
                ))

            moduleLogger.info("Current workflow step var uids: %s" % pprint.pformat(workflow_step_var_uids))

            moduleLogger.info('Scattering inputs %s' % pprint.pformat(scattered_inputs))

            scatter_objects = {
                key: self.track_down_var_uid_producer_info(dynamic_vars[key]) for key in scattered_inputs
            }

            moduleLogger.info('Scattering objects %s' % pprint.pformat(scatter_objects))
            # VV: Verify that all of the sources of the scattered inputs have a fixed array size

            errors = []
            scatter_len = None
            for key in scatter_objects:
                if 'job-order-object' in scatter_objects[key]:
                    this_len = len(scatter_objects[key]['job-order-object'])
                elif 'cwl-gather-from-uids' in scatter_objects[key]:
                    this_len = scatter_objects[key]['cwl-gather-from-uids']['from']
                else:
                    errors.append('Cannot extract length of Scattered array from %s' % scatter_objects[key])
                    continue

                if scatter_len is None:
                    scatter_len = this_len
                elif this_len != scatter_len:
                    errors.append('Scattered input %s has len %d (must match %d)' % (
                        key, this_len, scatter_len
                    ))

            if errors:
                raise ValueError('\n'.join(errors))
            # VV: We don't need the scattered objects anymore
            del scatter_objects

            # VV: @tag:ScatteredInput
            # VV: Scattered inputs should generate 1 CWL-interpreter that extract one element out of a CWL array
            #     object that's part of the job-order.

            scatter_transformations = {}
            all_scatter_vars = {}

            for scatter_idx in range(scatter_len):
                scatter_dynamic_vars = copy.deepcopy(dynamic_vars)

                for param_name in scattered_inputs:
                    source_uid = scatter_dynamic_vars[param_name]
                    input_var_uid = "%s#input/%s/%s:%d" % (workflow_flow_uid, step_name, param_name, scatter_idx)

                    source_info = self.track_down_var_uid_producer_info(source_uid)
                    if 'job-order-object' in source_info:
                        scatter_transformations[input_var_uid] = (
                            source_uid,
                            {
                                'cwl-interpreter': {
                                    # VV: the only source is `source_uid` which is the Job-Order array object
                                    'inputs': {},
                                    'compute': {
                                        'valueFrom': '$(self[%d])' % scatter_idx
                                    }
                                }
                            }
                        )
                    elif 'cwl-gather-from-uids' in source_info:
                        # VV: We're scattering the results of a previous gather, fetch the appropriate index
                        producer = source_info['cwl-gather-from-uids']['from'][scatter_idx]
                        scatter_transformations[input_var_uid] = (producer, {})
                    else:
                        raise NotImplementedError('Cannot Scatter %s' % pprint.pformat(source_info))

                    # VV: Feed the step-input with the scattered fraction of the array input object
                    scatter_dynamic_vars[param_name] = input_var_uid

                all_scatter_vars[scatter_idx] = scatter_dynamic_vars

            self.var_uid_built_from.update(scatter_transformations)

            moduleLogger.info("All scatter variables: %s" % pprint.pformat(all_scatter_vars))
            moduleLogger.info("Scatter VAR-UIDS: %s" % pprint.pformat(scatter_transformations))

            scatter_instances = []
            for scatter_idx in all_scatter_vars:
                scatter_dynamic_vars = all_scatter_vars[scatter_idx]

                if isinstance(blueprint, Workflow):
                    workflow_instance = self.digest_workflow(
                        blueprint, dynamic_vars=scatter_dynamic_vars,
                        inputs_record_schema=inputs, outputs_record_schema=outputs,
                        parent_hints=step_hints, parent_requirements=step_requirements,
                        replica_idx=scatter_idx
                    )
                    scatter_instances.append(workflow_instance)
                elif isinstance(blueprint, CommandLineTool):
                    cmdline_instance = self.digest_commandline_tool(
                        blueprint, dynamic_vars=scatter_dynamic_vars,
                        inputs_record_schema=inputs, outputs_record_schema=outputs,
                        parent_hints=step_hints, parent_requirements=step_requirements,
                        replica_idx=scatter_idx, step_name=step_name
                    )
                    scatter_instances.append(cmdline_instance)

            scatter_uids = [instance.flow_uid for instance in scatter_instances]

            moduleLogger.info("Scatter instances: %s" % pprint.pformat(scatter_uids))

            # VV: Update records for the outputs of the current step
            for output in outputs['fields']:
                output_name = output['name']

                scattered_output_var_uids = ['%s#output/%s' % (scatter_flow_uid, output_name)
                                             for scatter_flow_uid in scatter_uids]
                workflowstep_var_uid_outputs[output_name] = (None, {
                    'cwl-gather-from-uids': {
                        'from': scattered_output_var_uids
                    }
                })

        for output_name in workflowstep_var_uid_outputs:
            var_uid = '%s#output/%s/%s' % (workflow_flow_uid, step_name, output_name)
            self.var_uid_built_from[var_uid] = workflowstep_var_uid_outputs[output_name]

        return workflowstep_var_uid_outputs

    def digest_workflow(self,  # type: Cwl
                        workflow,  # type: Workflow
                        dynamic_vars,  # type: Dict[str, str]

                        # VV: These parameters indicate inherited configuration from some WorkflowStep/Workflow
                        inputs_record_schema=None,  # type: Optional[Dict[str, Any]]
                        outputs_record_schema=None,  # type: Optional[Dict[str, Any]]
                        parent_hints=None,  # type: Optional[List[Dict[str, str]]]
                        parent_requirements=None,  # type: Optional[List[Dict[str, str]]]
                        replica_idx=None,  # type: Optional[int]
                        ):  # type: (...) -> ZWorkflow
        cwl_uid = workflow.tool['id']
        moduleLogger.critical("Started Processing workflow %s" % cwl_uid)
        moduleLogger.critical('Dynamic vars: %s' % pprint.pformat(dynamic_vars))
        
        flow_uid = self._generate_uid_for_tool(workflow.metadata['id'], replica_idx)

        # VV: Override schemas based on information supplied by the parent Tool
        inputs_record_schema = convert_schema(inputs_record_schema or workflow.inputs_record_schema)
        outputs_record_schema = convert_schema(outputs_record_schema or workflow.outputs_record_schema)

        requirements = override_dict(
            workflow.requirements, parent_requirements, keys_allow_many=[self.featureEnvVar]
        )
        hints = override_dict(workflow.hints, parent_hints, keys_allow_many=[Cwl.extLSFBackend])

        # VV: Disable initial working dir for Workflows, we only suppport it for CommandLineTools
        supported_workflow_requirements = set(Cwl.supported_requirements) - {Cwl.featureInitialWorkDir}

        for requirement in requirements:
            if 'class' not in requirement:
                continue

            class_type = requirement['class']

            if class_type.startswith('file://') or class_type.startswith('http://'):
                class_type = os.path.split(class_type)[1]

            if class_type not in supported_workflow_requirements:
                msg = "Unsupported Workflow.requirement %s" % requirement
                moduleLogger.critical(msg)
                raise ValueError(msg)

        may_be_null = self._digest_tool_dynamic_vars(
            flow_uid, dynamic_vars, workflow, inputs_record_schema
        )

        moduleLogger.critical('May be Null: %s' % may_be_null)

        ordered_steps = sort_workflow_steps(workflow)

        moduleLogger.critical('Ordered steps: %s' % pprint.pformat([e.flow_uid for e in ordered_steps]))
        steps = cast(List[WorkflowStep], [e.instance for e in ordered_steps])

        # VV: Keep track of Tools in steps defined under this Workflow tool
        nested_cmdline = {}  # type: Dict[str, ZCommandLineTool]
        nested_workflow = {}  # type: Dict[str, ZWorkflow]

        input_names = [e['name'] for e in inputs_record_schema['fields']]
        self.log.info("Workflow input names: %s" % pprint.pformat(input_names))

        step_outputs = {}

        for step in steps:
            step_cwl_uid = step.tool['id']
            step_name = step_cwl_uid.split('#')[-1]

            step_outputs[step_name] = self.digest_workflowstep(
                step=step, workflow=workflow, workflow_dynamic_vars=copy.deepcopy(dynamic_vars),
                workflow_hints=hints, workflow_requirements=requirements, workflow_flow_uid=flow_uid,
                nested_cmdline=nested_cmdline, nested_workflow=nested_workflow,
            )

        for output in outputs_record_schema['fields']:
            step_name, step_output_name = output['outputSource'].split('#')[1].split('/')

            workflow_output_var_uid = '%s#output/%s' % (flow_uid, output['name'])
            step_var_uid = '%s#output/%s/%s' % (flow_uid, step_name, step_output_name)

            transformation = {}

            if 'outputBinding' in output:
                outputBinding = output['outputBinding'].copy()

                transformation['inputs'] = {
                    ref_input_name: dynamic_vars[ref_input_name] \
                    for ref_input_name in Cwl.discover_references_to_variables(outputBinding, ['valueFrom', 'glob'])
                }

                outputBinding['object-type'] = output['type']
                transformation['compute'] = outputBinding
                transformation['producer-uid-component'] = workflow_output_var_uid
                transformation = {'cwl-interpreter': transformation}

            self.var_uid_built_from[workflow_output_var_uid] = (step_var_uid, transformation)

        return ZWorkflow(
            flow_uid=flow_uid, cwl_id=cwl_uid, replica_idx=replica_idx, tool_blueprint=workflow,
            input_schemas=inputs_record_schema, output_schemas=outputs_record_schema,
            dynamic_vars=dynamic_vars, hints=hints, requirements=requirements
        )

    def digest_commandline_tool(
            self,  # type: Cwl
            cmdline,  # type: CommandLineTool
            dynamic_vars,  # type: Dict[str, str]

            # VV: These parameters indicate inherited configuration from some WorkflowStep/Workflow
            inputs_record_schema=None,  # type: Optional[Dict[str, Any]]
            outputs_record_schema=None,  # type: Optional[Dict[str, Any]]
            parent_hints=None,  # type: Optional[List[Dict[str, str]]]
            parent_requirements=None,  # type: Optional[List[Dict[str, str]]]
            replica_idx=None,  # type: Optional[int]
            step_name=None,  # type: Optional[str]
    ):  # type: (...) -> ZCommandLineTool

        """
        VV: @tag FlowLikeDependency
        producers: Maps input names to specific outputs of Producers (instantiated CommandLineTools)
        subjects: Maps input names to specific outputs of Subjects (instantiated CommandLineTools)
        """
        # VV: @tag CommandLineToolInstanceUID
        cwl_id = cmdline.metadata['id']
        moduleLogger.info("Started Processing commandlinetool %s" % cwl_id)
        moduleLogger.info('Dynamic vars: %s' % pprint.pformat(dynamic_vars))
        
        flow_uid = self._generate_uid_for_tool(cmdline.metadata['id'], replica_idx)

        dynamic_vars = dynamic_vars or {}

        inputs_record_schema = inputs_record_schema or cmdline.inputs_record_schema
        outputs_record_schema = outputs_record_schema or cmdline.outputs_record_schema

        self.component_outputs[flow_uid] = set()

        requirements = override_dict(
            cmdline.requirements, parent_requirements, keys_allow_many=[self.featureEnvVar]
        )

        may_be_null = self._digest_tool_dynamic_vars(
            flow_uid, dynamic_vars, cmdline, inputs_record_schema
        )

        if may_be_null:
            self.log.info("Tool %s has Null inputs: %s" % (flow_uid, may_be_null))

        # VV: @tag FlowUID
        # VV: Map an input object to the flow_uid of the components that need to be finished before
        #     the input object can be evaluated
        fed_values = set(dynamic_vars.keys()).difference(set(may_be_null))

        producers = {
            key: self.get_producers_of_var_uid(dynamic_vars[key]) for key in fed_values
        }  # type: Dict[str, List[str]]

        subjects = {}  # type: Dict[str, List[str]]

        self.log.info("Producers of %s are %s" % (flow_uid, dumps_json(producers)))

        hints = override_dict(cmdline.hints, parent_hints, keys_allow_many=[Cwl.extLSFBackend])
        hints, repeat_interval = self._digest_hint_observe(flow_uid, cwl_id, hints, subjects, producers)

        # VV: Register step dependencies (connect current Tool with its producers/subjects)
        dependencies_to_producers = set()  # type: Set[DependencyInfo]
        dependencies_to_subjects = set()  # type: Set[DependencyInfo]

        for acc, collection in [
            (dependencies_to_producers, producers),
            (dependencies_to_subjects, subjects)
        ]:
            temp = set()
            for group in list(collection.values()):
                temp.update(set(group))

            acc.update(set([self.step_dependencies[comp_flow_uid] for comp_flow_uid in temp]))

        # VV: Build the Dependency graph AFTER we sort out which of the Producers are in fact Subjects
        #     via the `ObserveInput` hint (see above)
        # set(map(lambda input_name: self.step_dependencies[subjects[input_name].producer_uid], subjects))
        dependency = DependencyInfo(
            uid=flow_uid,
            producers=dependencies_to_producers,
            subjects=dependencies_to_subjects,
            # VV: @tag BackPatchDependencyInfo
            # VV: FIXME HACK We'll backpatch this info before returning from this method
            instance=None
        )

        self.step_dependencies[flow_uid] = dependency
        inputs = inputs_record_schema['fields']
        # VV: Filter-out any pessimistic dependencies to upper-level components (components that
        #     instantiated before this Workflow instance whose outputs are not actually consumed/observed
        #     by any of the tools defined by this workflow)

        # VV: Fill in self.component_outputs with information about the outputs of this CommandLineTool instance

        outputs = outputs_record_schema.get('fields', [])
        for output in outputs:
            assert 'type' in output
            output_name = output['name']
            # VV: tool_instance is back-patched once all of the OutputInfo objects have been created
            output_info = OutputInfo(output_name, output, tool_instance=None)
            self.component_outputs[flow_uid].add(output_info)

        inputs = inputs_record_schema.get('fields', [])

        for key in dynamic_vars:
            # VV: Skip null-typed inputs
            if dynamic_vars[key] is None:
                continue

            input_schema = [inp for inp in inputs if inp['name'] == key][0]
            var_uid = '%s#input/%s' % (flow_uid, key)
            transformation = {}

            if 'inputBinding' in input_schema:
                inputBinding = input_schema['inputBinding'].copy()
                transformation['inputs'] = {
                    ref_input_name: dynamic_vars[ref_input_name] \
                    for ref_input_name in Cwl.discover_references_to_variables(inputBinding, ['valueFrom'])
                }
                inputBinding['object-type'] = input_schema['type']
                transformation['compute'] = inputBinding
                transformation = {'cwl-interpreter': transformation}

            self.var_uid_built_from[var_uid] = (dynamic_vars[key], transformation)

        # VV: @tag BackPatchDependencyInfo
        instance = ZCommandLineTool(
            flow_uid=flow_uid,
            cwl_id=cwl_id,
            tool_blueprint=cmdline,
            dynamic_vars=dynamic_vars,
            hints=hints,
            requirements=requirements,
            dependency_info=self.step_dependencies[flow_uid],
            component_outputs=self.component_outputs[flow_uid],
            repeat_interval=repeat_interval,
            replica_idx=replica_idx,
            input_schemas=inputs,
            output_schemas=outputs,
            step_name=step_name
        )
        dependency.instance = instance
        self.inst_cmd[instance.flow_uid] = instance

        # VV: Backpatch Instance to OutputInfo for ZCommandLineTool outputs
        for output in instance.component_outputs:
            output.instance = instance

            var_uid = '%s#output/%s' % (flow_uid, output.output_name)
            assert var_uid not in self.var_uid_to_process
            self.var_uid_to_process[var_uid] = instance

            schema = output.output_record_schema.copy()

            try:
                del schema['_tool_entry']
            except KeyError:
                pass

            transformation = {}

            if 'outputBinding' in schema:
                outputBinding = schema['outputBinding'].copy()

                transformation['inputs'] = {
                    ref_input_name: dynamic_vars[ref_input_name] \
                    for ref_input_name in Cwl.discover_references_to_variables(outputBinding, ['valueFrom', 'glob'])
                }
                outputBinding['object-type'] = schema['type']
                transformation['compute'] = outputBinding
                transformation['producer-uid-component'] = flow_uid
                transformation = {'cwl-interpreter': transformation}

            # VV: The input flowuid is set to None to indicate that this var_uid is being generated here
            self.var_uid_built_from[var_uid] = (None, transformation)

        return instance

    def build_transformations_graph(self, var_uid):
        graph = DiGraph()

        unknown = [var_uid]
        moduleLogger.info("Build transformation graph for %s (%s)" % (
            var_uid, self.var_uid_built_from[var_uid]
        ))
        while unknown:
            var_uid = unknown.pop(0)

            source, transformation = self.var_uid_built_from[var_uid]
            if source is None and transformation == {}:
                moduleLogger.debug('Skipping empty %s' % var_uid)
                continue

            info = self.track_down_var_uid_producer_info(var_uid)

            graph.add_node(var_uid)

            inputs = []
            # VV: `inputs` maps an input object name to the var-uid that builds it
            input_objects = info.get('inputs', {})  # type: Dict[str, str]

            inputs.extend(list(input_objects.values()))
            inputs.extend(info.get('gather', []))
            inputs.append(info.get('producer-uid-interpreter', None))
            inputs.extend(info.get('cwl-gather-from-uids', {}).get('from', []))
            cwl_interpreter_uid = info.get('cwl-interpreter-uid', None)
            if cwl_interpreter_uid != var_uid:
                inputs.append(cwl_interpreter_uid)
            inputs = [input_uid for input_uid in inputs if input_uid is not None]

            # VV: Filter out any duplicates
            inputs = list(set(inputs))

            edges = [(node, var_uid) for node in inputs]
            graph.add_edges_from(edges)
            unknown.extend([node for node in inputs if graph.has_node(node) is False])

        moduleLogger.debug('Sorted var-uids:')
        for node in topological_sort(graph):
            moduleLogger.debug('  %s' % node)
        moduleLogger.debug('  ---')

        return graph

    def track_down_var_uid_producer_info(
            self,  # type: Cwl
            var_uid,  # type: str
    ):
        # type: (...) -> Dict[str, str]
        """Find generator of `var_uid` (or job-order parameter).
        """
        transformation = {}
        producer_flow_uid = var_uid

        moduleLogger.debug("Tracking down %s" % var_uid)

        while transformation == {} and producer_flow_uid is not None:
            producer_flow_uid, transformation = self.var_uid_built_from[producer_flow_uid]

        transformation = copy.deepcopy(transformation)

        if 'job-order' in transformation:
            producer_info = {
                'job-order-object-name': transformation['job-order'][0],
                'job-order-object': transformation['job-order'][1],
            }
            return producer_info

        if 'cwl-interpreter' in transformation:
            transformation = transformation['cwl-interpreter']

            ret = {
                'cwl-interpreter-uid': var_uid,
                'inputs': transformation['inputs'],
                'compute': transformation['compute']
            }

            if producer_flow_uid is not None:
                ret['producer-uid-interpreter'] = producer_flow_uid

            if transformation.get('producer-uid-component') is not None:
                ret['producer-uid-component'] = transformation['producer-uid-component']

            return ret

        if 'cwl-gather-from-uids' in transformation:
            transformation['cwl-interpreter-uid'] = var_uid
            return transformation

        msg = 'Cannot handle var uid %s with producer %s and transformation %s' % (
            var_uid, producer_flow_uid, transformation,
        )
        moduleLogger.critical(msg)
        raise NotImplementedError(msg)
