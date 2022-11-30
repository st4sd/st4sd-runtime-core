#! /usr/bin/env python
# coding=UTF-8
# Copyright IBM Inc. 2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis

from __future__ import print_function

import argparse
import os
import pprint
import traceback
import uuid
from typing import Any, Dict, List, Optional, Tuple

import yaml

from experiment.runtime.interpreters.cwl import (evaluate_javascript_expression,
                                                 expand_object, expression_to_legs,
                                                 object_to_string, object_to_type,
                                                 validate_type)

"""
VV: Schemas (see #405 for more information)

## Expression (https://www.commonwl.org/v1.0/Workflow.html#Expressions)

expression: string

An expression can contain multiple JS code fragments in the form of a single JS statement 
wrapped in $() or a multi-line body of a JS function wrapped in ${} which is expected to return a JSON object.

## CommandLineArg

CommandLineArg:
  name: str
  <fields of CommandLineBinding|Argument>  

## CommandLineBinding (https://www.commonwl.org/v1.0/CommandLineTool.html#CommandLineBinding)

CommandLineBinding:
  name: str | None
  loadContents?: bool
  position?: int
  prefix?: str
  separate?: bool
  itemSeparator?: bool
  valueFrom?: expression
  shellQuote?: bool

## Argument (https://www.commonwl.org/v1.0/CommandLineTool.html#CommandLineTool)

Argument: 
  <CommandLineBinding fields>

## Blueprint:

Blueprint:
  name: CommandLineArg
"""

# VV: This is also part of CWL-interpreter I need to create a CWL-utility library so that both interpreters
#     have a common pool of utility functions

CWLNull, CWLInt, CWLString, CWLFloat, CWLFile, CWLDirectory, CWLArray = (
    'null', 'int', 'string', 'float', 'File', 'Directory', 'array'
)

# VV: NOTE Keep in mind that CWLArray IS NOT EXPLICITLY RETURNED by object_to_type(),
#     this is why it's not in this tuple
CWL_SUPPORTED_TYPES = (
    CWLInt, CWLString, CWLFloat, CWLFile, CWLDirectory
)


def expression_to_parts(self, inputs, expression, invoke_javascript=True):
    def subexpr_to_string(sub_expr, do_invoke=invoke_javascript):
        if sub_expr.startswith('$(') or sub_expr.startswith('${'):
            if do_invoke:
                evaluated = evaluate_javascript_expression(self, inputs, sub_expr)
                return evaluated
        else:
            return sub_expr

    legs = expression_to_legs(expression)

    ret = list(map(subexpr_to_string, legs))

    return ret
#####################


def cwl_load_contents(cwlobject):
    assert validate_type(object_to_type(cwlobject), CWLFile)

    with open(cwlobject['path'], 'r') as f:
        contents = f.read(64 * 1024)

    cwlobject['contents'] = contents


def validate_only_defined_once(dictionaries):
    # type: (List[Dict[str, Any]]) -> None
    # VV: Ensure that an input is ONLY defined once

    dictionaries = [set(col.keys()) for col in dictionaries]

    multiple_definitions = set()

    for i, coll in enumerate(dictionaries):
        rem = dictionaries[i + 1:]
        for future_coll in rem:
            multiple_definitions.update(coll.intersection(future_coll))

    if multiple_definitions:
        raise Exception("Input variables defined multiple times: %s" % multiple_definitions)


def valid_cwl_interpreter_output(path_value, input_errors=None):
    # type: (str, Optional[List[Any]]) -> Optional[Dict[str, Any]]
    # VV: Populate inputs with objects produced by some other cwl-interpreter
    input_errors = input_errors if input_errors is not None else []

    if os.path.exists(path_value):
        try:
            with open(path_value, 'r') as f:
                cwl_object = yaml.safe_load(f)
        except:
            error_desc = "Unable to parse output-YAML file %s (%s)" % (
                path_value, traceback.format_exc()
            )
            if input_errors is not None:
                input_errors.append(error_desc)
                return None
            else:
                raise Exception(error_desc)
        return cwl_object


def parse_arguments():
    # type: () -> Tuple[Dict[str, Any], Dict[str, Any], List[Any]]

    parser = argparse.ArgumentParser()

    parser.add_argument('-i', '--input', action='append', nargs=2,
                        metavar=('INPUT_NAME', 'PATH_VALUE'),
                        help="Point to CWD of cwl-interpreter which generates value of INPUT_NAME",
                        required=False, default=[])

    parser.add_argument('-c', '--constant_input', action='append', nargs=2,
                        metavar=('INPUT_NAME', 'PATH_VALUE'),
                        help="Populate INPUT_NAME with YAML object located at PATH_VALUE",
                        required=False, default=[])

    parser.add_argument('-b', '--blueprint', help='Path to YAML file that contains the blueprint', default=None)
    parser.add_argument('-r', '--raw_blueprint', help='Blueprint contents encoded in YAML', default=None)
    parser.add_argument('-f', '--fake', help="This is a hack! it lets you inject fake dependencies to this component",
                        action='append')
    args = parser.parse_args()

    blueprint = None
    if args.blueprint is not None:
        with open(args.blueprint, 'r') as f:
            blueprint = yaml.safe_load(f)
    elif args.raw_blueprint is not None:
        blueprint = yaml.safe_load(args.raw_blueprint)

    assert blueprint is not None

    input_args = dict([(key, path) for (key, path) in args.input])

    constant_args = dict([(key, path) for (key, path) in args.constant_input])

    validate_only_defined_once([input_args, constant_args])

    input_objects = {}

    input_errors = []

    for input_name in input_args:
        input_objects[input_name] = valid_cwl_interpreter_output(input_args[input_name], input_errors)

    for input_name in constant_args:
        path_value = constant_args[input_name]
        try:
            with open(path_value, 'r') as f:
                cwl_object = yaml.safe_load(f)
        except:
            error_desc = "Unable to parse YAML file %s (%s)" % (
                path_value, traceback.format_exc()
            )
            input_errors.append(error_desc)
        else:
            input_objects[input_name] = cwl_object

    return blueprint, input_objects, input_errors


def commandlinebinding_to_string(cmdline_binding, inputs):
    # type: (Dict[str, Any], Dict[str, Any]) -> str
    input_object_name = cmdline_binding.get('name')
    self = None

    print("Converting %s" % pprint.pformat(cmdline_binding))
    if inputs:
        for name in inputs:
            print('  Input: %s=%s' % (name, pprint.pformat(inputs[name], indent=2)))

    def extract_option(key, dictionary=None, default_value=None):
        # type: (str, Optional[Dict[str, Any]], Optional[Any]) -> Any
        if dictionary is None:
            dictionary = cmdline_binding
        return dictionary.get(key, default_value)

    load_contents = extract_option('loadContents', default_value=False)
    position = extract_option('position', default_value=0)
    prefix = extract_option('prefix', default_value=None)
    separate = extract_option('separate', default_value=True)
    item_separator = extract_option('itemSeparator')
    value_from = extract_option('valueFrom')
    shell_quote = extract_option('shellQuote', default_value=True)

    value = None

    # VV: Order of operations: loadContents, valueFrom
    self = inputs.get(input_object_name)

    if load_contents:
        cwl_load_contents(self)

    if value_from is not None:
        value = expression_to_parts(self=self, inputs=inputs, expression=value_from, invoke_javascript=True)

        # VV: an expression can consist of many $(), ${} objects so convert all of them into strings and
        #     just concatenate the strings to produce a single one
        converted = [object_to_string(obj, prefix='', item_separator=item_separator, separate=separate) for obj in value]

        value = ''.join(converted)

        # VV: Now treat the result of the expression as an object and apply the `prefix` and `separate` rules
        value = object_to_string(value, prefix=prefix, separate=separate)
    else:
        value = object_to_string(self, prefix=prefix, item_separator=item_separator, separate=separate)

    return value


def main():
    path_cwl_errors = 'cwl_errors.yaml'
    if os.path.exists(path_cwl_errors):
        os.remove(path_cwl_errors)

    blueprint, inputs, input_errors = parse_arguments()

    if input_errors:
        print(pprint.pformat(input_errors))

        with open(path_cwl_errors, 'w') as f:
            yaml.safe_dump(input_errors, f)

    print('Inputs before expansion')
    pprint.pprint(inputs)

    for input_name in inputs:
        inputs[input_name] = expand_object(inputs[input_name])

    print('Inputs after expansion')
    pprint.pprint(inputs)

    for input_name in inputs:
        path = '%s.txt' % input_name

        if os.path.isfile(path):
            os.remove(path)

    cmdline_strings = dict([
        ('%s.txt' % input_name, commandlinebinding_to_string(blueprint[input_name], inputs)) for input_name in blueprint
    ])

    for file_name in cmdline_strings:
        fake_name = str(uuid.uuid4())
        with open(fake_name, 'w') as f:
            f.write(cmdline_strings[file_name])

        os.rename(fake_name, file_name)


if __name__ == "__main__":
    main()
