#! /usr/bin/env python
# coding=UTF-8
# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis


from __future__ import print_function

import argparse
import glob
import json
import os
import pprint
import shutil
import sys
import traceback

import experiment.runtime.interpreters.js
import yaml
from six import string_types

import js2py

convert_cwl_object_to_python = experiment.runtime.interpreters.js.convert_js_object_to_python

"""
Recipes:

CWLType: 'null' | 'boolean' | 'int' | 'long' | 'float' | 'double' | 'string' | 'File' | 'Directory' 

InputArraySchema: {
  'type': 'array',
  'items': [CWLType | InputArraySchema]
}

CWLFile: {
  'class': 'File',
  'location': str (location accessible by Flow),
  'path': str (location accessible by COMPONENT),
  'basename': str,
  'dirname': str,
  'nameroot': str,
  'nameext': str,
  'secondaryFiles': [CWLFile | CWLDirectory],
  'contents': str
}

CWLDirectory: {
  'class': 'Directory',
  'location': str,
  'path': str,
  'basename': str,
  'listing': [CWLFile | CWLDirectory]
}

CWLValue: int | string | float | bool | CWLFile | CWLDirectory

Blueprint:

{
  'inputs': {
    ?'types': {
      'input-name': 
        [ CWLType | InputArraySchema ] 
      }  # input-name
    },  # types
    ? 'constants': {
      'input-name': CWLValue | [CWLValue]
    }  # constants
  },  # inputs
  *** VV: 'self' is OPTIONAL ***
  ? 'self': input-name,
  ? 'valueFrom': string,
  ? 'glob': string,
  ? 'loadContents': bool,
  ? 'secondaryFiles': [
    expression
  ]
  ? 'convert': CWLFile | CWLDirectory | CWLInt | CWLString | CWLFLoat
  ? 'copy-to': destination path (can contain ENVIRONMENT VARIABLES)
}

Order of execution: glob, loadContents, outputEval, secondaryFiles, convert, copy-to
"""

CWLNull, CWLInt, CWLString, CWLFloat, CWLFile, CWLDirectory, CWLArray = (
    'null', 'int', 'string', 'float', 'File', 'Directory', 'array'
)

# VV: NOTE Keep in mind that CWLArray IS NOT EXPLICITLY RETURNED by object_to_type(),
#     this is why it's not in this tuple
CWL_SUPPORTED_TYPES = (
    CWLInt, CWLString, CWLFloat, CWLFile, CWLDirectory
)


class CWLUnsupportedType(Exception):
    def __init__(self, cwl_object, object_type):
        self.object_type = object_type
        self.cwl_object = cwl_object
        self.message = "Unsupported type %s for object %s" % (
            pprint.pformat(object_type or "?"), pprint.pformat(cwl_object or "?")
        )


def validate_type(object_type, type_collection):
    if isinstance(type_collection, string_types+(dict,)):
        type_collection = [type_collection]

    return any(
        [object_type_is_instance(object_type, expected_type) for expected_type in type_collection]
    )


def object_type_is_instance(object_type, expected_type):
    if isinstance(object_type, dict):
        assert object_type['type'] == CWLArray
        # VV: object type is an array, but the expected type is a single object
        if isinstance(expected_type, dict) is False:
            return False
        assert expected_type['type'] == CWLArray

        object_items = object_type['items']
        expected_items = expected_type['items']

        valid = all([validate_type(item, expected_items) for item in object_items])

        return valid
    else:
        # VV: object type is a single object, but the expected type is an array
        return object_type == expected_type


def object_to_type(cwlobject):
    if cwlobject is None:
        return CWLNull
    elif isinstance(cwlobject, float):
        return CWLFloat
    elif isinstance(cwlobject, int):
        return CWLInt
    elif isinstance(cwlobject, string_types):
        return CWLString
    elif isinstance(cwlobject, list):
        item_types = set(map(object_to_type, cwlobject))

        cwltype = {
            'type': CWLArray,
            'items': tuple(sorted(item_types))
        }

        return cwltype
    elif isinstance(cwlobject, dict):
        obj_type = cwlobject.get('class', cwlobject.get('type'))
        if obj_type in (CWLFile, CWLDirectory):
            return obj_type
        else:
            print("Object type is %s" % obj_type)
            print("object is %s" % cwlobject)

            raise CWLUnsupportedType(obj_type, cwlobject)

    raise CWLUnsupportedType(type(cwlobject), cwlobject)


def parse_arguments(args=None):
    # VV: --input arguments are meant to connect CWL-interpreters together
    #     --string_input/file_input/directory_input/number_input aim to facilitate
    #                       feeding the output of OTHER kind of interpreters or just plain DataReferences
    #                       to this CWL-interpreter
    parser = argparse.ArgumentParser()

    parser.add_argument('-b', '--blueprint', help="Path to YAML blueprint", required=True)
    parser.add_argument('-i', '--input', action='append', nargs=2,
                        metavar=('INPUT_NAME', 'PATH_VALUE'),
                        help="Path to YAML file containing cwl-description of object INPUT_NAME",
                        required=False, default=[])
    parser.add_argument('-s', '--string_input', action='append', nargs=2,
                        metavar=('INPUT_NAME', 'STRING_VALUE'),
                        help="Fill in INPUT_NAME (which is a string) with a value",
                        required=False, default=[])
    parser.add_argument('-f', '--file_input', action='append', nargs=2,
                        metavar=('INPUT_NAME', 'FILE_PATH'),
                        help="Fill in INPUT_NAME (which is a File) with a path that will construct the CWLFile",
                        required=False, default=[])
    parser.add_argument('-d', '--directory_input', action='append', nargs=2,
                        metavar=('INPUT_NAME', 'FILE_PATH'),
                        help="Fill in INPUT_NAME (which is a Directory) with a path "
                             "that will construct the CWLDirectory",
                        required=False, default=[])
    parser.add_argument('-n', '--number_input', action='append', nargs=2,
                        metavar=('INPUT_NAME', 'NUMBER'),
                        help="Fill in INPUT_NAME (which is a CWLInt/CWLFloat) with a number",
                        required=False, default=[])
    parser.add_argument('-r', '--root_dir', help="Override root directory", default='.')

    parsed = parser.parse_args(args)

    with open(parsed.blueprint, 'r') as f:
        blueprint = yaml.safe_load(f)

    input_args = {
        key: path for (key, path) in parsed.input
    }

    string_input_args = {
        key: value for (key, value) in parsed.string_input
    }

    file_input_args = {
        key: path for (key, path) in parsed.file_input
    }

    directory_input_args = {
        key: path for (key, path) in parsed.directory_input
    }

    number_input_args = {
        key: number for (key, number) in parsed.number_input
    }

    # VV: Ensure that an input is ONLY defined once
    collections = [
        input_args, string_input_args, file_input_args, directory_input_args, number_input_args
    ]

    collections = [set(col.keys()) for col in collections]

    multiple_definitions = set()

    for i, coll in enumerate(collections):
        rem = collections[i + 1:]
        for future_coll in rem:
            multiple_definitions.update(coll.intersection(future_coll))

    if multiple_definitions:
        raise Exception("Input variables defined multiple times: %s" % multiple_definitions)

    input_objects = {}
    input_errors = []

    # VV: Populate inputs with objects produced by some other cwl-interpreter
    for input_name in input_args:
        path_value = input_args[input_name]

        if os.path.exists(path_value):
            with open(path_value, 'r') as f:
                cwl_object = yaml.safe_load(f)

            object_type = object_to_type(cwl_object)

            try:
                expected = blueprint['inputs']['types'][input_name]
            except KeyError:
                input_objects[input_name] = cwl_object
            else:
                if validate_type(object_type, expected) is False:
                    input_errors.append('Invalid type for input %s (expected: %s, got %s, object path: %s)' % (
                        input_name, pprint.pformat(expected), object_type, path_value
                    ))
                else:
                    input_objects[input_name] = cwl_object

        else:
            input_errors.append('Input file %s does not exist' % path_value)

    # VV: Populate string-inputs
    for input_name in string_input_args:
        string_value = string_input_args[input_name]

        try:
            expected = blueprint['inputs']['types'][input_name]
        except Exception:
            input_objects[input_name] = string_value
        else:
            object_type = object_to_type(string_value)
            if validate_type(object_type, expected) is False or expected != CWLString:
                input_errors.append('Invalid type for string-input %s (expected: %s, got %s)' % (
                    input_name, pprint.pformat(expected), string_value
                ))
            else:
                input_objects[input_name] = string_value

    # VV: Populate number-inputs (CWLInt and CWLFloat)
    for input_name in number_input_args:
        number = number_input_args[input_name]

        expected = blueprint['inputs']['types'][input_name]

        if expected == CWLFloat:
            try:
                number = float(number)
            except TypeError:
                number = "Nan: (%s)" % number
        object_type = object_to_type(number)
        if validate_type(object_type, expected) is False or expected not in [CWLInt, CWLFloat]:
            input_errors.append('Invalid type for number-input %s (expected: %s, got %s)' % (
                input_name, pprint.pformat(expected), number
            ))
        else:
            input_objects[input_name] = number

    # VV: Populate file-inputs
    for input_name in file_input_args:
        file_path = file_input_args[input_name]

        cwl_object = path_to_object(file_path)
        cwl_type = object_to_type(cwl_object)
        expected = blueprint['inputs']['types'][input_name]

        object_type = object_to_type(cwl_object)
        if validate_type(object_type, expected) is False or expected != CWLFile:
            input_errors.append('Invalid type for file-input %s (expected: %s, got %s, path %s)' % (
                input_name, expected, cwl_type, file_path
            ))
        else:
            input_objects[input_name] = cwl_object

    # VV: Populate directory-inputs
    for input_name in directory_input_args:
        file_path = directory_input_args[input_name]

        cwl_object = path_to_object(file_path)
        expected = blueprint['inputs']['types'][input_name]

        object_type = object_to_type(cwl_object)
        if validate_type(object_type, expected) is False or expected != CWLDirectory:
            input_errors.append('Invalid type for directory-input %s (expected: %s, path %s)' % (
                input_name, pprint.pformat(expected), file_path
            ))
        else:
            input_objects[input_name] = cwl_object

    constants = blueprint.get('inputs', {}).get('constants', {})

    if constants:
        constants = blueprint['inputs']['constants']

        for input_name in constants:
            input_objects[input_name] = constants[input_name]

    if input_errors:
        with open('cwl_error.yaml', 'w') as f:
            yaml.safe_dump(input_errors, f)

        raise Exception('Error loading/building input objects: %s' % pprint.pformat(input_errors))

    return blueprint, input_objects, parsed.root_dir


def apply_glob(self, inputs, expression, cwd=None, self_type=None):
    if self is not None:
        self_type = object_to_type(self)

    if self_type == CWLFile:
        basedir = cwd or self['dirname']
    elif self_type == CWLDirectory:
        basedir = cwd or self['path']
    else:
        assert False

    glob_expr = expression_to_string(self, inputs, expression)

    return path_to_object(os.path.join(basedir, glob_expr))


def load_contents(self):
    assert object_to_type(self) == CWLFile
    path = self['path']

    if path.startswith('file://'):
        path = path[7:]

    with open(path, 'r') as f:
        contents = f.read(64 * 1024)

    self['contents'] = contents

    return self


def traverse_expression(expression):
    scopes = {
        '{': 0,
        '(': 0,
    }

    end_to_begin = {
        '}': '{',
        ')': '(',
    }

    for idx, char in enumerate(expression):
        if char in scopes:
            scopes[char] += 1
        elif char in end_to_begin:
            scopes[end_to_begin[char]] -= 1

            if sum(scopes.values()) == 0:
                # VV: Return current idx + 1 for )
                return idx + 1
        else:
            if sum(scopes.values()) == 0:
                return None

    return None


def evaluate_javascript_expression(self, inputs, expression):
    try:
        expression = expression.strip()
        if expression.startswith('$('):

            fake_function = """
            function $(self, inputs) {
                return %s;
            }
            """ % expression[2:-1]
        elif expression.startswith('${'):
            fake_function = """
            function $(self, inputs) {
                %s
            }
            """ % expression[2:-1]
        else:
            raise Exception("Unknown JS sub-expression type: %s" % expression)
        try:
            func = js2py.eval_js(fake_function)
        except:
            print("could not process \"%s\"" % expression)
            raise

        cwlobject = func(self, inputs)

        return convert_cwl_object_to_python(cwlobject)
    except:
        print("Failed to convert '%s'" % pprint.pformat(self,indent=2))
        print("inputs: %s" % pprint.pformat(inputs or [], indent=2))
        print("expression: %s" % expression)

        raise


def object_to_string(cwlobject, prefix='', item_separator=' ', separate=True):
    try:
        cwl_type = object_to_type(cwlobject)
    except CWLUnsupportedType as e:
        cwl_type = None

        print("Error when obtaining type")
        print(e.message)
        raise

    if cwl_type in [CWLFile, CWLDirectory]:
        if os.path.exists(cwlobject['path']) is False:
            return ""
        evaluated = cwlobject['path']
    elif isinstance(cwl_type, dict):
        try:
            assert cwl_type['type'] == CWLArray

            def convert_item(item, item_sep=item_separator):
                return object_to_string(item, item_separator=item_sep)

            parts = list(map(convert_item, cwlobject))

            evaluated = item_separator.join(parts)
        except:
            raise NotImplementedError("Converting %s into strings is not supported (%s) (exc: %s)" % (
                cwl_type, pprint.pformat(cwlobject), traceback.format_exc()
            ))
    elif cwl_type in [CWLString, CWLInt, CWLFloat]:
        evaluated = str(cwlobject)
    elif cwl_type is CWLNull:
        evaluated = ''
    elif cwl_type is None:
        evaluated = json.dumps(cwlobject)
    else:
        raise NotImplementedError(
            "Converting %s into strings is not supported (%s)" % (
                cwl_type, pprint.pformat(cwlobject)
            )
        )

    if prefix:
        prefix_separator = ' ' if separate else ''
        evaluated = prefix_separator.join([prefix, evaluated])

    return evaluated


def expression_to_legs(expression):
    legs = []

    output = ""
    max_steps = 100

    while expression and max_steps > 0:
        next_expr = expression.find('$(')
        next_func = expression.find('${')

        interesting_indices = [e for e in [next_expr, next_func] if e > -1]

        if interesting_indices:
            next_js = min(interesting_indices)

            if next_js > 0:
                if expression[next_js-1] == '\\':
                    not_cwl_expression = expression[:next_js+1]
                    remaining = expression[next_js+1:]

                    # VV: `not_cwl_expression` now ends with \$ remove the '\' character
                    not_cwl_expression = ''.join((not_cwl_expression[:-2], '$'))

                    legs.append(not_cwl_expression)
                    expression = remaining
                    continue

            if next_js > 0:
                # VV: There's something else before the first javascript expression
                legs.append(expression[:next_js])
                expression = expression[next_js:]
            else:
                ends_at = traverse_expression(expression[1:])
                legs.append(expression[:ends_at + 1])
                # VV: Skip ) and }
                expression = expression[ends_at + 1:]
        else:
            # VV: No javascript stuff left
            legs.append(expression)
            expression = ""

        max_steps -= 1

    if expression:
        raise Exception("Did not manage to fully process expression, remaining %s" % expression)

    return legs


def expression_to_string(self, inputs, expression, invoke_javascript=True):
    def subexpr_to_string(sub_expr, invoke_javascript=invoke_javascript):
        print("Evaluating: '%s'" % sub_expr)
        if sub_expr.startswith('$(') or sub_expr.startswith('${'):
            if invoke_javascript:
                evaluated = evaluate_javascript_expression(self, inputs, sub_expr)
                try:
                    evaluated = object_to_string(evaluated)
                except:
                    raise NotImplementedError("Failed to convert '%s' for '%s' from expression '%s' with inputs %s" % (
                        evaluated, self, sub_expr, pprint.pformat(inputs)
                    ))


                return evaluated
        else:
            return sub_expr

    legs = expression_to_legs(expression)

    return ''.join(map(subexpr_to_string, legs))


def expand_object(object_description):
    object_description = convert_cwl_object_to_python(object_description)

    try:
        if isinstance(object_description, string_types):
            return object_description
        elif isinstance(object_description, dict):
            if 'type' not in object_description and 'class' not in object_description:
                return object_description

            obj_type = object_description.get('class', object_description.get('type'))
            location = object_description.get('location')
            path = object_description.get('path')

            if obj_type == CWLDirectory:
                obj_dir = path_to_directory(path, location, False)
                object_description.update(obj_dir)
            elif obj_type == CWLFile:
                obj_file = path_to_file(path, location, False)
                object_description.update(obj_file)
        elif isinstance(object_description, list):
            object_description = list(
                map(expand_object, object_description)
            )
    except:
        import traceback
        print("Exception: %s" % traceback.format_exc())
        print("Offending object")
        pprint.pprint(object_description)
        raise


    return object_description

def path_to_object(path):
    if os.path.isfile(path):
        return path_to_file(path)
    elif os.path.isdir(path):
        return path_to_directory(path)
    else:
        return None


def path_to_file(path, location=None, validate=True):
    if location is None:
        location = 'file://%s' % path

    if validate:
        if location.startswith('file://'):
            if os.path.isfile(location[7:]) is False:
                return None
        else:
            raise Exception("Unknown protocol for location: %s" % location)

    if path is None:
        path = location

    if path.startswith('file://'):
        path = path[7:]

    path = os.path.expandvars(path)

    dirname, basename = os.path.split(path)
    nameroot, nameext = os.path.splitext(basename)

    cwl_file = {
        'class': CWLFile,
        'location': location,
        'path': path,
        'dirname': dirname,
        'basename': basename,
        'nameroot': nameroot,
        'nameext': nameext,
    }

    return cwl_file


def path_to_directory(path, location=None, validate=True):
    if location is None:
        location = 'file://%s' % path

    if validate:
        if location.startswith('file://'):
            if os.path.isdir(location[7:]) is False:
                return None
        else:
            raise Exception("Unknown protocol for location: %s" % location)

    if path is None:
        path = location

    if path.startswith('file://'):
        path = path[7:]

    path = os.path.expandvars(path)

    dirname, basename = os.path.split(path)

    cwl_directory = {
        'class': CWLDirectory,
        'location': location,
        'path': path,
        'basename': basename,
    }

    return cwl_directory


def find_secondary_files(self, inputs, secondary_files, cwd=None):
    assert object_to_type(self) == CWLFile

    cwd = cwd or self['dirname']

    expressions = [expression_to_string(self, inputs, expression) for expression in secondary_files]

    globs = list(
        [os.path.join(cwd, path) for path in expressions]
    )

    secondary_files = []

    for glob_exp in globs:
        secondary_files.extend(glob.glob(glob_exp))

    secondary_files = list(map(
        path_to_object, secondary_files
    ))

    secondary_files = [obj for obj in secondary_files if obj is not None]

    self['secondaryFiles'] = secondary_files

    return self


def copy_to_location(self, location):
    if location.startswith('file://'):
        location = location[7:]

    location = os.path.expandvars(location)

    if isinstance(self, string_types):
        raise ValueError('Cannot copy_to_location %s' % self)
    elif isinstance(self, list):
        for k in self:
            copy_to_location(k, location)
    elif isinstance(self, dict):
        obj_type = object_to_type(self)

        src = self.get('path')

        if obj_type == CWLFile:
            print("Copying file '%s' to %s" % (src, location))
            shutil.copy(src, location)
        elif obj_type == CWLDirectory:
            print("Copying directory '%s' to %s" % (src, location))
            dir_name = os.path.split(src)[1]
            new_location = os.path.join(location, dir_name)
            try:
                os.makedirs(new_location)
            except:
                pass
            shutil.copytree(src, new_location)
        else:
            raise ValueError('Cannot copy_to_location %s' % self)
    else:
        raise ValueError('Cannot copy_to_location %s' % self)


def convert_to_type(self, new_type):
    if new_type == CWLFloat:
        return float(self)
    elif new_type == CWLInt:
        return int(self)
    elif new_type == CWLString:
        return str(self)
    elif new_type == CWLFile:
        return path_to_file(self)
    elif new_type == CWLDirectory:
        return path_to_directory(self)
    else:
        raise NotImplementedError("Cannot convert %s to %s" % (
            pprint.pformat(self), new_type
        ))


def apply_blueprint(blueprint, input_objects, input_root_dir):
    self = input_objects[blueprint['self']] if 'self' in blueprint else None

    tasks = [task for task in ['glob', 'loadContents',
                                                    'valueFrom', 'secondaryFiles', 'convert', 'copy-to'] if task in blueprint]

    value_from = blueprint.get('valueFrom', '')

    if value_from:
        legs = expression_to_legs(value_from)
        if len(legs) > 1:
            value_from_task = lambda: expression_to_string(self, input_objects, value_from)
        else:
            value_from_task = lambda: evaluate_javascript_expression(self, input_objects, blueprint['valueFrom'])
    else:
        value_from_task = lambda: expression_to_string(self, input_objects, value_from)

    for task in tasks:
        do_task = {
            'glob': lambda: apply_glob(
                self, input_objects, blueprint['glob'], cwd=input_root_dir, self_type=blueprint.get('object-type', None)
            ),
            'loadContents': lambda: load_contents(self),
            'valueFrom': value_from_task,
            'secondaryFiles': lambda: find_secondary_files(self, input_objects, blueprint['secondaryFiles']),
            'convert': lambda: convert_to_type(self, blueprint['convert']),
            'copy-to': lambda: copy_to_location(self, blueprint['copy-to'])
        }

        self = do_task[task]()

        if self is None:
            break

    return self


def main():
    remove_files = ['cwl_output.yaml', 'cwl_error.yaml']
    for p in remove_files:
        if os.path.exists(p):
            os.remove(p)

    blueprint = {}
    try:
        blueprint, input_objects, input_root_dir = parse_arguments()

        print('Blueprint')
        pprint.pprint(blueprint)

        print('Inputs before expansion')
        pprint.pprint(input_objects)

        for input_name in input_objects:
            input_objects[input_name] = expand_object(input_objects[input_name])

        print('Inputs after expansion')
        pprint.pprint(input_objects)

        self = apply_blueprint(blueprint, input_objects, input_root_dir)

    except:
        tmp_file = 'definitely_not_cwl_error.yaml'
        final_file = 'cwl_error.yaml'
        err = {
            'Exception': traceback.format_exc()
        }
        output_file = sys.stderr
        output_object = err

        print(err['Exception'], file=sys.stderr)
    else:
        tmp_file = 'definitely_not_cwl_output.yaml'
        final_file = 'cwl_output.yaml'
        output_file = sys.stdout
        output_object = self

    try:
        print(object_to_string(output_object), file=output_file, end='')
    except:
        if 'copy-to' in blueprint:
            pass
        else:
            raise

    with open(tmp_file, 'w') as f:
        yaml.safe_dump(output_object, f)

    os.rename(tmp_file, final_file)

    if output_file == sys.stderr:
        exit(1)


if __name__ == '__main__':
    main()
