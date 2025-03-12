#! /usr/bin/env python
# coding=UTF-8
# Copyright IBM Inc. 2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis


from __future__ import print_function

import argparse
import json
import os

import subprocess
import typing

import yaml


"""
VV: There're 2 kinds of supported expressions:

1. $(SINGLE JAVASCRIPT statement which can access inputs via `inputs.<InputName>`)
  - e.g $(inputs.salary * 10), this expects a `salary` number input and it evaluates to a number object
         which is equal to 10 times the value of the input `salary`
2. ${body of function which has exactly 1 parameter `inputs` DO NOT INCLUDE HEADER but include `return` statements}
   - e.g. ${
     let sum = 0;
     for (var i=0; i<inputs.numbers.length); ++i)
       sum += inputs.numbers[i];
     return sum
   } This expects an input number array named `numbers`

Outputs are stored in the --output file
"""


class Javascript:
    NodeJSAvailable = None

    @classmethod
    def execute_js(cls, code: str) -> typing.Any:
        if cls.NodeJSAvailable is False:
            raise ValueError("node js is unavailable")
        marker = "ST4SD_OUTPUT_START_MARKER"

        wrapped_method = f"""
function st4sd_wrapper() {{
{code}
}}

output = st4sd_wrapper();

process.stdout.write("{marker}\\n");
process.stdout.write(JSON.stringify(output));"""

        proc = subprocess.Popen(["node", "-e", wrapped_method], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        exit_code = proc.wait()

        try:
            stdout = proc.stdout.read()
            if isinstance(stdout, bytes):
                stdout = stdout.decode('utf-8')
        except Exception as e:
            stdout = f"unable to get stdout of node - underlying reason {e}"

        try:
            stderr = proc.stderr.read()
            if isinstance(stderr, bytes):
                stderr = stderr.decode('utf-8')
        except Exception as e:
            stderr = f"unable to get stdout of node - underlying reason {e}"

        if exit_code != 0:
            raise ValueError(f"node exited with exit code {exit_code}. "
                             f"Its stdout was:\n{stdout}\n\nIts stderr was:\n{stderr}")

        start = stdout.rfind(marker)
        if start == -1:
            raise ValueError(f"Unable to find the {marker} marker in the stdout of node. "
                             f"Its stdout was:\n{stdout}\n\nIts stderr was:\n{stderr}")

        raw = stdout[start+len(marker):].lstrip("\n")
        return json.loads(raw)


    @classmethod
    def detect_node_js(cls):
        try:
            proc = subprocess.Popen(["node", "--version"],stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            exit_code = proc.wait()
        except FileNotFoundError:
            # VV: There's no node executable on this machine
            return False

        return exit_code==0


if Javascript.NodeJSAvailable is None:
    Javascript.NodeJSAvailable = Javascript.detect_node_js()


def pretty_json(entry):
    return json.dumps(entry, sort_keys=True, indent=4, separators=(',', ': '))


def parse_arguments(args=None):
    # VV: --input arguments are meant to connect JS-interpreters together
    #     --string_input/file_input/directory_input/number_input aim to facilitate
    #                       feeding the output of OTHER kind of interpreters or just plain DataReferences
    #                       to this CWL-interpreter
    parser = argparse.ArgumentParser()

    parser.add_argument('-b', '--blueprint', help='Path to TXT file containing JS expression', default=None)
    parser.add_argument('-e', '--expression', help='Javascript expression $() for SINGLE javascript statements, and '
                                                   '${} for multi-line body of function. In both cases '
                                                   'the javascript code has access to the `inputs` dictionary.',
                        default=None)
    parser.add_argument('-i', '--input', action='append', nargs=2,
                        metavar=('INPUT_NAME', 'PATH_VALUE'),
                        help="Point to CWD of cwl-interpreter which generates value of INPUT_NAME",
                        required=False, default=[])
    parser.add_argument('-s', '--string_input', action='append', nargs=2,
                        metavar=('INPUT_NAME', 'STRING_VALUE'),
                        help="Fill in INPUT_NAME (which is a string) with a value",
                        required=False, default=[])
    parser.add_argument('-f', '--file_input', action='append', nargs=2,
                        metavar=('INPUT_NAME', 'PATH_TO_FILE'),
                        help="Fill in INPUT_NAME with the contents of a file (up to 64k)",
                        required=False, default=[])
    parser.add_argument('-n', '--number_input', action='append', nargs=2,
                        metavar=('INPUT_NAME', 'NUMBER'),
                        help="Fill in INPUT_NAME with a number (int/float)",
                        required=False, default=[])
    parser.add_argument('-o', '--output', help='Path to output YAML file (default: js_output.yml)',
                        default='js_output.yml')
    parsed = parser.parse_args(args)

    try:
        with open(parsed.blueprint, 'r') as f:
            blueprint = f.read()
    except:
        blueprint = parsed.expression

    input_args = {
        key: path for (key, path) in parsed.input
    }

    string_input_args = {
        key: value for (key, value) in parsed.string_input
    }

    number_input_args = {
        key: number for (key, number) in parsed.number_input
    }

    file_input_args = {
        key: path for (key, path) in parsed.file_input
    }
    # VV: Ensure that an input is ONLY defined once
    collections = [
        input_args, string_input_args, number_input_args, file_input_args
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

    # VV: Populate inputs with objects produced by some other cwl-interpreter
    for input_name in input_args:
        path_value = input_args[input_name]

        if os.path.exists(path_value):
            with open(path_value, 'r') as f:
                js_object = yaml.safe_load(f)
            input_objects[input_name] = js_object
        else:
            input_objects[input_name] = None

    # VV: Populate string-inputs
    for input_name in string_input_args:
        string_value = string_input_args[input_name]
        input_objects[input_name] = string_value

    # VV: Populate number-inputs (first try float, then int)
    for input_name in number_input_args:
        number = number_input_args[input_name]
        try:
            number = float(number)
        except TypeError:
            number = int(number)
        input_objects[input_name] = number

    # VV: Read up to 64KB for file inputs
    for input_name in file_input_args:
        with open(file_input_args[input_name], 'r') as f:
            input_objects[input_name] = f.read(64 * 1024)

    assert blueprint is not None

    return blueprint, input_objects, parsed.output


def evaluate_javascript_expression(expression, **kwargs):
    lines = [
        f"{key} = {json.dumps(value)}" for key, value in kwargs.items()
    ]

    expression = expression.strip()
    if expression.startswith('$('):
        lines.append(f"return {expression[2:-1]};")
    elif expression.startswith('${'):
        lines.append(expression[2:-1])
    else:
        raise Exception("Unknown JS sub-expression type: %s" % expression)

    code = ";\n".join(lines)

    try:
        ret = Javascript.execute_js(code)
    except Exception as e:
        print(f"could not process \"{expression}\" due to {e}")
        raise

    return ret


def main():
    blueprint, input_objects, final_file = parse_arguments()

    remove_files = [final_file]
    for p in remove_files:
        if os.path.exists(p):
            os.remove(p)

    output_object = evaluate_javascript_expression(blueprint, input=input_objects)
    print(output_object)

    tmp_file = 'definitely_not_js_output.yml'

    with open(tmp_file, 'w') as f:
        yaml.safe_dump(output_object, f)

    os.rename(tmp_file, final_file)


if __name__ == '__main__':
    main()
