# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# coding=UTF-8
#
# IBM Confidential
# OCO Source Materials
# 5747-SM3
# (c) Copyright IBM Corp. 2019
# The source code for this program is not published or otherwise
# divested of its trade secrets, irrespective of what has
# been deposited with the U.S. Copyright Office.
# Author(s): Vassilis Vassiliadis
# Contact: Vassilis.Vassiliadis@ibm.com

from __future__ import print_function

import os
import shutil
import tempfile

from experiment.model.frontends.cwl import Cwl
from experiment.runtime.interpreters.cwl import CWLUnsupportedType
from experiment.runtime.interpreters.cwl_cmdline import (CWLArray, CWLDirectory,
                                                         CWLFile, CWLFloat, CWLInt,
                                                         CWLNull, CWLString,
                                                         commandlinebinding_to_string,
                                                         cwl_load_contents,
                                                         object_to_type, validate_type)

from .reactive_testutils import logger


def test_match_type():
    cwl_type = [
        'null',
        'int',
        'File',
        {
            'type': 'array',
            'items': [
                'string',
                'File'
            ]
        }
    ]

    assert validate_type(CWLInt, cwl_type)

    type_array_file = {
        'type': 'array',
        'items': [
            'File'
        ]
    }

    assert validate_type(type_array_file, cwl_type)

    assert validate_type(CWLString, cwl_type) is False

    type_invalid_array_int = {
        'type': 'array',
        'items': [
            'int'
        ]
    }

    assert validate_type(type_invalid_array_int, cwl_type) is False


def test_extract_type():
    def check_invalid(type_invalid):
        try:
            object_to_type(type_invalid)
        except CWLUnsupportedType:
            pass
        else:
            raise Exception("Should not have considered %s as valid" % type_invalid)

    type_null = None
    type_int = 1
    type_float = 1.0
    type_str = "hello world"
    type_array_int = [
        1, 2, 3
    ]
    type_array_mixed_int_str = [
        1, 'two', 3
    ]
    type_file = {
        'class': CWLFile
    }
    type_directory = {
        'class': CWLDirectory
    }
    type_invalid = {
        'class': "I don't know boss, this looks fake to me"
    }
    type_invalid2 = {
        'definitely not class': 'File'
    }
    type_array_mixed_int_file = [
        type_int,
        type_file
    ]

    assert object_to_type(type_null) == CWLNull
    assert object_to_type(type_int) == CWLInt
    assert object_to_type(type_float) == CWLFloat
    assert object_to_type(type_str) == CWLString
    assert object_to_type(type_array_int) == {
        'type': CWLArray,
        'items': (
            CWLInt,
        )
    }

    assert object_to_type(type_array_mixed_int_str) == {
        'type': CWLArray,
        'items': (
            CWLInt,
            CWLString,
        )
    }

    assert object_to_type(type_file) == CWLFile
    assert object_to_type(type_directory) == CWLDirectory

    assert object_to_type(type_array_mixed_int_file) == {
        'type': CWLArray,
        'items': (
            CWLFile,
            CWLInt,
        )
    }

    for invalid in [type_invalid, type_invalid2]:
        check_invalid(invalid)

    assert validate_type(object_to_type(type_file), CWLFile)


def test_commandlinebinding_string():
    self = 'world'

    inputs = {
        'world': self
    }

    cmdlinebinding = {
        'name': 'world',
        'prefix': '--hello',
        'valueFrom': '$(self)'
    }

    ret = commandlinebinding_to_string(cmdlinebinding, inputs)

    assert ret == '--hello world'


def test_commandlinebinding_string_no_separate():
    self = 'world'

    inputs = {
        'world': self
    }

    cmdlinebinding = {
        'name': 'world',
        'prefix': '--hello=',
        'valueFrom': '$(self)',
        'separate': False,
    }

    ret = commandlinebinding_to_string(cmdlinebinding, inputs)

    assert ret == '--hello=world'


def test_commandlinebinding_argument():
    inputs = {
        'hello': 'hello',
        'world': 'world',
    }

    cmdlinebinding = {
        'name': None,
        'prefix': '--',
        'valueFrom': '$(inputs.hello) $(inputs.world)',
        'separate': False,
    }

    ret = commandlinebinding_to_string(cmdlinebinding, inputs)

    assert ret == '--hello world'

def test_javascript_expression():
    tmp_dir = os.path.abspath(tempfile.mkdtemp())

    try:
        tmp_file = tempfile.NamedTemporaryFile(dir=tmp_dir, delete=False)
        msg = 'Hello world!'
        with tmp_file as f:
            f.write(msg)

        path = os.path.abspath(f.name)
        cwl_file = {
            'class': CWLFile,
            'path': path,
            'location': 'file://%s' % path
        }

        cwl_directory = {
            'class': CWLDirectory,
            'path': tmp_dir,
            'location': 'file://%s' % tmp_dir
        }

        assert object_to_type(cwl_file) == CWLFile
        assert object_to_type(cwl_directory) == CWLDirectory

        inputs = {
            'str': "world!",
            'number': 1,  # VV: This is an integer
            'decimals': 1.23,  # VV: This is a float
            'file': cwl_file,
            'dir': cwl_directory,
        }

        assert commandlinebinding_to_string({
            'name': 'file',
            'loadContents': True,
            'valueFrom': """${ return self.contents }""",
        }, inputs) == msg

        assert commandlinebinding_to_string({
            'name': 'file',
            'loadContents': True,
            'valueFrom': """${ return self.contents }""",
        }, inputs) == msg

        assert commandlinebinding_to_string({
            'name': 'file',
            'loadContents': True,
            'valueFrom': """$(self.contents.split(' ')[0]) world!""",
        }, inputs) == msg

        assert commandlinebinding_to_string({
            'name': 'file',
            'loadContents': True,
            'valueFrom': """$(self.contents.split(' ')[0]) $(inputs.str)""",
        }, inputs) == msg

        assert commandlinebinding_to_string({
            'name': 'file',
            'loadContents': True,
            'valueFrom': """$(self.contents.split(' ')[0]) $(inputs.number)""",
        }, inputs) == "Hello 1"

        # assert expression_to_string(cwl_file, inputs,
        #                             """$(self.contents.split(' ')[0]) $(inputs.number)""") == "Hello 1"

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_gen_file_directory():
    tmp_dir = tempfile.mkdtemp()

    try:
        tmp_file = tempfile.NamedTemporaryFile(dir=tmp_dir, delete=False)
        msg = 'Hello world!'
        with tmp_file as f:
            f.write(msg)
        path = os.path.abspath(f.name)
        cwl_file = {
            'class': CWLFile,
            'path': path,
            'location': 'file://%s' % path
        }

        cwl_directory = {
            'class': CWLDirectory,
            'path': tmp_dir,
            'location': 'file://%s' % tmp_dir
        }
        assert object_to_type(cwl_file) == CWLFile
        assert object_to_type(cwl_directory) == CWLDirectory

        cwl_load_contents(cwl_file)

        assert cwl_file['contents'] == msg
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_complex_javascript():
    function = """${
function trim_all(buffer) {
  let n = buffer.length;

  for (var i=0; i<n; ++i) {
    buffer[i] = buffer[i].trim();
  }

  return buffer;
}

let lines = self.contents.split("\\n");

let column_names = lines[0].split(";");
trim_all(column_names);
let match_name = inputs.target;

const num_lines = lines.length;

for (var i=1; i<num_lines; i++) {
  columns = trim_all(lines[i].split(";"));
  if ( columns[0] == match_name ) {
    return columns[1];
  }
}

return null;
}"""
    csv_contents = """Name;Power level
Goku; 9001
Vegetta; 2000
Krillin; 1
"""
    inputs = {
        'target': 'Goku',
        'diff': {
            'class': CWLFile,
            "contents": csv_contents
        },
        'number': 7000,
    }

    assert commandlinebinding_to_string({
        'name': 'diff',
        'valueFrom': function,
    }, inputs) == "9001"


def test_01_cmd_blueprint(output_dir):
    contents_cwl_main = """
#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: echo
arguments:
  - position: 1
    prefix: '-n '
    separate: false
    valueFrom: world
  - hello

inputs:
  out_file:
    type: string
    inputBinding:
      separate: false 
      position: 2
      prefix: >
outputs:
  out:
    type: File
    outputBinding:
      glob: $(inputs.out_file)
"""
    contents_job_order = """out_file: Hello world!
"""

    files = {
        'main.cwl': contents_cwl_main,
        'job_order.yml': contents_job_order,
    }

    paths = { }

    for name in files:
        contents = files[name]
        path = os.path.join(output_dir, name)

        paths[name] = path

        with open(path, 'w') as f:
            f.write(contents)

    # ----------------

    cwl = Cwl.process_cwl_doc(paths['main.cwl'], [paths['job_order.yml']])

    instance = list(cwl.inst_cmd.values())[0]
    bindings  = instance.preprocess_cwlcmdline(cwl)

    input_sources = {
        input_name: cwl.track_down_var_uid_producer_info(
            instance.dynamic_vars[input_name]
        ) for input_name in instance.dynamic_vars
    }

    import pprint
    logger.warn(pprint.pformat(instance.__dict__))

    print('Bindings')
    for name in bindings:
        print(('Name: %s' % name))
        pprint.pprint(bindings[name], indent=2)
        if name in input_sources:
            print('Source:', pprint.pformat(input_sources[name]))

    assert bindings == {
        'out_file': {'name': 'out_file',
                      'position': 2,
                      'prefix': '',
                      'separate': False,
                      'shellQuote': True},
        'zz_arg0': {'name': 'zz_arg0',
                     'position': 1,
                     'prefix': '-n ',
                     'separate': False,
                     'shellQuote': True,
                     'valueFrom': 'world'},
        'zz_arg1': {'name': 'zz_arg1',
                     'position': 0,
                     'separate': True,
                     'shellQuote': True,
                     'valueFrom': 'hello'}
    }

    assert input_sources == {'out_file': {'job-order-object': 'Hello world!',
              'job-order-object-name': 'out_file'}}

    blueprints, input_sources = instance.preprocess_output_cwl(cwl)

    print('Blueprints')
    for name in blueprints:
        print(('Name: %s' % name))
        pprint.pprint(blueprints[name], indent=2)

    assert blueprints == {
        'out': {
            'name': 'out',
            'outputBinding': {
                'glob': '$(inputs.out_file)'
            },
            'type': 'File'
        }
    }

    assert input_sources == {'out': {'out_file': {'job-order-object': 'Hello world!',
                       'job-order-object-name': 'out_file'}}}
