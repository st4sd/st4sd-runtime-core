# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import print_function
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    pass
import os
from . import utils
import hashlib


def test_custom_memoization_hash(output_dir):
    dummy_file = os.path.join(output_dir, 'fake')

    file_reference_string = ':'.join((dummy_file, 'ref'))

    flowir = """
    components:
    - name: hello
      stage: 0
      command:
        executable: echo
        arguments: 'hello world >file.txt && echo hi>file2.txt'
      workflowAttributes:
        memoization:
          embeddingFunction: |
            return "hello";
    - name: dict_hello
      stage: 0
      command:
        executable: echo
        arguments: 'hello world >file.txt && echo hi>file2.txt'
      workflowAttributes:
        memoization:
          embeddingFunction: |
            return {"hello": "world", "bye": "world"};   
    - name: echo_args 
      stage: 0
      command:
        executable: cat
        arguments: "%s"
      references: 
        - "%s"
      workflowAttributes:
        memoization:
          embeddingFunction: |
            return {
                "executable": executable,
                "arguments": arguments,
                "upstream": upstream,
                "files": files, 
                "files_md5": files_md5,
                "image": image,
                "fuzzy_info": fuzzy_info,
            }
    """ % (file_reference_string, file_reference_string)

    with open(dummy_file, 'wb') as f:
        f.write(b'012345')

    cexp = utils.experiment_from_flowir(flowir, output_dir)
    n_hello = cexp.graph.nodes['stage0.hello']
    n_d_hello = cexp.graph.nodes['stage0.dict_hello']
    n_echo = cexp.graph.nodes['stage0.echo_args']

    hello = n_hello['componentSpecification']  # type: "ComponentSpecification"
    dict_hello = n_d_hello['componentSpecification']  # type: "ComponentSpecification"
    echo = n_echo['componentSpecification']  # type: "ComponentSpecification"

    md5 = hashlib.md5()
    md5.update("hello".encode('utf-8'))
    md5_hash = md5.hexdigest()

    assert hello.memoization_info_fuzzy == "hello"
    assert hello.memoization_hash_fuzzy == '-'.join(('custom', md5_hash))

    assert dict_hello.memoization_info_fuzzy == {"hello": "world", "bye": "world"}
    assert dict_hello.memoization_hash_fuzzy

    assert echo.memoization_info_fuzzy['files'][file_reference_string] == '012345'
