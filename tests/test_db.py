# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# coding=UTF-8
#
# IBM Confidential
# OCO Source Materials
# 5747-SM3
# (c) Copyright IBM Corp. 2021
# The source code for this program is not published or otherwise
# divested of its trade secrets, irrespective of what has
# been deposited with the U.S. Copyright Office.
# Author(s): Vassilis Vassiliadis
# Contact: Vassilis.Vassiliadis@ibm.com

import os

import pandas

import experiment.model.frontends.flowir
import experiment.model.graph
import experiment.model.hooks.interface
import experiment.service.db
from .utils import experiment_from_flowir


def test_sanitize_mongodb_documents_no_recursion():
    """Converts a fairly complex dictionary to a properly formatted dictionary that could be inserted into MongoDB"""
    same = {"hello": "world"}

    doc = [{
        "hello": "world",
        "to-str": {
            0: "to-str",
            1: [
                {
                    "0": "no-change",
                    None: "to-str",
                    "to-str": [[[[{0: "to-str"}, 1, 2, None, "hello", same], same], same]],
                    'same': same,
                    5: same
                }]}}]

    print(dir(experiment.service.db.Mongo))
    print(experiment.service.db.__file__)

    sanitized = experiment.service.db.Mongo._mongo_keys_to_str(doc)

    # VV: The only modification we're currently making is to ensure that Keys of dictionaries are Strings
    expected = [{
        "hello": "world",
        "to-str": {
            "0": "to-str",
            "1": [
                {
                    "0": "no-change",
                    "None": "to-str",
                    "to-str": [[[[{"0": "to-str"}, 1, 2, None, "hello", same], same], same]],
                    'same': same,
                    "5": same
                }]}}]

    # VV: assert is not recursion friendly; that said, _preprocess_document works for recursive objects too
    assert sanitized == expected


def test_interface_mongo_documents(output_dir):
    hooks = """
from __future__ import annotations
from typing import List, Union, Dict
import os
import pandas

def get_properties(property_name:str, property_output_file: str, input_id_file: str) -> pandas.DataFrame:
    return pandas.read_csv(property_output_file, sep=';', index_col=False)

"""

    flowir = """
interface:
  description: Computes the band-gap
  inputSpec:
    hasAdditionalData: false
    namingScheme: SMILES
    inputExtractionMethod:
      csvColumn:
        source:
          path: data/smiles.csv
        args:
          column: smiles

  propertiesSpec:
    - name: BandGap
      propertyExtractionMethod:
        csvDataFrame:
          source:
            keyOutput: BandGap
    - name: LambdaMax
      propertyExtractionMethod:
        hookGetProperties:
          source:
            keyOutput: LambdaMax

output:
  BandGap:
    data-in: stage0.simulate/band-gap.csv:ref
  LambdaMax:
    data-in: stage0.simulate/lambda-max.csv:ref
  HelloWorld:
    data-in: stage0.hello-world:output
  
components:
- name: simulate
  command:
    executable: echo
    arguments: super smart simulation
- name: hello-world
  command:
    executable: echo
    arguments: I produce a key output but it is not associated with any key properties
- name: dummy
  command:
    executable: echo
    arguments: I do not produce key outputs
"""
    manifest = """
    hooks: hooks:copy
    """

    # VV: Some start with S others with A. LambdaMax handles all, band-gap only those that start with A
    smiles = ['S0', 'S1', 'A0', 'A1']
    csv_smiles = "index;smiles\n" + "\n".join((f"{idx};{s}" for (idx, s) in enumerate(smiles)))

    exp = experiment_from_flowir(flowir, output_dir, extra_files={
        'hooks/interface.py': hooks,
        'manifest.yaml': manifest,
        'data/smiles.csv': csv_smiles,
    })

    # VV: Register measured properties
    prop_lambda_max = pandas.DataFrame.from_dict({
        "input-id": smiles,
        "LambdaMax": [1, 2, 3, 4],
        "label": [0, 1, 2, 3]})
    prop_lambda_max.to_csv(
        os.path.join(exp.instanceDirectory.stageDir, 'stage0', 'simulate', 'lambda-max.csv'),
        index=False, sep=";")

    prop_band_gap = pandas.DataFrame.from_dict({
        "input-id": smiles,
        "BandGap": [10, 20, 30, 40],
        "label": [0, 1, 2, 3]})
    prop_band_gap.to_csv(
        os.path.join(exp.instanceDirectory.stageDir, 'stage0', 'simulate', 'band-gap.csv'),
        index=False, sep=";")

    # VV: Process the interface to populate input-ids.json and properties.csv
    interface = exp.configuration.get_interface()
    interface = experiment.model.hooks.interface.Interface(interface=interface, label="FlowIR.interface")
    input_ids, additional_data = interface.extract_input_ids(exp=exp)

    exp.set_interface_input_ids(input_ids, store_on_disk=True)
    exp.set_interface_additional_input_data(additional_data, store_on_disk=True)
    exp.configuration.store_unreplicated_flowir_to_disk()

    measured = interface.extract_properties(exp=exp, input_ids=input_ids)
    exp.set_measured_properties(measured, True)

    doc_exp = [x for x in exp.generate_document_description("ci-cd") if x['type'] == 'experiment'][0]

    # VV: Finally, test the `interface` keys in the experiment and component MongoDocuments
    exp_interface = exp.configuration.get_interface()
    exp_interface['outputFiles'] = sorted([
        os.path.join("output", "input-ids.json"),
        os.path.join("output", "properties.csv"),
    ])

    assert doc_exp["interface"] == exp_interface