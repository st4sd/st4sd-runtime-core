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

import logging
import math
import os
from typing import List

import experiment.model.codes
import experiment.model.errors
import experiment.model.storage
import experiment.runtime.control
import experiment.runtime.errors
import experiment.model.hooks
import experiment.model.graph
import pytest

from .utils import generate_controller_for_flowir
from .utils import experiment_from_flowir

logger = logging.getLogger()
logger.setLevel(20)


def test_input_spec_get_input_ids(output_dir):
    hooks = """
from __future__ import annotations
from typing import List, Union, Dict
import os

import experiment.model.graph
import experiment.model.errors
import pandas


def get_input_ids(input_id_file: str, variables: Dict[str, str]) -> List[str]:
    df = pandas.read_csv(input_id_file, sep=';', index_col=False)
    return df['smiles'].tolist()

"""

    flowir = """
interface:
  description: Computes the band-gap
  inputSpec:
    hasAdditionalData: false
    namingScheme: SMILES
    inputExtractionMethod:
      hookGetInputIds:
        source:
          path: data/smiles.csv

  propertiesSpec: []

components:
  # VV: This component doesn't deal with input systems at all
  - name: hello-world
    command:
      executable:
        echo
      arguments: hello world
"""
    manifest = """
hooks: hooks:copy
"""

    smiles = ['S0', 'S1', 'A0', 'A1']
    smiles = "index;smiles\n" + "\n".join((f"{idx};{s}" for (idx, s) in enumerate(smiles)))

    exp = experiment_from_flowir(flowir, output_dir, extra_files={
        'hooks/interface.py': hooks,
        'manifest.yaml': manifest,
        'data/smiles.csv': smiles,
    })

    interface = exp.configuration.get_interface()
    interface = experiment.model.hooks.interface.Interface(interface=interface, label="FlowIR.interface")
    input_ids, additional_data = interface.extract_input_ids(exp=exp)

    assert input_ids == ['S0', 'S1', 'A0', 'A1']


def test_hooks_all_ok(output_dir):
    hooks = """
from __future__ import annotations
from typing import List, Union, Dict

import pandas

def get_properties(property_name:str, property_output_file: str, input_id_file: str) -> pandas.DataFrame:
    if property_name not in ["BandGap", "LambdaMax"]:
        raise KeyError(f"unknown property name {property_name}")
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
    data-in: stage0.comp-band-gap/band-gap.csv:ref
  LambdaMax:
    data-in: stage0.comp-lambda-max/lambda-max.csv:ref
        
components:
  - name: comp-band-gap
    command:
      executable: python
      expandArguments: none
      arguments: |
        <<'EOF' 
        import pandas as pd
        # VV: This will expand to the path
        path = "data/smiles.csv:ref"
        property = "%(property)s"

        df = pd.read_csv(path, sep=';')

        data = {'input-id':[], property: []}

        for idx, row in df.iterrows():
            # VV: Only process "Axxxx" SMILES
            if row['smiles'].startswith('A'):
                data['input-id'].append(row['smiles'])
                data[property].append((idx+1)*%(factor)s)

        files = {'lambdamax': 'lambda-max', 'bandgap': 'band-gap'}

        ret = pd.DataFrame.from_dict(data)
        ret.to_csv(f"{files[property.lower()]}.csv", index=False, sep=';')
        EOF
    references:
      - data/smiles.csv:ref
    variables:
      property: BandGap
      factor: 10

  - name: comp-lambda-max
    command:
      executable: python
      expandArguments: none
      arguments: |
        <<'EOF'
        import pandas as pd
        # VV: This will expand to the path
        path = "data/smiles.csv:ref"
        property = "%(property)s"

        df = pd.read_csv(path, sep=';')

        data = {'input-id':[], property: []}

        for idx, row in df.iterrows():
            data['input-id'].append(row['smiles'])
            data[property].append((idx+1)*%(factor)s)

        files = {'lambdamax': 'lambda-max', 'bandgap': 'band-gap'}

        ret = pd.DataFrame.from_dict(data)
        ret.to_csv(f"{files[property.lower()]}.csv", index=False, sep=';')
        EOF
    references:
      - data/smiles.csv:ref
    variables:
      property: LambdaMax
      factor: 100

  # VV: This component doesn't deal with input systems at all
  - name: hello-world
    command:
      executable:
        echo
      arguments: hello world
"""
    manifest = """
hooks: hooks:copy
"""

    # VV: Some start with S others with A. LambdaMax handles all, band-gap only those that start with A
    smiles = ['S0', 'S1', 'A0', 'A1']
    smiles = "index;smiles\n" + "\n".join((f"{idx};{s}" for (idx, s) in enumerate(smiles)))

    controller = generate_controller_for_flowir(flowir, output_dir, extra_files={
        'hooks/interface.py': hooks,
        'manifest.yaml': manifest,
        'data/smiles.csv': smiles,
    })
    exp = controller.experiment

    dict_interface = exp.configuration.get_interface()

    assert dict_interface['outputFiles'] == []

    interface = experiment.model.hooks.interface.Interface(interface=dict_interface, label="FlowIR.interface")
    input_ids, additional_data = interface.extract_input_ids(exp=exp)

    exp.set_interface_input_ids(input_ids, store_on_disk=True)
    exp.set_interface_additional_input_data(additional_data, store_on_disk=True)
    exp.configuration.store_unreplicated_flowir_to_disk()

    dict_interface = exp.configuration.get_interface()
    assert dict_interface['outputFiles'] == [os.path.join('output', 'input-ids.json')]

    controller.run()

    # VV: Experiment finished, extract measured properties
    dict_interface = exp.configuration.get_interface()
    interface = experiment.model.hooks.interface.Interface(interface=dict_interface, label="FlowIR.interface")
    properties = interface.extract_properties(exp=exp, input_ids=input_ids)
    exp.set_measured_properties(properties, True)

    dict_interface = exp.configuration.get_interface()
    assert dict_interface['outputFiles'] == sorted([
        os.path.join('output', 'input-ids.json'),
        os.path.join('output', 'properties.csv'),
    ])

    properties = exp.get_measured_properties()

    assert properties['input-id'].tolist() == ['S0', 'S1', 'A0', 'A1']
    bandgap: List[float] = properties['bandgap'].tolist()
    assert math.isnan(bandgap[0])
    assert math.isnan(bandgap[1])
    assert bandgap[2:] == [30.0, 40.0]
    assert properties['lambdamax'].tolist() == [100.0, 200.0, 300.0, 400.0]


def test_get_properties_with_variables(output_dir):
    hooks = """
from __future__ import annotations
from typing import List, Union, Dict

import pandas

def get_properties(property_name:str, property_output_file: str, input_id_file: str, variables: Dict[str, str]
    ) -> pandas.DataFrame:
    if property_name not in ["BandGap", "LambdaMax"]:
        raise KeyError(f"unknown property name {property_name}")
    df = pandas.read_csv(property_output_file, sep=';', index_col=False)

    prefix = variables['prefix']
    df[property_name] = df[property_name].apply(lambda x: f"{prefix}-{x}")
    return df
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
        hookGetProperties:
          source:
            keyOutput: BandGap

output:
  BandGap:
    data-in: stage0.comp-band-gap/band-gap.csv:ref

variables:
  default:
    global:
      prefix: hi

components:
  - name: comp-band-gap
    command:
      executable: python
      expandArguments: none
      arguments: |
        <<'EOF' 
        import pandas as pd
        # VV: This will expand to the path
        path = "data/smiles.csv:ref"
        property = "%(property)s"

        df = pd.read_csv(path, sep=';')
        data = {'input-id':[], property: []}

        for idx, row in df.iterrows():
            data['input-id'].append(row['smiles'])
            data[property].append((idx+1)*%(factor)s)

        files = {'lambdamax': 'lambda-max', 'bandgap': 'band-gap'}

        ret = pd.DataFrame.from_dict(data)
        ret.to_csv(f"{files[property.lower()]}.csv", index=False, sep=';')
        EOF
    references:
      - data/smiles.csv:ref
    variables:
      property: BandGap
      factor: 10
"""
    manifest = """
hooks: hooks:copy
"""

    smiles = ['S0', 'S1', 'A0', 'A1']
    smiles = "index;smiles\n" + "\n".join((f"{idx};{s}" for (idx, s) in enumerate(smiles)))

    controller = generate_controller_for_flowir(flowir, output_dir, extra_files={
        'hooks/interface.py': hooks,
        'manifest.yaml': manifest,
        'data/smiles.csv': smiles,
    })
    exp = controller.experiment

    dict_interface = exp.configuration.get_interface()

    assert dict_interface['outputFiles'] == []

    interface = experiment.model.hooks.interface.Interface(interface=dict_interface, label="FlowIR.interface")
    input_ids, additional_data = interface.extract_input_ids(exp=exp)

    exp.set_interface_input_ids(input_ids, store_on_disk=True)
    exp.set_interface_additional_input_data(additional_data, store_on_disk=True)
    exp.configuration.store_unreplicated_flowir_to_disk()

    controller.run()

    # VV: Experiment finished, extract measured properties
    dict_interface = exp.configuration.get_interface()
    interface = experiment.model.hooks.interface.Interface(interface=dict_interface, label="FlowIR.interface")
    properties = interface.extract_properties(exp=exp, input_ids=input_ids)
    exp.set_measured_properties(properties, True)

    properties = exp.get_measured_properties()

    assert properties['input-id'].tolist() == ['S0', 'S1', 'A0', 'A1']
    bandgap: List[str] = properties['bandgap'].tolist()
    assert bandgap == ['hi-10', 'hi-20', 'hi-30', 'hi-40']


def test_merge_dataframes_with_shared_columns(output_dir):
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
    - name: homo
      propertyExtractionMethod:
        csvDataFrame:
          source:
            keyOutput: homo
    - name: lumo
      propertyExtractionMethod:
        csvDataFrame:
          source:
            keyOutput: lumo

output:
  homo:
    data-in: stage0.comp-homo/homo.csv:ref
  lumo:
    data-in: stage0.comp-lumo/lumo.csv:ref
        
components:
  - name: comp-homo
    command:
      executable: python
      expandArguments: none
      arguments: |
        <<'EOF' 
        import pandas as pd
        path = "data/smiles.csv:ref"
        property = "%(property)s"

        df = pd.read_csv(path, sep=';')

        data = {'input-id':[], 'label': [], property: []}

        for idx, row in df.iterrows():
          data['input-id'].append(row['smiles'])
          data['label'].append(idx)
          data[property].append((idx+1)*%(factor)s)

        ret = pd.DataFrame.from_dict(data)
        ret.to_csv(f"{property.lower()}.csv", index=False, sep=';')
        EOF
    references:
      - data/smiles.csv:ref
    variables:
      property: homo
      factor: 5

  - name: comp-lumo
    command:
      executable: python
      expandArguments: none
      arguments: |
        <<'EOF' 
        import pandas as pd
        path = "data/smiles.csv:ref"
        property = "%(property)s"

        df = pd.read_csv(path, sep=';')

        data = {'input-id':[], 'label': [], property: []}

        for idx, row in df.iterrows():
          data['input-id'].append(row['smiles'])
          data['label'].append(idx)
          data[property].append((idx+1)*%(factor)s)

        ret = pd.DataFrame.from_dict(data)
        ret.to_csv(f"{property.lower()}.csv", index=False, sep=';')
        EOF
    references:
      - data/smiles.csv:ref
    variables:
      property: lumo
      factor: 3

  # VV: This component doesn't deal with input systems at all
  - name: hello-world
    command:
      executable:
        echo
      arguments: hello world
"""
    manifest = """
hooks: hooks:copy
"""

    # VV: Some start with S others with A. LambdaMax handles all, band-gap only those that start with A
    smiles = ['S0', 'S1']
    smiles = "index;smiles\n" + "\n".join((f"{idx};{s}" for (idx, s) in enumerate(smiles)))

    controller = generate_controller_for_flowir(flowir, output_dir, extra_files={
        'manifest.yaml': manifest,
        'data/smiles.csv': smiles,
    })
    exp = controller.experiment

    all_comps = [x for x in exp.graph.nodes]

    dict_interface = exp.configuration.get_interface()

    assert dict_interface['outputFiles'] == []

    interface = experiment.model.hooks.interface.Interface(interface=dict_interface, label="FlowIR.interface")
    input_ids, additional_data = interface.extract_input_ids(exp=exp)

    exp.set_interface_input_ids(input_ids, store_on_disk=True)
    exp.set_interface_additional_input_data(additional_data, store_on_disk=True)
    exp.configuration.store_unreplicated_flowir_to_disk()

    dict_interface = exp.configuration.get_interface()
    assert dict_interface['outputFiles'] == [os.path.join('output', 'input-ids.json')]

    controller.run()

    # VV: Experiment finished, extract measured properties
    dict_interface = exp.configuration.get_interface()
    interface = experiment.model.hooks.interface.Interface(interface=dict_interface, label="FlowIR.interface")
    properties = interface.extract_properties(exp=exp, input_ids=input_ids)
    exp.set_measured_properties(properties, True)

    dict_interface = exp.configuration.get_interface()
    assert dict_interface['outputFiles'] == sorted([
        os.path.join('output', 'input-ids.json'),
        os.path.join('output', 'properties.csv'),
    ])

    properties = exp.get_measured_properties()
    assert properties.columns.tolist() == ['input-id', 'label', 'homo', 'lumo']
    assert properties['label'].tolist() == [0, 1]


def test_use_variables_in_inputspec_hooks(output_dir):
    hooks = """
from __future__ import annotations
from typing import List, Union, Dict

import pandas

def get_input_ids(input_id_file: str, variables: Dict[str, str]) -> List[str]:
    df = pandas.read_csv(input_id_file, sep=';', index_col=False)
    return [ f"{variables['prefix']}-{x}" for x in df['smiles'].tolist()]

"""

    flowir = """
variables:
  default:
    global:
      prefix: hi

interface:
  description: Computes the band-gap
  inputSpec:
    hasAdditionalData: false
    namingScheme: SMILES
    inputExtractionMethod:
        hookGetInputIds:
          source:
            path: data/smiles.csv
  propertiesSpec: []

components:
  - name: hello
    command:
      executable: echo
      expandArguments: none
      arguments: hello
"""
    manifest = """
hooks: hooks:copy
"""

    smiles_list = ['S0', 'S1', 'A0', 'A1']
    smiles = "index;smiles\n" + "\n".join((f"{idx};{s}" for (idx, s) in enumerate(smiles_list)))

    exp = experiment_from_flowir(flowir, output_dir, extra_files={
        'hooks/interface.py': hooks,
        'manifest.yaml': manifest,
        'data/smiles.csv': smiles,
    })

    dict_interface = exp.configuration.get_interface()
    interface = experiment.model.hooks.interface.Interface(interface=dict_interface, label="FlowIR.interface")
    input_ids, additional_data = interface.extract_input_ids(exp=exp)

    assert input_ids == [f"hi-{x}" for x in smiles_list]


def test_use_variables_in_inputspec_hooks_with_user_variables(output_dir):
    hooks = """
from __future__ import annotations
from typing import List, Union, Dict

import pandas

def get_input_ids(input_id_file: str, variables: Dict[str, str]) -> List[str]:
    df = pandas.read_csv(input_id_file, sep=';', index_col=False)
    return [ f"{variables['prefix']}-{x}" for x in df['smiles'].tolist()]

"""

    flowir = """
variables:
  default:
    global:
      prefix: hi

interface:
  description: Computes the band-gap
  inputSpec:
    hasAdditionalData: false
    namingScheme: SMILES
    inputExtractionMethod:
        hookGetInputIds:
          source:
            path: data/smiles.csv
  propertiesSpec: []

components:
  - name: hello
    command:
      executable: echo
      expandArguments: none
      arguments: hello
"""
    manifest = """
hooks: hooks:copy
"""
    variables="""
global:
    prefix: user
"""
    smiles_list = ['S0', 'S1', 'A0', 'A1']
    smiles = "index;smiles\n" + "\n".join((f"{idx};{s}" for (idx, s) in enumerate(smiles_list)))

    exp = experiment_from_flowir(flowir, output_dir, extra_files={
        'hooks/interface.py': hooks,
        'manifest.yaml': manifest,
        'data/smiles.csv': smiles,
        '../variables.yaml': variables,
    }, variable_files=[os.path.join(output_dir, 'variables.yaml')])

    exp.log.info(exp.configuration.get_global_variables())
    exp.log.info(exp.configuration.get_user_variables())

    dict_interface = exp.configuration.get_interface()
    interface = experiment.model.hooks.interface.Interface(interface=dict_interface, label="FlowIR.interface")
    input_ids, additional_data = interface.extract_input_ids(exp=exp)

    assert input_ids == [f"user-{x}" for x in smiles_list]


def test_use_inputspec_hook_no_variables(output_dir):
    hooks = """
from __future__ import annotations
from typing import List, Union, Dict

import pandas

def get_input_ids(input_id_file: str) -> List[str]:
    df = pandas.read_csv(input_id_file, sep=';', index_col=False)
    return df['smiles'].tolist()

"""

    flowir = """
variables:
  default:
    global:
      prefix: hi

interface:
  description: Computes the band-gap
  inputSpec:
    hasAdditionalData: false
    namingScheme: SMILES
    inputExtractionMethod:
        hookGetInputIds:
          source:
            path: data/smiles.csv
  propertiesSpec: []

components:
  - name: hello
    command:
      executable: echo
      expandArguments: none
      arguments: hello
"""
    manifest = """
hooks: hooks:copy
"""
    variables="""
global:
    prefix: user
"""

    smiles_list = ['S0', 'S1', 'A0', 'A1']
    smiles = "index;smiles\n" + "\n".join((f"{idx};{s}" for (idx, s) in enumerate(smiles_list)))

    exp = experiment_from_flowir(flowir, output_dir, extra_files={
        'hooks/interface.py': hooks,
        'manifest.yaml': manifest,
        'data/smiles.csv': smiles,
    })

    dict_interface = exp.configuration.get_interface()
    interface = experiment.model.hooks.interface.Interface(interface=dict_interface, label="FlowIR.interface")
    input_ids, additional_data = interface.extract_input_ids(exp=exp)

    assert input_ids == smiles_list


def test_interface_extraction_method_exceptions(output_dir):
    hooks = """
from __future__ import annotations
from typing import List, Union, Dict

import pandas

def get_input_ids(input_id_file: str, variables: Dict[str, str]) -> List[str]:
    # VV: oh no, I'm using the variable "filename" instead of "input_id_file" this will raise an exception
    df = pandas.read_csv(filename, sep=';', index_col=False)
    return df['smiles'].tolist()
"""

    flowir = """
interface:
  description: Computes the band-gap
  inputSpec:
    hasAdditionalData: false
    namingScheme: SMILES
    inputExtractionMethod:
        hookGetInputIds:
          source:
            path: data/smiles.csv
  propertiesSpec:
  - name: hello
    propertyExtractionMethod:
      csvDataFrame:
        source:
          keyOutput: hello
        args:
          column: doesNotExistWhichIsFine

output:
  hello:
    data-in: stage0.hello:output

components:
  - name: hello
    command:
      executable: echo
      expandArguments: none
      arguments: hello
"""
    manifest = """
hooks: hooks:copy
"""

    smiles_list = ['S0', 'S1', 'A0', 'A1']
    smiles = "index;smiles\n" + "\n".join((f"{idx};{s}" for (idx, s) in enumerate(smiles_list)))

    exp = experiment_from_flowir(flowir, output_dir, extra_files={
        'hooks/interface.py': hooks,
        'manifest.yaml': manifest,
        'data/smiles.csv': smiles,
    })

    dict_interface = exp.configuration.get_interface()
    interface = experiment.model.hooks.interface.Interface(interface=dict_interface, label="FlowIR.interface")
    with pytest.raises(experiment.model.errors.InterfaceInputSpecExtractionError) as e:
        _ = interface.extract_input_ids(exp=exp)

    exc: experiment.model.errors.RuntimeHookError = e.value.underlyingError

    assert exc.message == "hookGetInputIds failed with get_input_ids(input_id_file, variables) failed with " \
                          "NameError: name 'filename' is not defined"

    # VV: Now check what happens when we extract properties without having inputIds
    dict_interface = exp.configuration.get_interface()
    interface = experiment.model.hooks.interface.Interface(interface=dict_interface, label="FlowIR.interface")

    with pytest.raises(experiment.model.errors.InterfacePropertySpecExtractionError) as e:
        _ = interface.extract_properties(exp=exp, input_ids=exp.get_input_ids())

    exc: experiment.model.errors.RuntimeHookError = e.value.underlyingError
    assert e.value.propety_name == "all properties"
    assert exc.message == "missing inputIds - check whether FlowIR.interface.inputSpec is problematic"
