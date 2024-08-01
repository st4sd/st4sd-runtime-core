# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis
# Contact: Vassilis.Vassiliadis@ibm.com

from __future__ import annotations

import logging
import math
import os
from typing import List

import pandas

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


def test_dsl2_input_spec_get_input_ids(output_dir):
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

    dsl = """
    entrypoint:
      entry-instance: hello-world
      execute:
      - target: <entry-instance>
        args: {}
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
      - signature:
            name: hello-world
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

    exp = experiment_from_flowir(
        dsl,
        output_dir,
        extra_files={
            'hooks/interface.py': hooks,
            'manifest.yaml': manifest,
            'data/smiles.csv': smiles,
        },
        is_flowir=False
    )

    interface = exp.configuration.get_interface()
    interface = experiment.model.hooks.interface.Interface(interface=interface, label="FlowIR.interface")
    input_ids, additional_data = interface.extract_input_ids(exp=exp)

    assert input_ids == ['S0', 'S1', 'A0', 'A1']


def test_dsl2_hooks_all_ok(output_dir):
    hooks = """
from __future__ import annotations
from typing import List, Union, Dict

import pandas

def get_properties(property_name:str, property_output_file: str, input_id_file: str) -> pandas.DataFrame:
    if property_name not in ["BandGap", "LambdaMax"]:
        raise KeyError(f"unknown property name {property_name}")
    return pandas.read_csv(property_output_file, sep=';', index_col=False)
"""

    dsl = """
entrypoint:
  entry-instance: main
  execute:
  - target: <entry-instance>
    args:
      smiles: data/smiles.csv:ref

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
    - name: BandGap
      data-in: <entry-instance/band-gap>/band-gap.csv:ref
    - name: LambdaMax
      data-in: <entry-instance/lambda-max>/lambda-max.csv:ref

workflows:
- signature:
    name: main
    parameters:
    - name: smiles
  
  steps:
    band-gap: comp-band-gap
    lambda-max: comp-lambda-max
    hello: hello
  
  execute:
  - target: <band-gap>
    args:
      smiles: "%(smiles)s"
  - target: <lambda-max>
    args:
      smiles: "%(smiles)s"
  - target: <hello>

components:
  - signature:
      name: comp-band-gap
      parameters:
      - name: smiles
    command:
      executable: python
      expandArguments: none
      arguments: |
        <<'EOF' 
        import pandas as pd
        # VV: This will expand to the path
        path = "%(smiles)s"
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
    variables:
      property: BandGap
      factor: 10

  - signature:
      name: comp-lambda-max
      parameters:
      - name: smiles

    command:
      executable: python
      expandArguments: none
      arguments: |
        <<'EOF'
        import pandas as pd
        # VV: This will expand to the path
        path = "%(smiles)s"
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

    variables:
      property: LambdaMax
      factor: 100

  # VV: This component doesn't deal with input systems at all
  - signature:
      name: hello
    command:
      executable: echo
      arguments: hello world
"""
    manifest = """
hooks: hooks:copy
"""

    # VV: Some start with S others with A. LambdaMax handles all, band-gap only those that start with A
    smiles = ['S0', 'S1', 'A0', 'A1']
    smiles = "index;smiles\n" + "\n".join((f"{idx};{s}" for (idx, s) in enumerate(smiles)))

    controller = generate_controller_for_flowir(
        dsl,
        output_dir,
        extra_files={
            'hooks/interface.py': hooks,
            'manifest.yaml': manifest,
            'data/smiles.csv': smiles,
        },
        is_flowir=False
    )
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

    properties: pandas.DataFrame = exp.get_measured_properties()
    assert sorted(properties['input-id'].tolist()) == sorted(['S0', 'S1', 'A0', 'A1'])

    rows_no_bandgap = properties.loc[properties['input-id'].isin(['S0', 'S1'])]
    rows_with_bandgap = properties.loc[properties['input-id'].isin(['A0', 'A1'])]

    print(rows_no_bandgap)

    bandgap: List[float] = rows_no_bandgap['bandgap'].tolist()
    assert any(map(math.isnan, bandgap)) is True

    print(rows_with_bandgap)

    bandgap: List[float] = rows_with_bandgap['bandgap'].tolist()
    assert all(map(math.isfinite, bandgap)) is True

    for i, input_id in enumerate(['S0', 'S1', 'A0', 'A1']):
        expected = 100.0 * (i+1)
        row = properties.loc[properties['input-id'] == input_id]
        assert row['lambdamax'].tolist() == [expected]


def test_dsl2_get_properties_with_variables(output_dir):
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

    dsl = """
entrypoint:
  entry-instance: comp-band-gap
  execute:
  - target: <entry-instance>
    args:
      smiles: data/smiles.csv:ref
      prefix: hi
    
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
    - name: BandGap
      data-in: <entry-instance>/band-gap.csv:ref
      

components:
  - signature:
      name: comp-band-gap
      parameters:
      - name: smiles
      - name: prefix
    command:
      executable: python
      expandArguments: none
      arguments: |
        <<'EOF' 
        import pandas as pd
        # VV: This will expand to the path
        path = "%(smiles)s"
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
    
    variables:
      property: BandGap
      factor: 10
"""
    manifest = """
hooks: hooks:copy
"""

    smiles = ['S0', 'S1', 'A0', 'A1']
    smiles = "index;smiles\n" + "\n".join((f"{idx};{s}" for (idx, s) in enumerate(smiles)))

    controller = generate_controller_for_flowir(dsl, output_dir, extra_files={
        'hooks/interface.py': hooks,
        'manifest.yaml': manifest,
        'data/smiles.csv': smiles,
    }, is_flowir=False)
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

    for (input_id, bandgap) in {'S0': 'hi-10', 'S1': 'hi-20', 'A0': 'hi-30', 'A1': 'hi-40'}.items():
        assert  properties.loc[properties['input-id'] == input_id]['bandgap'].tolist() == [bandgap]


def test_dsl2_merge_dataframes_with_shared_columns(output_dir):
    dsl = """
entrypoint:
  entry-instance: main
  execute:
    - target: <entry-instance>
      args:
        smiles: data/smiles.csv:ref
      
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
    - name: homo
      data-in: <entry-instance/homo>/homo.csv:ref
    - name: lumo
      data-in: <entry-instance/lumo>/lumo.csv:ref

workflows:
- signature:
    name: main
    parameters:
    - name: smiles
  
  steps:
    homo: comp-homo
    lumo: comp-lumo
    hello: hello

  execute:
  - target: <homo>
    args:
      smiles: "%(smiles)s"
  - target: <lumo>
    args:
      smiles: "%(smiles)s"
  - target: <hello>

components:
  - signature:
      name: comp-homo
      parameters:
      - name: smiles
    command:
      executable: python
      expandArguments: none
      arguments: |
        <<'EOF' 
        import pandas as pd
        path = "%(smiles)s"
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

    variables:
      property: homo
      factor: 5

  - signature:
      name: comp-lumo
      parameters:
      - name: smiles
    command:
      executable: python
      expandArguments: none
      arguments: |
        <<'EOF' 
        import pandas as pd
        path = "%(smiles)s"
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

    variables:
      property: lumo
      factor: 3

  # VV: This component doesn't deal with input systems at all
  - signature:
      name: hello
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

    controller = generate_controller_for_flowir(
        dsl,
        output_dir,
        extra_files={
            'manifest.yaml': manifest,
            'data/smiles.csv': smiles,
        },
        is_flowir=False
    )
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


def test_dsl2_use_variables_in_inputspec_hooks(output_dir):
    hooks = """
from __future__ import annotations
from typing import List, Union, Dict

import pandas

def get_input_ids(input_id_file: str, variables: Dict[str, str]) -> List[str]:
    df = pandas.read_csv(input_id_file, sep=';', index_col=False)
    return [ f"{variables['prefix']}-{x}" for x in df['smiles'].tolist()]

"""

    dsl = """
entrypoint:
  entry-instance: hello
  execute:
  - target: <entry-instance>
    args:
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
  - signature:
      name: hello
      parameters:
       - name: prefix
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

    exp = experiment_from_flowir(
        dsl,
        output_dir,
        extra_files={
            'hooks/interface.py': hooks,
            'manifest.yaml': manifest,
            'data/smiles.csv': smiles,
        },
        is_flowir=False
    )

    dict_interface = exp.configuration.get_interface()
    interface = experiment.model.hooks.interface.Interface(interface=dict_interface, label="FlowIR.interface")
    input_ids, additional_data = interface.extract_input_ids(exp=exp)

    assert input_ids == [f"hi-{x}" for x in smiles_list]


def test_dsl2_use_variables_in_inputspec_hooks_with_user_variables(output_dir):
    # VV: TODO Not sure about whether this one makes sense with DSL 2.0

    hooks = """
from __future__ import annotations
from typing import List, Union, Dict

import pandas

def get_input_ids(input_id_file: str, variables: Dict[str, str]) -> List[str]:
    df = pandas.read_csv(input_id_file, sep=';', index_col=False)
    return [ f"{variables['prefix']}-{x}" for x in df['smiles'].tolist()]

"""

    dsl = """
entrypoint:
  entry-instance: hello
  execute:
  - target: <entry-instance>
    args:
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
  - signature:
      name: hello
      parameters:
       - name: prefix
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

    exp = experiment_from_flowir(
        dsl,
        output_dir,
        extra_files={
            'hooks/interface.py': hooks,
            'manifest.yaml': manifest,
            'data/smiles.csv': smiles,
            '../variables.yaml': variables,
        },
        variable_files=[os.path.join(output_dir, 'variables.yaml')],
        is_flowir=False
    )

    exp.log.info(exp.configuration.get_global_variables())
    exp.log.info(exp.configuration.get_user_variables())

    dict_interface = exp.configuration.get_interface()
    interface = experiment.model.hooks.interface.Interface(interface=dict_interface, label="FlowIR.interface")
    input_ids, additional_data = interface.extract_input_ids(exp=exp)

    assert input_ids == [f"user-{x}" for x in smiles_list]


def test_dsl2_use_inputspec_hook_no_variables(output_dir):
    hooks = """
from __future__ import annotations
from typing import List, Union, Dict

import pandas

def get_input_ids(input_id_file: str) -> List[str]:
    df = pandas.read_csv(input_id_file, sep=';', index_col=False)
    return df['smiles'].tolist()

"""

    dsl = """
entrypoint:
  entry-instance: hello
  execute:
  - target: <entry-instance>
    args:
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
  - signature:
      name: hello
      parameters:
       - name: prefix
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

    exp = experiment_from_flowir(
        dsl,
        output_dir,
        extra_files={
            'hooks/interface.py': hooks,
            'manifest.yaml': manifest,
            'data/smiles.csv': smiles,
        },
        is_flowir=False
    )

    dict_interface = exp.configuration.get_interface()
    interface = experiment.model.hooks.interface.Interface(interface=dict_interface, label="FlowIR.interface")
    input_ids, additional_data = interface.extract_input_ids(exp=exp)

    assert input_ids == smiles_list


def test_dsl2_interface_extraction_method_exceptions(output_dir):
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
entrypoint:
  entry-instance: hello
  execute:
  - target: <entry-instance>
  
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
    - name: hello
      data-in: <entry-instance>:output

components:
  - signature:
      name: hello
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

    exp = experiment_from_flowir(
        flowir,
        output_dir,
        extra_files={
            'hooks/interface.py': hooks,
            'manifest.yaml': manifest,
            'data/smiles.csv': smiles,
        },
        is_flowir=False
    )

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
