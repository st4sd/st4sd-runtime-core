# Copyright IBM Inc. 2015, 2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis

import typing

import pytest
import yaml

import experiment.model.frontends.flowir
import experiment.model.graph
import experiment.model.frontends.dsl


@pytest.fixture()
def simple_flowir() -> experiment.model.frontends.flowir.DictFlowIR:
    return experiment.model.frontends.flowir.yaml_load("""
            application-dependencies:
              default:
              - dataset
            environments:
              default:
                my-env:
                  FOO: bar

            variables:
              default:
                global:
                  backend: kubernetes

            components:
            - name: hello
              command:
                executable: sh
                arguments: -c "hello %(backend)s; ls -lth dataset:ref; cat input/msg.txt:ref"
                expandArguments: "none"
                environment: my-env
              references:
              - dataset:ref
              - input/msg.txt:ref
              - input/msg.txt:copy
              - data/other.txt:copy
            """)


@pytest.fixture()
def dsl_one_workflow_one_component_one_step_no_datareferences() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load("""
    entrypoint:
      entry-instance: main
      execute:
      - args:
          foo: world
    workflows:
    - signature:
        name: main
        parameters:
        - name: foo
      steps:
        greetings: echo
      execute:
      - target: <greetings>
        args:
          message: "hello"
          other: "%(foo)s"
    components:
    - signature:
        name: echo
        parameters:
        - name: message
        - name: other
          default: a default value
        - name: environment
          default:
            DEFAULTS: PATH:LD_LIBRARY_PATH
            AN_ENV_VAR: ITS_VALUE
      command:
        environment: "%(environment)s"
        executable: echo
        arguments: "%(message)s %(other)s"        
    """)


@pytest.fixture()
def dsl_one_workflow_one_component_two_steps_no_edges() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load("""
    entrypoint:
      entry-instance: main
      execute:
      - args:
          foo: world
    workflows:
    - signature:
        name: main
        parameters:
        - name: foo
        - name: unused
          default: value-unused
      steps:
        one: echo
        two: echo
      execute:
      - target: <one>
        args:
          message: "hello from one"
          other: "%(foo)s"
      - target: <two>
        args:
          message: "hello from two"
          other: "%(foo)s"
    components:
    - signature:
        name: echo
        parameters:
        - name: message
        - name: other
          default: a default value
        - name: environment
          default:
            DEFAULTS: PATH:LD_LIBRARY_PATH
            AN_ENV_VAR: ITS_VALUE
      command:
        environment: "%(environment)s"
        executable: echo
        arguments: "%(message)s %(other)s"        
    """)


@pytest.fixture()
def dsl_one_workflow_one_component_two_steps_with_edges() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load("""
    entrypoint:
      entry-instance: main
      execute:
      - args:
          foo: world
    workflows:
    - signature:
        name: main
        parameters:
        - name: foo
        - name: unused
          default: value-unused
      steps:
        one: echo
        two: echo
      execute:
      - target: <one>
        args:
          message: "hello from one"
          other: "%(foo)s"
      - target: <two>
        args:
          message: "hello from two"
          other: "<one>:output"
    components:
    - signature:
        name: echo
        parameters:
        - name: message
        - name: other
          default: a default value
        - name: environment
          default:
            DEFAULTS: PATH:LD_LIBRARY_PATH
            AN_ENV_VAR: ITS_VALUE
      command:
        environment: "%(environment)s"
        executable: echo
        arguments: "%(message)s %(other)s"        
    """)

@pytest.fixture()
def dsl_two_workflows_one_component_one_step() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load("""
    entrypoint:
      entry-instance: main
      execute:
      - args:
          foo: world
    workflows:
    - signature:
        name: main
        parameters:
        - name: foo
      steps:
        wrapper: actual
      execute:
      - target: <wrapper>
        args:
          foo: "%(foo)s"
    - signature:
        name: actual
        parameters:
        - name: foo
      steps:
        greetings: echo
      execute:
      - target: <greetings>
        args:
          message: "hello"
          other: "%(foo)s"
    components:
    - signature:
        name: echo
        parameters:
        - name: message
        - name: other
          default: a default value
        - name: environment
          default:
            DEFAULTS: PATH:LD_LIBRARY_PATH
            AN_ENV_VAR: ITS_VALUE
      command:
        environment: "%(environment)s"
        executable: echo
        arguments: "%(message)s %(other)s"
    """)



@pytest.fixture()
def dsl_band_gap_pm3_gamess_us() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load("""
    entrypoint:
      entry-instance: main
      execute:
      - args:
          backend: kubernetes
          basis: GBASIS=PM3
          collabel: label
          gamess-command: bin/run-gamess.sh
          gamess-gpus: '0'
          gamess-grace-period-seconds: '1800'
          gamess-image: nvcr.io/hpc/gamess:17.09-r2-libcchem
          gamess-restart-hook-file: semi_empirical_restart.py
          gamess-version: '00'
          gamess-walltime-minutes: '700'
          mem: '4295000000'
          number-processors: '8'
          numberMolecules: '1'
          startIndex: '0'
          input.input_smiles.csv: input/input_smiles.csv
          data.input_molecule.txt: data/input_molecule.txt
    workflows:
    - signature:
        name: main
        parameters:
        - name: input.input_smiles.csv
        - name: data.input_molecule.txt
          default: data/input_molecule.txt
        - name: backend
          default: kubernetes
        - name: basis
          default: GBASIS=PM3
        - name: collabel
          default: label
        - name: gamess-command
          default: bin/run-gamess.sh
        - name: gamess-gpus
          default: '0'
        - name: gamess-grace-period-seconds
          default: '1800'
        - name: gamess-image
          default: nvcr.io/hpc/gamess:17.09-r2-libcchem
        - name: gamess-restart-hook-file
          default: semi_empirical_restart.py
        - name: gamess-version
          default: '00'
        - name: gamess-walltime-minutes
          default: '700'
        - name: mem
          default: '4295000000'
        - name: number-processors
          default: '8'
        - name: numberMolecules
          default: '1'
        - name: startIndex
          default: '0'
      steps:
        stage0.GetMoleculeIndex: stage0.GetMoleculeIndex
        stage0.SetBasis: stage0.SetBasis
        stage1.CreateLabels: stage1.CreateLabels
        stage0.SMILESToXYZ: stage0.SMILESToXYZ
        stage0.XYZToGAMESS: stage0.XYZToGAMESS
        stage1.GeometryOptimisation: stage1.GeometryOptimisation
        stage1.ExtractEnergies: stage1.ExtractEnergies
      execute:
      - target: <stage0.GetMoleculeIndex>
        args:
          numberMolecules: '%(numberMolecules)s'
          replica: '%(replica)s'
          startIndex: '%(startIndex)s'
      - target: <stage0.SetBasis>
        args:
          basis: '%(basis)s'
          param0: '"%(data.input_molecule.txt)s":copy'
      - target: <stage1.CreateLabels>
        args:
          collabel: '%(collabel)s'
          param0: <stage0.GetMoleculeIndex>:output
          param1: '"%(input.input_smiles.csv)s":ref'
      - target: <stage0.SMILESToXYZ>
        args:
          backend: '%(backend)s'
          param0: '"%(input.input_smiles.csv)s":copy'
          param1: <stage0.GetMoleculeIndex>:output
      - target: <stage0.XYZToGAMESS>
        args:
          backend: '%(backend)s'
          param0: <stage0.SMILESToXYZ>:ref
          param1: <stage0.SetBasis>/input_molecule.txt:ref
          param2: <stage0.GetMoleculeIndex>:output
      - target: <stage1.GeometryOptimisation>
        args:
          backend: '%(backend)s'
          gamess-command: '%(gamess-command)s'
          gamess-gpus: '%(gamess-gpus)s'
          gamess-grace-period-seconds: '%(gamess-grace-period-seconds)s'
          gamess-image: '%(gamess-image)s'
          gamess-restart-hook-file: '%(gamess-restart-hook-file)s'
          gamess-version: '%(gamess-version)s'
          gamess-walltime-minutes: '%(gamess-walltime-minutes)s'
          mem: '%(mem)s'
          number-processors: '%(number-processors)s'
          param0: <stage0.XYZToGAMESS>/molecule.inp:copy
      - target: <stage1.ExtractEnergies>
        args:
          backend: '%(backend)s'
          param0: <stage1.GeometryOptimisation>:ref
          param1: <stage1.CreateLabels>:output
    components:
    - signature:
        name: stage0.GetMoleculeIndex
        parameters:
        - name: numberMolecules
        - name: replica
        - name: startIndex
      command:
        executable: python
        arguments: -c "print(%(startIndex)s + %(replica)s),"
      workflowAttributes:
        restartHookOn:
        - ResourceExhausted
        replicate: '%(numberMolecules)s'
        repeatRetries: 3
        memoization:
          disable: {}
      resourceRequest: {}
      resourceManager:
        config: {}
        lsf: {}
        kubernetes: {}
        docker:
          imagePullPolicy: Always
    - signature:
        name: stage0.SetBasis
        parameters:
        - name: param0
        - name: basis
      command:
        executable: sed
        interpreter: bash
        arguments: -i'.bak' -e 's/#BASIS#/%(basis)s/g' input_molecule.txt
        expandArguments: none
      workflowAttributes:
        restartHookOn:
        - ResourceExhausted
        repeatRetries: 3
        memoization:
          disable: {}
      resourceRequest: {}
      resourceManager:
        config: {}
        lsf: {}
        kubernetes: {}
        docker:
          imagePullPolicy: Always
    - signature:
        name: stage1.CreateLabels
        parameters:
        - name: param0
        - name: param1
        - name: collabel
      command:
        executable: python
        arguments: -c "import pandas;  input_file='%(param1)s';  row_indices='%(param0)s';  m=pandas.read_csv(input_file,
          engine='python', sep=None);  print(','.join([str(m.iloc[int(index)]['%(collabel)s'])
          for index in row_indices.split()]))"
        expandArguments: none
      workflowAttributes:
        restartHookOn:
        - ResourceExhausted
        aggregate: true
        repeatRetries: 3
        memoization:
          disable: {}
      resourceRequest: {}
      resourceManager:
        config: {}
        lsf: {}
        kubernetes: {}
        docker:
          imagePullPolicy: Always
    - signature:
        name: stage0.SMILESToXYZ
        parameters:
        - name: param0
        - name: param1
        - name: backend
        - name: env-vars
          default: {}
      command:
        executable: bin/rdkit_smiles2coordinates.py
        arguments: --input input_smiles.csv --row %(param1)s
        environment: '%(env-vars)s'
      workflowAttributes:
        restartHookOn:
        - ResourceExhausted
        repeatRetries: 3
        memoization:
          disable: {}
      resourceRequest: {}
      resourceManager:
        config:
          backend: '%(backend)s'
        lsf: {}
        kubernetes:
          image: quay.io/st4sd/community-applications/rdkit-st4sd:2019.09.1
        docker:
          imagePullPolicy: Always
    - signature:
        name: stage0.XYZToGAMESS
        parameters:
        - name: param0
        - name: param1
        - name: param2
        - name: backend
        - name: env-vars
          default: {}
      command:
        executable: bin/make_gamess_input_from_template_and_xyz.py
        arguments: -xp %(param0)s -xf %(param2)s -g %(param1)s -sp %(param0)s -sf %(param2)s
        environment: '%(env-vars)s'
      workflowAttributes:
        restartHookOn:
        - ResourceExhausted
        repeatRetries: 3
        memoization:
          disable: {}
      resourceRequest: {}
      resourceManager:
        config:
          backend: '%(backend)s'
        lsf: {}
        kubernetes:
          image: quay.io/st4sd/community-applications/rdkit-st4sd:2019.09.1
        docker:
          imagePullPolicy: Always
    - signature:
        name: stage1.GeometryOptimisation
        parameters:
        - name: param0
        - name: backend
        - name: gamess-command
        - name: gamess-gpus
        - name: gamess-grace-period-seconds
        - name: gamess-image
        - name: gamess-restart-hook-file
        - name: gamess-version
        - name: gamess-walltime-minutes
        - name: mem
        - name: number-processors
        - name: env-vars
          default:
            PATH: /usr/local/bin/:/usr/local/bin/:/venvs/st4sd-runtime-core/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
            VERNO: '00'
      command:
        executable: '%(gamess-command)s'
        arguments: molecule.inp %(gamess-version)s %(number-processors)s
        environment: '%(env-vars)s'
      workflowAttributes:
        restartHookFile: '%(gamess-restart-hook-file)s'
        restartHookOn:
        - KnownIssue
        - Success
        - ResourceExhausted
        shutdownOn:
        - KnownIssue
        repeatRetries: 3
        memoization:
          disable: {}
      resourceRequest:
        numberThreads: '%(number-processors)s'
        memory: '%(mem)s'
        gpus: '%(gamess-gpus)s'
      resourceManager:
        config:
          backend: '%(backend)s'
          walltime: '%(gamess-walltime-minutes)s'
        lsf: {}
        kubernetes:
          image: '%(gamess-image)s'
          gracePeriod: '%(gamess-grace-period-seconds)s'
        docker:
          imagePullPolicy: Always
    - signature:
        name: stage1.ExtractEnergies
        parameters:
        - name: param0
        - name: param1
        - name: backend
        - name: env-vars
          default: {}
      command:
        executable: bin/extract_gmsout.py
        arguments: -l %(param1)s %(param0)s
        environment: '%(env-vars)s'
      workflowAttributes:
        restartHookOn:
        - ResourceExhausted
        aggregate: true
        repeatRetries: 3
        memoization:
          disable: {}
      resourceRequest: {}
      resourceManager:
        config:
          backend: '%(backend)s'
        lsf: {}
        kubernetes:
          image: quay.io/st4sd/community-applications/rdkit-st4sd:2019.09.1
        docker:
          imagePullPolicy: Always
    """)


def test_parse_hallucinated_simple_dsl2(simple_flowir: experiment.model.frontends.flowir.DictFlowIR):
    graph = experiment.model.graph.WorkflowGraph.graphFromFlowIR(simple_flowir, {})

    dsl = graph.to_dsl()

    namespace = experiment.model.frontends.dsl.Namespace(**dsl)

    print(yaml.safe_dump(namespace.dict(
        by_alias=True, exclude_unset=True, exclude_defaults=True, exclude_none=True), indent=2))

    print("-----------")
    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)
    print(yaml.safe_dump(flowir.raw(), indent=2))


def test_parse_band_gap_pm3_gamess_us(dsl_band_gap_pm3_gamess_us: typing.Dict[str, typing.Any]):
    namespace = experiment.model.frontends.dsl.Namespace(**dsl_band_gap_pm3_gamess_us)

    print(yaml.safe_dump(namespace.dict(
        by_alias=True, exclude_unset=True, exclude_defaults=True, exclude_none=True), indent=2))
    print("-----------")
    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)
    print(yaml.safe_dump(flowir.raw(), indent=2))

    errors = flowir.validate()

    assert len(errors) == 0

    components = sorted([
        f"stage{comp['stage']}.{comp['name']}" for comp in flowir.get_components()
    ])

    assert components == sorted([
        'stage0.GetMoleculeIndex',
        'stage0.SMILESToXYZ',
        'stage0.SetBasis',
        'stage0.XYZToGAMESS',
        'stage1.CreateLabels',
        'stage1.ExtractEnergies',
        'stage1.GeometryOptimisation'
    ])


@pytest.mark.parametrize("input_outputs", [
    ("<workflow/component/file>:ref", ["workflow", "component", "file"], "ref", "<workflow/component/file>:ref"),
    ('"<workflow/component>"/file:ref', ["workflow", "component", "file"], "ref", "<workflow/component/file>:ref"),
    ('"<workflow>"/component/file:ref', ["workflow", "component", "file"], "ref", "<workflow/component/file>:ref"),
    ('"<workflow>":ref', ["workflow"], "ref", "<workflow>:ref"),
    ('<workflow>:ref', ["workflow"], "ref", "<workflow>:ref"),
    ('"<workflow>"', ["workflow"], None, "<workflow>"),
    ('<workflow>', ["workflow"], None, "<workflow>"),
    ("<workflow/component/file>", ["workflow", "component", "file"], None, "<workflow/component/file>"),
    ('"<workflow/component>"/file', ["workflow", "component", "file"], None, "<workflow/component/file>"),
    ('<stage0.XYZToGAMESS>/molecule.inp:copy', ["stage0.XYZToGAMESS", "molecule.inp"], "copy",
        "<stage0.XYZToGAMESS/molecule.inp>:copy")
])
def test_parse_step_references(input_outputs: typing.Tuple[
    str, typing.List[str], typing.Optional[str], str,
]):
    ref_str, expected_location, expected_method, expected_rewrite = input_outputs

    ref = experiment.model.frontends.dsl.OutputReference.from_str(ref_str)

    assert ref.location == expected_location
    assert ref.method == expected_method
    assert ref.to_str() == expected_rewrite


def test_replace_many_parameter_references():
    x = experiment.model.frontends.dsl._replace_many_parameter_references(
        what="%(message)s %(other)s",
        loc=[],
        parameters= {
            "message": "hello",
            "other": "world",
            "environment": {"hi": "there"}
        },
        field=[],
    )

    assert x == "hello world"


@pytest.mark.parametrize("fixture_name", [
    "dsl_one_workflow_one_component_one_step_no_datareferences",
    "dsl_two_workflows_one_component_one_step",
])
def test_dsl2_single_workflow_single_component_single_step_no_datareferences(fixture_name: str, request):
    dsl = request.getfixturevalue(argname=fixture_name)
    namespace = experiment.model.frontends.dsl.Namespace(**dsl)

    print(
        yaml.safe_dump(
            namespace.dict(
                exclude_unset=True,
                exclude_none=True,
                exclude_defaults=True,
                by_alias=True
            ),
            sort_keys=False
        )
    )

    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)

    print(yaml.safe_dump(flowir.raw(), sort_keys=False))

    raw_flowir = flowir.raw()
    assert len(raw_flowir["components"]) == 1
    assert raw_flowir["components"][0] == {
        "stage": 0,
        "name": "greetings",
        "references": [],
        "command": {
            "executable": "echo",
            "arguments": "hello world",
            "environment": "env0"
        }
    }

    assert len(raw_flowir["environments"]["default"]) == 1
    assert raw_flowir["environments"]["default"]["env0"] == {
        "DEFAULTS": "PATH:LD_LIBRARY_PATH",
        "AN_ENV_VAR": "ITS_VALUE"
    }

    errors = flowir.validate()

    assert len(errors) == 0


def test_dsl2_single_workflow_one_component_two_steps_no_edges(
    dsl_one_workflow_one_component_two_steps_no_edges: typing.Dict[str, typing.Any]
):
    namespace = experiment.model.frontends.dsl.Namespace(**dsl_one_workflow_one_component_two_steps_no_edges)

    print(
        yaml.safe_dump(
            namespace.dict(
                exclude_unset=True,
                exclude_none=True,
                exclude_defaults=True,
                by_alias=True
            ),
            sort_keys=False
        )
    )

    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)

    print(yaml.safe_dump(flowir.raw(), sort_keys=False))

    raw_flowir = flowir.raw()
    assert len(raw_flowir["components"]) == 2
    assert [x for x in raw_flowir["components"] if x['name'] == 'one'][0] == {
        "stage": 0,
        "name": "one",
        "references": [],
        "command": {
            "executable": "echo",
            "arguments": "hello from one world",
            "environment": "env0"
        }
    }

    assert [x for x in raw_flowir["components"] if x['name'] == 'two'][0] == {
        "stage": 0,
        "name": "two",
        "references": [],
        "command": {
            "executable": "echo",
            "arguments": "hello from two world",
            "environment": "env0"
        }
    }

    assert len(raw_flowir["environments"]["default"]) == 1
    assert raw_flowir["environments"]["default"]["env0"] == {
        "DEFAULTS": "PATH:LD_LIBRARY_PATH",
        "AN_ENV_VAR": "ITS_VALUE"
    }


    assert raw_flowir["variables"]["default"]["global"] == {
        "foo": "world",
        "unused": "value-unused"
    }

    errors = flowir.validate()

    assert len(errors) == 0



def test_dsl2_single_workflow_one_component_two_steps_with_edges(
    dsl_one_workflow_one_component_two_steps_with_edges: typing.Dict[str, typing.Any]
):
    namespace = experiment.model.frontends.dsl.Namespace(**dsl_one_workflow_one_component_two_steps_with_edges)

    print(
        yaml.safe_dump(
            namespace.dict(
                exclude_unset=True,
                exclude_none=True,
                exclude_defaults=True,
                by_alias=True
            ),
            sort_keys=False
        )
    )

    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)

    print(yaml.safe_dump(flowir.raw(), sort_keys=False))

    raw_flowir = flowir.raw()
    assert len(raw_flowir["components"]) == 2
    assert [x for x in raw_flowir["components"] if x['name'] == 'one'][0] == {
        "stage": 0,
        "name": "one",
        "references": [],
        "command": {
            "executable": "echo",
            "arguments": "hello from one world",
            "environment": "env0"
        }
    }

    assert [x for x in raw_flowir["components"] if x['name'] == 'two'][0] == {
        "stage": 0,
        "name": "two",
        "references": [
            "stage0.one:output"
        ],
        "command": {
            "executable": "echo",
            "arguments": "hello from two stage0.one:output",
            "environment": "env0"
        }
    }

    assert len(raw_flowir["environments"]["default"]) == 1
    assert raw_flowir["environments"]["default"]["env0"] == {
        "DEFAULTS": "PATH:LD_LIBRARY_PATH",
        "AN_ENV_VAR": "ITS_VALUE"
    }

    assert raw_flowir["variables"]["default"]["global"] == {
        "foo": "world",
        "unused": "value-unused"
    }

    errors = flowir.validate()
    assert len(errors) == 0
