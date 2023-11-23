# Copyright IBM Inc. 2015, 2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis

import typing

import pytest
import yaml

import experiment.model.frontends.dsl
import experiment.model.frontends.flowir
import experiment.model.graph

import experiment.model.errors

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


@pytest.fixture()
def dsl_no_workflow() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load("""
    entrypoint:
      entry-instance: print
      execute:
      - target: "<entry-instance>"
        args:
          message: Hello world
    components:
    - signature:
        name: print
        parameters:
          - name: message
      command:
        executable: echo
        arguments: "%(message)s"
    """)


@pytest.fixture()
def dsl_step_via_param_more_complex() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load("""
    entrypoint:
        entry-instance: main
        execute:
          - target: "<entry-instance>"
    workflows:
    - signature:
        name: main
        parameters: []
      steps:
        producer: inner-produce
        consumer: inner-consume
      execute:
        - target: "<producer>"
        - target: "<consumer>"
          args:
            producer: <producer/producer>
    - signature:
        name: inner-produce
        parameters: []
      steps:
        producer: generate
      execute:
      - target: <producer>
        args: {}
    - signature:
        name: inner-consume
        parameters: 
        - name: producer
      steps:
        consumer: echo
      execute:
      - target: <consumer>
        args:
          message: "%(producer)s/outputs/msg.txt:output"
    components:
    - signature:
        name: generate
        parameters: []
      command:
        executable: sh
        arguments: -c "mkdir outputs; echo hi >outputs/msg.txt"
        expandArguments: none
    - signature:
        name: echo
        parameters:
        - name: message
      command:
        executable: echo
        arguments: "%(message)s"
    """)


@pytest.fixture()
def dsl_step_via_param() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load("""
entrypoint:
    entry-instance: main
    execute:
      - target: "<entry-instance>"
workflows:
- signature:
    name: main
    parameters: []
  steps:
    producer: generate
    consumer-wf: inner-consume
  execute:
    - target: <consumer-wf>
      args:
        producer: <producer>
    - target: <producer>

- signature:
    name: inner-consume
    parameters: 
    - name: producer
  steps:
    consumer: echo
  execute:
  - target: <consumer>
    args:
      message: "%(producer)s/outputs/msg.txt:output"
components:
- signature:
    name: generate
    parameters: []
  command:
    executable: sh
    arguments: -c "mkdir outputs; echo hi >outputs/msg.txt"
    expandArguments: none
- signature:
    name: echo
    parameters:
    - name: message
  command:
    executable: echo
    arguments: "%(message)s"
    """)


@pytest.fixture()
def dsl_with_cycle() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load("""
    entrypoint:
      entry-instance: main
      execute:
      - target: <entry-instance>
    
    workflows:
    - signature:
        name: main
    
      steps:
        cycle: main
        message: echo
      execute:
      - target: <message>
      - target: <cycle>
    
    components:
    - signature:
        name: echo
      command:
        executable: echo
        arguments: hello world
    """)


@pytest.fixture()
def dsl_nested_workflows() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load("""
        entrypoint:
          entry-instance: test
          execute:
          - target: "<entry-instance>"
            args: {}
        workflows:
        - signature:
            name: test
            parameters:
            - name: input.a
              default: ''
            - name: b
              default: '20'
          steps:
            generator: generator
            consumer: consumer
          execute:
          - target: "<generator>"
            args:
              a: "%(input.a)s:output"
              b: "%(b)s"
          - target: "<consumer>"
            args:
              one: "<generator>"
        components:
        - signature:
            name: generator
            parameters:
            - name: a
              default: ''
            - name: b
              default: ''
          command:
            executable: a
            arguments: "%(a)s %(b)s"
        - signature:
            name: consumer
            parameters:
            - name: one
              default: ''
          command:
            executable: a
            arguments: "%(one)s:ref"
        """)


@pytest.fixture()
def dsl_conflicting_templates() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load("""
    entrypoint:
      entry-instance: main
      execute:
      - target: <entry-instance>
        args: {}
    
    workflows:
    - signature:
        name: main
      execute: []
      steps: {}
    - signature:
        name: main
      execute: []
      steps: {}
    components:
    - signature:
        name: main
      command:
        executable: echo
        arguments: hello
    - signature:
        name: comp
      command:
        executable: echo
        arguments: hello
    - signature:
        name: comp
      command:
        executable: echo
        arguments: hello
    """)

def test_dsl_conflicting_templates(dsl_conflicting_templates: typing.Dict[str, typing.Any]):
    namespace = experiment.model.frontends.dsl.Namespace(**dsl_conflicting_templates)
    scopes = experiment.model.frontends.dsl.ScopeStack()
    with pytest.raises(experiment.model.errors.DSLInvalidError) as e:
        scopes.discover_all_instances_of_templates(namespace)

    assert e.value.errors() == [
        {'loc': ['workflows', 1], 'msg': 'There already is a Workflow template called main'},
        {'loc': ['components', 0], 'msg': 'There already is a Workflow template called main'},
        {'loc': ['components', 2], 'msg': 'There already is a Component template called comp'}
    ]


def test_dsl_nested_workflows(dsl_nested_workflows: typing.Dict[str, typing.Any]):
    namespace = experiment.model.frontends.dsl.Namespace(**dsl_nested_workflows)
    experiment.model.frontends.dsl.namespace_to_flowir(namespace)


def test_detect_cycle(dsl_with_cycle: typing.Dict[str, typing.Any]):
    namespace = experiment.model.frontends.dsl.Namespace(**dsl_with_cycle)
    scopes = experiment.model.frontends.dsl.ScopeStack()


    with pytest.raises(experiment.model.errors.DSLInvalidError) as e:
        scopes.discover_all_instances_of_templates(namespace)

    exc = e.value

    assert len(exc.underlying_errors) == 1
    assert str(exc.underlying_errors[0].underlying_error) == "The computational graph contains a cycle"
    assert exc.underlying_errors[0].location == ["workflows", 0, "execute", 1]


def test_output_reference_split():
    ref = experiment.model.frontends.dsl.OutputReference.from_str("<entry-instance/producer/outputs/msg.txt>:output")
    scopes = [("entry-instance",), ("entry-instance", "producer"), ("entry-instance", "consumer-wf"),
        ("entry-instance", "consumer-wf", "consumer"), ]

    producer, fileref = ref.split(scopes)

    assert producer == ("entry-instance", "producer")
    assert fileref == "outputs/msg.txt"


def test_parse_dsl_step_via_param(dsl_step_via_param: typing.Dict[str, typing.Any]):
    namespace = experiment.model.frontends.dsl.Namespace(**dsl_step_via_param)

    print(yaml.safe_dump(namespace.dict(by_alias=True, exclude_none=True), indent=2, sort_keys=False))

    print("-----------")
    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)
    print(yaml.safe_dump(flowir.raw(), indent=2, sort_keys=False))

    assert flowir.validate() == []

    consumer = flowir.get_component((0, "consumer"))
    producer = flowir.get_component((0, "producer"))

    assert producer['references'] == []
    assert consumer['references'] == ["stage0.producer/outputs/msg.txt:output"]


def test_parse_dsl_step_via_param_more_complex(dsl_step_via_param_more_complex: typing.Dict[str, typing.Any]):
    namespace = experiment.model.frontends.dsl.Namespace(**dsl_step_via_param_more_complex)

    print(yaml.safe_dump(namespace.dict(by_alias=True, exclude_none=True), indent=2, sort_keys=False))

    print("-----------")
    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)
    print(yaml.safe_dump(flowir.raw(), indent=2, sort_keys=False))

    assert flowir.validate() == []

    consumer = flowir.get_component((0, "consumer"))
    producer = flowir.get_component((0, "producer"))

    assert producer['references'] == []
    assert consumer['references'] == ["stage0.producer/outputs/msg.txt:output"]


def test_parse_dsl_no_workflow(dsl_no_workflow: typing.Dict[str, typing.Any]):
    namespace = experiment.model.frontends.dsl.Namespace(**dsl_no_workflow)

    print(yaml.safe_dump(namespace.dict(by_alias=True, exclude_unset=True, exclude_defaults=True, exclude_none=True),
        indent=2))

    print("-----------")
    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)
    print(yaml.safe_dump(flowir.raw(), indent=2))

    assert flowir.validate() == []


def test_parse_hallucinated_simple_dsl2(simple_flowir: experiment.model.frontends.flowir.DictFlowIR):
    import experiment.model.errors
    graph = experiment.model.graph.WorkflowGraph.graphFromFlowIR(simple_flowir, {})

    dsl = graph.to_dsl()

    namespace = experiment.model.frontends.dsl.Namespace(**dsl)

    print(yaml.safe_dump(namespace.dict(by_alias=True, exclude_unset=True, exclude_defaults=True, exclude_none=True),
        indent=2))

    print("-----------")
    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)
    print(yaml.safe_dump(flowir.raw(), indent=2))

    # VV: We don't support manifests yet
    errors = flowir.validate()
    assert len(errors) == 2

    err_unknown_reference = \
    [x for x in errors if isinstance(x, experiment.model.errors.FlowIRUnknownReferenceInArguments)][0]
    err_unknown_component = \
    [x for x in errors if isinstance(x, experiment.model.errors.FlowIRReferenceToUnknownComponent)][0]

    assert err_unknown_reference.ref_unknown == "dataset:ref"
    assert err_unknown_reference.component == "hello"
    assert err_unknown_reference.stage == 0

    # VV: flowir also tries to see if there's a `dataset` component the developer is trying to reference
    assert err_unknown_component.references == ["stage0.dataset"]
    assert err_unknown_component.component == "hello"
    assert err_unknown_component.stage == 0


def test_parse_band_gap_pm3_gamess_us(dsl_band_gap_pm3_gamess_us: typing.Dict[str, typing.Any]):
    namespace = experiment.model.frontends.dsl.Namespace(**dsl_band_gap_pm3_gamess_us)

    print(yaml.safe_dump(namespace.dict(by_alias=True, exclude_unset=True, exclude_defaults=True, exclude_none=True),
        indent=2))
    print("-----------")
    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)
    print(yaml.safe_dump(flowir.raw(), indent=2))

    errors = flowir.validate()

    assert len(errors) == 0

    components = sorted([f"stage{comp['stage']}.{comp['name']}" for comp in flowir.get_components()])

    assert components == sorted(
        ['stage0.GetMoleculeIndex', 'stage0.SMILESToXYZ', 'stage0.SetBasis', 'stage0.XYZToGAMESS',
            'stage1.CreateLabels', 'stage1.ExtractEnergies', 'stage1.GeometryOptimisation'])


@pytest.mark.parametrize("input_outputs", [
    ("<workflow/component/file>:ref", ["workflow", "component", "file"], "ref", "<workflow/component/file>:ref"),
    ('"<workflow/component>"/file:ref', ["workflow", "component", "file"], "ref", "<workflow/component/file>:ref"),
    ('"<workflow>"/component/file:ref', ["workflow", "component", "file"], "ref", "<workflow/component/file>:ref"),
    ('"<workflow>":ref', ["workflow"], "ref", "<workflow>:ref"),
    ('<workflow>:ref', ["workflow"], "ref", "<workflow>:ref"), ('"<workflow>"', ["workflow"], None, "<workflow>"),
    ('<workflow>', ["workflow"], None, "<workflow>"),
    ("<workflow/component/file>", ["workflow", "component", "file"], None, "<workflow/component/file>"),
    ('"<workflow/component>"/file', ["workflow", "component", "file"], None, "<workflow/component/file>"), (
    '<stage0.XYZToGAMESS>/molecule.inp:copy', ["stage0.XYZToGAMESS", "molecule.inp"], "copy",
    "<stage0.XYZToGAMESS/molecule.inp>:copy")])
def test_parse_step_references(input_outputs: typing.Tuple[str, typing.List[str], typing.Optional[str], str,]):
    ref_str, expected_location, expected_method, expected_rewrite = input_outputs

    ref = experiment.model.frontends.dsl.OutputReference.from_str(ref_str)

    assert ref.location == expected_location
    assert ref.method == expected_method
    assert ref.to_str() == expected_rewrite


def test_replace_many_parameter_references():
    x = experiment.model.frontends.dsl._replace_many_parameter_references(
        what="%(message)s %(other)s",
        parameters={"message": "hello", "other": "world", "environment": {"hi": "there"}},
    )

    assert x == "hello world"


@pytest.mark.parametrize("fixture_name", ["dsl_one_workflow_one_component_one_step_no_datareferences",
    "dsl_two_workflows_one_component_one_step", ])
def test_dsl2_single_workflow_single_component_single_step_no_datareferences(fixture_name: str, request):
    dsl = request.getfixturevalue(argname=fixture_name)
    namespace = experiment.model.frontends.dsl.Namespace(**dsl)

    print(yaml.safe_dump(namespace.dict(exclude_unset=True, exclude_none=True, exclude_defaults=True, by_alias=True),
        sort_keys=False))

    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)

    print(yaml.safe_dump(flowir.raw(), sort_keys=False))

    raw_flowir = flowir.raw()
    assert len(raw_flowir["components"]) == 1
    assert raw_flowir["components"][0] == {"stage": 0, "name": "greetings", "references": [],
        "command": {"executable": "echo", "arguments": "hello world", "environment": "env0"}}

    assert len(raw_flowir["environments"]["default"]) == 1
    assert raw_flowir["environments"]["default"]["env0"] == {"DEFAULTS": "PATH:LD_LIBRARY_PATH",
        "AN_ENV_VAR": "ITS_VALUE"}

    errors = flowir.validate()
    assert len(errors) == 0


def test_dsl2_single_workflow_one_component_two_steps_no_edges(
        dsl_one_workflow_one_component_two_steps_no_edges: typing.Dict[str, typing.Any]):
    namespace = experiment.model.frontends.dsl.Namespace(**dsl_one_workflow_one_component_two_steps_no_edges)

    print(yaml.safe_dump(namespace.dict(exclude_unset=True, exclude_none=True, exclude_defaults=True, by_alias=True),
        sort_keys=False))

    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)

    print(yaml.safe_dump(flowir.raw(), sort_keys=False))

    raw_flowir = flowir.raw()
    assert len(raw_flowir["components"]) == 2
    assert [x for x in raw_flowir["components"] if x['name'] == 'one'][0] == {"stage": 0, "name": "one",
        "references": [], "command": {"executable": "echo", "arguments": "hello from one world", "environment": "env0"}}

    assert [x for x in raw_flowir["components"] if x['name'] == 'two'][0] == {"stage": 0, "name": "two",
        "references": [], "command": {"executable": "echo", "arguments": "hello from two world", "environment": "env0"}}

    assert len(raw_flowir["environments"]["default"]) == 1
    assert raw_flowir["environments"]["default"]["env0"] == {"DEFAULTS": "PATH:LD_LIBRARY_PATH",
        "AN_ENV_VAR": "ITS_VALUE"}

    assert raw_flowir["variables"]["default"]["global"] == {"foo": "world", "unused": "value-unused"}

    errors = flowir.validate()
    assert len(errors) == 0


def test_dsl2_single_workflow_one_component_two_steps_with_edges(
        dsl_one_workflow_one_component_two_steps_with_edges: typing.Dict[str, typing.Any]):
    namespace = experiment.model.frontends.dsl.Namespace(**dsl_one_workflow_one_component_two_steps_with_edges)

    print(yaml.safe_dump(namespace.dict(exclude_unset=True, exclude_none=True, exclude_defaults=True, by_alias=True),
        sort_keys=False))

    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)

    print(yaml.safe_dump(flowir.raw(), sort_keys=False))

    raw_flowir = flowir.raw()
    assert len(raw_flowir["components"]) == 2
    assert [x for x in raw_flowir["components"] if x['name'] == 'one'][0] == {"stage": 0, "name": "one",
        "references": [], "command": {"executable": "echo", "arguments": "hello from one world", "environment": "env0"}}

    assert [x for x in raw_flowir["components"] if x['name'] == 'two'][0] == {"stage": 0, "name": "two",
        "references": ["stage0.one:output"],
        "command": {"executable": "echo", "arguments": "hello from two stage0.one:output", "environment": "env0"}}

    assert len(raw_flowir["environments"]["default"]) == 1
    assert raw_flowir["environments"]["default"]["env0"] == {"DEFAULTS": "PATH:LD_LIBRARY_PATH",
        "AN_ENV_VAR": "ITS_VALUE"}

    assert raw_flowir["variables"]["default"]["global"] == {"foo": "world", "unused": "value-unused"}

    errors = flowir.validate()
    assert len(errors) == 0


def test_dsl2_extra_arguments(
    dsl_one_workflow_one_component_two_steps_with_edges: typing.Dict[str, typing.Any]
):
    namespace = experiment.model.frontends.dsl.Namespace(**dsl_one_workflow_one_component_two_steps_with_edges)

    print(yaml.safe_dump(namespace.dict(exclude_unset=True, exclude_none=True, exclude_defaults=True, by_alias=True),
        sort_keys=False))

    with pytest.raises(experiment.model.errors.DSLInvalidError) as e:
        experiment.model.frontends.dsl.namespace_to_flowir(namespace, override_entrypoint_args={
            "foo": "this is ok",
            "bar": "this should raise an exception",
        })

    exc = e.value

    assert len(exc.underlying_errors) == 1
    assert str(exc.underlying_errors[0].underlying_error) == "Unknown parameter bar"
    assert exc.underlying_errors[0].parameter_name == "bar"


def test_validate_dsl_with_unknown_params():
    dsl = yaml.safe_load("""
        entrypoint:
          entry-instance: bad
          execute:
          - target: "<entry-instance>"
            args: {}
        workflows:
        - signature:
            name: bad
          steps:
            unknown: unknown
          execute:
          - target: "<unknown>"
        components:
        - signature:
            name: unknown
          command:
            executable: a
            arguments: "%(hello)s"
        """)

    namespace = experiment.model.frontends.dsl.Namespace(**dsl)
    with pytest.raises(experiment.model.errors.DSLInvalidError) as e:
        _ = experiment.model.frontends.dsl.namespace_to_flowir(namespace)

    exc = e.value

    assert len(exc.underlying_errors) == 1
    print(exc.underlying_errors[0].pretty_error())
    assert exc.underlying_errors[0].location == ["components", 0, "command", "arguments"]
    assert str(exc.underlying_errors[0].underlying_error) == ('Reference to unknown parameter "hello". '
                                                              'Known parameters are {}')


def test_unknown_outputreference():
    dsl = yaml.safe_load("""
    entrypoint:
      entry-instance: outer
      execute:
      - target: "<entry-instance>"
        args: {}
    workflows:
    - signature:
        name: outer
      steps:
        left: left
        right: right
      execute:
      - target: "<left>"
      - target: "<right>"
    - signature:
        name: left
      steps:
        echo-left: echo
      execute:
      - target: "<echo-left>"
        args:
          message: "<right/echo-right>:output"
    - signature:
        name: right
      steps:
        echo-right: not-echo
      execute:
      - target: "<echo-right>"
    components:
    - signature:
        name: echo
        description:
        parameters:
        - name: message
      command:
        executable: echo
        arguments: "%(message)s"
    - signature:
        name: not-echo
        description:
        parameters:
        - name: message
          default: hello
      command:
        executable: echo
        arguments: "%(message)s"
    """)

    namespace = experiment.model.frontends.dsl.Namespace(**dsl)
    with pytest.raises(experiment.model.errors.DSLInvalidError) as e:
        _ = experiment.model.frontends.dsl.namespace_to_flowir(namespace)

    exc = e.value

    assert len(exc.underlying_errors) == 1

    assert exc.errors() == [
        {
            'loc': ['workflows', 1, 'execute', 0, 'signature', 'parameters', 0],
            'msg': 'OutputReference <right/echo-right>:output does not reference any of the known siblings []'
        }
    ]


def test_validate_dsl_with_variables():
    dsl = yaml.safe_load("""
        entrypoint:
          entry-instance: with-vars
          execute:
          - target: "<entry-instance>"
        components:
        - signature:
            name: with-vars
          command:
            executable: echo
            arguments: "%(greeting)s"
          variables:
            greeting: "hi"
        """)

    namespace = experiment.model.frontends.dsl.Namespace(**dsl)
    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace).raw()

    assert flowir['components'][0]['command']['arguments'] == "%(greeting)s"


def test_validate_dsl_with_conflicting_variables_and_parameters():
    dsl = yaml.safe_load("""
        entrypoint:
          entry-instance: bad
          execute:
          - target: "<entry-instance>"
        components:
        - signature:
            name: bad
            parameters:
            - name: greeting
              default: hello
          command:
            executable: echo
            arguments: "%(greeting)s"
          variables:
            greeting: "hi"
        """)

    namespace = experiment.model.frontends.dsl.Namespace(**dsl)
    with pytest.raises(experiment.model.errors.DSLInvalidError) as e:
        experiment.model.frontends.dsl.namespace_to_flowir(namespace)

    exc = e.value
    assert isinstance(e.value, experiment.model.errors.DSLInvalidError)
    assert exc.errors() == [
        {
            'loc': ['components', 0, 'variables', 'greeting'],
            'msg': 'There is a parameter with the same name as the variable'
         }
    ]


def test_validate_dsl_with_replicas():
    dsl = yaml.safe_load("""
        entrypoint:
          entry-instance: with-replicas
          execute:
          - target: "<entry-instance>"
        workflows:
        - signature:
            name: with-replicas
          steps:
            replicate: replica-print
            plain: print
          execute:
          - target: <replicate>
            args:
              replicas: 2
              message: "I am replicating"
          - target: <plain>
            args:
              message: <replicate>:output
        components:
        - signature:
            name: replica-print
            parameters:
            - name: replicas
              default: 0
            - name: message
          command:
            executable: echo
            arguments: "%(message)s from %(replica)s"
          workflowAttributes:
            replicate: "%(replicas)s"
        - signature:
            name: print
            parameters:
            - name: message
          command:
            executable: echo
            arguments: "%(message)s"
          workflowAttributes:
            aggregate: true
        """)

    namespace = experiment.model.frontends.dsl.Namespace(**dsl)
    flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)

    # VV: We can now replicate the graph and get 2 + 1 components
    x = experiment.model.graph.WorkflowGraph.graphFromFlowIR(flowir.raw(), manifest={}, primitive=False)

    _replicating_0 = x.graph.nodes['stage0.replicate0']
    _replicating_1 = x.graph.nodes['stage0.replicate1']
    _plain = x.graph.nodes['stage0.plain']
