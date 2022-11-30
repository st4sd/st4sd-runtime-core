# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import print_function

import experiment.model.codes
import experiment.model.graph

#NOTE: Following test rely in the fixtures creating the inputs to also supply the expected results of parsing those inputs

def test_component_identifier(test_identifier):
    
    '''Test ability of ComponentIdentifier to parse relative and nonrelative component identifiers'''

    cid = experiment.model.graph.ComponentIdentifier(test_identifier['identifier'], test_identifier['index'])
    assert cid.componentName == test_identifier['results']['name']
    assert cid.namespace == test_identifier['results']['namespace']
    assert cid.stageIndex == test_identifier['results']['index']
    assert cid.identifier == test_identifier['results']['identifier']

def test_data_reference(test_reference):

    '''Test ability of DataReference to parse relative and absolute references to components with/without single component paths'''

    ref = experiment.model.graph.DataReference(test_reference['identifier'], test_reference['index'])
    assert ref.producerName == test_reference['results']['name']
    assert ref.namespace == test_reference['results']['namespace']
    assert ref.stageIndex == test_reference['results']['index']
    assert ref.method == test_reference['results']['method']
    assert ref.path == test_reference['results']['filePath']
    assert ref.producerIdentifier.identifier == test_reference['results']['identifier']

def test_multi_component_path_data_reference(test_multicomponent_path_reference):

    '''Test ability of DataReference to parse absolute references to components with multi-component paths'''

    ref = experiment.model.graph.DataReference(test_multicomponent_path_reference['identifier'], test_multicomponent_path_reference['index'])
    assert ref.producerName == test_multicomponent_path_reference['results']['name']
    assert ref.namespace == test_multicomponent_path_reference['results']['namespace']
    assert ref.stageIndex == test_multicomponent_path_reference['results']['index']
    assert ref.method == test_multicomponent_path_reference['results']['method']
    #The file path should be file under the producer without the initial `/`
    assert ref.path == test_multicomponent_path_reference['results']['filePath']
    assert ref.producerIdentifier.identifier == test_multicomponent_path_reference['results']['identifier']

def test_direct_data_reference(test_direct_reference):

    '''Test ability of DataReference to parse references to input files i.e. not in components'''

    ref = experiment.model.graph.DataReference(test_direct_reference['identifier'])
    assert ref.producerName == test_direct_reference['results']['name']
    #Direct references should not have namespaces or stageIndexes
    assert ref.namespace is None
    assert ref.stageIndex is None
    assert ref.method == test_direct_reference['results']['method']
    #The producer identifier should be the containing dir of the file
    assert ref.producerIdentifier.identifier == test_direct_reference['results']['identifier']
    #The file path should be file under the containing dir WITHOUT the `/`
    assert ref.path == test_direct_reference['results']['filePath']
