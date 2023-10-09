# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

'''Module containing functions and classes for working with a Workflow Graph'''
from __future__ import print_function
from __future__ import annotations

import copy
import glob
import hashlib
import logging
import os.path
import pprint
import re
import traceback
import weakref
from typing import (TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set,
                    Tuple, Union, NamedTuple)
from typing import cast

import networkx
import networkx.drawing.nx_agraph
from future.utils import raise_with_traceback
from past.builtins import cmp

from collections import OrderedDict

import experiment.appenv
import experiment.model.conf
import experiment.model.codes
import experiment.model.errors
import experiment.model.executors
import experiment.model.frontends.flowir
import experiment.model.interface
import experiment.model.storage

import experiment.runtime.utilities.container_image_cache


try:
    import StringIO
    StringIO = StringIO.StringIO
except ImportError:
    from io import StringIO


if TYPE_CHECKING:
    from experiment.model.frontends.flowir import FlowIRComponentId
    from experiment.model.conf import FlowIRExperimentConfiguration
    from experiment.model.frontends.flowir import DictFlowIRComponent, DictFlowIRResourceManager
    from experiment.model.storage import ExperimentInstanceDirectory, ExperimentPackage
    import experiment.model.storage

    ExperimentSource = Union[ExperimentPackage, ExperimentInstanceDirectory]


graphLogger = logging.getLogger('graph')

def stream_path_to_index(path, stream_type):
    filename = os.path.split(path)[1]
    name, ext = os.path.splitext(filename)
    if '.%s' % stream_type == ext:
        try:
            return int(name)
        except TypeError:
            return None

    return None


#Primitive functions

def TransitiveReduction(g):

    '''Wraps networkx transitive_reduction so node attributes are preserved'''

    # The transitive_reduction doesn't preserve node attributes
    pt = networkx.transitive_reduction(g)
    t = networkx.DiGraph()
    t.add_nodes_from(g.nodes(data=True))
    t.add_edges_from(pt.edges())

    return t

def Draw(u, edges=None, output=None):

    '''Draws workflow graph u

    Nodes are coloured by stage and given shape dependent on type

    NOTE: This function fail if node names start with `%`
    This has a reserved meaning inside `pygraphviz` and will lead to difficult to understand errors.
    That is, the node name is allowed to start with `%` and this will not raise an error.
    However the `%` will cause certain functions to return None instead of the node-name
    as this None will eventually cause an error.

    For reference this issue stems from `pygraphviz.graphvis.agnameof` in `graphviz.i`
    as of pygraphviz 1.6

    Parameters:
        u: A workflow graph - see below for required node properties
        edges: Optional. A list of edges to draw
        output: Optional. Graph will be saved to this location if given

    U nodes must have the following properties
    isAggregate
    isBlueprint
    stageIndex'''

    import matplotlib.pyplot

    #Get aggregating node categories
    all_nodes = u.nodes(data=True)

    aggregating = [n for n in all_nodes if n[1].get('isAggregate')]
    replicated = [n for n in all_nodes if n[1].get('isBlueprint')]
    other = [n for n in all_nodes if (not n[1].get('isBlueprint') and not n[1].get('isAggregate')) == True]

    # same layout using matplotlib with no labels
    #matplotlib.pyplot.title('draw_networkx')
    matplotlib.pyplot.figure(figsize=(30,30))

    #A = networkx.nx_agraph.to_agraph(e.graph)
    pos=networkx.drawing.nx_agraph.graphviz_layout(u, prog='dot')

    networkx.draw_networkx_nodes(u, pos ,vmin=0, vmax=9,
                                 node_color=[n[1]['stageIndex'] for n in aggregating],
                                 nodelist=[n[0] for n in aggregating],
                                 node_shape='p', node_size=800)
    networkx.draw_networkx_nodes(u, pos, vmin=0, vmax=9,
                                 node_color=[n[1]['stageIndex'] for n in replicated],
                                 nodelist=[n[0] for n in replicated],
                                 node_shape='d', node_size=800)
    networkx.draw_networkx_nodes(u, pos, vmin=0, vmax=9,
                                 node_color=[n[1].get('stageIndex', n[1].get('stage')) for n in other],
                                 nodelist=[n[0] for n in other],
                                 node_shape='o', node_size=800)

    edges = u if edges is None else edges
    networkx.draw_networkx_edges(edges, pos, arrows=False)
    networkx.draw_networkx_labels(u, pos, labels={n[0]:n[0] for n in all_nodes}, font_size=10)

    if output:
        matplotlib.pyplot.savefig(output)

def DrawBlockModel(bm):

    import matplotlib.pyplot

    matplotlib.pyplot.figure(figsize=(4,10))
    pos=networkx.nx_agraph.graphviz_layout(bm, prog='dot')
    networkx.draw(bm, pos, with_labels=True, font_size=8, arrows=True)


def ConnectedComponents(nodeSet, baseGraph):
    '''Given a set of nodes returns the connected components they define'''

    rs = networkx.subgraph(baseGraph, nodeSet)
    partitions = [c for c in networkx.components.connected_components(rs.to_undirected())]

    return partitions


def SubgraphSpanningNodes(x, y, g):

    '''Returns the subgraph spanning from x to y'''

    nodes = []
    paths = networkx.all_simple_paths(g, source=x, target=y)
    nodesInPath = {node for path in paths for node in path}
    nodes.extend(nodesInPath)
    return g.subgraph(nodes)

def ExecutionPath(x, y, p):

    '''Returns the ordered execution path from x to y'''

    s = SubgraphSpanningNodes(x, y, p)
    return list(networkx.topological_sort(s))

def AllLeafPaths(g):

    '''Returns all paths leading from root to leaves of g'''

    leaf_nodes = [node for node in g.nodes() if g.in_degree(node) != 0 and g.out_degree(node) == 0]
    a = []
    for l in leaf_nodes:
        paths = [c for c in networkx.algorithms.dag.root_to_leaf_paths(g) if c[-1] == l]
        a.extend(paths)

    return a

def AllRootToLeafGraphs(g):
    # type: (networkx.DiGraph) -> List[List]

    '''Returns all subgraphs leading from a single root to leaves of g

    Note: Multiple instances of the same subgraph may be returned

    Params:
        g: A networkX Digraph instance

    Returns:
          A list of lists of node-names.
          Each sub-list contains the names of the nodes in a sub-graph from a single root to a leaf
          Each sub-list is topologically sorted so the root is first element and the leaf is the last
    '''

    leaf_nodes = [node for node in g.nodes() if g.in_degree(node) != 0 and g.out_degree(node) == 0]
    a = []
    for l in leaf_nodes:
        paths = [c for c in networkx.algorithms.dag.root_to_leaf_paths(g) if c[-1] == l]
        for path in paths:
            path = ExecutionPath(path[0], path[-1], g)
            a.append(path)

    paths = []
    for el in a:
        paths.append([n for n in el])

    return paths


def AllLeafGraphs(g):
    # type: (networkx.DiGraph) -> List[Set]

    '''Returns the subgraphs leading from roots to leaves of g

    There is one subgraph per leaf node. It includes all root-nodes with a path to the lead

    Params:
        g: A networkX Digraph instance

    Returns:
          A list of sets of node-names.
          Each set contains the names of the nodes in a leaf graph
    '''

    leaf_nodes = [node for node in g.nodes() if g.in_degree(node) != 0 and g.out_degree(node) == 0]
    subgraphs = []

    for l in leaf_nodes:
        paths = [c for c in networkx.algorithms.dag.root_to_leaf_paths(g) if c[-1] == l]
        rootSubgraphs = []
        for path in paths:
            #This is a subgraph to the leaf from a single root node
            rootSubgraph = ExecutionPath(path[0], path[-1], g)
            rootSubgraphs.append(rootSubgraph)

        #Combine all the single root subgraphs to get the complete subgraph to the root node
        s = set()
        for rootSubgraph in rootSubgraphs:
            s.update([n for n in rootSubgraph])

        subgraphs.append(s)

    return subgraphs


def EquivalenceClosure(g, paths, isLinear=False):
    '''Creates a function which identifies nodes that form unique analysis chains

    Two nodes, u and v, are part of a unique chain if for every path to every leaf node
    where u appears v also appears and vice-versa (i.e never independently)

    i.e. it means whenever U is applied to get an output, V must also be applied.

    Params:
        g: The graph
        paths: A list of lists. Each list is a node-set, assumed to be a subgraph, however could be anything
        isLinear: An additional constraint that there is only one route from U->V
            Equivalent to all nodes between U->V having indegree 1.

    Returns:
        A closure, C, such that C(u,v) returns True if u and v are part of a unique chain
    '''

    def equivalence(u, v):

        '''U and V are in the same block if U and V appear together in all paths and never separately'''

        filtered = [x for x in paths if u in x or v in x]
        equivalent = True
        for p in filtered:
            if u in p and v in p:
                continue
            else:
                equivalent = False
                break

        #If equivalent check and isLinear is True check this condition
        #A chain is linear if any of the following conditions is true (all these conditions are equivalent)
        # - if all intervening points are equivalent
        # - No intervening point has indegree > 1
        # - There is only one path from U->V
        #
        #If the second condition is not true then it means is another path to V
        #that doesn't pass through some element in the path (assuming graph only allows one edge between nodes)

        if equivalent and isLinear:
            #All paths in filtered contain both nodes
            #If its linear all these paths must be the same there's only one route
            #If its not-linear every path must have an element that has in-degree > 0.
            #We will use the second condition to check
            path = [n for n in ExecutionPath(u,v,g)]
            for n in path[1:-1]:
                if g.in_degree(n) > 1:
                    equivalent = False
                    break

        return equivalent

    return equivalence

def ReplicationBlockModel(g):

    '''Returns a block-model of g where components are grouped into replication blocks'''

    # Compute the subgraph including all nodes that are replicated
    # All components in a single replicated block are connected
    # Thus partition the above subgraph into connected-components will give all replicated blocks
    partitions = ConnectedComponents([n[0] for n in g.nodes(data=True) if n[1]['isBlueprint'] == True], g)
    other = [n[0] for n in g.nodes(data=True) if n[1]['isBlueprint'] != True]
    partitions.extend([[n] for n in other])

    return networkx.quotient_graph(g, partition=partitions, relabel=True)

def SubgraphAnalysisBlockModel(g):

    '''Returns a block-model of g where components are grouped into analysis blocks

    Blocks are defined by subgraphs all of whoms elements are either traversed or not on the way to a leaf node

    i.e. if any of the components in the graph are required to create a leaf-node all the element in the graph are required'''

    return networkx.quotient_graph(g, partition=EquivalenceClosure(g, AllLeafGraphs(g)), relabel=True)

def PathAnalysisBlockModel(g):

    '''Returns a block-model of g where components are grouped into analysis blocks

    Blocks are defined by contiguous linear paths all of whoms elements are either traversed or not on the way to a leaf node

    i.e. if any of the components in the graph are required to create a leaf-node all the element in the path are required.

    Subgraph blocks can contain cycles i.e. one components output is split to N and then later joined.
    Path blocks are linear

    A subgraph block will contain path blocks.
    '''

    return networkx.quotient_graph(g, partition=EquivalenceClosure(g, AllLeafPaths(g), isLinear=True), relabel=True)


def BlocksToComponentsMap(g):

    '''Returns a dictionary mapping block index to the components in that block

    The components are topologically sorted'''

    # Blocks is a special networkx view object (what g is)
    # If you iterate it using 'for x in y' you will get a list of tuples
    # However if you access it x[i] you will get the second element of the tuple whose first element is [i]

    results = {}
    for n in g.nodes(data=True):
        sort = networkx.topological_sort(n[1]['graph'])
        results[n[0]] = [s for s in sort]

    return results


def ProducersForNode(nodeLabel, graph, reversed=False):

    '''Returns the nodes which are upstream of a node in the given DAG

    Params:
        nodeLabel. A hashable object. The label of a node in the graph
        graph. A networkx.Digraph instance

    Returns:
            A list of node labels
        '''

    # Note graph is directed so their are no edges going to producer only from
    # Have to reference the direction to get producers
    if reversed is False:
        graph = networkx.reverse(graph)

    inputEdges = graph.edges(nodeLabel)

    return [el[1] for el in inputEdges]


def ConsumersForNode(nodeLabel, graph):

    '''Returns the nodes which are downstream of a node in the given DAG

    Params:
        nodeLabel. A hashable object. The label of a node in the graph
        graph. A networkx.Digraph instance
        '''

    # Note graph is directed so their are no edges going to producer only from
    # Have to reference the direction to get producers
    inputEdges = graph.edges(nodeLabel)
    return [el[1] for el in inputEdges]



class ExternalReferenceParameter(NamedTuple):
    """The name follows the rules in _dsl_parameter_name_from_external_reference()

    the value is that of @name that the workflow should propagate to steps
    entrypoing is the value that the entrypoint should propagate to the workflow value
    """
    name: str
    value: str
    entrypoint: str


def _dsl_parameter_name_from_external_reference(ref: str) -> ExternalReferenceParameter:
    """Utility to generates a parameter name from a reference to an external path (input, app-dep, etc)

    Examples: ::

        func("input/foo.txt:ref") -> name=input.foo.txt and value="%(input.foo.txt)":ref
        func("data/foo.txt:ref") -> name=data.foo.txt and value="%(data.foo.txt)":ref
        func("foo:ref") -> name=manifest.foo and value="%(manifest.foo)s":ref
        func("foo/bar.txt:ref") -> name=manifest.foo and value="%(manifest.foo)s/bar.txt":ref

    Arguments:
        ref: A string representation of a DataReference
    """

    _, directory, filename, method = experiment.model.frontends.flowir.FlowIR.ParseDataReferenceFull(ref)

    entrypoint = directory.split(":", 1)[0]

    tokens = directory.split("/", 1)

    if len(tokens) == 2 and tokens[0] in ['input', 'data']:
        producer, filename = tokens

        if filename:
            param_name = '.'.join((producer, filename))
        else:
            param_name = producer
        value = f'"%({param_name})s":{method}'
        return ExternalReferenceParameter(name=param_name, value=value, entrypoint=entrypoint)
    else:
        producer = '.'.join(("manifest", directory))

        if filename:
            value = f'"%({producer})s/{filename}":{method}'
        else:
            value = f'"%({producer})s":{method}'
        return ExternalReferenceParameter(name=producer, value=value, entrypoint=entrypoint)


class ComponentIdentifier:

    '''A component label has the form $namespace.$componentname

    NB: Currently $namespace is assumed to be of form stage$NUM'''

    def __init__(self, name, # type: str
                 index=None  # type: int
                 ):

        '''

        Args:
            name: Must be fully qualified component reference - $namespace.$name
        '''

        self._id = name

        (
            self._index, self._componentName, self.hasIndex
        ) = experiment.model.frontends.flowir.FlowIR.ParseProducerReference(self._id, index)

        self._namespace = "stage%d" % self._index if self._index is not None else None
    
    def to_uid(self, instance:str) -> str:
        """
        
        Args:
            instance: File URI of workflow instance, schema is file://$GATEWAY_ID/absolute/path/to/instance/dir

        Returns:
            A component UID which is in the form of
                file://$GATEWAY_ID/absolute/path/to/instance/dir&stage<stage index>.<component name> .
                We escape the symbols & and % in the UID with '%26' and '%25' respectively
        """
        ref = self.identifier.replace('%', '%25').replace('&', '%26')
        return '&'.join((instance, ref))
    
    def __hash__(self):

        return hash(self.identifier)

    def __eq__(self, other):

        return self.identifier == other.identifier

    def __cmp__(self, other):

        '''Returns the same as cmp(self.stringRepresentation, other.stringRepresentatino)

        DataReference are identical if their string reps are identical'''

        return cmp(self.identifier, other.identifer)

    def __str__(self):

        return self.identifier

    def __repr__(self):
        return 'ComponentIdentifier(%s)' % self.identifier

    @property
    def relativeIdentifier(self):
        # type: () -> str

        '''Returns the relative reference to a component as a string

        stage1.ClusterAnalysis -> Returns ClusterAnalysis

        This is current equivalent to componentName but this name is also used for readability purposes
        '''

        return self.componentName

    @property
    def identifier(self):
        # type: () -> str

        '''Returns the full reference to the component as a string.

        Example stage1.ClusterAnalysis -> Returns stage1.ClusterAnalysis

        If no namespace is given then identifier and relativeIdentifier are the same
        '''

        return "%s.%s" % (self.namespace, self.componentName) if self.namespace is not None else self.componentName

    @property
    def namespace(self):
        # type: () -> str

        '''Returns the namespace part of the component  of None if no namespace is defined.

            Example stage1.ClusterAnalysis/ -> Returns stage1'''

        return self._namespace

    @property
    def stageIndex(self):

        return self._index


    @property
    def componentName(self):
        # type: () -> str

        '''Returns the component name

        Example stage1.ClusterAnalysis -> Returns ClusterAnalysis'''

        return self._componentName

    @property
    def flowir_id(self):
        # type: (ComponentIdentifier) -> FlowIRComponentId
        return (self.stageIndex, self._componentName)


class DataReference(object):
    methods = list(experiment.model.frontends.flowir.FlowIR.data_reference_methods)
    Copy, Link, Ref, CopyOut, Extract, Output, LoopRef, LoopOutput = methods

    # VV: These methods resolve to a Path
    pathMethods = [
        Copy, Link, Ref, CopyOut, Extract, LoopRef, LoopOutput
    ]

    # VV: These methods do not necessarily resolve to a path, but they DO access the FileSystem
    #     you can use the location() method to compute out which is the path that they access
    filesystemMethods = [
        Copy, Link, Ref, CopyOut, Extract, Output, LoopRef, LoopOutput
    ]

    '''Class representing a data reference.

    A data reference object represents output of a producer (where/what it is) and how
    that output should be consumed by a consumer.

    A data reference has the format: $producerReference[/file]:method
    $producerReference has the form $namespace.$producer
    
    NOTE: Current $namespaces must be of form stage$[INTEGER]

    A data reference has up to four parts
        1 - stage (optional)
        2 - producer (directory name or job name)
        3 - files (optional) a file name or a glob
        4 - method: How the reference is to be consumed

    The methods/types of DataReference are
        1 - copy: The reference is to be copied from the producer
        2 - copyout: The reference is to be copied from the producer and is also output of the consumer 
        3 - link: A link to the producer file is to be created
        4 - ref: The reference is just a path.
        5 - extract: The reference is to an archive which will be extracted
        6 - output: Reference description MUST contain a file OR a glob which evaluates to a SINGLE File.
                    The reference evaluates to the contents of the file

    If no files are given the reference is to the working directory of the producer job (except for the 
    case of `output` reference method where if no files are given the reference is considered to be
    invalid)'''

    def __init__(self, reference,  # type: str
                 stageIndex=None  # type: int
                 ):

        """
        Params:
            reference: str
                The reference string
            stageIndex: Optional[int]
                --- (optional) The namespace this DataReference should be interpreted w.r.t
                This is only used if reference contains no namespace component.


        Exceptions
             Raises ValueError if method is not one of (copy/copyout/link/ref)
        """

        identifier, self.fileRef, self._referenceMethod = experiment.model.conf.ParseDataReference(reference)

        if self.method not in DataReference.methods:
            raise ValueError('Unknown method, "%s", in reference "%s"' % (self.method, reference))

        self._producerIdentifier = ComponentIdentifier(identifier, stageIndex)

    def __hash__(self):

        return hash(self.stringRepresentation)

    def __eq__(self, other):

        return self.stringRepresentation == other.stringRepresentation

    def __cmp__(self, other):

        '''Returns the same as cmp(self.stringRepresentation, other.stringRepresentatino)

        DataReference are identical if their string reps are identical'''

        return cmp(self.stringRepresentation, other.stringRepresentation)

    def true_reference_to_component_id(self, workflowGraph):
        # type: (WorkflowGraph) -> Optional[List[FlowIRComponentId]]
        try:
            _ = workflowGraph.graph[self._producerIdentifier.identifier]
        except KeyError as error:
            try:
                placeholder = workflowGraph._placeholders[self.producerIdentifier.identifier]
            except KeyError:
                # VV: this is neither a component nor a placeholder, it has to be a direct reference
                return None
            else:
                if self.method != self.LoopRef:
                    producerIdentifiers = [placeholder['latest']]
                else:
                    producerIdentifiers = placeholder['represents']

                ret = []
                for pid in producerIdentifiers:
                    cid = ComponentIdentifier(pid)
                    ret.append((cid.stageIndex, cid.componentName))
                return ret
        else:
            prod_comp_id = (self._producerIdentifier.stageIndex, self._producerIdentifier.componentName)
            return [prod_comp_id]

    @property
    def stringRepresentation(self):

        return self.absoluteReference

    def __str__(self):

        return self.stringRepresentation

    @property
    def relativeReference(self): # type: (DataReference) -> str

        '''Returns the relative string representation of the receiver'''

        refString = self.producerIdentifier.relativeIdentifier

        if self.fileRef is not None:
            refString = os.path.join(refString, self.fileRef)

        return "%s:%s" % (refString, self.method)

    @property
    def absoluteReference(self):  # type: (DataReference) -> str

        '''Returns the absolute string representation of the receiver'''

        refString = self.producerIdentifier.identifier

        if self.fileRef is not None:
            refString = os.path.join(refString, self.fileRef)

        return "%s:%s" % (refString, self.method)

    @property
    def producerIdentifier(self):  # type: (DataReference) -> ComponentIdentifier

        '''Returns a ComponentIdentifier object representing the component the receiver refers to.

        Example stage1.ClusterAnalysis/somefile.txt:ref -> Returns stage1.ClusterAnalysis'''

        return self._producerIdentifier

    @property
    def producerName(self):  # type: (DataReference) -> str

        return self._producerIdentifier.componentName

    @property
    def namespace(self): # type: (DataReference) -> str

        '''Returns the namespace part of the component  of None if no namespace is defined.

            Example stage1.ClusterAnalysis/somefile.txt:ref -> Returns stage1'''

        return self.producerIdentifier.namespace

    @property
    def stageIndex(self): # type: (DataReference) -> int

        return self.producerIdentifier.stageIndex

    @property
    def path(self):  # type: (DataReference) -> str

        '''Returns the filepath part of the reference or None if no path is define'''

        return self.fileRef

    def location(self, workflowGraph):
        # type: (DataReference, WorkflowGraph) -> str

        try:
            producer_node = workflowGraph.graph.nodes[self.producerIdentifier.identifier]
            try:
                if self.fileRef is None and self.method == DataReference.Output:
                    specification = producer_node['componentSpecification']  # type: ComponentSpecification
                    return specification.path_to_stdout()

                location = producer_node['componentInstance'].directory
            except KeyError:
                raise experiment.model.errors.InternalInconsistencyError(
                    "Attempt to resolve a component reference without concrete instance"
                )
        except KeyError:  # NONODE - Assume direct reference
            producer = self.producerName
            if workflowGraph.rootStorage is not None:
                location = workflowGraph.rootStorage.resolvePath(producer)
            else:
                raise experiment.model.errors.InternalInconsistencyError(
                    "Attempt to resolve a direct reference without defined storage"
                )

        if self.fileRef is not None:
            location = os.path.join(location, self.fileRef)

        return location

    @property
    def method(self):  # type: (DataReference) -> str

        '''Returns the references method'''

        return self._referenceMethod

    def isDirectReference(self, workflowGraph):
        # type: (WorkflowGraph) -> bool
        '''Check if reference is direct w.r.t workflowGraph'''

        isDirect = self.true_reference_to_component_id(workflowGraph) is None

        return isDirect


    def resolve(
            self,  # type: DataReference
            workflowGraph,  # type: WorkflowGraph
        ):
        # type: (...) -> str

        '''Resolves the receiver w.r.t workflowGraph

        The nodes of the workflowGraph must have a 'componentInstance' attribute,
        and the value of the workflowGraph rootStorage property must be non-NONE

        Args:
            workflowGraph:

        Returns:
            A path as a string'''

        #Note: We can have the concept of the 'level' of a ref. That is the amount of information
        #the graph must contain to resolve the reference
        #Currently all ref types required L2 graphs (the condition that must have componentInstance attrib)
        #Its possible to envision refs that require L3 (run time info as well) in future
        #If the ref knows its level then this method could auto-check if it can resolve a given ref.

        isDirect = False

        producer = self.producerName

        def looped_reference_to_paths():
            """Returns path of looped reference"""
            # VV: AggregatedRef methods can only be applied to Placeholder nodes and they expand to *all*
            #     loop-instances of the placeholder component (i.e. to the components it `represent`s)
            try:
                placeholder = workflowGraph._placeholders[self.producerIdentifier.identifier]
            except KeyError:
                msg = "%s DataReference uses unknown Placeholder component %s" % (
                    self.absoluteReference, self.producerIdentifier.identifier
                )
                raise_with_traceback(experiment.model.errors.DataReferenceInconsistencyError(self, msg))

            represents = placeholder['represents']
            represents = sorted(
                represents,
                # VV: Sort on iteration number from stage<idx:%d>.<iteration-no:%d>#<name:str>
                key=lambda c: c.split('.', 1)[1].split('#', 1)[0]
            )

            agg_references = []
            for name in represents:
                try:
                    node = workflowGraph.graph.nodes[name]
                    try:
                        specification = node['componentSpecification']  # type: ComponentSpecification
                        cid = specification.identification
                        location = workflowGraph.rootStorage.workingDirectoryForComponent(cid.stageIndex, cid.componentName)
                    except Exception as e:
                        msg = "Attempt to resolve a component reference without concrete instance. Error: %s" % e
                        graphLogger.critical(msg)
                        raise_with_traceback(experiment.model.errors.InternalInconsistencyError(msg))
                except Exception:
                    msg = "%s DataReference with Placeholder component %s (%s) references unknown component %s. " \
                          "Known components: %s" % (
                        self.absoluteReference, self.producerIdentifier.identifier, placeholder, name,
                        list(workflowGraph.graph.nodes)
                    )
                    raise_with_traceback(experiment.model.errors.DataReferenceInconsistencyError(self, msg))
                else:
                    if self.method == DataReference.LoopOutput:
                        if self.fileRef is None:
                            reference = specification.path_to_stdout()
                        else:
                            reference = os.path.join(location, self.fileRef)
                    else:
                        if self.fileRef:
                            reference = os.path.join(location, self.fileRef)
                        else:
                            reference = location

                    agg_references.append(reference)
            return agg_references

        if self.method == DataReference.LoopRef:
            agg_references = looped_reference_to_paths()
            return ' '.join(agg_references)
        elif self.method == DataReference.LoopOutput:
            agg_references = looped_reference_to_paths()

            not_found = []
            contents = []
            for path in agg_references:
                if os.path.exists(path):
                    try:
                        with open(path, 'r') as f:
                            contents.append(f.read().rstrip('\n'))
                    except Exception as e:
                        graphLogger.warning("Could not read LoopOutput reference %s on behalf of %s. Error: %s" % (
                            path, self.stringRepresentation, e
                        ))
                        not_found.append(path)
                else:
                    graphLogger.warning("LoopOutput reference %s on behalf of %s not found"% (
                        path, self.stringRepresentation
                    ))
                    not_found.append(path)

            if not_found:
                raise_with_traceback(experiment.model.errors.DataReferenceFilesDoNotExistError(
                    [(self, input_file) for input_file in not_found]
                ))
            return ' '.join(contents)

        prod_comp_id = (self._producerIdentifier.stageIndex, self._producerIdentifier.componentName)

        if self.producerIdentifier.identifier in workflowGraph._placeholders:
            producerIdentifier = workflowGraph._placeholders[self.producerIdentifier.identifier]['latest']
            graphLogger.log(14, "DataReference \"%s\" refers to placeholder %s" % (
                self.stringRepresentation, producerIdentifier
            ))
        else:
            producerIdentifier = self.producerIdentifier.identifier

        try:
            producer_node = workflowGraph.graph.nodes[producerIdentifier]
            try:
                specification = producer_node['componentSpecification']  # type: ComponentSpecification
                cid = specification.identification
                location = workflowGraph.rootStorage.workingDirectoryForComponent(cid.stageIndex, cid.componentName
                                                                                  )
            except Exception as e:
                raise_with_traceback(experiment.model.errors.InternalInconsistencyError(
                    "Attempt to resolve a component reference without concrete instance"
                ))
        except KeyError: #NONODE - Assume direct reference
            graphLogger.log(14, "Could not find a component node for the id %s (comp_id=%s)" % (
                producerIdentifier, prod_comp_id))

            isDirect = True
            if workflowGraph.rootStorage is not None:
                location = workflowGraph.rootStorage.resolvePath(producer)
            else:
                raise_with_traceback(experiment.model.errors.InternalInconsistencyError(
                    "Attempt to resolve a direct reference without defined storage"
                ))

        if self.fileRef is None:
            reference = location
        else:
            reference = os.path.join(location, self.fileRef)

        # VV: 'output' types resolve to the contents of the file that they point to (or stdout of component
        #     if fileRef is not specified)
        if self.method == DataReference.Output:
            if isDirect or self.fileRef is not None:
                matched = glob.glob(reference)

                if len(matched) == 0:
                    raise experiment.model.errors.DataReferenceFilesDoNotExistError([(self, reference)])
                elif len(matched) > 1:
                    msg = ("Illegal 'output' reference to files: \"%s\". 'output' "
                           "references must point to exactly one file." % matched)
                    raise experiment.model.errors.DataReferenceInconsistencyError(self, msg)

                input_file = matched[0]
            else:
                producer_spec = producer_node['componentSpecification']  # type: ComponentSpecification
                input_file = producer_spec.path_to_stdout()

            if os.path.exists(input_file) is False:
                if isDirect:
                    msg = "Direct reference file %s does not exist !" % input_file
                    graphLogger.warning(msg)
                    raise experiment.model.errors.DataReferenceFilesDoNotExistError([(self, input_file)])
                else:
                    msg = "Could not find file %s produced by perhaps it's not produced yet" % (
                        input_file
                    )
                    graphLogger.debug(msg)
                    raise experiment.model.errors.DataReferenceFilesDoNotExistError([(self, input_file)])

            if os.path.isfile(input_file) is False:
                msg = ":output DataReferences can only point to Files (%s is a Directory)" % input_file
                raise_with_traceback(experiment.model.errors.DataReferenceInconsistencyError(self, msg))
            try:
                with open(input_file, 'rb') as f:
                    # VV: FIXME We should probably put an upper limit to the number of bytes that we can read
                    contents = f.read()
                    try:
                        # VV: DataReferences may point to binary files, to avoid inserting binary symbols into
                        # cmd-lines convert the contents of the referenced file to to utf-8 while replacing
                        # non-utf-8 symbols with the U+FFFD character (question mark with black background)
                        contents = contents.decode('utf-8', 'replace')
                        contents = contents.rstrip('\n')
                    except AttributeError:
                        # VV: keep python3 happy
                        pass
                    except Exception as e:
                        graphLogger.log(15, 'Error while decoding %s: %s' % (input_file, e))
                        graphLogger.log(15, traceback.format_exc())
                        graphLogger.warning("Could not decode contents of %s to utf-8 -- will use bytes" % input_file)
                return contents
            except Exception as  e:
                if isDirect:
                    msg = "Exception encountered %s while reading contents of file %s ('output' reference)" % (
                        e, input_file)

                    graphLogger.warning("%s\nException: %s" % (msg, traceback.format_exc()))
                    raise_with_traceback(experiment.model.errors.DataReferenceInconsistencyError(self, msg))
                else:
                    msg = "Could not find file %s produced by perhaps it's not produced yet" % (input_file)
                    graphLogger.debug(msg)
                    raise_with_traceback(
                        experiment.model.errors.DataReferenceFilesDoNotExistError([(self, input_file)]))

        return reference

# What Specification needs from the WFG
# Node Configuration Options
#    wfg = configurationForNode
#    wfg = setOptionForNode
#    wfg = removeOptionsForNode
#    wfg = environmentForNode
# Root Storage (rootStorage) - for resolving relative paths (executables in direct case)
# DataReferences - the means to resolve them both to entities in graph and to locations on disk
#    wfg = dataReferencesForNode
#    wfg = graph (to discriminate producer and data references, for checking data-refs (cycles))
#    wfg = self (to resolve argument strings -> need to pass wfg to DataReference.resolve() as it needs wfg.graph and wfg.rootStorage )


class ComponentSpecification(experiment.model.interface.InternalRepresentationAttributes):

    '''View on a node in a WorkflowGraph

    Instances of this class will (eventually) read/write directly to the configuration stored in workflowGraph

    Currently worfklowGraph does not store FlowIR but the DOSINI conf-files.
    As a result for the moment this class reads the conf from the files

    '''

    def __init__(self,  # type: ComponentSpecification
                 identifier, # type: ComponentIdentifier
                 workflowGraph  # type: WorkflowGraph
                 ):

        '''

        Args:
            identifier: A ComponentIdentifier instance
            workflowGraph: A WorkflowGraph instance
        '''

        self.workflowGraphRef = weakref.ref(workflowGraph)  # type: Callable[[], WorkflowGraph]
        self._identification = identifier

        # VV: memoization_info is used to generate (and cache) memoization_hash
        self._memoization_info = None  # type: Optional[Dict[str, Any]]
        self._memoization_hash = None  # type: Optional[str]
        self._memoization_info_fuzzy = None  # type: Optional[Dict[str, Any]]
        self._memoization_hash_fuzzy = None  # type: Optional[str]

    def memoization_reset(self):
        self._memoization_hash = None
        self._memoization_info = None
        self._memoization_info_fuzzy = None
        self._memoization_hash_fuzzy = None

    def path_to_stdout(self):
        """Returns path to stdout.

        Returns most recently generated file for RepeatingEngines (relies on the fact that RepeatingEngines generate
        indexed stdout files under a `streams` folder located in their working directory).
        """
        location = self.workflowGraph.rootStorage.workingDirectoryForComponent(self.identification.stageIndex,
                                                                               self.identification.componentName)

        if self.workflowAttributes['isRepeat']:
            streams_dir = os.path.join(location, 'streams')

            stream_type = 'stdout'
            search = os.path.join(streams_dir, '*.%s' % stream_type)
            existing = glob.glob(search)
            existing = [stream_path_to_index(path, stream_type) for path in existing]
            existing = [index for index in existing if index is not None]

            if not existing:
                raise experiment.model.errors.DataReferenceFilesDoNotExistError(
                    [(self, search)]
                )

            # VV: There are some stdout streams, so just grab the most recent one
            index = max(existing)
            return os.path.join(location, 'streams', '%d.%s' % (index, stream_type))
        else:
            return os.path.join(location, 'out.stdout')

    @staticmethod
    def _memoization_info_to_hash(info):
        if info is None:
            return None

        remaining = [info]
        buf = ''

        while remaining:
            obj = remaining.pop(0)
            if isinstance(obj, dict):
                for k in sorted(obj, reverse=True):
                    remaining.insert(0, k)
                    remaining.insert(1, obj[k])
            elif isinstance(obj, experiment.model.frontends.flowir.PrimitiveTypes) or obj is None:
                buf += str(obj)
            elif isinstance(obj, list):
                for k in sorted(obj, reverse=True):
                    remaining.insert(0, k)
            else:
                raise ValueError("Cannot generate hash of %s: %s" % (type(obj), obj))

        md5 = hashlib.md5()
        md5.update(buf.encode('utf-8'))

        return md5.hexdigest()

    def dsl_component_blueprint(self) -> Dict[str, Any]:
        """Returns the component blueprint in the DSL of st4sd.

        This is a WIP method and subject to changes.
        """
        comp_no_platform = self.workflowGraphRef().configurationForNode(
            self.identification.identifier, raw=True,
            omitDefault=True, is_primitive=True)
        comp_with_platform = self.workflowGraphRef().configurationForNode(
            self.identification.identifier, raw=True,
            omitDefault=False, is_primitive=True)

        comp_vars = OrderedDict(comp_no_platform.get('variables', {}))

        all_var_refs = experiment.model.frontends.flowir.FlowIR.discover_references_to_variables(comp_with_platform)

        # VV: Order of parameters:
        # 1. reference parameters
        # 2. parameters which define values of variables (e.g. platform)
        # 3. the env-var parameter
        parameters = []

        command = comp_with_platform.get('command', {})

        # VV: In the new DSL the references are auto-generated from parameters whose value follows the
        # <producer>[/fileref]:<method> schema. Need to auto-generate parameters for references and then rewrite
        # the instances of references in the arguments (because that's the only place that they can appear) so that
        # they look like parameter references (i.e. %(hello)s).
        # Use param<index of reference> for the name of the auto-generated parameter.
        # FIXME I cannot think of a way to fully handle a :copy reference - the developer may have used information
        # encoded in the reference (e.g. the filename that gets copied into) in the arguments.
        top_level_folders = self.workflowGraph.configuration.top_level_folders
        app_deps = self.workflowGraph.configuration.get_application_dependencies()

        arguments: str = command.get('arguments', '')

        for (i, ref) in enumerate(comp_with_platform['references']):
            new_param = f"param{i}"
            parameters.append({'name': new_param})
            if not arguments:
                continue

            # VV: Completely expand reference and identify whether it's to a Component or a directory (e.g. app-dep)
            stageIndex, jobName, filename, method = experiment.model.frontends.flowir.FlowIR.ParseDataReferenceFull(
                ref, index=self.identification.stageIndex,special_folders=top_level_folders,
                application_dependencies=app_deps)
            ref = experiment.model.frontends.flowir.FlowIR.compile_reference(jobName, filename, method, stageIndex)

            dref = DataReference(ref)
            arguments = arguments.replace(dref.absoluteReference, f'%({new_param})s')
            if dref.stageIndex == self.identification.stageIndex:
                arguments = arguments.replace(dref.relativeReference, f'%({new_param})s')

        if arguments:
            command['arguments'] = arguments

        parameters.extend([{'name': name} for name in sorted(set(all_var_refs).difference(comp_vars))])

        # VV: In the new DSL environment is either a dictionary of key: value items OR a reference to a parameter
        if 'environment' in command:
            # VV: We can generate a parameter, call it "env-vars" and assign the environment to it so that
            # callers of this component can update the environment. If the component already has such a variable
            # then just use the entire environment as is with no way to override it.

            if any(filter(lambda x: x['name'] == 'env-vars', parameters)):
                command['environment'] = self.environment
            else:
                parameters.append({'name': 'env-vars', 'default': self.environment})
                command['environment'] = "%(env-vars)s"

        signature=OrderedDict( (('name', self.identification.identifier), ('parameters', parameters)) )
        dsl = OrderedDict(( ('signature', signature), ))

        if comp_vars:
            dsl['variables'] = comp_vars

        dsl['command'] = command

        keep = ['resourceManager', 'resourceRequest', 'workflowAttributes']
        for x in keep:
            if x in comp_with_platform:
                dsl[x] = comp_with_platform[x]

        return dsl

    def get_dsl_args(self) -> Dict[str, Any]:
        """Returns the arguments for the DSL blueprint of this component

        This is the dictionary for calling the "signature" of this component.
        """

        comp_no_platform = self.workflowGraphRef().configurationForNode(
            self.identification.identifier, raw=True,
            omitDefault=True, is_primitive=True)
        comp_with_platform = self.workflowGraphRef().configurationForNode(
            self.identification.identifier, raw=True,
            omitDefault=False, is_primitive=True)

        own_vars = comp_no_platform.get('variables', {})

        all_var_refs = experiment.model.frontends.flowir.FlowIR.discover_references_to_variables(comp_with_platform)

        # VV: All variables that are not explicitly set by the component must be arguments of the parent workflow
        my_args = {name: f"%({name})s" for name in sorted(set(all_var_refs).difference(own_vars))}

        top_level_folders = self.workflowGraph.configuration.top_level_folders
        app_deps = self.workflowGraph.configuration.get_application_dependencies()

        # VV: References are just parameters called "param<Number>"
        for (i, ref) in enumerate(comp_with_platform['references']):
            new_param = f"param{i}"
            # VV: Completely expand reference and identify whether it's to a Component or a directory (e.g. app-dep)
            stageIndex, jobName, filename, method = experiment.model.frontends.flowir.FlowIR.ParseDataReferenceFull(
                ref, index=self.identification.stageIndex, special_folders=top_level_folders,
                application_dependencies=app_deps)
            ref = experiment.model.frontends.flowir.FlowIR.compile_reference(jobName, filename, method, stageIndex)

            if stageIndex is not None:
                # VV: This is a dependency to a Component, rewrite the reference to <StepName>[/fileref]:method
                dref = DataReference(ref)

                step_ref = f"<{dref.producerIdentifier.identifier}>"
                if dref.fileRef:
                    step_ref += "/" + dref.fileRef
                step_ref += ":" + dref.method
                my_args[new_param] = step_ref
            else:
                # VV: this is a reference to a path that is not computed by other steps (app-dep, data, input, etc)
                # this reference will have a corresponding workflow parameter.
                wf_param = _dsl_parameter_name_from_external_reference(ref)
                my_args[new_param] = wf_param.value

        return my_args

    @property
    def memoization_info(self):
        if not self._memoization_info:
            self._memoization_info = self._compute_memoization_info(False)
        return copy.deepcopy(self._memoization_info)

    @property
    def memoization_hash(self):
        if not self._memoization_hash:
            self._memoization_hash = self._memoization_info_to_hash(self.memoization_info)
        return self._memoization_hash

    @property
    def memoization_info_fuzzy(self):
        if not self._memoization_info_fuzzy:
            self._memoization_info_fuzzy = self._compute_memoization_info(True)
        return copy.deepcopy(self._memoization_info_fuzzy)

    @property
    def memoization_hash_fuzzy(self):
        if not self._memoization_hash_fuzzy:
            self._memoization_hash_fuzzy = self._memoization_info_to_hash(self.memoization_info_fuzzy)
            # VV: This is to ensure that there're no conflicts between flow-generated and user-generated fuzzy hashes
            if self.workflowAttributes.get('memoization', {}).get('embeddingFunction'):
                self._memoization_hash_fuzzy = '-'.join(('custom', self._memoization_hash_fuzzy))
        return self._memoization_hash_fuzzy

    def _compute_memoization_info(self, fuzzy=False):
        # VV: Attempt to process input-files so that we can find out that we shouldn't recurse early
        #     (recursion is safe because Workflow graphs are acyclic (DAG))
        log = logging.getLogger(self.identification.identifier)

        def truncate_file(path, max_size=64 * 1024):
            ret = StringIO()
            with open(path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    ret.write(chunk[:max_size - len(chunk)].decode('utf-8'))
                    del chunk
                    if ret.tell() >= max_size:
                        break

            return ret.getvalue()

        def md5_of_file(path):
            # VV: see: https://stackoverflow.com/a/3431838
            md5 = hashlib.md5()
            with open(path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    md5.update(chunk)
            return md5.hexdigest()

        lbl = 'fuzzy' if fuzzy else 'strong'

        try:
            # VV: If this is non-empty then the workflow developer has asked to generate their own fuzzy hashes
            custom_js = self.workflowAttributes.get('memoization', {}).get('embeddingFunction')

            datarefs = sorted(self.dataReferences,
                              key=lambda d: len(d.stringRepresentation), reverse=True)

            info_files = {}

            for d in datarefs:
                identifier = d.producerIdentifier.identifier
                if identifier not in self.workflowGraph.graph.nodes:
                    # VV: Include references to files generated by producers, absolute paths, or ones bundled in the
                    #     experiment instance (i.e. data/input/bin)
                    prod_ref = None
                else:
                    prod_ref = identifier

                try:
                    location = d.location(self.workflowGraph)
                except (experiment.model.errors.DataReferenceInconsistencyError,
                        experiment.model.errors.InternalInconsistencyError,
                        experiment.model.errors.DataReferenceFilesDoNotExistError) as e:
                    log.info("Cannot compute %s memoization info because %s for DataReference %s" % (
                        lbl, e, d.stringRepresentation))
                    return None
                try:
                    # VV: Refuse to compute memoization hashes when referencing paths that do not exist
                    if not location:
                        raise NotImplementedError("cannot handle %s reference because "
                                                  "it does not have a location on the filesystem" % d.absoluteReference)

                    if os.path.exists(location) is False:
                        if (custom_js and fuzzy) or not fuzzy or prod_ref is None:
                            log.info("Path %s does not exist - will not compute %s memoization" % (location, lbl))
                            return None

                    file_hash = ''

                    if os.path.isdir(location):
                        # VV: We're referencing a folder - raise an exception if we're expecting contents
                        if d.method in [DataReference.Output, DataReference.LoopOutput]:
                            raise ValueError("%s reference to folder instead of a file" % d.absoluteReference)
                        # VV: skip reference, we don't want it to end up in the `info_files` dictionary. This object
                        #     is used by the code that follows this loop to decide whether references point to files
                        #     or to producers of files/directories.
                        continue

                    # VV: At this point we're certain that the current referenced path either doesnt exist, or is a file
                    # If the file does not exist then this is an error as the reference is invalid
                    if os.path.isfile(location) is False:
                        raise ValueError("%s references a file which does not exist - will not generate %s memoization"
                                         % (d.absoluteReference, lbl))

                    # VV: Compute the file_hash IFF FILE exists AND one of the following is true:
                    # a) not fuzzy, OR
                    # b) direct reference to FILE not generated by component, OR
                    # c) JS code to generate Custom memoization hash AND fuzzy
                    if (custom_js and fuzzy) or (not fuzzy) or (fuzzy and prod_ref is None):
                        file_hash = md5_of_file(location)

                    if not fuzzy:
                        if not file_hash:
                            raise ValueError("Hash of %s is empty - will not generate %s memoization"
                                % (d.absoluteReference, lbl))
                        info_files[d.absoluteReference] = {
                            'hash': file_hash, 'method': d.method, 'location': location, 'md5_hash': file_hash,}
                    elif fuzzy:
                        # VV: In Fuzzy memoization we use hashes for files not generated by Components, and
                        # references to the hashes of the Producer for files generated by Components
                        if prod_ref is None:
                            if not file_hash:
                                raise ValueError("Hash of %s is empty - will not generate %s memoization"
                                                 % (d.absoluteReference, lbl))
                            prod_hash = file_hash
                        else:
                            node = self.workflowGraphRef().graph.nodes[prod_ref]
                            producer = node['componentSpecification']  # type: ComponentSpecification
                            prod_hash = producer.memoization_hash_fuzzy
                            prod_hash = '#'.join(('fuzzy', prod_hash, d.fileRef or ''))
                        info_files[d.absoluteReference] = {'hash': prod_hash, 'method': d.method, 'location': location}
                        if custom_js:
                            info_files[d.absoluteReference]['md5_hash'] = file_hash
                except Exception as e:
                    log.log(15, traceback.format_exc())
                    log.info("Referenced path %s resulted in %s - will not compute %s memoization" % (location, e, lbl))
                    return None
            info_producers = {}
            for p in self.producers.values():
                idef = p.identification.identifier
                if idef not in info_producers:
                    if fuzzy is False:
                        info_producers[idef] = p.memoization_hash
                    else:
                        info_producers[idef] = p.memoization_hash_fuzzy

            arguments = self.commandDetails.get('arguments', '')  # type: str

            # VV: Discover references in the arguments string and use that to decide whether to replace the absolute
            # or relative form of a reference. When replacing use regular expressions to make sure that we don't
            all_references = {}
            comp_ids = self.workflowGraph.configuration.get_flowir_concrete(False).\
                get_component_identifiers(False, True)

            experiment.model.frontends.flowir.FlowIR.discover_reference_strings(
                arguments, self.identification.stageIndex,
                comp_ids, all_references)

            for i, d in enumerate(datarefs):
                original_reference = None
                if d.absoluteReference in all_references:
                    original_reference = d.absoluteReference
                elif d.relativeReference in all_references:
                    original_reference = d.relativeReference

                if original_reference is None:
                    # VV: the reference is not present in the arguments string, this is fine for methods such as
                    # copy, link, extract, skip replacing it
                    continue

                if d.absoluteReference in info_files:
                    file_hash = info_files[d.absoluteReference]['hash']
                    if not file_hash:
                        raise ValueError("Referenced file %s does not have a %s hash" % (
                            info_files[d.absoluteReference]['location'], lbl))
                    replacement = ':'.join(('file', file_hash))
                elif d.producerIdentifier.identifier in info_producers:
                    prod_hash = info_producers[d.producerIdentifier.identifier]
                    if not prod_hash:
                        raise ValueError("Producer %s does not have a %s hash" % (
                            d.producerIdentifier.identifier, lbl))
                    replacement = ':'.join(('producer' if not fuzzy else 'fuzzy', prod_hash))
                else:
                    continue
                replacement = ':'.join((replacement, d.method))
                pattern = re.compile(r'\b' + re.escape(original_reference) + r'\b')
                arguments = re.sub(pattern, replacement, arguments)

            blueprint_name = self.identification.componentName.rstrip('0123456789')

            # VV: We need to fetch the executables before they were resolved. We don't want to have to resolve
            #     the executables of archived experiments before generating the memoization hashes of the components
            #     (it may even be impossible if we're computing the memoization hash of a component
            #     on a platform that does not have say, a Kubernetes backend).
            comp_id = (self.identification.stageIndex, blueprint_name)
            comp_flowir = self.workflowGraph.configuration._unreplicated.get_component_configuration(
                comp_id=comp_id, raw=False, include_default=True, is_primitive=True, ignore_convert_errors=True,
                inject_missing_fields=True)

            info_commandline = {
                'executable': comp_flowir.get('command', {}).get('executable', ''),
                'arguments': arguments,
            }

            resourceManager = self.resourceManager
            backend_type = resourceManager.get('config', {}).get('backend', 'local')

            def postprocess_backend(backend_type, info_backend):
                # type: (str, Dict[str, Any]) -> Dict[str, Any]
                # VV: The only backend types that have relative info are kubernetes and lsf, the info we care
                #     about is pertaining to the image that the container uses
                if backend_type == 'kubernetes':
                    # VV: The kubernetes backend always uses an image
                    return {'image': info_backend['image']}
                elif backend_type == 'lsf' and info_backend.get('dockerImage'):
                    # VV: lack of a dockerImage is equivalent to using the local backend
                    # VV: Use a consistent key with the kubernetes backend
                    return {'image': info_backend['dockerImage']}
                return {}

            info_backend = postprocess_backend(backend_type, resourceManager.get(backend_type, {}))
            ret = {
                # VV: We don't care about the names of the files or the producers that generated them, we only care
                #     about the hashes of the files, and the reference methods
                'files': [':'.join((info_files[k]['hash'], info_files[k]['method'])) for k in info_files],
                'command': info_commandline,
                'backend': info_backend,
            }

            if fuzzy and custom_js:
                # VV: lazy import Javascript modules only when there's at least 1 component with custom fuzzy hash
                import js2py
                # VV: Not using the `as interpreter_js` leads to the `experiment` local variable to be
                # garbage collected in turn references to `experiment.<anything>` all raise UnboundLocalError
                # exceptions even if the imports are at the top of the file.
                import experiment.runtime.interpreters.js as interpreter_js

                files_md5 = {k: info_files[k]['md5_hash'] for k in info_files}
                files = {k: truncate_file(info_files[k]['location']) for k in info_files}
                upstream = {k: info_producers[k] for k in info_producers}

                # VV: js2py adds an arguments parameter
                js_function = """function $(_arguments, executable, image, upstream, files, files_md5, fuzzy_info) {
                    arguments=_arguments;
                    %s
                }""" % custom_js
                js_function = js2py.eval_js(js_function)
                ret = js_function(
                    info_commandline['arguments'], info_commandline['executable'], info_backend.get('docker', None),
                    upstream, files, files_md5, ret)

                ret = interpreter_js.convert_js_object_to_python(ret)
        except Exception as e:
            log.log(15, traceback.format_exc())
            log.info("Unexpected exception %s - will not generate %s memoization info" % (e, lbl))
            return None
        else:
            return ret

    @property
    def identification(self):
        # type: () -> ComponentIdentifier
        return self._identification

    @property
    def workflowGraph(self) -> WorkflowGraph:
        '''Returns the experiment.model.graph.WorkflowGraph object the receiver is part of'''

        return self.workflowGraphRef()

    @property
    def configuration(self):
        # type: () -> DictFlowIRComponent

        return self.workflowGraphRef().configurationForNode(
            self.identification.identifier, raw=self.workflowGraph.configuration.is_raw,
            omitDefault=False, is_primitive=self.workflowGraph.isPrimitive)

    @property
    def commandDetails(self):
        # type: () -> Dict[str, Any]
        """Returns the component specifications `command` section"""
        comp = self.configuration

        command = comp['command']

        return command

    @property
    def command(self) -> experiment.model.executors.Command:
        '''Returns an executor.Command object representing the receivers command line

        Important!: This is not a stateful object. A new instance is returned on each call to this property

            Notes: This method
            - Will attempt to resolve the raw argument string w.r.t the storage.
                NB No resolution will occur if 'componentInstance' data has
                not been added to the graphs nodes: see DataReference.resolve().
                This will not raise an error
            - Will attempt to resolve executable with relative paths w.r.t rootStorage, if this available
            - Will resolve pathless executable unless config options specify otherwise
            - If workflowGraph has rootStorage this will be used a workingDir when resolving shell substitutions
            - If rootStorage is not set the current working directory will be used as the working dir when resolving shell substitutions
            '''

        import experiment.runtime.backends_base

        commandDetails = self.commandDetails
        executable = commandDetails['executable']
        rootStorage = self.workflowGraphRef().rootStorage  # type: ExperimentInstanceDirectory
        rootLocation = None if rootStorage is None else rootStorage.location

        #TODO: Possibly should be moved
        backend_type = self.resourceManager['config']['backend']
        if backend_type not in ['kubernetes', 'docker']:
            executableChecker = experiment.model.executors.LocalExecutableChecker()
        elif backend_type == 'kubernetes':
            executableChecker = experiment.runtime.backends_base.KubernetesExecutableChecker(
                resourceManager=self.resourceManager)
        else:
            executableChecker = experiment.runtime.backends_base.DockerExecutableChecker(
                resourceManager=self.resourceManager)

        #Do not attempt to find pathless executables here.
        #The default is to assuming pathless executable will be found in the environment and only check if explicitly asked
        #In this case it will happend if checkExecutable is called
        try:
            return experiment.model.executors.Command(executable,
                                                            arguments=self.resolveArguments(ignoreErrors=True),
                                                            workingDir=rootLocation,
                                                            environment=self.environment,
                                                            basePath=rootLocation,
                                                            resolvePath=commandDetails.get('resolvePath'),
                                                            resolveShellSubstitutions=False,
                                                            useCommandEnvironment=True,
                                                            executableChecker=executableChecker
                                                            )
        except Exception as e:
            graphLogger.critical("Failed to generate command() for %s with configuration %s" % (
                self.identification.identifier, self.configuration))
            raise_with_traceback(e)


    @property
    def environment(self):
        # type: () -> Dict[str, Any]
        env = self.workflowGraphRef().environmentForNode(self.identification.identifier)

        return env

    @property
    def resourceRequest(self):
        # type: () -> Dict[str, Any]

        """Returns the ComponentSpecifications resource-request"""

        # VV: Fetch the fully resolved FlowIR because this field may contain typed options that are generated
        #     via string-interpolation
        comp = self.configuration

        return comp['resourceRequest']

    @property
    def resourceManager(self):
        # type: () -> DictFlowIRResourceManager

        # VV: Fetch the fully resolved FlowIR because this field may contain typed options that are generated
        #     via string-interpolation
        comp = self.configuration

        resourceManager = comp['resourceManager']

        return resourceManager

    @property
    def executors(self):
        # type: () -> Dict[str, List[Dict[str, Dict[str, str]]]]
        comp = self.configuration

        executors = comp['executors']

        # VV: Remove lsf-dm-in and lsf-dm-out when Flow is not running in hybrid mode
        if experiment.appenv.HybridConfiguration.defaultConfiguration().isHybrid is False:
            if 'pre' in executors:
                executors['pre'] = [x for x in executors['pre'] if x.get('name') != 'lsf-dm-in']

            if 'post' in executors:
                executors['post'] = [x for x in executors['post'] if x.get('name') != 'lsf-dm-out']

        return executors

    @property
    def workflowAttributes(self):
        # type: () -> Dict[str, Any]

        """Returns any workflow attributes defined for the component

        Raises ValueError if any of the workflow-attributes are invalid.
        """

        # VV: Fetch the fully resolved FlowIR because this field may contain typed options that are generated
        #     via string-interpolation
        comp = self.configuration

        workflowAttributes = comp['workflowAttributes']

        return workflowAttributes

    @property
    def isAggregating(self):
        # type: () -> bool
        """Returns whether Node is aggregating replicated nodes"""
        return self.workflowAttributes['aggregate'] is True

    @property
    def isReplicating(self):
        # type: () -> bool
        """Returns whether Node is replicating"""
        return self.workflowAttributes['replicate'] not in [None, 0]

    @property
    def isLooping(self):
        # type: () -> bool
        """Returns whether Node is Looping"""
        # VV: This is an ugly way to check whether a component is looped
        return "loopIteration" in self.customAttributes and "#" in self.identification.identifier

    @property
    def isAggregatingLoopedNodes(self):
        # type: () -> bool
        """Returns whether Node is aggregating looped nodes

        The node may not have the `aggregate` workflowAttribute hot, BUT it may still be
        referencing Placeholder with `:loopref` or `:loopoutput` DataReferences which *do*
        resolve into multiple nodes. In other words, this node could be an implicit-aggregator
        """
        for dr in self.dataReferences:
            if dr.method in [DataReference.LoopRef, DataReference.LoopOutput]:
                return True

        return False

    @property
    def customAttributes(self):
        # type: () -> Dict[str, Any]
        """Returns copy of variables
        """

        comp = self.configuration

        variables = comp.get('variables', {})

        return variables

    @property
    def rawDataReferences(self):
        # type: () -> List[str]

        '''A receivers data-references as a list of strings'''

        return self.workflowGraphRef().dataReferencesForNode(self.identification.identifier)

    @property
    def dataReferences(self):
        # type: () -> List[DataReference]

        '''A receivers data-references as a list of graph.DataReference objects'''

        #NOTE: Pass self.stageIndex to resolve all references in same stage
        return self.inputDataReferences + self.componentDataReferences

    @property
    def inputDataReferences(self):
        # type: () -> List[DataReference]

        '''A receivers input data-references as a list of graph.DataReference objects

        Requires that the edges have been added to the WorkflowGraph the receiver has access t

        Returns:
            List of DataReferences'''

        inputRefs = []
        for r in self.workflowGraphRef().dataReferencesForNode(self.identification.identifier):
            #Create a DataReference object assuming this is a component in graph i.e. has a namespace
            #Note: stageIndex parameter is only used if `r` has no namespace piece
            ref = DataReference(r, stageIndex=self.identification.stageIndex)
            if ref.isDirectReference(self.workflowGraphRef()):
                inputRefs.append(DataReference(r))

        return inputRefs

    @property
    def componentDataReferences(self):
        # type: () -> List[DataReference]

        '''A receivers component data-references as a list of graph.DataReference objects

         Requires that the edges have been added to the WorkflowGraph the receiver has access to'''

        inputRefs = []
        for r in self.workflowGraphRef().dataReferencesForNode(self.identification.identifier):
            # Create a DataReference object assuming this is a component in graph i.e. has a namespace
            ref = DataReference(r, stageIndex=self.identification.stageIndex)
            
            if ref.isDirectReference(self.workflowGraph) is False:
                #The ref was in the graph
                inputRefs.append(ref)

        return inputRefs

    @property
    def producers(self):
        # type: () -> Dict[DataReference, ComponentSpecification]

        '''Returns a dict of DataReference:ComponentSpecification pairs for the receivers producers'''

        specs = {}
        for ref in self.componentDataReferences:
            # VV: loopRef dataReferences may point to multiple components
            ret = ref.true_reference_to_component_id(self.workflowGraph)

            if ret is None:
                raise ValueError("Component reference %s of %s does not point to a Component" % (
                    ref.stringRepresentation, self.identification.identifier
                ))

            for stage_index, name in ret:

                producer_id = ComponentIdentifier(name=name, index=stage_index)
                try:
                    comp_node = self.workflowGraphRef().graph.nodes[producer_id.identifier]
                except KeyError:
                    raise_with_traceback(ValueError("There is no predecessor of %s named %s for reference %s. "
                                                    "Known nodes are %s" % (
                                                        self.identification.identifier,
                                                        ref.producerIdentifier.identifier, ref.stringRepresentation,
                                                        [name for name in self.workflowGraphRef().graph.nodes]
                                                    )))
                    raise

                if 'componentSpecification' in comp_node:
                    specs[ref] = comp_node['componentSpecification']
                else:
                    raise NotImplementedError("Do not know how to extract producer information from %s" % comp_node)

        return specs

    @property
    def workingDir(self):

        return None

    def setOption(self, key, value):

        '''Sets the options key to value

        Note: Affects the underlying configuration'''

        self.workflowGraphRef().setOptionForNode(self.identification.identifier, key, value)

    def removeOption(self, key):

        '''Remove the value associated with the option

        Note: In some cases this will cause the option to assume a default value (e.g. you can't delete job-type)
        if accessed via the ComponentSpecification API'''

        self.workflowGraphRef().removeOptionForNode(self.identification.identifier, key)

    def checkWorkflowOptions(self):
        '''Checks the values given for workflow specific options are consistent

        Exceptions:
            This method willl raise a ValueError
            if any issues are encountered
        '''

        d = self.workflowAttributes

    def checkDataReferences(self):
        '''Checks a jobs data dependencies

        For each component reference this method checks
        - that there are no copy or ref references to specific producer data in the same stage
            e.g. files in producer directories
        - that a source component does not consume from a repeating component in the same stage
        - that there are no unused references of type :ref in the reference list
            - All these references should be used in the command line
            - If not then possible errors are
               - Forgotten to include in CL
               - Mispelling in command line or reference
                    This will either be detected in next test or in the direct test
                    as the mispelt ref will be interpreted as a direct ref
               - Wrong Component referenced
               - Location is hardcoded and reference only include to connnect graph
        - that there are no unresolved references in the command line
             - Forgotten to include in references
             - Mispelling

        Exceptions:

        This method will raise DataReferenceFilesDoNotExistError if a producer cannot be found.

        Note: This method requires that the WorkflowGraph the node is part of has all edges added'''

        # VV: copy and copy-out dependencies are forbidden when source and destination components
        #     have a Subject->Observer dependency

        if self.workflowAttributes['isRepeat']:
            copy_methods = [DataReference.CopyOut, DataReference.Copy]

            for reference in self.componentDataReferences:
                if reference.stageIndex == self.identification.stageIndex and reference.method in copy_methods:
                    raise experiment.model.errors.DataReferenceInconsistencyError(
                        reference,
                        "You cannot %s a component directory in the same stage as a component" % reference.method
                    )

        #Check if we are in a cycle
        try:
            #If no cycle exception is raised
            edges = networkx.find_cycle(self.workflowGraphRef().graph, source=self.identification.identifier,
                                        orientation='original')
        except:
            pass
        else:
            #If we're here there was no exception which means there was a cycle - raise an exception
            graphLogger.critical("Cycle detected in graph: %s" % edges)
            outRef = edges[0][1]
            inRef = edges[-1][0]
            raise experiment.model.errors.CircularComponentReferenceError(DataReference(outRef), DataReference(inRef))

        unresolved = []
        unused = []

        _ = self.resolveArguments(unresolved=unresolved, unused=unused, ignoreErrors=True)

        if len(unresolved) >= 1:
            raise unresolved[0]

        if len(unused) >= 1:
            raise unused[0]

    def checkExecutorOptions(self):

        '''Checks the values given for executor specific options are consistent

        Exceptions:
            This method willl raise an ExecutorOptionsError
            if any issues are encountered
        '''

        #Require to trigger backend

        experiment.model.executors.CheckSpecification(self)

    def checkExecutable(self, ignoreTestExecutablesError=False, updateSpecification=True):

        '''Checks a components executable exists and can be executed.

        Resolves pathless executables using defined environment
        Resolves relative paths if the graph root-storage has been set

        NOTE: Modifies the underlying configuration if a change to the path occurs

        Parameters:
            ignoreTestExecutablesError(bool):  Instead of raising an Exception for a failed executable check it
              just prints a warning message on the console. This may be useful in scenarios where a backend is
              unavailable (e.g. kubernetes, LSF, etc) but there exist cached memoization candidates available which
              can be used instead of executing a task.
            updateSpecification(bool): When set to True, on a successful check will also update the configuration with
              the resolved executable. If the component is using the kubernetes backend the container image will also
              be updated to the fully resolved image-id that was first used for this particular image tag

        Exceptions:
            Raises ComponentExecutableCannotBeFoundError if the executable is not resolvable, cannot be found,
                or is not executable

            Note when the executable path is relative and there is no root storage a ValueError is raised
            (path is not resolvable), unless the path is relative to CWD at time of calling
        '''

        interpreter = self.commandDetails.get('interpreter', None)
        resourceManager = self.resourceManager

        if interpreter is not None:
            if resourceManager['config']['backend'] != "local":
                msg = 'Interpreter components (%s) can only use the "local" backend' % (
                    self.identification.identifier
                )
                graphLogger.warning(msg)
                raise ValueError(msg)

            if interpreter not in experiment.model.frontends.flowir.FlowIR.Interpreters:
                errors = []
                experiment.model.conf.suggest_alternative(
                    interpreter, experiment.model.frontends.flowir.FlowIR.Interpreters, 'command.interpreter',
                    self.identification.identifier, {}, errors
                )

                msg = errors[0]
                graphLogger.warning(msg)
                raise ValueError(msg)

        if resourceManager['config']['backend'] != 'migrated':
            try:
                #Note: Be aware self.command returns new object on each call
                command = self.command
                preCheck = command.executable
                command.updateAndCheckExecutable()
                postCheck = command.executable

                if updateSpecification and (preCheck != postCheck):
                    graphLogger.log(19, 'Updating executable of %s to %s' % (self.identification.identifier, postCheck))
                    self.setOption('#command.executable', postCheck)

                if updateSpecification and (self.resourceManager['config']['backend'] in ['kubernetes', 'docker']):
                    backend_type = self.resourceManager['config']['backend']
                    img_original = self.resourceManager[backend_type]['image']
                    img_snapshot = experiment.runtime.utilities.container_image_cache.cache_lookup(img_original)

                    if img_original != img_snapshot:
                        graphLogger.log(19, 'Updating %s image of %s to %s' % (
                            backend_type, self.identification.identifier, img_snapshot))
                        self.setOption(f'#resourceManager.{backend_type}.image', img_snapshot)
            except Exception as e:
                if ignoreTestExecutablesError:
                    msg = "Check executable found an issue with executable %s, component: %s, environment id: %s=%s -" \
                          " have been instructed to ignore errors, will not raise an exception" % (
                        self.command.executable, self.identification.identifier,
                        self.commandDetails.get('environment', '*NoneDefined*'), self.environment)
                    graphLogger.info(msg)
                else:
                    if resourceManager['config']['backend'] != 'simulator':
                        msg = "Check executable found an issue with executable %s, component: %s, " \
                              "environment id %s, $PATH=%s" % (self.command.executable, self.identification.identifier,
                            self.commandDetails.get('environment', '*NoneDefined*'),
                            self.environment.get('PATH', '*NoneDefined*'))

                        raise_with_traceback(experiment.model.errors.ComponentExecutableCannotBeFoundError(
                            desc=msg, underlyingError=e))
                    else:
                        graphLogger.log(19, "Check executable found an issue with %s but this is a simulated job" % e)

    def resolveArguments(self, unresolved=None, unused=None, ignoreErrors=False):

        '''Attempts to resolve the references in  a components argument string

        NB: Requires `componentInstance` data has been added to each node

        Parameters:
            unresolved (list|None): If a list, on return contains 0+ UndeclaredDataReferenceError objects
                Note: In practice this will be a single object containing info on all undeclared references
            unused (list|None): If a list, on return contains 0+ UnusedDataReferenceError objects.
                One for each reference of type 'ref' that is not used in the commandLine
            ignoreErrors: If True, the receiver ignores errors resolving references due to no concrete storage
                being set. Note unused will still be populated by such references

        Returns:
              The resolve command line string.
              If the receiver has no arguments this method returns the empty string

        '''

        # Resolve Arguments
        arguments = self.commandDetails.get('arguments')
        if arguments is None:
            return ""

        graphLogger.debug("Resolve arguments for %s, arguments \"\"\"%s\"\"\"" % (
            self.identification, arguments
        ))

        my_workflowAttributes = self.workflowAttributes

        errors = []

        for reference in self.dataReferences:
            graphLogger.debug("Reference: \"%s\"" % reference)
            graphLogger.debug('References:')
            try:
                try:
                    reference_value = reference.resolve(self.workflowGraphRef())

                except experiment.model.errors.DataReferenceFilesDoNotExistError as e:
                    if len(e.referenceErrors) != 1:
                        raise experiment.model.errors.InternalInconsistencyError(
                            "experiment.errors.DataReferenceFilesDoNotExistError() for %s should contain exactly "
                            "one entry" % reference.stringRepresentation
                        )
                    if reference.method in [reference.Output, reference.LoopOutput]:
                        graphLogger.log(14, "Ignoring missing reference %s "
                                            "(because its output and I assume that it will be generated)"
                                        % reference.absoluteReference)
                        reference_value=""
                    else:
                        ref, path = e.referenceErrors[0]

                        producer_index = ref.producerIdentifier.stageIndex

                        if producer_index == self.identification.stageIndex and my_workflowAttributes['isRepeat']:
                            graphLogger.warning("Pretending that reference %s is not missing on behalf of Observer %s" %(
                                ref.stringRepresentation, self.identification.identifier
                            ))
                            # VV: The data-reference failed to resolve, but it's fine because this is a
                            #     Subject->Observer dependency
                            if ref.method in [DataReference.Output, DataReference.LoopOutput]:
                                # VV: For :output references, just pretend that the stdout file is empty
                                reference_value = ""
                            elif ref.method in DataReference.pathMethods:
                                # VV: For pathMethods pretend that the file is there and resolve to its path
                                reference_value = path
                            else:
                                reference_value = None
                                raise experiment.model.errors.InternalInconsistencyError(
                                    "Do not know how to deal with missing DataReference %s"
                                    % reference.stringRepresentation
                                )
                        else:
                            # VV: This is a Producer->Consumer dependency! raise the exception
                            raise
            except experiment.model.errors.InternalInconsistencyError:
                if not ignoreErrors:
                    raise
                else:
                    reference_value = None

            graphLogger.debug("Reference value: %s" % reference_value)

            errors = []

            if reference.method in [DataReference.Output, DataReference.LoopOutput]:
                # VV: The reference value is in fact the CONTENTS of the file that the data-reference points to
                reference_value = reference_value or ""
                if arguments.find(reference.absoluteReference) != -1:
                    arguments = arguments.replace(reference.absoluteReference, reference_value)
                elif arguments.find(reference.relativeReference) != -1:
                    arguments = arguments.replace(reference.relativeReference, reference_value)
                else:
                    if unused is not None:
                        unused.append(experiment.model.errors.UnusedDataReferenceError(self.identification.identifier,
                                                                                       reference,
                                                "All declared references of type "
                                                "'%s' must be used in component command line ("
                                                "could find neither \"%s\" nor \"%s\" in \"%s\"" % (
                                                    reference.method,
                                                    reference.absoluteReference, reference.relativeReference,
                                                    arguments
                                         )))
                    message = 'Could not locate reference %s in arguments %s' % (
                        reference.absoluteReference, arguments
                    )
                    graphLogger.warning(message)
            elif reference_value is not None and reference.method in [DataReference.Ref, DataReference.LoopRef]:
                # VV: The reference_value is definitely a path because it's a "ref" type
                path = reference_value
                if arguments.find(reference.absoluteReference) == -1 and arguments.find(reference.relativeReference) == -1:
                    if unused is not None:
                        unused.append(experiment.model.errors.UnusedDataReferenceError(self.identification.identifier,
                                                                                       reference,
                                                "All declared references of type "
                                                "'%s' must be used in component command line("
                                                "could find neither \"%s\" nor \"%s\" in \"%s\"" % (
                                                    reference.method,
                                                    reference.absoluteReference, reference.relativeReference,
                                                    arguments
                                         )))
                else:
                    # Resolve the reference in the command line
                    if arguments.find(reference.absoluteReference) == -1:
                        arguments = arguments.replace(reference.relativeReference, path)
                    else:
                        arguments = arguments.replace(reference.absoluteReference, path)

        # Check for unresolved/undeclared references in CL - this is anything of form :ref :link

        supported_ref_methods = [DataReference.Ref, DataReference.Output,
                                 DataReference.LoopRef, DataReference.LoopOutput]

        full_args = re.compile('(-[a-zA-Z0-9_-]+)?=')

        for method in DataReference.methods:
            if arguments.find(":%s" % method) != -1:
                position = arguments.find(":%s" % method)
                comp = arguments[:position].split()[-1]

                match = re.search(full_args, comp)

                if match is not None:
                    comp = comp[match.end():]

                if method in supported_ref_methods:
                    t = (method, position, comp, arguments)
                    message = (
                            'Possible unresolved reference of type "%s" at position '
                            '%d in resolved argument string: "%s" arguments "%s"' % t
                    )
                    graphLogger.warning(message)
                    errors.append(message)
                else:
                    t = (method, position, comp, arguments, supported_ref_methods)
                    message = (
                            'Unsupported reference of type "%s" at position '
                            '%d in resolved argument string: "%s" arguments "%s". Supported methods are %s' % t
                    )
                    graphLogger.warning(message)
                    errors.append(message)

        if len(errors) > 0 and unresolved is not None:
            unresolved.append(
                experiment.model.errors.UndeclaredDataReferenceError(self.identification.identifier, errors))

        # VV: Fill-in to expand any `<file:ref>[<index>]` string interpolations
        cls_flowir = experiment.model.frontends.flowir.FlowIR

        if not self.workflowGraph.configuration.is_raw:
            arguments = cls_flowir.fill_in(arguments, self.customAttributes, self.configuration)

        return arguments


def generate_workflow_access_api(wg, cid, isPrimitive):
    # type: (WorkflowGraph, ComponentIdentifier, bool) -> Dict[str, Callable[[Optional[Any]], Any]]
    # ComponentSpecification instances only hold weakrefs to the graph
    # NOTE: In case reference is missing the stageIndex we add it here
    specification = ComponentSpecification(cid, wg)

    ret = dict(
        setOption=lambda key, value: wg.setOptionForNode(cid.identifier, key, value),
        getConfiguration=lambda x: wg.configurationForNode(cid.identifier, raw=x, is_primitive=isPrimitive),
        getEnvironment=lambda: wg.environmentForNode(cid.identifier),
        getInputReferences=lambda: wg.inputReferencesForNode(cid.identifier),
        getVariables=lambda: wg.variablesForNode(cid.identifier),
        getReferences=lambda: wg.resolvedReferencesForNode(cid.identifier),
        isPrimitive=isPrimitive,
        componentSpecification=specification,
        references=specification.rawDataReferences,
    )

    return ret


def CreateNode(wg, # type: WorkflowGraph
               g,  # type: networkx.DiGraph
                reference,  # type: str
                index,  # type: int
                isReplicationPoint, # type: bool
                isBlueprint,  # type: bool
                isAggregate,  # type: bool
                isPrimitive,  # type: bool
               ):
    # type: (...) -> ComponentSpecification
    #Make sure the id of the node is a fully resolved ComponentIdentifier
    cid = ComponentIdentifier(reference, index)
    #Function so the value of reference is preserved

    api = generate_workflow_access_api(wg, cid, isPrimitive)

    g.add_node(cid.identifier,
               stageIndex=index,
               isReplicationPoint=isReplicationPoint,
               isBlueprint=isBlueprint,
               isAggregate=isAggregate,
               level=index,
               rank=index,
               **api
               )

    return api['componentSpecification']


class WorkflowGraph(object):
    '''Object which represent a workflow graph.

    Instance of this class enhance a networkx.DiGraph with Flow IR features.

    This includes ability to query a nodes
    - variables
    - producers/consumers with flow reference syntax (both component and data)
    - environments
    - platforms

    To run Flow IR related validation on
    - keywords
    - references
    - variables
    - graph structure

    And to perform replication operations.

    If it's rootStorage property is set (does not automatically happen in __init__()) it can also resolve paths
    '''

    inherit_attributes = ['component']

    def __init__(self,  # type: WorkflowGraph
                 configuration,  # type: FlowIRExperimentConfiguration
                 platform=None,  # type: Optional[str]
                 primitive=True,  # type: bool
                 inherit_graph=None,  # type: Optional[WorkflowGraph]
                 ):
        self.log = logging.getLogger('graph.workflowgraph')
        self._storage: Optional[experiment.model.storage.StorageStructurePathResolver] = None
        self.isPrimitive = primitive
        self._graph = None  # type: networkx.DiGraph
        self.platform = platform
        self._flowir_configuration = configuration  # type: FlowIRExperimentConfiguration

        # VV: @tag:PlaceholderMetadata
        # VV: Maps placeholder meta-nodes to references of the respective instantiated looped components
        # VV: Contains metadata for placeholders:
        # {
        #    'represents': List[str],  # references of all instantiated looped components
        #    'state': str, experiments.codes.(FINISHED_STATE|RUNNING_STATE|FAILED_STATE|SHUTDOWN_STATE)
        #    'DoWhileId': str, # name of DoWhile document stage<%d>.<str> (ref of component which instantiated it)
        #    'latest': str, # reference of most recent instance (i.e. ref from `represents` with highs=est iter)
        # }
        self._placeholders = {}  # type: Dict[str, Dict[str, Union[str, List[str]]]]

        documents = experiment.model.frontends.flowir.deep_copy(self._flowir_configuration._documents)

        # VV: Collections of documents: outer key is the type of the collection (e.g. DoWhile), next to outer key
        #     is the id of the document. Each document always contains the `document` key which holds the definition
        #     of the document. Some keys have a `state` key (e.g. DoWhile ones)
        self._documents = {}  # type: Dict[str, Dict[str, Dict[str, Dict[str, Any]]]]

        for lbl in documents:
            if lbl not in self._documents:
                self._documents[lbl] = {}
            for doc_name in documents[lbl]:
                self._documents[lbl][doc_name] = {
                    'document': documents[lbl][doc_name]
                }

        if self.isPrimitive:
            self._graph = self._createPrimitiveGraph(inherit_graph)
        else:
            self._graph = self._createCompleteGraph(inherit_graph)

    def to_dsl(self):
        """Returns the entire DSL of the Workflowgrap (entrypoint, workflows, and components)

        This is a WIP method and subject to changes.
        """
        graph = self.graph

        """
        entrypoint: # describes how to run this YAML file
              entry-instance: the-identifier-of-a-class
              execute: # the same as workflow.execute with the difference that there is always 1 step
                # and the name of the step is always <entry-instance>
                - target: "<entry-instance>"
                  args: # Instantiate the class
                    parameter-name: parameter value # see notes
        """
        configuration = self.configuration
        if isinstance(configuration, experiment.model.conf.DSLExperimentConfiguration):
            return configuration.dsl_namespace.dict(by_alias=True)

        platform_vars = self._concrete.get_platform_variables()[experiment.model.frontends.flowir.FlowIR.LabelGlobal]
        workflow = self.dsl_workflow_blueprint()

        main_args = OrderedDict()

        for name in sorted(platform_vars):
            main_args[name] = platform_vars[name]

        top_level_folders = self.configuration.top_level_folders
        app_deps = self.configuration.get_application_dependencies()

        # VV: References that do not point to components are parameters of workflow
        # Those that are pointing to `input` files should also be part of entrypoint.execute.args
        # Those that point to app-deps/data, etc should not be part of entrypoint but should have a default value
        #   (the reference string)
        known_param_refs = set()

        # VV: Must visit references in the correct order
        for name in sorted(graph.nodes):
            spec: ComponentSpecification = graph.nodes[name]['componentSpecification']
            for ref in sorted(spec.rawDataReferences):

                if ref in known_param_refs:
                    continue

                wf_param = _dsl_parameter_name_from_external_reference(ref)

                stageIndex, jobName, filename, method = experiment.model.frontends.flowir.FlowIR.ParseDataReferenceFull(
                    ref, index=spec.identification.stageIndex, special_folders=top_level_folders,
                    application_dependencies=app_deps)

                if stageIndex is not None:
                    # VV: This is a reference that points to a component, skip it
                    continue

                main_args[wf_param.name] = wf_param.entrypoint

                known_param_refs.add(ref)

        entrypoint = OrderedDict(( ('entry-instance', 'main'), ))
        entrypoint['execute'] = [
                {
                    "target": "<entry-instance>",
                    "args": main_args
                }
            ]

        dsl = OrderedDict()

        dsl['entrypoint'] = entrypoint
        dsl['workflows'] = [workflow]
        dsl['components'] = [
               cast(ComponentSpecification, graph.nodes[name]['componentSpecification']).dsl_component_blueprint()
                    for name in networkx.topological_sort(graph)
            ]

        return dsl

    def dsl_workflow_blueprint(self):
        """Returns the workflow blueprint in the DSL of st4sd.

        This is a WIP method and subject to changes.
        """
        platform_vars = self._concrete.get_platform_variables()[experiment.model.frontends.flowir.FlowIR.LabelGlobal]

        graph = self.graph

        top_level_folders = self.configuration.top_level_folders
        app_deps = self.configuration.get_application_dependencies()

        # VV: References that do not point to components are parameters of workflow
        # Those that are pointing to `input` files should also be part of entrypoint.execute.args
        # Those that point to app-deps/data, etc should not be part of entrypoint but should have a default value
        #   (the reference string)
        known_param_refs = set()

        params_no_default = []
        params_with_default = []
        # VV: Must visit references in the correct order
        for name in sorted(graph.nodes):
            spec: ComponentSpecification = graph.nodes[name]['componentSpecification']
            for ref in sorted(spec.rawDataReferences):
                wf_param = _dsl_parameter_name_from_external_reference(ref)

                if wf_param.name in known_param_refs:
                    continue

                stageIndex, jobName, filename, method = experiment.model.frontends.flowir.FlowIR.ParseDataReferenceFull(
                    ref, index=spec.identification.stageIndex, special_folders=top_level_folders,
                    application_dependencies=app_deps)

                if stageIndex is not None:
                    # VV: This is a reference that points to a component, skip it
                    continue

                if jobName.split("/")[0] != "input":
                    params_with_default.append({'name': wf_param.name, 'default': wf_param.entrypoint})
                else:
                    params_no_default.append({'name': wf_param.name})

                known_param_refs.add(wf_param.name)

        # VV: The order of parameters is:
        # 1. parameters with no default values (e.g. input files)
        # 2. parameters with default values that are references (e.g. application-dependencies)
        # 3. parameters with default values that are variables

        parameters = []
        parameters.extend(sorted(params_no_default, key=lambda x: x['name']))
        parameters.extend(sorted(params_with_default, key=lambda x: x['name']))

        # VV: Variables which manifest as parameters with default values should be at the bottom of the parameter list
        # VV: The arguments to the workflow are platform variables
        # TODO we need a way to handle stage variables - they are basically different variables which
        # happen to occupy the same name as the global platform variables but only for a subset of components
        # those that belong in that certain stage index
        parameters.extend([{'name': name, "default": platform_vars[name]} for name in sorted(platform_vars)])

        signature = OrderedDict((('name', "main"), ('parameters', parameters)))
        workflow = OrderedDict((('signature', signature),))

        workflow['steps'] = OrderedDict( ((name, name) for name in networkx.topological_sort(graph)) )
        workflow['execute'] = [
            {
                'target': f"<{name}>",
                'args': cast(ComponentSpecification, graph.nodes[name]['componentSpecification']).get_dsl_args()
            } for name in networkx.topological_sort(graph)
        ]

        return workflow

    @classmethod
    def graphFromExperimentInstanceDirectory(
            cls,
            experimentInstanceDir,  # type: ExperimentInstanceDirectory
            primitive=True,  # type: bool
            variable_substitute=True,  # type: bool
            systemvars=None,  # type: Optional[Dict[str, str]]
            configPatches=None,  # type: Optional[Dict[int, List[str]]]
            platform=None,
            is_instance=True,
            createInstanceConfiguration=True,  # type: bool
            updateInstanceConfiguration=False,  # type: bool
            **kwargs):
        """
        Generate a graph from an experiment.model.storage.ExperimentInstanceDirectory

        Args:
            experimentInstanceDir: An ExperimentInstanceDirectory
            platform(str): (optional) name of package platform to use, default value maps to "default"
            format_priority(Optional[List[str]]): Indicates priorities of formats
                (setting this parameter to None will prioritize DOSINI over flowir i.e. ['dosini', 'flowir'])
            primitive(bool): If True will generate a primitive graph, if False will generate a Replicated one
            variable_substitute(bool): Whether to perform variable substitution, optional for a primitive graph
                but required for a replicated one
            systemvars(Dict[str, str]): A dictionary of environment variables.
                    These variables are added to every environment - even environments with no vars/name
            configPatches(Dict[int, List[str]]): A dictionary whose keys are stage indexes and whose values
                are a list of configuration files. These configuration files will be layered on-top of the default
                configuration file for stage index.
            is_instance(bool): when True, will load the stored instance files, when False
                will load the package configuration
            createInstanceConfiguration: If set to True will auto-generate instance files provided that they do not
                already exist. Set updateInstanceFiles to True too to update existing files.
            updateInstanceConfiguration: Set to true to update instance files if they already exist
        """
        # VV: Discover user variables when not loading an instance
        variable_files = []
        potential_files = ['variables.conf', 'variables.yaml', 'variables.yml']
        for name in potential_files:
            try:
                user_variables = os.path.join(experimentInstanceDir.inputDir, name)
            except AttributeError:
                graphLogger.log(19, "WorkflowGraph will skip reading user variables "
                                    "because it's loading a PackageDirectory")
            else:
                if os.path.exists(user_variables):
                    variable_files.append(user_variables)
                graphLogger.log(19,
                                "WorkflowGraph attempted to discover user variables, found: %s" % variable_files)

        # VV: What if a user loaded multiple files? We *can* just override the information in the files iteratively
        # and generate 1 user-variables dictionary (in fact this is exactly what experimentConfigurationFactory
        # is doing, but that could lead to weird scenarios where users are not 100% sure of which variables are
        # actually used). For the sake of simplicity, and clarity, forbid loading more than 1 user-variables files
        if len(variable_files) > 1:
            raise experiment.model.errors.ExperimentTooManyVariableFilesError(experimentInstanceDir, variable_files)

        flowir_configuration = experiment.model.conf.ExperimentConfigurationFactory.configurationForExperiment(
            experimentInstanceDir.location,
            systemvars=systemvars,
            configPatches=configPatches,
            createInstanceFiles=createInstanceConfiguration,
            updateInstanceFiles=updateInstanceConfiguration,
            primitive=primitive,
            variable_files=variable_files,
            platform=platform,
            is_instance=is_instance,
            manifest=experimentInstanceDir.manifestData,
            variable_substitute=variable_substitute,
            **kwargs
        )

        workflowGraph = cls(configuration=flowir_configuration,
                            platform=flowir_configuration.platform_name,
                            primitive=primitive)

        workflowGraph.rootStorage = experimentInstanceDir

        return workflowGraph

    @classmethod
    def graphFromFlowIR(
            cls,
            flowir: experiment.model.frontends.flowir.DictFlowIR,
            manifest: experiment.model.frontends.flowir.DictManifest,
            documents: Optional[experiment.model.frontends.flowir.DictManyDocuments] = None,
            platform: Optional[str] = None,
            primitive: bool = True,
            variable_substitute: bool = True,
            **kwargs
    ) -> "WorkflowGraph":
        """
        Generate a graph from FlowIR and Manifest dictionaries.

        Resulting WorkflowGraph does not have a rootStorage therefore cannot resolve path to files.

        Args:
            flowir: A Dictionary containing FlowIR
            manifest: The manifest is a dictionary, with targetFolder: sourceFolder entries. Each
                sourceFolder will be copied or linked to populate the respective targetFolder. Source folders can be
                absolute paths, or paths relative to the path of the FlowIR YAML file. SourceFolders may also be
                suffixed with :copy or :link to control whether targetFolder will be copied or linked to sourceFolder
                (default is copy). TargetFolders are interpreted as relative paths under the instance directory. They
                may also be the resulting folder name of some applicationDependency. They may include a path-separator
                (.e.g /) but must not be absolute paths.
            platform: Name of FlowIR platform to use
            primitive(bool): If True will generate a primitive graph, if False will generate a Replicated one
            variable_substitute(bool): Whether to perform variable substitution, optional for a primitive graph
                but required for a replicated one
        """
        concrete = experiment.model.frontends.flowir.FlowIRConcrete(flowir, platform, documents)

        exp_conf = experiment.model.conf.FlowIRExperimentConfiguration(
            concrete=concrete, path=None, is_instance=False, primitive=primitive, manifest=manifest,
            createInstanceFiles=False, updateInstanceFiles=False, variable_substitute=variable_substitute,
            platform=platform,
            # VV: Default values for rest of parameters
            variable_files=None, system_vars=None, config_patches=None
        )

        workflowGraph = cls(configuration=exp_conf, platform=platform or exp_conf.platform_name, primitive=primitive)
        return workflowGraph

    @classmethod
    def graphFromPackage(cls,
                         experimentPackage,  # type: ExperimentPackage
                         platform=None,  # type: Optional[str]
                         primitive=True,  # type: bool
                         variable_substitute=True,  # type: bool
                         systemvars=None,  # type: Optional[Dict[str, str]]
                         createInstanceConfiguration=True,  # type: bool
                         updateInstanceConfiguration=False,  # type: bool
                         is_instance: bool = False,
                         variable_files: Optional[List[str]] = None,
                         configPatches=None,  # type: Optional[Dict[int, List[str]]]
                         validate: bool = True,
                         **kwargs):
        """
        Generate a graph from an ExperimentPackage.

        This method re-uses the FlowIRExperimentConfiguration of the experimentPackage but it also parametrizes it

        Args:
            experimentPackage: A ExperimentPackage or ExperimentInstanceDirectory
            platform(str): (optional) name of package platform to use, default value maps to "default"
            primitive(bool): If True will generate a primitive graph, if False will generate a Replicated one
            variable_substitute(bool): Whether to perform variable substitution, optional for a primitive graph
                but required for a replicated one
            systemvars(Dict[str, str]): A dictionary of environment variables.
                    These variables are added to every environment - even environments with no vars/name
            createInstanceConfiguration: If set to True will auto-generate instance files provided that they do not
                already exist. Set updateInstanceFiles to True too to update existing files.
            updateInstanceConfiguration: Set to true to update instance files if they already exist
            variable_files: List of paths to variable files. If multiple files are provided then they are layered
                starting from the first and working towards the last. This means that the value of a variable that
                 exists in multiple layers will be the one that the last layer defines.
            configPatches: A dictionary whose keys are stage indexes and whose values
                are a list of configuration files. These configuration files will be layered on-top of the default
                configuration file for stage index (deprecated).
            validate: Whether to fully validate FlowIR - gets forced to True when building a Replicated graph

        """
        flowir_configuration = experimentPackage.flow_ir_conf

        if primitive is False and validate is False:
            graphLogger.warning("Forcing validate to True in order to build a "
                                "non-primitive (i.e. replicated) WorkflowGraph")
            validate = True

        if primitive is False and variable_substitute is False:
            graphLogger.warning("Forcing variable_substitute to True in order to build a "
                                "non-primitive (i.e. replicated) WorkflowGraph")
            variable_substitute = True


        flowir_configuration.parametrize(
            platform=platform,
            systemvars=systemvars,
            config_patches=configPatches,
            createInstanceFiles=createInstanceConfiguration,
            updateInstanceFiles=updateInstanceConfiguration,
            primitive=primitive,
            variable_files=variable_files,
            is_instance=is_instance,
            manifest=experimentPackage.manifestData,
            variable_substitute=variable_substitute,
            validate=validate,
            **kwargs
        )

        workflowGraph = cls(configuration=flowir_configuration,
                            platform=flowir_configuration.platform_name,
                            primitive=primitive)

        workflowGraph.rootStorage = experimentPackage

        return workflowGraph

    def active_backends(self):
        backends = [data['componentSpecification'].resourceManager['config']['backend'] for node, data in
                    self.graph.nodes(data=True)]

        backends = [x for x in backends if x is not None]
        backends = list(set(backends))

        return backends

    @property
    def _concrete(self):
        return self.configuration.get_flowir_concrete(return_copy=False)

    @property
    def configuration(self) -> FlowIRExperimentConfiguration:
        return self._flowir_configuration

    def get_stage_description(self, stage_index):
        #  type: (int) -> Tuple[List[str], Dict[str, str]]
        """ Returns a) Component names, and b) Stage Options.
        """
        return self._concrete.get_stage_description(stage_index)

    @property
    def numberStageConfigurations(self):
        return self._concrete.numberStageConfigurations

    @property
    def rootStorage(self):
        # type: () -> experiment.model.storage.StorageStructurePathResolver

        '''Returns an object that can be used to resolve relative paths

        If no such object has been explicitly set then this method returns None'''

        return self._storage

    @rootStorage.setter
    def rootStorage(self, storage  # type: experiment.model.storage.StorageStructurePathResolver
                    ):
        '''

        Args:
            storage (StorageStructurePathResolution):
                Object that can be used to resolve relative paths.

        Returns:
            None

        '''

        self._storage = storage

    def _createPrimitiveGraph(
            self,  # type: WorkflowGraph
            inherit_graph=None,  # type: Optional[WorkflowGraph]
            include_default=True  # type: bool
    ):
        # type: (...) -> networkx.DiGraph
        """Returns a networkx.DiGraph object of the experiments primitive graph

        If an `inherit_graph` is provided the resulting networkx graph will inherit the following node properties:
        - component (see class inherit_attributes)
        """

        # Create the job objects for each job configuration in the stage that's NOT a blueprint
        # All sections not default OR called META are assumed to be jobs

        # VV: @tag:FlowIR:Components
        g = networkx.DiGraph()
        edges = []
        # VV: @tag:FlowIR:Components

        component_ids = self._concrete.get_component_identifiers(False)

        for comp_id in component_ids:
            comp = self._concrete.get_component_configuration(
                comp_id, include_default=include_default, is_primitive=True, raw=True
            )
            stage_index = comp['stage']
            name = comp['name']
            is_aggregate = comp['workflowAttributes']['aggregate']
            is_replicate = comp['workflowAttributes']['replicate'] not in [None, 0]

            node_spec = CreateNode(
                self,
                g,
                name,
                stage_index,
                is_replicate,
                False,
                is_aggregate,
                isPrimitive=True,
            )

            modifiedRefs = []
            # NOTE: Cannot use dataReferences until graph is complete
            # as it relies on the complete graph to correctly identify component and non-component references
            # and at this stage the graph is not complete
            for ref in node_spec.rawDataReferences:
                # Create a DataReference instance for this ref assuming its a component ref - filter it later
                pid = DataReference(ref, stageIndex=stage_index).producerIdentifier
                if pid not in modifiedRefs:
                    modifiedRefs.append(pid)

            edges.append((node_spec.identification, modifiedRefs))

            producer_ids = []
            for ref in node_spec.rawDataReferences:
                # See comment _createCompleteGraph for why we have to use rawDataReferences
                dataRef = DataReference(ref, stageIndex=stage_index)
                try:
                    componentName, replica = experiment.model.conf.SplitComponentName(dataRef.producerName)
                except ValueError:
                    componentName = dataRef.producerName

                # Create a new ref without the replica index - note this is of type ComponentIdentifier
                # whereas original is of type DataReference
                # We can change type as we only need ComponentIdentifier information when creating edges below
                pid = ComponentIdentifier(componentName, dataRef.stageIndex)
                if pid not in producer_ids:
                    producer_ids.append(pid)

            edges.append((node_spec.identification, producer_ids))

            cid = node_spec.identification.identifier
            if inherit_graph and inherit_graph.graph.has_node(cid):
                inherit_node = inherit_graph.graph.nodes[cid]
                for attr in self.inherit_attributes:
                    if attr in inherit_node:
                        g.nodes[cid][attr] = inherit_node[attr]

        # A list of tuples. Each tuple is a (ComponentIdentifier, [list of ComponentIdentifier] pairs)
        for node, producers in edges:
            for pid in producers:
                if g.has_node(pid.identifier):
                    g.add_edge(pid.identifier, node.identifier, length=(node.stageIndex - pid.stageIndex + 1))

        return g

    def get_document_metadata(self, doc_type, doc_name):
        return self._documents[doc_type][doc_name]

    def compute_dowhile_state(self, dw_name, all_looped_ids):
        graphLogger.info("Will update state for DoWhile %s" % dw_name)
        graphLogger.log(14, "All looped ids: %s" % pprint.pformat(all_looped_ids))
        FlowIR = experiment.model.frontends.flowir.FlowIR
        dw = self._documents[FlowIR.LabelDoWhile][dw_name]
        document = dw['document']

        # VV: Discover current iteration
        ref_condition = document['condition']
        import_in_stage = document['stage']

        cond_stage, cond_name, c_file, c_method = FlowIR.ParseDataReferenceFull(ref_condition, import_in_stage)
        self.log.info("Condition \"reference\" %s is produced by %s" % (
            ref_condition, (cond_stage, cond_name)
        ))

        condition_instances = sorted(
            [c for c in all_looped_ids if c[1].split('#', 1)[1] == cond_name],
            # VV: Sort on iteration number from stage<idx:%d>.<iteration-no:%d>#<name:str>
            key=lambda c: int(c[1].split('#', 1)[0]),
            reverse=True
        )

        latest = condition_instances[0]

        graphLogger.log(14, "All looped conditions: %s" % condition_instances)

        stage_index, producer = latest
        latest_cond_id = (int(stage_index), producer)

        current_condition = FlowIR.compile_reference(latest_cond_id[1], c_file, c_method, latest_cond_id[0])

        # VV: Format is stage<idx:%d>.<iteration-no:%d>#<component-name:%s>
        current_iteration_no = int(latest_cond_id[1].split('#', 1)[0])
        dw['state'] = {
            'currentCondition': current_condition,
            'currentIteration': current_iteration_no,
        }

        graphLogger.info("Latest state of DoWhile %s is %s" % (dw_name, pprint.pformat(dw['state'])))

    def _get_all_looped_ids(self, use_cached_ids=False):
        # type: (bool) -> List[FlowIRComponentId]
        # VV: Discover all looped components
        all_looped_ids = []

        all_ids = self._concrete.get_component_identifiers(not use_cached_ids)

        def is_component_looped(comp_id):
            # type: (FlowIRComponentId) -> bool
            stage_idx, name = comp_id
            return '#' in name

        return list(filter(is_component_looped, all_ids))

    def update_dowhile_states(self):
        FlowIR = experiment.model.frontends.flowir.FlowIR
        docs = self._documents.get(FlowIR.LabelDoWhile, {})

        if not docs:
            return

        all_looped_ids = self._get_all_looped_ids()

        graphLogger.log(14, "All looped ids:\n%s" % pprint.pformat(all_looped_ids))

        for dw_name in docs:
            self.compute_dowhile_state(dw_name, all_looped_ids)

    def _discover_dowhile_placeholders(
            self,  # type: WorkflowGraph
            dw_name,  # type: str
            platform_variables,  # type: Dict[str, Any]
            remaining_looped_ids,  # type: Set[FlowIRComponentId]
    ):
        # type: (...) -> Dict[str, Any]
        FlowIR = experiment.model.frontends.flowir.FlowIR
        docs = self._documents[FlowIR.LabelDoWhile]
        doc = docs[dw_name]['document']

        top_level_folders = self.configuration.top_level_folders

        rep_placeholders = FlowIR.apply_replicate(
            doc['components'], platform_variables, ignore_missing_references=True,
            application_dependencies=self.configuration.get_application_dependencies(),
            top_level_folders=top_level_folders)

        import_stage = doc['stage']  # type: int

        # VV: Project names of component-placeholders to import_stage
        placeholder_ids = [(comp['stage'] + import_stage, comp['name']) for comp in rep_placeholders]
        do_while_placeholders = {}

        def looped_id_match_placeholder_id(looped_id, placeholder_id):
            # type: (FlowIRComponentId, FlowIRComponentId) -> bool
            # VV: The blueprint-id of the looped component is the component id but without the iteration number
            #     which is encoded as prefixing the component name with `<iteration-number:%d>#`
            bp_name = looped_id[1].split('#', 1)[1]

            blueprint_of_loop = (looped_id[0], bp_name)
            return blueprint_of_loop == placeholder_id

        for p_id in placeholder_ids:
            p_ref = 'stage%d.%s' % p_id
            if p_ref in do_while_placeholders:
                old_doc_name = do_while_placeholders[p_ref]['DoWhileId']
                new_doc_name = 'stage%d.%s' % (doc['stage'], doc['name'])
                raise ValueError("Placeholder \"%s\" is contained in at least 2 DoWhile documents: "
                                 "\"%s\" and \"%s\"" % (p_ref, old_doc_name, new_doc_name))

            # VV: Finished/Shutdown/Failed placeholders do not need to be updated, as they're already done
            last_state = self._placeholders.get(p_ref, {}).get('state', experiment.model.codes.RUNNING_STATE)

            if last_state != experiment.model.codes.RUNNING_STATE:
                self.log.info("Will skip updating the state of placeholder %s because it's %s" % (
                    p_ref, last_state
                ))
                continue
            else:
                self.log.info("Updating state of placeholder %s" % p_ref)

            # VV: Discover matching components and remove them from list of looped-ids, because a comp-id can
            #     only be matched by one placeholder-id
            matched_components = set([comp_id for comp_id in remaining_looped_ids if looped_id_match_placeholder_id(comp_id, p_id)])
            remaining_looped_ids.difference_update(matched_components)
            matched_refs = ['stage%d.%s' % c_id for c_id in matched_components]

            latest = sorted(
                matched_components,
                # VV: Sort on iteration number from stage<idx:%d>.<iteration-no:%d>#<name:str>
                key=lambda c: c[1].split('#', 1)[0],
                reverse=True
            )[0]

            # VV: @tag:PlaceholderMetadata
            do_while_placeholders[p_ref] = {
                'DoWhileId': dw_name,
                'represents': matched_refs,
                'latest': 'stage%d.%s' % latest,
                'state': experiment.model.codes.RUNNING_STATE,
                'stage': import_stage
            }

            graphLogger.log(14, "Matched looped components %s with placeholder %s" % (
                matched_refs, p_ref
            ))

        return do_while_placeholders

    def map_placeholders_to_looped_instances_of_components(self):

        FlowIR = experiment.model.frontends.flowir.FlowIR
        docs = self._documents.get(FlowIR.LabelDoWhile, {})

        if not docs:
            return

        graphLogger.info("Will update information for DoWhile placeholders")

        remaining_looped_ids = set(self._get_all_looped_ids())

        # VV: Maps placeholder-names to {
        #   'document': <DoWhile-document>,
        #   'represents': List[str](references of all instantiated matching looped components),
        # }
        do_while_placeholders = {}  # type: Dict[str, Dict[str, Any]]
        platform_variables = self._concrete.get_platform_variables()

        for dw_name in docs:
            these_placeholders = self._discover_dowhile_placeholders(dw_name, platform_variables, remaining_looped_ids)
            do_while_placeholders.update(these_placeholders)

        if remaining_looped_ids:
            msg = "Failed to match some looped component ids to any placeholder of the DoWhile documents. Offending " \
                  "ids are: %s. Matched placeholders are %s" % (pprint.pformat(remaining_looped_ids),
                                                                pprint.pformat(list(do_while_placeholders.keys())))
            graphLogger.critical(msg)
            raise ValueError(msg)

        self._placeholders.update(do_while_placeholders)

        graphLogger.log(14, "New Placeholder entries:%s" % pprint.pformat({
                key: self._placeholders[key]['represents'] for key in self._placeholders }))

    def instantiate_dowhile_next_iteration(self, do_while, next_iter_number, store_flowir_to_disk):
        # type: (Dict[str, Any], int, bool) -> List[str]
        """Instantiates components for the next iteration of a DoWhile and updates concrete and graph, returns
           names (stage<stage-idx:%d>.<new-iteration-no:%d>#<component-name:%s>[<replica-id:%d>]) of new components.

        VV: Algorithm

        - Fill inputBindings with the values off of loopBindings.
        - Create new components for `iteration_no=current_iteration_no+1`.
        - Create a replicated graph
        - Compare with `graph` to discover the components of the new iteration (this will find replicated components)
        - Add the new nodes to `concrete`
        - Add the new nodes to `graph`
        - Inspect placeholder nodes in `graph` and remove dependencies to just finished iteration
        """
        # VV: Important to work on a copy of do_while to guarantee that future iterations are not affected
        do_while = experiment.model.frontends.flowir.deep_copy(do_while)
        FlowIR = experiment.model.frontends.flowir.FlowIR
        expand_bindings = experiment.model.frontends.flowir.expand_bindings

        curr_iter_number = next_iter_number - 1
        if curr_iter_number < 0:
            raise ValueError("DoWhile iterations can not be negative (%d)" % curr_iter_number)

        # VV: Use the DoWhile template to generate a list of primitive components
        stage_idx = do_while['stage']
        dw_comp_instantiate_name = do_while['name']

        dw_name = 'stage%s.%s' % (stage_idx, dw_comp_instantiate_name)
        dw_label = 'stage%d.%s#%d' % (stage_idx, dw_comp_instantiate_name, next_iter_number)

        self.log.info("Instantiating DoWhile %s" % dw_label)

        # VV: Expand bindings to correctly handle references to latest loop-instances of components
        bindings = do_while.get('bindings', {})
        bindings = expand_bindings(bindings, stage_idx)
        do_while = {key: do_while[key] for key in do_while if key != 'bindings'}

        foreign_components = self._concrete.get_component_identifiers(True, False)

        dw_components, _ = experiment.model.frontends.flowir.instantiate_dowhile(
            do_while, bindings, stage_idx, dw_comp_instantiate_name, foreign_components, label=dw_label,
            iteration_no=next_iter_number)

        # VV: Update FlowIRConcrete with new components
        add_errors = []

        unreplicated = self.configuration._unreplicated
        for comp in dw_components:
            try:
                # VV: Ignore conversion errors because we're adding incomplete FlowIR definitions, in a later step
                #     we'll validate everything after we replicate the FlowIR configuration. Do keep track of any other
                #     errors and bail if something goes wrong
                FlowIR.convert_component_types(comp, True, is_primitive=True)
                unreplicated.add_component(comp)
                self.log.log(14, "Adding component stage%d.%s: %s" % (
                    comp['stage'], comp['name'], pprint.pformat(comp)
                ))
            except Exception as e:
                add_errors.append(e)

        # VV: Need to replicate new iteration
        self.configuration._unreplicated = unreplicated
        self.configuration.replicate()

        if add_errors:
            raise ValueError("Errors creating new components for DoWhile#%d: %s" % (
                next_iter_number, '\n'.join(map(str, add_errors))))

        # VV: Create new replicated graph, should contain *all* iterations up to (and including) `next_iter_number`
        new_graph = self._createCompleteGraph(inherit_graph=self)

        # VV: Discover component nodes that differ between the 2 graphs and then make sure that they're
        #     looped-components

        updated_comp_names = []
        new_comp_names = []

        # VV: Copy the nodes from the new graph to the old one and update their API so that it points to the old graph
        # (i.e. setOption, getConfiguration, etc).
        # Old nodes will see their predecessors updated if their FlowIR definition contains references to placeholders
        old_nodes = set(self.graph.nodes)
        try:
            for name in networkx.topological_sort(new_graph):
                if name not in updated_comp_names and self.graph.has_node(name) is False:
                    new_comp_names.append(name)
                    updated_comp_names.append(name)

                data = new_graph.nodes[name]
                cid = data['componentSpecification'].identification  # type: ComponentIdentifier
                data.update(generate_workflow_access_api(self, cid, data['isPrimitive']))

                predecessors = new_graph.predecessors(name)

                if name not in old_nodes:
                    self.log.info("Add node %s when handling new iteration %s" % (name, dw_label))
                    self.graph.add_node(name, **data)
                else:
                    self.log.log(13, "Will update node %s when handling new iteration %s with predecessors %s" % (
                        name, dw_label, predecessors))

                for p in predecessors:
                    self.graph.add_edge(p, name)
        except networkx.NetworkXUnfeasible as e:
            try:
                cycle = networkx.find_cycle(new_graph)
            except Exception:
                pass
            else:
                graphLogger.critical("Cycle in graph: %s" % ['->'.join(edge) for edge in cycle])
            raise_with_traceback(e)

        if store_flowir_to_disk:
            self.configuration.store_unreplicated_flowir_to_disk()

        return new_comp_names

    def _createCompleteGraph(
            self,  # type: WorkflowGraph
            inherit_graph=None,  # type: Optional[WorkflowGraph]
            include_default=True  # type: bool
    ):
        # type: (...) -> networkx.DiGraph
        '''Returns a networkx.DiGraph object of the experiment.

        All nodes in returned graph have the following properties which are functions
        - getConfiguration.
             This is a function that can be used to get the configuration of the component
             on the currently active platform.
             Call like: getConfiguration(raw=True)
        - getEnvironment
             This is a function that can be used to get the environment of the component
             on the currently active platform
             Call like: getEnvironment(raw=True)
        - getInputReferences:
             This returns the components references to input data
        - getVariables:
             This returns any variables directly used by the component

        Note to get a nodes producer references just get its input edges.

        If an `inherit_graph` is provided the resulting networkx graph will inherit the following node properties:
        - component (see class inherit_attributes)
        '''
        # VV: First process any DoWhile documents
        self.map_placeholders_to_looped_instances_of_components()
        self.update_dowhile_states()

        g = networkx.DiGraph()
        edges = []
        # VV: @tag:FlowIR:Components

        component_ids = self._concrete.get_component_identifiers(False)

        FlowIR = experiment.model.frontends.flowir.FlowIR

        for comp_id in component_ids:
            comp = self._concrete.get_component_configuration(comp_id, include_default=include_default)
            stage_index = comp['stage']
            name = comp['name']
            is_aggregate = comp['workflowAttributes']['aggregate']
            is_replicate = comp['workflowAttributes']['replicate'] not in [None, 0]

            node_spec = CreateNode(
                self,
                g,
                name,
                stage_index,
                is_replicate,
                False,
                is_aggregate,
                isPrimitive=False,
            )

            modifiedRefs = []

            references = node_spec.rawDataReferences

            application_dependencies = self.configuration.get_application_dependencies()
            top_level_folders = self.configuration.top_level_folders
            # NOTE: Cannot use dataReferences until graph is complete
            # as it relies on the complete graph to correctly identify component and non-component references
            # and at this stage the graph is not complete
            for ref in references:
                ref_stage, ref_name, filename, method = FlowIR.ParseDataReferenceFull(
                    ref, stage_index, application_dependencies=application_dependencies,
                    special_folders=top_level_folders)
                if ref_stage is None:
                    continue

                # Create a DataReference instance for this ref assuming its a component ref - filter it later
                pid = DataReference(ref, stageIndex=stage_index).producerIdentifier

                if pid.identifier in self._placeholders:
                    p_name = pid.identifier
                    # VV: Placeholder dependencies expand to *all* currently known instances of placeholder
                    #     + the condition, because we want to make sure that the component will wait till the
                    #     termination of the loop
                    try:
                        placeholder = experiment.model.frontends.flowir.deep_copy(self._placeholders[p_name])
                        represents = placeholder['represents']  # type: List[str]

                        dw_metadata = self.get_document_metadata(FlowIR.LabelDoWhile, placeholder['DoWhileId'])
                        doc = dw_metadata['document']
                        current_condition = dw_metadata['state']['currentCondition']
                        condition_id = DataReference(current_condition, doc['stage']).producerIdentifier.identifier

                        predecessors = represents
                        # VV: The component which produces the condition should not have a dependency to itself
                        if condition_id != node_spec.identification.identifier:
                            predecessors.append(condition_id)
                    except Exception as e:
                        msg = ("Failed to discover predecessors of placeholder %s for %s. Error: %s" % (
                            p_name, e, node_spec.identification.identifier))
                        graphLogger.critical(msg)
                        raise_with_traceback(ValueError(e))
                    else:
                        graphLogger.log(14, "Known predecessors of placeholder %s are %s (for %s)" % (
                            p_name, predecessors, node_spec.identification.identifier
                        ))

                        for predec_ref in predecessors:
                            predec_pid = ComponentIdentifier(predec_ref)
                            if predec_pid not in modifiedRefs:
                                modifiedRefs.append(predec_pid)
                else:
                    if pid not in modifiedRefs:
                        modifiedRefs.append(pid)

            edges.append((node_spec.identification, modifiedRefs))

            cid = node_spec.identification.identifier
            if inherit_graph and inherit_graph.graph.has_node(cid):
                inherit_node = inherit_graph.graph.nodes[cid]
                for attr in self.inherit_attributes:
                    if attr in inherit_node:
                        g.nodes[cid][attr] = inherit_node[attr]

        # A list of tuples. Each tuple is a (ComponentIdentifier, [list of ComponentIdentifier] pairs)
        for node, references in edges:
            for pid in references:
                if g.has_node(pid.identifier):
                    g.add_edge(pid.identifier, node.identifier, length=(node.stageIndex - pid.stageIndex + 1))

        return g

    @property
    def graph(self):
        # type:() -> networkx.DiGraph
        '''Returns a networkx.DiGraph object of the experiment.

        NOTE: Modifying this graph modifies the receiver

        All nodes in returned graph have the following properties which are functions
        - getConfiguration.
             This is a function that can be used to get the configuration of the component
             on the currently active platform.
             Call like: getConfiguration(raw=True)
        - getEnvironment
             This is a function that can be used to get the environment of the component
             on the currently active platform
             Call like: getEnvironment(raw=True)
        - getInputReferences:
             This returns the components references to input data
        - getVariables:
             This returns any variables directly used by the component

        Note to get a nodes producer references just get its input edges.
        '''

        return self._graph

    def configurationForNode(self, nodeName, raw=None, omitDefault=False, is_primitive=None,
                             inject_missing_fields=True):
        # type: (str, bool, bool, bool, bool) -> DictFlowIRComponent
        """Returns FlowIR description of component.

        Parameters:
            nodeName:
                It's expected to be `stage%d.%s % (stage_index:int, component_name)`
            raw:
                When True the returned configuration contains references to variables (i.e. `%(<variable_name>)s`)
                When False, a FlowIR instance will attempt to perform stringInterpolation.
            omitDefault:
                When True the returned configuration contains information that is explicitly defined
                in the component description.
            is_primitive:
                When True it is OK for the configuration to not include the `replica` variable.
        """
        if raw is None:
            raw = self.configuration.is_raw
        if is_primitive is None:
            is_primitive = self.isPrimitive
        return self.configuration.configurationForNode(
            nodeName, raw, omitDefault, is_primitive, inject_missing_fields=inject_missing_fields
        )

    def getOptionForNode(self, nodeName, key):
        return self.configuration.getOptionForNode(nodeName, key)

    def setOptionForNode(self, nodeName, key, value):
        return self.configuration.setOptionForNode(nodeName, key, value)

    def removeOptionForNode(self, nodeName, key):
        return self.configuration.removeOptionForNode(nodeName, key)

    def variablesForNode(self, nodeName):
        return self.configuration.variablesForNode(nodeName)

    def dataReferencesForNode(self, nodeName):
        # type: (str) -> List[str]

        """Returns the resolved data-reference strings for nodeName
        """
        return self.configuration.dataReferencesForNode(nodeName)

    def inputReferencesForNode(self, nodeName):
        # type: (str) -> List[str]

        '''Returns the input data (i.e. non-component) references for a node as strings

        This is a convenience method for accessing the nodes ComponentSpecification instance'''

        refs = self._graph.nodes[nodeName]['componentSpecification'].inputDataReferences  # type: List[DataReference]

        return [r.stringRepresentation for r in refs]

    def resolvedReferencesForNode(self, nodeName):
        # type: (str) -> List[str]

        '''Returns the component references for a node fully resolved as strings'''
        comp_desc = self._graph.nodes[nodeName]['componentSpecification']  # type: ComponentSpecification
        refs = comp_desc.componentDataReferences
        return [r.absoluteReference for r in refs]

    def producerReferencesForNode(self, nodeName):
        # type: (str) -> List[str]

        '''Returns the identifiers of the nodes nodeName consumes from

        Note: There is a sublte difference between this method and resolveReferencesForNode

        - This looks at edges in the graph to determine producers
        - resolvedReferencesForNode checks which of a nodes references are in the graph

        i.e. if an edge has been not been added then the two methods will return different answers

        '''

        #Note graph is directed so their are no edges going to producer only from
        #Have to reference the direction to get producers
        inputEdges = networkx.reverse(self.graph).edges(nodeName)
        return [el[1] for el in inputEdges]

    def environmentForNode(self, nodeName: str) -> Dict[str, str]:
        """Build environment for node.

        If a component uses the `none` environment we assume that the environment is just {}.

        Otherwise we fetch the appropriate environment (which can be the default one).

        In all cases we layer the constructed environment on top of the system environment variables generated by Flow.
        """
        return self.configuration.environmentForNode(nodeName)

    def environmentWithName(self, environment_name, expand=True):
        return self.configuration.environmentWithName(environment_name, expand)

    def defaultEnvironment(self):
        # type: () -> Dict[str, str]
        return self.configuration.defaultEnvironment()

    def changePlatform(self, platform):

        '''Creates a new graph based on a different platform'''

        return WorkflowGraph(self.configuration,
                             platform=platform,
                             primitive=self.isPrimitive)

    def replicate(self):

        '''Return the replicated version of receiver

        If the receiver is already replicated, returns the receiver'''

        if not self.isPrimitive:
            return self
        else:
            return WorkflowGraph(self.configuration,
                                 platform=self.platform,
                                 primitive=False)

    def primitive(self):

        '''Return the primitive version of receiver

        If the receiver is already primitive, returns the receiver'''

        if self.isPrimitive:
            return self
        else:
            return WorkflowGraph(self.configuration,
                                 platform=self.platform,
                                 primitive=True)

    @property
    def platforms(self):
        return self._concrete.platforms
