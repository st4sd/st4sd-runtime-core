#
# coding=UTF-8
# Copyright IBM Inc. 2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis

from __future__ import annotations

import threading
import collections
import copy
import difflib
import json
import logging
import os
import pprint
import re
import traceback
from string import Template
from threading import RLock
from typing import (Any, Callable, Dict, List, MutableMapping, Optional, Set,
                    Text, Tuple, Union, cast)
import psutil
import networkx
from future.utils import raise_with_traceback
import yaml
import yaml.error

from six import string_types

import experiment.model.codes
import experiment.model.errors
from past.builtins import cmp


# VV: https://stackoverflow.com/a/16782282 -- Encode dictionaries in the form of OrderedDicts
#     to preserve custom order of keys (see experiment.model.frontends.flowir.FlowIR.pretty_flowir_sort())
import experiment.model.errors



def represent_ordereddict(dumper, data):
    value = []

    for item_key, item_value in list(data.items()):
        node_key = dumper.represent_data(item_key)
        node_value = dumper.represent_data(item_value)

        value.append((node_key, node_value))

    return yaml.MappingNode('tag:yaml.org,2002:map', value)


yaml.add_representer(collections.OrderedDict, represent_ordereddict, yaml.SafeDumper)
# VV: Disable the use of alias tags when storing yaml (i.e if an object is used N times
#     it will be stored N times rather than dumping it once and then using N-1 alias references)
yaml.SafeDumper.ignore_aliases = lambda *data: True


def yaml_load(stream, Loader=yaml.SafeLoader):
    return yaml.load(stream, Loader)


def yaml_dump(data, stream=None, **kwargs):

    try:
        return yaml.dump(data, stream, yaml.SafeDumper, **kwargs)
    except TypeError as e:
        if 'sort_keys' in kwargs:
            # VV: PyYAML has changed its API and for some versions `sort-keys` breaks .dump()
            new_kwargs = {
                key: kwargs[key] for key in kwargs if key !='sort_keys'
            }
            return yaml.dump(data, stream, yaml.SafeDumper, **new_kwargs)
        else:
            raise


BasicTypes = Union[float, int, bool, Text]
DictManifest = Dict[str, str]
DictDocument = Dict[str, Any]
# VV: Collection of Documents in the form of a nested dictionary. Outer key is type of Document
# next key is ID of Document and innermost dictionary contains the contents of the document.
DictManyDocuments = Dict[str, Dict[str, DictDocument]]
DictFlowIR = MutableMapping[Text, Any]
DictFlowIRVariableCollection = MutableMapping[Text, BasicTypes]
DictFlowIRComponent = MutableMapping[Text, Any]
DictFlowIRResourceManager = MutableMapping[str, Dict[str, Any]]
DictFlowIRInterface = MutableMapping[str, Any]
DictFlowIROutput = MutableMapping[str, MutableMapping[str, Any]]

# VV: (stage_index, component_name)
FlowIRComponentId = Tuple[int, Text]

flowirLogger = logging.getLogger('flowir')


PrimitiveTypes = (float, int, bool, ) + string_types


def preprocess_import_document_paths(doc_path, base_path):
    errs = validate_object_schema(doc_path, string_types, '%s.import' % base_path)

    if errs:
        flowirLogger.critical("Failed to import")
        for e in errs:
            flowirLogger.critical("  %s" % str(e))

        if len(errs) > 1:
            raise ValueError(errs)
        else:
            raise errs[0]

    # VV: Documents that are not absolute paths should be searched in the parent folder of the
    #     document that imports them

    if doc_path.startswith('http:') or doc_path.startswith('https://'):
        msg = ("Cannot import \"%s\" because fetching documents over "
               "the network is not supported yet" % (doc_path))
        flowirLogger.warning(msg)
        raise NotImplementedError(msg)
    elif doc_path.startswith('/') is False:
        parent_uri = os.path.split(base_path)[0]
        doc_path = os.path.join(parent_uri, doc_path)
        doc_path = os.path.normpath(os.path.abspath(doc_path))

    return doc_path


def rewrite_reference(reference, binding_values, import_to_stage, owner_component_stage):
    stage_idx, orig_producer, original_filename, orig_method = \
        FlowIR.ParseDataReferenceFull(reference, owner_component_stage)

    if orig_producer not in binding_values:
        # VV: Apply stage-offset
        if stage_idx is not None:
            new_ref = FlowIR.compile_reference(orig_producer, original_filename, orig_method,
                                               stage_idx + import_to_stage)
        else:
            # VV: This is a reference to an `input` or a `data` file, no need to rewrite
            return reference
    else:
        # VV: Compile a reference based on the binding_values information
        binding_ref = binding_values[orig_producer]
        stage_idx, producer, binding_filename, binding_method = FlowIR.ParseDataReferenceFull(binding_ref)

        if binding_method != orig_method:
            raise ValueError("Cannot rewrite binding reference %s based on %s ")

        new_filename = binding_filename
        if binding_filename != original_filename and binding_filename and original_filename:
            raise experiment.model.errors.FlowIRInvalidBindingModifyFilename(binding_ref, reference)
        elif original_filename:
            new_filename = original_filename
        new_ref = FlowIR.compile_reference(producer, new_filename, binding_method, stage_idx)

    if reference != new_ref:
        flowirLogger.info("Rewrote reference %s to %s while importing document" % (reference, new_ref))

    return new_ref


def rewrite_all_references(
        value,
        binding_values,
        known_components,
        owner_component_stage,
        import_to_stage,
        iter_number=None,
        looped_ids=None  # type: Optional[Set[FlowIRComponentId]]
    ):
    out_map = {}
    looped_ids = looped_ids or set()
    all_known = set(known_components).union(looped_ids)
    _ = FlowIR.discover_reference_strings(value, owner_component_stage, all_known, out_map)

    for match in out_map:
        rewrite = rewrite_reference(out_map[match], binding_values, import_to_stage, owner_component_stage)

        stage_index, producer, filename, method = FlowIR.ParseDataReferenceFull(rewrite, import_to_stage)
        
        comp_id = (stage_index, producer)
        
        if comp_id not in all_known:
            flowirLogger.info("Skip rewritting \"%s\" because it points to unknown component %s" % (match, comp_id))
            continue

        if iter_number is not None:
            flowirLogger.log(14, "Looking for component %s (because of %s->%s)" % (
                (stage_index, producer), match, out_map[match]
            ))
            # VV: Rename stage<index:%d>.<component> to stage<index:%d>.<loop-iteration:%d>#<component-name>
            if (stage_index, producer) in looped_ids and method not in ['loopref', 'loopoutput']:
                new_stage_index, _, _, _ = FlowIR.ParseDataReferenceFull(rewrite, import_to_stage)
                rewrite = FlowIR.compile_reference('%d#%s' % (iter_number, producer), filename, method, new_stage_index)
                flowirLogger.log(14, "Will substitute %s with %s in %s" % (
                    match, rewrite, value
                ))

        pattern = r'\b' + re.escape(match) + r'\b'

        try:
            value = re.sub(pattern, rewrite, value, 1)
        except Exception:
            flowirLogger.critical("Failed to res.sub(\"%s\", \"%s\", \"%s\"" % (pattern, rewrite, value))
            raise

    return value


def rewrite_components(
        components,  # type: List[DictFlowIRComponent]
        bindings,  # type: Dict[str, str]
        known_components,  # type: Set[FlowIRComponentId]
        import_to_stage,  # type: int
        iteration_no=None,  # type: Optional[int]
        looped_components=None  # type: Optional[Set[FlowIRComponentId]]
    ):
    rewritten_components = []

    for comp in components:
        # VV: Traverse object to update bindings with their values, and fix the stage-index of refs to components
        new_comp = FlowIR.replace_strings(
            comp,
            lambda val: rewrite_all_references(
                val,
                bindings,
                known_components,
                comp.get('stage', 0),
                import_to_stage,
                iter_number=iteration_no,
                looped_ids=looped_components,
            ))

        # VV: Now we need to apply the stage-offset
        new_comp['stage'] = comp.get('stage', 0) + import_to_stage

        if iteration_no is not None:
            # VV: Record current iteration number in `loopIteration` variable (this is to looping what
            #     `replica` is to replicate)
            if 'variables' not in new_comp:
                new_comp['variables'] = {}

            new_comp['variables']['loopIteration'] = iteration_no

            new_comp['name'] = '%d#%s' % (iteration_no, new_comp['name'])

        rewritten_components.append(new_comp)

    return rewritten_components


def validate_input_bindings_names(input_bindings):
    illegal_binding_values = set(input_bindings).intersection(FlowIR.SpecialFolders)
    if illegal_binding_values:
        raise ValueError("inputBindings cannot have keys: %s because %s are reserved keywords" % (
            list(illegal_binding_values), FlowIR.SpecialFolders
        ))


def rewrite_loopbindings_for_stage_offset(loop_bindings, stage_offset):
    # type: (Dict[str, str], int, List[str]) -> Dict[str, str]
    """Rewrites loopBindings by adding the stage offset of the virtual-component which imports the DoWhile document

    Arguments:
        loop_bindings(Dict[str, str]): Key:value pairs where key is a name, and value a string representation of
            a DataReference
        stage_offset(int): the stage of the virtual-component which imports the DoWhile document that contains
            the loop_bindings
    Returns
        A rewritten version of the loop_bindings dictionary where stage indices of DataReferences are incremented by
        the @stage_offset
    """
    if loop_bindings is None:
        return {}

    ret = {}

    for name in loop_bindings:
        ref = loop_bindings[name]
        stage_idx, producer, filename, method = FlowIR.ParseDataReferenceFull(ref)
        if stage_idx is None:
            stage_idx = 0
        ret[name] = FlowIR.compile_reference(producer, filename, method, stage_idx+stage_offset)
    
    return ret
        

def validate_provided_bindings(bindings, input_bindings, foreign_components, label, doc_type,
                               loop_bindings=None, in_loop_ids=None):
    missing_binding_values = set(input_bindings) - set(bindings)

    if missing_binding_values:
        raise ValueError("There were missing binding values: %s for %s document %s. Provided ones where: %s" % (
            list(missing_binding_values), label, doc_type, list(bindings.keys())
        ))

    checks = [(bindings, 'Binding', foreign_components), (loop_bindings or {}, 'Loop-Binding', in_loop_ids)]
    mismatch_binding_methods = []

    for collection, name, known in checks:
        for binding in collection:
            stage_idx, producer, filename, method = FlowIR.ParseDataReferenceFull(collection[binding])
            if method != input_bindings[binding]['type']:
                mismatch_binding_methods.append("%s value \"%s\" of \"%s\" has method \":%s\" "
                                                "but was expected to be \":%s\"" % (
                                                    name, binding, label, method, input_bindings[binding]['type']
                                                ))

            if stage_idx is not None:
                # VV: This is a component, it must be contained in `foreign_components`
                if (stage_idx, producer) not in known:
                    raise experiment.model.errors.FlowIRReferenceToUnknownComponent(
                        collection[binding], producer, stage_idx, foreign_components
                    )

    if mismatch_binding_methods:
        msg = "Invalid methods for dynamic binding: %s" % '.'.join(mismatch_binding_methods)
        flowirLogger.warning(msg)
        raise ValueError(msg)


def extract_ids_from_components(components, in_stage):
    known_components = []
    for comp in components:
        known_components.append((comp.get('stage', 0) + in_stage, comp['name']))

    return known_components


def instantiate_dowhile(
        dw_template,  # type: Dict[str, Any]
        bindings,  # type:  Dict[str, str]
        import_to_stage,  # type: int
        import_with_name, # type: str
        foreign_components,  # type: Set[FlowIRComponentId]
        label='*unknown*',  # type: str
        iteration_no=0,  # type: int
    ):
    # type: (...) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]
    """

    # VV: See hartreechem/flow#704
    """
    new_dw_template = deep_copy(dw_template)
    components = dw_template.get(FlowIR.FieldComponents, [])  # type: List[Dict[str, Any]]
    input_bindings = dw_template['inputBindings']
    loop_bindings = dw_template.get('loopBindings', {})

    validate_input_bindings_names(input_bindings)

    # VV: TODO Validate the DoWhile-type document before merging
    all_ids = extract_ids_from_components(components, import_to_stage)
    inloop_ids = set(all_ids)

    if len(inloop_ids) != len(all_ids):
        raise ValueError("There are duplicate Component(s): %s" % (all_ids))

    loop_bindings = rewrite_loopbindings_for_stage_offset(loop_bindings, import_to_stage)
    validate_provided_bindings(bindings, input_bindings, foreign_components, label, 'DoWhile',
                               loop_bindings=loop_bindings, in_loop_ids=inloop_ids)

    if iteration_no > 0 and loop_bindings:
        # VV: This is not the first iteration, the loop-bindings should be applied to the components first
        bindings = {key: bindings[key] for key in bindings if key not in loop_bindings}

        # VV: Before we start re-writting we need to project the loop-bindings to stage0, that is remove
        #     the import stage from all references. This way stage0 references will point to the first
        #     stage in the loop section (i.e. stage 0)
        #     We also want to reference components in the last iteration, not the one that we're about to generate

        loop_bindings = deep_copy(loop_bindings)

        for key in loop_bindings:
            value = loop_bindings[key]
            stage_idx, producer, filename, method = FlowIR.ParseDataReferenceFull(value, 0)
            if method not in ['loopref', 'loopoutput']:
                producer = '%d#%s' % (iteration_no-1, producer)

            value = FlowIR.compile_reference(producer, filename, method, stage_index=stage_idx)
            loop_bindings[key] = value

        bindings.update(loop_bindings)

    flowirLogger.info("Bindings for DoWhile stage%s.%s#%d are %s" % (
        import_to_stage, import_with_name, iteration_no, pprint.pformat(bindings)
    ))

    rewritten_components = rewrite_components(components, bindings, foreign_components, import_to_stage,
                                              iteration_no, inloop_ids)

    # VV: Update the templates of the components with their new stages
    template_components = new_dw_template.get('components', [])
    for comp in template_components:
        comp['stage'] = comp.get('stage', 0)

    new_dw_template['components'] = template_components
    # VV: These fields can be used to generate new iterations, the document now contains its entire state
    new_dw_template['bindings'] = bindings
    # VV: Do not update loopBindings as they'd be rewritten each time this method is invoked and their stage indices
    # would just keep incrementing; just make sure that the key exists.
    if 'loopBindings' not in new_dw_template:
        new_dw_template['loopBindings'] = {}
    new_dw_template['stage'] = import_to_stage
    new_dw_template['name'] = import_with_name

    if 'condition' not in new_dw_template:
        raise ValueError("DoWhile %s does not contain a condition" % label)
    condition = new_dw_template['condition']
    cond_stage, cond_name, cond_file, cond_method = FlowIR.ParseDataReferenceFull(condition, 0)
    cond_stage = cond_stage or 0

    if (cond_stage+import_to_stage, cond_name) not in inloop_ids:
        raise ValueError("Invalid condition component stage%s.%s for DoWhile %s. Condition must be produced by "
                         "a looped component [%s]" % (
            cond_stage, cond_name, label, ', '.join(['stage%s.%s' % (s, n) for (s, n) in inloop_ids])))

    if cond_method != 'output':
        raise ValueError("Invalid condition %s for DoWhile %s. Conditions must be \":output\"" % (condition, label))
    
    # VV: Generate the absolute form of the condition
    new_dw_template['condition'] = FlowIR.compile_reference(cond_name, cond_file, cond_method, cond_stage)

    return rewritten_components, new_dw_template


def instantiate_workflow(
        wf_template,  # type: Dict[str, Any]
        bindings,  # type: Dict[str, str]
        import_to_stage,  # type: int
        foreign_components,  # type: Set[FlowIRComponentId]
        label='*unknown*'  # type:  str
    ):
    # type: (...) -> List[Dict[str, Any]]
    """Instantiates a workflow template based on a dictionary of values for the wf bindings and returns a list of
     components.

    # VV: See hartreechem/flow#703
    """
    components = wf_template.get(FlowIR.FieldComponents, [])
    input_bindings = wf_template['inputBindings']

    validate_input_bindings_names(input_bindings)
    validate_provided_bindings(bindings, input_bindings, foreign_components, label, FlowIR.LabelWorkflow)

    all_ids = extract_ids_from_components(components, import_to_stage)
    known_components = set(all_ids)
    if len(known_components) != len(all_ids):
        raise ValueError("There are duplicate Component(s): %s" % (all_ids))

    all_components = known_components.union(foreign_components)

    rewritten_components = rewrite_components(components, bindings, all_components, import_to_stage)

    return rewritten_components


def expand_bindings(bindings, in_stage):
    for bind in bindings:
        stage_index, producer, filename, method = FlowIR.ParseDataReferenceFull(bindings[bind], in_stage)
        new_binding = FlowIR.compile_reference(producer, filename, method, stage_index)

        flowirLogger.info("Rewrote binding %s:%s to %s" % (bind, bindings[bind], new_binding))

        bindings[bind] = new_binding

    return bindings


def package_document_load(package_path, is_instance):
    # type: (str, bool) -> Tuple[DictFlowIR, Dict[str, Any]]
    """Returns the FlowIR that is stored in `package_path` and the description of any meta-documents such as DoWhile

    The meta-documents are returned as the second item in the returned tuple. They are encoded in a dictionary with
    the top-level key being the type of the document (e.g. for DoWhile documents the key would be "DoWhile").

    Each Document-collection is encoded as a dictionary with the key being the name of the document (
    which is stage<idx:%d>.<component_name:str>, where component is the component entry that instantiates the Document
    i.e. the one which contains the $import field).

    Loading an instance will not expand the iteration 0 of loops.
    """

    if os.path.isfile(package_path) is False:
        raise experiment.model.errors.ExperimentMissingConfigurationError(f"Missing configuration at {package_path}")

    try:
        with open(package_path, 'r') as f:
            root = yaml_load(f)
    except yaml.error.YAMLError as e:
        raise experiment.model.errors.ExperimentInvalidConfigurationError(
            f"Configuration contains Malformed YAML: {str(e)}", underlyingError=e) from e

    if isinstance(root, dict) is False:
        raise TypeError("Expected workflow in %s to be a dictionary, but it is a %s" % (package_path, type(root)))

    if FlowIR.FieldComponents not in root:
        return root, {}

    components = root[FlowIR.FieldComponents]  # type: List[DictFlowIRComponent]

    # VV: FIXME Need to validate fields of root before merging the definition with the document

    new_components = []
    dw_loops = {}

    do_while_components = []
    workflow_components = []

    for i, comp in enumerate(components):
        if '$import' in comp:
            new_components.append(deep_copy(comp))
            doc_path = preprocess_import_document_paths(comp['$import'], package_path)

            with open(doc_path, 'r') as f:
                doc = yaml_load(f)

            if isinstance(doc, dict) is False:
                raise TypeError("Document %s was expected to be a dictionary, but it is a %s" % (doc_path, type(doc)))

            if doc.get('type') in [FlowIR.LabelWorkflow, FlowIR.LabelDoWhile]:
                # VV: Make sure that bindings are in the `expanded` form before they're used to instantiate a
                #     template
                if 'bindings' in comp:
                    bindings = comp['bindings']
                    bindings = expand_bindings(bindings, comp.get('stage', 0))
                else:
                    bindings = {}
            else:
                bindings = None

            doc_stage = int(comp.get('stage', 0))

            try:
                doc_name = comp['name']
            except KeyError:
                raise ValueError("Cannot instantiate document \"%s\" in stage %d without a \"name\" (component "
                                 "index: %d)" % (
                    doc.get('type'), doc_stage, i
                ))

            doc_id = 'stage%d.%s' % (doc_stage, doc_name)

            # VV: See hartreechem/flow#703 and hartreechem/flow#704
            if doc.get('type') == FlowIR.LabelWorkflow:
                label = 'stage%d.%s-workflow(%s)' % (doc_stage, doc_name, doc_path)
                workflow_components.append(
                    (doc_path, doc_id, doc, bindings, doc_stage, doc_name, label)
                )
            elif doc.get('type') == FlowIR.LabelDoWhile:

                label = 'stage%d.%s-dowhile(%s)' % (doc_stage, doc_name, doc_path)

                do_while_components.append(
                    (doc_path, doc_id, doc, bindings, doc_stage, doc_name, label)
                )
            else:
                raise NotImplementedError("Unknown type for imported by a component document: %s (type is %s)" % (
                    doc_path, doc.get('type')))

        else:
            new_components.append(comp)

    # VV: DoWhile documents require knowledge of all Components before we can be instantiated so that they can tell
    #     which references should be rewritten
    component_ids = set()  # type: Set[FlowIRComponentId]
    for comp in new_components:
        try:
            component_ids.add((comp.get('stage', 0), comp['name']))
        except Exception:
            raise  ValueError("Illegal component definition is missing \"name\" key: %s" % comp)

    # VV: Register expected components (and placeholders) from soon-to-be-imported Workflow (and DoWhile) documents
    for tup in workflow_components + do_while_components:
        (doc_path, doc_id, doc, bindings, doc_stage, doc_name, label) = tup
        for comp in doc.get('components', []):
            component_ids.add((comp.get('stage', 0) + doc_stage, comp['name']))

    for tup in workflow_components:
        (doc_path, doc_id, doc, bindings, doc_stage, doc_name, label) = tup
        try:
            foreign_ids = component_ids.copy()
            for comp in doc.get('components', []):
                component_ids.remove((comp.get('stage', 0) + doc_stage, comp['name']))

            wf_components = instantiate_workflow(doc, bindings, doc_stage, foreign_ids, doc_path)
            new_components.extend(wf_components)
        except Exception as e:
            flowirLogger.warning("Could not import Workflow document at %s. Exception %s" % (
                doc_path, e))
            raise_with_traceback(e)

    for tup in do_while_components:
        (doc_path, doc_id, doc, bindings, doc_stage, doc_name, label) = tup
        try:
            foreign_ids = component_ids.copy()
            for comp in doc.get('components', []):
                component_ids.remove((comp.get('stage', 0) + doc_stage, comp['name']))

            dw_components, new_dw_doc = instantiate_dowhile(doc, bindings, doc_stage, doc_name, foreign_ids, label, 0)

            # VV: Do not generate iteration 0 when loading instances, the components already exist
            if is_instance is False:
                new_components.extend(dw_components)

            dw_loops[doc_id] = new_dw_doc
        except Exception as e:
            flowirLogger.warning("Could not import Workflow document at %s. Exception %s" % (
                doc_path, e))
            raise_with_traceback(e)

    if new_components:
        root[FlowIR.FieldComponents] = new_components

        flowirLogger.log(14, "Loaded document(s). Resulting components: %s\n" % pprint.pformat(new_components))

    documents = {}

    if dw_loops:
        documents[FlowIR.LabelDoWhile] = dw_loops

    return root, documents


class FlowIRCache(object):
    """
    Components are tagged as: "component:<platform-name>:stage<stage-index>:<component-name>". Such entries contain
                              the Fully resolved non-primitive (i.e. replicated) FlowIR definition of components

    A disabled cache will still allow inserting/removing definitions but it won't return cached values (it will
    pretend that they don't exist by raising a ValueError)
    """
    def __init__(self):
        self._cache = {}  # type: Dict[str, Dict[str, Any]]
        self._lock = RLock()
        self._enabled = True

    def keys(self):
        with self._lock:
            return list(self._cache.keys())

    def __contains__(self, reference):
        return self.in_cache(reference)

    def __getitem__(self, reference):
        # type: (str) -> Dict[str, Dict[str, Any]]
        return self.get(reference)

    def __setitem__(self, reference, flowir):
        # type: (str, Dict[str, Any]) -> None
        return self.set(reference, flowir)

    def __delitem__(self, reference):
        # type: (str) -> None
        return self.invalidate_reference(reference)

    def pop(self, reference):
        # type: (str) -> None
        return self.invalidate_reference(reference)

    def disable(self):
        with self._lock:
            self._enabled  = False

    def enable(self):
        with self._lock:
            self._enabled = True

    def clear(self):
        with self._lock:
            self._cache.clear()

    def in_cache(self, reference):
        # type: (str) -> bool
        """Check if FlowIR definition of reference exists in cache
        """

        with self._lock:
            return reference in self._cache

    def get(self, reference):
        # type: (str) -> Dict[str, Dict[str, Any]]
        """Returns cached FlowIR definition of reference, raises KeyError exception if FlowIR definition does not exist
        """
        with self._lock:
            if self._enabled is False:
                raise ValueError("Lookup of %s FlowIR definition in cache will not be performed "
                               "because the cache is disabled at the moment" % reference)
            return deep_copy(self._cache[reference])

    def set(self, reference, flowir):
        # type: (str, Dict[str, Any]) -> None
        with self._lock:
            self._cache[reference] = flowir

    def invalidate_reference(self, reference):
        # type: (str) -> None
        """Invalidates FlowIR definiton of reference
        """
        with self._lock:
            try:
                del self._cache[reference]
            except Exception:
                pass

    def invalidate_reg_expression(self, reg_expression):
        pattern = re.compile(reg_expression)
        return self.invalidate_selector(lambda name, value: pattern.match(name) is not None)

    def invalidate_selector(self, query):
        # type: (Callable[[str, Dict[str, Any]], bool]) -> Dict[str, Any]
        """Invalidates all entries that match query(entry-name, entry-value), returns a Dictionary with removed entries.
        Args:
            query: Function that receives 2 arguments and returns a boolean. The first argument is
                   the name of a cache entry, the second the value. When query(entry-name, entry-value) returns True
                   the matched entry is removed from the cache
        """

        removed = {}

        with self._lock:
            for entry_name in list(self._cache):
                try:
                    if query(entry_name, self._cache[entry_name]):
                        removed[entry_name] = self._cache[entry_name]
                        self._cache.pop(entry_name)
                        flowirLogger.log(19, "Will remove FlowIR cache for %s" % entry_name)
                except Exception as e:
                    flowirLogger.warning("%s raised exception %s when processing entry %s" % (query, e, entry_name))

        return removed


def str_to_bool(s):
    # type: (str) -> bool
    if isinstance(s, bool):
        return s

    return {
        'true': True,
        'false': False,
        'yes': True,
        'no': False,
    }[s.lower()]


def is_primitive_or_empty(which):
    try:
        is_empty = (which is None) or (len(which) == 0)
    except Exception:
        is_empty = False

    return is_empty or isinstance(which, PrimitiveTypes)

def is_primitive(which):
    return (which is None) or isinstance(which, PrimitiveTypes)


def pretty_json(entry):
    return json.dumps(entry, sort_keys=True, indent=4, separators=(',', ': '))


def deep_copy(value):
    """Returns a deep copy of `value`.
    """
    return copy.deepcopy(value)


def replace_env_var(string, key, value):
    # VV: re.finditer() returns matches in the order found when scanning the string from left to right
    #     apply the replace() instructions using the reverse order so that the indices reported by
    #     finditer() are correct
    template = Template(string)
    return template.safe_substitute({key: value})


def expand_vars(string, environment):
    # type: (str, Dict[str, str]) -> str
    """Expands references to environment variables with their values.

    Args:
        string: Source string
        environment: Dictionary of environment variables (name: value)

    Returns:
        The `source` string but with all references to the environment variables replaced
        with their values.
    """
    template = Template(string)
    return template.safe_substitute(environment)


class ValidateMany(object):
    def __init__(self, schema, label_generator=None):
        self.schema = schema
        self.label_generator = label_generator

    def __str__(self):
        return ''.join(['Many(', Text(self.schema), ')'])


class ValidateOr(object):
    def __init__(self, *args):
        self.schema = list(args)

    def __str__(self):
        return ''.join(['Or(', Text(self.schema), ')'])


class ValidateOptional(object):
    def __init__(self, schema):
        self.schema = schema

    def __str__(self):
        return ''.join(['Optional(', Text(self.schema), ')'])


def schema_to_option_list(what, output_list):
    """Extracts human readable description of a schema in the form of a list of valid options

    Arguments:
        what(Any): A schema (e.g. ValidateOptional, Dict, str, type, tuple, etc)
        output_list(List[str]): A list of human readable strings explaining the valid options that the schema
            represents. This list object is updated.
    """

    if is_primitive_or_empty(what):
        output_list.append(str(what))
    elif isinstance(what, ValidateOptional):
        output_list.append("None")
        schema_to_option_list(what.schema, output_list)
    elif isinstance(what, type):
        output_list.append(str(what))
    elif isinstance(what, tuple):
        for t in what:
            output_list.append(str(t))
    elif isinstance(what, (list, dict)):
        for t in what:
            schema_to_option_list(t, output_list)
    elif isinstance(what, ValidateOr):
        # VV: Let this be a bit ugly for now
        for w in what.schema:
            output_list.append(str(w))
    elif isinstance(what, ValidateMany):
        nested = []
        schema_to_option_list(what.schema, nested)
        output_list.append(str(nested))
    elif isinstance(what, Callable):
        output_list.append('Func(%s)' % str(what))
    else:
        raise NotImplementedError("Unknown schema %s" % what, what)

    unique = set(output_list)
    del output_list[:]
    output_list.extend(unique)


def validate_object_schema(obj, schema, root_label, already_validated=None, report_validated=None):
    # type: (MutableMapping, MutableMapping, Text, Optional[Set], Optional[Set]) -> List[Exception]
    errors = []  # type: List[Exception]

    remaining = [(obj, schema, root_label)]

    # VV: Holds ids of objects that have already been validated; protects against infinite recursion
    validated = already_validated or set()

    # flowirLogger.debug("Validate object %s" % id(obj))

    def generate_label(item):
        try:
            return Text(item)
        except Exception:
            return ''.join(['obj(', id(item), ')'])

    def safe_call(func, arg):
        try:
            return func(arg)
        except Exception:
            return False

    while remaining:
        c_obj, c_schema, c_label = remaining.pop(0)
        id_obj = id(c_obj)
        schema_label = c_schema if is_primitive_or_empty(c_schema) else ('complex: %s' % Text(c_schema))
        orig_schema = c_schema
        object_label = '%s:%s' % (c_label, id_obj)

        if id_obj in validated:
            errors.append(RuntimeError("Object %s is used recursively" % object_label))
            continue

        if is_primitive_or_empty(c_obj) is False:
            # flowirLogger.debug("Processing %s" % object_label, level=10)
            validated.add(id_obj)

        if isinstance(c_schema, ValidateOptional):
            c_schema = c_schema.schema

        def err_could_not_be_validated():
            """Utility method to generate 1 FlowIRValueInvalid exception for current c_obj being tested"""
            valid_choices = []
            schema_to_option_list(c_schema, valid_choices)
            errors.append(experiment.model.errors.FlowIRValueInvalid(c_label, c_obj, valid_choices))

        if (isinstance(orig_schema, ValidateOptional)) and (c_obj is None):
            continue

        if is_primitive(c_schema):
            if c_obj != c_schema:
                err_could_not_be_validated()
        elif isinstance(c_schema, (tuple, type)):
            if isinstance(c_obj, c_schema) is False:
                err_could_not_be_validated()
        elif isinstance(c_schema, Callable):
            if safe_call(c_schema, c_obj) is False:
                err_could_not_be_validated()
        elif isinstance(c_schema, dict):
            # VV: Dictionaries schemas for both keys and values can be either constants or types
            #  - Constant: There *must* be a key in c_obj that matches the constant
            #  - Type: All keys for which a `constant` didn't match must have matched with one Type

            # VV: This is filled in the first time an unknown key is found
            valid_keys = []

            if isinstance(c_obj, dict) is False:
                errors.append(experiment.model.errors.FlowIRValueInvalid(object_label, c_obj, [str(dict)]))
                continue

            maybe = tuple([s_key for s_key in c_schema if isinstance(s_key, ValidateOptional)])
            types = tuple([s_key for s_key in c_schema if isinstance(s_key, (tuple, type)) and s_key not in maybe])
            constants = [s_key for s_key in c_schema if s_key not in types and s_key not in maybe]

            matched_rules = []

            maybe = cast(Tuple[ValidateOptional], maybe)

            for key in c_obj:
                this_key = generate_label(key)
                key_label = '.'.join([c_label, this_key])

                if key in constants:
                    # VV: A Constant matched the key, check the key next
                    entry = (c_obj[key], c_schema[key], key_label)
                    matched_rules.append(key)
                    remaining.append(entry)
                    continue

                matched = False

                for key_schema in types:
                    if isinstance(key, key_schema) or (
                        isinstance(key_schema, Callable) and safe_call(key_schema, key)
                    ):
                        matched_rules.append(key_schema)
                        entry = (c_obj[key], c_schema[key_schema], key_label)
                        remaining.append(entry)
                        matched = True
                        break

                if matched:
                    continue

                # VV: The key was not matched by any of the Constant or Type rules, check out the Optional ones
                for key_schema in maybe:
                    actual = key_schema.schema
                    if isinstance(actual, (tuple, type)):
                        if isinstance(key, actual) or (isinstance(actual, Callable) and safe_call(actual, key)):
                            matched_rules.append(key_schema)
                            entry = (c_obj[key], c_schema[key_schema], key_label)
                            remaining.append(entry)
                            break
                    elif key == actual or (isinstance(actual, Callable) and safe_call(actual, key) is True):
                        entry = (c_obj[key], c_schema[key_schema], key_label)
                        matched_rules.append(key_schema)
                        remaining.append(entry)
                        break
                else:
                    most_appropriate = None
                    if not(valid_keys):
                        schema_to_option_list(c_schema, valid_keys)

                    possibilities = difflib.get_close_matches(this_key, valid_keys)
                    if len(possibilities):
                        most_appropriate = possibilities[0]

                    errors.append(experiment.model.errors.FlowIRKeyUnknown(key_label, valid_keys, most_appropriate))

            # VV: Discover any rules that were not matched AND are not ValidateOptional
            missing = [
                rule for rule in c_schema
                if rule not in matched_rules and isinstance(rule, ValidateOptional) is False
            ]
            missing_types = [t for t in missing if isinstance(t, (tuple, type))]

            # VV: If any matched constant can be matched by a `missing` rule then discard the rule
            for m in matched_rules:
                if isinstance(m, (tuple, type)) is True:
                    continue
                for t in [t for t in missing_types if isinstance(m, t)]:
                    missing.remove(t)

            for key in missing:
                missing_label = '.'.join([c_label, generate_label(key)])
                errors.append(experiment.model.errors.FlowIRKeyMissing(missing_label))
        elif isinstance(c_schema, List):
            # VV: List schemas indicate an `Or` check, i.e. all c_obj entries must match at least one constant/type
            if isinstance(c_obj, List) is False:
                errors.append(experiment.model.errors.FlowIRValueInvalid(c_label, c_obj, [str(list)]))
                continue
            or_schema = ValidateOr(*tuple(c_schema))

            for index, child in enumerate(c_obj):
                child_label = ''.join([c_label, '[', Text(index), ']'])
                entry = (child, or_schema, child_label)
                remaining.append(entry)
        elif isinstance(c_schema, ValidateMany):
            # VV: `ValidateMany` schemas are meant for collections of objects which must match a single rule
            #     a `ValidateMany` schema may also carry a method to generate a label out of the object and its index
            #     if the generator fails or if it is not supplied the child label will be [<index>]
            generator = c_schema.label_generator
            c_schema = c_schema.schema

            if isinstance(c_obj, List) is False:
                errors.append(experiment.model.errors.FlowIRValueInvalid(c_label, c_obj, [str(list)]))
                continue
            for index, child in enumerate(c_obj):
                try:
                    child_label = generator(child, index)
                    child_label = ''.join([':', child_label])
                except (KeyboardInterrupt, SystemExit):
                    raise
                except Exception:
                    child_label = ''
                label = ''.join([c_label, '[', Text(index), child_label, ']'])

                entry = (child, c_schema, label)
                remaining.append(entry)
        elif isinstance(c_schema, ValidateOr):
            c_schema = c_schema.schema
            for list_schema in c_schema:
                # VV: TODO Implement checking an object against a set of schemas without using recursion
                validated_by_child = set()

                child_error = validate_object_schema(
                    c_obj, list_schema, c_label, already_validated, validated_by_child)
                if bool(child_error) is False:
                    break
                else:
                    undo = validated - validated_by_child
                    validated.clear()
                    validated.update(undo)
            else:
                valid_keys = []
                schema_to_option_list(c_schema, valid_keys)
                errors.append(experiment.model.errors.FlowIRValueInvalid(c_label, c_obj, valid_keys))
        else:
            msg = "Cannot validate %s with expected type %s" % (object_label, schema_label)
            errors.append(NotImplementedError(msg))

    if report_validated is not None:
        report_validated.update(validated)

    return errors


class ComponentIdentifier(object):
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
        self._index, self._componentName, self.hasIndex = FlowIR.ParseProducerReference(self._id, index=index)

        self._namespace = "stage%d" % self._index if self._index is not None else None

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


def extract_producer_id(from_reference):
    reference, _, _ = FlowIR.ParseDataReference(from_reference)
    stageIndex, jobName, hasIndex = FlowIR.ParseProducerReference(reference)

    if hasIndex:
        return (stageIndex, jobName)

    return None


class Manifest:
    def __init__(self, manifest: DictManifest, validate=True):
        """Instantiate a Manifest class using a manifest dictionary

        Args:
            manifest: The manifest is a dictionary, with targetFolder: sourceFolder entries. Each
                sourceFolder will be copied or linked to populate the respective targetFolder. Source folders can be
                absolute paths, or paths relative to the path of the FlowIR YAML file. SourceFolders may also be
                suffixed with :copy or :link to control whether targetFolder will be copied or linked to sourceFolder
                (default is copy). TargetFolders are interpreted as relative paths under the instance directory. They
                may also be the resulting folder name of some applicationDependency. They may include a path-separator
                (.e.g /) but must not be absolute paths.
            validate: Triggers the validation of manifest contents

        Raises:
             experiment.model.errors.FlowIRManifestSyntaxException:
                if manifest syntax is invalid
        """
        manifest = manifest or {}
        try:
            self._manifest = {str(x): str(manifest[x]) for x in manifest}
        except Exception as e:
            raise experiment.model.errors.FlowIRManifestInvalidType(manifest)

        if validate:
            self.validate()

    @classmethod
    def fromDirectory(cls, path: str, validate=True, method="copy", resolve_paths=True,
                      include_dirs=True, include_files=False) -> "Manifest":
        implied_manifest = dict()

        if os.path.isdir(path):
            for entry in os.listdir(path):
                relpath = os.path.join(path, entry)
                fullpath = os.path.normpath(os.path.abspath(relpath))
                if include_dirs and os.path.isdir(fullpath):
                    implied_manifest[entry] = ':'.join((fullpath if resolve_paths else relpath, method))
                elif include_files and os.path.isfile(fullpath):
                    implied_manifest[entry] = ':'.join((fullpath if resolve_paths else relpath, method))

        return cls(implied_manifest, validate)

    @classmethod
    def fromFile(cls, path: str, validate=True) -> "Manifest":
        try:
            manifest = yaml_load(open(path, 'r'))
        except FileNotFoundError as e:
            raise_with_traceback(experiment.model.errors.FlowIRManifestMissing(path))
        else:
            return cls(manifest, validate)


    @property
    def manifestData(self) -> DictManifest:
        """Returns a copy of the manifest dictionary"""
        return {x: self._manifest[x] for x in self._manifest}

    @property
    def top_level_folders(self):
        # type: () -> List[str]
        """A list of top level folders in the package/instance directory"""

        # VV: manifest can include keys which describe nested folders. Extract the left-most folders out of such keys
        return [x.split(os.path.pathsep, 1)[0] for x in self._manifest]

    def validate(self):
        """Validates contents of manifest dictionary

        Raises experiment.error.FlowIRManifestSyntaxException
        """
        for target in self._manifest:
            source = self._manifest[target]
            if os.path.isabs(target):
                raise experiment.model.errors.FlowIRManifestKeyIsAbsolutePath(target)
            try:
                _, method = source.rsplit(':', 1)
            except ValueError:
                method = 'copy'

            # VV: FIXME experiment.data.DataReference defines these but that file also imports "experiment.conf"
            if method not in ['copy', 'link']:
                raise experiment.model.errors.FlowIRManifestSourceInvalidReferenceMethod(target, source)

    def update(self, other: Union[DictManifest, "Manifest"]):
        """Updates the current manifest with another one

        Args:
            other: Contents of a manifest, or a Manifest object

        Returns:
            None
        """
        if isinstance(other, Manifest):
           other_manifest  = other.manifestData
        else:
            other_manifest = other.copy()

        shadow = set(other).intersection(other_manifest)

        if shadow:
            flowirLogger.warning(f"The manifest update shadows the manifest entries "
                                 f"{list(shadow)} - will assume that this is intentional")
        self._manifest.update({str(x): str(other_manifest[x]) for x in other_manifest})

    def clear(self):
        self._manifest.clear()


class FlowIR(object):
    NewestVersion = (0, 3, 0)

    _basic_types = string_types + (bool, int, float,)
    data_reference_methods = ['copy', 'link', 'ref', 'copyout', 'extract', 'output',
                              'loopref', 'loopoutput']  # type: List[str]

    # VV: Used to compute the default cpuUnitsPerCore instead of using a hard-coded value that's POWER specific
    _mtx_cpuUnitsPerCore = threading.RLock()
    _default_cpuUnitsPerCore = None

    Backends = [
        'simulator',
        'local',
        'lsf',
        'kubernetes',
    ]

    Interpreters = [
        'bash',
        'javascript',
        'cwl',
        'cwlcmdline',
    ]

    stageOptions = ['continue-on-error', 'stage-name']

    VariablePattern = r'%\([a-zA-Z0-9_-]+\)s'
    VariablePatternIncomplete = r'%\([a-zA-Z0-9_-]+\)(^s)?'

    (
        FieldVariables, FieldComponents, FieldEnvironments,
        FieldStatusReport, FieldApplicationDependencies, FieldOutput,
        FieldBlueprint, FieldPlatforms, FieldVirtualEnvironments,
        FieldVersion, FieldInterface
    ) = (
        'variables', 'components', 'environments',
        'status-report', 'application-dependencies', 'output',
        'blueprint', 'platforms', 'virtual-environments',
        'version', 'interface'
    )
    LabelGlobal, LabelDefault, LabelStages, LabelDoWhile, LabelWorkflow = (
        'global', 'default', 'stages', 'DoWhile', 'Workflow'
    )

    LabelKubernetesQosGuaranteed, LabelKubernetesQosBurstable, LabelKubernetesQosBestEffort = (
        'guaranteed', 'burstable', 'besteffort'
    )

    SpecialEnvironments = ['SANDBOX', 'ENVIRONMENT']

    SpecialFolders = ['input', 'data', 'bin', 'conf']

    DocumentTypes = [LabelDoWhile, LabelWorkflow]

    ExtractionMethodsInput = ["hookGetInputIds", "csvColumn"]
    ExtractionsMethodsProperty = ["hookGetProperties", "csvDataFrame"]

    # VV: Currently all extraction methods need a source
    ExtractionMethodsWhichDoNotNeedSource = []

    @classmethod
    def expand_potential_component_reference(
            cls, 
            ref, 
            stage_context, 
            known_components: Optional[Union[List[FlowIRComponentId], Dict[int, List[str]]]],
            top_level_folders, force_expand=False):
        """Expands a reference to its absolute string representation (stage%d.%s) where applicable

        This method can be used by other methods who have access to either (or both):
        a) the entire collection of component ids
        b) the folders in the root directory of an instance/package

        Args:
            ref(str): A producer reference to be expanded
            stage_context(int): Stage to evaluate the references under
            known_components: Collection of components, can either be a list of [stage index, component name] tuples or
                a List whose keys are stage indices and values lists of component names (str)
            top_level_folders(List[str]): List of names for top-level folders in package/instance
            force_expand(bool): Expand to absolute form regardless of whether there is enough information
                to support this decision (in the form of top_level_folders and known_components)

        Returns:
            str - a producer reference string representation may be the absolute flavour if can be expanded
        """
        stage_index, producer, filename, method = cls.ParseDataReferenceFull(ref, None)
        maybe_index = stage_index if stage_index is not None else stage_context
        references_component = force_expand

        if cls.is_var_reference(producer):
            # VV: Do not attempt to expand references whose producer is a variable reference - we assume that the
            # user has a dependency to some file/folder that is *not* produced by a component
            return ref

        direct_reference = top_level_folders is not None and (
                stage_index is None and producer in top_level_folders) or '/' in producer

        if top_level_folders and not direct_reference:
            references_component = True

        # VV: This *could* have been a weird corner case had we supported components which can have the same name
        # as top-level-folders (e.g. app-deps, special folders (bin, data, etc) as well as top-level-folders in the
        # package/instance directory - but we don't support that so it's easier to expand references
        if known_components is not None and producer in known_components.get(maybe_index, []):
            references_component = True

        if references_component:
            ref = cls.compile_reference(producer, filename, method, maybe_index)

        return ref

    @classmethod
    def expand_component_references(cls, references, stage_context, known_components,
                                    application_dependencies, top_level_folders):
        """Expands a list of references to their absolute string representation

        This method can be used by other methods who have access to at least one:
        a) the entire collection of component ids
        b) the folders in the root directory of an instance/package AND its application-dependencies

        Args:
            references(List[str]): component configuration dictionary
            stage_context(int): Stage to evaluate the references under
            known_components(Optional[Dict[int, str]]): (Optional) Dictionary of stage indices to component
                names in stage
            application_dependencies(Optional[List[str]]): List of application_dependencies paths
            top_level_folders(Optional[List[str]]): List of names for top-level folders in package/instance

        Returns:
            List[str] - A list of rewritten references so that they're fully expanded
        """
        if not references:
            return references

        app_dep_folders = [cls.application_dependency_to_name(x) for x in (application_dependencies or [])]
        top_level_folders = list(top_level_folders or []) + app_dep_folders + cls.SpecialFolders

        return [cls.expand_potential_component_reference(ref, stage_context, known_components, top_level_folders, False)
                for ref in references]


    @classmethod
    def default_cpuUnitsPerCore(cls):
        # type: (FlowIR) -> float
        """Computes the default value of resourceManager.kubernetes.cpuUnitsPerCore as the ratio of logical to physical
        cores.

        Instead of returning a hard-coded value of 8.0 (which is applicable to certain POWER cpus) it's best to compute
        the ratio of logical to physical cores and use that as the default cpuUnitsPerCore.

        From: https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-cpu
        "One cpu, in Kubernetes, is equivalent to 1 vCPU/Core for cloud providers and 1 hyperthread
        on bare-metal Intel processors."
        """
        with cls._mtx_cpuUnitsPerCore:
            if cls._default_cpuUnitsPerCore is None:
                try:
                    logical = psutil.cpu_count(logical=True)
                    physical = psutil.cpu_count(logical=False)
                    cls._default_cpuUnitsPerCore = logical / float(physical)
                except Exception as e:
                    # VV: when unable to discover defaultCpuUnitsPerCore return hard-coded value 8.0
                    cls._default_cpuUnitsPerCore = 8.0

            return cls._default_cpuUnitsPerCore

    @classmethod
    def compile_component_aggregate(cls, component, count, refs_to_replicate):
        # type: (DictFlowIRComponent, int, List[str]) -> DictFlowIRComponent
        translation_map = {}

        component = deep_copy(component)

        comp_stage = component.get('stage', None)

        for ref in refs_to_replicate:
            for replica in range(count):
                stage_index, producer, filename, method = cls.ParseDataReferenceFull(ref, comp_stage)
                rewritten = cls.compile_reference(
                    producer, filename, method, stage_index=stage_index, replica_id=replica
                )

                original_long = FlowIR.compile_reference(producer, filename, method, stage_index)
                original_short = FlowIR.compile_reference(producer, filename, method)

                for ref_str in [original_long, original_short]:
                    if ref_str not in translation_map:
                        translation_map[ref_str] = []

                    translation_map[ref_str].append(rewritten)

        def aggregate(string):
            # type: (str) -> str
            for ref in refs_to_replicate:
                # This re will find references followed by paths
                # If ref is followed by a path then we have to replicate the path everywhere
                # e.g. Component:ref/file.txt -> Component1:ref/file.txt Component2:ref/file.txt etc
                update_refs = [ref]
                stage_index, producer, filename, method = cls.ParseDataReferenceFull(ref, None)
                if stage_index is not None:
                    # VV: We want to add the ABSOLUTE reference second so that we do not end up with:
                    # stage<idx>.stage<idx>.<component name>
                    extra_ref = cls.compile_reference(
                        producer=producer, filename=filename, method=method)
                    update_refs.append(extra_ref)
                for ref in update_refs:
                    expression = re.compile(r"%s((?:/[\w.*]+)+,*)?" % ref)
                    orig_string = string
                    m = expression.search(string)
                    if m is not None:
                        # Check if we have a path after the reference
                        if m.group(1) is not None:
                            path = m.group(1)
                            # Now check if there is a comma at end of path - if there is join using a comma
                            separator = " "
    
                            # VV: FIXME What if someone uses this hack in the `references` field ?
                            if path[-1] == ",":
                                separator = ","
                                path = path[:-1]
    
                            replacement = ["%s%s" % (el, path) for el in translation_map[ref]]
                            replacement = separator.join(replacement)
                            string = expression.sub(replacement, string)
                        else:
                            string = string.replace(ref, " ".join(translation_map[ref]))
                        if string != orig_string:
                            # VV: if we replaced the Absolute ref we must skip replacing the relative ref becuase
                            # we'll end up with stage<idx>.stage<idx>.<component name>
                            break

            return string

        component = FlowIR.replace_strings(component, aggregate, in_place=True)

        # VV: The replacement happens at the string level which means that
        #     we have to split the `references` field again
        new_references = []
        references = component.get('references', [])

        for ref in references:
            new_references.extend(ref.split())

        component['references'] = new_references

        return component

    @classmethod
    def compile_component_replica(cls, component, replica, total_replicas, refs_to_replicate):
        # type: ( DictFlowIRComponent, int, int, List[str]) -> DictFlowIRComponent
        component = deep_copy(component)
        # VV: Patch backpatch FlowIR with `replica` variable, and `replicate` workflowAttributes field
        variables = component.get('variables', {})
        workflowAttributes = component.get('workflowAttributes', {})

        variables['replica'] = replica
        workflowAttributes['replicate'] = total_replicas

        component['variables'] = variables
        component['workflowAttributes'] = workflowAttributes

        component['name'] = '%s%d' % (component['name'], replica)

        translation = {}
        owner_stage = component.get('stage', 0)

        for original in refs_to_replicate:
            stage_index, producer, filename, method = cls.ParseDataReferenceFull(original, owner_stage)

            if stage_index is None:
                continue

            rewritten = cls.compile_reference(
                producer, filename, method, stage_index=stage_index, replica_id=replica
            )

            original_long = FlowIR.compile_reference(producer, filename, method, stage_index)
            original_short = FlowIR.compile_reference(producer, filename, method)

            translation[original_long] = rewritten
            translation[original_short] = rewritten

        # VV: Ensure that references are replaced from the longest one to the shortest one so that
        #     there is no way that a partial reference is replaced. This is probably overkill;
        #     references end with the `method` postfix (e.g. ':ref').
        sorted_translation = sorted(translation, key=lambda name: len(name), reverse=True)

        def translation_func(string):
            # type: (str) -> str
            for original in sorted_translation:
                string = string.replace(original, translation[original])

            return string

        component = cls.replace_strings(component, translation_func, in_place=True)

        return component

    @classmethod
    def apply_replicate(cls, flowir_components, platform_variables, ignore_missing_references,
                        application_dependencies, top_level_folders):
        # type: (List[DictFlowIRComponent], Dict[str, Any], bool, List[str], List[str]) -> List[DictFlowIRComponent]
        """Performs replication to a collection of components

        Arguments:
            flowir_components: Definitions of components
            platform_variables: Collection of variables grouped in stage-scopes
            ignore_missing_references: Set to True to avoid raising exception if any component references components
                not in the @flowir_components list
            application_dependencies: List of application_dependencies paths
            top_level_folders: List of names for top-level folders in package/instance

        Returns
            A list of replicated components
        """
        application_dependencies = application_dependencies or []
        top_level_folders = top_level_folders or []
        resolved_components = {}
        for comp in flowir_components:
            resolved = deep_copy(comp)
            stage_idx = comp.get('stage', 0)
            c_id = (stage_idx, comp['name'])

            stage_vars = deep_copy(platform_variables.get(cls.LabelStages, {}).get(stage_idx, {}))
            comp_vars = resolved.get('variables', {})

            # VV: Figure out the values of the variables that are accessible by the component and use said variables
            #     to resolve its workflowAttributes.replicate and workflowAttributes.aggregate
            visible_vars = deep_copy(platform_variables.get(cls.LabelGlobal, {}))
            visible_vars = cls.override_object(visible_vars, stage_vars)
            visible_vars = cls.override_object(visible_vars, comp_vars)
            ref = 'stage%d.%s' % c_id

            def to_bool(s):
                if isinstance(s, bool):
                    return s
                return {
                    'true': True,
                    'false': False,
                    'y': True,
                    'n': False,
                    'yes': True,
                    'no': False,
                }[s.lower()]

            for key, convert in [ ('replicate', int), ('aggregate', to_bool)]:
                label = '%s.workflowAttributes.%s' % (ref, key)

                try:
                    value = resolved['workflowAttributes'][key]
                    if value is None:
                        continue
                except KeyError:
                    pass
                else:
                    value = cls.fill_in(
                        value, visible_vars, label=label, is_primitive=True)
                    try:
                        value = convert(value)
                    except Exception:
                        raise ValueError("Failed to convert \"%s\" to appropriate type for %s" % (
                            value, key
                        ))

                    resolved['workflowAttributes'][key] = value

            resolved_components[c_id] = resolved

        replicate_instructions = cls.propagate_replicate(
            list(resolved_components.values()), ignore_missing_references,
            application_dependencies=application_dependencies, top_level_folders=top_level_folders)

        # VV: We now know which components need to be replicated and how many replicas to generate for them, as well as
        #     which components are aggregating their upstream replicas

        all_components = []  # type: List[DictFlowIRComponent]

        for comp in flowir_components:
            c_id = (comp.get('stage', 0), comp['name'])
            comp_ref = 'stage%d.%s' % c_id

            replicate, aggregate = replicate_instructions[c_id]

            if replicate is not None:
                if isinstance(replicate, int) is False:
                    raise ValueError("Replicate instructions for \"stage%d.%s\" was expected to be an integer or None, "
                                     "but was %s" % (c_id[0], c_id[1], replicate))
            else:
                replicate = 0

            # VV: Gather all references to replicated predecessors
            references = comp.get('references', [])
            replicated_refs = []

            for ref in references:
                stage_idx, producer, filename, method = FlowIR.ParseDataReferenceFull(
                    ref, c_id[0],application_dependencies=application_dependencies, special_folders=top_level_folders)

                if stage_idx is None:
                    # VV: This is not a reference to a component
                    continue
                ref_id = (stage_idx, producer)
                try:
                    ref_replicate, is_aggregate = replicate_instructions[ref_id]
                except KeyError:
                    if ignore_missing_references:
                        continue
                    msg = "Component \"%s\" references unknown \"stage%d.%s\"" % (
                        comp_ref, stage_idx, producer
                    )
                    flowirLogger.critical(msg)
                    raise ValueError(msg)

                if ref_replicate is not None and ref_replicate > 0 and is_aggregate is False:
                    # VV: Need to replicate/aggregate references in both Absolute and Relative format
                    # So here we place the ABSOLUTE reference and then inside component_aggregate we search for
                    # both ABSOLUTE and if we don't find that the RELATIVE reference
                    replicated_refs.append(
                        FlowIR.compile_reference(producer=producer, filename=filename, method=method,
                                                 stage_index=stage_idx),
                    )

            flowirLogger.info(f"Replicated refs are {replicated_refs}")

            if aggregate:
                flowirLogger.info("Aggregate job stage%s.%s" % (c_id[0], comp['name']))
                aggregate_comp = cls.compile_component_aggregate(comp, replicate, replicated_refs)
                all_components.append(aggregate_comp)
            elif replicate > 0:
                flowirLogger.info("Replicate job stage%s.%s (%s) (replicated refs: %s)" % (
                    c_id[0], comp['name'], replicate, replicated_refs
                ))
                for replica_idx in range(replicate):
                    comp_replica = cls.compile_component_replica(comp, replica_idx, replicate, replicated_refs)
                    all_components.append(comp_replica)
            else:
                all_components.append(comp)

        return all_components

    @classmethod
    def application_dependency_to_name(cls, app_dep_id):
        # type: (str) -> str
        """Extracts the name out of an application dependency

        Args:
            app_dep_id(str): Application dependency (i.e. "<name>", "<name>.<extension>", "/some/name", or
                "/some/path/<NAME>.<extension>")

        Returns
            Removes leading path to app-dep and trailing extension, then returns lowercase name: "<name>"
        """
        app_dep_id = app_dep_id.rstrip('/')

        if app_dep_id.startswith('/') is False:
            package_folder = app_dep_id
        else:
            package_folder = os.path.split(app_dep_id)[1]

        return os.path.splitext(package_folder)[0].lower()

    @classmethod
    def propagate_replicate(
            cls,  # type: FlowIR
            flowir_components,  # type: List[DictFlowIRComponent]
            ignore_missing_references,  # type: bool
            platform_variables=None,  # type: Dict[str, Any]
            application_dependencies=None,  # type: List[str]
            top_level_folders=None,  # type: List[str]
    ):
        # type: (...) -> Dict[FlowIRComponentId, Tuple[Union[None, str, int], bool]]
        """Applies replicate/aggregate rules to insert workflowAttributes.replicate values into component definitions.

        Returns a dictionary whose keys are component ids, tuples of (stage-index, component-name), and values are
        tuples of (propagated-replica, aggregate).

        Arguments:
            propagated-replica: This is either a variable reference, an integer literal, or None. The later indicates
                that the component is not to be replicated.
            aggregate: This is a boolean indicating whether the component is aggregating or not.
            ignore_missing_references: Whether to avoid raising an exception when a reference points to unknown
                component
            application_dependencies: Known application dependencies (FlowIR.LabelApplicationDependencies)
            top_level_folders: List of names for top-level folders in package/instance
        """
        # VV: Maps component-ids to a `returned` tuple (see doxygen of method)
        ret = {}  # type: Dict[FlowIRComponentId, Tuple[Union[None, str, int], bool]]

        # VV: First build a graph so that we visit the graphs in topological order, if this is a sub-graph we can
        #     tolerate missing references and assume that they are not points of replication

        g = networkx.DiGraph()
        known_ids = []

        # VV: For a set of components whose name matches <iteration-no:%d>#<component-name:str>[<replica:%d>] there
        #     exits a placeholder component
        placeholders = {}

        for comp in flowir_components:
            c_id = (comp.get('stage', 0), comp['name'])
            known_ids.append(c_id)
            g.add_node('stage%d.%s' % c_id, definition=comp, c_id=c_id)

            idx, name = c_id
            tokens = name.split('#', 1)
            if len(tokens) > 1:
                # VV: Just map the placeholder to any of the represented components, we just care for the
                # workflowAttributes.replicate/aggregate entries which are identical across all instances
                # of placeholder
                placeholders[(idx, tokens[1])] = c_id

        for comp in flowir_components:
            c_id = (comp.get('stage', 0), comp['name'])
            replicate = comp.get('workflowAttributes', {}).get('replicate', None)
            aggregate = comp.get('workflowAttributes', {}).get('aggregate', None)

            if platform_variables is not None \
                    and (isinstance(replicate, string_types) or isinstance(aggregate, string_types)):
                # VV: Figure out the values of the variables that are accessible by the component and use said variables
                #     to resolve its workflowAttributes.replicate and workflowAttributes.aggregate
                stage_vars = deep_copy(platform_variables.get(cls.LabelStages, {}).get(c_id[0], {}))
                comp_vars = deep_copy(comp.get('variables', {}))
                visible_vars = deep_copy(platform_variables.get(cls.LabelGlobal, {}))
                visible_vars = cls.override_object(visible_vars, stage_vars)
                visible_vars = cls.override_object(visible_vars, comp_vars)
                ref = 'stage%d.%s' % c_id

                if isinstance(replicate, string_types):
                    label = '%s.workflowAttributes.%s' % (ref, 'replicate')
                    replicate = cls.fill_in(replicate, visible_vars, label=label, is_primitive=True)

                if isinstance(aggregate, string_types):
                    label = '%s.workflowAttributes.%s' % (ref, 'aggregate')
                    aggregate = cls.fill_in(aggregate, visible_vars, label=label, is_primitive=True)

            ret[c_id] = (replicate, aggregate)

            c_ref = 'stage%d.%s' % c_id
            owner_stage = comp.get('stage', 0)

            for ref in comp.get('references', []):
                # VV: Could be referencing an application dependency folder
                stage_index, producer, filename, method = cls.ParseDataReferenceFull(
                    ref, owner_stage, application_dependencies=application_dependencies,
                    special_folders=top_level_folders)

                if stage_index is None:
                    # VV: This is not a reference to a component
                    continue

                p_ref = 'stage%d.%s' % (stage_index, producer)

                if g.has_node(p_ref) is False:
                    # VV: `p_ref` seems to be unknown, perhaps it's a reference to a placeholder node or
                    # to an application dependency

                    r_id = (stage_index, producer)

                    if r_id not in placeholders:
                        if ignore_missing_references:
                            continue
                        raise experiment.model.errors.FlowIRReferenceToUnknownComponent(
                            p_ref, c_id[1], c_id[0], known_ids)
                    else:
                        # VV: Rewrite predecessor so that it points to one of the placeholder instances, this is how we
                        #     access the workflowAttributes.replicate/aggregate values of the blueprint
                        p_ref = 'stage%d.%s' % placeholders[r_id]

                g.add_edge(p_ref, c_ref)

        for node_name in networkx.topological_sort(g):
            node = g.nodes[node_name]
            c_id = node['c_id']

            predecessors = [g.nodes[name] for name in g.predecessors(node_name)]
            predecessor_replicate = [ret[cd['c_id']][0]
                                     if str(ret[cd['c_id']][1]).lower() in ["false", "none"]
                                     else None for cd in predecessors]

            # VV: Append own information, keep all non-None entries, and throw away duplicates
            all_replicate = predecessor_replicate + [ret[c_id][0]]
            all_replicate = set([rep for rep in all_replicate if rep is not None])

            if len(all_replicate) > 1:
                raise ValueError("Replicate values of \"%s\"'s predecessors are not consistent: %s" % (
                    node_name, all_replicate))
            elif len(all_replicate) == 1:
                replicate = all_replicate.pop()

                try:
                    replicate = int(replicate)
                except ValueError:
                    pass

                ret[c_id] = (replicate, ret[c_id][1])
            else:
                ret[c_id] = (None, ret[c_id][1])

            # VV: Ensure that `replicate` is a boolean constant
            if str(ret[c_id][1]).lower() == "true":
                ret[c_id] = (ret[c_id][0], True)
            else:
                ret[c_id] = (ret[c_id][0], False)

        for p_id in placeholders:
            # VV: have the placeholders mirror the workflowAttributes.aggregate/replicate
            #     of the components that they represent
            ret[p_id] = ret[placeholders[p_id]]

        return ret

    @classmethod
    def discover_platforms(cls, flowir):
        # type: (DictFlowIR) -> List[str]
        known_platforms = [cls.LabelDefault]

        for key in [cls.FieldEnvironments, cls.FieldApplicationDependencies, cls.FieldVariables, cls.FieldBlueprint,
                    cls.FieldVirtualEnvironments]:
            known_platforms.extend(list(flowir.get(key, {}).keys()))

        known_platforms.extend(flowir.get(cls.FieldPlatforms, []))

        return sorted(set(known_platforms))

    @classmethod
    def from_dict(cls, flowir: "DictFlowIR") -> "FlowIR":
        """Instantiates a FlowIR out of a dictionary

        Args:
            flowir: A dictionary containing FlowIR

        Returns:
            A FlowIR instance

        Raises:
            experiment.model.errors.FlowIRNotDictionary:
                If @flowir is not a dictionary
        """
        ret = cls()

        if isinstance(flowir, dict) is False:
            raise experiment.model.errors.FlowIRNotDictionary(flowir)

        ret.flowir = deep_copy(flowir or {})

        # VV: Environment names are expected to be lowercase
        if cls.FieldEnvironments in ret.flowir:
            environments = ret.flowir[cls.FieldEnvironments]

            for platform in environments:
                platform_environments = environments[platform]

                for name in list(platform_environments.keys()):
                    if name != name.lower():
                        assert name.lower() not in platform_environments
                        platform_environments[name.lower()] = platform_environments[name]
                        del platform_environments[name]

        ret.flowir[FlowIR.FieldPlatforms] = cls.discover_platforms(ret.flowir)

        return ret

    def __init__(self):
        self.flowir = {}

    @classmethod
    def is_var_reference(cls, what):
        if isinstance(what, string_types) is False:
            return False

        variable_pattern = re.compile(FlowIR.VariablePattern)
        if variable_pattern.search(what):
            return True

        index_pattern = re.compile("""\[(\d+)\]""")

        return bool(index_pattern.search(what))

    @classmethod
    def is_type_or_var(
            cls,
            value,
            expected_types,
            can_be_none=False,
    ):
        variable_pattern = re.compile(FlowIR.VariablePattern)
        index_pattern = re.compile("""\[(\d+)\]""")

        is_type = (can_be_none and value is None) or isinstance(value, expected_types) or (
                isinstance(value, string_types) and variable_pattern.match(value) is not None
        )

        if is_type is False and isinstance(value, string_types):
            # VV: Check if this is a string representation of an array
            # an array has the suffix [<number>]
            return index_pattern.search(value)

        return is_type


    @classmethod
    def _schema_memory(cls, value):
        try:
            cls.memory_to_bytes(value)
        except:
            return cls.is_type_or_var(value, int, can_be_none=True)
        else:
            return True

    @classmethod
    def default_flowir_structure(cls):
        # type: () -> DictFlowIR

        return {
            cls.FieldInterface: None,
            cls.FieldBlueprint: {
                cls.LabelDefault: {}
            },
            cls.FieldStatusReport: {},
            cls.FieldApplicationDependencies: {},  # type: MutableMapping[Text, List[str]]
            cls.FieldOutput: {},
            cls.FieldPlatforms: [cls.LabelDefault],  # type: List[Text]
            cls.FieldComponents: [],  # type: List[DictFlowIRComponent]
            cls.FieldVariables: {
                cls.LabelDefault: {
                    cls.LabelGlobal: {},  # type: DictFlowIRVariableCollection
                    cls.LabelStages: {},  # type: MutableMapping[int, DictFlowIRVariableCollection]
                }
            },
            cls.FieldEnvironments: {
                cls.LabelDefault: {},  # type: DictFlowIRVariableCollection
            },
            cls.FieldVirtualEnvironments: {},
            cls.FieldVersion: ".".join((str(x) for x in cls.NewestVersion))
        }

    @classmethod
    def validate_flowir_version(cls, version):
        #type: (Text) -> Tuple[int, int, int]
        try:
            major, minor, revision = version.split('.')

            major = int(major)
            minor = int(minor)

            revision = int(revision)

            return major, minor, revision
        except Exception:
            raise ValueError('"%s" is an invalid version' % version)

    @classmethod
    def stage_identifier_to_stage_index(cls, key):
        # type: (Union[str, int]) -> int
        """Convert a stage identifier (e.g. for stage index N: 'N', 'stageN', or N) to a stage index (N)

        Arguments:
            key(Union[str, int]): Can be: a) an integer, b) "stage%d" string, or c) "%d" string

        Return an integer

        Raises experiment.model.errors.FlowIRInconsistency if key does not match expected format
        """
        if isinstance(key, int):
            return key
        elif isinstance(key, string_types):
            try:
                return int(key)
            except ValueError:
                if key.startswith('stage'):
                    try:
                        return int(key[5:])
                    except ValueError:
                        pass

        raise experiment.model.errors.FlowIRInconsistency("status-report key %s is invalid" % key, flowir=None, extra=key)

    @classmethod
    def type_flowir_interface_structure(cls) -> ValidateOptional:
        def generate_extraction_method_source(
                path=False,
                pathList=False,
                keyOutput=False
        ) -> Dict[ValidateOptional, ValidateOptional]:
            ret = {}
            if path:
                ret[ValidateOptional("path")] = ValidateOptional(string_types)
            
            if pathList:
                ret[ValidateOptional("pathList")] = ValidateOptional(ValidateMany(string_types))
            
            if keyOutput:
                ret[ValidateOptional("keyOutput")] = ValidateOptional(string_types)
            
            return ret
        
        def schema_hook_get_input_ids() -> Dict[Any, Any]:
            return {
                ValidateOptional("source"): generate_extraction_method_source(path=True, pathList=True),
            }
    
        def schema_csv_column() -> Dict[Any, Any]:
            return {
                ValidateOptional("source"): generate_extraction_method_source(path=True, pathList=True),
                ValidateOptional("args"): {
                    # VV: can have any arguments as long as their name is a string
                    string_types: (lambda x: True)
                }
            }

        def schema_hook_get_properties() -> Dict[Any, Any]:
            return {
                ValidateOptional("source"): generate_extraction_method_source(path=True, pathList=True, keyOutput=True),
            }

        input_spec = {
            "namingScheme": string_types,
            "inputExtractionMethod": {
                ValidateOptional("hookGetInputIds"): schema_hook_get_input_ids(),
                ValidateOptional("csvColumn"): schema_csv_column(),
            },
            ValidateOptional("hasAdditionalData"): bool,
        }

        def schema_csv_dataframe() -> Dict[Any, Any]:
            return {
                ValidateOptional("source"): generate_extraction_method_source(path=True, pathList=True, keyOutput=True),
                ValidateOptional("args"): {
                    # VV: can have any arguments as long as their name is a string
                    string_types: (lambda x: True),
                    ValidateOptional("columnNames"): {
                        string_types: string_types
                    }
                }
            }

        property_spec = {
            "name": string_types,
            ValidateOptional("description"): string_types,
            "propertyExtractionMethod": {
                ValidateOptional("hookGetProperties"): schema_hook_get_properties(),
                ValidateOptional("csvDataFrame"): schema_csv_dataframe(),
            },
        }

        interface = {
            ValidateOptional("id"): string_types,
            ValidateOptional("description"): string_types,
            "inputSpec": input_spec,
            "propertiesSpec": ValidateMany(property_spec),
            ValidateOptional("inputs"): ValidateOptional(ValidateMany(string_types)),
            ValidateOptional("additionalInputData"): ValidateOptional({
                # VV: <input id>: [additional input path 1, additional input path 2, ...]
                string_types: ValidateMany(string_types),
            }),
            ValidateOptional("outputFiles"): ValidateOptional(ValidateMany(string_types)),
        }
        return ValidateOptional(interface)

    @classmethod
    def type_flowir_structure(cls):
        def comp_to_label(comp, index):
            name = comp['name']
            stage_index = comp['stage']
            label = ''.join(['stage', Text(stage_index), '.', name])
            return label

        def is_valid_status_report_key(key):
            try:
                _ = cls.stage_identifier_to_stage_index(key)
                return True
            except Exception:
                return False

        variable_collection = {ValidateOptional(string_types): PrimitiveTypes}
        variables_type = {
            cls.LabelGlobal: variable_collection,
            cls.LabelStages: {ValidateOptional(int): variable_collection}
        }
        environment_collection = {ValidateOptional(string_types): variable_collection}
        blueprint = cls.type_flowir_component(flavor='blueprint')

        return {
            cls.FieldInterface: cls.type_flowir_interface_structure(),
            cls.FieldComponents: ValidateMany(cls.type_flowir_component(flavor='full'), comp_to_label),
            cls.FieldVariables: {
                cls.LabelDefault: variables_type,
                ValidateOptional(string_types): variables_type,
            },
            cls.FieldEnvironments: {
                cls.LabelDefault: environment_collection,
                ValidateOptional(string_types): environment_collection,
            },
            cls.FieldBlueprint: {
                ValidateOptional(string_types): {
                    ValidateOptional(cls.LabelGlobal): blueprint,
                    ValidateOptional(cls.LabelStages): {ValidateOptional(int): blueprint}
                }
            },
            cls.FieldStatusReport: {
                ValidateOptional(is_valid_status_report_key): {
                    ValidateOptional('stage-weight'): float,
                    ValidateOptional('executable'): string_types,
                    ValidateOptional('arguments'): string_types,
                    ValidateOptional('references'): ValidateMany(string_types),
                }
            },
            cls.FieldApplicationDependencies: {ValidateOptional(string_types): ValidateMany(string_types)},
            cls.FieldOutput: {
                ValidateOptional(string_types): {
                    ValidateOptional('description'): string_types,
                    ValidateOptional('stages'): ValidateMany(is_valid_status_report_key),
                    ValidateOptional('type'): string_types,
                    'data-in': cls.ParseDataReference,
                }
            },
            cls.FieldPlatforms: ValidateMany(string_types),
            cls.FieldVirtualEnvironments: {ValidateOptional(string_types): ValidateMany(string_types)},
            ValidateOptional(cls.FieldVersion): cls.validate_flowir_version,
        }

    @classmethod
    def default_component_structure(cls):
        return {
            'stage': 0,
            'references': [],
            'variables': {},
            'workflowAttributes': {
                # VV: Engine will try to use `restart.py` if restartHookFile is set to None
                'restartHookFile': None,
                'aggregate': False,
                'replicate': None,
                'isMigratable': False,
                'isMigrated': False,
                'repeatInterval': None,
                'repeatRetries': 3,
                'maxRestarts': None,
                'shutdownOn': [],
                'restartHookOn': [experiment.model.codes.exitReasons['ResourceExhausted']],
                'isRepeat': False,
                'memoization': {
                    'disable': {
                        'strong': False,
                        'fuzzy': False,
                    },
                    'embeddingFunction': None,
                },
                'optimizer': {
                    'disable': False,
                    'exploitChance': 0.9,
                    'exploitTarget': 0.75,
                    'exploitTargetLow': 0.25,
                    'exploitTargetHigh': 0.5
                }
            },
            'resourceManager': {
                'config': {
                    'backend': 'local',
                    'walltime': 60.0,
                },
                'lsf': {
                    'statusRequestInterval': 20,
                    'queue': 'normal',
                    'reservation': None,
                    'resourceString': None,
                    'dockerImage': None,
                    'dockerProfileApp': None,
                    'dockerOptions': None,
                },
                'kubernetes': {
                    'image': None,
                    'qos': None,
                    'image-pull-secret': None,
                    'namespace': 'default',
                    'api-key-var': None,
                    'host': 'http://localhost:8080',
                    'cpuUnitsPerCore': None,
                    'gracePeriod': None,
                    'podSpec': None,
                }
            },
            'resourceRequest': {
                'numberProcesses': 1,
                'numberThreads': 1,
                'ranksPerNode': 1,
                'threadsPerCore': 1,
                'memory': None,
                'gpus': None,
            },
            'executors': {
                'pre': [],
                'main': [],
                'post': [],
            },
            'command': {
                'executable': None,
                'arguments': '',
                'resolvePath': True,
                'expandArguments': 'double-quote',
                'interpreter': None,
            }
        }

    @classmethod
    def type_flowir_component_import(cls):
        ret = {
            'name': str,
            ValidateOptional('stage'): int,
            ValidateOptional('bindings'): {
                ValidateOptional(str): str
            },
            '$import': str,
        }

        return ret

    @classmethod
    def type_flowir_document_dowhile(cls):
        def binding_name(text):
            pattern = re.compile(r'^[a-zA-Z_]+[A-Za-z0-9_-]+$')
            match = pattern.match(text)
            ret = match is not None
            return ret

        def reference_method(text):
            return text in FlowIR.data_reference_methods

        ret = {
            'type': FlowIR.LabelDoWhile,
            ValidateOptional('inputBindings'): {
                ValidateOptional(binding_name): {
                    'type': reference_method
                }
            },

            ValidateOptional('loopBindings'): {
                ValidateOptional(str): str
            },

            'components': ValidateMany(cls.type_flowir_component(flavor='DoWhile')),
            'condition': lambda val: cls.ParseDataReferenceFull(val) is not None,

            # VV: This is part of the component that instantiates a DoWhile but a DoWhile document is automatically
            #     augmented to include 'bindings', this is also the case for 'name' and 'stage'
            ValidateOptional('bindings'): {
                ValidateOptional(str): str
            },
            'name': binding_name,
            ValidateOptional('stage'): int,
        }

        return ret

    @classmethod
    def _validate_restart_hook_file(cls, value):
        # type: (Optional[string_types]) -> bool
        """Validates whether the value of a workflowAttributes.restartHookFile field is valid

        Valid values are:
        1. strings NOT containing a file separator, OR
        2. None

        Returns True on valid @value, and False otherwise
        """
        if isinstance(value, string_types) is False and value is not None:
            return False
        if value and os.path.sep in value:
            return False
        return True

    @classmethod
    def type_flowir_component(cls, flavor):
        # type: (str) -> Dict[str, Any]
        """

        Args:
            flavor: can be ["full", "blueprint", "DoWhile"]. All flavors have the exact same keys but:
              full: requires all keys (with a few exceptions for fields which can be omitted)
              blueprint: forbids the use of `variables`, `name` and `stage` other keys are optional
              DoWhile: like `blueprint` but may also contain `variables`, `name, and `stage`

        Returns:

        """
        # VV: Blueprints and Components can have the stage/name options but the override field of the component
        #     cannot. Moreover, blueprints and override fields are all optional but that is not the same for
        #     all of the keys of a component.
        #     The easiest way to enforce these constraints while keeping the code size down is to,
        #     generate a function that is generating a `key` function
        def generate_key_function(is_blueprint):
            # VV: Global and Stages blueprints do not have any required keys
            def key(what, is_blueprint=is_blueprint):
                if is_blueprint is False or isinstance(key, ValidateOptional):
                    return what
                return ValidateOptional(what)

            return key

        def max_restarts_int(value):
            if isinstance(value, int):
                return value >= -1
            return False
        
        def is_dictionary_or_none(value):
            return value is None or isinstance(value, dict)

        def generate_blueprint(is_blueprint):
            key = generate_key_function(is_blueprint)
            dont_restart_on = (
                experiment.model.codes.exitReasons['Killed'], experiment.model.codes.exitReasons['Cancelled'])
            restart_hook_on = (x for x in experiment.model.codes.exitReasons if x not in dont_restart_on)
            ret = {
                key('references'): ValidateMany(string_types),
                key('workflowAttributes'): {
                    key('restartHookFile'): cls._validate_restart_hook_file,
                    key('aggregate'): bool,
                    key('replicate'): ValidateOr(int, None, cls.is_var_reference),
                    key('isMigratable'): ValidateOr(bool, cls.is_var_reference),
                    key('isMigrated'): bool,
                    key('repeatInterval'): ValidateOr(int, float, None, cls.is_var_reference),
                    key('shutdownOn'): ValidateMany(string_types),
                    key('restartHookOn'): ValidateMany(ValidateOr(cls.is_var_reference, *restart_hook_on)),
                    # VV: Make isRepeat optional because its value depends on `repeatInterval`
                    ValidateOptional('isRepeat'): bool,
                    key('maxRestarts'): ValidateOr(max_restarts_int, None, cls.is_var_reference),
                    key('repeatRetries'): ValidateOr(int, None, cls.is_var_reference),
                    key('memoization'): {
                        key('embeddingFunction'): ValidateOr(None, string_types),
                        key('disable'): {
                            key('strong'): ValidateOr(bool, cls.is_var_reference),
                            key('fuzzy'): ValidateOr(bool, cls.is_var_reference),
                        },
                    },
                    key('optimizer'): {
                        key('disable'): ValidateOr(bool, cls.is_var_reference),
                        key('exploitChance'): ValidateOr(float, cls.is_var_reference),
                        key('exploitTarget'): ValidateOr(float, cls.is_var_reference),
                        key('exploitTargetLow'): ValidateOr(float, cls.is_var_reference),
                        key('exploitTargetHigh'): ValidateOr(float, cls.is_var_reference),
                    }
                },
                key('resourceManager'): {
                    key('config'): {
                        key('backend'): string_types,
                        key('walltime'): ValidateOr(int, float, cls.is_var_reference),
                    },
                    key('lsf'): {
                        key('statusRequestInterval'): ValidateOr(int, float, cls.is_var_reference),
                        key('queue'): string_types,
                        key('reservation'): ValidateOr(string_types, None),
                        key('resourceString'): ValidateOr(string_types, None),
                        key('dockerImage'): ValidateOr(string_types, None),
                        key('dockerProfileApp'): ValidateOr(string_types, None),
                        key('dockerOptions'): ValidateOr(string_types, None),
                    },
                    key('kubernetes'): {
                        key('image'): ValidateOr(None, string_types),
                        key('qos'): ValidateOr(cls.is_var_reference, cls.str_to_kubernetes_qos),
                        key('image-pull-secret'): ValidateOr(None, string_types),
                        key('namespace'): ValidateOr(None, string_types),
                        key('api-key-var'): ValidateOr(None, string_types),
                        key('host'): ValidateOr(None, string_types),
                        key('cpuUnitsPerCore'): ValidateOr(int, float, cls.is_var_reference, None),
                        key('gracePeriod'): ValidateOr(None, int, cls.is_var_reference),
                        # VV: No validation for optional field podSpec other than it should be a dictionary or None
                        key('podSpec'): is_dictionary_or_none
                    }
                },
                key('resourceRequest'): {
                    key('numberProcesses'): ValidateOr(int, cls.is_var_reference),
                    key('numberThreads'): ValidateOr(int, cls.is_var_reference),
                    key('ranksPerNode'): ValidateOr(int, cls.is_var_reference),
                    key('threadsPerCore'): ValidateOr(int, cls.is_var_reference),
                    key('memory'): ValidateOr(int, None, cls._schema_memory, cls.is_var_reference),
                    key('gpus'): ValidateOr(None, int, cls.is_var_reference),
                },
                key('executors'): {
                    key('pre'): ValidateMany(
                        # VV: If a blueprint includes a command it should be complete
                        ValidateOr(
                            # VV: we currently support just 1 `pre` command but we can support more in the future
                            {
                                'name': 'lsf-dm-in',
                                'payload': string_types
                            },
                        )
                    ),
                    key('main'): [],
                    key('post'): ValidateMany(
                        ValidateOr(
                            # VV: we currently support just 1 `post` command but we can support more in the future
                            {
                                'name': 'lsf-dm-out',
                                'payload': string_types
                            },
                        )
                    )
                },
                key('command'): {
                    key('resolvePath'): ValidateOr(bool, cls.is_var_reference),
                    ValidateOptional('executable'): string_types,
                    ValidateOptional('arguments'): string_types,
                    ValidateOptional('environment'): ValidateOr(string_types, None),
                    ValidateOptional('interpreter'): ValidateOr(string_types, None),
                    # VV: @tag pyYAML-Convert-no-to-False
                    # VV: FIXME pyYAML converts 'no' to False (see https://stackoverflow.com/q/36463531) so I'm using
                    #     none instead. In the future it might be worth investing time to properly fix the yaml loader
                    ValidateOptional('expandArguments'): ValidateOr('double-quote', 'none', cls.is_var_reference),
                },
            }
            return ret

        if flavor not in ['blueprint', 'full', 'DoWhile']:
            raise ValueError("Flavor expected to be one of ['blueprint', 'full', 'DoWhile'] but its \"%s\"" % flavor)

        ret = generate_blueprint(flavor in ["blueprint", "DoWhile"])

        key = generate_key_function(flavor in ["blueprint", "DoWhile"])
        ret.update({
            key('name'): string_types,
            # VV: When stage is omitted it's assumed to be 0
            key('stage'): ValidateOptional(int),
        })

        if flavor != "blueprint":
            ret[ValidateOptional('variables')] = {ValidateOptional(string_types): ValidateOr(*PrimitiveTypes)}

            # VV: Components may override any of their options/variables based on the used platform except for
            # stage and name
            component_blueprint = generate_blueprint(is_blueprint=True)
            component_blueprint[ValidateOptional('variables')] = {
                ValidateOptional(string_types): ValidateOr(*PrimitiveTypes)
            }

            ret[ValidateOptional('override')] = {
                ValidateOptional(str): component_blueprint
            }

        return ret

    @classmethod
    def discover_indirect_dependencies_to_variables(cls, text, context, out_missing_variables=None):
        # type: (str, Dict[str, str], Optional[List[str]]) -> List[str]

        out_missing_variables = out_missing_variables if out_missing_variables is not None else []
        ret = []  # type: List[str]

        pattern = re.compile(cls.VariablePattern)

        unprocessed = [text]

        while unprocessed:
            part = unprocessed.pop()

            referenced_variables = [s.group()[2:-2] for s in pattern.finditer(part)]

            for var_name in referenced_variables:
                if var_name not in context:
                    if var_name not in out_missing_variables:
                        out_missing_variables.append(var_name)
                else:
                    ret.append(var_name)
                    unprocessed.append(context[var_name])

        return ret


    @classmethod
    def extract_id_of_folder(cls, folder):
        return os.path.split(folder)[1].lower()

    @classmethod
    def get_interpreter_executables(cls):
        interpreters = {
            # VV: map interpreter names to executables
            'bash': None,
            'javascript': 'python -m experiment.interpreter_js',
            'cwl': 'python -m experiment.interpreter_cwl',
            'cwlcmdline': 'python -m experiment.interpreter_cwl_cmdline',
        }

        return interpreters

    @classmethod
    def discover_references_to_variables(cls, obj):
        # type: (Any) -> List[str]
        remaining = [obj]
        result = set()
        pattern = re.compile(FlowIR.VariablePattern)

        while remaining:
            to_scan = remaining.pop()

            if isinstance(to_scan, dict):
                for key in to_scan:
                    remaining.append(to_scan[key])
            elif isinstance(to_scan, string_types):
                result.update([e.group()[2:-2] for e in pattern.finditer(to_scan)])
            elif isinstance(to_scan, (list, set, tuple)):
                remaining.extend(to_scan)

        return sorted(result)

    @classmethod
    def discover_placeholder_identifiers(
            cls,
            component_ids: Optional[Union[List[FlowIRComponentId], Dict[int, List[str]]]]) -> List[FlowIRComponentId]:
        """Builds a list of Compoent placeholders from a collection of component identifiers that may contain looping
        containers.

        A component placeholder is one that doesn't physically exist in the Workflow Graph. It is dynamically updated
        to point to the last instance of a looped component after the iteration that the looped component is in terminates.

        Args:
            component_ids: Dictionary whose keys are stages and values lists of component name. If Collection is
                already in list form this method just echoes the same list

        Returns:
            List of [stage index, component name] tuples of Component placeholders
        """
        component_ids = cls.aggregate_identifiers_to_list(component_ids)
        looped_components = [comp_id for comp_id in component_ids if '#' in comp_id[1]]
        # VV: Remove `<iteration-number:%d>#` prefix from names of looped-components
        placeholders = set([(comp_id[0], comp_id[1].split('#', 1)[1]) for comp_id in looped_components])

        return placeholders

    @classmethod
    def aggregate_identifiers_to_list(
            cls,
            component_ids: Optional[Union[List[FlowIRComponentId], Dict[int, List[str]]]]) -> List[FlowIRComponentId]:
        """Generates a list of component identifiers out of a dictionary {<stage: index>: [list of component names]}

        Args:
            component_ids: Dictionary whose keys are stages and values lists of component name. If Collection is
                already in list form this method just echoes the same list

        Returns:
            List of [stage index, component name] tuples
        """
        if component_ids is None:
            component_ids = component_ids or []

        if isinstance(component_ids, dict) is True:
            organized: List[FlowIRComponentId] = []
            for stage in component_ids:
                for name in component_ids[stage]:
                    organized.append((stage, name))

            return organized
        else:
            return component_ids

    @classmethod
    def organize_identifiers_to_stages(
            cls,
            component_ids: Optional[Union[List[FlowIRComponentId], Dict[int, List[str]]]],) -> Dict[int, List[str]]:
        """Organizes a collection of component identifiers to {<stage: index>: [list of component names]}

        Args:
            component_ids: Collection of tuples in the format [stage index, component name]. If Collection is
                already in Dictionary form this method just echoes the same dictionary.

        Returns:
            Dictionary in the format: {<stage: index>: [list of component names]}
        """
        if component_ids is None:
            component_ids = component_ids or []

        if isinstance(component_ids, dict) is False:
            organized = {}  # type: Dict[int, List[str]]
            component_ids = component_ids or []

            for stage, name in component_ids:
                if stage not in organized:
                    organized[stage] = []

                organized[stage].append(name)
            return organized
        else:
            return component_ids

    @classmethod
    def discover_reference_strings(
            cls,
            text: str,
            implied_stage: Optional[int],
            component_ids: Optional[Union[List[FlowIRComponentId], Dict[int, List[str]]]],
            out_map: Optional[Dict[str, str]] = None) -> List[str]:
        """Discovers all references to components in a string
        
        Args:
            text: String to detect references in
            implied_stage: Implied stage for references in "relative-representation" (e.g. hello:ref instead of 
                stage42.hello:ref)
            component_ids: Collection of components, can either be a list of [stage index, component name] tuples or
                a List whose keys are stage indices and values lists of component names (str)
            out_map: If this value is not None, it is expected to be a Dictionary which this method will first clear
                then populate with a mapping of detected references ot the absolute representations of the detected
                references.

        Returns:
            A list of detected references
        """
        known_components = cls.organize_identifiers_to_stages(component_ids)

        method_pattern = '|'.join(cls.data_reference_methods)

        # VV: Variable references or paths that contain numbers, letters, `_`, `-`, `.` followed by a reference method
        pattern = re.compile(r"([.a-zA-Z0-9_/-]|%s)+:(%s)" % (FlowIR.VariablePattern, method_pattern))

        # pattern = re.compile("[^ ]+:(%s)" % method_pattern)
        remaining = text

        all_refs = [s.group() for s in pattern.finditer(remaining)]

        def to_absolute_reference(ref):
            return cls.expand_potential_component_reference(ref, implied_stage, known_components, None, False)

        mapping = {
            ref: to_absolute_reference(ref) for ref in all_refs
        }

        if out_map is not None:
            out_map.clear()
            out_map.update(mapping)

        return sorted(mapping.values())

    @classmethod
    def filter_out_deprecated_component_fields(cls, component):
        # type: (DictFlowIRComponent) -> None
        """Filters deprecated fields of FlowIR in-place."""
        references = component.get('references')

        if references:
            remove_references = []
            for ref in references:
                _, _, method = cls.ParseDataReference(ref)

                if method.lower() in ['comp', 'component']:
                    remove_references.append(ref)

            if remove_references:
                component['references'] = [
                    ref for ref in references if ref not in remove_references
                ]

    @classmethod
    def _validate_interface(
            cls,
            interface: Dict[str, Any],
            label_interface: str,
            output_names: List[str],
    ) -> List[Exception]:
        errors = []

        if isinstance(interface, dict) is False:
            return []

        def validate_extraction_method(
                parent: Dict[str, Any],
                root_label: str,
                method_type: str,
                errors: List[Exception] = errors
        ):
            if method_type not in parent:
                return

            prop_extract_method = parent[method_type]
            if isinstance(prop_extract_method, dict) is False:
                return

            # VV: There should be exactly 1
            methods = {x: prop_extract_method[x] for x in prop_extract_method if prop_extract_method[x] is not None}

            if len(methods) != 1:
                errors.append(
                    experiment.model.errors.FlowIRSyntaxException(
                        f"{root_label} does not contain a single extraction method definition. it contains {methods}")
                )
                return

            method_name, configuration = methods.popitem()

            if isinstance(configuration, dict) is False:
                return

            if 'source' not in configuration:
                if method_name not in cls.ExtractionMethodsWhichDoNotNeedSource:
                    errors.append(experiment.model.errors.FlowIRKeyMissing(
                        key_name=f"{root_label}.{method_type}.{method_name}.source"))
                return

            source = configuration['source']
            if isinstance(source, dict) is False:
                return

            # VV: here, we can only check for the validity of the optional Key `keyOutput`
            output = source.get('keyOutput')
            if output is None:
                return

            # VV: Check whether dev selected a valid output name or not
            if output not in output_names:
                possibilities = difflib.get_close_matches(output, output_names)
                if len(possibilities):
                    most_appropriate = possibilities[0]
                else:
                    most_appropriate = None

                key_name = f"{root_label}.{method_type}.{method_name}.source.keyOutput"
                errors.append(experiment.model.errors.FlowIRValueInvalid(
                    key_name, output, output_names, most_appropriate))

        try:
            # VV: There is no valid interface definition
            if interface is None or isinstance(interface, dict) is False:
                return errors

            # VV: This is a required key, but may point to {}}
            input_spec = interface.get('inputSpec', {})
            if input_spec:
                validate_extraction_method(
                    input_spec, f"{label_interface}.inputSpec", "inputExtractionMethod", errors)

            duplicates = set()
            processed = set()

            # VV: Required key, but may be empty
            properties_spec = interface.get('propertiesSpec', [])
            for idx, property in enumerate(properties_spec):
                if isinstance(property, dict) is False:
                    continue

                validate_extraction_method(
                    property, f"{label_interface}.propertiesSpec[{idx}]", "propertyExtractionMethod", errors)

                if 'name' in property:
                    if property['name'] in processed:
                        duplicates.add(property['name'])
                    processed.add(property['name'])

            if duplicates:
                errors.append(experiment.model.errors.FlowIRSyntaxException(
                    f"Multiple propertiesSpec entries in {label_interface} for each of properties {list(duplicates)}"))

        except Exception as e:
            flowirLogger.info(f"Could not validate FlowIR because of {e}. Traceback: {traceback.format_exc()}")
            errors.append(experiment.model.errors.FlowIRConfigurationErrors([e], None))

        return errors

    @classmethod
    def validate(cls, flowir, documents):
        # type: (Union[FlowIR, DictFlowIR], Dict[str, Dict[str, Dict[str, Any]]]) -> List[Exception]
        if isinstance(flowir, FlowIR):
            flowir = deep_copy(flowir.flowir)
        elif isinstance(flowir, dict) is False:
            raise experiment.model.errors.FlowIRInconsistency("FlowIR is expected to be "
                                                        "a dictionary, not %s" % type(flowir), flowir)

        type_flowir = cls.type_flowir_structure()

        # VV: Validate each component on its own because there could be document creator
        #     components (e.g those which instantiate DoWhile). This kind of documents
        #     have a different structure compared to Components which execute code.
        components = flowir['components']
        flowir['components'] = []

        errors = validate_object_schema(flowir, type_flowir, 'FlowIR')

        errors_is_list = validate_object_schema(components, ValidateMany(dict), 'FlowIR.components')
        errors.extend(errors_is_list)

        if errors_is_list:
            type_component = cls.type_flowir_component(flavor='blueprint')
            type_component_import = cls.type_flowir_component_import()

            for i, component in enumerate(components):
                if '$import' not in component:
                    schema = type_component
                else:
                    schema = type_component_import

                try:
                    errors.extend(validate_object_schema(
                        component, schema, 'FlowIR.components[%d:stage%s.%s]' % (
                            i, component.get('stage', 0), component.get('name', '*unknown*'))))
                except Exception as e:
                    errors.append(e)

        for doc_type in documents:
            if doc_type == FlowIR.LabelDoWhile:
                schema = FlowIR.type_flowir_document_dowhile()
            else:
                errors.append(experiment.model.errors.FlowIRInvalidDocumentType(doc_type, FlowIR.DocumentTypes))
                continue

            for doc_name in documents[doc_type]:
                label = 'Document[%s].%s' % (doc_type, doc_name)
                doc = documents[doc_type][doc_name]
                errors.extend(validate_object_schema(doc, schema, root_label=label))

        for err in errors:
            flowirLogger.log(19, 'Validator error: %s' % Text(err))

        # VV: Finally, look for incomplete variables substitutions
        pattern_incomplete = re.compile(FlowIR.VariablePatternIncomplete)

        def check_for_incomplete(input_str, label):
            incomplete_matches = [x.group() for x in pattern_incomplete.finditer(input_str)
                                  if x.end() == len(input_str) or input_str[x.end()] != 's']
            if incomplete_matches:
                errors.append(experiment.model.errors.FlowIRVariablesIncomplete(incomplete_matches, label))
        try:
            FlowIR.visit_all(flowir, check_for_incomplete)
        except Exception as e:
            errors.append(e)

        try:
            for idx, comp in enumerate(components):
                try:
                    if comp.get('name'):
                        label = 'components.stage%s.%s' % (comp.get('stage', '0'), comp.get('name'))
                    else:
                        label = 'components[%d]' % idx
                    FlowIR.visit_all(comp, check_for_incomplete, label)
                except Exception as e:
                    errors.append(e)
        except Exception as e:
            errors.append(e)

        # VV: Finally validate the interface, above we looked at the "syntax" i.e. are we are using the correct words
        # below, we will check if the words make sense (grammar check?)

        try:
            output_names = [name for name in flowir.get(cls.FieldOutput, {})]
            interface = flowir.get(cls.FieldInterface, {})
            errors.extend(cls._validate_interface(interface, "FlowIR.interface", output_names))
        except Exception as e:
            errors.append(e)

        return errors

    @classmethod
    def validate_component(
            cls,  # type: FlowIR
            component: DictFlowIRComponent,
            comp_schema: Optional[Dict[Any, Any]] = None,
            component_ids: Optional[Union[List[FlowIRComponentId], Dict[int, List[str]]]] = None,
            known_platforms: Optional[List[str]] = None,
            top_level_folders: Optional[List[str]] = None,
    ) -> List[Exception]:
        """

        Args:
            component: FlowIR definition of component to validate
            comp_schema: Schema to compare this component against, defaults to type_flowir_component('flavor'='full')
            component_ids: Collection of component ids, either a list of [stage index, name] tuples or a dictionary
                whose keys are stage indices and values Lists of component names.
            known_platforms: List of known FlowIR platforms
            top_level_folders: List of top-level-folders in workflow root directory

        Returns:
            A list of Exceptions identifying issues with the Component definition
        """
        known_platforms = known_platforms or []

        component_name = component.get('name', '*Unknown*')
        stage_index = component.get('stage', 0)

        comp_name = ''.join(['stage', str(stage_index), '.', str(component_name)])

        errors = []

        if isinstance(component, dict) is False:
            errors.append(experiment.model.errors.FlowIRValueInvalid('Illegal component', component, [str(dict)]))
            return errors

        if '$import' not in component:
            try:
                schema = comp_schema or cls.type_flowir_component(flavor='full')
                errors.extend(validate_object_schema(component, schema, comp_name))
            except Exception as e:
                flowirLogger.log(15, traceback.format_exc())
                flowirLogger.info("Unable to validate component %s due to %s - will report component is invalid" % (
                    comp_name, e))
                errors.append(e)
                return errors
        else:
            try:
                errors.extend(validate_object_schema(component, cls.type_flowir_component_import(), comp_name))
            except Exception as e:
                errors.append(e)

        if component_name in cls.SpecialFolders:
            e = experiment.model.errors.DictFlowIRComponent({}, {}, {'name': (component_name, '')},
                                                      stage_index, "Components cannot be named %s "
                                                                   "(reserved keywords)." % cls.SpecialFolders)
            errors.append(e)

        # VV: There's nothing more to check for document creator components
        if '$import' in component:
            return errors

        # VV: Need more checks ?
        if 'backend' in component.get('resourceManager', {}).get('config', {}):
            backend = component['resourceManager']['config']['backend']

            if backend not in cls.Backends and cls.is_var_reference(backend) is False:
                invalid = {}
                invalid['resourceManager.config.backend'] = (backend, cls.Backends)
                errors.append(experiment.model.errors.FlowIRInvalidComponent(
                    {}, {}, invalid, component_name, stage_index
                ))

        if component_ids is not None:
            # VV: If flowir is supplied then we can run more checks, i.e. look for references to non-existing
            #     components
            my_name = component.get('name', '?')
            my_stage = component.get('stage', 0)

            component_ids = cls.organize_identifiers_to_stages(component_ids)
            folders = list(top_level_folders) + cls.SpecialFolders
            references = [cls.expand_potential_component_reference(r, my_stage, component_ids, folders)
                          for r in component.get('references', [])]

            flowirLogger.log(12, "Inspecting references of stage%s.%s references: %s" % (
                my_stage, my_name, pprint.pformat(references)))

            arguments = component.get('command', {}).get('arguments', '')
            # VV: Extract absolute-representation of references in arguments and compare them with
            # list of absolute references in component definition
            arg_refs = dict()
            _ = cls.discover_reference_strings(arguments, my_stage, component_ids, arg_refs)

            comp_ids_collection = cls.aggregate_identifiers_to_list(component_ids)

            for actual_ref in arg_refs:
                if arg_refs[actual_ref] not in references:
                    if '#' not in my_name:
                        # VV: If this is not a looping component then this is definitely a problem
                        errors.append((experiment.model.errors.FlowIRUnknownReferenceInArguments(
                            actual_ref, references, my_name, my_stage, comp_ids_collection,
                            top_level_folders)))
                    else:
                        # VV TODO if this is a looping component then it might be referencing a Placeholder node
                        # we'll eventually rewrite DoWhiles  so this check is safe to leave un-implemented for now.
                        # We could use something like cls.discover_placeholder_identifiers(comp_ids_collection) here
                        pass

            comps_missing = cls.validate_references(references, comp_ids_collection, my_stage, top_level_folders)
            for cmissing in comps_missing:
                errors.append(experiment.model.errors.FlowIRReferenceToUnknownComponent(
                    cmissing, component_name, stage_index, comp_ids_collection))

        overriding_platforms = component.get('override', {})
        unknown_platforms = set(overriding_platforms) - set(known_platforms)
        for p in unknown_platforms:
            errors.append(experiment.model.errors.FlowIRPlatformUnknown(p, {cls.FieldPlatforms: known_platforms}))

        return errors

    @classmethod
    def pretty_flowir_component_soft(cls, component):
        # type: (DictFlowIRComponent) -> DictFlowIRComponent
        order_component_keys = [
            'stage', 'name', 'command', 'references', 'workflowAttributes', 'executors', 'resourceManager',
            'resourceRequest', 'variables', 'override',
        ]

        pretty_comp = collections.OrderedDict()

        for key in order_component_keys:
            if key in component:
                pretty_comp[key] = component[key]

        if 'override' in pretty_comp:
            old_override = pretty_comp['override']
            del pretty_comp['override']
            pretty_comp['override'] = {}
            # VV: Prettify the override dictionaries, and also sort them based on the platform name
            for key in sorted(old_override):
                # VV: Recursion is safe for properly defined FlowIR; if a component is invalid we got bigger problems
                pretty_comp['override'][key] = cls.pretty_flowir_component_soft(old_override[key])

        for key in component:
            if key not in pretty_comp:
                pretty_comp[key] = component[key]

        return pretty_comp

    @classmethod
    def pretty_flowir_sort(cls, flowir):
        """Sort main and component-level keys in FlowIR description
        """
        order_flowir_keys = [
            cls.FieldInterface,
            cls.FieldApplicationDependencies, cls.FieldVirtualEnvironments, cls.FieldStatusReport,
            cls.FieldOutput, cls.FieldPlatforms, cls.FieldEnvironments,
            cls.FieldBlueprint, cls.FieldComponents, cls.FieldVariables,
            cls.FieldVersion,
        ]

        ret = collections.OrderedDict()

        for key in order_flowir_keys:
            if key in flowir:
                ret[key] = flowir[key]

        for key in flowir:
            if key not in ret:
                ret[key] = flowir[key]

        if cls.FieldComponents in ret:
            pretty_components = []

            for comp in ret[cls.FieldComponents]:
                pretty_components.append(cls.pretty_flowir_component_soft(comp))
            ret[cls.FieldComponents] = pretty_components

        return ret

    @classmethod
    def compress_flowir(cls, flowir, allow_recursion=False):
        # type: (DictFlowIR, bool) -> DictFlowIR
        """Compresses all but the FieldEnvironments` fields in a FlowIR description
        """
        flowir = deep_copy(flowir)
        environments = deep_copy(flowir.get(cls.FieldEnvironments, None))
        cls.compress_object(flowir, allow_recursion)
        if environments is not None:
            flowir[cls.FieldEnvironments] = environments

        return flowir

    @classmethod
    def compress_object(cls, which, allow_recursion=True):
        remaining = [('expand', which, lambda: None)]

        def dict_remove(parent, child_key):
            def remove():
                del parent[child_key]

            return remove

        def iter_remove(parent, child_obj):
            def remove():
                parent.remove(child_obj)

            return remove

        processed = set()

        while remaining:
            cmd, obj, to_remove = remaining.pop(0)

            if isinstance(obj, PrimitiveTypes):
                continue

            if cmd == 'expand':
                id_obj = id(obj)
                if id_obj in processed:
                    if allow_recursion is False:
                        raise RuntimeError("Object %s is used recursively" % obj)
                    continue

                if is_primitive_or_empty(obj) is False:
                    processed.add(id_obj)

            if isinstance(obj, dict):
                obj = cast(dict, obj)

                if len(obj) == 0:
                    to_remove()
                    continue
                if cmd == 'attempt_delete':
                    continue
                elif cmd == 'expand':
                    for key in obj:
                        entry = ('expand', obj[key], dict_remove(obj, key))
                        # VV: Insert at the top of the list so that children are processed before parent
                        remaining.insert(0, entry)
                    entry = ('attempt_delete', obj, to_remove)

                    # VV: Process parent right after its children
                    remaining.insert(len(obj), entry)
                else:
                    raise NotImplementedError("Cannot handle %s for dict %s" % (
                        cmd, obj
                    ))
            elif isinstance(obj, (list, set)):
                if len(obj) == 0:
                    to_remove()
                    continue
                if cmd == 'attempt_delete':
                    continue
                elif cmd == 'expand':
                    for child in obj:
                        entry = ('expand', child, iter_remove(obj, child))
                        remaining.insert(0, entry)

                    entry = ('attempt_delete', obj, to_remove)
                    remaining.insert(len(obj), entry)
                else:
                    raise NotImplementedError("Cannot handle %s for iterable %s" % (
                        cmd, obj
                    ))

        return which

    @classmethod
    def validate_references(cls, references: List[str],
                            component_ids: List[FlowIRComponentId],
                            implied_stage: Optional[int] = None,
                            top_level_folders: Optional[List[str]] = None) -> List[str]:
        """Validates references against known component identifiers, and top-level folders in the workflow

        Args:
            references: List of references to check
            component_ids: Collection of components, can either be a list of [stage index, component name] tuples or
                a List whose keys are stage indices and values lists of component names (str)
            implied_stage: implied_stage: Implied stage for references in "relative-representation" (e.g. hello:ref instead of 
                stage42.hello:ref)
            top_level_folders: List of top-level-folders in workflow root directory

        Returns:
            A list of unknown component absolute-identifiers that the references point to
        """

        component_ids = cls.aggregate_identifiers_to_list(component_ids)

        refs_to_components = {}
        refs_to_non_components = []

        for ref in references:
            stageIndex, jobName, filename, method = cls.ParseDataReferenceFull(
                ref, implied_stage, None, top_level_folders)

            if stageIndex is not None:
                refs_to_components[(stageIndex, jobName)] = ref
            else:
                refs_to_non_components.append(ref)

        comps_found = {comp_id: False for comp_id in refs_to_components}

        # VV: For a set of components whose name matches <iteration-no:%d>#<component-name:str>[<replica:%d>] there
        #     exits a placeholder component
        placeholders = set()
        for comp_id in component_ids:
            idx, name = comp_id
            tokens = name.split('#', 1)
            if len(tokens) > 1:
                placeholders.add((idx, tokens[1]))

        for comp_id in comps_found:
            if comp_id in component_ids:
                comps_found[comp_id] = True
            elif comp_id in placeholders:
                comps_found[comp_id] = True
                flowirLogger.log(14, "Referenced component stage%d.%s is mapped to a placeholder" % (
                    comp_id[0], comp_id[1]
                ))

        comps_missing = set([k for k in ['stage%s.%s' % key
                                         if comps_found[key] is False
                                         else None for key in comps_found]
                             if k is not None])

        if comps_missing:
            # VV: There're still some dependencies to components which appear to be invalid. Check if they
            #     are pointing to a Symbolic dependency (for example an out-of-loop component reference a looped one)
            placeholders = cls.discover_placeholder_identifiers(component_ids)

            for comp_id in comps_found:
                if comps_found[comp_id] is True:
                    continue

                if comp_id in placeholders:
                    comp_ref = 'stage%s.%s' % comp_id
                    try:
                        comps_missing.remove(comp_ref)
                    except KeyError:
                        pass
                    else:
                        flowirLogger.log(15, "Missing reference to %s is matched by symbolic dependency" % comp_ref)

        return sorted(comps_missing)

    @classmethod
    def suggest_alternative(cls, value, valid_values, option_name, componentName, defaults, errors, cutoff=0.8):
        # type: (str, List[str], str, str, Dict[str, str], List[str], float) -> None
        # VV: Validates an option against a list of valid_values
        possibilities = difflib.get_close_matches(value, valid_values, cutoff=cutoff)
        if len(possibilities):
            if option_name not in defaults:
                errors.append('%s: unknown %s %s. Did you mean: %s?' % (
                    componentName, option_name, value, possibilities[0]
                ))
            else:
                errors.append(
                    '%s: unknown %s %s (inherited from defaults). Did you mean: %s?' % (
                        componentName, option_name, value, possibilities[0]
                    ))
        else:
            errors.append('%s: unknown %s %s. No close match found' % (
                componentName, option_name, value
            ))

    @classmethod
    def discover_typos(cls, options, valid_keywords, cutoff=0.8):
        # type: (Dict[str, str], List[str], float) -> List[Tuple[str, str]]
        # VV: Validates an option against a list of valid_values
        potential_typos = []  # type: List[Tuple[str, str]]

        for keyword in valid_keywords:
            possibilities = difflib.get_close_matches(keyword, options, cutoff=cutoff)
            if len(possibilities):
                potential_typos.append((possibilities[0], keyword))

        return potential_typos

    @classmethod
    def     compile_reference(cls, producer, filename, method, stage_index=None, replica_id=None):
        # type: (str, str, str, Optional[int], Optional[int]) -> str
        '''Returns a data reference string given components.

        Note: This function does not create a producer reference string.
        This must have been created previously.

        Parameters:
            producer: The producer reference
            filename: The file under the producer if any
            method: The reference method/type
            stage_index: the stage index
        '''

        if replica_id is not None:
            producer = '%s%d' % (producer, replica_id)

        if filename is None:
            retval = "%s:%s" % (producer, method)
        else:
            retval = "%s/%s:%s" % (producer, filename, method)

        if stage_index is not None:
            retval = 'stage%d.%s' % (stage_index, retval)

        return retval

    @classmethod
    def CreateNode(
            cls,  # type: FlowIR
            flowir_description,  # type: DictFlowIRComponent
            g,  # type: networkx.DiGraph
            reference,  # type: str
            index,  # type: int
            isReplicationPoint,  # type: bool
            isBlueprint,  # type: bool
            isAggregate  # type: bool
    ):

        # Make sure the id of the node is a fully resolved ComponentIdentifier
        cid = ComponentIdentifier(reference, index)
        # Function so the value of reference is preserved
        g.add_node(cid.identifier,
                   flowir=flowir_description,
                   stageIndex=index,
                   isReplicationPoint=isReplicationPoint,
                   isBlueprint=isBlueprint,
                   isAggregate=isAggregate,
                   level=index,
                   rank=index)

    @classmethod
    def ParseDataReference(cls, value):
        # type: (str) -> Tuple[str, Optional[str], str]
        '''Breaks the data reference into parts

        Parameters:
            value - A data reference.

        Returns:
            A tuple (producerReference, file, method)
            File may be None
            If method is not given it defaults to ref

        Exceptions:
            Raises ValueError if reference string contains no method, and invalid method, or has two many ':'
         '''

        reference = None
        filename = None
        method = None
        try:
            # Will raise value error if != 1 colon
            reference, method = value.split(':')

            # Reference sub-dirs of a component:
            # If the reference is to a component the string before the first path separator (if present)
            #    is the producer ref
            # If the producer is a directory the ref is the string before the last path separator
            # This means without further information there's no way to differentiate
            # A) GenerateInput/subdir1/somefile (component ref, split at GenerateInput) and
            # B) data/subdir1/somefile (dir ref, split at subdir1)
            # To this end we consider that components cannot be have the same name as a special folder
            # see FlowIR.SpecialFolders

            if reference.startswith('/'):
                # VV: This is an absolute reference parse it like this: <path/to/dir>/file
                reference, filename = os.path.split(reference)
            else:
                tokens = reference.split(os.path.sep, 1)
                if len(tokens) > 1:
                    if tokens[0] in cls.SpecialFolders:
                        # VV: This is a direct reference
                        filename = None
                    else:
                        # VV: This is a <producer>/<path/to/filename> situation
                        reference, filename = tokens
                else:
                    filename = None
        except ValueError as e:
            exc = ValueError("Invalid reference '%s' - missing reference method. Error: %s" % (value, e))
            raise_with_traceback(exc)

        return reference, filename, method

    @classmethod
    def is_datareference_to_component(cls, value, top_level_folders=None):
        # type: (str, List[str]) -> bool
        """Returns whether a reference points to a Component
        
        Arguments:
            value: A string representation of a reference
            top_level_folders: (optional) List of folders in the root directory of a package/instance
        """
        reference, filename, method = cls.ParseDataReference(value)
        stageIndex, jobName, hasIndex = cls.ParseProducerReference(reference)

        pattern = re.compile(FlowIR.VariablePattern)
        top_level_folders = top_level_folders or []
        
        special_folders = cls.SpecialFolders + top_level_folders
        
        # VV: File references, and references to specialfolders (bin, data, etc) cannot have a stageIndex
        if (jobName in special_folders and hasIndex is False) \
                or (os.path.sep in jobName and hasIndex is False) \
                or pattern.search(jobName):
            return False

        return True


    @classmethod
    def ParseDataReferenceFull(cls, value, index=None, application_dependencies=None, special_folders=None):

        '''TODO: Change to ParseDataReference and rename current ParseDataReference'''

        reference, filename, method = cls.ParseDataReference(value)
        stageIndex, jobName, hasIndex = cls.ParseProducerReference(reference, index)

        pattern = re.compile(FlowIR.VariablePattern)

        if application_dependencies:
            app_deps = [cls.application_dependency_to_name(x) for x in application_dependencies]
        else:
            app_deps = []
        special_folders = list(special_folders or []) + cls.SpecialFolders + list(app_deps)

        # VV: File references, and references to specialfolders (bin, data, etc) cannot have a stageIndex
        if (jobName in special_folders and hasIndex is False)\
               or (os.path.sep in jobName and hasIndex is False) \
               or pattern.search(jobName):
            stageIndex = None

        return stageIndex, jobName, filename, method

    @classmethod
    def ParseProducerReference(cls, reference, index=None):
        """Parses a producer reference string returning the producer name and the stage Index.

        A producer reference string has form ($stageName).$producerName
        $stageName is optional.

        Parameters:
            reference - The reference string
            index - (Optional) The index of the stage the reference is relative to if it is not
                specifed in the reference
        Returns:
            A tuple (stageIndex, producerName, hasIndex)
            If reference contains no stageIndex and job is not provided stageIndex==None
            hasIndex is True if reference contained a stageIndex false otherwise

        Note: In the case where the producer is a directory the stageIndex has no meaning

        Errors:
            Raises a ValueError if $stageName has incorrect format. Must be stage$i.
            Raises a ValueError if the producer reference contains more than one '.
        """

        # Possibilities
        # No "." in reference or reference starts with '/' indicating an absolute path
        #  - The passed reference is a component name or a directory
        # A "." in the reference
        #  - If the reference contains "stage$NUM" then
        #      - $NUM is the stage index
        #      - the portion after the `.` is a component name
        #  - If the reference does not contain "stage$NUM" then
        #        - The passed reference is a directory

        hasIndex = False
        jobName = None
        stageIndex = None
        if reference.startswith('/') or reference.find('.') == -1:
            jobName = reference
            if index is not None:
                stageIndex = index
        else:
            # There is a `.` in the reference but confirm it is actually a stage reference

            # This will raise ValueError if there is not a '.'
            try:
                stage, jobName = reference.split('.', 1)
            except ValueError:
                raise ValueError('Job reference invalid: %s' % reference)

            stageRe = re.compile(r"stage([0-9]+)")
            # Check that the putative `stage` part of the reference is an actual stage reference
            match = stageRe.match(stage)
            if match is not None:
                stageIndex = int(match.group(1))
                hasIndex = True
            else:
                # Assume this is a path with a `.` in it
                jobName = reference
                if index is not None:
                    stageIndex = index

        return stageIndex, jobName, hasIndex

    @classmethod
    def SplitReplicatedComponentName(cls, componentName):

        '''Given the name of a component returns the basename and replica index

        Parameters:
            componentName: The name of a replicated experiment component

        Returns:
            A tuple with two elements.

        Raises ValueError if no replica index can be determined'''

        exp = re.compile("([A-Za-z_]+)([0-9]+)")
        m = exp.match(componentName)

        tokens_loop = componentName.split('#', 1)

        if len(tokens_loop) == 2:
            componentName = tokens_loop[1]

        if m is None:
            raise ValueError('Cannot determine replica from component name: %s' % componentName)
        else:
            simIndex = int(m.group(2))
            name = m.group(1)

        if len(tokens_loop) == 2:
            name = '#'.join((tokens_loop[0], name))

        return name, simIndex

    @classmethod
    def override_object(cls, old, new):
        """Overrides `old` with the contents of `new`, both objects can be mutated by this method

        This method follows the following recursive rules:
            1. Dictionaries:
               - keys of old that don't exist in new are copied
               - keys of new that don't exist in old are copied
               - keys that exist in both dictionaries are recursively overriden
            2. Non Dictionaries (e.g. strings, ints, floats, Lists, booleans, etc)
                - old is replaced by new
            3. If either object is None the other is selected, if both objects are None, None is returned
            4. If `new` is not the same type as `old` it replaces old
        """
        final_value = []
        overriden = set()

        def set_final_return(value):
            assert len(final_value) == 0
            final_value.append(value)

        remaining = [(old, new, set_final_return)]

        while remaining:
            old, new, merge_ret = remaining.pop(0)
            id_old = id(old)

            if is_primitive_or_empty(old) is False:
                if id_old in overriden:
                    raise RuntimeError

                overriden.add(id_old)

            if isinstance(old, dict):
                ret = old  # type: collections.MutableMapping
                new = new or {}

                # VV: Start with `old` as the base layer
                merge_ret(ret)
                keys_old = set(old.keys())
                keys_new = set(new.keys())

                keys_common = keys_old.intersection(keys_new)
                keys_novel = keys_new.difference(keys_old)

                # VV: Copy the novel keys of `new`
                for key in keys_novel:
                    ret[key] = new[key]

                # VV: `recursively` override common keys
                for key in keys_common:
                    entry = (
                        old[key], new[key],
                        lambda value, ret=ret, key=key: ret.__setitem__(key, value)
                    )
                    remaining.append(entry)
            elif new is not None:
                merge_ret(new)
            else:
                merge_ret(old)

        return final_value[0]

    @staticmethod
    def _diff_dict_keys(a, b):
        # type: (Dict[str, Any], Dict[str, Any]) -> Tuple[Set[str], Set[str], Set[str]]

        a_keys = set(a.keys())
        b_keys = set(b.keys())

        common_keys = a_keys.intersection(b_keys)
        only_a = a_keys.difference(b_keys)
        only_b = b_keys.difference(a_keys)

        return common_keys, only_a, only_b

    @staticmethod
    def _diff_objects(a, b, root_key=None):
        def new_key(keys):
            if isinstance(keys, string_types):
                keys = [keys]
            keys = [root_key] + keys
            keys = [key for key in keys if key is not None]
            return '.'.join(keys)

        # VV: These are not dictionaries
        if type(a) == type(b) and isinstance(a, dict) is False:
            if isinstance(a, (string_types, int, float, bool)):
                if a != b:
                    return {new_key([]): (a, b)}
                else:
                    return {}
            elif isinstance(a, list):
                min_size = min(len(a), len(b))

                if min_size == 0:
                    return {}
                diff = {}
                for i, a_b in enumerate(zip(a[:min_size], b[:min_size])):
                    diff.update(FlowIR._diff_objects(a_b[0], a_b[1], root_key=new_key(['%s' % i])))

                for i, val in a[min_size:]:
                    diff[new_key(['%s' % (i + min_size)]) + '(missing rhs)'] = (val, None)

                for i, val in a[min_size:]:
                    diff[new_key(['%s' % (i + min_size)]) + '(missing lhs)'] = (None, val)

                return diff

        if a is None and b is None:
            return {}

        try:
            if a == b:
                return {}
        except:
            pass

        if a is None:
            return {new_key([]) + '(missing lhs)': (a, b)}

        if b is None:
            return {new_key([]) + '(missing rhs)': (a, b)}

        # VV: a/b are dictionaries
        common, from_a, from_b = FlowIR._diff_dict_keys(a, b)

        diff = {}

        for key in from_a:
            diff[new_key(key) + '(missing rhs)'] = (a[key], None)

        for key in from_b:
            diff[new_key(key) + '(missing lhs)'] = (None, b[key])

        for key in common:
            if isinstance(a[key], dict):
                inner_diff = FlowIR._diff_objects(a[key], b[key])
                inner_diff = dict([
                    (new_key([key, inner_key]), inner_diff[inner_key]) for inner_key in inner_diff
                ])

                diff.update(inner_diff)
            elif isinstance(a[key], string_types + (float, int, bool)):
                if a[key] != b[key]:
                    diff[new_key([key])] = (a[key], b[key])
            elif isinstance(a[key], list):
                inner_diff = FlowIR._diff_objects(a[key], b[key], root_key=new_key([key]))
                if inner_diff:
                    diff[new_key([key])] = inner_diff
            else:
                assert Exception("Cannot handle value %s" % a[key])

        return diff

    @staticmethod
    def _diff_components(a, b):
        common, from_a, from_b = FlowIR._diff_dict_keys(a, b)
        diff = {}

        for key in from_a:
            diff[key] = (a[key], None)

        for key in from_b:
            diff[key] = (None, b[key])

        for key in common:
            if key == 'references':
                ref_a = set(a['references'])
                ref_b = set(b['references'])

                if ref_a != ref_b:
                    diff[key] = (ref_a, ref_b)
            elif key in ['command', 'workflowAttributes',
                         'executors', 'variables', 'resourceRequest', 'resourceManager']:
                dict_a = a[key]
                dict_b = b[key]
                diff.update(FlowIR._diff_objects(dict_a, dict_b, key))
            elif key in ['name', 'stage']:
                assert a[key] == b[key]
            else:
                raise Exception("Cannot compare component field %s" % key)

        return diff

    @classmethod
    def visit_all(cls, obj, visit_func, label=None):
        # type: (Any, Callable[[Any, str], None], Optional[str]) -> None
        def try_call(obj, label):
            try:
                visit_func(obj, label)
            except Exception:
                pass

        if label is None:
            label = ''
        visited = set()
        destinations = [(obj, label)]
        while destinations:
            obj, label = destinations.pop(0)
            if id(obj) in visited:
                continue
            visited.add(id(obj))

            try_call(obj, label)

            if isinstance(obj, dict):
                for key in obj:
                    child_label = '.'.join((label, str(key)))
                    destinations.append((obj[key], child_label))
            elif isinstance(obj, list):
                for i, k in enumerate(obj):
                    child_label = ''.join((label, '[%s]' % i))
                    destinations.append((k, child_label))


    @classmethod
    def replace_strings(
            cls,  # type: FlowIR
            obj,  # type: Any
            translation_func,  # type: Callable[[str], str]
            in_place=False  # type: bool
    ):
        # type: (...) -> Any
        if obj is None:
            return None

        if in_place is False:
            obj = deep_copy(obj)

        if isinstance(obj, dict):
            for key in obj:
                obj[key] = cls.replace_strings(obj[key], translation_func, in_place)
            return obj
        elif isinstance(obj, string_types):
            return translation_func(obj)
        elif isinstance(obj, (int, float, bool)):
            return obj
        elif isinstance(obj, list):
            for i in range(len(obj)):
                obj[i] = cls.replace_strings(obj[i], translation_func, in_place)
            return obj
        elif in_place is False:
            if isinstance(obj, (tuple, set)):
                ret = [cls.replace_strings(entry, translation_func, in_place) for entry in obj]
                return type(obj)(ret)

        raise NotImplementedError('Cannot process %s type: %s (in_place=%s)' % (
            str(obj), type(obj), in_place
        ))

    @classmethod
    def fill_in(cls, obj, context, flowir=None, ignore_errors=False, label=None, is_primitive=False):
        # type: (Any, Dict[str, str], Optional[DictFlowIR], bool, Optional[str], bool) -> Any
        """Resolves an object by removing references to variables and expanding array accesses (See notes)

        Note:
            The resolved object does not contain references to variables (see is_primitive) or accesses to arrays.
            Except for array-accesses to a file:ref. For these, the true path cannot be known. To this end,
            you need to first resolve and replace any present DataReferences before invoking this method.

        Args:
            obj: which object to fill in with values of variables/options (from context dictionary)
            context: collection of variables, %(hello)s resolves to context['hello']
            flowir: enveloping FlowIR definition (only used when raising exceptions so that it's easier to debug)
            ignore_errors: this option configures fill_in not to immediately terminate on an exception, it will
                  attempt to resolve the object as much as possible before returning (can be used to debug)
            label: errors will be tagged with this label (fill_in generates child object labels based on this value)
            is_primitive: set this to True to ignore errors to variables which can't be known for primitive graphs
                  (such as `replica`). Such variable references are left untouched.

        Returns:
            A resolved object
        """
        flowir = flowir or {}

        if isinstance(obj, dict):
            for key in obj:
                if label is not None:
                    key_label = '%s.%s' % (label, key)
                else:
                    key_label = key

                obj[key] = cls.fill_in(
                    obj[key], context, flowir=flowir, ignore_errors=ignore_errors,
                    label=key_label, is_primitive=is_primitive
                )
            return obj
        elif isinstance(obj, string_types):
            try:
                obj = cls.interpolate(
                    obj, context, flowir, False, label=label, ignore_errors=ignore_errors, is_primitive=is_primitive
                )
            except experiment.model.errors.FlowIRVariableUnknown:
                if ignore_errors is False:
                    raise
            return obj
        elif isinstance(obj, (int, float, bool)):
            return obj
        elif isinstance(obj, (list, set, tuple)):
            ret = []
            for i, value in enumerate(obj):
                if label is not None:
                    item_label = '%s[%d]' % (label, i)
                else:
                    item_label = None
                ret.append(cls.fill_in(
                    value, context, flowir=flowir, ignore_errors=ignore_errors,
                    label=item_label, is_primitive=is_primitive
                ))
            return type(obj)(ret)
        elif obj == None:
            return None

        raise NotImplementedError('Cannot process %s type: %s' % (
            str(obj), type(obj)
        ))

    def diff(self, other):
        # type: (FlowIR, FlowIR) -> Dict[str, Tuple[Any, Any]]
        diff = {}
        other = cast(FlowIR, other)

        common, me, them = self._diff_dict_keys(self.flowir, other.flowir)

        for key in me:
            diff[key] = (self.flowir[key], None)

        for key in them:
            diff[key] = (None, other.flowir[key])

        pending = [(key, self.flowir[key], other.flowir[key]) for key in common]

        while pending:
            full_key, val_mine, val_other = pending.pop(0)
            if full_key != FlowIR.FieldComponents:
                non_empty = val_mine or val_other

                # VV: Both are none, therefore equal!
                if non_empty is None:
                    continue

                if val_mine is not None and val_other is None:
                    diff[full_key] = (val_mine, None)
                    continue
                elif val_mine is None and val_other is not None:
                    diff[full_key] = (None, val_other)
                    continue
                elif val_mine is not None and val_other is not None:
                    if type(val_mine) is not type(val_other):
                        raise Exception(
                            '%s and %s have different types (%s, %s) but same key %s' % (
                                val_mine, val_other, type(val_mine), type(val_other), full_key
                            )
                        )

                if isinstance(non_empty, string_types+(float, int, bool)):
                    # VV: Primitive types
                    if val_mine != val_other:
                        diff[full_key] = (val_mine, val_other)
                elif isinstance(non_empty, dict):
                    common, me, them = self._diff_dict_keys(val_mine, val_other)

                    for key in me:
                        diff['%s.%s' % (full_key, key)] = (val_mine[key], None)

                    for key in them:
                        diff['%s.%s' % (full_key, key)] = (None, val_other[key])

                    pending.extend([('%s.%s' % (full_key, key), val_mine[key], val_other[key]) for key in common])
                elif isinstance(non_empty, list):
                    set_mine = set(val_mine)

                    set_other = set(val_other)

                    if set_mine != set_other:
                        diff[full_key] = (val_mine, val_other)
                else:
                    raise Exception(
                        'Cannot diff %s with %s' % (
                            val_mine, val_other
                        )
                    )
            else:
                # VV: Components are a special case of set/lists()
                for comp in self.flowir[FlowIR.FieldComponents]:
                    comp_id = 'stage%d.%s' % (comp['stage'], comp['name'])

                    ocomp = [e for e in other.flowir[FlowIR.FieldComponents]
                             if e['name'] == comp['name'] and e['stage'] == comp['stage']]

                    if len(ocomp) == 1:
                        comp_diff = self._diff_components(comp, ocomp[0])
                        comp_diff = dict([
                            ('%s.%s.%s' % (FlowIR.FieldComponents, comp_id, key), comp_diff[key]) for key in comp_diff
                        ])
                    elif len(ocomp) == 0:
                        comp_diff = {}
                    else:
                        comp_diff = {
                            '%s.%s': (comp, ocomp)
                        }

                    diff.update(comp_diff)

        return diff

    @classmethod
    def str_to_kubernetes_qos(cls, value: Optional[str]) -> str:
        if value is None:
            return cls.LabelKubernetesQosGuaranteed

        value = value.lower()
        if value in (cls.LabelKubernetesQosGuaranteed,
                     cls.LabelKubernetesQosBurstable, cls.LabelKubernetesQosBestEffort):
            return value
        raise ValueError(f'"{value}" is an invalid Kubernetes QOS level')

    @classmethod
    def memory_to_bytes(cls, value):
        # type: (Optional[str]) -> Optional[int]
        if value is None:
            return None

        try:
            return int(value)
        except ValueError:
            integer = int(value[:-2])

            if value.endswith('Mi'):
                return integer * 1024 * 1024
            elif value.endswith('Gi'):
                return integer * 1024 * 1024 * 1024
            else:
                raise ValueError("Cannot convert %s to memory bytes" % value)

    @classmethod
    def convert_component_types(cls, comp, ignore_convert_errors=False, out_errors=None, is_primitive=False):
        # type: (DictFlowIRComponent, bool, Optional[List[Exception]], bool) -> None
        """Convert component fields to their appropriate type in-place.

        Keep in mind that this method converts the fields of the components to their expected type in place. In other
        words if an exception is raised the component could be in a partially converted state. The caller is expected
        to make a copy of the component before they call this method if they wish to keep the original component
        description.

        Args:
            comp: Component description
            ignore_convert_errors: Set this to True to ignore any errors
            out_errors: List to store exceptions, if set to None convert_component_types will attempt to convert the
                the fields of the component and if errors are raised it will capture them and re-raise them just
                before exiting. If a list is provided, the caller of this method is expected to handle the errors. In
                both cases the process will not stop when it encounters an exception.
            is_primitive: Whether the component is in its primitive form or not. If it's in the primitive form it
                follows slightly more relaxed rules. For example, a primitive replica-component could be using its
                `replica` variable to access an array in order to build some of its fields. But the `replica` variable
                cannot be known for the primitive version of a replica and it's impossible to apply type-conversion
                before resolving the `replica` variable. To this end, this method expects that some of the fields
                cannot be fully resolved when `is_primitive` is set to True. It discards errors which stem from
                the `replica` variable being unknown.
        """
        original_out_errors = out_errors

        out_errors = out_errors if out_errors is not None else []

        full_name = 'stage%s.%s' % (
            comp.get('stage', '*Unknown*'),
            comp.get('name', '*Unknown*')
        )

        def optional_int(value):
            if value is not None:
                return int(value)

        expected_types = {
            'command': {
                'arguments': str,
                'environment': str,
                'executable': str,
                'resolvePath': str_to_bool,
                'interpreter': str,
                'expandArguments': str,
            },
            'workflowAttributes': {
                'restartHookFile': str,
                'replicate': int,
                'aggregate': bool,
                'isMigratable': bool,
                'isMigrated': bool,
                'repeatInterval': int,
                'repeatRetries': int,
                'isRepeat': bool,
                # VV: when maxRestarts is None, the Engine/RepeatingEngine objects decides max number of restarts
                'maxRestarts': optional_int,
                'optimizer': {
                    'disable': bool,
                    'exploitChance': float,
                    'exploitTarget': float,
                    'exploitTargetLow': float,
                    'exploitTargetHigh': float,
                }
            },
            'resourceRequest': {
                'numberProcesses': int,
                'numberThreads': int,
                'ranksPerNode': int,
                'threadsPerCore': int,
                'memory': cls.memory_to_bytes,
                'gpus': int,
            },
            'resourceManager': {
                'config': {
                    'backend': str,
                    'walltime': float,
                },
                'lsf': {
                    'queue': str,
                    'reservation': str,
                    'resourceString': str,
                    'statusRequestInterval': float,
                    'dockerImage': str,
                    'dockerProfileApp': str,
                    'dockerOptions': str,
                },
                'kubernetes': {
                    'qos': cls.str_to_kubernetes_qos,
                    'image': str,
                    'image-pull-secret': str,
                    'namespace': str,
                    'api-key-var': str,
                    'host': str,
                    'cpuUnitsPerCore': float,
                    'gracePeriod': int,
                    'podSpec': dict,
                }
            },
        }
        comp = comp or {}

        def convert(value, expected_type, label):
            if isinstance(value, string_types + (int, bool,)):
                try:
                    value = expected_type(value)
                except:
                    if ignore_convert_errors:
                        return value

                    # VV: We could be attempting to fill in a `primitive` graph so `replica` is expected to be
                    #     an unknown variable
                    if is_primitive is True:
                        pattern = re.compile(FlowIR.VariablePattern)

                        unresolved_variables = [
                            s.group()[2:-2] for s in pattern.finditer(str(value))]  # type: List[str]

                        if unresolved_variables == ['replica']:
                            flowirLogger.info(
                                "Could not convert '%s=%s' to %s of %s=%s but will treat this as "
                                "the expected behaviour because we are dealing with a primitive graph." % (
                                label, value, expected_type, full_name, pretty_json(comp)
                            ))
                            return value

                    flowirLogger.critical("Failed to convert '%s'='%s' to %s of %s=%s" % (
                        label, value, expected_type, full_name, pretty_json(comp)
                    ))

                    out_errors.append(
                        experiment.model.errors.FlowIRInvalidFieldType(label, expected_type, value)
                    )
                    raise
                return value
            elif isinstance(value, dict):
                if expected_type == dict:
                    # VV: this means that we do not care about the value
                    return value

                for key in value:
                    if key in expected_type:
                        try:
                            value[key] = convert(value[key], expected_type[key], '%s.%s' % (label, key))
                        except Exception as e:
                            if ignore_convert_errors:
                                continue
                            msg = "Failed to convert %s=%s of %s=%s, because of %s" % (
                                key, value[key], full_name, pretty_json(value), e
                            )
                            flowirLogger.critical(msg)
                            out_errors.append(ValueError(msg))

                return value

            return value

        # VV: Recursively convert the component in-place
        convert(comp, expected_types, 'stage%s.%s' % (comp['stage'], comp['name']))

        if original_out_errors is None and len(out_errors):
            raise experiment.model.errors.FlowIRFailedComponentConvertType(comp, out_errors)

    @classmethod
    def digest_interpreter_field(cls, comp):
        # type: (DictFlowIRComponent) -> DictFlowIRComponent
        comp = deep_copy(comp)

        if 'command' not in comp:
            return comp

        interpreter = comp['command'].get('interpreter')

        if interpreter and comp['command'].get('executable') is None:
            interpreters = cls.get_interpreter_executables()
            if interpreter in interpreters:
                interpreter_executable = interpreters[interpreter]
                if interpreter_executable:
                    arguments = interpreters[interpreter] + ' ' + comp['command'].get('arguments', '')
                else:
                    arguments = comp['command'].get('arguments', '')

                # VV: Interpreters can skip defining interpreters because Flow patches-in that information
                if arguments != ' ':
                    if arguments[0] not in ['"', "'"]:
                        tokens = arguments.split(' ')
                    else:
                        end_quote = arguments.find(arguments[0], 1)
                        if end_quote != -1:
                            tokens = [arguments[1:end_quote]] + arguments[end_quote + 1:].split(' ')
                            # VV: The very first token is the quoted contents + everything till the first space
                            #     e.g. "'usr/not bin/'echo hello world" will evaluate to
                            #     [usr/not bin/echo, hello, world]
                            tokens = [arguments[1:end_quote]] + tokens[1:]
                        else:
                            full_name = 'stage%s.%s' % (comp.get('name', '*Unknown*'), comp.get('stage', '*Unknown*'))
                            raise experiment.model.errors.ExperimentInvalidConfigurationError(
                                'Component %s has arguments which start which contain a stray %s symbol' % (
                                    full_name, arguments[0]
                                )
                            )
                    interpreter_executable = tokens[0]
                    arguments = ' '.join(tokens[1:])

                    comp['command']['executable'] = interpreter_executable
                    comp['command']['arguments'] = arguments
            else:
                raise ValueError('Component stage%s.%s is using unknown interpreter %s' % (
                    comp.get('stage', '*Unknown*'), comp.get('name', '*Unknown*'), interpreter
                ))

            if comp['command'].get('executable', '') == '':
                raise ValueError('Component stage%s.%s has not been configured to use an executable' % (
                    comp.get('stage', '*Unknown*'), comp.get('name', '*Unknown*')
                ))

        return comp

    @classmethod
    def inject_default_values_to_component(cls, comp, all_values=True):
        # type: (DictFlowIRComponent, bool) -> DictFlowIRComponent
        if all_values:
            default = cls.default_component_structure()
            default_restartHookOn = default['workflowAttributes']['restartHookOn']
            del default['workflowAttributes']['restartHookOn']
        else:
            default_restartHookOn = False
            default = {
                'stage': 0,
            }

        comp = cls.override_object(default, comp)

        # VV: workflowAttributes.restartHookOn is a List, we cannot use `override_object` to insert default values
        if default_restartHookOn and comp.get('workflowAttributes', {}).get('restartHookOn') is None:
            if 'workflowAttributes' not in comp:
                comp['workflowAttributes'] = {}
            comp['workflowAttributes']['restartHookOn'] = default_restartHookOn

        if 'repeatInterval' in comp.get('workflowAttributes', {}):
            comp['workflowAttributes']['isRepeat'] = comp['workflowAttributes']['repeatInterval'] not in [None, 0]

        return comp

    def inject_default_values(self, components_too=True):
        flowir = self.override_object(self.default_flowir_structure(), deep_copy(self.flowir))
        interface = flowir.get(FlowIR.FieldInterface)
        if interface is not None:
            # VV: The interface is either None or a dictionary. When it is a dictionary all keys are required except for
            # description
            # inputSpec.hasAdditionalData
            # propertiesSpec[#].description
            # inputs -> computed by Runtime, so set to None to indicate that it has not been computed
            # additionalData -> computed by Runtime, so set to None to indicate that it has not been computed
            if 'description' not in interface:
                interface['description'] = ""

            if 'hasAdditionalData' not in interface['inputSpec']:
                interface['inputSpec']['hasAdditionalData'] = False

            if 'additionalData' not in interface:
                interface['additionalInputData'] = None
            if 'inputs' not in interface:
                interface['inputs'] = None

            if 'outputFiles' not in interface:
                interface['outputFiles'] = []

        if self.FieldVersion not in flowir:
            flowir[self.FieldVersion] = '.'.join(map(str, FlowIR.NewestVersion))

        known_platforms = self.discover_platforms(flowir)
        flowir[FlowIR.FieldPlatforms] = known_platforms

        for platform in flowir[FlowIR.FieldPlatforms]:
            for key in [FlowIR.FieldEnvironments, FlowIR.FieldVariables]:
                if platform not in flowir[key]:
                    flowir[key][platform] = {}

            if FlowIR.LabelGlobal not in flowir[FlowIR.FieldVariables][platform]:
                flowir[FlowIR.FieldVariables][platform][FlowIR.LabelGlobal] = {}
            if FlowIR.LabelStages not in flowir[FlowIR.FieldVariables][platform]:
                flowir[FlowIR.FieldVariables][platform][FlowIR.LabelStages] = {}

        if FlowIR.LabelDefault not in flowir[FlowIR.FieldEnvironments]:
            flowir[FlowIR.FieldEnvironments][FlowIR.LabelDefault] = {}

        variable_collections = flowir[FlowIR.FieldVariables]

        for coll in variable_collections:
            for key in [FlowIR.LabelGlobal, FlowIR.LabelStages]:
                if key not in variable_collections[coll]:
                    variable_collections[coll][key] = {}

        expanded = []
        max_stage = -1
        for comp in flowir[self.FieldComponents]:
            if 'stage' not in comp:
                comp['stage'] = 0

            if '$import' in comp:
                expanded.append(comp)
                continue
            comp = self.inject_default_values_to_component(comp, all_values=components_too)
            try:
                max_stage = max(max_stage, int(comp['stage']))
            except Exception:
                pass
            expanded.append(comp)

        flowir[self.FieldComponents] = expanded
        num_stages = max_stage + 1

        if num_stages:
            weights = []
            for idx in range(num_stages):
                if idx not in flowir[self.FieldStatusReport]:
                    flowir[self.FieldStatusReport][idx] = {'stage-weight': 0.0}
                elif 'stage-weight' not in flowir[self.FieldStatusReport][idx]:
                    flowir[self.FieldStatusReport][idx]['stage-weight'] = 0.0

                try:
                    stage_weight = float(flowir[self.FieldStatusReport][idx]['stage-weight'])
                except ValueError:
                    stage_weight = 0.0

                weights.append(stage_weight)

            # VV: adding floats is hard, let's assume that there're at most 2 decimals
            int_weights = [int(e * 1000) for e in weights]

            if sum(int_weights) != 1000:
                fallbackWeight = int(1000 / num_stages) / 1000.0

                flowirLogger.log(19, "Stage weights do not add to one: %s = %3.3lf\n" % (weights, sum(weights)))
                flowirLogger.log(19, "All stage-weights will default to %3.3lf\n" % fallbackWeight)

                for idx in range(num_stages):
                    flowir[self.FieldStatusReport][idx]['stage-weight'] = fallbackWeight

                # VV: When it's not easy to automatically divide 1.0 into num_stages, make the last one slightly
                #     more important
                if num_stages * int(100*fallbackWeight) != 1000:
                    flowir[self.FieldStatusReport][num_stages-1]['stage-weight'] = (
                            1000-((num_stages-1) * int(1000/num_stages))
                    )/1000.0

        return flowir

    @classmethod
    def scope_route_to_context(cls, route, current_context):
        # type: (str, Dict[str, str]) -> Dict[str, Any]

        scopes = route.split('.')

        if len(scopes) == 1:
            return current_context

        if scopes[0] == 'flow':
            raise experiment.model.errors.FLowIRSymbolTableNotImplemented()

        raise NotImplementedError(
            'Have not implemented scope_route lookup'
        )

    @classmethod
    def _expand_array_access(cls, value):
        # type: (str) -> str
        """Expands an array access, will not attempt to expand arrays that point to DataReference (`:ref`).

        An array access can be 2 things:
        - a space separated list of items followed by [index:integer]
        - or a path to a file followed by [index:integer]

        Examples:
        - -c hello world[0] will resolve to "-c"
        - /path/to/data/file.yml[1] expects that the file exists, and contains a YAML array. This access evaluates to
          second item in the array
        - /path/to/non-yaml-file[2] expects that the file exists, and contains lines of text. This access evaluates to
          the third line of text in the file.
        """
        # VV: Check if string is in fact an array access
        index_token = re.compile(r'\[(\d+)\]')

        if index_token.search(value) is None:
            return value

        last_search_pos = 0

        while True:
            matches = list(index_token.finditer(value, last_search_pos))
            if not matches:
                break

            match = matches[0]

            # VV: An array is either:
            # 1. a space separated array of values
            # 2. a text file which separates values with a new line
            # 3. a yaml file that contains an array of values

            array_or_file = value[last_search_pos: match.start()].strip()
            file = value[last_search_pos: match.start()].split()[-1]
            str_index = match.group(1)
            try:
                index = int(str_index)
            except ValueError:
                raise ValueError("Index to array access \"%s\" is \"%s\" but was expected to be an integer" % (
                    array_or_file, str_index
                ))

            # VV: If a file reference ends in `:ref` then refuse to read the file, and just completely skip this match
            #     this means that we have not resolved the `:ref` yet so there is no way to know where the file is!
            if file.endswith(':ref'):
                last_search_pos = match.end()
                continue

            if os.path.exists(file):
                if os.path.splitext(file)[1] in ['.yml', '.yaml']:
                    # VV: Handle YAML files
                    with open(file, 'rb') as f:
                        data = yaml_load(f)[index]
                else:
                    # VV: Handle files which contain lines
                    with open(file) as f:
                        for _ in range(index):
                            f.readline()
                        data = f.readline().strip()
                # VV: Replace just the `<file>[<index>]` part of the string (there could me multiple
                #     works before <file> that are separated with spaces)
                last_search_pos = match.start()-len(file)
            else:
                # VV: Handle space-separated arrays of values
                data = array_or_file.split()[index]

            value = ''.join((
                # VV: characters in value right before `array_of_file` which began at index `last_search_pos`
                value[:last_search_pos],
                # VV: followed by the value of the array-access
                data,
                # VV: and finally everything else that remains in the string
                value[match.end():]
            ))

            # VV: Next iteration, search for array-accesses right after the one that we handled just now
            last_search_pos = match.end()

        return value

    @classmethod
    def interpolate(cls, input_str, context, flowir=None,
                    use_symbol_table=True, label=None, ignore_errors=False,
                    is_primitive=False):
        # type: (str, Dict[str, str], Optional[DictFlowIRComponent], bool, Optional[str], bool, bool) -> str
        """ Applies string interpolation to resolve references to variables, and array-accesses  (See notes)

        Note:
            The resolved string does not contain references to variables (see is_primitive) or accesses to arrays.
            Except for array-accesses to a file:ref. For these, the true path cannot be known. To this end,
            you need to first resolve and replace any present DataReferences before invoking this method.

            There is an argument `use_symbol_table`, currently the value of this parameter is ignored.
            In the future, setting this to True will enable resolving %(hello.world)s to context['hello']['world'].
            However, this feature is currently *disabled* because there is no use for it. Just let the method use
            the default value.
        Args:
            input_str: which string to apply string-interpolation to
            context: collection of variables, %(hello)s resolves to context['hello']
            flowir: enveloping FlowIR definition (only used when raising exceptions so that it's easier to debug)
            ignore_errors: this option configures fill_in not to immediately terminate on an exception, it will
                  attempt to resolve the object as much as possible before returning (can be used to debug)
            label: errors will be tagged with this label (fill_in generates child object labels based on this value)
            is_primitive: set this to True to ignore errors to variables which can't be known for primitive graphs
                  (such as `replica`). Such variable references are left untouched.

        Returns:
            A string

        """
        flowir = flowir or {}

        # VV: First, look for variables or variables followed by indices
        #     (indices can be either integers or other variables)
        #     When no more variables can be resolved, check whether the remaining string is an array access
        #     Finally, check whether the resulting string is an access to an array and resolve that too

        int_or_var_index = r'\[(\d+|'+FlowIR.VariablePattern+')\]'
        index_pattern = re.compile(r'\['+FlowIR.VariablePattern+'\]')
        pattern = re.compile(FlowIR.VariablePattern + '('+ int_or_var_index + ')?')
        resolved_variables = {}  # type: Dict[str, str]

        safe_to_be_unknown = set(['replica']) if is_primitive else set()

        if isinstance(input_str, (int, float, bool)):
            return input_str

        if isinstance(input_str, string_types) is False:
            raise experiment.model.errors.InternalInconsistencyError('Format "%s" is not a string (label: %s)' % (
                input_str, label
            ))

        def match_to_name_index(match):
            # type: (re.Match) -> Tuple[str, Optional[str]]
            whole = match.group()
            tokens = whole.split('[', 1)

            if len(tokens) > 1:
                name = tokens[0]
                index = whole[len(name)+1:-1]
            else:
                name = whole
                index = None
            # VV: Get rid of '%(' prefix and ')s' postfix
            name = name[2:-2]

            return name, index

        # VV: Currently we don't support the use of `context` as a multi-level symbol table for artificial reasons.
        #    (i.e. this function could support it we're just limiting the string-interpolation functionality till we
        #    see a reason to enable it)
        def resolve_using_symbol_table(variable_name, safe_to_be_unknown=safe_to_be_unknown):
            inner_context = cls.scope_route_to_context(variable_name, context)
            inner_variable = variable_name.split('.')[-1]
            if inner_variable not in inner_context:
                raise experiment.model.errors.FlowIRVariableUnknown(variable_name, inner_context, flowir, label=label)

            variable_format = inner_context[inner_variable]

            # VV: Variables can be primitives (e.g. 'replica')
            if isinstance(variable_format, (int, bool, float)):
                variable_value = repr(variable_format)
            elif isinstance(variable_format, string_types):
                variable_label = '%s:%%(%s)s' % (label, inner_variable)
                variable_value = cls.interpolate(
                    variable_format, inner_context, label=variable_label, is_primitive=is_primitive
                )
            else:
                raise experiment.model.errors.FlowIRVariableInvalid(
                    input_str, variable_name, inner_context, flowir, label=label
                )

            resolved_variables[variable_name] = variable_value
            return variable_value

        # VV: First resolve all variable indices, the step after this one expects indices to be already
        #     in `integer`-ready form
        search_from = 0
        while True:
            # VV: search_from is used to skip searching for variables which are OK to be unknown (such as the
            #     replica variable when the graph is still in its primitive form)
            matches = sorted(index_pattern.finditer(input_str, search_from), key=lambda s: s.start())
            if not matches:
                break

            match = matches[0]

            # VV: discard '[%(' and ')s]'
            variable_name = match.group()[2+1:-(2+1)]

            if variable_name in resolved_variables:
                variable_value = resolved_variables[variable_name]
            else:
                if use_symbol_table is False:
                    if len(variable_name.split('.')) > 1:
                        if ignore_errors and use_symbol_table is False:
                            safe_to_be_unknown.add(variable_name)
                            search_from = match.start() + 1
                            continue
                        raise ValueError("ArrayIndex \"%s\" can only be resolved using the symbol table %s" % (
                            variable_name, context
                        ))
                try:
                    # VV: Use recursion to figure out the value of the variable (it could be a variable that's equal
                    #     to another variable, etc)
                    variable_value = resolve_using_symbol_table(variable_name)
                except experiment.model.errors.FlowIRVariableUnknown as e:
                    if ignore_errors and use_symbol_table is False or (
                            is_primitive and e.variable_route in safe_to_be_unknown):
                        # VV: The index is in one of the safe-to-be-unknown variables which means that we cannot
                        #    expand the ArrayAccess, so we leave `format` as it is and let the caller deal with it
                        return input_str

                    raise ValueError("ArrayIndex \"%s\" can only be resolved using the symbol table %s" % (
                        variable_name, context
                    ))

            input_str = ''.join((
                # VV: Leave the '['
                input_str[:match.start() + 1],
                variable_value,
                # VV: Leave the ']'
                input_str[match.end() - 1:]
            ))

        # VV: Then resolve all `variable[index]` and `variable` instances
        search_from = 0
        while True:
            # VV: search_from is used to skip searching for variables which are OK to be unknown (such as the
            #     replica variable when the graph is still in its primitive form)
            matches = sorted(pattern.finditer(input_str, search_from), key=lambda s: s.start())
            if not matches:
                break

            match = matches[0]
            
            # VV: Replace one variable match every time till there're no more `must_resolve` variables left
            variable_name, array_index = match_to_name_index(match)

            # VV: re-use already previously resolved variable_names
            if variable_name in resolved_variables:
                variable_value = resolved_variables[variable_name]
            else:
                # VV: A variable can potentially refer to some other context (scope of variables)
                if use_symbol_table is False:
                    if len(variable_name.split('.')) > 1:
                        safe_to_be_unknown.add(variable_name)
                        search_from = match.start() + 1
                        continue

                try:
                    variable_value = resolve_using_symbol_table(variable_name)
                except experiment.model.errors.FlowIRVariableUnknown as e:
                    if ignore_errors or (is_primitive and variable_name in safe_to_be_unknown):
                        search_from = match.start() + 1
                        variable_value = '%%(%s)s' % variable_name
                    else:
                        raise

            # VV: Resolve array-accesses which have use the contents of a variable as the array
            if array_index is not None:
                resolved = cls._expand_array_access('%s[%s]' % (variable_value, array_index))
            else:
                resolved = variable_value

            input_str = ''.join((
                input_str[:match.start()],
                resolved,
                input_str[match.end():]
            ))

        # VV: Check whether there's an array access on constant arrays or files
        input_str = cls._expand_array_access(input_str)

        # VV: Finally, check if there're any incomplete variables
        pattern_incomplete = re.compile(FlowIR.VariablePatternIncomplete)
        incomplete_matches = [x.group() for x in pattern_incomplete.finditer(input_str)
                              if x.end()==len(input_str) or input_str[x.end()] != 's']
        if incomplete_matches and ignore_errors is False:
            raise experiment.model.errors.FlowIRVariablesIncomplete(incomplete_matches, label)

        return input_str


def map_placeholder_id_to_iteration(comp_id, do_whiles, known_component_ids, out_comp=False):
    loop_ids = [c for c in known_component_ids if (len(c[1].split('#', 1)) == 2)]
    condition_instances = [c for c in loop_ids if c[1].split('#', 1)[1] == comp_id[1]]

    if not condition_instances:
        return None

    latest = sorted(
        condition_instances,
        # VV: Sort on iteration number from stage<idx:%d>.<iteration-no:%d>#<name:str>
        key=lambda c: c[1].split('#', 1)[0],
        reverse=True
    )[0]

    stage_index, producer = latest
    match = (int(stage_index), producer)

    if out_comp is False:
        return match

    # VV: We want the definition inside one of the do_whiles documents
    for dw in do_whiles:
        for comp_def in dw['document']['components']:
            if comp_id == (comp_def['stage'], comp_def['name']):
                return comp_def

    return None


class FlowIRConcrete(object):
    def __init__(
            self,
            flowir_0: DictFlowIR,
            platform: Optional[str],
            documents: Optional[DictManyDocuments],
    ):
        """Instantiates a FlowIRConcrete

        Args:
            flowir_0: Dictionary describing FlowIR
            platform: name of FlowIR platform
            documents: Collection of Documents in the form of a nested dictionary. Outter key is type of Document
                next key is ID of Document and innermost dictionary contains the contents of the document.
        """
        self._cache = FlowIRCache()
        flowir_0 = flowir_0 or {}

        # VV: Apply any Component modification rules here
        flowir_0 = deep_copy(flowir_0)
        components = flowir_0.get(FlowIR.FieldComponents, None)
        if components:
            def pipeline(component):
                if '$import' in component:
                    return component

                # VV: Deprecate options such as `repeat-while`
                FlowIR.filter_out_deprecated_component_fields(component)
                # VV: Ensure that the component has all of its required features (but don't inject any default values)
                component = FlowIR.inject_default_values_to_component(component, all_values=False)
                return component

            components = list(map(pipeline, components))

            flowir_0[FlowIR.FieldComponents] = components

        self._component_dictionary = dict()  # type: Dict[Tuple[int, str], DictFlowIRComponent]
        self._flowir_0 = flowir_0
        self._platform = platform or FlowIR.LabelDefault

        self.configure_platform(self._platform)
        raw = FlowIR.from_dict(self._flowir_0)
        self._flowir = raw.inject_default_values(False)
        self.refresh_component_dictionary()

        documents = deep_copy(documents or {})

        # VV: Collections of documents: outer key is the type of the collection (e.g. DoWhile). Nested to outer key
        #     is the id of the document. The id then points to the contents of the document
        self._documents = documents # type: Dict[str, Dict[str, Dict[str, Any]]]

    def set_interface(self, interface: Optional[DictFlowIRInterface]):
        """Updates the interface (see FlowIR.type_flowir_interface_structure for spec).

        The interface is optional, a None value indicates that the definition does not have an interface.
        """
        self._flowir[FlowIR.FieldInterface] = copy.deepcopy(interface)

    def get_interface(self, return_copy=True) -> Optional[DictFlowIRInterface]:
        """Returns the interface (see FlowIR.type_flowir_interface_structure for spec).

        The interface is optional, a None value indicates that the definition does not have an interface.

        Args:
            return_copy: Whether to return a deep copy or the value as is (default is True).

        Returns:
            A dictionary representation of the interface (See FlowIR.type_flowir_interface_structure() for spec).
              Or `None` if the definition does not have an interface.
        """

        # VV: The interface is optional (may not exist at all)
        if return_copy:
            return copy.deepcopy(self._flowir.get(FlowIR.FieldInterface))
        else:
            return self._flowir.get(FlowIR.FieldInterface)

    def validate(self, top_level_folders=None):
        # type: (Optional[List[str]]) -> List[experiment.model.errors.FlowIRException]
        """Validate a FlowIR definition

        Arguments:
            top_level_foolders: (optional) a list of names of folders that are in the root-directory of the
               package/instance
        
        Returns
            A list of FlowIRExceptions
        """

        out_errors = FlowIR.validate(self.raw(), self._documents)

        component_identifiers = self.get_component_identifiers(True)

        # VV: See if there're any loops and include the placeholder names too
        placeholder_identifiers = self.get_placeholder_identifiers()

        all_identifiers = placeholder_identifiers.union(component_identifiers)

        comp_schema = FlowIR.type_flowir_component(flavor='full')
        platforms = self.platforms

        # VV: FIXME This is a temporary trick till we enforce people to record *all* appllication-sources except input:
        # data, bin, hooks, and conf
        top_level_folders = list(top_level_folders or [])
        top_level_folders.extend(map(FlowIR.application_dependency_to_name, self.get_application_dependencies()))

        for comp_id in component_identifiers:
            try:
                comp = self.get_component(comp_id)
                # VV: Validate the raw definition of import components and the
                #     fully extended FlowIR configuration of actual components (import
                #     components have just bindings, name, stage, and import fields)
                if '$import' not in comp:
                    comp = self.get_component_configuration(
                        comp_id, include_default=True, is_primitive=True, raw=False)
            except Exception as e:
                out_errors.append(e)
            else:
                comp_errors = FlowIR.validate_component(
                    comp, comp_schema=comp_schema, component_ids=all_identifiers, known_platforms=platforms,
                    top_level_folders=top_level_folders)

                out_errors.extend(comp_errors)
                env_name = comp.get('command', {}).get('environment', '').lower()
                if env_name not in ['', 'none']:
                    # VV: `None` and `''` resolves to the environment variables of the workflow orchestrator process
                    # `"environment" resolves to the "environment" environment in the active platform, if not specified
                    #   it resolves to an empty set of environmetns
                    # `"none"` resolves to an EMPTY set of environment variables
                    # others resolve to the environment name
                    try:
                        _ = self.get_environment(env_name, strict_checks=True)
                    except experiment.model.errors.FlowIREnvironmentUnknown as e:
                        # VV: environments are also inheritted, if `env_name` is not defined for active platform,
                        # look for it in the default platform before registering that this is an unknown environment
                        if e.platform != FlowIR.LabelDefault:
                            try:
                                _ = self.get_environment(env_name, strict_checks=False)
                            except experiment.model.errors.FlowIREnvironmentUnknown as e:
                                flowirLogger.info(self.environments())
                                out_errors.append(e)
                        else:
                            out_errors.append(e)
                    except Exception as e:
                        out_errors.append(e)

        return out_errors

    @property
    def active_platform(self):
        # type: () -> str
        """Returns name of active platform, if platform is not set returns `default`"""
        return self._platform or FlowIR.LabelDefault

    def refresh_component_dictionary(self):
        """Builds a dictionary to make component lookup fast.
        """
        self._component_dictionary.clear()

        for i, component in enumerate(self._flowir[FlowIR.FieldComponents]):
            try:
                comp_id = (component['stage'], component['name'])
            except KeyError:
                expected = {'stage': 'int', 'name': 'str'}
                missing = {x:expected[x] for x in expected if x not in component}
                raise experiment.model.errors.FlowIRInvalidComponent(
                    missing=missing, extra={}, invalid={}, componentName=component.get('name'),
                    stage_index=component.get('stage'), message='Component does not have a valid ID')
            if comp_id in self._component_dictionary:
                raise experiment.model.errors.FlowIRInconsistency(
                      'Component stage%s.%s exists multiple times in FlowIR description' % (
                        comp_id[0], comp_id[1]
                      ), self._flowir, None, ('comp_id_too_many', component)
                  )
            self._component_dictionary[comp_id] = component

    def replicate(self, platform=None, ignore_errors=False, top_level_folders=None):
        # type: (str, bool, List[str]) -> DictFlowIR
        """Replicates a primitive FlowIRConcrete

        Arguments:
            platform: (optional) name of platform to use for the replicated FlowIR
            ignore_errors: Set to True to ignore errors
            top_level_folders: List of names for top-level folders in package/instance

        Returns
            A DictFlowIR containing the raw replicated FlowIR
        """
        platform = platform or self._platform
        instance = self.instance(platform, ignore_errors=ignore_errors, fill_in_all=False)

        platform_variables = instance[FlowIR.FieldVariables][FlowIR.LabelDefault]
        instance[FlowIR.FieldComponents] = FlowIR.apply_replicate(
            instance[FlowIR.FieldComponents], platform_variables, False, self.get_application_dependencies(),
            top_level_folders=top_level_folders)

        return instance

    def update_application_dependencies(self, app_deps, platform=None):
        # type: (List[str], Optional[str]) -> None
        platform = platform or self._platform

        if platform not in self.platforms:
            raise experiment.model.errors.FlowIRPlatformUnknown(platform, self._flowir)

        self._flowir[FlowIR.FieldApplicationDependencies][platform] = app_deps

    def get_application_dependencies(self, platform=None):
        # type: (Optional[str]) -> List[str]
        platform = platform or self._platform

        app_deps = self._flowir.get(FlowIR.FieldApplicationDependencies, {})

        try:
            platform_deps = app_deps[platform]
        except KeyError:
            # VV: This platform does not define an `application-depedencies`
            if platform != FlowIR.LabelDefault:
                # VV: If it's not the `default` platform then just fetch the app-deps of the default platform
                platform_deps = app_deps.get(FlowIR.LabelDefault, [])
            else:
                # VV: If this is the default platform then assume that there're no `application-dependencies` entries
                platform_deps = []

        return platform_deps

    def get_virtual_environments(self, platform=None):
        # type: (Optional[str]) -> List[str]
        platform = platform or self._platform

        venvs = self._flowir.get(FlowIR.FieldVirtualEnvironments, {})
        global_venvs = venvs.get(FlowIR.LabelDefault, [])

        platform_venvs = venvs.get(platform, [])

        platform_ids = [
            FlowIR.extract_id_of_folder(entry) for entry in platform_venvs
        ]

        venvs = platform_venvs + [
            venv for venv in global_venvs if FlowIR.extract_id_of_folder(venv) not in platform_ids
        ]

        return venvs

    def get_output(self, return_copy=True) -> DictFlowIROutput:
        """Returns a dictionary with the specification of the key-outputs

        The format of the dictionary is:

        <keyOutputName>:
            data-in: a reference to a file that a component produces (reference method :ref/:copy)
            description (optional): human readable text
            stages (optional but required data-in does not contain a "stage%d." prefix):
            - stage0
            - 1

        Notice that:

        1. IF data-in contains a "stage%d." prefix then then "stages" MUST NOT be set
        2. IF data-in does not contain a "stage%d." prefix, then "stages" MUST be set

        Args:
            return_copy: If True returns a copy, otherwise the actual dictionary. Defaults to True

        Returns:
            A dictionary of dictionaries
        """
        try:
            output = self._flowir[FlowIR.FieldOutput]
        except KeyError as e:
            raise experiment.model.errors.FlowIRInconsistency('Missing field %s' % FlowIR.FieldOutput, self._flowir, e)

        if return_copy:
            output = deep_copy(output)

        return output

    def get_status(self, return_copy=True):
        try:
            status = self._flowir[FlowIR.FieldStatusReport]
        except KeyError as e:
            raise experiment.model.errors.FlowIRInconsistency(
                'Missing field %s' % FlowIR.FieldStatusReport, self._flowir, e
            )

        if return_copy:
            status = deep_copy(status)

        return status

    def get_stage_number(self):
        # type: () -> int
        try:
            components = self._flowir[FlowIR.FieldComponents]
        except KeyError as e:
            raise experiment.model.errors.FlowIRInconsistency(
                'Missing field %s' % FlowIR.FieldComponents, self._flowir, e
            )

        stages = set()
        for comp in components:
            stages.add(comp['stage'])

        assert list(range(len(stages))) == sorted(stages)

        return len(stages)

    def get_stage_description(self, stage_index):
        #  type: (int) -> Tuple[List[str], Dict[str, str]]
        """ Returns a) Component names, and b) Stage Options.
        """
        component_ids = self.get_component_identifiers(False)

        stage_components = [comp_id for comp_id in component_ids if comp_id[0] == stage_index]
        stage_component_names = [comp_id[1] for comp_id in stage_components]
        stage_variables = self.get_default_stage_variables(stage_index)

        non_option_keys = [key for key in stage_variables if key not in FlowIR.stageOptions]
        for key in non_option_keys:
            del stage_variables[key]

        return stage_component_names, deep_copy(stage_variables)

    @property
    def numberStageConfigurations(self):
        # VV: TODO Come up with a way to tag FlowIR as dirty so that we can cache the numberStageConfigurations
        #     value for as long as FlowIR is clean and recompute it when it's dirty

        stages = set()  # type: Set[int]

        try:
            components = self._flowir[FlowIR.FieldComponents]
        except KeyError as e:
            raise experiment.model.errors.FlowIRInconsistency(
                'Missing field %s' % FlowIR.FieldComponents, self._flowir, e
            )

        for comp in components:
            stages.add(int(comp['stage']))

        return len(stages)

    def copy(self):
        # type: () -> FlowIRConcrete
        return FlowIRConcrete(
            self._flowir,
            self._platform,
            self._documents
        )

    def configure_platform(
            self,  # type: FlowIRConcrete
            platform,  # type: Optional[str]
    ):
        # type: (...) -> None
        """Converts Level0 graph to Level1 graph (platform-specific)"""
        # VV: First setup logistics
        self._platform = platform or FlowIR.LabelDefault

    def raw(self):
        # type: () -> DictFlowIR
        """Converts self._flowir to Level0 graph (i.e. `raw` dict)"""

        return deep_copy(self._flowir)

    def get_component_variable_references(self, comp_id, include_default=True):
        component = self.get_component_configuration(comp_id, raw=True, include_default=include_default)
        return FlowIR.discover_references_to_variables(component)

    def instance(
            self, platform=None, ignore_errors=False,
            fill_in_all=False, is_primitive=False, inject_missing_fields=True
    ):
        # type: (Optional[str], bool, bool, bool, bool) -> DictFlowIR
        """Generates instance FlowIR for a specific platform.

        Args:
            platform: If not set defaults to `default` (FlowIR.FieldDefault)
            ignore_errors: The selected platform can be incorrect, when this option is set to True instance() will
            not stop if it encounters any errors during the process (e.g. during interpolation or type conversion)
            fill_in_all: When set to True component variables will be augmented to include stage/global variables
            too. When set to False the only variables that will exist in a component will be those defined in its
            scope. Specifically for DOSINI configuration the component variables will also include user-defined
            variables which have been read from the `input/variables.conf` file (this happens in the DOSINI frontend
            during parsing and cannot be undone).

        Returns: A fully developed FlowIR description (DictFlowIR)

        """

        platform = platform or self._platform

        possible_errors = []

        # VV: resolve global variables
        global_default_variables = self.get_default_global_variables() if platform != FlowIR.LabelDefault else {}
        global_platform_variables = self.get_platform_global_variables(platform)

        global_variables = global_default_variables
        global_variables.update(global_platform_variables)

        # VV: Resolve global variables
        for name in global_variables:
            try:
                global_variables[name] = FlowIR.interpolate(
                    global_variables[name], global_variables, label='variables.default.global.%s' % name
                )
            except experiment.model.errors.FlowIRVariableUnknown as e:
                flowirLogger.warning('While interpolating global variables: %s' % e.message)
                possible_errors.append(e)

        # VV: Group components into stages
        comp_identifiers = self.get_component_identifiers(recompute=True, include_documents=True)
        comp_stages = {}

        for comp_id in comp_identifiers:
            stage, name = comp_id
            if stage not in comp_stages:
                comp_stages[stage] = []

            comp = self.get_component(comp_id)

            # VV: Document components (like DoWhile) do not have fields like resourceManager, just [bindings, $import,
            #     name, stage]
            if '$import' not in comp:
                comp = self.get_component_configuration(
                    comp_id, raw=True, include_default=False, platform=platform,
                    inject_missing_fields=inject_missing_fields, is_primitive=is_primitive
                )

            # VV: Fetch component FlowIR after applying blueprint inheritance without including any default variables
            comp_stages[stage].append(comp)

        # VV: Resolve stage variables
        stage_variables = {}

        for stage_index in comp_stages:
            default_stage_vars = (
                self.get_default_stage_variables(stage_index)
            )

            # VV: Global platform variables should override those defined in the stage
            #     (if the platform is the default one then the stage variables should always override global ones)
            if platform != FlowIR.LabelDefault:
                default_stage_vars = {
                    key: default_stage_vars[key] for key in default_stage_vars if key not in global_platform_variables
                }

            platform_stage_vars = self.get_platform_stage_variables(stage_index, platform)

            this_stage_vars = default_stage_vars
            this_stage_vars.update(platform_stage_vars)

            stage_variables[stage_index] = this_stage_vars

            context = global_variables.copy()
            context.update(this_stage_vars)

            for name in this_stage_vars:
                try:
                    this_stage_vars[name] = FlowIR.interpolate(
                        this_stage_vars[name], context, label='variables.default.stages.%d.%s' % (stage_index, name),
                        is_primitive=is_primitive
                    )
                except experiment.model.errors.FlowIRVariableUnknown as e:
                    flowirLogger.warning('While interpolating stage index variables: %s' % e.message)
                    possible_errors.append(e)

        # VV: Resolve environments
        try:
            default_environments = (
                self.get_environments(platform=FlowIR.LabelDefault) if platform != FlowIR.LabelDefault else {}
            )
        except experiment.model.errors.FlowIRPlatformUnknown:
            default_environments = {}

        platform_environments = self.get_environments(platform)

        environments = default_environments
        environments.update(platform_environments)

        global_variables = FlowIR.fill_in(
            global_variables, context=global_variables, flowir=self._flowir, ignore_errors=True,
            label='global variables for environments', is_primitive=is_primitive
        )

        for env_name in environments:
            env_variables = global_variables.copy()
            env_variables.update(environments[env_name])

            # VV: FIXME Deprecate this behaviour!
            #     In DOSINI format, environments are allowed to access the env-variables that they define using
            #     variable references
            environments[env_name] = FlowIR.fill_in(
                environments[env_name], context=environments[env_name], flowir=self._flowir, ignore_errors=True,
                label='environments.default.%s' % env_name, is_primitive=is_primitive
            )

            environments[env_name] = FlowIR.fill_in(
                environments[env_name], context=env_variables, flowir=self._flowir, ignore_errors=ignore_errors,
                label='environments.default.%s' % env_name, is_primitive=is_primitive
            )

        # VV: Resolve components
        components = []

        for stage_index in comp_stages:

            for comp in comp_stages[stage_index]:
                if '$import' in comp:
                    components.append(comp)
                    continue

                comp_variables = comp.get('variables', {})

                context = global_variables.copy()
                context.update(stage_variables[stage_index].copy())
                context.update(comp_variables)

                if fill_in_all is False:
                    comp['variables'] = self.get_component_variables(
                        (stage_index, comp['name']), platform,
                        include_default_global=False,
                        include_platform_stage=False,
                        include_default_stage=False,
                        include_platform_global=False
                    )

                    comp['variables'] = FlowIR.fill_in(
                        comp['variables'], context=context, flowir=self._flowir, ignore_errors=ignore_errors,
                        label='components.stage%d.%s.variables' % (comp['stage'], comp['name']),
                        is_primitive=is_primitive
                    )
                else:
                    comp['variables'] = comp_variables
                    comp = FlowIR.fill_in(
                        comp, context=context, flowir=self._flowir, ignore_errors=ignore_errors,
                        label='components.stage%d.%s.variables' % (comp['stage'], comp['name']),
                        is_primitive=is_primitive
                    )

                FlowIR.convert_component_types(
                    comp, ignore_convert_errors=ignore_errors, is_primitive=is_primitive
                )

                # VV: keep just the override options for the currently selected platform
                if 'override' in comp:
                    if platform in comp['override']:
                        # VV: keep just the one platform
                        comp['override'] = {
                            platform: comp['override'][platform]
                        }
                    else:
                        del comp['override']

                components.append(comp)

        # VV: Resolve global blueprint
        global_blueprint = self.get_default_global_blueprint()
        global_blueprint = FlowIR.override_object(global_blueprint, self.get_platform_blueprint(platform))

        global_blueprint = FlowIR.fill_in(
            global_blueprint, context=global_variables, flowir=self._flowir, ignore_errors=ignore_errors,
            label='blueprint.default.global', is_primitive=is_primitive
        )

        # VV: Repeat for the stage blueprints
        stages_blueprint = {}
        for stage_index in stage_variables:
            global_stage_blueprint = (
                self.get_default_stage_blueprint(stage_index)
            )
            platform_stage_blueprint = self.get_platform_stage_blueprint(stage_index, platform)

            stage_blueprint = FlowIR.override_object(global_stage_blueprint, platform_stage_blueprint)

            context = global_variables.copy()
            context.update(stage_variables[stage_index].copy())

            stage_blueprint = FlowIR.fill_in(
                stage_blueprint, context=context, flowir=self._flowir, ignore_errors=ignore_errors,
                label='blueprint.default.stages.%d' % (stage_index), is_primitive=is_primitive
            )

            stages_blueprint[stage_index] = stage_blueprint

        # VV: Resolve the remaining bits and pieces of FlowIR
        output = FlowIR.fill_in(
            self.get_output(), context=global_variables, flowir=self._flowir, ignore_errors=ignore_errors,
            label='output', is_primitive=is_primitive
        )

        status = FlowIR.fill_in(
            self.get_status(), context=global_variables, flowir=self._flowir, ignore_errors=ignore_errors,
            label='status-report', is_primitive=is_primitive
        )

        default_virtual_environments = (
            self.get_virtual_environments(FlowIR.LabelDefault) if platform != FlowIR.LabelDefault else []
        )
        platform_virtual_environments = self.get_virtual_environments(platform)

        virtual_environments = default_virtual_environments
        for venv in platform_virtual_environments:
            if venv not in virtual_environments:
                virtual_environments.append(venv)

        virtual_environments = FlowIR.fill_in(
            virtual_environments, context=global_variables, flowir=self._flowir,
            ignore_errors=ignore_errors, label='virtual-environments', is_primitive=is_primitive
        )

        interface = self.get_interface()

        application_dependencies = self.get_application_dependencies(platform)

        application_dependencies = FlowIR.fill_in(
            application_dependencies, context=global_variables, flowir=self._flowir,
            ignore_errors=ignore_errors, label='application-dependencies', is_primitive=is_primitive
        )

        # VV: put everything back together
        flowir = {
            FlowIR.FieldVariables: {
                FlowIR.LabelDefault: {
                    FlowIR.LabelGlobal: global_variables,
                    FlowIR.LabelStages: stage_variables,
                }
            },
            FlowIR.FieldEnvironments: {
                FlowIR.LabelDefault: environments,
            },
            FlowIR.FieldComponents: components,
            FlowIR.FieldBlueprint: {
                FlowIR.LabelDefault: {
                    FlowIR.LabelGlobal: global_blueprint,
                    FlowIR.LabelStages: stages_blueprint,
                }
            },
            FlowIR.FieldPlatforms: sorted(set([FlowIR.LabelDefault, platform])),
            FlowIR.FieldOutput: output,
            FlowIR.FieldStatusReport: status,
            FlowIR.FieldVirtualEnvironments: {
                FlowIR.LabelDefault: virtual_environments
            },
            FlowIR.FieldApplicationDependencies: {
                FlowIR.LabelDefault: application_dependencies
            },
            FlowIR.FieldInterface: interface,
        }

        return flowir

    @property
    def platforms(self):
        return FlowIR.discover_platforms(self._flowir)

    def environments(self, platform=None):
        # type: (Optional[str]) -> Dict[str, Dict[str, str]]
        """Returns copy of all environments of Level1 graph for specific platform"""
        platform = platform or self._platform

        try:
            environments = self._flowir[FlowIR.FieldEnvironments]
        except KeyError as e:
            raise experiment.model.errors.FlowIRInconsistency(
                'Missing %s field' % FlowIR.FieldEnvironments, self._flowir, e
            )

        default_environments = {}

        if platform != FlowIR.LabelDefault:
            try:
                default_environments = environments[FlowIR.LabelDefault]
            except KeyError:
                pass

        try:
            environments_platform = deep_copy(environments[platform])
        except KeyError:
            raise experiment.model.errors.FlowIRPlatformUnknown(platform, self._flowir)

        for env in environments_platform:
            if env in default_environments:
                from_default = deep_copy(default_environments[env])

                from_default.update(environments_platform[env])
                environments_platform[env] = from_default

        for env in default_environments:
            if env not in environments_platform:
                environments_platform[env] = default_environments[env]

        return environments_platform

    def add_platform(self, platform):
        platform = platform or 'default'

        if platform in self.platforms:
            raise experiment.model.errors.FlowIRPlatformExists(platform, self.platforms)
        try:
            cast(List[str], self._flowir[FlowIR.FieldPlatforms]).append(platform)
            cast(Dict[str, Dict[str, str]], self._flowir[FlowIR.FieldEnvironments])[platform] = {}
            cast(Dict[str, Dict[str, str]], self._flowir[FlowIR.FieldVariables])[platform] = {}
        except Exception as exc:
            raise experiment.model.errors.FlowIRInconsistency(
                'Failed to add new platform "%s"' % platform, self._flowir, exc
            )

    def get_environments(
            self,  # type: FlowIRConcrete
            platform=None,  # type: Optional[str]
    ):
        # type: (...) -> Dict[str, Dict[str, str]]
        platform = platform or self._platform

        if platform not in self.platforms:
            raise experiment.model.errors.FlowIRPlatformUnknown(platform, self._flowir)

        try:
            platform_environments = self._flowir[FlowIR.FieldEnvironments]
        except KeyError:
            raise experiment.model.errors.FlowIRInconsistency(
                'Missing "environments" field', self._flowir
            )
        try:
            platform_environments = platform_environments[platform]
        except KeyError:
            raise experiment.model.errors.FlowIRPlatformUnknown(platform, self._flowir)

        platform_environments = deep_copy(platform_environments)
        return platform_environments

    def get_environment(
            self,
            name: str,
            platform: str | None = None,
            strict_checks: bool = False,
    ) -> Dict[str, str]:
        platform = platform or self._platform

        platform_environments = self.get_environments(platform)

        name = name.lower()

        if platform != FlowIR.LabelDefault:
            try:
                default_environment = self.get_environment(name, FlowIR.LabelDefault)
            except experiment.model.errors.FlowIREnvironmentUnknown:
                default_environment = None
        else:
            default_environment = {}

        try:
            environment = platform_environments[name]
        except KeyError:
            # VV: It's valid for an environment to be global (i.e. non-platform specific)
            if default_environment is None or strict_checks is True:
                raise experiment.model.errors.FlowIREnvironmentUnknown(
                    name, platform, self._flowir
                )
            else:
                environment = {}

        default_environment = default_environment or {}
        default_environment.update(environment)

        def env_value_to_string(value: Any) -> str:
            if value is None:
                return ""
            return str(value)

        default_environment = {str(x): env_value_to_string(default_environment[x]) for x in default_environment}

        return default_environment

    def set_environment(
            self,  # type: FlowIRConcrete
            name,  # type: str
            update,  # type: Dict[str, str]
            platform=None,  # type: Optional[str]
    ):
        # type: (...) -> None
        platform = platform or self._platform
        name = name.lower()
        self._flowir[FlowIR.FieldEnvironments][platform][name] = update

    def add_environment(
            self,  # type: FlowIRConcrete
            name,  # type: str
            environment,  # type: Dict[str, str]
            platform=None,  # type: Optional[str]
    ):
        # type: (...) -> None
        platform = platform or self._platform
        name = name.lower()
        try:
            _ = self.get_environment(name, platform, strict_checks=True)
        except experiment.model.errors.FlowIREnvironmentUnknown:
            # VV: this is a brand new environment
            self._flowir[FlowIR.FieldEnvironments][platform][name] = environment
        else:
            raise experiment.model.errors.FlowIREnvironmentExists(name, platform, self._flowir)

    def get_component_variables(
            self,  # type: FlowIRConcrete
            comp_id,  # type: Tuple[int, str]
            platform=None,  # type: Optional[str]
            include_default_global=True,
            include_default_stage=True,
            include_platform_global=True,
            include_platform_stage=True,
            include_platform_override=True,
    ):
        # type: (...) -> Dict[str, Any]
        """Returns copy of variables for component with identifier `comp_id`

        VV: Order of inheritance:
            1. Default global variables (configuration)
            2. Default stage variables (configuration)
            3. Platform global variables (configuration)
            4. Platform stage variables (configuration)
            5. Component variables (configuration)
        """
        platform = platform or self._platform

        if platform not in self.platforms:
            raise experiment.model.errors.FlowIRPlatformUnknown(platform, self._flowir)

        stage_index, comp_name = comp_id

        # VV: Request for copies of the various scopes to avoid invalidating the FlowIR cache; because the FlowIRCache
        # significantly reduces the overhead of resolving cascading FlowIR configurations
        component = self.get_component(comp_id, return_copy=True)

        variables = {}
        if include_default_global:
            variables.update(self.get_default_global_variables(return_copy=True))
        if include_default_stage:
            variables.update(self.get_default_stage_variables(stage_index, return_copy=True))
        if include_platform_global and platform != FlowIR.LabelDefault:
            variables.update(self.get_platform_global_variables(platform, return_copy=True))
        if include_platform_stage and platform != FlowIR.LabelDefault:
            variables.update(self.get_platform_stage_variables(stage_index, platform, return_copy=True))
        variables.update(component.get('variables', []))

        if include_platform_override:
            variables.update(component.get('override', {}).get(platform, {}).get('variables', {}))

        return variables

    def get_component_variable(
            self,  # type: FlowIRConcrete
            comp_id,  # type: Tuple[int, str]
            variable,  # type: str
    ):
        return self.get_component_variables(comp_id)[variable]

    def _get_component_variables_ref(self, comp_id, return_copy):
        # type: (Tuple[int, str], bool) -> Dict[str, Any]
        comp = self.get_component(comp_id, return_copy)
        try:
            variables = comp['variables']
        except KeyError as e:
            raise experiment.model.errors.FlowIRInconsistency(
                'Missing component field %s for %s' % (
                    'variables', str(comp_id)
                ), self._flowir, e, (comp_id, )
            )

        return cast(Dict[str, Any], variables)

    def remove_component_option(self, comp_id, option_route):
        if '#' not in option_route:
            self.delete_component_variable(comp_id, option_route)
        else:
            option_route = option_route[1:]
            comp_desc = self.get_component(comp_id, return_copy=False)

            route = option_route.split('.')
            context = comp_desc

            for point in route[:-1]:
                context = context[point]

            variable_name = route[-1]
            del context[variable_name]

    def set_component_option(self, comp_id, option_route, value):
        if '#' not in option_route:
            self.set_component_variable(comp_id, option_route, value)
        else:
            option_route = option_route[1:]
            comp_desc = self.get_component(comp_id, return_copy=False)

            route = option_route.split('.')
            context = comp_desc

            for point in route[:-1]:
                context = context[point]

            variable_name = route[-1]
            context[variable_name] = value

    def set_component_variable(
            self,  # type: FlowIRConcrete
            comp_id,  # type: Tuple[int, str]
            variable,  # type: str
            value,  # type: Any
    ):
        # VV: All changes to component variables are only visible to the component,
        #     e.g. if the component was accessing `variable` through the `GLOBAL` variables
        #          modifying the value of said `variable` will create a new instance of it
        #          that's only visible by `comp_id` (see get_component_variables())
        variables = self._get_component_variables_ref(comp_id, False)
        variables[variable] = value

    def delete_component_variable(
            self,  # type: FlowIRConcrete
            comp_id,  # type: Tuple[int, str]
            variable,  # type: str
    ):
        variables = self._get_component_variables_ref(comp_id, False)

        try:
            del variables[variable]
        except KeyError as e:
            raise experiment.model.errors.FlowIRVariableUnknown(
                variable, variables, self._flowir
            )

    def get_placeholder_identifiers(self):
        # type: () -> Set[FlowIRComponentId]
        ret = set()

        do_whiles = self._documents.get(FlowIR.LabelDoWhile, {})

        for dw_name in do_whiles:
            dw = do_whiles[dw_name]
            # VV: Fake a :ref reference
            ret = ret.union([(int(comp.get('stage', dw['stage'])), comp['name']) for comp in dw.get('components', [])])

        return ret

    def get_component_identifiers(self, recompute, include_documents=False):
        # type: (bool, bool) -> Set[FlowIRComponentId]
        if recompute is False:
            all_keys = set(self._component_dictionary.keys())

            if include_documents is False:
                # VV: Do not return components which instantiate documents

                for lbl in self._documents:
                    for doc_key in self._documents[lbl]:
                        try:
                            index, name, has_index = FlowIR.ParseProducerReference(doc_key)
                            if has_index:
                                all_keys.remove((index, name))
                        except Exception:
                            pass

            return all_keys

        return self._get_real_component_identifiers(include_documents)

    def _get_real_component_identifiers(self, include_documents=False):
        # type: (bool) -> Set[FlowIRComponentId]

        try:
            components = self._flowir[FlowIR.FieldComponents]
        except KeyError as e:
            raise experiment.model.errors.FlowIRInconsistency(
                'Missing field %s' % FlowIR.FieldComponents, self._flowir, e, ('get_component_identifiers', )
            )

        comp_identifiers = set()
        duplicates = set()

        for comp in components:
            try:
                if include_documents is False and '$import' in comp:
                    continue

                comp_id = (comp['stage'], comp['name'])
            except Exception as e:
                raise experiment.model.errors.FlowIRInconsistency(
                    'Invalid component ID', self._flowir, e, ('get_component_identifiers_aggregate', comp)
                )
            if comp_id in comp_identifiers:
                duplicates.add(comp_id)

            comp_identifiers.add(comp_id)

        if duplicates:
            raise experiment.model.errors.FlowIRInconsistency("Duplicate components: %s" % duplicates, self._flowir)

        return comp_identifiers

    def get_components(self, return_copy=True):
        # type: (bool) -> List[DictFlowIRComponent]

        try:
            components = self._flowir[FlowIR.FieldComponents]
        except KeyError as e:
            raise experiment.model.errors.FlowIRInconsistency(
                'Missing field %s' % FlowIR.FieldComponents, self._flowir, e
            )

        if return_copy:
            return deep_copy(components)
        else:
            return components

    def get_default_global_blueprint(self):
        # type: () -> DictFlowIRComponent
        """Returns default-platform blueprint for all components in package.
        """
        return self.get_platform_blueprint(FlowIR.LabelDefault)

    def get_default_stage_blueprint(self, stage_index):
        # type: (int) -> DictFlowIRComponent
        """Returns default-platform blueprint for components in stage @stage_index.
        """
        return self.get_platform_stage_blueprint(stage_index, FlowIR.LabelDefault)

    def get_platform_blueprint(self, platform=None):
        # type: (Optional[str]) -> DictFlowIRComponent
        """Returns platform blueprint for all components in package.
        """
        platform = platform or self._platform
        
        if platform not in self.platforms:
            raise experiment.model.errors.FlowIRPlatformUnknown(platform, self._flowir)
        
        blueprint = self._flowir.get(FlowIR.FieldBlueprint, {})

        platform_blueprint = blueprint.get(platform, {})

        template = platform_blueprint.get(FlowIR.LabelGlobal, {})

        return deep_copy(template)

    def get_platform_stage_blueprint(self, stage_index, platform=None):
        # type: (int, Optional[str]) -> DictFlowIRComponent
        """Returns platform blueprint for components in stage @stage_index.
        """
        platform = platform or self._platform

        if platform not in self.platforms:
            raise experiment.model.errors.FlowIRPlatformUnknown(platform, self._flowir)

        blueprint = self._flowir.get(FlowIR.FieldBlueprint, {})

        platform_blueprint = blueprint.get(platform, {})

        stages_blueprint = platform_blueprint.get(FlowIR.LabelStages, {})

        template = stages_blueprint.get(stage_index, {})

        return deep_copy(template)

    def get_component_configuration(self, comp_id, raw=False, include_default=False,
                                    platform=None, ignore_convert_errors=False, is_primitive=False,
                                    inject_missing_fields=True):
        # type: (FlowIRComponentId, bool, bool, Optional[str], bool, bool, bool) -> DictFlowIRComponent
        """Returns FlowIR configuration for component (applies FlowIR inheritance)

         VV: Order of inheritance:
            1. Builtin flow blueprint
            2. Default global blueprint (configuration)
            3. Default stage blueprint (configuration)
            4. Platform global blueprint (configuration)
            5. Platform stage blueprint (configuration)
            6. Component definition (configuration)
            7. Component override for specific platform (configuration)
            8. Resolve interpreter option which may affect command.executable and command.arguments
        """
        platform = platform or self._platform

        need_fully_resolved_flowir = (raw is False and inject_missing_fields
                                      and include_default and is_primitive is False)
        try:
            cache_label = 'component:%s:stage%s:%s' % (platform, comp_id[0], comp_id[1])
        except Exception as e:
            flowirLogger.critical('Failed to create component-cache label: %s-%s-%s' % (
                platform, comp_id[0], comp_id[1]))
            raise experiment.model.errors.InternalInconsistencyError(str(traceback.format_exc()) + '\n%s' % str(e))

        if need_fully_resolved_flowir:
            if self._cache.in_cache(cache_label):
                return self._cache[cache_label]

        component = self.get_component(comp_id)
        variables = self.get_component_variables(
            comp_id,
            platform=platform,
            include_default_stage=include_default,
            include_platform_stage=include_default,
            include_default_global=include_default,
            include_platform_global=include_default,
        )

        sequence = []
        if inject_missing_fields:
            flow_defaults = FlowIR.inject_default_values_to_component({})
            sequence = [flow_defaults]

        global_blueprint = self.get_default_global_blueprint()
        global_stage_blueprint = self.get_default_stage_blueprint(comp_id[0])
        platform_global_blueprint = self.get_platform_blueprint(platform)
        platform_stage_blueprint = self.get_platform_stage_blueprint(comp_id[0], platform)

        ret = {}
        sequence += [
            global_blueprint, global_stage_blueprint,
            platform_global_blueprint, platform_stage_blueprint, component
        ]

        comp_platform_override = component.get('override', {}).get(platform, None)

        if comp_platform_override:
            sequence.append(deep_copy(comp_platform_override))

        for override in sequence:
            ret = FlowIR.override_object(ret, override)

        # VV: Perform interpreter modifications *after* the component override so that the digest_interpreter_field
        #     uses the the top-most layer of information (i.e. the overriden description of the component)
        if inject_missing_fields:
            interpreter_modification = FlowIR.digest_interpreter_field(ret)
            ret = FlowIR.override_object(ret, interpreter_modification)

        #VV: The variables have already been taken care off, there's no need to go through them again
        #    just insert them after fully resolving the FlowIR description of the component
        ret['variables'] = variables

        if raw is False:
            full_name = 'components.stage%s.%s' % comp_id
            if need_fully_resolved_flowir:
                # VV: Add anything that is missing just before applying string interpolation
                ret = FlowIR.inject_default_values_to_component(ret, True)

            ret = FlowIR.fill_in(ret, variables, flowir=self._flowir, label=full_name, is_primitive=is_primitive)
            FlowIR.convert_component_types(
                ret, ignore_convert_errors=ignore_convert_errors, is_primitive=is_primitive
            )

        # VV: Interpreters will *never* expand their arguments
        if ret.get('command', {}).get('interpreter', None) is not None:
            ret['command']['expandArguments'] = 'none'

        # VV: If this is a fully resolved flowir-configuration then store it in the cache
        if need_fully_resolved_flowir:
            self._cache[cache_label] = deep_copy(ret)

        return ret

    def invalidate_cache_for_component(self, comp_id):
        self._cache.invalidate_reg_expression(r'component:.*:stage%s:%s' % (
            comp_id[0], comp_id[1]))

    def update_component(self, comp_id, new_flowir):
        # type: (FlowIRComponentId, DictFlowIRComponent) -> None
        if comp_id not in self._component_dictionary:
            raise experiment.model.errors.FlowIRComponentUnknown(comp_id, self._flowir)

        with self._cache._lock:
            component = self._component_dictionary[comp_id]

            component.clear()
            component.update(new_flowir)

            self.invalidate_cache_for_component(comp_id)

    def get_component(self, comp_id, return_copy=True):
        # type: (FlowIRComponentId, bool) -> DictFlowIRComponent
        """Returns JUST the FlowIR information contained in the component definition.

        Use get_component_configuration() to get the full FlowIR definition of the component.
        """

        if comp_id not in self._component_dictionary:
            raise experiment.model.errors.FlowIRComponentUnknown(comp_id, self._flowir)

        component = self._component_dictionary[comp_id]

        if return_copy:
            return deep_copy(component)

        self._cache.invalidate_reg_expression(r"component:.*:stage%s:%s" % (comp_id[0], comp_id[1]))
        return component

    def delete_component(self, comp_id, ignore_errors=False):
        # type: (Tuple[int, str], bool) -> None
        try:
            try:
                components = (cast(List[DictFlowIRComponent], self._flowir[FlowIR.FieldComponents]))
            except KeyError as e:
                raise experiment.model.errors.FlowIRInconsistency(
                    'Missing %s field' % FlowIR.FieldComponents, self._flowir, e
                )

            for comp in components:
                if comp['stage'] == comp_id[0] and comp['name'] == comp_id[1]:
                    components.remove(comp)
                    break
            else:
                raise experiment.model.errors.FlowIRComponentUnknown(comp_id, self._flowir)

            try:
                del self._component_dictionary[comp_id]
            except KeyError:
                pass

            self._cache.invalidate_reg_expression(r'component:.*:stage%s:%s' % (
                comp['stage'], comp['name']
            ))
        except:
            if ignore_errors is False:
                raise

    def add_component(self, description, insert_copy=True):
        # type: (DictFlowIRComponent, bool) -> None
        description = {} or description

        if insert_copy:
            description = deep_copy(description)

        # VV: Ensure that the component has all of its required features (but don't inject any default values)
        description = FlowIR.inject_default_values_to_component(description, all_values=False)

        comp_id = (description['stage'], description['name'])

        try:
            _ = self.get_component(comp_id)
        except experiment.model.errors.FlowIRComponentUnknown:
            pass
        else:
            raise experiment.model.errors.FlowIRComponentExists(comp_id, description, self._flowir)

        try:
            components = cast(List[DictFlowIRComponent], self._flowir[FlowIR.FieldComponents])
        except KeyError as e:
            raise experiment.model.errors.FlowIRInconsistency(
                'Missing field %s' % FlowIR.FieldComponents, self._flowir, e, ('add_component', description)
            )

        try:
            components.append(description)
            self._component_dictionary[(description['stage'], description['name'])] = description
        except Exception as e:
            reason = 'Failed to insert component %s' % str(comp_id)
            raise experiment.model.errors.FlowIRInconsistency(
                reason, self._flowir, e, ('add_component', description)
            )

    # VV: Global variables
    def get_default_global_variables(
            self,  # type: FlowIRConcrete
            return_copy=True,  # type: bool
    ):
        # type: (bool) -> Dict[str, str]

        return self.get_platform_global_variables(FlowIR.LabelDefault, return_copy=return_copy)

    def get_global_variable(
            self,  # type: FlowIRConcrete
            variable,  # type: str
    ):
        global_variables = self.get_default_global_variables()

        try:
            return global_variables[variable]
        except KeyError as e:
            raise experiment.model.errors.FlowIRVariableUnknown(
                variable, global_variables, self._flowir
            )

    def set_global_variable(
            self,  # type: FlowIRConcrete
            variable,  # type: str
            value,  # type: str
    ):
        self.set_platform_global_variable(variable, value, FlowIR.LabelDefault)
        self._cache.clear()

    def get_platform_variables(self, platform=None):
        # type: (Optional[str]) -> Dict[str, Any]
        """Returns dictionary containing global/stage variables for some platform.

        If platform is set to None will return the variables for active platform
        """
        variables = {
            FlowIR.LabelGlobal: FlowIR.override_object(
                self.get_default_global_variables(), self.get_platform_global_variables(platform)
            ),
            FlowIR.LabelStages: {
                stage_idx: FlowIR.override_object(
                    self.get_default_stage_variables(stage_idx),
                    self.get_platform_stage_variables(stage_idx, platform)
                ) for stage_idx in range(self.get_stage_number())
            }
        }

        return variables

    # VV: Platform Global variables
    def get_platform_global_variables(
            self,  # type: FlowIRConcrete
            platform=None,  # type: Optional[str]
            return_copy=True,  # type: bool
    ):
        # type: (Optional[str], bool) -> Dict[str, str]
        platform = platform or self._platform

        if platform not in self.platforms:
            raise experiment.model.errors.FlowIRPlatformUnknown(platform, self._flowir)

        if return_copy is False:
            self._cache.clear()

        try:
            variables = self._flowir[FlowIR.FieldVariables]
        except KeyError as e:
            raise experiment.model.errors.FlowIRInconsistency(
                'Missing field %s' % FlowIR.FieldVariables, self._flowir, e
            )
        if platform not in variables:
            variables[platform] = {}

        platform_variables = variables[platform]

        if FlowIR.LabelGlobal not in platform_variables:
            platform_variables[FlowIR.LabelGlobal] = {}

        platform_variables = platform_variables[FlowIR.LabelGlobal]

        if return_copy:
            platform_variables = deep_copy(platform_variables)

        return platform_variables

    def get_platform_global_variable(
            self,  # type: FlowIRConcrete
            variable,  # type: str
            platform=None,  # type: Optional[str]
    ):
        return self.get_platform_global_variables(platform)[variable]

    def set_platform_global_variable(
            self,  # type: FlowIRConcrete,
            variable,  # type: str
            value,  # type: str
            platform=None,  # type: Optional[str]
    ):
        platform = platform or self._platform
        if FlowIR.FieldVariables not in self._flowir:
            self._flowir[FlowIR.FieldVariables] = {}

        if platform not in self._flowir[FlowIR.FieldVariables]:
            self._flowir[FlowIR.FieldVariables][platform] = {}

        if FlowIR.LabelGlobal not in self._flowir[FlowIR.FieldVariables][platform]:
            self._flowir[FlowIR.FieldVariables][platform][FlowIR.LabelGlobal] = {}

        self._flowir[FlowIR.FieldVariables][platform][FlowIR.LabelGlobal][variable] = value

        self._cache.clear()

    # VV: Platform Stage variables
    def get_platform_stage_variables(
            self,  # type: FlowIRConcrete
            stage_index,  # type: int
            platform=None,  # type: Optional[str]
            return_copy=True,  # type: bool
    ):
        # type: (...) -> Dict[str, str]
        platform = platform or self._platform

        if return_copy is False:
            self._cache.clear()

        try:
            variables = self._flowir[FlowIR.FieldVariables]
        except KeyError as e:
            raise experiment.model.errors.FlowIRInconsistency(
                'Missing field %s' % FlowIR.FieldVariables, self._flowir, e, ('get_platform_stage_variables', )
            )

        try:
            platform_variables = variables[platform]
        except KeyError as e:
            raise experiment.model.errors.FlowIRPlatformUnknown(platform, self._flowir)

        try:
            stage_variables = platform_variables[FlowIR.LabelStages]
        except KeyError as e:
            raise experiment.model.errors.FlowIRInconsistency(
                'Missing field %s' % FlowIR.LabelStages, self._flowir, e, ('get_platform_stage_variables_stage',)
            )

        try:
            stage_variables = stage_variables[stage_index]
        except KeyError:
            stage_variables = {}

        if return_copy:
            stage_variables = deep_copy(stage_variables)

        return stage_variables

    def get_platform_stage_variable(
            self,  # type: FlowIRConcrete
            stage_index,  # type: int
            variable,  # type: str
            platform,  # type: Optional[str]
    ):
        return self.get_platform_stage_variables(stage_index, platform)[variable]

    def set_platform_stage_variable(
            self,  # type: FlowIRConcrete,
            stage_index,  # type: int
            variable,  # type: str
            value,  # type: str
            platform=None,  # type: Optional[str]
    ):
        platform = platform or self._platform
        if FlowIR.FieldVariables not in self._flowir:
            self._flowir[FlowIR.FieldVariables] = {}

        variables = self._flowir[FlowIR.FieldVariables]

        if platform not in variables:
            variables[platform] = {}

        variables = variables[platform]

        if FlowIR.LabelStages not in variables:
            variables[FlowIR.LabelStages] = {}

        stage_vars = variables[FlowIR.LabelStages]

        if stage_index not in stage_vars:
            stage_vars[stage_index] = {}

        stage_vars[stage_index][variable] = value

        self._cache.clear()

    # VV: Stage variables
    def get_default_stage_variables(
            self,  # type: FlowIRConcrete
            stage_index,  # type: int
            return_copy=True,  # type: bool
    ):
        return self.get_platform_stage_variables(stage_index, FlowIR.LabelDefault, return_copy=return_copy)

    def get_stage_variable(
            self,  # type: FlowIRConcrete
            stage_index,  # type: int
            variable,  # type: str
    ):
        return self.get_default_stage_variables(stage_index)[variable]

    def set_stage_variable(
            self,  # type: FlowIRConcrete
            stage_index,  # type: int
            variable,  # type: str
            value,  # type: str
    ):
        self._flowir[FlowIR.FieldVariables][FlowIR.LabelDefault][FlowIR.LabelStages][stage_index][variable] = value

        self._cache.clear()

    def get_global_variables(self):
        # type: () -> Dict[str, Any]
        """Returns all global variables (layers platform-global variables to default-global ones if necessary)

        Returns:
            A dictionary of key:value pairs, one for each of the global variables
        """

        vars_global = self.get_default_global_variables(return_copy=True)
        if self.active_platform != FlowIR.LabelDefault:
            vars_platform_global = self.get_platform_global_variables(return_copy=True)
            vars_global.update(vars_platform_global)

        return vars_global

    def get_stage_variables(self, stage):
        # type: (int) -> Dict[str, Any]
        """Returns all stage variables (layers platform-stage variables onto default-stage ones if necessary)

        Returns:
            A dictionary of key:value pairs, one for each of the stage variables
        """

        vars_stage = self.get_default_stage_variables(stage, return_copy=True)
        if self.active_platform != FlowIR.LabelDefault:
            vars_platform_stage = self.get_platform_stage_variables(stage, return_copy=True)
            vars_stage.update(vars_platform_stage)

        return vars_stage

    def get_workflow_variables(self):
        # type: () -> Dict[str, Any]
        """Returns global and stage variables (applies platform layering)

        Returns a dictionary with the following format:

        {
          'global: {var(str): value(Union[int, float, bool, str])}
          'stages': {stage(int): {var(str):value(Union[int, float, bool, str])}}
        }
        """
        num_stages = self.numberStageConfigurations

        all_variables = {
            'global': self.get_global_variables(),
            'stage': {
                stage_idx: self.get_stage_variables(stage_idx) for stage_idx in range(num_stages)
            }
        }

        return all_variables
