#!/usr/bin/env python
#
# coding=UTF-8
# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

"""Contains classes/functions for interacting with experiment data in databases

The primary use of the database is to enable rapid querying and retrieval of component
data stored in CSV files.

Example Usage: Get cmc data from all experiments with names like H3T4PressureForceFieldB

db = experiment.service.db.Mongo.defaultInterface()
data = db.getData(filename="cmc.csv", experiment="*PressureForceFieldB*")

Advantages over filesystem search
- Disassociate data from a filesystem hierarchy
i.e Data doesn't have to be collected under a single root/user or have a given structure
- Faster access
- Possibility to add queries not easily based on non-fs metadata (this requires some tagging not currently in place)
e.g Experiments on a particular molecule, primary data-file of a component, based on some component option value??

Initial support is for MongoDB."""

from __future__ import print_function
from __future__ import annotations

import copy
import json
import logging
import operator
import os
import pickle
import pprint

import re
import sys
import time
import traceback
import zipfile
import tempfile

import urllib.request
import urllib.parse
import urllib.error

import uuid
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Union, cast, Set, Iterable

import numpy as np
import requests
import requests.cookies

from six import string_types

import pandas

import experiment.utilities.data
import experiment.utilities.fsearch
import experiment.model.codes
import experiment.model.conf
import experiment.model.data
import experiment.model.errors
import experiment.model.frontends.flowir
import experiment.model.storage
import experiment.runtime.status
import experiment.service.errors

from functools import reduce

from future.utils import raise_with_traceback

import urllib3

from io import BytesIO


try:
    import concurrent.futures
except ImportError:
    print("Cannot import concurrent.futures", file=sys.stderr)


if TYPE_CHECKING:
    import experiment.runtime.workflow
    import experiment.runtime.engine
    from experiment.model.data import Experiment, ComponentDocument

import pymongo
import pymongo.errors

try:
    import tinydb
except ImportError:
    logging.getLogger().debug("Unable to import tinydb module - tinydb interface not available")

# VV: Suppress warnings about contacting a https URL that uses self-verified TLS
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# VV: A dictionary whose keys are strings, and values can be strings, DictMongoQuery, or List[DictMongoQuery],
# because this is a recursive type we'll just assume Values can be "Any"
DictMongoQuery = Dict[str, Any]


DictMongo = Union[DictMongoQuery, experiment.model.data.ComponentDocument]

# VV: Function that receives exactly 3 arguments:
#   filename, contents_of_file, document_of_component
# and returns an Arbitrary object
PostProcessFn = Callable[[Optional[str], Optional[bytes], experiment.model.data.ComponentDocument], Any]


def LoadMatrix(f):

    filename = os.path.split(f)[1]
    print('Loading', f)

    m = None

    try:
        m = experiment.utilities.data.matrixFromCSVFile(f, readHeaders=True)
        #if "name" in doc:
        #    m.setName("%s:%s" % (doc["name"], os.path.split(f)[1]))
    except experiment.utilities.data.CouldNotDetermineCSVDialectError:
        print('Could not determine CSV format for %s. Skipping' % f, file=sys.stderr)
    except ValueError as error:
        print('Error while processing %s' % f, file=sys.stderr)
        print(error, file=sys.stderr)
        print('Skipping', file=sys.stderr)

    return filename, m

def MatchComponentDataByReplica(seta, setb):

    '''Matches matrices belong to components with the same replica index

    The is useful because components with the same replica index usually produce
    results/analysis related to the same data-point.

    However simple zip will not work in the case the matrices have become unordered
    OR a replica of a given component failed to produce a matrix (and hence there will be no match)

    Parameters:
        seta: A list of matrices each from a replicated component
        setb: A list of matrices each from a replicated component

    Returns:
        A dictionary whose keys are replica indexes e.g. 0,1,2 etc
        The value of each key  is a lists containing the matrices from the input sets
        which were created by components with the given replica index.

        For example dict[2] will return all matrices in the input sets which were
        created by components with replica index 2'''

    # Get replica of each matrix in setA
    replicaDict = {}
    for m in seta:
        replicaDict[ReplicaFromComponentData(m)] = [m]

    for g in setb:
        replica = ReplicaFromComponentData(g)
        if replica not in replicaDict:
            print(('No data present for replica %d in setb' % replica))
        else:
            replicaDict[replica].append(g)
            print(('Matched %s to %s' % (g.name(), replicaDict[replica][0].name())))

    return replicaDict

def ReplicaFromComponentData(matrix):

    '''Returns the replica index of the component the matrix/data-frame comes from

    Matrices/Data-Frames returned from dbs are named using a convention that allows
    the replica to be extracted.
    If such a matrix is renamed this function will not work

    Parameters:
        matrix: An experiment.utilities.data.Matrix instance returned from a database class
            defined in this module

    Exceptions:
        Raises ValueError if a replica index cannot be determined'''

    return experiment.model.data.ReplicaFromComponentName(matrix.name().split(':')[0])

def AddExperimentsUnderDirectory(path, db, platform, attempt_shadowdir_repair=True):

    '''Adds all experiments under path to db

    db must support addExperimentAtLocation'''

    experiments = experiment.utilities.fsearch.Search(path, lambda x: x == 'experiment.conf')
    experiments = [os.path.split(os.path.split(path)[0])[0] for path in experiments]
    for location in experiments:
        try:
            db.addExperimentAtLocation(location, platform, attempt_shadowdir_repair=attempt_shadowdir_repair)
        except Exception as error:
            print("Error adding experiment at %s. Reason: %s" % (location, error), file=sys.stderr)


def preprocess_file_uri(instance):
    # type: (str) -> str
    """If instance does not begin with `file://` automatically injects: `.*` so that the queries involving this
    file uri will match the instance on *any* gateway.
    """

    if instance and instance.startswith('file://') is False and instance.startswith('.*') is False:
        instance = ''.join(('.*', instance,))

    return instance


class _DatabaseBackend:
    """Implements methods that are orthogonal to the Database backend e.g. download files, resolve gateways, etc"""
    def __init__(self, gateway_registry_url=None, own_gateway_url=None, override_local_gateway_url=None,
                 own_gateway_id=None, max_http_batch=10, session: Session = None):
        if session is None:
            session = Session(None, None, None)

        self._session: Session = session

        # VV: Cache gateway urls for at most 30 minutes
        self._cache_reset_every_seconds = 30 * 60.0
        self._cache_reset_at = time.time() + self._cache_reset_every_seconds
        self._cache_resolved_gateways = {}

        if gateway_registry_url is not None and gateway_registry_url.endswith('/'):
            gateway_registry_url = gateway_registry_url[:-1]

        if own_gateway_url and own_gateway_url.endswith('/'):
            own_gateway_url = own_gateway_url[:-1]

        self._gateway_registry_url = gateway_registry_url
        self._own_gateway_url = own_gateway_url
        self._override_local_gateway_url = override_local_gateway_url
        self._own_gateway_id = own_gateway_id or str(uuid.getnode())
        self.max_http_batch = max_http_batch or 10
        self.log = logging.getLogger('MongoBase')

    @classmethod
    def _util_preprocess_stage_and_component_reference(cls, stage, component):
        """Many methods expect that a component is either a name or a reference of a component including the stage
        this method receives a component and stage argument, validates that both arguments make sense with each
        other and then returns appropriate new values for component and stage so that these new values can
        safely override the arguments of other methods in this class
        Args:
            stage(int): The index of a stage you want to retrieve data from (integer)
            component(str): The name/reference of a component you want to retrieve data from (string or regex).
               If component is in reference format (i.e stage<%d>.%s) `%d` will be used by the `stage` argument
               if `stage` is not None then  this method
               raises experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage exception
        Raises:
            MongoQueryContainsComponentReferenceAndExtraStage - if component is reference and stage is not None
        Returns:
            A tuple with 2 entries: (stage: Optional[int], component: Optional[str])
        """

        # VV: Don't handle non-string components or those which are empty; echo current values of component and stage
        if isinstance(component, string_types) is False or not component:
            return stage, component

        str_stage, sep, comp_name = component.partition('.')
        if sep == '':
            # VV: No point in looking for a stage part if there's no `.` character at all
            return stage, component

        pattern = re.compile(r"(stage)([0-9]+)")
        match = pattern.match(str_stage)

        if match:
            if stage is not None:
                raise experiment.service.errors.MongoQueryContainsComponentReferenceAndExtraStage(comp_name, stage)
            return int(match.groups()[1]), comp_name

        return stage, component

    def register_experiment_with_gateway(self, gateway_url, instance, verify_tls=False):
        # type: (str, str, bool) -> None
        if instance.startswith('file://') is False:
            raise NotImplementedError("Expected a file:// URI received \"%s\"" % instance)

        encoded_location = urllib.parse.quote(instance, safe='')
        url = '%s/experiment/api/v1.0/location/%s' % (gateway_url, encoded_location)

        reply = {}
        try:
            self.log.info("Registering to my gateway at %s" % url)
            reply = self._session.post(url, verify=verify_tls).json()
        except Exception:
            self.log.info("Could not register %s with %s.\nException:%s" % (
                instance, gateway_url, traceback.format_exc()))

        if 'result' not in reply or reply['result'] is False:
            if 'error' in reply:
                error_returned = reply['error']
            else:
                error_returned = "Gateway did not provide error message"
            self.log.warning("Failed to register %s with %s. Reason: %s" % (
                instance, url, error_returned))

        encoded_own_url = urllib.parse.quote(self._own_gateway_url, '')

        url = '%s/gateway/api/v1.0/unique_id/%s/host/%s/label/%s' % (
            self._gateway_registry_url, self._own_gateway_id, encoded_own_url, self._own_gateway_id
        )
        try:
            self.log.info("Registering gateway to GatewayRegistry at %s" % url)
            reply = self._session.post(url, verify=verify_tls)
        except Exception:
            self.log.info("Could not register %s with GatewayRegistry.\nException:%s" % (
                self._own_gateway_url, traceback.format_exc()
            ))

    @classmethod
    def filename_to_regex_string(cls, filename):
        # type: (Optional[str]) -> Optional[str]
        """Convert filename to a string with which a regular expression can be generated.

        Prefix filename with `.*`; the filename matches an entire file-path, by prefixing it
        users can still search for names of files (e.g. `component_performance.csv`)
        """
        if filename is not None and filename.startswith('/') is False and filename.startswith('.*') is False:
            filename = '.*%s' % filename
        return filename

    def generate_matrix(self, res_filename, res_contents, doc):
        # type: (Optional[str], Optional[Union[str, bytes]], Dict[str, Any]) -> Optional["experiment.utilities.data.Matrix"]
        matrix = None
        try:
            if not res_contents:
                matrix = experiment.utilities.data.matrixFromCSVFile(res_filename, readHeaders=True)
            else:
                try:
                    res_contents = res_contents.decode('utf-8', 'ignore')
                except AttributeError:
                    pass
                res_contents = res_contents.rstrip()
                matrix = experiment.utilities.data.matrixFromCSVRepresentation(res_contents, readHeaders=True)
            entry_dict = {
                'component': doc['name'], 'instance': doc['instance'],
                'stage': doc['stage']
            }

            setattr(matrix, 'meta', entry_dict)

            if 'name' in doc:
                matrix.setName(doc['name'])
        except experiment.utilities.data.CouldNotDetermineCSVDialectError as error:
            self.log.warning('Could not determine CSV format for %s. Skipping' % res_filename)
            raise_with_traceback(error)
        except Exception as error:
            if res_contents:
                self.log.log(15, "Could not decode contents\n%s" % res_contents)
            self.log.log(15, traceback.format_exc())
            raise_with_traceback(error)

        return matrix

    def resolve_gateways(self, gateway_ids, verify_tls=False):
        # type: (List[str], bool) -> Dict[str, str]
        """Resolves a list of gateway ids to a Dictionary that maps gateway ids to their URL
        """

        url = '%s/gateways/api/v1.0/unique_ids' % self._gateway_registry_url
        response = self._session.post(url, json=gateway_ids, verify=verify_tls)

        if response.status_code in [403, 401]:
            raise experiment.service.errors.UnauthorisedRequest(
                url, "POST", response, 'POST Request arguments %s' % gateway_ids)
        elif response.status_code != 200:
            raise experiment.service.errors.InvalidHTTPRequest(
                url, "POST", response, 'POST Request arguments %s' % gateway_ids)

        info = response.json()

        resolved = {}
        for k in info:
            host = info[k]['host']
            if host.endswith('/'):
                host = host[:-1]
            resolved[k] = host

        return resolved

    def fetch_from_gateway(self, gateway_url, bundle, max_http_batch=None, verify_tls=False):
        # type: (str, Dict[str, List[str]], Optional[int], bool) -> Dict[str, bytes]
        """Downloads a large number of files from a specific gateway, in batches

        Arguments:
            gateway_url: url that gateway can be reached on
            bundle: {
                <URI of instance that contains file (file://$GATEWAY_ID/absolute/path/to/folder)>: [
                    /absolute/path/to/file,
                    ...
                ]
            }
            max_http_batch: Maximum number of files to request in a single HTTP REQUEST, if set to None,
               defaults to self.max_http_batch=10
        Returns:
            {
                /absolute/path/to/file: contents-of-file (bytes)
            }
        """
        max_http_batch = max_http_batch or self.max_http_batch
        if gateway_url.endswith('/'):
            gateway_url = gateway_url[:-1]

        def fetch_batch(file_urls, gateway_url=gateway_url):
            # type: (Dict[str, List[str]], str) -> Dict[str, bytes]
            """Downloads a batch of files ({instance: List[absolute path]}) from a given gateway.

            This method is deprecated and will be removed in MVP2 stack version 1.1.0

            Args:
                file_urls(Dict[str, List[str]): Format of dictionary is {instance: List[absolute path]} where
                  instance is the file:/// URI of the instance and the list it points to are the absolute paths of
                  files  to fetch from said instance. Gateway will not serve files that are not under the instance dir
                gateway_url(str): URL endpoint of gateway service

            Returns a Dictionary whose keys are the absolute paths of the files, and values the contents of the files
            """
            expected_files = []
            for inst in file_urls:
                expected_files.extend(file_urls[inst])

            try:
                url = '%s/files/api/v1.0' % gateway_url

                reply = self._session.post(url, json=file_urls, verify=verify_tls)
                files = json.loads(reply.text)
            except Exception:
                self.log.info("Could not fetch files %s from gateways_url %s.\nException:%s" % (
                    bundle, gateway_url, traceback.format_exc()
                ))
                return {}

            if reply.status_code != 200:
                self.log.info("Could not fetch files because HTTP %d: %s" % (reply.status_code, reply.text))
                return {}

            unknown = {k: files[k] for k in files if k not in expected_files}

            if unknown:
                self.log.warning("Received unknown files, will disregard: %s" % unknown)
                for k in unknown:
                    del files[k]

            return files

        def fetch_batch_v1_1(file_urls, gateway_url=gateway_url):
            # type: (Dict[str, List[str]], str) -> Dict[str, bytes]
            """Downloads Zip file containing batch of files ({instance: List[absolute path]}) from a given gateway

            Args:
                file_urls(Dict[str, List[str]): Format of dictionary is {instance: List[absolute path]} where
                  instance is the file:// URI of the instance and the list it points to are the absolute paths of
                  files  to fetch from said instance. Gateway will not serve files that are not under the instance dir
                gateway_url(str): URL endpoint of gateway service

            Returns a Dictionary whose keys are the absolute paths of the files, and values the contents of the files
            """
            expected_files = []
            for inst in file_urls:
                expected_files.extend(file_urls[inst])

            url = '%s/files/api/v1.1' % gateway_url

            reply = self._session.post(url, json=file_urls, verify=verify_tls, stream=True)

            if reply.status_code == 404:
                # VV: Raise an exception for 404 so that fetch_from_gateway() method falls back to
                # old v1.0 api, the v1.0 api will be deprecated in v1.1
                raise experiment.service.errors.InvalidHTTPRequest(
                    url, "post", reply, "files/api/v1.1 is not supported")
            if reply.status_code != 200:
                self.log.info("Could not fetch files because HTTP %d: %s" % (reply.status_code, reply.text))
                raise experiment.service.errors.InvalidHTTPRequest(
                    url, "post", reply, "files/api/v1.1 did not succeed")

            filelike = BytesIO(reply.content)
            z = zipfile.ZipFile(filelike, mode='r')

            def prefix_path(path):
                # type: (str) -> str
                """Ensures that path starts with '/'

                ZipFile strips leading `/` prefix
                """
                if path.startswith('/') is False:
                    return '/%s' % path
                return path

            files = {prefix_path(k): z.read(k) for k in z.namelist()}
            z.close()

            unknown = {k: files[k] for k in files if k not in expected_files}

            if unknown:
                self.log.warning("Received unknown files, will disregard: %s" % unknown)
                for k in unknown:
                    del files[k]

            return files

        ret = {}
        file_tuples = []
        for uri_exp in bundle:
            files = bundle[uri_exp]
            file_tuples.extend([(uri_exp, f) for f in files])

        def prepare_batch_from_tuples(tuples):
            # type: (List[Tuple[str, str]]) -> Dict[str, List[str]]
            """Aggregates tuples in the form of a dictionary; The keys is an instance and value a list of files
            """
            batch = {}
            for uri_exp, rel_path in tuples:
                if uri_exp not in batch:
                    batch[uri_exp] = []
                batch[uri_exp].append(rel_path)
            return batch

        # VV: First, attempt to use the new API to fetch files from a gateway, if the gateway has not been updated
        # yet, fallback to the old API (old API cannot fetch binary files)
        fetch_func = fetch_batch_v1_1

        for start in range(0, len(file_tuples), max_http_batch):
            tuples = file_tuples[start: start + max_http_batch]
            fetched = {}

            # VV: Repeat for as long there are more files to fetch AND the gateway replies with data
            while len(tuples):
                # VV: filter out files that were successfully retrieved, and try getting the remaining
                tuples = [(uri_exp, file) for (uri_exp, file) in tuples if file not in fetched]
                if len(tuples) == 0:
                    break

                batch = prepare_batch_from_tuples(tuples)
                try:
                    fetched = fetch_func(batch)
                except experiment.service.errors.InvalidHTTPRequest as e:
                    if e.response.status_code == 404 and fetch_func == fetch_batch_v1_1:
                        self.log.warning("Gateway does not support files/api/v1.1, falling back to "
                                         "old API - old API is deprecated and will be removed in "
                                         "MVP2 stack release 1.1.0.")
                        fetch_func = fetch_batch
                        fetched = fetch_func(batch)
                    else:
                        raise_with_traceback(e)

                ret.update(fetched)

                if len(fetched) < len(tuples):
                    self.log.info("Asked for %d files but got %d - consider decreasing max_http_batch=%d. Will "
                                  "attempt to fetch remaining files)" % (len(tuples), len(fetched), max_http_batch))
                else:
                    self.log.info("Fetched batch with %d files" % len(fetched))

                if len(fetched) == 0 and len(tuples):
                    self.log.warning("Could not fetch files: %s" % pprint.pformat(batch))
                    break
        self.log.info("Downloaded %d files in total" % len(ret))
        return ret

    def fetch_remote_file(self, instance, relative_path):
        # type: (str, str) -> bytes
        """Downloads single file from remote gateway.

        Args:
            instance: file://$GATEWAY_ID/absolute/path/to/instance/dir
            relative_path: /relative/to/instance/dir/path/to/file
        Returns:
            contents of the file
        """
        res_gateway, path = experiment.model.storage.partition_uri(instance)
        if relative_path.startswith('/'):
            relative_path = relative_path[1:]
        file_path = os.path.join(path, relative_path)
        url_gateway = self.resolve_gateway(res_gateway)

        fetched_files = self.fetch_from_gateway(url_gateway, {instance: [file_path]})

        if file_path not in fetched_files:
            raise_with_traceback(ValueError("File %s does not exist on gateway %s" % (file_path, url_gateway)))

        return fetched_files[file_path]

    def resolve_gateway(self, gateway_name, _api_verbose=False, _skip_cache=False):
        """Resolves a gateway id to a URL, may use cache and may reset cache if cache is old

        Arguments:
            gateway_name(str): unique identifier of gateway
            _api_verbose(bool): when True prints out more information about the HTTP request - useful for debugging
            _skip_cache(bool): if true attempt to resolve gateway without checking the cache at all

        Returns
            the gateway URL

        Raises
            experiment.errors.UnknownGateway - if GatewayRegistry does not have an entry for gateway id
            experiment.errors.EnhancedException - if unable to reach the gateway registry or decode the response
        """
        if self._cache_reset_at < time.time():
            self._cache_resolved_gateways.clear()
            self._cache_reset_at = time.time() + self._cache_reset_every_seconds
            self.log.info("Gateway URL cache is reset")

        if _skip_cache or gateway_name not in self._cache_resolved_gateways:
            self.log.info("Resolving gateway %s" % gateway_name)
            try:
                gateway_urls = self.resolve_gateways([gateway_name])
            except experiment.service.errors.InvalidHTTPRequest as e:
                raise_with_traceback(e)
            except Exception as e:
                raise_with_traceback(experiment.model.errors.EnhancedException(
                    "Failed to resolve gateway %s" % gateway_name, e))

            if gateway_name not in gateway_urls:
                raise experiment.service.errors.UnknownGateway(gateway_name, self._gateway_registry_url)
            self._cache_resolved_gateways[gateway_name] = gateway_urls[gateway_name]

        return self._cache_resolved_gateways[gateway_name]


class Mongo(_DatabaseBackend):

    '''Interface to MongoDB containing information on Experiments

    The main use of the interface is to retrieve CSV froma'''

    @classmethod
    def defaultInterface(cls):

        pass

    def __init__(self, host="localhost", port=27017, database='db', collection="experiments",
                 gateway_registry_url=None, own_gateway_url=None, override_local_gateway_url=None,
                 own_gateway_id=None, mongo_username=None, mongo_password=None, mongo_authSource=None,
                 max_http_batch=10, quote_mongo_credentials=True, session: Session = None):

        '''Initialise a Mongo object connecting to a specific database'''
        super(Mongo, self).__init__(gateway_registry_url, own_gateway_url, override_local_gateway_url, own_gateway_id,
                                    max_http_batch, session=session)
        self.host = host
        self.port = port
        self.databaseName = database
        self.collectionName = collection

        if session is None:
            session = Session(None, None, None)

        extra_args = {}

        # VV: pymongo v4 requires quoting "%" in credentials https://github.com/mongodb/mongo-python-driver/pull/755
        if mongo_username is not None:
            if quote_mongo_credentials:
                mongo_username = mongo_username.replace('%', '%25')
            extra_args['username'] = mongo_username

        if mongo_password is not None:
            if quote_mongo_credentials:
                mongo_password = mongo_password.replace('%', '%25')
            extra_args['password'] = mongo_password

        if mongo_authSource is not None:
            extra_args['authSource'] = mongo_authSource

        self.client = pymongo.MongoClient(self.host, self.port, **extra_args)
        self.database = self.client[self.databaseName]
        self.collection = self.database[self.collectionName]
        self._session = session

        self.log = logging.getLogger('Mongo')

    @classmethod
    def _mongo_values_to_regex(cls, query: DictMongoQuery) -> DictMongoQuery:
        """Modifies in-place string values in a query to regular expressions

        It converts "key: value" to "key: {"$regex": value}" for string values, traverses @query to
        modify nested dictionaries

        Args:
            query: A Dictionary to process; this method modifies the dictionary

        Returns:
            The processed dictionary, which is stored on the same object as the argument to this method.
        """
        def generate_regular_expression(x: str):
            if isinstance(x, string_types):
                return {"$regex": x}
            return x

        # VV: Process dictionary to convert strings to regular expressions - be aware that there could be
        # queries to arrays (see code below involving '$in')
        already_processed = set()

        def process_dictionary(dictionary):
            if id(dictionary) in already_processed:
                return

            already_processed.add(id(dictionary))

            for q in dictionary:
                if isinstance(dictionary[q], string_types):
                    dictionary[q] = generate_regular_expression(dictionary[q])
                elif isinstance(dictionary[q], dict):
                    process_dictionary(dictionary[q])

        process_dictionary(query)

        return query

    @classmethod
    def _mongo_keys_to_str(cls, document: Union[List[DictMongo], DictMongo]) \
            -> (Union[List[DictMongo], DictMongo]):
        """This method makes a @document compatible with mongodb, may also process a list of documents. Should
        also be used to preprocess queries too.

        Arguments:
            document: One or more Mongo dictionaries (can be queries or actual documents)

        Rules:
        - MongoDB expects that keys of dictionaries are strings.

        Returns:
            one or more dictionaries representing mongo queries/documents
        """
        # VV: The code below inspects all nested fields of @document, preprocesses nested objects and finally
        # returns the preprocessed copy of @document

        # VV: Maintain a list of sanitized objects to avoid infinite recursion
        processed = {}
        document = copy.deepcopy(document)

        # VV: 1 entry for each object to sanitize (these can be nested dictionaries and lists of objects)
        # Each entry in this list is a tuple (object, callback_to_update_object_value_in_parent_object)
        remaining = [(document, None)]  # type: List[Tuple[Any, Optional[Callable[[Any], None]]]]

        def process_list(objects, cb_update_parent):
            # type: (List[Any], Optional[Callable[[Any], None]]) -> None
            """Ensures that all entries within the @objects array are valid parts of a MongoDB documentDescriptor"""
            if cb_update_parent:
                cb_update_parent(objects)
            processed[id(objects)] = objects

            for idx, value in enumerate(objects):
                if experiment.model.frontends.flowir.is_primitive_or_empty(value) is False:
                    # VV: Strangely, using idx instead of index leads to the wrong result
                    remaining.insert(0, (value, lambda what, parent=objects, index=idx:
                                         parent.__setitem__(index, what)))

        def process_dictionary(dictionary, cb_update_parent):
            # type: (Dict[str, Any], Optional[Callable[[Any], None]]) -> None
            """Ensures that all entries within the @dictionary dictionary are valid for a MongoDB documentDescriptor"""

            processed[id(dictionary)] = dictionary

            if cb_update_parent:
                cb_update_parent(dictionary)

            for key in list(dictionary):
                value = dictionary[key]
                new_key = key
                if isinstance(key, string_types) is False:
                    if experiment.model.frontends.flowir.is_primitive(key) is False:
                        raise ValueError("Dictionary %s contains a key which is not a string/bool/int/float or None and"
                                         "cannot be converted to a string which is a hard requirement for MongoDB "
                                         "documentDescriptors" % dictionary)
                    new_key = str(key)
                dictionary.pop(key)

                if experiment.model.frontends.flowir.is_primitive_or_empty(value):
                    dictionary[new_key] = value
                else:
                    remaining.insert(0, (value, lambda what,
                                                       parent=dictionary,
                                                       key=new_key: parent.__setitem__(key, what)))

        # VV: Process all objects pending sanitization, if an object is traversed in the past there's no
        # need to re-process it
        while remaining:
            obj, cb_update_value = remaining.pop(0)

            # VV: Do not process the same object more than 1 times
            id_obj = id(obj)
            if id_obj in processed:
                cb_update_value(processed[id_obj])
                continue

            if experiment.model.frontends.flowir.is_primitive_or_empty(obj):
                if cb_update_value:
                    cb_update_value(obj)
            elif isinstance(obj, dict):
                process_dictionary(obj, cb_update_value)
            elif isinstance(obj, (list, set)):
                process_list(list(obj), cb_update_value)

        processed.clear()

        # VV: This is a deep copy of the original @document parameter, which has been made suitable to
        # store in MongoDB or query it
        return document

    def preprocess_query(
            self,
            filename: Optional[str] = None,
            component: Optional[str] = None,
            stage: Optional[int] = None,
            instance: Optional[str] = None,
            type: Optional[str] = None,
            query: Optional[DictMongoQuery] = None,
            ) -> DictMongoQuery:
        """Prepare a MongoDB query dictionary

        Parameters other than @query may end up overriding those you define in @query. Method may also post-process
        the values of parameters that are not @query. This method post-processes the query right before sending it to
        Mongo so that its keys are strings and its string-values are regular-expressions.

        Args:
            instance: File URI in the form of file://$GATEWAY_ID/absolute/path/to/instance/dir
            stage: index of stage comprising component
            component: The name/reference of a component you want to retrieve data from (string or regex).
               If component is in reference format (i.e stage<%d>.%s) `%d` will be used by the `stage` argument
               if `stage` is not None then  this method
               raises experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage exception
            filename: relative to the working dir of the component path
            type: type of the document to return (e.g. "component", "user-metadata", "experiment")
            query: A mongo query to use for further filtering results

        Raises:
            experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage: if component is reference and
                stage is not None

        Returns:
            Returns the resulting MongoQuery dictionary
        """
        filename = self.filename_to_regex_string(filename)

        instance = preprocess_file_uri(instance)

        stage, component = self._util_preprocess_stage_and_component_reference(stage, component)

        queryDict = {}
        if instance is not None:
            queryDict["instance"] = instance

        if component is not None:
            queryDict["name"] = component

        if stage is not None:
            queryDict["stage"] = stage

        if filename is not None:
            # VV: _mongo_values_to_regex will turn this into: {"$elemMatch": {"$regex": filename}}
            queryDict['files'] = {"$elemMatch": filename}

        if type is not None:
            queryDict['type'] = type

        query = (query or {}).copy()

        # VV: Preprocess the queryDict which we synthesized out of parameters to this method, leave @query as is
        # 1. its keys are strings
        # 2. its string-values are regular-expression

        queryDict = self._mongo_keys_to_str(queryDict)
        queryDict = self._mongo_values_to_regex(queryDict)

        # VV: Finally, update the @query with contents of the synthesized queryDict this way we override any
        # of the contents of query that clash with those we automatically generated to make getDocument more
        # intuitive to use, users that want to just use mongo should *only* use the @query parameter which we
        # do *NOT* modify at all
        query.update(queryDict)

        return query

    def is_connected(self):
        try:
            self.database.command('ping')
        except pymongo.errors.ConnectionFailure:
            return False
        except Exception:
            self.log.warning("Unable to check MongoDB connectivity, exception: %s - will assume disconnection"
                             % traceback.format_exc())
            return False
        else:
            return True

    def __str__(self):
        values = (self.collectionName, self.databaseName, self.host, self.port)
        return "Mongo interface serving collection %s in database %s on %s:%d" % values

    def upsertUserMetadataTags(self, documents, tags):
        # type: (List[Dict[str, Any]], Dict[str, Any]) -> None
        """Updates user-metadata @documents to include @tags and stores them on the ST4SD Datastore backend.
        Also updates associated `experiment` documents.

        Args:
            documents(List[Dict[str, Any]], Dict[str, Any]): Documents to update
            tags(Dict[str, Any]): Tags to upsert into the documents
        """
        invalid_docs = [d for d in documents if d.get('instance') == '' or d.get('type') != 'user-metadata']
        if invalid_docs:
            raise ValueError("Valid user-metadata documents must have an instance URI and type: 'user-metadata', "
                             "the following documents were invalid %s" % pprint.pformat(invalid_docs))

        # VV: Update the user-metadata documents with the contents of @tag (use copies because we don't want to modify
        # the arguments to this method)
        documents = copy.deep_copy(documents)  # type: List[Dict[str, str]]
        tags = copy.deep_copy(tags)

        for d in documents:
            d.update(tags)

        # VV: We also need to update the `metadata.user-metadata` field of any associated `experiment` docs
        exp_docs = []
        instances = list(set([doc['instance'] for doc in documents]))
        max_instances_per_call = 100
        self.log.info("Generating experiment documents to reflect changes to user-metadata ones")
        # VV: To avoid generating a huge regular expression, ask for experiment documents in batches
        for batch_start in range(0, len(instances), max_instances_per_call):
            exp_docs.extend(self.getDocument(type='experiment',
                                             instance='^(%s)$' % '|'.join(
                                                 instances[batch_start: (batch_start + max_instances_per_call)])))

        for d in exp_docs:
            if 'metadata' not in d:
                d['metadata'] = {}
            metadata = d['metadata']
            if 'userMetadata' not in metadata:
                metadata['userMetadata'] = {}
            user_metadata = metadata['userMetadata']
            user_metadata.update(tags)

        self.log.info("Updating MongoDB with changes to user-metadata and experiment documents")
        self._upsert_documents(documents + exp_docs)

    def _experimentInCollection(self, location):
        # type: (str) -> bool
        '''Returns True if an experiment instance is in the database - based on path

        Parameter:
            location (string): The path to the instance
        '''
        location = preprocess_file_uri(location)

        query = {'instance': location, 'type': 'experiment'}

        def check_for_any_document():
            cursor = self._query_mongodb(query)

            # VV: If we got even 1 document that means that the experiment exists
            for _ in cursor:
                return True

            return False

        return self._retry_on_pymongo_disconnect(check_for_any_document)

    def addExperimentAtLocation(self, location, platform=None, gateway_id=None,
                                do_register=True, attempt_shadowdir_repair=True, update=True):
        # type: (str, Optional[str], Optional[str], bool, bool, bool) -> bool
        expDir = experiment.model.storage.ExperimentInstanceDirectory(
            location, attempt_shadowdir_repair=attempt_shadowdir_repair)
        exp = experiment.model.data.Experiment(expDir, platform=platform,
                                               updateInstanceConfiguration=False,
                                               is_instance=True)
        return self.addExperiment(exp, gateway_id, do_register=do_register)

    @classmethod
    def _filter_cdb_document_fields(cls, doc):
        # type: (Dict[str, Any]) -> Dict[str, Any]
        extract = {
            # VV: A `component` document matches the new one if it has the same name, and stage on top of
            #     the same instance, and type fields
            'component': lambda d: {k: d[k] for k in ['name', 'stage']},
            'experiment': lambda d: {},
            'user-metadata': lambda d: {},
        }

        ret = {k: doc[k] for k in ['instance', 'type']}
        ret.update(extract[doc['type']](doc))
        return ret

    def _upsert_documents(self, documents):
        # type: (List[ComponentDocument]) -> None
        """Upserts documents into the CDB, expects documents to have been generated by Flow.

        First remove any matching flow documents (e.g. experiment, component, user-metadata, etc ones) and then
        insert the @documents.

        Args:
            documents: A list of Mongo document (i.e. Dictionaries with str keys)
        """
        documents = self._mongo_keys_to_str(documents)
        delete_ops = [pymongo.DeleteMany(self._filter_cdb_document_fields(doc)) for doc in documents]
        insert_ops = [pymongo.InsertOne(doc) for doc in documents]

        def do_upsert():
            self.collection.bulk_write(delete_ops + insert_ops)
            # VV: Sends "fsync" command to admin database to persist changes to the filesystem
            # do not lock the MongoDB instance - that would prevent writes till we explicitly
            # unlock it
            self.client['admin'].command('fsync', lock=False)

        self._retry_on_pymongo_disconnect(do_upsert)

    def addExperiment(self, exp, gateway_id=None, do_register=True):
        # type: ("experiment.data.Experiment", Optional[str], bool) -> bool
        '''Adds the experiment to the mongo db so it can be queried'''

        #Each experiment is a document
        #stages, components are embedded (2 embed levels)
        #Files are array attributes of components (assuming no querying on file properties)
        #
        #The reason for embedding v multi-doc is that the purposes of the DB is only
        #to access data from running/complete experiment instances.
        #Most data stored will be specific to each document (little repetition)
        #For the repition that exists e.g. components->stage, component command lines
        #it is small and has no particular value (in this case) for quering indepedently.
        #as they contain no data.
        #
        #Assembling the json to insert is recursive - ask each component to output its json.
        gateway_id = gateway_id or self._own_gateway_id
        instance = exp.generate_instance_location(gateway_id)
        documents = exp.generate_document_description(gateway_id)
        self.log.info("Adding experiment \"%s\"" % instance)
        self._upsert_documents(documents)

        if do_register and (self._own_gateway_url or self._override_local_gateway_url):
            own_gateway_url = self._override_local_gateway_url or self._own_gateway_url
            self.register_experiment_with_gateway(own_gateway_url, instance)

        return True

    def _query_mongodb(
            self,
            query: DictMongoQuery,
            _api_verbose: bool = True
    ) -> pymongo.collection.Cursor:
        """Queries the mongoDB and returns a Cursor to a list of matching DocumentDescriptors

        Args:
            query: A dictionary containing a mongo query
            _api_verbose: This is not used here
        """
        return self.collection.find(query)

    def _kernel_getDocument(
            self,
            filename: Optional[str] = None,
            component: Optional[str] = None,
            stage: Optional[int] = None,
            instance: Optional[str] = None,
            type: Optional[str] = None,
            query: Optional[DictMongoQuery] = None,
            include_properties: List[str] | None = None,
            stringify_nan: bool = False,
            _api_verbose=True,
            ) -> Iterable[ComponentDocument]:
        """Queries the ST4SD Datastore (CDB) for MongoDB documents.

        Parameters other than @query may end up overriding those you define in @query. Method may also post-process
        the values of parameters that are not @query. This method post-processes the query right before sending it to
        Mongo so that its keys are strings and its string-values are regular-expressions.

        Args:
            instance: File URI in the form of file://$GATEWAY_ID/absolute/path/to/instance/dir
            stage: index of stage comprising component
            component: The name/reference of a component you want to retrieve data from (string or regex).
               If component is in reference format (i.e stage<%d>.%s) `%d` will be used by the `stage` argument
               if `stage` is not None then  this method
               raises experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage exception
            filename: relative to the working dir of the component path
            type: type of the document to return (e.g. "component", "user-metadata", "experiment")
            query: A mongo query to use for further filtering results
            include_properties: A list of column names or a list with just the string "*" which will automatically
                expand to all the column names in the properties DataFrame that matching experiments have measured.
                The column names in include_properties are case-insensitive, and
                the returned DataFrame will contain lowercase column names.
                When this argument is provided the ST4SD Datastore API (mongo-proxy) will insert the field
                `interface.propertyTable` to matching `experiment` type mongoDocuments. This field will contain
                a dictionary representation of the properties DataFrame that the matching experiment measured. The
                columns in that DataFrame will be those specified in `include_properties`. Column names that do not
                exist will in the DataFrame will be silently discarded. The column `input-id` will always be fetched
                even if not specified in `include_properties`.
            stringify_nan: A boolean flag that allows converting NaN and infinite values to strings
            _api_verbose: Set to true to print INFO level messages

        Raises:
            experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage: if component is reference and
                stage is not None

        Returns:
            Returns an Iterable of dictionaries created out of MongoDB documents
        """
        # VV: First generate a new query by combining the synthetic dictionary that we generate out of
        # @filename, @component, @stage, @instance, and @type and then using it to override the @query parameter
        # Note that we do not modify, or preprocesses, the contents of the @query parameter at all
        query = self.preprocess_query(filename=filename, component=component, stage=stage, instance=instance,
                                      type=type, query=query)

        self.log.debug("Query dict: %s" % query)
        # VV: query_mongodb returns a cursor
        cursor = self._retry_on_pymongo_disconnect(lambda: self._query_mongodb(
            query, _api_verbose=_api_verbose))

        if include_properties:
            include_properties = [x.lower() for x in include_properties]

            def process(x: DictMongo) -> DictMongo:
                if x.get('type') == 'experiment':
                    try:
                        if 'interface' not in x:
                            return x
                        output_files = x['interface'].get('outputFiles')
                        if not output_files:
                            return x

                        # VV: There can be multiple paths in outputFiles, we need to guess which is the one that points
                        # to properties, it's probably the one whose filename is `properties.csv`
                        path_properties = [p for p in output_files if p and os.path.basename(p) == "properties.csv"]

                        if len(path_properties) == 1:
                            # VV: This is a relative path to the ${INSTANCE_ROOT_DIR} turn it into an absolute path
                            _, instance_dir = experiment.model.storage.partition_uri(x['instance'])
                            path = os.path.join(instance_dir, path_properties[0])

                            df: pandas.DataFrame = pandas.read_csv(path, sep=None, engine="python")

                            # VV: If include_properties == ["*"] we just include the entire DataFrame
                            if include_properties != ["*"]:
                                # VV: Filter out columns which do not exist in DataFrame, and always include "input-id"
                                columns = set(include_properties).intersection(df.columns)
                                columns.add("input-id")
                                df = df[list(columns)]

                            if stringify_nan:
                                df.fillna('NaN', inplace=True)
                                df.replace(np.inf, 'inf', inplace=True)
                                df.replace(-np.inf, '-inf', inplace=True)

                            x['interface']['propertyTable'] = df.to_dict(orient="list")
                    except Exception as e:
                        self.log.warning(f"Unable to inject properties in {x['instance']} due to {e} - ignoring error - \n {traceback.format_exc()}")
                return x
            return map(process, cursor)
        else:
            return cursor

    def getDocument(
            self,
            filename: Optional[str] = None,
            component: Optional[str] = None,
            stage: Optional[int] = None,
            instance: Optional[str] = None,
            type: Optional[str] = None,
            query: Optional[DictMongoQuery] = None,
            include_properties: List[str] | None = None,
            stringify_nan: bool = False,
            _api_verbose=True,
            ) -> List[ComponentDocument]:
        """Queries the ST4SD Datastore (CDB) for MongoDB documents.

        Parameters other than @query may end up overriding those you define in @query. Method may also post-process
        the values of parameters that are not @query. This method post-processes the query right before sending it to
        Mongo so that its keys are strings and its string-values are regular-expressions.

        Args:
            instance: File URI in the form of file://$GATEWAY_ID/absolute/path/to/instance/dir
            stage: index of stage comprising component
            component: The name/reference of a component you want to retrieve data from (string or regex).
               If component is in reference format (i.e stage<%d>.%s) `%d` will be used by the `stage` argument
               if `stage` is not None then  this method
               raises experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage exception
            filename: relative to the working dir of the component path
            type: type of the document to return (e.g. "component", "user-metadata", "experiment")
            query: A mongo query to use for further filtering results
            include_properties: A list of column names or a list with just the string "*" which will automatically
                expand to all the column names in the properties DataFrame that matching experiments have measured.
                The column names in include_properties are case-insensitive, and
                the returned DataFrame will contain lowercase column names.
                When this argument is provided the ST4SD Datastore API (mongo-proxy) will insert the field
                `interface.propertyTable` to matching `experiment` type mongoDocuments. This field will contain
                a dictionary representation of the properties DataFrame that the matching experiment measured. The
                columns in that DataFrame will be those specified in `include_properties`. Column names that do not
                exist will in the DataFrame will be silently discarded. The column `input-id` will always be fetched
                even if not specified in `include_properties`.
            stringify_nan: A boolean flag that allows converting NaN and infinite values to strings
            _api_verbose: Set to true to print INFO level messages

        Raises:
            experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage: if component is reference and
                stage is not None

        Returns:
            Returns a List of dictionaries created out of MongoDB documents
        """
        return list(self._kernel_getDocument(filename=filename, component=component, stage=stage, instance=instance,
                                             type=type, query=query, include_properties=include_properties,
                                             stringify_nan=stringify_nan, _api_verbose=_api_verbose))

    def _retry_on_pymongo_disconnect(self, func, max_disconnections=3):
        # type: (Callable[[], Any], int) -> Any
        """Executes @func(), retrying if MongoDB disconnects. Re-raises Exception on too many disconnections.

        We consider max_disconnections to be "too many".

        Raises:
            pymongo.errors.ConnectionFailure
        """
        disconnections = 0
        while True:
            try:
                return func()
            except pymongo.errors.ConnectionFailure as e:
                disconnections += 1
                if disconnections >= max_disconnections:
                    self.log.critical("Too many (%d) consecutive disconnections from MongoDB" % disconnections)
                    raise_with_traceback(e)
                else:
                    self.log.critical("%d consecutive disconnections from MongoDB - will retry" % disconnections)

    def valuesMatchingQuery(self,
                            filename: Optional[str] = None,
                            component: Optional[str] = None,
                            stage: Optional[int] = None,
                            instance: Optional[str] = None,
                            type: Optional[str] = None,
                            query: Optional[DictMongoQuery] = None
                            ) -> Union[Set[str]]:
        '''Returns unique sets of field values in the receiver which match query

        For example if the query was component="Co*" it would return a set containing
        all component names starting with "Co*"

        If there are multiple query fields the elements of the set are lists
        '''
        # VV: First build the query so that we can tell later exactly which fields we queried for
        actual_query = self.preprocess_query(filename=filename, component=component, stage=stage, instance=instance,
                                             type=type, query=query)

        # VV: Then query the documents and filter out fields which we did not explicitly query
        # keep in mind that we could have queried for nested fields so if a query key is in the form of
        # "a.b.c" we end up keeping around everything under "a"

        all_docs: List[ComponentDocument] = self.getDocument(query=query)
        fields: List[str] = [k.split('.', 1)[0] for k in actual_query if actual_query[k] is not None]
        s = set()

        for d in all_docs:
            v = [d[f] for f in fields if f in d]

            if len(v) == 1:
                s.add(v[0])
            else:
                s.update(v)

        return s

    @classmethod
    def _util_preprocess_stage_and_component_reference(cls, stage: Optional[int], component: Optional[str]) \
            -> Tuple[Optional[int], Optional[str]]:
        """Many methods expect that a component is either a name or a reference of a component including the stage
        this method receives a component and stage argument, validates that both arguments make sense with each
        other and then returns appropriate new values for component and stage so that these new values can
        safely override the arguments of other methods in this class

        Args:
            stage: The index of a stage you want to retrieve data from (integer)
            component: The name/reference of a component you want to retrieve data from (string or regex).
               If component is in reference format (i.e stage<%d>.%s) `%d` will be used by the `stage` argument
               if `stage` is not None then  this method
               raises experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage exception

        Raises:
            experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage: if component is
                reference and stage is not None

        Returns:
            A tuple with 2 entries: (stage: Optional[int], component: Optional[str])
        """

        # VV: Don't handle non-string components or those which are empty; echo current values of component and stage
        if isinstance(component, string_types) is False or not component:
            return stage, component

        str_stage, sep, comp_name = component.partition('.')
        if sep == '':
            # VV: No point in looking for a stage part if there's no `.` character at all
            return stage, component

        pattern = re.compile(r"(stage)([0-9]+)")
        match = pattern.match(str_stage)

        if match:
            if stage is not None:
                raise experiment.service.errors.MongoQueryContainsComponentReferenceAndExtraStage(comp_name, stage)
            return int(match.groups()[1]), comp_name

        return stage, component

    def _matching_files_in_docs(
            self,  # type: Mongo
            mongo_docs,  # type: List[ComponentDocument]
            filename,  # type: Optional[str]
        ):
        """Finds files which are referenced by provided DocumentDescriptions and match a filename regular expression

        Returns:
            A dictionary whose keys are matching fileURIs (file://gatewayId/abs/path/to/file) and values the
              DocumentDescriptor of the component which generated the file
        """
        # VV: Maps `file://gatewayId/path/to/file` to the DocumentDescription of the component which generated it
        file_to_doc = {}  # type: Dict[str, Dict[str, Any]]

        if filename:
            filename = re.compile(self.filename_to_regex_string(filename))
        else:
            return {}

        for doc in mongo_docs:
            doc_files = [x for x in doc.get('files', []) if filename.match(x)]
            if not doc_files:
                continue
            gw, _ = experiment.model.storage.partition_uri(doc['instance'])
            for res_filename in doc_files:
                file_uri = experiment.model.storage.generate_uri(gw, res_filename)
                file_to_doc[file_uri] = doc

        return file_to_doc

    def findFilesAndDocuments(
            self,
            filename: Optional[str] = None,
            component: Optional[str] = None,
            stage: Optional[int] = None,
            instance: Optional[str] = None,
            query: Optional[DictMongoQuery] = None,
        ) -> Dict[str, ComponentDocument]:
        """Finds files matching filters and returns DocumentDescriptors of the Components that generated the files and
        the relative-to-the-component-directory path to the files

        The matching is greedy i.e all files which satisfy conditions are returned.

        Parameters:
            filename:A string or a regex (uncompiled). The name of the file you want to retrieve
            component(str): The name/reference of a component you want to retrieve data from (string or regex).
               If component is in reference format (i.e stage<%d>.%s) `%d` will be used by the `stage` argument
               if `stage` is not None then  this method
               raises experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage exception
            stage: The index of a stage you want to retrieve data from (integer)
            instance: The location of the experiment you want to retrieve data from (string or regex)
                it can be a file URI (file://$HOSTNAME/$ABSOLUTE_PATH)

        Raises:
            experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage: if component in reference format
                and `stage` is not None

        Returns:
            A Dictionary whose keys are fileURIs (file://gatewayId/path/to/file) and values the DocumentDescription
            of the component which generated the file
        """
        kernel_query = lambda: self.getDocument(filename=filename, component=component,
                                     stage=stage, instance=instance, query=query)
        docs = self._retry_on_pymongo_disconnect(kernel_query)
        return self._matching_files_in_docs(docs, filename)

    def getFilesAndPostProcess(
        self,
        filename: Optional[str] = None,
        component: Optional[str] = None,
        stage: Optional[int] = None,
        instance: Optional[str] = None,
        post_process: PostProcessFn = None,
        myself: Optional[str] = None,
        query: Optional[DictMongoQuery] = None) -> List[Tuple[ComponentDocument, str, Union[bytes, object]]]:
        """Finds files matching filters and returns DocumentDescriptors of the Components that generated the files
        and contents/objects representing the files.

        Objects may be generated by a @post_process function on the contents of the files. Also, the matching is
        greedy i.e all files which satisfy conditions are returned.

        Args:
            filename:A string or a regex (uncompiled). The name of the file you want to retrieve
            component: The name/reference of a component you want to retrieve data from (string or regex).
               If component is in reference format (i.e stage<%d>.%s) `%d` will be used by the `stage` argument
               if `stage` is not None then  this method
               raises experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage exception
            stage: The index of a stage you want to retrieve data from (integer)
            instance: The location of the experiment you want to retrieve data from (string or regex)
                it can be a file URI (file://$HOSTNAME/$ABSOLUTE_PATH)
            post_process: Function that receives exactly 3 arguments:
                filename, contents_of_file, document_of_component use this function to post-process the
                contents of a file and return the resulting object instead of the raw contents
                (e.g. convert contents into an experiment.utilities.data.Matrix instance)
            myself: Name of local gateway, can be set to None
            query: A mongo query to use for further filtering results

        Raises:
            experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage: if component in reference
                format and `stage` is not None

        Returns:

            A list of tuples, one for each file retrieved
                The first element is a dictionary containing metadata about the component which produced the file
                The second is the URI file://<GATEWAY-HOSTNAME>/path/to/file
                The third is the raw data of the file or the result of post
        """
        # VV: First discover files we'd like to download and associate them with the MongoDocument of the component
        # that produced said files
        file_to_doc = self.findFilesAndDocuments(filename=filename, component=component, stage=stage,
                                                 instance=instance, query=query)

        # VV: next, create a bundle of the files (gatewayID->instanceURI->fileURI->documentDescription)
        file_map = self._generate_file_map(file_to_doc)

        # VV: Finally, download the "file_map" and return [(dictMongo, file uri, contents/post-processed contents), ...]
        return self._fetch_and_postprocess(file_map=file_map, post_process=post_process, myself=myself)

    @classmethod
    def _generate_file_map(cls, file_to_doc: Dict[str, DictMongo]) -> Dict[str, Dict[str, Dict[str, Dict[str, Any]]]]:
        """Groups many file URIs each of which is produced by an entity of which we have a mongoDescription.

        Args:
            file_to_doc: A dictionary whose keys are file URIs (the file to download) and values are the producers
                of the file.

        Returns:
            A deeply nested "file_map" dictionary  gatewayID -> instanceUri -> fileURI -> documentDescription.
                The method __fetch_and_postprocess() can use this dictionary to download files in an efficient way
        """
        # VV: Map each individual file to its relative path in the FILE_URI of the workflow instance that
        # produced it.
        # VV: gatewayID -> instanceUri -> fileURI -> documentDescription
        file_map: Dict[str, Dict[str, Dict[str, Dict[str, Any]]]] = {}

        for uri in file_to_doc:
            doc = file_to_doc[uri]
            gw, _ = experiment.model.storage.partition_uri(uri)
            if gw not in file_map:
                file_map[gw] = {}

            gw = file_map[gw]

            if doc['instance'] not in gw:
                gw[doc['instance']] = {}

            instance = gw[doc['instance']]
            instance[uri] = doc

        return file_map

    def _fetch_and_postprocess(
            self,
            file_map: Dict[str, Dict[str, Dict[str, ComponentDocument]]],
            myself: Optional[str],
            post_process: Optional[PostProcessFn],
        ) -> List[Tuple[ComponentDocument, str, Any]]:
        """Retrieves the contents of files and optionally applies a post_process function

        Args:
            file_map: Deeply nested dictionary: gatewayID -> instanceUri -> fileURI -> documentDescription
            myself: Optional ID of local gateway (e.g. hermes-dev, hartree, etc)
            post_process: Function that receives exactly 3 arguments: filename, contents_of_file, document_of_component
                use this function to post-process the contents of a file and return the resulting object instead
                of the raw contents (e.g. convert contents in an experiment.utilities.data.Matrix)
        Returns:
            A list of tuples, one for each file retrieved
            The first element is a dictionary containing metadata about the component which produced the file
            The second is the URI file://<GATEWAY-HOSTNAME>/path/to/file
            The third is the raw data of the file or the result of post_process func if that argument is not None
        """
        results = []  # type: List[Tuple[Dict[str, Any], Any, str]]

        for gw in file_map:
            if (gw or myself) != myself:
                try:
                    url_gateway = self.resolve_gateway(gw)
                except Exception as e:
                    self.log.log(15, "Traceback: %s" % traceback.format_exc())
                    self.log.info("Unable to resolve gateway url for %s because of %s - will not download "
                                  "files from it." % (gw, e))
                    continue

                # VV: Maintain a map of absolute-file-path to (DocumentDescriptor, file://URI) to translate the
                # results of fetch_from_gateway() to the 3-field tuple that _fetch_and_postprocess returns
                translation = {}
                bundle = {}
                for instance_uri in file_map[gw]:
                    bundle[instance_uri] = []
                    for file_uri in file_map[gw][instance_uri]:
                        _, file_path = experiment.model.storage.partition_uri(file_uri)
                        translation[file_path] = (file_map[gw][instance_uri][file_uri], file_uri)
                        bundle[instance_uri].append(file_path)

                # VV: Make a single request to the remote gateway to fetch all of the files (fetch_from_gateway will
                # automatically fetch the files in batches)
                data = self.fetch_from_gateway(url_gateway, bundle)
                for path in data:
                    contents = data[path]
                    doc, file_uri = translation[path]
                    if post_process:
                        try:
                            contents = post_process(path, contents, doc)
                        except Exception as e:
                            self.log.log(15, traceback.format_exc())
                            self.log.info("Unable to post_process %s with %s error %s - will skip file" % (
                                file_uri, post_process, e))
                            continue
                    results.append((doc, file_uri, contents))
            else:
                for instance_uri in file_map[gw]:
                    for file_uri in file_map[gw][instance_uri]:
                        doc = file_map[gw][instance_uri][file_uri]
                        _, path = experiment.model.storage.partition_uri(file_uri)
                        try:
                            with open(path, 'rb') as f:
                                contents = f.read()
                            if post_process:
                                contents = post_process(path, contents, doc)
                        except Exception as e:
                            self.log.log(15, traceback.format_exc())
                            self.log.info("Unable to post_process %s with %s error %s - will skip file" % (
                                file_uri, post_process, e))
                            raise
                        results.append((doc, file_uri, contents))

        return results

    def getData(self,
                filename: Optional[str] = None,
                component: Optional[str] = None,
                stage: Optional[int] = None,
                instance: Optional[str] = None,
                myself: Optional[str] = None,
                query: Optional[DictMongoQuery] = None) -> List[Tuple[ComponentDocument, experiment.utilities.data.Matrix]]:
        """Finds csv files matching filters and returns dataframe objects with their contents

        The matching is greedy = All csv files which satisfy conditions are returned.
        Note: Only files ending in "csv" are considered.

        Parameters:
            filename:A string or a regex (uncompiled). The name of the file you want to retrieve, if set to None
                this argument is set to r'.*\.csv' so that it matches file ending in ".csv"
            component(str): The name/reference of a component you want to retrieve data from (string or regex).
               If component is in reference format (i.e stage<%d>.%s) `%d` will be used by the `stage` argument
               if `stage` is not None then  this method
               raises experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage exception
            stage: The index of a stage you want to retrieve data from (integer)
            instance: The location of the experiment you want to retrieve data from (string or regex)
                it can be a file URI (file://$HOSTNAME/$ABSOLUTE_PATH)
            myself: Id of local gateway (e.g. hermes-dev, hartree, etc)
            query: A mongo query to use for further filtering results

        Raises:
            experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage: if component in reference
                format and `stage` is not None

        Returns:
            A list of tuples, one for each CSV file read
            The first element is a dictionary containing metadata about the component which produced the file
            The second is an experiment.utilities.data.Matrix object containing the data
        """
        stage, component = self._util_preprocess_stage_and_component_reference(stage, component)

        if not filename:
            filename = r'.*\.csv'

        ret = self.getFilesAndPostProcess(filename=filename, component=component,
                                          stage=stage, instance=instance, myself=myself,
                                          post_process=self.generate_matrix, query=query)
        # VV: Drop the fileURI from List[(MongoDB dictionary, fileURI, experiment.utilities.data.Matrix)]
        return [(x[0], cast(experiment.utilities.data.Matrix, x[2])) for x in ret]

    def _may_update_insert_document(self, doc, query, update):
        """Conditionally upsert a single document into the MongoDB
        Args:
            doc(Dict[str, Any]): Document to conditionally upsert
            query(Dict[str, Any]): Query to match existing documents
            update(bool): Setting this to True will trigger upsert, setting this to False will perform insert

        Returns:
            boolean indicating whether the MongoDB database was updated
        """
        doc = self._mongo_keys_to_str(doc)

        if update:
            self.log.info("Upserting component document")
            self._upsert_documents([doc])
            modified_db = True
        else:
            modified_db = False

            results = list(self.collection.find(query))
            if results:
                self.log.warning("Cannot add new component document - document already exists")
            else:
                self.log.info("Inserting new component document")
                self.collection.insert_one(doc)
                modified_db = True

        return modified_db

    def _annotate_component_doc_and_generate_query(self, exp, stage, componentName, gateway_id=None):
        # type: (Experiment, int, str, str) -> Tuple[Dict[str, Any], Dict[str, Any]]
        """Generate a Component document, annotate it, and generate a query matching the document"""
        component = exp.findJob(stage, componentName)  # type: experiment.model.data.Job
        instance = exp.generate_instance_location(gateway_id)  # type: str

        if component is None:
            t = (componentName, stage, instance)
            raise ValueError("Could not find component %s in stage %d of experiment at %s" % t)

        query = {
            'instance': instance,
            'name': componentName,
            'stage': stage,
        }

        doc = exp.annotate_component_documents(gateway_id, [component.documentDescription])[0]

        return query, doc

    def addData(self, exp, stage, componentName, update=False, gateway_id=None):
        # type: (Experiment, int, str, bool, str) -> bool
        """Conditionally upsert a Component DocumentDescriptor to the MongoDB"""
        instance = exp.generate_instance_location(gateway_id)  # type: str
        query, doc = self._annotate_component_doc_and_generate_query(exp, stage, componentName, gateway_id)

        modified_db = self._may_update_insert_document(doc, query, update)

        # VV: We've updated the files under the experiment, inform the gateway
        if modified_db and (self._own_gateway_url or self._override_local_gateway_url):
            own_gateway_url = self._override_local_gateway_url or self._own_gateway_url
            self.register_experiment_with_gateway(own_gateway_url, instance)

        return modified_db


class Session:
    def __init__(self, auth_token: str | None, auth_cookie_name: str | None, bearer_key: str | None,
                 max_retries: int = 5, secs_between_retries: int = 5):
        """A HTTP Session wrapper to include authentication and retry requests on failure

        Args:
            max_retries(int): Maximum number of attempts for a failing request
            secs_between_retries(int): Time to wait between successive retries
            auth_token(str): Auth token generated to use in HTTPS requests (header name in @auth_cookie_name)
                If both auth_token and bearer_key are provided, only bearer_key is used.
            auth_cookie_name(str): Name of header whose value is set to @auth_token
            bearer_key(str): API key that can be used as a `Bearer token` for "Basic HTTPS authentication".
                If both auth_token and bearer_key are provided, only bearer_key is used.
        """
        self.log = logging.getLogger('Session')
        self._auth_token = auth_token
        self._bearer_key = bearer_key
        self._auth_cookie_name = auth_cookie_name
        self._max_request_retries = max_retries
        self._secs_between_retries = secs_between_retries

        self._session = requests.Session()

        self._update_session_authentication()

    def update_auth_token(self, cc_auth_token: str | None) -> None:
        self._auth_token = cc_auth_token
        self._update_session_authentication()

    def update_bearer_key(self, cc_bearer_key: str | None) -> None:
        self._bearer_key = cc_bearer_key
        self._update_session_authentication()

    def _update_session_authentication(self):
        if self._bearer_key is not None:
            if self._auth_cookie_name is not None:
                try:
                    self._session.cookies.pop(self._auth_cookie_name)
                except KeyError:
                    pass
            self._session.headers.update({'Authorization': f"Bearer {self._bearer_key}"})
        elif self._auth_token is not None:
            try:
                del self._session.headers['Authorization']
            except KeyError:
                pass
            cookie = requests.cookies.create_cookie(self._auth_cookie_name, self._auth_token)
            self._session.cookies.set_cookie(cookie)
        else:
            # VV: Both the Bearer Key and the Cookie are empty so clear them
            try:
                self._session.cookies.pop(self._auth_cookie_name)
            except KeyError:
                pass

            try:
                del self._session.headers['Authorization']
            except KeyError:
                pass

    def api_path_to_url(self, path: str, endpoint: str) -> str:
        if path.startswith('/'):
            path = path[1:]
        if endpoint.endswith('/'):
            endpoint = endpoint[:-1]
        return '/'.join((endpoint, path))

    def _api_request(self, method: str, url: str, json: Any, _api_verbose: bool, **kwargs) -> requests.Response:
        """Sends a HTTP request to endpoint/path and returns the requests.Response object

        Args:
            method(str): type of the HTTP method (get, post, delete, put)
            url(str): Full HTTP url
            json(Optional[Any]): JSON payload to send along the HTTP request
            _api_verbose(bool): when True prints out more information about the HTTP request - useful for debugging
            **kwargs: Parameters to requests.<method>

        Raises:
            experiment.errors.UnauthorisedRequest for http status codes 403 and 401

        Notes:
            1. This function may also inject authorisation headers for requests self.cc_endpoint
            2. On exceptions, other than experiment.errors.UnauthorisedRequest,
              retries up to self.max_request_retries
        """
        # url = self.api_path_to_url(path, endpoint)

        func = {
            'get': self._session.get,
            'post': self._session.post,
            'delete': self._session.delete,
            'put': self._session.put,
        }
        if method not in func:
            raise ValueError("Unknown HTTP method %s, available %s" % (method, ', '.join(func)))

        if _api_verbose:
            self.log.info("HTTP %s to %s PAYLOAD=%s" % (method, url, pprint.pformat(json)))

        retries = 0
        orig_kwargs = kwargs.copy()
        orig_kwargs['json'] = json

        # VV: Unless otherwise specified, trust self-verified TLS certificates
        if 'verify' not in kwargs:
            kwargs['verify'] = False

        while True:
            try:
                response = func[method](url, json=json, **kwargs)  # type: requests.Response

                if response.status_code in [403, 401]:
                    raise experiment.service.errors.UnauthorisedRequest(
                        url, method, response, '%s Request arguments %s' % (method, orig_kwargs))
                elif response.status_code != 200:
                    raise experiment.service.errors.InvalidHTTPRequest(
                        url, method, response, '%s Request arguments %s' % (method, orig_kwargs))
                return response
            except requests.exceptions.RequestException as e:
                retries += 1
                if retries > self._max_request_retries:
                    raise_with_traceback(e)
                else:
                    if _api_verbose:
                        self.log.warning("Error during HTTP %s to %s - %s will retry" % (method, url, e))

                    time.sleep(self._secs_between_retries)

    def get(self, url: str, json: Any = None, _api_verbose: bool = True, **kwargs) -> requests.Response:
        """Send HTTP GET request to URL and returns the Response object
        Args:
            url(str): HTTP URL
            json(Any): JSON payload to send along the HTTP request
            _api_verbose(bool): when True print out information about the request
            **kwargs: Parameters to requests.get

        Raises:
            experiment.errors.UnauthorisedRequest for http status codes 403 and 401

        Notes:
            1. This function may also inject authorisation headers for requests
            2. On exceptions, other than experiment.errors.UnauthorisedRequest,
              retries up to self.max_request_retries
        """
        return self._api_request('get', url=url, _api_verbose=_api_verbose, json=json, **kwargs)

    def post(self, url: str, json: Any = None, _api_verbose: bool = True, **kwargs) -> requests.Response:
        """Sends a HTTP POST request to URL and returns the Response object
        Args:
            url(str): HTTP Path
            json(Optional[Any]): JSON payload to send along the HTTP request
            _api_verbose(bool): when True print out information about the request
            **kwargs: Parameters to requests.post

        Raises:
            experiment.errors.UnauthorisedRequest for http status codes 403 and 401

        Notes:
            1. This function may also inject authorisation headers for requests
            2. On exceptions, other than experiment.errors.UnauthorisedRequest,
              retries up to self.max_request_retries
        """
        return self._api_request('post', url=url, _api_verbose=_api_verbose, json=json, **kwargs)

    def delete(self, url: str, json: Any = None, _api_verbose: bool = True, **kwargs) -> requests.Response:
        """Sends a HTTP DELETE request to URL and returns the Response object
        Args:
            url(str): HTTP Path
            json(Optional[Any]): JSON payload to send along the HTTP request
            _api_verbose(bool): when True print out information about the request
            **kwargs: Parameters to requests.delete

        Raises:
            experiment.errors.UnauthorisedRequest for http status codes 403 and 401
            experiment.errors.InvalidHttpRequest when http status code is other than 200

        Notes:
            1. This function may also inject authorisation headers for requests
            2. On exceptions, other than experiment.errors.UnauthorisedRequest,
              retries up to self.max_request_retries
        """
        return self._api_request('delete', url=url, _api_verbose=_api_verbose, json=json, **kwargs)

    def put(self, url: str, json: Any = None, _api_verbose: bool = True, **kwargs) -> requests.Response:
        """Sends a HTTP PUT request to URL and returns the Response object
        Args:
            url(str): HTTP Path
            json(Optional[Any]): JSON payload to send along the HTTP request
            _api_verbose(bool): when True print out information about the request
            **kwargs: Parameters to requests.put

        Raises:
            experiment.errors.UnauthorisedRequest for http status codes 403 and 401

        Notes:
            1. This function may also inject authorisation headers for requests self.cc_endpoint
            2. On exceptions, other than experiment.errors.UnauthorisedRequest,
              retries up to self.max_request_retries
        """
        return self._api_request('put', url=url, _api_verbose=_api_verbose, json=json, **kwargs)


class MongoClient(Mongo):
    """Offers the same Api with Mongo() with the difference that interaction with the MongoDB backend happens
    via a thin REST-API wrapper. The wrapper invokes internal functions of Mongo()"""

    def __init__(self, remote_mongo_server_url, gateway_registry_url=None,
                 own_gateway_url=None, override_local_gateway_url=None,
                 own_gateway_id=None, max_http_batch=10, session: Session = None):
        # VV: Calls constructors up-to (but not including) Mongo - i.e. just _DatabaseBackend
        super(Mongo, self).__init__(gateway_registry_url, own_gateway_url, override_local_gateway_url, own_gateway_id,
                         max_http_batch)
        if remote_mongo_server_url is not None and remote_mongo_server_url.endswith('/'):
            remote_mongo_server_url = remote_mongo_server_url[:-1]

        self._remote_mongo_url = remote_mongo_server_url

        self.log = logging.getLogger('MongoClient')
        if session is None:
            session = Session(None, None, None)

        self._session: Session = session

    def __str__(self):
        return "MongoClient interface serving values via the REST-API proxy that is hosted on %s" %\
                self._remote_mongo_url

    def is_connected(self, verify_tls=False):
        # VV: trailing "/" is important
        url = '%s/hello/' % (self._remote_mongo_url)
        reply = None
        try:
            reply = self._session.get(url, verify=verify_tls).json()
            self.log.log(14, "Hello response was %s" % reply)
            assert reply == "hello"
        except Exception:
            self.log.log(15, "Unable to connect to MongoDB proxy, exception: %s" % traceback.format_exc())
            return False
        return True

    def _may_update_insert_document(self, doc, query, update, verify_tls=False):
        """Conditionally upsert a single document into the MongoDB
        Args:
            doc(Dict[str, Any]): Document to conditionally upsert
            query(Dict[str, Any]): Query to match existing documents
            update(bool): Setting this to True will trigger upsert, setting this to False will perform insert

        Returns:
            boolean indicating whether the MongoDB database was updated
        """
        url = '%s/documents/api/v1.0/may-insert' % self._remote_mongo_url
        payload = {'doc': doc, 'query': query, 'update': update}
        response = self._session.post(url, verify=verify_tls, json=payload)

        if response.status_code != 200:
            raise experiment.service.errors.InvalidHTTPRequest(url, 'post', response,
                                                       ' - unable to _may_update_insert_document in MongoClient')
        return response.json()['updated']

    def _upsert_documents(self, documents, verify_tls=False):
        # type: (List[Dict[str, Any]], bool) -> None
        url = '%s/documents/api/v1.0/upsert' % self._remote_mongo_url

        max_batch_size = 200
        # VV: Upset the documents in batches of max_batch_size, to avoid sending 1 huge request
        for batch_start in range(0, len(documents), max_batch_size):
            batch_end = batch_start + max_batch_size
            response = self._session.post(url, verify=verify_tls, json={
                'documents': documents[batch_start:batch_end]})

            if response.status_code != 200:
                raise experiment.service.errors.InvalidHTTPRequest(
                    url, 'post', response,
                    ' - unable to upsert_documents(docs[%d:%d]) in MongoClient' % (batch_start, batch_end))

    def _query_mongodb(
            self,
            query: DictMongoQuery,
            include_properties: List[str] | None = None,
            stringify_nan: bool = False,
            verify_tls: bool = False,
            _api_verbose:bool = True,
    ) -> List[ComponentDocument]:
        """Delegates a query to the MongoDB proxy, in turn the MongoDB proxy invokes Mongo._query_mongodb() and returns
        the results

        Args:
            query: the mongoquery
            include_properties: A list of column names or a list with just the string "*" which will automatically
                expand to all the column names in the properties DataFrame that matching experiments have measured.
                The column names in include_properties are case-insensitive, and
                the returned DataFrame will contain lowercase column names.
                When this argument is provided the ST4SD Datastore API (mongo-proxy) will insert the field
                `interface.propertyTable` to matching `experiment` type mongoDocuments. This field will contain
                a dictionary representation of the properties DataFrame that the matching experiment measured. The
                columns in that DataFrame will be those specified in `include_properties`. Column names that do not
                exist will in the DataFrame will be silently discarded. The column `input-id` will always be fetched
                even if not specified in `include_properties`.
            stringify_nan: A boolean flag that allows converting NaN and infinite values to strings
            verify_tls: whether to ensure that the remote endpoint is using properly configured TLS
            _api_verbose: Set to true to print INFO level messages

        Returns:
                A list of matching MongoDocuments
        """
        url = '%s/documents/api/v1.0/query' % self._remote_mongo_url
        params = {'stringifyNaN': stringify_nan}

        if include_properties:
            params['includeProperties'] = ','.join([x.lower() for x in include_properties])

        reply = None
        try:
            response = self._session.post(url, json=query, verify=verify_tls, params=params)  # type: requests.Response
            if response.status_code != 200:
                raise experiment.service.errors.InvalidHTTPRequest(
                    url, 'post', response, ". mongoProxy.Query REST-API returned status code %s!=200, response "
                                           "contents: %s - This may indicate an issue with the mongoProxy service OR "
                                           "a transient network connection problem. If the problem persists "
                                           "contact the admins." % (response.status_code, response.content))

            reply = response.json()
        except Exception as e:
            self.log.warning("Could not fetch and decode reply %s - raise %s" % (reply, e))
            raise_with_traceback(e)
        lbl_docs = 'document-descriptors'
        if isinstance(reply, dict) is False or lbl_docs not in reply:
            raise ValueError("mongoProxy.Query REST-API returned an invalid JSON document (%s) - "
                             "This indicates an issue with the mongoProxy service, contact the admins." % reply)
        return reply[lbl_docs]

    def _kernel_getDocument(
            self,
            filename: Optional[str] = None,
            component: Optional[str] = None,
            stage: Optional[int] = None,
            instance: Optional[str] = None,
            type: Optional[str] = None,
            query: Optional[DictMongoQuery] = None,
            include_properties: List[str] | None = None,
            stringify_nan: bool = False,
            _api_verbose=True,
            ) -> List[ComponentDocument]:
        """Queries the ST4SD Datastore (CDB) for MongoDB documents.

        Parameters other than @query may end up overriding those you define in @query. Method may also post-process
        the values of parameters that are not @query. This method post-processes the query right before sending it to
        Mongo so that its keys are strings and its string-values are regular-expressions.

        Args:
            instance: File URI in the form of file://$GATEWAY_ID/absolute/path/to/instance/dir
            stage: index of stage comprising component
            component: The name/reference of a component you want to retrieve data from (string or regex).
               If component is in reference format (i.e stage<%d>.%s) `%d` will be used by the `stage` argument
               if `stage` is not None then  this method
               raises experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage exception
            filename: relative to the working dir of the component path
            type: type of the document to return (e.g. "component", "user-metadata", "experiment")
            query: A mongo query to use for further filtering results
            include_properties: A list of column names or a list with just the string "*" which will automatically
                expand to all the column names in the properties DataFrame that matching experiments have measured.
                The column names in include_properties are case-insensitive, and
                the returned DataFrame will contain lowercase column names.
                When this argument is provided the ST4SD Datastore API (mongo-proxy) will insert the field
                `interface.propertyTable` to matching `experiment` type mongoDocuments. This field will contain
                a dictionary representation of the properties DataFrame that the matching experiment measured. The
                columns in that DataFrame will be those specified in `include_properties`. Column names that do not
                exist will in the DataFrame will be silently discarded. The column `input-id` will always be fetched
                even if not specified in `include_properties`.
            stringify_nan: A boolean flag that allows converting NaN and infinite values to strings
            _api_verbose: Set to true to print INFO level messages

        Raises:
            experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage: if component is reference and
                stage is not None

        Returns:
            Returns a List of dictionaries created out of MongoDB documents
        """
        # VV: First generate a new query by combining the synthetic dictionary that we generate out of
        # @filename, @component, @stage, @instance, and @type and then using it to override the @query parameter
        # Note that we do not modify, or preprocesses, the contents of the @query parameter at all
        query = self.preprocess_query(filename=filename, component=component, stage=stage, instance=instance,
                                      type=type, query=query)

        self.log.debug("Query dict: %s" % query)
        return self._query_mongodb(query, include_properties=include_properties, stringify_nan=stringify_nan,
                                   _api_verbose=_api_verbose)


class Experiment(object):

    '''Wrapper around a single experiment allowing it to act like a document store

    The main use of the interface is to retrieve CSV from the expereimnt'''

    @classmethod
    def dbForLocation(cls, location, platform=None, attempt_shadowdir_repair=True):

        '''Returns a db initialised with the experiment data at location

        Note: Require platform to configure instance correctly.
        TODO: This should really be stored somehwere in the instance dir'''

        d = experiment.model.storage.ExperimentInstanceDirectory(
            location, attempt_shadowdir_repair=attempt_shadowdir_repair)
        e = experiment.model.data.Experiment(d, platform=platform,
                                             updateInstanceConfiguration=False, is_instance=True)

        return Experiment(e)

    def __init__(self, expInstance: "Experiment"):

        '''Initialise a Mongo object connecting to a specific database'''

        self.exp = expInstance

        self.dbPath = os.path.join(self.exp.instanceDirectory.location, "data/exp.doc")
        self.log = logging.getLogger("output.experimentdb")

        if not os.path.exists(self.dbPath):
            self.data = self.exp.documentDescription
            try:
                with open(self.dbPath, 'w') as f:
                    pickle.dump(self.data, f)
            except Exception as error:
                self.log.warning("Error writing db file: %s" % error)
        else:
            try:
                with open(self.dbPath) as f:
                    self.data = pickle.load(f)
            except Exception as error:
                self.log.warning("Error reading db file: %s" % error)
                self.data = self.exp.documentDescription

        self.log = logging.getLogger()

    def __str__(self):

        return "Experiment DB interface accessing data at %s" % self.exp.instanceDirectory.location

    def update(self):

        '''Updates the document description used to access experiment data

        Use when experiment is live and its contents are changing'''

        self.data = self.exp.documentDescription
        try:
            with open(self.dbPath, 'w') as f:
                pickle.dump(self.data, f)
        except Exception as error:
            self.log.warning("Error writing db file: %s" % error)

    def getData(self, filename=None, component=None, stage=None, instance=None, limit=100):

        '''Finds csv files matching filters and returns dataframe objects with their contents

        The matching is greedy = All csv files which satisfy conditions are returned.
        Note: Only files ending in "csv" are considered.

        Parameters:
            filename:A string or a regex (uncompiled). The name of the file you want to retrieve
            component: The name of a component you want to retrieve data from (string or regex)
            stage: The index of a stage you want to retrieve data from (integer)
            instance: Not used
            limit: The number of CSV objects to return. Default 100.
                Caution: Components can contain many thousands of CSV files!

        Returns:
            A list of tuples, one for each CSV file read
            The first element is a dictionary containing metadata on the file (see below for contents)
            The second is an experiment.utilities.data.Matrix object containing the data
        '''

        # 1. Find components whose fields match the given values (Mongo Query)
        # 2. Filter in app for the filename ....
        #
        # Alternate: Perform entire operation in Mongo using aggregate and filters etc.

        queryDict = {}

        if component is not None:
            queryDict["name"] = re.compile(component)

        if stage is not None:
            queryDict["stage"] = stage

        self.log.debug("Query dict: %s", queryDict)

        result = []
        for document in self.data:
            nameMatchResult = True
            if "name" in queryDict and "name" in document:
                if queryDict["name"].match(document["name"]) is None:
                    nameMatchResult = False
            elif "name" in queryDict:
                nameMatchResult = False #No name field in doc

            stageMatchResult = True
            if "stage" in queryDict and "stage" in document:
                if queryDict["stage"].match(document["stage"]) is None:
                    stageMatchResult = None
            elif "stage" in queryDict:
                stageMatchResult = False

            if (nameMatchResult and stageMatchResult) is True:
                    result.append(document)

        self.log.debug("Found %d matching documents" % len(result))

        d = []
        allFilenames = []
        #Search for matches doc by doc
        #This allows us to use the doc metadata to tag the dataframes
        for doc in result:
            files = []
            if "files" in doc:
                files = [el for el in doc["files"] if os.path.splitext(el)[1] == ".csv"]

            #Filter files based on filename
            if filename is not None:
                fre = re.compile(filename)
                # Elements of files are full paths - compare against filename only
                #print >>sys.stderr, 'Search string:', filename, 'Files:', files
                #for f in files:
                #   #print 'Matching against:', os.path.split(f)[1], 'After escaping:', re.escape(os.path.split(f)[1])
                #   res =  fre.match(re.escape(os.path.split(f)[1]))
                #   print 'Result of escaped mathc', res
                #   print  'Result of non-escapend match', fre.match(os.path.split(f)[1])

                files = [f for f in files if fre.match(os.path.split(f)[1]) is not None]

            if len(files) > limit:
                self.log.warning("%d files were found - will only instantiate dataframes for first %d" % (
                    len(files), limit))
                files = files[:limit]

            # instantiate all the dataframes
            # TODO: Need to handle headers or lack thereof

            executor = concurrent.futures.ProcessPoolExecutor()
            d = executor.map(LoadMatrix, files, chunksize=2)

        return d


class Tiny(object):

    '''TinyDB interface'''

    def __init__(self, location):

        '''Initialise a TinyDB object connecting to a specific database'''

        self.location = location
        self.db = tinydb.TinyDB(location)
        self.log = logging.getLogger()

    def __str__(self):

        return "TinyDB interface at %s with %d objects" % (self.location, len(self.db))

    def _experimentInCollection(self, location):
        # type: (str) -> bool
        '''Returns True if an experiment instance is in the database - based on path

        Parameter:
            location (string): The path to the instance or a file://$HOSTNAME/$ABSOLUTE_PATH URI
        '''

        location = preprocess_file_uri(location)

        retval = False
        d = self.db.search(tinydb.where('instance').matches(location))

        if len(d) != 0:
            retval = True

        return retval

    def addExperimentAtLocation(self, location, platform=None, attempt_shadowdir_repair=True):

        if self._experimentInCollection(location) is True:
            self.log.info("Experiment documents already present in collection")
        else:
            expDir = experiment.model.storage.ExperimentInstanceDirectory(
                location, attempt_shadowdir_repair=attempt_shadowdir_repair)
            exp = experiment.model.data.Experiment(expDir, platform=platform,
                                                   updateInstanceConfiguration=False, is_instance=True)
            self.addExperiment(exp)

    def addExperiment(self, exp: "Experiment"):

        '''Adds the experiment to the mongo db so it can be queried'''

        # Each experiment is a document
        # stages, components are embeded (2 embed levels)
        # Files are array attributes of components (assuming no querying on file properties)
        #
        # The reason for embeding v multi-doc is that the purposes of the DB is only
        # to access data from running/complete experiment instances.
        # Most data stored will be specific to each document (little repetition)
        # For the repition that exists e.g. components->stage, component command lines
        # it is small and has no particular value (in this case) for quering indepedently.
        # as they contain no data.
        #
        # Assembling the json to insert is recursive - ask each component to output its json.
        # FIXME: Add component CL to documentDescription

        # Check has the experiments documents already been added
        if self._experimentInCollection(exp.instanceDirectory.location) is True:
            self.log.info("Experiment documents already present in collection")
        else:
            self.db.insert_multiple(exp.documentDescription)


    def addData(self, exp: "Experiment", stage, componentName, update=False):

        '''Adds data from component in stage of exp to the database

        Use to add data from components which were not in the experiment when originaly added (for example)

        If component is already present this method does nothing'''

        component = exp.findJob(stage, componentName)

        instance = preprocess_file_uri(exp.instanceDirectory.location)

        if component is None:
            t = (componentName, stage, instance)
            raise ValueError("Could not find component %s in stage %d of experiment at %s" % t)

        query = tinydb.Query()

        queryList = []
        queryList.append(query.instance.matches(instance))
        queryList.append(query.name.matches(componentName))
        queryList.append((query.stage == stage))

        self.log.debug("Queries %s", queryList)
        queryInstance = reduce(operator.and_,queryList)
        self.log.debug("Query Instance %s", queryInstance)
        result = self.db.search(queryInstance)

        doc = component.documentDescription

        # Have to add the same data experiment does when documentDescription is called on it
        # FIXME: This should be encapsulated
        try:
            name = os.path.split(exp.instanceDirectory.packageLocation)[1]
        except AttributeError:
            name = "Unknown"

        doc["experimentName"] = name
        doc['instance'] = instance

        if len(result) != 0:
            #TODO: Add update flag
            print('Data for component %s already present for this experiment' % componentName, file=sys.stderr)
            if update and len(result) == 1:
                print('Will update element %d' % result[0].eid, file=sys.stderr)
                self.db.update(fields=doc, eids=[result[0].eid])
            elif update:
                print('Cannot update - more than one element matched criteria', file=sys.stderr)
        else:
            self.db.insert(doc)

    def _matchDataFrame(self, query, docs):

        '''Returns all the files matching query in docs

        Args:
            query: A string. May be a regular expression. Will be matched to filenames in docs
            docs: A list of dictionaries returned by getDocument

        Returns:
            A dictionary whose keys are indexes and whose values are the full filenames matching the query
            The index gives the location of the owning document in docs
        '''

        allFiles = {}
        for index, doc in enumerate(docs):
            files = []
            #Load all the filenames
            if "files" in doc:
                files.extend([el for el in doc["files"] if os.path.splitext(el)[1] in [".dat", ".csv"]])

            # Filter files based on filename
            if query is not None:
                fre = re.compile(query)
                # Elements of files are full paths - compare against filename only
                # print >>sys.stderr, 'Search string:', filename, 'Files:', files
                # for f in files:
                #   #print 'Matching against:', os.path.split(f)[1], 'After escaping:', re.escape(os.path.split(f)[1])
                #   res =  fre.match(re.escape(os.path.split(f)[1]))
                #   print 'Result of escaped mathc', res
                #   print  'Result of non-escapend match', fre.match(os.path.split(f)[1])

                files = [f for f in files if fre.match(os.path.split(f)[1]) is not None]

            if len(files) != 0:
                allFiles[index] = files

        return allFiles

    def getData(self, filename=None, component=None, stage=None, instance=None, limit=100):

        '''Finds csv files matching filters and returns dataframe objects with their contents

        The matching is greedy = All csv files which satisfy conditions are returned.
        Note: Only files ending in "csv" are considered.

        Parameters:
            filename:A string or a regex (uncompiled). The name of the file you want to retrieve
            component: The name of a component you want to retrieve data from (string or regex)
            stage: The index of a stage you want to retrieve data from (integer)
            instance: Not used
            limit: The number of CSV objects to return. Default 100.
                Caution: Components can contain many thousands of CSV files!

        Returns:
            A list of tuples, one for each CSV file read
            The first element is a dictionary containing metadata on the file (see below for contents)
            The second is an experiment.utilities.data.Matrix object containing the data
        '''

        result = self.getDocument(component, stage, instance)

        d = []

        # Search for matches doc by doc
        # This allows us to use the doc metadata to tag the dataframes
        files = self._matchDataFrame(filename, result)
        if len(files) == 0:
            filesFound = 0
        else:
            filesFound = reduce(operator.add, [len(files[k]) for k in list(files.keys())])

        # instantiate all the dataframes
        # TODO: Need to handle headers or lack thereof

        #executor = concurrent.futures.ProcessPoolExecutor()
        #try:
        #    d = executor.map(LoadMatrix, files, chunksize=2)
        #except Exception as error:
        #    print >>sys.stderr, error
        #    raise

        instantiatedFiles = []
        count = 0
        for index in list(files.keys()):
            doc = result[index]
            for f in files[index]:
                try:
                    d.append(experiment.utilities.data.matrixFromCSVFile(f, readHeaders=True))
                    setattr(d[-1], 'meta',
                            {'component': doc['name'], 'instance': doc['instance'],
                             'stage': doc['stage']})
                except experiment.utilities.data.CouldNotDetermineCSVDialectError:
                    print('Could not determine CSV format for %s. Skipping' % f, file=sys.stderr)
                except ValueError as error:
                    print('Error while processing %s' % f, file=sys.stderr)
                    print(error, file=sys.stderr)
                    print('Skipping', file=sys.stderr)

                else:
                    count+=1
                    instantiatedFiles.append(f)
                    if "name" in doc:
                        d[-1].setName("%s:%s" % (doc["name"], os.path.split(f)[1]))

                if count == limit:
                    self.log.warning(
                        "%d files were found - will only instantiate dataframes for first %d" % (filesFound, limit))
                    break

        filenames = [os.path.split(f)[1] for f in instantiatedFiles]

        return list(zip(filenames, d))

    def getDocument(self,
                    component=None,
                    stage=None,
                    instance=None,
                    experimentName=None,
                    dataframe=None,
                    **kwargs):

        '''Finds document files matching filters

           Parameters:
               component: The name of a component you want to retrieve data from (string or regex)
               stage: The index of a stage you want to retrieve data from (integer)
               instance: String or regex absolute path or file://$HOSTNAME/$ABSOLUTE_PATH URI
               experimentName: Not used. Have to check its being recorded correctly
               dataframe: The name/regex of a file required to be in the document

           Returns:
               A list of documents matching the query (dictionaries)
        '''
        instance = preprocess_file_uri(instance)

        query = tinydb.Query()

        queryList = []
        if experimentName is not None:
            queryList.append(query.instance.matches(experimentName))

        if instance is not None:
            queryList.append(query.instance.matches(instance))

        if component is not None:
            queryList.append(query.name.matches(component))

        if stage is not None:
            queryList.append((query.stage == stage))

        # VV: Transform kwargs into queries; emulates query.<key>.matches(pattern)
        for q in kwargs:
            queryList.append(query[q].matches(kwargs[q]))

        self.log.debug("Queries %s", queryList)
        try:
            queryInstance = reduce(operator.and_, queryList)
        except TypeError:
            queryInstance = None

        self.log.debug("Query Instance %s", queryInstance)
        if queryInstance is not None:
            result = self.db.search(queryInstance)
        else:
            result = self.db.all()

        if dataframe is not None:
            #Returns a dictionary whose keys are the indexes of documents in result
            #that contains files matching the query
            files = self._matchDataFrame(dataframe, result)
            result = [result[index] for index in list(files.keys())]

        return result

    def valuesMatchingQuery(self, **query):

        '''Returns unique sets of field values in the receiver which match query

        For example if the query was component="Co*" it would return a set containing
        all component names starting with "Co*"

        If there are multiple query fields the elements of the set are lists
        '''

        docs = self.getDocument(**query)

        fields = [k for k in list(query.keys()) if query[k] is not None]
        s = set()

        #The document database contains component documents and other documents
        #i.e. not every document is a component
        #so not every document has a component field
        #Have to replace "component" with "name"

        if 'component' in fields:
            i = fields.index('component')
            fields[i] = 'name'

        for d in docs:
            v = [d[f] for f in fields if f in list(d.keys())]
            if len(v) == 1:
                s.add(v[0])
            else:
                s.update(v)

        return s

    def isNonEmptyQuery(self, component=None,
                        stage=None,
                        instance=None,
                        experimentName=None,
                        dataframe=None):

        '''Returns True of the query refers to an entity/set of entities that exist in the DB

        If a dataframe is given then it must be in all documents matched by the other fields for the
        query to be non-empty.
        '''

        docs = self.getDocument(component=component,
                                stage=stage,
                                instance=instance,
                                experimentName=experimentName)

        if dataframe is not None and len(docs) != 0:
            files = self._matchDataFrame(dataframe, docs)
            retval = True if len(files) != 0 else False
        else:
            retval = True if len(docs) != 0 else False

        return retval

        #Example of aggregation with non-flat schema (components subdocs of experiments)
#To get indvidual embeded documents the following process is required
#1. Match - Find and return top-level documents that contain embedded document matching query - NO FILTERING
#2. Unwind - Operate on the result of a match to convert subdocuments into documents - CONVERT
#3. Filter - Filter the unwound docs based on the original query - FILTER
# c = exps.aggregate([
#     #Returns a set of experiments documents with components called MixtureSimulation3
#     #This is the annoying part - since the match is on the top-level document (experiment)
#     #EVERY component of each matching experiment is in the match (i.e. full experiment docs are retunred)
#     { "$match": {
#         "components.name": "MixtureSimulation3"
#     }},
#
#     #Converts the contents of the components field of the returned documents into stand-alone documents
#     #Now we have an array of components. However although a match was done above, there will be
#     #alot of redundant components (as the match didn't filter the matching subdocs).
#     {"$unwind":"$components"},
#
#     #Filters the unwound components based on some criteria - in this case the same one as original
#     { "$match": {
#         "components.name": "MixtureSimulation3",
#         "components.stage": 4
#     }}
# ])


class ExperimentRestAPI:
    """Wrapper to the ST4SD Datastore (CDB) and ST4SD Runtime Service REST-APIs
    """
    def __init__(self, consumable_computing_rest_api_url, cdb_registry_url=None, cdb_rest_url=None,
                 max_retries=10, secs_between_retries=5, test_cdb_connection=True,
                 mongo_host=None, mongo_port=None, mongo_user=None, mongo_password=None, mongo_authsource=None,
                 mongo_database=None, mongo_collection=None, max_http_batch=10, cc_auth_token=None,
                 cc_bearer_key=None, validate_auth=True, discover_cdb_urls=True):
        """Instantiate a wrapper to the ST4SD Datastore (CDB) and ST4SD Runtime Service REST-APIs

        The ST4SD Datastore is also known as Centralized Database (CDB).

        Args:
            consumable_computing_rest_api_url(string): URL to ST4SD Runtime Service REST-API endpoint
            cdb_registry_url(string): URL to ST4SD Datastore registry REST API endpoint
            cdb_rest_url(string): URL to ST4SD Datastore MongoDB REST-API endpoint
            max_retries(int): Maximum number of attempts for a failing request
            secs_between_retries(int): Time to wait between successive retries
            max_http_batch(int): Maximum number of files per batch when downloading files from
              Cluster Gateways
            test_cdb_connection(bool): If set to True, the constructor will assert that it can access the
              ST4SD Datastore MongoDB REST-API endpoint
            mongo_host(str): Host (ip/domain) of MongoDB backend (not required when using a cdb_rest_url)
            mongo_port(int): Port of MongoDB backend (not required when using a cdb_rest_url)
            mongo_user(str): Username to authenticate to MongoDB (not required when using a cdb_rest_url)
            mongo_password(str): Password to use when authenticating to MongoDB (not required when using a cdb_rest_url)
            mongo_database(str): Name of the database in MongoDB (not required when using a cdb_rest_url)
            mongo_collection(str): Name of the collection in the MongoDB database (not required when using a
                cdb_rest_url)
            cc_auth_token(str): Auth token generated by the ST4SD Runtime Service REST-API. Will be used whenever
                ExperimentRestAPI sends HTTP requests to @consumable_computing_rest_api_url.
                If both cc_auth_token and cc_bearer_key are provided, only cc_bearer_key is used.
            cc_bearer_key(str): API key that can be used as a `Bearer token` to authenticate to the Consumable Computing
                REST-API ExperimentRestAPI sends HTTP requests to @consumable_computing_rest_api_url.
                If both cc_auth_token and cc_bearer_key are provided, only cc_bearer_key is used.
            validate_auth(bool): Attempt to validate the authorisation key/token (provided the key/token is not None)
            discover_cdb_urls(bool): Attempt to query the CC end-point to get any non-provided CDB urls
              (provided that consumable_computing_rest_api_url is not None)
        """

        self.log = logging.getLogger('ExperimentRestAPI')
        self.cc_end_point = consumable_computing_rest_api_url
        self.max_request_retries = max_retries
        self.secs_between_retries = secs_between_retries

        # VV: Create a session with auth-credentials that work for one cluster,
        # reuse the session for HTTPS requests to cdb-mongodb-proxy, cdb-gateway, cdb-gateway-registry,
        # and cc-rest-api
        self._session: Session = Session(
            auth_token=cc_auth_token, auth_cookie_name="oauth-proxy", bearer_key=cc_bearer_key,
            max_retries=self.max_request_retries, secs_between_retries=self.secs_between_retries
        )

        if validate_auth is True and (cc_bearer_key is not None or cc_auth_token is not None):
            try:
                self.api_request_get("", _api_verbose=False, decode_json=False)
            except experiment.service.errors.UnauthorisedRequest:
                raise
            else:
                self.log.info("Authenticated to %s" % self.cc_end_point)

        if consumable_computing_rest_api_url and consumable_computing_rest_api_url.endswith('/'):
            consumable_computing_rest_api_url = consumable_computing_rest_api_url[:-1]
        if cdb_rest_url and cdb_rest_url.endswith('/'):
            cdb_rest_url = cdb_rest_url[:-1]
        if cdb_registry_url and cdb_registry_url.endswith('/'):
            cdb_registry_url = cdb_registry_url[:-1]

        def is_http_url(url):
            # type: (str) -> bool
            """Empty URLs are considered neither http, nor https. The absense of a https:// prefix for a non-empty
            URL means that the URL is a http:// one
            """
            return url and url.startswith('https://') is False

        def warn_about_http_url(url, name):
            # type: (str, str) -> None
            """Print a warning message if a url is http://"""
            if is_http_url(url):
                self.log.warning("%s %s is not a https:// URL - if this is the intended URL you should upgrade your "
                                 "workflow stack deployment to use https:// URLs, a http:// url may also be a "
                                 "programming error." % (name, url))

        for url, name in [(cdb_registry_url, 'cdb_registry_url'), (cdb_rest_url, 'cdb_rest_url'),
                          (consumable_computing_rest_api_url, 'consumable_computing_rest_api_url')]:
            warn_about_http_url(url, name)

        if discover_cdb_urls and consumable_computing_rest_api_url:
            # VV: if there's any missing URL, fetch the url-map and then fill-in the missing URLs to the CDB services
            if cdb_registry_url is None or cdb_rest_url is None:
                try:
                    # VV: trailing "/" is important
                    url_map = self.api_request_get('url-map/', _api_verbose=True)
                except Exception as e:
                    if is_http_url(consumable_computing_rest_api_url):
                        self.log.info("Unable to retrieve CDB service URL-map from ConsumableComputing which is served "
                                      "via http:// if this is not the intended URL schema switch to a https:// URL and "
                                      "create a new instance of the %s class - %s" % (e, type(self)))
                    else:
                        self.log.info("Unable to retrieve CDB service URL-map from ConsumableComputing - %s:%s" % (
                            type(e), e))
                else:
                    try:
                        if cdb_rest_url is None:
                            cdb_rest_url = url_map['cdb-rest']
                            self.log.info("Discovered cdb-rest URL = %s" % cdb_rest_url)
                        if cdb_registry_url is None:
                            cdb_registry_url = url_map['cdb-gateway-registry']
                            self.log.info("Discovered cdb-gateway-registry URL = %s" % cdb_registry_url)
                    except Exception as e:
                        self.log.info("Unable to use CDB Service URL-map obtained from ConsumableComputing, map: %s, "
                                      "error %s" % (url_map, e))

        # VV: Print a message when there is limited CDB functionality

        you_will_miss = []
        if cdb_rest_url is None:
            you_will_miss.append("query database")
        if cdb_registry_url is None:
            you_will_miss.append("retrieve files")
        if you_will_miss:
            self.log.warning("Missing CDB URLs - you cannot %s via cdb_ methods" % ' OR '.join(you_will_miss))

        self.cdb_registry_url = cdb_registry_url
        self.cdb_rest_url = cdb_rest_url

        if self.cdb_rest_url:
            cl = MongoClient(self.cdb_rest_url, self.cdb_registry_url,
                                  max_http_batch=max_http_batch, session=self._session)
            endpoint = self.cdb_rest_url
        elif mongo_host and mongo_port:
            cl = Mongo(mongo_host, mongo_port, mongo_database, mongo_collection, cdb_registry_url,
                            mongo_username=mongo_user, mongo_password=mongo_password,
                            mongo_authSource= mongo_authsource, max_http_batch=max_http_batch, session=self._session)
            endpoint = 'tcp://%s:%s' % (mongo_host, mongo_port)
        else:
            cl = None
            endpoint = None  # VV: keep python linter happy

        self.cl = cl  # type: Mongo

        if test_cdb_connection:
            if self.cl is None or self.cl.is_connected() is False:
                if self.cdb_rest_url and is_http_url(self.cdb_rest_url):
                    raise ValueError("Could not connect to mongodb endpoint at %s - http:// URLs are deprecated, this "
                                     "might be a programmer error, should you be using a https:// URL instead?"
                                     % endpoint)
                else:
                    raise ValueError("Could not connect to mongodb endpoint at %s" % endpoint)
            else:
                self.log.info("MongoDB endpoint %s is reachable" % endpoint)

    def update_consumable_computing_auth_token(self, cc_auth_token):
        # type: (Optional[str]) -> None
        self._session.update_auth_token(cc_auth_token)

    def update_consumable_computing_bearer_key(self, cc_bearer_key):
        # type: (Optional[str]) -> None
        self._session.update_bearer_key(cc_bearer_key)

    def api_path_to_url(self, path, endpoint=None):
        endpoint = endpoint or self.cc_end_point
        if path.startswith('/'):
            path = path[1:]
        if endpoint.endswith('/'):
            endpoint = endpoint[:-1]
        return '/'.join((endpoint, path))

    def api_request_get(self, path, json_payload=None, decode_json=True, return_response=False,
                        _api_verbose=True, endpoint=None, **kwargs):
        # type: (str, Optional[Any], bool, bool, bool, str, Dict[str, Any]) -> Union[Any, str, requests.Response]
        """Send HTTP GET request and return either the JSON or the Text of the HTTP response
        Args:
            path(str): HTTP Path
            json_payload(Optional[Any]): JSON payload to send along the HTTP request
            decode_json(bool): When True, decodes response text into a JSON dictionary, else returns raw
                response text
            return_response(bool): When True, returns requests.Response object (overrides decode_json)
            _api_verbose(bool): when True print out information about the request
            endpoint(str): HTTP host domain
            **kwargs: Parameters to requests.get

        Raises:
            experiment.errors.UnauthorisedRequest for http status codes 403 and 401

        Notes:
            1. This function may also inject authorisation headers for requests self.cc_endpoint
            2. On exceptions, other than experiment.errors.UnauthorisedRequest,
              retries up to self.max_request_retries
        """
        url = self._session.api_path_to_url(path, endpoint or self.cc_end_point)
        response = self._session.get(url=url, json=json_payload, _api_verbose=_api_verbose, **kwargs)

        if return_response:
            return response
        if decode_json:
            return response.json()
        return response.text

    def api_request_post(self, path, json_payload=None, decode_json=True, _api_verbose=True, endpoint=None, **kwargs):
        # type: (str, Optional[Any], bool, bool, str, Dict[str, Any]) -> Union[Any, str]
        """Sends a HTTP POST request to endpoint/path and return either the JSON or the Text of the HTTP response
        Args:
            path(str): HTTP Path
            json_payload(Optional[Any]): JSON payload to send along the HTTP request
            decode_json(bool): When true decodes response text into a JSON dictionary, else returns raw
                response text
            _api_verbose(bool): when True print out information about the request
            endpoint(str): HTTP host domain
            **kwargs: Parameters to requests.post

        Raises:
            experiment.errors.UnauthorisedRequest for http status codes 403 and 401

        Notes:
            1. This function may also inject authorisation headers for requests self.cc_endpoint
            2. On exceptions, other than experiment.errors.UnauthorisedRequest,
              retries up to self.max_request_retries
        """
        url = self._session.api_path_to_url(path, endpoint or self.cc_end_point)
        response = self._session.post(url=url, json=json_payload, _api_verbose=_api_verbose, **kwargs)

        if decode_json:
            return response.json()
        return response.text

    def api_request_delete(self, path, json_payload=None, decode_json=True, _api_verbose=True, endpoint=None, **kwargs):
        # type: (str, Optional[Any], bool, bool, str, Dict[str, Any]) -> Union[Any, str]
        """Sends a HTTP DELETE request to endpoint/path and return either the JSON or the Text of the HTTP response
        Args:
            path(str): HTTP Path
            json_payload(Optional[Any]): JSON payload to send along the HTTP request
            decode_json(bool): When true decodes response text into a JSON dictionary, else returns raw
                response text
            _api_verbose(bool): when True print out information about the request
            endpoint(str): HTTP host domain
            **kwargs: Parameters to requests.delete

        Raises:
            experiment.errors.UnauthorisedRequest for http status codes 403 and 401
            experiment.errors.InvalidHttpRequest when http status code is other than 200

        Notes:
            1. This function may also inject authorisation headers for requests self.cc_endpoint
            2. On exceptions, other than experiment.errors.UnauthorisedRequest,
              retries up to self.max_request_retries
        """
        url = self._session.api_path_to_url(path, endpoint or self.cc_end_point)
        response = self._session.delete(url=url, json=json_payload, _api_verbose=_api_verbose, **kwargs)

        if decode_json:
            return response.json()
        return response.text

    def api_request_put(self, path, json_payload: Any = None, decode_json=True, _api_verbose=True,
                        endpoint: str = None, **kwargs):
        """Sends a HTTP DELETE request to endpoint/path and return either the JSON or the Text of the HTTP response
        Args:
            path(str): HTTP Path
            json_payload(Optional[Any]): JSON payload to send along the HTTP request
            decode_json(bool): When true decodes response text into a JSON dictionary, else returns raw
                response text
            _api_verbose(bool): when True print out information about the request
            endpoint(str): HTTP host domain
            **kwargs: Parameters to requests.put

        Raises:
            experiment.errors.UnauthorisedRequest for http status codes 403 and 401

        Notes:
            1. This function may also inject authorisation headers for requests self.cc_endpoint
            2. On exceptions, other than experiment.errors.UnauthorisedRequest,
              retries up to self.max_request_retries
        """
        url = self._session.api_path_to_url(path, endpoint or self.cc_end_point)
        response = self._session.put(url=url, json=json_payload, _api_verbose=_api_verbose, **kwargs)

        if decode_json:
            return response.json()
        return response.text

    def api_relationship_push(self, relationship: Dict[str, Any], _api_verbose=False) -> Dict[str, Any]:
        """Creates a new relationship

        Args:
            relationship: The definition of the relationship
            _api_verbose(bool): when True print out information about the request

        Returns:
            The relationship that the ST4SD runtime service registers in its database

        Raises:
            experiment.errors.UnauthorisedRequest - when user is unauthorized to the ST4SD Runtime Service REST-API
            experiment.errors.InvalidHTTPRequest - when response HTTP status is other than 200
        """
        if _api_verbose:
            self.log.info(f"Creating relationship {json.dumps(relationship, indent=2)}")

        ret: Dict[str, Any] = self.api_request_post(
            'relationships/', json_payload=relationship, _api_verbose=_api_verbose, decode_json=True)

        return ret["entry"]

    def api_relationship_delete(self, relationship_identifier: str, _api_verbose=False):
        """Deletes a relationship from the runtime service database

        Args:
            relationship_identifier: Unique identifier of the relationship
            _api_verbose(bool): when True print out information about the request

        Returns(bool): True when instance deleted, False otherwise

        Raises:
            experiment.errors.UnauthorisedRequest - when user is unauthorized to the ST4SD Runtime Service REST-API
            experiment.errors.InvalidHTTPRequest - when response HTTP status is other than 200
        """
        # VV: Expect status code 200 for Success, and 400/404 for failure, treat anything else as an error
        if _api_verbose:
            self.log.info("Deleting relationship %s" % relationship_identifier)
        return self.api_request_delete(
            'relationships/%s' % relationship_identifier, _api_verbose=_api_verbose, decode_json=False)

    def api_relationship_synthesize(
            self,
            relationship_identifier: str,
            package_config: Dict[str, Any],
            new_package_name: str,
            _api_verbose: bool = False
    ) -> Dict[str, Any]:
        """Synthesizes a new parameterised virtual experiment package using the information encoded in a relationship

        Args:
            relationship_identifier: The identifier of the relationship
            package_config: The configuration options (e.g. parameterisation) of the new package
            new_package_name: The name of the new package
            _api_verbose(bool): when True print out information about the request

        Returns:
            The definition of the resulting parameterised virtual experiment package.

        Raises:
            experiment.errors.UnauthorisedRequest - when user is unauthorized to the ST4SD Runtime Service REST-API
            experiment.errors.InvalidHTTPRequest - when response HTTP status is other than 200
        """
        if _api_verbose:
            self.log.info(f"Synthesizing new paraneterised virtual experiment package from relationship "
                          f"{relationship_identifier} and config {json.dumps(package_config, indent=2)}")

        ret: Dict[str, Any] = self.api_request_post(
            f'relationships/{relationship_identifier}/synthesize/{new_package_name}',
            json_payload=package_config, _api_verbose=_api_verbose, decode_json=True)

        return ret["result"]

    def api_relationship_list(
        self,
        print_too: bool = False,
        treat_problems_as_errors: bool = False,
        _api_verbose: bool = False,
    ) -> Dict[str, Any]:
        """Returns a list of all relationship entries on the runtime service

        Method prints warnings (and optionally raises exceptions) if relationship definitions contain problems.

        Args:
            print_too: if True, method will also format and print the reply of the ST4SD Runtime Service REST-API server
            treat_problems_as_errors: If set to True, will raise a ProblematicEntriesError exception if runtime
                service reports that relationship definitions contain problems
            _api_verbose: when True print out information about the request

        Returns:
            A dictionary whose keys are `${relationshipIdentifier}` and values are the relationship definitions

        Raises:
            experiment.errors.UnauthorisedRequest: when user is unauthorized to the ST4SD Runtime Service REST-API
            experiment.errors.InvalidHTTPRequest: when response HTTP status is other than 200
            experiment.service.errors.ProblematicEntriesError: if treat_problems_as_error is True and ST4SD Runtime
                Service REST-API reports that relationship definitions contain problems
        """
        results: Dict[str, Any] = self.api_request_get('relationships/', _api_verbose=_api_verbose)

        relationships: List[Dict[str, Any]] = results['entries']
        relationships: Dict[str, Any] = {x['identifier']: x for x in relationships}

        if print_too:
            self.log.info(f"RestAPI returned the following relationships: {json.dumps(relationships, indent=2)}")

        problems: List[Dict[str, Any]] = results.get('problems')

        if problems:
            self.log.warning(f"RestAPI is reporting the following problems with the relationships {problems}")

            if treat_problems_as_errors:
                raise experiment.service.errors.ProblematicEntriesError(
                    problems, "Runtime service reports problematic relationship entries")

        return relationships

    def api_relationship_get(
        self,
        relationship_identifier: str,
        print_too: bool = False,
        treat_problems_as_errors: bool = False,
        _api_verbose: bool = False,
    ) -> Dict[str, Any]:
        """Returns a relationship entry on the runtime service

        Method prints warnings (and optionally raises exceptions) if the relationship definition contains problems.

        Args:
            relationship_identifier: The identifier of the relationship to retrieve
            print_too: if True, method will also format and print the reply of the ST4SD Runtime Service REST-API server
            treat_problems_as_errors: If set to True, will raise a ProblematicEntriesError exception if runtime
                service reports that the relationship definition contains problems
            _api_verbose: when True print out information about the request

        Returns:
            The relationship definition

        Raises:
            experiment.errors.UnauthorisedRequest: when user is unauthorized to the ST4SD Runtime Service REST-API
            experiment.errors.InvalidHTTPRequest: when response HTTP status is other than 200
            experiment.service.errors.ProblematicEntriesError: if treat_problems_as_error is True and ST4SD Runtime
                Service REST-API reports that the relationship definition contains problems
        """
        results: Dict[str, Any] = self.api_request_get(
            f'relationships/{relationship_identifier}', _api_verbose=_api_verbose)

        relationship: Dict[str, Any] = results['entry']

        if print_too:
            self.log.info(f"RestAPI returned the following relationship: {json.dumps(relationship, indent=2)}")

        problems: List[Dict[str, Any]] = results.get('problems')

        if problems:
            self.log.warning(f"RestAPI is reporting the following with the \"{relationship_identifier}\" "
                             f"relationship: {problems}")

            if treat_problems_as_errors:
                raise experiment.service.errors.ProblematicEntriesError(
                    problems, f"Runtime service reports problematic relationship entry {relationship_identifier}")

        return relationship

    def api_experiment_delete(self, experiment_id, _api_verbose=False):
        """Deletes an experiment definition

        Args:
            experiment_id(str): Unique identifier of the experiment definition (as shown on the consumable computing
              REST API server)
            _api_verbose(bool): when True print out information about the request

        Returns(bool): True when instance deleted, False otherwise

        Raises:
            experiment.errors.UnauthorisedRequest - when user is unauthorized to the ST4SD Runtime Service REST-API
            experiment.errors.InvalidHTTPRequest - when response HTTP status is other than 200
        """
        # VV: Expect status code 200 for Success, and 400/404 for failure, treat anything else as an error
        self.log.info("Deleting experiment definition %s" % experiment_id)
        return self.api_request_delete('experiments/%s' % experiment_id, _api_verbose=False, decode_json=False)

    def api_experiment_list(
        self,
        print_too=False,
        treat_problems_as_errors: bool = False,
        _api_verbose=True
    ) -> Dict[str, Dict[str, Any]]:
        """Returns a list of all virtual experiment entries on the runtime service

        Method prints warnings (and optionally raises exceptions) if the definition of the parameterised virtual
         experiment packages contain problems.

        Args:
            print_too: if True, method will also format and print the reply of the ST4SD Runtime Service REST-API server
            treat_problems_as_errors: If set to True, will raise a ProblematicEntriesError exception if runtime
                service reports that the definition of the parameterised virtual experiment packages contain problems
            _api_verbose: when True print out information about the request

        Returns:
            A dictionary whose keys are `${experimentName}@${experimentDigest}` and values are the experiment
            definitions
        Raises:
            experiment.errors.UnauthorisedRequest: when user is unauthorized to the ST4SD Runtime Service REST-API
            experiment.errors.InvalidHTTPRequest: when response HTTP status is other than 200
            experiment.service.errors.ProblematicEntriesError: if treat_problems_as_error is True and ST4SD Runtime
                Service REST-API reports that the definitions of the parameterised virtual experiment packages
                contain problems
        """

        results: Dict[str, Any] = self.api_request_get('experiments/', _api_verbose=_api_verbose)
        experiments: List[Dict[str, Any]] = results['entries']

        problems: List[Dict[str, Any]] = results.get('problems')

        if problems:
            self.log.warning(f"RestAPI is reporting the following problems with the parameterised "
                             f"virtual experiment packages {problems}")

            if treat_problems_as_errors:
                raise experiment.service.errors.ProblematicEntriesError(
                    problems, "Runtime service reports problematic parameterised virtual experiment package entries")

        with_uids = {
            f"{x['metadata']['package']['name']}@{x['metadata']['registry']['digest']}": x for x in experiments
        }

        if print_too:
            first = True
            for exp in experiments:
                if not first:
                    self.log.info("-----")

                first = False
                package_name: str = exp.get('metadata', {}).get('package', {}).get('name')
                if package_name:
                    registry_tag: str = exp.get('metadata', {}).get('registry',{}).get('tag')
                    if registry_tag:
                        registry_tag = ":".join((package_name, registry_tag))
                        self.log.info(f"Tag: {registry_tag}")

                    registry_digest: str = exp.get('metadata', {}).get('registry',{}).get('digest')
                    if registry_digest:
                        registry_digest = "@".join((package_name, registry_digest))
                        self.log.info(f"Digest: {registry_digest}")

                self.log.info(json.dumps(exp, sort_keys=True, indent=2, separators=(',', ': ')))

        return with_uids

    def api_experiment_get(
        self,
        experiment_identifier: str,
        print_too=False,
        treat_problems_as_errors: bool = False,
        _api_verbose=True
    ) -> Dict[str, Any]:
        """Returns one virtual experiment entries

        Method prints warnings (and optionally raises exceptions) if the definition of the parameterised virtual
        experiment package contains problems

        Args:
            experiment_identifier: The identifier of the virtual experiment. It can be one of:
                1. $virtualExperimentName
                     (e.g. "toxicity-prediction")
                2. $virtualExperimentName:$tag
                     (e.g. "toxicity-prediction:latest")
                3. $virtualExperimentName@$digest
                     (e.g. "toxicity-prediction@sha256x009f782fae8b1acc7bbe6bd3abba7c6e29c73831ba7cbcf86d37ffe5")
            print_too: if True, method will also format and print the reply of the ST4SD Runtime Service REST-API server
            treat_problems_as_errors: If set to True, will raise a ProblematicEntriesError exception if runtime
                service reports that the definition of the parameterised virtual experiment package contains problems
            _api_verbose: when True print out information about the request

        Returns:
            The virtual experiment definition

        Raises:
            experiment.errors.UnauthorisedRequest: when user is unauthorized to the ST4SD Runtime Service REST-API
            experiment.errors.InvalidHTTPRequest: when response HTTP status is other than 200
            experiment.service.errors.ProblematicEntriesError: if treat_problems_as_error is True and ST4SD Runtime
                Service REST-API reports that the definition of the parameterised virtual experiment package
                contains problems
        """

        results: Dict[str, Any] = self.api_request_get(
            f'experiments/{experiment_identifier}', _api_verbose=_api_verbose)
        ve = results['entry']

        if print_too:
            self.log.info(f"Definition of virtual experiment {experiment_identifier} is {json.dumps(ve, indent=2)}")

        problems: List[Dict[str, Any]] = results.get('problems')

        if problems:
            self.log.warning(f"RestAPI is reporting the following with the \"{experiment_identifier}\" "
                             f"parameterised virtual experiment package: {problems}")

            if treat_problems_as_errors:
                raise experiment.service.errors.ProblematicEntriesError(
                    problems, f"Runtime service reports problematic entry for the {experiment_identifier} "
                              "parameterised virtual experiment package")

        return ve

    def api_experiment_push(self, package: Dict[str, Any], _api_verbose=True):
        """Pushes a new definition of a parameterised virtual experiment package

        Method overrides ${metadata.package.name}:latest regardless of whether latest is part of
        ${metadata.package.tags}.

        Args:
            package: Definition of an experiment package (Check ST4SD Runtime Service REST-API swagger
              for the schema of a package)
            _api_verbose: when True print out information about the request

        Returns:
            The Dict[str, Any] response from the REST-API
        """
        try:
            ret: Dict[str, Any] = self.api_request_post('experiments/', json_payload=package, _api_verbose=_api_verbose)
        except experiment.service.errors.InvalidHTTPRequest as http_error:
            try:
                error_info = http_error.response.json()
                if isinstance(error_info, dict) is False:
                    raise NotImplementedError()
            except Exception:
                # VV: This HTTP-error is not one we can further characterize, raise it as is
                raise http_error from http_error

            if http_error.response.status_code == 404 and 'unknownExperiment' in error_info:
                # VV: here, error_info is expected to be a dictionary with the keys:
                #  unknownExperiment: this should match experiment_id
                #  message: This will be something along the lines of "Experiment @experiment_id not found
                raise experiment.service.errors.HTTPUnknownExperimentId(
                    url=http_error.url, http_method=http_error.http_method, response=http_error.response,
                    experiment_id=error_info['unknownExperiment'])
            elif http_error.response.status_code == 400 and 'invalidVirtualExperimentDefinition' in error_info:
                raise experiment.service.errors.HTTPInvalidVirtualExperimentDefinitionError(
                    url=http_error.url, http_method=http_error.http_method, response=http_error.response,
                    validation_errors=error_info['invalidVirtualExperimentDefinition'])
            elif http_error.response.status_code == 400 and 'experimentNamingRule' in error_info:
                # VV: here, error_info is expected to be a dictionary with the keys:
                #  invalidExperimentID: this should match experiment_id
                #  experimentNamingRule: a string explaining the naming rule for experiments
                #  message: This will be something along the lines of "Experiment id X is invalid. Naming rule is ..."
                raise experiment.service.errors.HTTPInvalidExperimentID(
                    url=http_error.url, http_method=http_error.http_method, response=http_error.response,
                    experiment_id=error_info['invalidExperimentID'],
                    naming_rule=error_info['experimentNamingRule'])
            elif http_error.response.status_code == 409 and 'experimentIDAlreadyExists' in error_info:
                # VV: here, error_info is expected to be a dictionary with the keys:
                #  experimentIDAlreadyExists: this should match experiment_id
                #  message: This will be something along the lines of "Experiment id X already exists"
                raise experiment.service.errors.HTTPExperimentIDAlreadyExists(
                    url=http_error.url, http_method=http_error.http_method, response=http_error.response,
                    experiment_id=error_info['experimentIDAlreadyExists'])
            elif http_error.response.status_code == 400 and 'inaccessibleContainerRegistries' in error_info:
                # VV: here, error_info is expected to be a dictionary with the keys:
                #  inaccessibleContainerRegistries: list of container registries for which none of the
                #    ST4SD Runtime Service REST-API imagePullSecrets can be used to pull
                #  message: This will be something along the lines of "There are no imagePullSecrets for
                #    container registries [%s]"
                raise experiment.service.errors.HTTPExperimentInaccessibleContainerRegistries(
                    url=http_error.url, http_method=http_error.http_method, response=http_error.response,
                    containerRegistries=list(error_info['inaccessibleContainerRegistries']))
            # VV: Cannot further specialize HTTP-ERROR, raise it as is
            raise http_error from http_error

        ve: Dict[str, Any] = ret['result']

        if _api_verbose:
            package_name = ve['metadata']['package']['name']
            registry_tags = ve['metadata']['registry']['tags']
            registry_digest = ve['metadata']['registry']['digest']

            registry_digest = '@'.join((package_name, registry_digest))

            all_registry_tag = [':'.join((package_name, registry_tag)) for registry_tag in registry_tags]

            self.log.info(f"{all_registry_tag} point to {registry_digest}")

        return ve

    def api_experiment_upsert(self, package, _api_verbose=True):
        """Creates an experiment if it does not exist, updates its definition if it already exists

        Method is deprecated, use api.api_experiment_push()

        Args:
            package: Definition of an experiment package (Check ST4SD Runtime Service REST-API swagger
              for the schema of a package)
            _api_verbose: when True print out information about the request

        Returns:
            The Dict[str, Any] response from the REST-API
        """
        self.log.warning("api_experiment_upsert() is deprecated, use api_experiment_push()")
        return self.api_experiment_push(package=package, _api_verbose=_api_verbose)

    def api_rest_uid_delete(self, rest_uid):
        """Deletes an instance

        Args:
            rest_uid(str): Unique identifier of the experiment instance (as shown on the consumable computing
              REST API server)

        Returns(bool): True when instance deleted, False otherwise

        Raises:
            experiment.errors.UnauthorisedRequest - when user is unauthorized to the ST4SD Runtime Service REST-API
            experiment.errors.InvalidHTTPRequest - when response HTTP status is other than 200
        """
        # VV: Expect status code 200 for Success, and 400/404 for failure, treat anything else as an error
        self.log.info("Deleting experiment instance %s" % rest_uid)
        return self.api_request_delete('instances/%s' % rest_uid, _api_verbose=False, decode_json=False)

    def api_experiment_start(self, experiment_id: str, payload: Dict[str, Any]) -> str:
        """Starts an instance of an experiment and returns its rest_uid

        Args:
            experiment_id: Unique identifier of the experiment package (as stored on the consumable computing
              REST API server)
            payload: Check ST4SD Runtime Service REST-API swagger for the schema of the payload

        Returns: rest-uid of the experiment instance
        """
        self.log.info(
            "Starting a new experiment instance %s with a payload of %s" % (experiment_id, pprint.pformat(payload)))
        try:
            rest_uid = self.api_request_post('experiments/%s/start' % experiment_id, json_payload=payload)
            self.log.info("New experiment instance with rest-uid %s" % rest_uid)
            return rest_uid
        except experiment.service.errors.InvalidHTTPRequest as http_error:
            try:
                error_info = http_error.response.json()
                if isinstance(error_info, dict) is False:
                    raise NotImplementedError()
            except Exception:
                # VV: This HTTP-error is not one we can further characterize, raise it as is
                raise http_error from http_error

            if http_error.response.status_code == 404 and 'unknownExperiment' in error_info:
                # VV: here, error_info is expected to be a dictionary with the keys:
                #  unknownExperiment: this should match experiment_id
                #  message: This will be something along the lines of "Experiment @experiment_id not found
                raise experiment.service.errors.HTTPUnknownExperimentId(
                    url=http_error.url, http_method=http_error.http_method, response=http_error.response,
                    experiment_id=experiment_id)
            elif http_error.response.status_code == 400 and 'invalidStartPayload' in error_info:
                raise experiment.service.errors.HTTPInvalidExperimentStartPayload(
                    url=http_error.url, http_method=http_error.http_method, response=http_error.response,
                    validation_errors=error_info['invalidStartPayload'])
            elif http_error.response.status_code == 400 and 'dataInputConflicts' in error_info:
                # VV: here, error_info is expected to be a dictionary with the keys:
                #  dataInputConflicts: list of data/input files which share the same name
                #  message: This will be something along the lines of "Data and Input files X share the same name"
                raise experiment.service.errors.HTTPDataInputFilesConflicts(
                    url=http_error.url, http_method=http_error.http_method, response=http_error.response,
                    dataInputConflicts=list(error_info['dataInputConflicts']))
            elif http_error.response.status_code == 400 and 'inaccessibleContainerRegistries' in error_info:
                # VV: here, error_info is expected to be a dictionary with the keys:
                #  inaccessibleContainerRegistries: list of container registries for which none of the
                #    ST4SD Runtime Service REST-API imagePullSecrets can be used to pull
                #  message: This will be something along the lines of "There are no imagePullSecrets for
                #    container registries [%s]"
                raise experiment.service.errors.HTTPExperimentInaccessibleContainerRegistries(
                    url=http_error.url, http_method=http_error.http_method, response=http_error.response,
                    containerRegistries=list(error_info['inaccessibleContainerRegistries']))

            # VV: Couldn't determine exactly what went wrong, propagate the original http exception to the caller
            raise http_error from http_error

    def api_experiment_start_lambda(
            self,
            payload: Dict[str, Any],
            validate: bool = True,
            print_too: bool = True,
            _api_verbose: bool = False) -> str:
        """Starts an instance of a Lambda experiment and returns its rest_uid

        Args:
            payload: Definition of lambda package (check ST4SD Runtime Service REST-API swagger for the
              schema of the payload)
            validate: Whether to validate flowir before sending the payload to the ST4SD Runtime Service REST-API
              endpoint (default: True)
            print_too: if set to True this method will also print the reply of the ST4SD Runtime Service REST-API server
            _api_verbose: when True prints out more information about the HTTP request - useful for debugging
        Returns: rest-uid of the Lambda experiment instance
        """
        if _api_verbose:
            self.log.info("Starting a new Lambda Experiment with payload %s" % pprint.pformat(payload))

        if validate:
            try:
                flowir = payload['lambdaFlowIR']  # type: experiment.model.frontends.flowir.DictFlowIR
                concrete = experiment.model.frontends.flowir.FlowIRConcrete(flowir, None, {})
                flowir_errors = concrete.validate()
            except Exception as e:
                flowir = None
                concrete = None
                flowir_errors = [e]

            if flowir_errors:
                msg = '\n'.join([str(x) for x in flowir_errors])

                msg = 'Found %d errors:\n%s' % (len(flowir_errors), msg)
                raise experiment.model.errors.FlowIRInconsistency(reason=msg, flowir=flowir, exception=None,
                                                                  extra=flowir_errors)
            elif _api_verbose:
                self.log.info("FlowIR is valid")

            del concrete
            del flowir
        try:
            rest_uid = self.api_request_post('experiments/lambda/start', json_payload=payload,
                                                _api_verbose=_api_verbose)
            if print_too:
                self.log.info("New Lambda Experiment instance with rest-uid %s" % rest_uid)
            return rest_uid
        except experiment.service.errors.InvalidHTTPRequest as http_error:
            try:
                error_info = http_error.response.json()
                if isinstance(error_info, dict) is False:
                    raise NotImplementedError()
            except Exception:
                # VV: This HTTP-error is not one we can further characterize, raise it as is
                raise http_error from http_error

            if http_error.response.status_code == 400 and 'inaccessibleContainerRegistries' in error_info:
                # VV: here, error_info is expected to be a dictionary with the keys:
                #  inaccessibleContainerRegistries: list of container registries for which none of the
                #    ST4SD Runtime Service REST-API imagePullSecrets can be used to pull
                #  message: This will be something along the lines of "There are no imagePullSecrets for
                #    container registries [%s]"
                raise experiment.service.errors.HTTPExperimentInaccessibleContainerRegistries(
                    url=http_error.url, http_method=http_error.http_method, response=http_error.response,
                    containerRegistries=list(error_info['inaccessibleContainerRegistries']))

            # VV: Couldn't determine exactly what went wrong, propagate the original http exception to the caller
            raise http_error from http_error

    def api_rest_uid_status(
            self,
            rest_uid: str,
            include_properties: List[str] | None = None,
            stringify_nan: bool = False,
            print_too=True,
            _api_verbose=True
    ) -> Dict[str, Any]:
        """Fetches the status of an instance and may optionally include its measured properties

        Args:
            rest_uid: rest-uid returned from Consumable Computing API (e.g. api_instance_create(),
             api_instance_create_lambda())
            print_too: if set to True this method will also print the reply of the ST4SD Runtime Service REST-API server
            include_properties: A list of column names or a list with just the string "*" which will automatically
                expand to all the column names in the properties DataFrame that matching experiments have measured.
                The column names in include_properties are case-insensitive, and
                the returned DataFrame will contain lowercase column names.
                When this argument is provided the ST4SD runtime-service API will insert the field
                `experiment.interface.propertyTable` to the returned dictionary. This field will contain
                a dictionary representation of the properties DataFrame that the matching experiment measured. The
                columns in that DataFrame will be those specified in `include_properties`. Column names that do not
                exist will in the DataFrame will be silently discarded. The column `input-id` will always be fetched
                even if not specified in `include_properties`.
            stringify_nan: A boolean flag that allows converting NaN and infinite values to strings
            _api_verbose: if set to True this method will print info messages when retrying

        Returns:
            Dictionary containing the status of the instance (check the swagger model of ST4SD Runtime Service REST-API
            for the definition of the status dictionary)
        Raises:
            experiment.service.errors.InvalidHTTPRequest: if HTTP status is not 200
        """

        params = {'stringifyNaN': stringify_nan}
        if include_properties:
            params['includeProperties'] = ','.join(include_properties)

        status = self.api_request_get('instances/%s' % rest_uid, _api_verbose=_api_verbose, params=params)

        if print_too:
            self.log.info("Instance %s status: %s" % (rest_uid, json.dumps(status, indent=4, sort_keys=True)))

        return status

    def api_rest_uid_output(self, rest_uid, output_name, _api_verbose=False) -> Tuple[str, bytes]:
        """Gets a key-output that a workflow instance generated

        if output_name points to a folder the contents of the folder will be returned in the form of a ZIP file

        Args:
            rest_uid(str): identifier of workflow instance (workflow object name/rest-uid returned by instance_create)
            output_name(str): identifier of key-output
            _api_verbose(bool): whether to print debugging statements regarding HTTP requests

        Returns
            The tuple (filename:str, contents:bytes)
        """
        url = 'instances/%s/outputs/%s' % (rest_uid, output_name)
        response = self.api_request_get(url, return_response=True, _api_verbose=_api_verbose)

        # VV: we got some output in the form of a binary file, could also be a ZIP containing a folder
        try:
            content_disp = response.headers['Content-Disposition']
        except KeyError:
            raise experiment.service.errors.InvalidHTTPRequest(
                os.path.join(self.cc_end_point, url), 'GET',  response,
                info="Response does not contain Content-Disposition header; "
                "is Consumable Computing up-to-date ?")
        try:
            # VV: Content-Disposition has the format: `attachment; filename="<SOME FILENAME>"`
            filename = re.findall(r"filename=\"(.+)\"", content_disp)[0]
        except Exception:
            raise experiment.service.errors.InvalidHTTPRequest(
                os.path.join(self.cc_end_point, url), 'GET', response,
                info="Content-Disposition header does not contain a filename is Consumable Computing up-to-date ?")

        return filename, response.content

    def api_datasets_list(self, print_too=True, _api_verbose=False):
        # type: (bool, bool) -> List[str]
        """Lists available datasets as reported by the ST4SD Runtime Service REST-API

        Arguments:
            print_too(bool): setting this to True will also print the list of available datasets
            _api_verbose(bool): print verbose statements regarding HTTP request and Exceptions

        Returns:
            A list of strings, each of which is the id of a dataset
        """
        try:
            output = self.api_request_get('datasets/', _api_verbose=_api_verbose)  # type: List[str]
            if print_too:
                self.log.info("Available datasets on %s: %s" % (self.cc_end_point, output))
            return output
        except Exception as e:
            if _api_verbose:
                self.log.info(traceback.format_exc())
            raise e from e

    def api_datasets_create(self, dataset_name, endpoint, bucket, access_key_id, secret_access_key,
                            region=None, print_too=True, _api_verbose=False):
        """Creates a dataset that points to a S3 bucket

        Arguments:
            dataset_name(str): Name of dataset
            endpoint(str): S3 endpoint
            bucket(str): name of S3 bucket
            access_key_id(str): access key ID for S3 bucket
            secret_access_key(str): secret access key for S3 bucket
            region(Optional[str]): (optional) region for S3 bucket
            print_too(bool): setting this to True will also echo a success message on object creation
            _api_verbose(bool): print verbose statements regarding HTTP request and Exceptions
        """
        try:
            payload = {
                'accessKeyID': str(access_key_id),
                'secretAccessKey': str(secret_access_key),
                'bucket': str(bucket),
                'endpoint': str(endpoint),
                'region': str(region or ''),
            }
            self.api_request_post('datasets/s3/%s' % dataset_name, json_payload=payload, _api_verbose=_api_verbose)
        except experiment.service.errors.InvalidHTTPRequest as e:
            if e.response.status_code == 409:
                raise_with_traceback(ValueError("Dataset %s already exists on %s" % (dataset_name, self.cc_end_point)))
            raise_with_traceback(e)
        except Exception as e:
            if _api_verbose:
                self.log.info(traceback.format_exc())
            raise e from e
        if print_too:
            self.log.info("Successfully created Dataset %s" % dataset_name)

    def api_image_pull_secrets_list(self, print_too=True, _api_verbose=False):
        # type: (bool, bool) -> Dict[str, List[str]]
        """Lists available imagePulLSecrets as reported by the ST4SD Runtime Service REST-API. These imagePullSecrets
        will be automatically used by the Kubernetes Jobs that your workflow instances will create.

        Arguments:
            print_too(bool): setting this to True will also print the available imagePullSecrets
            _api_verbose(bool): print verbose statements regarding HTTP request and Exceptions

        Returns:
            A Dictionary, keys are imagePullSecrets, values are lists of docker-registries for which the
            imagePullSecret has pull access to.
        """
        try:
            output = self.api_request_get('image-pull-secrets/', _api_verbose=_api_verbose)
            if print_too:
                self.log.info("Available imagePullSecrets on %s: %s" % (self.cc_end_point, output))
            return output  # type: Dict[str, List[str]]
        except Exception as e:
            if _api_verbose:
                self.log.info(traceback.format_exc())
            raise_with_traceback(ValueError("Cannot get imagePullSecrets on %s - exception %s" % (
                self.cc_end_point, e)))

    def api_image_pull_secrets_create(self, secret_name, registry, username, password,
                                      print_too=True, _api_verbose=False):
        """Creates an imagePullSecret which has pull access to a docker registry.

        Arguments:
            secret_name(str): Name of secret
            registry(str): docker registry endpoint (url)
            username(str): username to authenticate
            password(str): password/api-key to authenticate with
            print_too(bool): setting this to True will also echo a success message on object creation
            _api_verbose(bool): print verbose statements regarding HTTP request and Exceptions

        Notes:

            This method receives a password/api-token in plaintext. Do NOT push your password/api-token to github
            or any other file hosting service. Remember to sanitize the calls to this method and never leave
            your password in plain view.
        """
        return self._api_image_pull_secrets_upsert(secret_name=secret_name, registry=registry, username=username,
                                                   password=password, update=False, print_too=print_too,
                                                   _api_verbose=_api_verbose)

    def api_image_pull_secrets_upsert(self, secret_name, registry, username, password,
                                      print_too=True, _api_verbose=False):
        """Creates an imagePullSecret which has pull access to a docker registry, updates existing secret if it alrady
        exists.

        Arguments:
            secret_name(str): Name of secret
            registry(str): docker registry endpoint (url)
            username(str): username to authenticate
            password(str): password/api-key to authenticate with
            print_too(bool): setting this to True will also echo a success message on object creation
            _api_verbose(bool): print verbose statements regarding HTTP request and Exceptions

        Notes:

            This method receives a password/api-token in plaintext. Do NOT push your password/api-token to github
            or any other file hosting service. Remember to sanitize the calls to this method and never leave
            your password in plain view.
        """
        return self._api_image_pull_secrets_upsert(secret_name=secret_name, registry=registry, username=username,
                                                   password=password, update=True, print_too=print_too,
                                                   _api_verbose=_api_verbose)

    def _api_image_pull_secrets_upsert(self, secret_name, registry, username, password, update,
                                      print_too, _api_verbose):
        """Creates or updates an imagePullSecret in order to provide pull access to a docker registry.

        Arguments:
            secret_name(str): Name of secret
            registry(str): docker registry endpoint (url)
            username(str): username to authenticate
            password(str): password/api-key to authenticate with
            update(bool): Setting this to True may overwrite existing imagePullSecrets
            print_too(bool): setting this to True will also echo a success message on object creation
            _api_verbose(bool): print verbose statements regarding HTTP request and Exceptions

        Notes:

            This method receives a password/api-token in plaintext. Do NOT push your password/api-token to github
            or any other file hosting service. Remember to sanitize the calls to this method and never leave
            your password in plain view.
        """
        try:
            payload = {
                'server': str(registry),
                'username': str(username),
                'password': str(password),
            }
            if update is False:
                if _api_verbose:
                    self.log.info("Creating new imagePullSecret %s on %s" % (secret_name, self.cc_end_point))
                self.api_request_post('image-pull-secrets/%s' % secret_name, json_payload=payload,
                                      _api_verbose=_api_verbose)
            else:
                if _api_verbose:
                    self.log.info("Updating existing imagePullSecret %s on %s" % (secret_name, self.cc_end_point))
                self.api_request_put('image-pull-secrets/%s' % secret_name, json_payload=payload,
                                      _api_verbose=_api_verbose)
        except experiment.service.errors.InvalidHTTPRequest as e:
            if e.response.status_code == 409:
                raise experiment.service.errors.InvalidHTTPRequest(
                    e.url, e.http_method, e.response, "ImagePullSecret already exists") from e
            elif e.response.status_code == 422:
                raise experiment.service.errors.InvalidHTTPRequest(
                    e.url, e.http_method, e.response,
                    "Name of ImagePullSecret conflicts with other Secret in same Namespace") from e
            raise e from e
        except Exception as e:
            if _api_verbose:
                self.log.info(traceback.format_exc())
            raise e from e

        if print_too:
            if update is False:
                self.log.info("Successfully created ImagePullSecret %s" % secret_name)
            else:
                self.log.info("Successfully updated ImagePullSecret %s" % secret_name)

    def cdb_get_data(self,
                     instance: Optional[str] = None,
                     stage: Optional[int] = None,
                     component: Optional[str] = None,
                     filename: Optional[str] = None,
                     query: Optional[DictMongoQuery] = None,
                     _api_verbose: bool = True,
    ) -> Tuple[List[DictMongo], List[experiment.utilities.data.Matrix]]:
        """Queries the centralized-db (CDB) for CSV files and returns a tuple of 2 lists for matching CSV files.

        Args:
            instance: The location of the experiment you want to retrieve data from (interpreted as regex string)
                it can be a file URI (file://$HOSTNAME/$ABSOLUTE_PATH).
                Overrides query['instance'].
            stage: The index of a stage you want to retrieve data from (integer)
                Overrides query['stage'].
            component: The name/reference of a component you want to retrieve data from (interpreted as regex string).
               If component is in reference format (i.e stage<%d>.%s) `%d` will be used by the `stage` argument
               if `stage` is not None then  this method
               raises experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage exception.
               Overrides query['name'] and query['stage'] (and @stage parameter).
            filename: The name of the file you want to retrieve (interpreted as regex string).
                Overrides query['file'].
            query: A mongo query to use for further filtering results
            _api_verbose: Set to true to print INFO level messages

        Raises:
            experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage:  if component in reference format
                and `stage` is not None

        Returns:
                A tuple of lists for matching CSV files. The first List contains Documents of Components which
                 produced the matching CSV files. The second List contains experiment.utilities.data.Matrix instances that
                 have been created using the contents of the CSV files.
                 The i-th entry in the 1st list corresponds to the i-th entry in the 2nd list.
        """
        self.log.info("Querying CDB for objects containing CSV data, query: %s" % pprint.pformat(query))

        retries = 0

        query = query or {}
        while True:
            try:
                ret = self.cl.getData(filename=filename, component=component, stage=stage, instance=instance,
                                      query=query)
                if _api_verbose is True:
                    if ret:
                        self.log.info("Found %d matching CSV objects" % len(ret))
                    else:
                        self.log.log(15, "Did not find any matching CSV objects")
                        return [], []
                stuff = list(zip(*ret))  # type: Tuple[List[Dict[str, Any]], List["experiment.utilities.data.Matrix"]]
                docs, files = stuff
                return docs, files
            except (requests.exceptions.RequestException, pymongo.errors.PyMongoError) as e:
                retries += 1
                if retries > self.max_request_retries:
                    raise_with_traceback(ValueError("Could not getData because %s" % e))
                else:
                    if _api_verbose:
                        self.log.warning("Error when getData - %s will retry" % e)

                    time.sleep(self.secs_between_retries)

    @classmethod
    def cdb_map_outputs_to_properties(cls, interface: DictMongo) -> Dict[str, List[str]]:
        """Maps the key outputs to the the names of the properties

        See experiment.module.frontends.flowir.FlowIR.type_flowir_interface_structure() for spec.

        Currently, the interface looks like:

        description: description of the virtual experiment  (string)
        inputSpec:
            namingScheme: the name of the input (string)
            source:
                file: relative path to source of instance directory
                extractionMethod: # only 1 field can be set
                    hook: true
            hasAdditionalData: bool
        propertiesSpec:
            - name: the name of the property (string)
              source:
                output: name of key-output in virtual experiment (string)
                extractionMethod: # only 1 field can be set
                    hook: true
        inputs:
        - string (the ID of one input)
        additionalInputData:
            <an input id (string)>:
            - list of additional input paths associated with the input id

        Args:
            interface: The interface description

        Returns:
            A dictionary whose keys are names of key-outputs in the experiment and values are a
                list of strings. Each string is a name of a property contained in the key output
        """

        ret: Dict[str, List[str]] = {}

        # VV: See experiment.module.frontends.flowir.FlowIR.type_flowir_interface_structure() for spec
        properties_spec: List[Dict[str, Any]] = interface.get('propertiesSpec', [])

        for prop in properties_spec:
            name: str = prop['name']
            source: Dict[str, Any] = prop['source']
            output_name: str = source['output']

            outputs = ret.get(output_name, [])
            outputs.append(name)
            ret[output_name] = outputs

        return ret

    @classmethod
    def cdb_get_property_names(cls, experiment_doc: DictMongo) -> List[str]:
        """Extracts the property names from the interface of an experiment document

        Args:
            experiment_doc: One experiment document

        Returns:
            A list of property names

        Raises:
            experiment.service.errors.NoPropertyError: The experiment does not measure properties
            experiment.service.errors.BadCallError: @experiment_doc is not an experiment MongoDocument
        """
        try:
            doc_type = experiment_doc['type']
        except Exception:
            doc_type = None

        if doc_type != "experiment":
            raise experiment.service.errors.BadCallError(
                f"Document is not an experiment (sniffed document type is {doc_type})")

        try:
            interface = experiment_doc.get('interface', {})
            properties_spec = interface.get('propertiesSpec', [])
            ret: List[str] = [spec['name'] for spec in properties_spec]
            if not ret:
                raise experiment.service.errors.NoPropertyError(experiment_doc['instance'], '*', None)
            return ret
        except experiment.service.errors.NoPropertyError as e:
            raise_with_traceback(e)
        except Exception as e:
            raise_with_traceback(experiment.service.errors.BadCallError(
                f"Document is not an experiment - error :{e} while inspecting interface"))

    @classmethod
    def cdb_get_key_output_names(cls, experiment_doc: DictMongo) -> List[str]:
        """Extracts the names of generated outputs from an experiment document

        Args:
            experiment_doc: One experiment document

        Returns:
            A list of output names

        Raises:
            experiment.service.errors.NoOutputError: The experiment has not generated any outputs
            experiment.service.errors.BadCallError: @experiment_doc is not an experiment MongoDocument
        """
        try:
            doc_type = experiment_doc['type']
        except Exception:
            doc_type = None

        if doc_type != "experiment":
            raise experiment.service.errors.BadCallError(
                f"Document is not an experiment (sniffed document type is {doc_type})")

        try:
            output: Dict[str, Dict[str, Any]] = experiment_doc.get('output', {})
            ret = list(output)
            if not ret:
                raise experiment.service.errors.NoOutputError(experiment_doc['instance'], '*', None)
            return ret
        except experiment.service.errors.NoOutputError as e:
            raise_with_traceback(e)
        except Exception as e:
            raise_with_traceback(experiment.service.errors.BadCallError(
                f"Document is not an experiment - error :{e} while inspecting interface"))

    def cdb_get_properties(
            self,
            instance: Optional[str] = None,
            query: Optional[DictMongoQuery] = None,
            properties: Optional[List[str]] = None,
            outputs: Optional[List[str]] = None,
            _api_verbose: bool = True,
    ) -> Tuple[List[DictMongo], List[pandas.DataFrame]]:
        """Queries the Datastore for CSV files and returns a tuple of 2 lists for matching CSV files containing
        measured properties.

        Args:
            instance: The location of the experiment you want to retrieve data from (interpreted as regex string)
                it can be a file URI (file://$HOSTNAME/$ABSOLUTE_PATH).
                Overrides query['instance'].
            query: A mongo query to use for further filtering results
            properties: A list of columns in the properties Dataframe that matching instances must have produced.
                Note that the column names may not be properties but auxiliary columns that interface.propertiesSpec
                insert into the dataframe.
                Optional, mutually exclusive with @outputs.
            outputs: A list of key output names. The returned Dataframes will only have columns that correspond to
                properties measured using the provided output names. Optional, mutually exclusive with @properties.
            _api_verbose: Set to true to print INFO level messages

        Returns:
                A tuple of lists for matching Dataframes. The first List contains Documents of Experiments which
                    produced the matching Dataframe files. The second List contains pandas.Dataframe instances that
                    have been created for hosting the extracted properties. They contain 1+N columns.
                    The key is `input-id`, the other N columns are names of properties
                    (see expDoc.interface.propertiesSpec[#].name).
                    The i-th entry in the 1st list corresponds to the i-th entry in the 2nd list.

        Raises:
            experiment.model.service.errors.NoInstanceError: No matching instance
            experiment.model.service.errors.NoPropertyError: At least 1 matching instance has not produced at least 1
                of the requested columns in the properties Dataframe.
            experiment.model.service.errors.NoOutputError: At least 1 matching instance has not produced at least 1
                of the requested key outputs
            experiment.model.service.errors.BadCallError: User provided both @outputs and @properties arguments
        """
        if bool(outputs) and bool(properties):
            raise experiment.service.errors.BadCallError(
                "\"properties\" and \"outputs\" are mutually exclusive arguments of cdb_get_properties()")

        query = (query or {}).copy()
        query['type'] = 'experiment'
        query = self.cl.preprocess_query(instance=instance, query=query)

        docs = self.cdb_get_document_experiment(query=query, _api_verbose=_api_verbose)

        if not docs:
            raise experiment.service.errors.NoInstanceError(query)

        ret_docs: List[DictMongo] = []
        ret_dataframes: List[pandas.DataFrame] = []

        # VV: Schema of dictionary is: fileURI -> experiment mongoDescription
        file_to_doc: Dict[str, DictMongo] = {}

        for doc in docs:
            properties_spec = doc.get('interface', {}).get('propertiesSpec', [])
            if not properties_spec:
                continue
            file_to_doc['/'.join((doc['instance'], 'output/properties.csv'))] = doc

        if _api_verbose:
            self.log.info(f"Found {len(file_to_doc)} properties.csv files")

        def callback_to_dataframe(filename: str, contents: bytes, mongo_doc: DictMongo) -> pandas.DataFrame:
            filelike = BytesIO(contents)
            df: pandas.DataFrame = pandas.read_csv(filelike, sep=None, engine="python")
            return df

        # VV: Deeply nested dictionary: gatewayID -> instanceUri -> fileURI -> documentDescription
        file_map: Dict[str, Dict[str, Dict[str, DictMongo]]] = self.cl._generate_file_map(file_to_doc)

        downloaded: List[Tuple[DictMongo, str, pandas.DataFrame]] = self.cl._fetch_and_postprocess(
            file_map, myself=None, post_process=callback_to_dataframe)

        all_uris = [doc['instance'] for doc in docs]

        for doc, file_uri, _ in downloaded:
            all_uris.remove(doc['instance'])

        if all_uris:
            first = all_uris[0]
            raise experiment.service.errors.NoPropertyError(first, '*', None)

        if properties:
            properties = list(properties)
            properties.insert(0, 'input-id')

        for doc, file_uri, dataframe in downloaded:
            # VV: Filter dataframe based on either properties of @outputs or @properties (or do not filter at all)
            ret_docs.append(doc)
            my_properties = []

            if outputs:
                output_to_properties = self.cdb_map_outputs_to_properties(doc["interface"])
                for o in outputs:
                    try:
                        my_properties.extend(output_to_properties[o])
                    except KeyError:
                        raise experiment.service.errors.NoOutputError(doc["instance"], o, list(output_to_properties))

                my_properties = ['input-id'] + list(set(my_properties))
            elif properties:
                my_properties = properties

            if my_properties:
                for x in my_properties:
                    if x not in dataframe.columns:
                        raise experiment.service.errors.NoPropertyError(doc['instance'], x, list(dataframe.columns))
                dataframe = dataframe.filter(items=my_properties)

            ret_dataframes.append(dataframe)

        return ret_docs, ret_dataframes

    def cdb_get_document_experiment(
            self,
            instance: str = None,
            query: Optional[DictMongoQuery] = None,
            include_properties: List[str] | None = None,
            stringify_nan: bool = False,
            _api_verbose=True) -> List[ComponentDocument]:
        """Queries the centralized-db (CDB) for experiment MongoDB documents

        Note that the arguments to this function do not reflect the schema of the user-metadata documents.

        Args:
            instance: The location of the experiment you want to retrieve data from (interpreted as regex string)
                it can be a file URI (file://$HOSTNAME/$ABSOLUTE_PATH).
                Overrides query['instance'].
            query: A mongo query to use for further filtering results
            include_properties: A list of column names or a list with just the string "*" which will automatically
                expand to all the column names in the properties DataFrame that matching experiments have measured.
                The column names in include_properties are case-insensitive, and
                the returned DataFrame will contain lowercase column names.
                When this argument is provided the ST4SD Datastore API (mongo-proxy) will insert the field
                `interface.propertyTable` to matching `experiment` type mongoDocuments. This field will contain
                a dictionary representation of the properties DataFrame that the matching experiment measured. The
                columns in that DataFrame will be those specified in `include_properties`. Column names that do not
                exist will in the DataFrame will be silently discarded. The column `input-id` will always be fetched
                even if not specified in `include_properties`.
            stringify_nan: A boolean flag that allows converting NaN and infinite values to strings
            _api_verbose: Set to true to print INFO level messages

        Raises:
            requests.exceptions.RequestException: if after @self.max_request_retries there's a
                requests.exceptions.RequestException exception while Querying mongo
            pymongo.errors.PyMongoError: if after @self.max_request_retries there's a
                pymongo.errors.PyMongoError exception while Querying mongo

        Returns:
            Returns a list of containing MongoDB documents (dictionaries)
        """
        # VV: Don't use a regex for the 'type' field, put it in `query` which is not processed
        query = (query or {}).copy()
        query['type'] = 'experiment'
        query = self.cl.preprocess_query(instance=instance, query=query)
        return self._kernel_cdb_get_document(
            query=query,
            include_properties=include_properties,
            stringify_nan=stringify_nan,
            _api_verbose=_api_verbose)

    def cdb_get_document_user_metadata(
            self,
            instance: str = None,
            query: Optional[DictMongoQuery] = None,
            _api_verbose=True) -> List[ComponentDocument]:
        """Queries the centralized-db (CDB) for user-metadata MongoDB documents

        Args:
            instance: The location of the experiment you want to retrieve data from (interpreted as regex string)
                it can be a file URI (file://$HOSTNAME/$ABSOLUTE_PATH).
                Overrides query['instance'].
            query: A mongo query to use for further filtering results, you can use this to filter results
                based on user provided tags, e.g. {'rest-uid': <some rest-uid>}
            _api_verbose: Set to true to print INFO level messages

        Raises:
            requests.exceptions.RequestException: if after @self.max_request_retries there's a
                requests.exceptions.RequestException exception while Querying mongo
            pymongo.errors.PyMongoError: if after @self.max_request_retries there's a
                pymongo.errors.PyMongoError exception while Querying mongo

        Returns:
            Returns a list of containing MongoDB documents (dictionaries)
        """
        # VV: Don't use a regex for the 'type' field, put it in `query` which is not processed
        query = (query or {}).copy()
        query['type'] = 'user-metadata'
        query = self.cl.preprocess_query(instance=instance, query=query)
        return self.cdb_get_document(query=query, _api_verbose=_api_verbose)

    def cdb_get_document_component(
            self,
            filename: Optional[str] = None,
            component: Optional[str] = None,
            stage: Optional[int] = None,
            instance: Optional[str] = None,
            query: Optional[DictMongoQuery] = None,
            _api_verbose: bool =True
            ) -> List[ComponentDocument]:
        """Queries the centralized-db (CDB) for MongoDB documents.

        Parameters other than @query may end up overriding those you define in @query.

        Args:
            instance: The location of the experiment you want to retrieve data from (interpreted as regex string)
                it can be a file URI (file://$HOSTNAME/$ABSOLUTE_PATH).
                Overrides query['instance'].
            stage: The index of a stage you want to retrieve data from (integer)
                Overrides query['stage'].
            component: The name/reference of a component you want to retrieve data from (interpreted as regex string).
               If component is in reference format (i.e stage<%d>.%s) `%d` will be used by the `stage` argument
               if `stage` is not None then  this method
               raises experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage exception.
               Overrides query['name'] and query['stage'] (and @stage parameter).
            filename: The name of the file you want to retrieve (interpreted as regex string).
                Overrides query['file'].
            query: A mongo query to use for further filtering results.
            _api_verbose: when True print out information about the request

        Raises:
            experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage: if component is reference and
                stage is not None

        Returns:
            Returns a list of dictionaries created out of MongoDB documents
        """
        # VV: The component query is the most complex one it's the one with the most fields for which we use
        # custom logic to translate to Mongo queries then query mongo with the resulting query dictionary

        # VV: Don't use a regex for the 'type' field, put it in `query` which is not processed
        # cdb_get_document converts its type param to a regex
        query = (query or {}).copy()
        query['type'] = 'component'
        query = self.cl.preprocess_query(filename=filename, component=component, stage=stage, instance=instance,
                                         query=query)
        return self.cdb_get_document(query=query, _api_verbose=_api_verbose)

    def _kernel_cdb_get_document(self,
            query: Optional[DictMongoQuery] = None,
            include_properties: List[str] | None = None,
            stringify_nan: bool = False,
            _api_verbose: bool = True) -> List[ComponentDocument]:
        """Queries the centralized-db (CDB) for MongoDB documents.

        All MongoDB documents have the `instance` and `type` top-level keys but they may also have more.

        Arguments:
            query: A mongo query to use for further filtering results
            include_properties: A list of column names or a list with just the string "*" which will automatically
                expand to all the column names in the properties DataFrame that matching experiments have measured.
                The column names in include_properties are case-insensitive, and
                the returned DataFrame will contain lowercase column names.
                When this argument is provided the ST4SD Datastore API (mongo-proxy) will insert the field
                `interface.propertyTable` to matching `experiment` type mongoDocuments. This field will contain
                a dictionary representation of the properties DataFrame that the matching experiment measured. The
                columns in that DataFrame will be those specified in `include_properties`. Column names that do not
                exist will in the DataFrame will be silently discarded. The column `input-id` will always be fetched
                even if not specified in `include_properties`.
            stringify_nan: A boolean flag that allows converting NaN and infinite values to strings
            _api_verbose: Set to true to print INFO level messages

        Raises:
            requests.exceptions.RequestException: if after @self.max_request_retries there's a
                requests.exceptions.RequestException exception while Querying mongo
            pymongo.errors.PyMongoError: if after @self.max_request_retries there's a
                pymongo.errors.PyMongoError exception while Querying mongo

        Returns:
            Returns a list of containing MongoDB documents (dictionaries)
        """
        if _api_verbose:
            self.log.info("Querying CDB for MongoDB Documents, query: %s" % pprint.pformat(query))

        retries = 0

        while True:
            try:
                ret = self.cl.getDocument(query=query, include_properties=include_properties,
                                          stringify_nan=stringify_nan, _api_verbose=_api_verbose)
                if _api_verbose:
                    if ret:
                        self.log.info("Found %d matching MongoDB Documents" % len(ret))
                    else:
                        self.log.info("Did not find any matching MongoDB Documents")
                return ret
            except (requests.exceptions.RequestException, pymongo.errors.PyMongoError) as e:
                retries += 1
                if retries > self.max_request_retries:
                    raise_with_traceback(e)
                else:
                    if _api_verbose:
                        self.log.warning("Error when getDocument %s - %s will retry" % (query ,e))

                    time.sleep(self.secs_between_retries)

    def cdb_get_document(
            self,
            query: Optional[DictMongoQuery] = None,
            _api_verbose: bool = True) -> List[ComponentDocument]:
        """Queries the centralized-db (CDB) for MongoDB documents.

        All MongoDB documents have the `instance` and `type` top-level keys but they may also have more.

        Arguments:
            query: A mongo query to use for further filtering results
            _api_verbose: Set to true to print INFO level messages

        Raises:
            requests.exceptions.RequestException: if after @self.max_request_retries there's a
                requests.exceptions.RequestException exception while Querying mongo
            pymongo.errors.PyMongoError: if after @self.max_request_retries there's a
                pymongo.errors.PyMongoError exception while Querying mongo

        Returns:
            Returns a list of containing MongoDB documents (dictionaries)
        """
        return self._kernel_cdb_get_document(query=query, _api_verbose=_api_verbose)

    def cdb_get_file_from_instance_uri(self, instance, rel_path, _api_verbose=True):
        """Download a single file under the directory of a workflow instance.

        Args:
            instance(str): File URI in the form of file://$GATEWAY_ID/absolute/path/to/instance/dir
            rel_path(str): relative path to a file under the root directory of the instance directory
            _api_verbose(bool): when True prints out more information about the HTTP request - useful for debugging

        Returns:
            the contents of the file (bytes)
        """
        if instance.startswith('file://') is False:
            raise ValueError("instance argument \"%s\" is not a valid file://${GATEWAY}/path/to/dir URI" % instance)

        retries = 0

        while True:
            try:
                return self.cl.fetch_remote_file(instance, rel_path)
            except (requests.exceptions.RequestException, pymongo.errors.PyMongoError) as e:
                retries += 1
                if retries > self.max_request_retries:
                    raise_with_traceback(ValueError("Could not fetch_remote_file because %s" % e))
                else:
                    if _api_verbose:
                        self.log.warning("Error when fetch_remote_file %s - %s will retry" % e)

                    time.sleep(self.secs_between_retries)

    def cdb_get_detailed_status_report(self, instance, pretty_print_instead=True,
                                       stages=None, categories=None, no_defaults=False, filters=None, print_too=True,
                                       _api_verbose=False):
        """Retrieves component status details report and pretty prints it to a string.

        The pretty-printer string is identical to the output of `einspect.py -f all -c producer -c task -c engine`.
        Bear in mind that this report is asynchronously generated, so unless the workflow is finished the
        information included in the report may not 100% reflect the current state of the workflow instance.

        Arguments:
            instance(str): The workflow instance URI for which you wish to retrieve its status details (
                a file URI has the format file://$HOSTNAME/$ABSOLUTE_PATH, it's the 'instance' field of all
                mongoDB documentDescriptions that the CDB returns via cdb_get_documents())
            pretty_print_instead(bool): If set to True this method will return a pretty-printed string representation
                of the report. Setting this to False will instead return a dictionary which was generated via
                experiment.service.db.Status.getWorkflowStatus().
                Default is True
            stages(List[int]): An optional list of stage indices, when provided status report will
                only contain entries for components in those stages.
                Only used if pretty_print_instead is set to True.
            categories(List[str]): Optional list of categories for which information is output.
                Available categories are producer, task, and engine
            no_defaults(bool): Omits output columns in the  categories - useful if there are too many columns
                being output.
                Only used if pretty_print_instead is set to True.
            filters(List[str]): Set filters that limit the components output to those exhibiting certain behaviour.
                With each use of this option an additional filter can be specified. Available filters are:
                failed, interesting, engine-exited, blocked-output, blocked-resource, not-executed, executing, and all.
                Default filters applied are: all
                Only used if pretty_print_instead is set to True.
            print_too(bool): If set to True, will also self.log.info() the result. Default is True
            _api_verbose(bool): when True prints out more information about the cdb request - useful for debugging
        """

        # VV: First fetch the output/status_details.json file
        try:
            json_file = self.cdb_get_file_from_instance_uri(instance, 'output/status_details.json')
        except Exception as e:
            if _api_verbose:
                self.log.info("Traceback: %s\nUnable to retrieve output/status_details.json file "
                              "from %s" % (traceback.format_exc()), instance)
            raise_with_traceback(ValueError("Unable to retrieve output/status_details.json file "
                                            "from %s - because of %s" % (instance, e)))
        try:
            # VV: Convert contents of file to dictionary
            workflowStatus = json.loads(json_file)  # type: Dict[str, Any]
            # VV: Then convert CSV representation of matrices back to experiment.utilities.data.Matrix objects, this is essentially
            # recreating the output of experiment.service.db.Status.getWorkflowStatus()
            for stageIndex in workflowStatus:
                for engine in workflowStatus[stageIndex]:
                    for k in workflowStatus[stageIndex][engine]:
                        csv = workflowStatus[stageIndex][engine][k].rstrip()
                        try:
                            matrix = experiment.utilities.data.Matrix.matrixFromCSVRepresentation(csv)
                        except Exception as e:
                            self.log.info("Unable to create a experiment.utilities.data.Matrix object out of %s" % csv)
                            raise e
                        workflowStatus[stageIndex][engine][k] = matrix
        except Exception as e:
            if _api_verbose:
                self.log.info("Traceback: %s\nUnable to parse output/status_details.json file "
                              "from %s" % (traceback.format_exc()), instance)
            raise experiment.model.errors.EnhancedException(
                f"Unable to parse output/status_details.json file from {instance} because of {e}", underlyingError=e
            ) from e

        if pretty_print_instead:
            if filters is None:
                filters = ['all']
            ret = experiment.runtime.status.StatusDB.pretty_print_status_details_report(
                workflowStatus, stages=stages, categories=categories, no_defaults=no_defaults, filters=filters)
        else:
            ret = workflowStatus

        if print_too:
            self.log.info("Status details of %s\n%s" % (instance, ret))

        return ret

    def cdb_get_document_user_metadata_for_rest_uid(self, rest_uid: str, _api_verbose:bool = True):
        """Retrieve the `user-metadata` Document associated with the rest_uid that the ST4SD Runtime Service REST-API
        returns after it creates a new instance for some experiment.

        Arguments:
            rest_uid: unique identifiers of experiment instance
            _api_verbose: Set to true to print INFO level messages

        Returns
            A user-metadata mongo document
        Raises:
            experiment.service.errors.NoMatchingDocumentError: If there is no matching document
        """
        if _api_verbose:
            self.log.info("Querying CDB for a user-metadata document that contains the field rest-uid=%s" % rest_uid)

        ret = self.cdb_get_document(query={'type': 'user-metadata', 'rest-uid': rest_uid}, _api_verbose=False)

        if len(ret) == 0:
            if _api_verbose:
                self.log.info(f"Could not find any workflow instances with the rest-uid \"{rest_uid}\"")
            raise experiment.service.errors.NoMatchingDocumentError(query={
                'type': 'user-metadata',
                'rest-uid': rest_uid})
        elif len(ret) > 1:
            raise ValueError("Multiple (%d) experiments match the same `rest-uid` field (%s)" % (len(ret), rest_uid))

        return ret[0]

    def cdb_get_user_metadata_document_for_rest_uid(self, rest_uid: str, _api_verbose:bool = True):
        """Retrieve the `user-metadata` Document associated with the rest_uid that the ST4SD Runtime Service REST-API
        returns after it creates a new instance for some experiment.

        Deprecated: Use

        Arguments:
            rest_uid: unique identifiers of experiment instance
            _api_verbose: Set to true to print INFO level messages

        Returns
            A user-metadata mongo document

        Raises:
            experiment.service.errors.NoMatchingDocumentError: If there is no matching document
        """
        self.log.warning("Method is deprecated, use method cdb_get_document_user_metadata_for_rest_uid() instead")
        return self.cdb_get_document_user_metadata_for_rest_uid(rest_uid, _api_verbose=_api_verbose)

    def cdb_get_document_experiment_for_rest_uid(
            self,
            rest_uid: str,
            include_properties: List[str] | None = None,
            stringify_nan: bool = False,
            query: Optional[DictMongoQuery] = None,
            _api_verbose:bool = True):
        """Retrieve the `experiment` Document associated with the rest_uid that the ST4SD Runtime Service REST-API
        returns after it creates a new instance for some experiment.

        Arguments:
            rest_uid: unique identifiers of experiment instance
            include_properties: A list of column names or a list with just the string "*" which will automatically
                expand to all the column names in the properties DataFrame that matching experiments have measured.
                The column names in include_properties are case-insensitive, and
                the returned DataFrame will contain lowercase column names.
                When this argument is provided the ST4SD Datastore API (mongo-proxy) will insert the field
                `interface.propertyTable` to matching `experiment` type mongoDocuments. This field will contain
                a dictionary representation of the properties DataFrame that the matching experiment measured. The
                columns in that DataFrame will be those specified in `include_properties`. Column names that do not
                exist will in the DataFrame will be silently discarded. The column `input-id` will always be fetched
                even if not specified in `include_properties`.
            stringify_nan: A boolean flag that allows converting NaN and infinite values to strings
            query: A mongo query to use for further filtering results
            _api_verbose: Set to true to print INFO level messages

        Returns
            An experiment MongoDocument
        Raises:
            experiment.service.errors.NoMatchingDocumentError: If there is no matching document
        """
        if _api_verbose:
            self.log.info("Querying CDB for 1 experiment documents with metadata.userMetadata.rest-uid=%s" % rest_uid)

        if query:
            query = copy.deepcopy(query)
        else:
            query = {}

        query['metadata.userMetadata.rest-uid'] = rest_uid

        ret = self.cdb_get_document_experiment(
            query=query,
            include_properties=include_properties,
            stringify_nan=stringify_nan,
            _api_verbose=_api_verbose)

        if len(ret) == 0:
            if _api_verbose:
                self.log.info(f"Could not find any workflow instances with the rest-uid \"{rest_uid}\"")
            raise experiment.service.errors.NoMatchingDocumentError(query={
                'metadata.userMetadata.rest-uid': rest_uid,
                'type': 'experiment,'
            })
        elif len(ret) > 1:
            raise ValueError("Multiple (%d) experiments match the same `rest-uid` field (%s)" % (len(ret), rest_uid))

        return ret[0]

    def cdb_get_components_raw_files(
        self,
        instance: Optional[str] = None,
        stage: Optional[int] = None,
        component: Optional[str] = None,
        filename: Optional[str] = None,
        query: Optional[DictMongoQuery] = None,
        _api_verbose: bool = True,
    ) -> Dict[str, Tuple[ComponentDocument, bytes]]:
        """Downloads files whose path matches @filename that Components in one or more workflow instances have created

        Parameters set to None will not be used to filter results

        Args:
            instance: The location of the experiment you want to retrieve data from (string or regex)
                it can be a file URI (file://$HOSTNAME/$ABSOLUTE_PATH)
            filename:A string or a regex (uncompiled). The name of the file you want to retrieve
            component: The name/reference of a component you want to retrieve data from (string or regex).
               If component is in reference format (i.e stage<%d>.%s) `%d` will be used by the `stage` argument
               if `stage` is not None then  this method
               raises experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage exception
            stage: The index of a stage you want to retrieve data from (integer)
            query: A mongo query to use for further filtering results
            _api_verbose: Set to true to print INFO level messages

        Raises:
            requests.exceptions.RequestException: if after @self.max_request_retries there's a
                requests.exceptions.RequestException exception while Querying mongo
            pymongo.errors.PyMongoError: if after @self.max_request_retries there's a
                pymongo.errors.PyMongoError exception while Querying mongo

        Returns:
            A Dictionary of {file_ur: [ComponentDocument, Contents_of_file]} for matching iles
        """
        msg = "%s produced by stage%s/%s in instance %s" % (filename, stage, component, instance)
        if _api_verbose:
            self.log.info("Fetching %s" % msg)

        retries = 0

        while True:
            try:
                return {
                    uri: (doc, filecontents) for doc, uri, filecontents in self.cl.getFilesAndPostProcess(
                    instance=instance, stage=stage, component=component, filename=filename, query=query)
                }
            except (requests.exceptions.RequestException, pymongo.errors.PyMongoError) as e:
                retries += 1

                if retries > self.max_request_retries:
                    raise_with_traceback(ValueError("Could not fetch %s because %s" % (msg, e)))
                else:
                    if _api_verbose:
                        self.log.info("Error %s" % e)

                    time.sleep(self.secs_between_retries)

    def cdb_get_components_last_stdout(self, instance=None, stage=None, component=None, _api_verbose=True):
        """Retrieves contents last recorded stdout of components

        Parameters set to None will not be used to filter results

        Args:
            instance(str): file://$GATEWAY_ID/absolute/path/to/instance/dir
            stage(int): index of stage comprising component
            component(str): name of component may be a component reference "stage%d.name". If @name contains
                only alphanumeric characters and '-', '_' characters then query turns into an exact match.
                Otherwise field is treated as a regular expression
            _api_verbose(bool): if set to True, method will info messages when retrying

        Returns: Dict[stdout_uri:str, Tuple[ComponentDocument:Dict[str, Any], stdout_contents:str]]

        Raises:
            MongoQueryContainsComponentReferenceAndExtraStage - if component is reference and stage is not None
        """
        # VV: Need to partition reference into stage/component here to be able to check just the
        # component name for regular-expression characters (a component reference contains the '.' character)
        stage, component = self.cl._util_preprocess_stage_and_component_reference(stage, component)

        if not any((True for x in (component or '') if (str.isalnum(x) is False) and (x not in '-_'))):
            component = ''.join((r"\b", component, r"\b"))

        return self.cdb_get_components_raw_files(instance, stage, component, 'out.stdout', _api_verbose=_api_verbose)

    def resolve_gateway(self, gateway_name, _api_verbose=True, _skip_cache=False):
        """Resolves a gateway id to a URL, may use cache and may reset cache if cache is old

        Arguments:
            gateway_name(str): unique identifier of gateway
            _api_verbose(bool): when True prints out more information about the HTTP request - useful for debugging
            _skip_cache(bool): if true attempt to resolve gateway without checking the cache at all

        Returns
            the gateway URL

        Raises
            experiment.errors.UnknownGateway - if GatewayRegistry does not have an entry for gateway id
            experiment.errors.EnhancedException - if unable to reach the gateway registry or decode the response
        """
        return self.cl.resolve_gateway(gateway_name, _api_verbose, _skip_cache)

    def cdb_query_component_files_exist(self, instance, stage, component):
        # type: (str, int, str) -> bool
        gateway_name, instance_location = experiment.model.storage.partition_uri(instance)
        instance_location = urllib.parse.quote_plus(instance_location)
        gateway_url = self.resolve_gateway(gateway_name)

        path = 'experiment?' \
              'experiment=%s' \
              '&stage=%d' \
              '&component=%s' % (instance_location, stage, component)
        # VV: Returns `True` or `False`, anything else is an error
        exists = self.api_request_get(path, endpoint=gateway_url, decode_json=False)

        return exists.lower() == 'true'

    def _stream_file(self, url, dir, prefix, suffix, _api_verbose=True, verify_tls=False):
        """Download a file, store it on the disk, and return its filepath"""
        retries = 0

        while True:
            try:
                with tempfile.NamedTemporaryFile(dir=dir, prefix=prefix, suffix=suffix, delete=False) as f:
                    with self._session.get(url, stream=True, verify=verify_tls) as r:
                        r.raise_for_status()
                        for chunk in r.iter_content(chunk_size=4096):
                            f.write(chunk)
                    return f.name
            except requests.exceptions.RequestException as e:
                retries += 1
                if retries > self.max_request_retries:
                    raise_with_traceback(ValueError("Could not download %s because %s" % (url, e)))
                else:
                    if _api_verbose:
                        self.log.warning("Error when downloading %s - %s will retry" % (url, e))

                    time.sleep(self.secs_between_retries)

    def cdb_download_component_files(
            self,  instance: str,  stage: Optional[int], component: Optional[str], extract_to: Optional[str],
            delete_zip : bool = True, _api_verbose : bool = True) -> str:
        """Downloads the entire working directory of a component and populates a folder on the local filesystem

        Parameters set to None will not be used to filter results

        Args:
            instance: file://$GATEWAY_ID/absolute/path/to/instance/dir
            stage: index of stage comprising component
            component: name of component may be a component reference "stage%d.name". If @name contains
                only alphanumeric characters and '-', '_' characters then query turns into an exact match.
                Otherwise field is treated as a regular expression
            extract_to: Path to directory to populate it with the contents of the working directory of the
                1 component. If set to None, method will skip extracting.
            delete_zip: Whether to download zip after downloading it, otherwise it'll be stored in a random
                path under a temporary folder
            _api_verbose: if set to True, method will info messages when retrying

        Returns:
            The path of the downloaded zip file (the file will *not* exist if delete_zip is set to true)

        Raises:
            MongoQueryContainsComponentReferenceAndExtraStage - if component is reference and stage is not None
        """
        stage, component = self.cl._util_preprocess_stage_and_component_reference(stage, component)

        gateway_name, instance_location = experiment.model.storage.partition_uri(instance)
        gateway_url = self.resolve_gateway(gateway_name)
        if _api_verbose:
            self.log.info("Downloading component stage%s.%s from instance %s via remote gateway %s" % (
                stage, component, instance_location, gateway_url))

        instance_location = urllib.parse.quote_plus(instance_location)

        dl_url = '%s/experiment/download?' \
               'experiment=%s' \
               '&stage=%d' \
               '&component=%s' % (gateway_url, instance_location, stage, component)

        zip_file_path = self._stream_file(dl_url, '/tmp', 'flow-component-', '.zip', _api_verbose=_api_verbose)

        if _api_verbose:
            self.log.info("Downloaded %s to %s" % (dl_url, zip_file_path))

        if extract_to is not None:
            try:
                z = zipfile.ZipFile(zip_file_path)
                z.extractall(extract_to)
                if _api_verbose:
                    self.log.info("Extracted %s to %s" % (zip_file_path, extract_to))
            except Exception as e:
                self.log.info("Failed to extract %s to %s because of %s" % (zip_file_path, extract_to, e))
                raise e

        if delete_zip:
            try:
                if os.path.exists(zip_file_path):
                    os.remove(zip_file_path)
                    if _api_verbose:
                        self.log.info("Deleted zip file %s after extraction" % zip_file_path)
            except Exception as e:
                self.log.info("Could not delete zip file %s after extraction because of %s "
                              "- ignoring error" % (zip_file_path, e))

        return zip_file_path

    def cdb_upsert_user_metadata(self, documents, tags):
        # type: (List[Dict[str, Any]], Dict[str, Any]) -> None
        """Updates user-metadata @documents to include @tags and stores them on the ST4SD Datastore backend.
        Also updates associated `experiment` documents.

        Args:
            documents(List[Dict[str, Any]], Dict[str, Any]): Documents to update
            tags(Dict[str, Any]): Tags to upsert into the documents
        """
        return self.cl.upsertUserMetadataTags(documents, tags)


class ExperimentRestAPIDeprecated:
    """Deprecated wrapper to the ST4SD Datastore (CDB) and ST4SD Runtime Service REST-APIs

    Please update your code to use ExperimentRestAPI
    """
    def __init__(self, consumable_computing_rest_api_url, cdb_registry_url, cdb_rest_url,
                 max_retries=10, secs_between_retries=5, test_cdb_connection=True,
                 mongo_host=None, mongo_port=None, mongo_user=None, mongo_password=None, mongo_authsource=None,
                 mongo_database=None, mongo_collection=None, max_http_batch=10, cc_auth_token=None,
                 cc_bearer_key=None, validate_auth=True, discover_cdb_urls=True):
        """Instantiate a wrapper to the ST4SD Datastore (CDB) and ST4SD Runtime Service REST-APIs

        Args:
            consumable_computing_rest_api_url(string): URL to ST4SD Runtime Service REST-API endpoint
            cdb_registry_url(string): URL to ST4SD Datastore registry REST API endpoint
            cdb_rest_url(string): URL to ST4SD Datastore MongoDB REST-API endpoint
            max_retries(int): Maximum number of attempts for a failing request
            secs_between_retries(int): Time to wait between successive retries
            max_http_batch(int): Maximum number of files per batch when downloading files from
              Cluster Gateways
            test_cdb_connection(bool): If set to True, the constructor will assert that it can access the
              ST4SD Datastore MongoDB REST-API endpoint
            mongo_host(str): Host (ip/domain) of MongoDB backend (not required when using a cdb_rest_url)
            mongo_port(int): Port of MongoDB backend (not required when using a cdb_rest_url)
            mongo_user(str): Username to authenticate to MongoDB (not required when using a cdb_rest_url)
            mongo_password(str): Password to use when authenticating to MongoDB (not required when using a cdb_rest_url)
            mongo_database(str): Name of the database in MongoDB (not required when using a cdb_rest_url)
            mongo_collection(str): Name of the collection in the MongoDB database (not required when using a
                cdb_rest_url)
            cc_auth_token(str): Auth token generated by the ST4SD Runtime Service REST-API. Will be used whenever
                ExperimentRestAPIDeprecated sends HTTP requests to @consumable_computing_rest_api_url.
                If both cc_auth_token and cc_bearer_key are provided, only cc_bearer_key is used.
            cc_bearer_key(str): API key that can be used as a `Bearer token` to authenticate to the Consumable Computing
                REST-API ExperimentRestAPIDeprecated sends HTTP requests to @consumable_computing_rest_api_url.
                If both cc_auth_token and cc_bearer_key are provided, only cc_bearer_key is used.
            validate_auth(bool): Attempt to validate the authorisation key/token (provided the key/token is not None)
            discover_cdb_urls(bool): Attempt to query the CC end-point to get any non-provided CDB urls
              (provided that consumable_computing_rest_api_url is not None)
        """

        self.log = logging.getLogger('ExperimentRestAPIDeprecated')

        # VV: Instantiate an instance of the new Api and delegate work to it
        self._api = ExperimentRestAPI(
            consumable_computing_rest_api_url=consumable_computing_rest_api_url,
            cdb_registry_url=cdb_registry_url, cdb_rest_url=cdb_rest_url,
            max_retries=max_retries, secs_between_retries=secs_between_retries,
            test_cdb_connection=test_cdb_connection,
            mongo_host=mongo_host, mongo_port=mongo_port, mongo_user=mongo_user,
            mongo_password=mongo_password, mongo_authsource=mongo_authsource,
            mongo_database=mongo_database, mongo_collection=mongo_collection,
            max_http_batch=max_http_batch, cc_auth_token=cc_auth_token,
            cc_bearer_key=cc_bearer_key, validate_auth=validate_auth,
            discover_cdb_urls=discover_cdb_urls
        )

    @property
    def cl(self) -> Mongo:
        return self._api.cl

    def update_consumable_computing_auth_token(self, cc_auth_token):
        # type: (Optional[str]) -> None
        return self._api.update_consumable_computing_auth_token(cc_auth_token=cc_auth_token)

    def update_consumable_computing_bearer_key(self, cc_bearer_key):
        # type: (Optional[str]) -> None
        return self._api.update_consumable_computing_bearer_key(cc_bearer_key=cc_bearer_key)

    def api_path_to_url(self, path, endpoint=None):
        return self._api.api_path_to_url(path=path, endpoint=endpoint)

    def api_request_get(self, path, json_payload=None, decode_json=True, return_response=False,
                        _api_verbose=True, endpoint=None, **kwargs):
        # type: (str, Optional[Any], bool, bool, bool, str, Dict[str, Any]) -> Union[Any, str, requests.Response]
        """Send HTTP GET request and return either the JSON or the Text of the HTTP response
        Args:
            path(str): HTTP Path
            json_payload(Optional[Any]): JSON payload to send along the HTTP request
            decode_json(bool): When True, decodes response text into a JSON dictionary, else returns raw
                response text
            return_response(bool): When True, returns requests.Response object (overrides decode_json)
            _api_verbose(bool): when True print out information about the request
            endpoint(str): HTTP host domain
            **kwargs: Parameters to requests.get

        Raises:
            experiment.errors.UnauthorisedRequest for http status codes 403 and 401

        Notes:
            1. This function may also inject authorisation headers for requests self.cc_endpoint
            2. On exceptions, other than experiment.errors.UnauthorisedRequest,
              retries up to self.max_request_retries
        """
        return self._api.api_request_get(path=path, json_payload=json_payload, decode_json=decode_json,
                                         return_response=return_response, _api_verbose=_api_verbose,
                                         endpoint=endpoint, **kwargs)

    def api_request_post(self, path, json_payload=None, decode_json=True, _api_verbose=True, endpoint=None, **kwargs):
        # type: (str, Optional[Any], bool, bool, str, Dict[str, Any]) -> Union[Any, str]
        """Sends a HTTP POST request to endpoint/path and return either the JSON or the Text of the HTTP response
        Args:
            path(str): HTTP Path
            json_payload(Optional[Any]): JSON payload to send along the HTTP request
            decode_json(bool): When true decodes response text into a JSON dictionary, else returns raw
                response text
            _api_verbose(bool): when True print out information about the request
            endpoint(str): HTTP host domain
            **kwargs: Parameters to requests.post

        Raises:
            experiment.errors.UnauthorisedRequest for http status codes 403 and 401

        Notes:
            1. This function may also inject authorisation headers for requests self.cc_endpoint
            2. On exceptions, other than experiment.errors.UnauthorisedRequest,
              retries up to self.max_request_retries
        """
        return self._api.api_request_post(path=path, json_payload=json_payload, decode_json=decode_json,
                                         _api_verbose=_api_verbose, endpoint=endpoint, **kwargs)

    def api_request_delete(self, path, json_payload=None, decode_json=True, _api_verbose=True, endpoint=None, **kwargs):
        # type: (str, Optional[Any], bool, bool, str, Dict[str, Any]) -> Union[Any, str]
        """Sends a HTTP DELETE request to endpoint/path and return either the JSON or the Text of the HTTP response
        Args:
            path(str): HTTP Path
            json_payload(Optional[Any]): JSON payload to send along the HTTP request
            decode_json(bool): When true decodes response text into a JSON dictionary, else returns raw
                response text
            _api_verbose(bool): when True print out information about the request
            endpoint(str): HTTP host domain
            **kwargs: Parameters to requests.delete

        Raises:
            experiment.errors.UnauthorisedRequest for http status codes 403 and 401
            experiment.errors.InvalidHttpRequest when http status code is other than 200

        Notes:
            1. This function may also inject authorisation headers for requests self.cc_endpoint
            2. On exceptions, other than experiment.errors.UnauthorisedRequest,
              retries up to self.max_request_retries
        """
        return self._api.api_request_delete(path=path, json_payload=json_payload, decode_json=decode_json,
                                          _api_verbose=_api_verbose, endpoint=endpoint, **kwargs)

    def api_request_put(self, path, json_payload=None, decode_json=True, _api_verbose=True, endpoint=None, **kwargs):
        # type: (str, Optional[Any], bool, bool, str, Dict[str, Any]) -> Union[Any, str]
        """Sends a HTTP DELETE request to endpoint/path and return either the JSON or the Text of the HTTP response
        Args:
            path(str): HTTP Path
            json_payload(Optional[Any]): JSON payload to send along the HTTP request
            decode_json(bool): When true decodes response text into a JSON dictionary, else returns raw
                response text
            _api_verbose(bool): when True print out information about the request
            endpoint(str): HTTP host domain
            **kwargs: Parameters to requests.put

        Raises:
            experiment.errors.UnauthorisedRequest for http status codes 403 and 401

        Notes:
            1. This function may also inject authorisation headers for requests self.cc_endpoint
            2. On exceptions, other than experiment.errors.UnauthorisedRequest,
              retries up to self.max_request_retries
        """
        return self._api.api_request_put(path=path, json_payload=json_payload, decode_json=decode_json,
                                            _api_verbose=_api_verbose, endpoint=endpoint, **kwargs)

    def api_experiment_delete(self, experiment_id, _api_verbose=False):
        """Deletes an experiment definition

        Args:
            experiment_id(str): Unique identifier of the experiment definition (as shown on the consumable computing
              REST API server)
            _api_verbose(bool): when True print out information about the request

        Returns(bool): True when instance deleted, False otherwise

        Raises:
            experiment.errors.UnauthorisedRequest - when user is unauthorized to the ST4SD Runtime Service REST-API
            experiment.errors.InvalidHTTPRequest - when response HTTP status is other than 200
        """
        # VV: Expect status code 200 for Success, and 400/404 for failure, treat anything else as an error
        return self._api.api_experiment_delete(experiment_id=experiment_id, _api_verbose=_api_verbose)

    def api_experiment_list(self, print_too=False, _api_verbose=True):
        return self._api.api_experiment_list(print_too=print_too, _api_verbose=_api_verbose)

    def api_experiment_upsert(self, package, experiments=None, _api_verbose=True):
        """Creates an experiment if it does not exist, updates its definition if it already exists

        Args:
            package(Dict[str, Any]): Definition of an experiment package (Check ST4SD Runtime Service REST-API swagger
              for the schema of a package)
            experiments(Dict[str, Dict[str, Any]]): (Optional) known experiments on the ST4SD Runtime Service REST-API
            _api_verbose(bool): when True print out information about the request

        Returns(requests.Response): response of the ST4SD Runtime Service REST-API
        """
        return self._api.api_experiment_upsert(package=package, experiments=experiments, _api_verbose=_api_verbose)

    def api_instance_delete(self, instance_id):
        """Deletes an instance

        Args:
            instance_id(str): Unique identifier of the experiment instance (as shown on the consumable computing
              REST API server)

        Returns(bool): True when instance deleted, False otherwise

        Raises:
            experiment.errors.UnauthorisedRequest - when user is unauthorized to the ST4SD Runtime Service REST-API
            experiment.errors.InvalidHTTPRequest - when response HTTP status is other than 200
        """
        return self._api.api_rest_uid_delete(rest_uid=instance_id)

    def api_instance_create(self, experiment_id, payload):
        """Creates an instance of an experiment

        Args:
            experiment_id(str): Unique identifier of the experiment package (as stored on the consumable computing
              REST API server)
            payload(Dict[str, Any]): Check ST4SD Runtime Service REST-API swagger for the schema of the payload

        Returns(str): rest-uid of the experiment instance
        """
        return self._api.api_experiment_start(experiment_id=experiment_id, payload=payload)

    def api_instance_create_lambda(self, payload, validate=True, print_too=True, _api_verbose=False):
        """Creates a lambda instance

        Args:
            payload(Dict[str, Any]): Definition of lambda package (check ST4SD Runtime Service REST-API swagger for the
              schema of the payload)
            validate(bool): Whether to validate flowir before sending the payload to the ST4SD Runtime Service REST-API
              endpoint (default: True)
            print_too: if set to True this method will also print the reply of the ST4SD Runtime Service REST-API server
            _api_verbose(bool): when True prints out more information about the HTTP request - useful for debugging
        Returns(str): rest-uid
        """
        return self._api.api_experiment_start_lambda(payload=payload, validate=validate, print_too=print_too,
                                                     _api_verbose=_api_verbose)

    def api_instance_status(self, instance_id, print_too=True, _api_verbose=True):
        """Fetches the status of an instance

        Args:
            instance_id: rest-uid returned from Consumable Computing API
            print_too: if set to True this method will also print the reply of the ST4SD Runtime Service REST-API server
            _api_verbose: if set to True this method will print info messages when retrying

        Returns: Dictionary[str, Any]
            Dictionary containing the status of the instance (check the swagger model of ST4SD Runtime Service REST-API
            for the definition of the status dictionary)
        """
        return self._api.api_rest_uid_status(rest_uid=instance_id, print_too=print_too, _api_verbose=_api_verbose)

    def api_instance_output(self, rest_uid, output_name, _api_verbose=False):
        """Gets a key-output that a workflow instance generated

        if output_name points to a folder the contents of the folder will be returned in the form of a ZIP file

        Args:
            rest_uid(str): identifier of workflow instance (workflow object name/rest-uid returned by instance_create)
            output_name(str): identifier of key-output
            _api_verbose(bool): whether to print debugging statements regarding HTTP requests

        Returns
            The tuple (filename:str, contents:bytes)
        """
        return self._api.api_rest_uid_output(rest_uid=rest_uid, output_name=output_name, _api_verbose=_api_verbose)

    def api_datasets_list(self, print_too=True, _api_verbose=False):
        # type: (bool, bool) -> List[str]
        """Lists available datasets as reported by the ST4SD Runtime Service REST-API

        Arguments:
            print_too(bool): setting this to True will also print the list of available datasets
            _api_verbose(bool): print verbose statements regarding HTTP request and Exceptions

        Returns:
            A list of strings, each of which is the id of a dataset
        """
        return self._api.api_datasets_list(print_too=print_too, _api_verbose=_api_verbose)

    def api_datasets_create(self, dataset_name, endpoint, bucket, access_key_id, secret_access_key,
                            region=None, print_too=True, _api_verbose=False):
        """Creates a dataset that points to a S3 bucket

        Arguments:
            dataset_name(str): Name of dataset
            endpoint(str): S3 endpoint
            bucket(str): name of S3 bucket
            access_key_id(str): access key ID for S3 bucket
            secret_access_key(str): secret access key for S3 bucket
            region(Optional[str]): (optional) region for S3 bucket
            print_too(bool): setting this to True will also echo a success message on object creation
            _api_verbose(bool): print verbose statements regarding HTTP request and Exceptions
        """
        return self._api.api_datasets_create(dataset_name=dataset_name, endpoint=endpoint, bucket=bucket,
                                             access_key_id=access_key_id, secret_access_key=secret_access_key,
                                             region=region, print_too=print_too, _api_verbose=_api_verbose)

    def api_image_pull_secrets_list(self, print_too=True, _api_verbose=False):
        # type: (bool, bool) -> Dict[str, List[str]]
        """Lists available imagePulLSecrets as reported by the ST4SD Runtime Service REST-API. These imagePullSecrets
        will be automatically used by the Kubernetes Jobs that your workflow instances will create.

        Arguments:
            print_too(bool): setting this to True will also print the available imagePullSecrets
            _api_verbose(bool): print verbose statements regarding HTTP request and Exceptions

        Returns:
            A Dictionary, keys are imagePullSecrets, values are lists of docker-registries for which the
            imagePullSecret has pull access to.
        """
        return self._api.api_image_pull_secrets_list(print_too=print_too, _api_verbose=_api_verbose)

    def api_image_pull_secrets_create(self, secret_name, registry, username, password,
                                      print_too=True, _api_verbose=False):
        """Creates an imagePullSecret which has pull access to a docker registry.

        Arguments:
            secret_name(str): Name of secret
            registry(str): docker registry endpoint (url)
            username(str): username to authenticate
            password(str): password/api-key to authenticate with
            print_too(bool): setting this to True will also echo a success message on object creation
            _api_verbose(bool): print verbose statements regarding HTTP request and Exceptions

        Notes:

            This method receives a password/api-token in plaintext. Do NOT push your password/api-token to github
            or any other file hosting service. Remember to sanitize the calls to this method and never leave
            your password in plain view.
        """
        return self._api.api_image_pull_secrets_create(
            secret_name=secret_name, registry=registry, username=username,
            password=password, print_too=print_too, _api_verbose=_api_verbose)

    def api_image_pull_secrets_upsert(self, secret_name, registry, username, password,
                                      print_too=True, _api_verbose=False):
        """Creates an imagePullSecret which has pull access to a docker registry, updates existing secret if it alrady
        exists.

        Arguments:
            secret_name(str): Name of secret
            registry(str): docker registry endpoint (url)
            username(str): username to authenticate
            password(str): password/api-key to authenticate with
            print_too(bool): setting this to True will also echo a success message on object creation
            _api_verbose(bool): print verbose statements regarding HTTP request and Exceptions

        Notes:

            This method receives a password/api-token in plaintext. Do NOT push your password/api-token to github
            or any other file hosting service. Remember to sanitize the calls to this method and never leave
            your password in plain view.
        """
        return self._api.api_image_pull_secrets_upsert(
            secret_name=secret_name, registry=registry, username=username,
            password=password, print_too=print_too, _api_verbose=_api_verbose)

    def _deprecated_query_to_new_query(
            self,
            query: Optional[DictMongoQuery] = None,
            ) -> DictMongoQuery:
        """Converts a deprecated query to a new query

        Parameters other than @query may end up overriding those you define in @query. Method may also post-process
        the values of parameters that are not @query. This method post-processes the query right before sending it to
        Mongo so that its keys are strings and its string-values are regular-expressions.

        Args:
            query: A deprecated mongo query to use for further filtering results (i.e. arguments that we used
                to provide to cdb_get_data() and cdb_get_document() methods)

        Raises:
            experiment.errors.MongoQueryContainsComponentReferenceAndExtraStage: if component is reference and
                stage is not None

        Returns:
            Returns the resulting MongoQuery dictionary
        """
        query: DictMongoQuery = copy.deepcopy(query or {})

        # VV: Convert deprecated experimentLocation to instance, then extract "special" keys which
        # CDBExperimentRestAPI transforms from the query and then return the output of preprocess_query.
        # The caller may use the dictionary they recieve as the value of `query` parameters to methods
        # such as ExperimentRestAPI.cdb_get_data() and ExperimentRestAPI.cdb_get_document()
        if 'experimentLocation' in query:
            query['instance'] = query['experimentLocation']
            query.pop('experimentLocation')

        def try_pop(key: str, dictionary: DictMongoQuery) -> Any:
            """Utility function to pop key from dictionary and return value, returns None if key does not exist"""
            try:
                return dictionary.pop(key)
            except KeyError:
                return None

        instance: Optional[str] = try_pop('instance', query)
        stage: Optional[int] = try_pop('stage', query)
        component: Optional[str] = try_pop('component', query)
        filename: Optional[str] = try_pop('filename', query)

        return self._api.cl.preprocess_query(
            filename=filename, component=component, stage=stage, instance=instance, query=query)

    def cdb_get_data(self, query, _api_verbose=True):
        # type: (DictMongoQuery, bool) -> Tuple[List[ComponentDocument], List["experiment.utilities.data.Matrix"]]
        """Queries the centralized-db (CDB) for CSV files and returns a tuple of 2 lists for matching CSV files.

        The first List contains Documents of Components which produced the matching CSV files. The
        second List contains experiment.utilities.data.Matrix instances that have been populated with the contents
        of the CSV files. The i-th entry in the 1st list corresponds to the i-th entry in the 2nd list.

        The fields of the query, are the same as the arguments to experiment.service.db.Mongo.getData():
            filename:A string or a regex (uncompiled). The name of the file you want to retrieve
            component: The name of a component you want to retrieve data from (string or regex)
            stage: The index of a stage you want to retrieve data from (integer)
            experimentLocation: The location of the experiment you want to retrieve data from (string or regex)
                it can be a file URI (file://$HOSTNAME/$ABSOLUTE_PATH)

        **Note**: Currently the `experimentLocation` parameter of .getData() is mapped to
                  the `instance` field of Documents (which is expected to be a instance_uri
                  with the format file://${HOSTNAME}/full/path/to/instance, or a regular expression)
        """
        query = self._deprecated_query_to_new_query(query)
        return self._api.cdb_get_data(query=query)

    def cdb_get_document(self, query, _api_verbose=True):
        """Queries the centralized-db (CDB) for MongoDB documents.

        The fields of the query must much those of experiment.service.db.Mongo.getDocument(), they are the fields that
        a "component"-type Document may be queried for:

        Query:
            experimentLocation(str): File URI in the form of file://$GATEWAY_ID/absolute/path/to/instance/dir
            stage(int): index of stage comprising component
            component(str): name of component
            filename(str): relative to the working dir of the component path
            type(str): type of the document to return (e.g. "component", "user-metadata", "experiment")

        However, the getDocuments may also query the CDB for other Mongo documents, such as "user-metadata" and
        "experiments".
        The query keys for an "experiment" document are:
            type: "experiment"  # including this restricts your search to just "experiment" documents
            instance: File URI in the form of file://$GATEWAY_ID/absolute/path/to/instance/dir
            name: Name of the experiment

        Similarly, for "user-metadata" documents:
            type: "user-metadata"
            instance: File URI in the form of file://$GATEWAY_ID/absolute/path/to/instance/dir
            <name>: `-m <name>:<value>` arguments to elaunch.py will instruct elaunch.py to include
               a <name> field in the "user-metadata" document that is associated with the experiment

        Returns a list of dictionaries created out of MongoDB documents

        **Note**: Currently the `experimentLocation` parameter of .getDocument() is mapped to
                  the `instance` field of Documents (which is expected to be a instance_uri
                  with the format file://${HOSTNAME}/full/path/to/instance, or a regular expression)
        """

        query = self._deprecated_query_to_new_query(query)
        return self._api.cdb_get_document(query=query)

    def cdb_get_file_from_instance_uri(self, instance_uri, rel_path, _api_verbose=True):
        """"""
        return self._api.cdb_get_file_from_instance_uri(instance=instance_uri, rel_path=rel_path,
                                                        _api_verbose=_api_verbose)

    def cdb_get_detailed_status_report(self, instance_uri, pretty_print_instead=True,
                                       stages=None, categories=None, no_defaults=False, filters=None, print_too=True,
                                       _api_verbose=False):
        """Retrieves component status details report and pretty prints it to a string.

        The pretty-printer string is identical to the output of `einspect.py -f all -c producer -c task -c engine`.
        Bear in mind that this report is asynchronously generated, so unless the workflow is finished the
        information included in the report may not 100% reflect the current state of the workflow instance.

        Arguments:
            instance_uri(str): The workflow instance URI for which you wish to retrieve its status details (
                a file URI has the format file://$HOSTNAME/$ABSOLUTE_PATH, it's the 'instance' field of all
                mongoDB documentDescriptions that the CDB returns via cdb_get_documents())
            pretty_print_instead(bool): If set to True this method will return a pretty-printed string representation
                of the report. Setting this to False will instead return a dictionary which was generated via
                experiment.service.db.Status.getWorkflowStatus().
                Default is True
            stages(List[int]): An optional list of stage indices, when provided status report will
                only contain entries for components in those stages.
                Only used if pretty_print_instead is set to True.
            categories(List[str]): Optional list of categories for which information is output.
                Available categories are producer, task, and engine
            no_defaults(bool): Omits output columns in the  categories - useful if there are too many columns
                being output.
                Only used if pretty_print_instead is set to True.
            filters(List[str]): Set filters that limit the components output to those exhibiting certain behaviour.
                With each use of this option an additional filter can be specified. Available filters are:
                failed, interesting, engine-exited, blocked-output, blocked-resource, not-executed, executing, and all.
                Default filters applied are: all
                Only used if pretty_print_instead is set to True.
            print_too(bool): If set to True, will also self.log.info() the result. Default is True
            _api_verbose(bool): when True prints out more information about the cdb request - useful for debugging
        """

        return self._api.cdb_get_detailed_status_report(
            instance=instance_uri, pretty_print_instead=pretty_print_instead, stages=stages, categories=categories,
            no_defaults=no_defaults, filters=filters, print_too=print_too, _api_verbose=_api_verbose)

    def cdb_get_metadata_for_rest_uid(self, instance_id, _api_verbose=True):
        """Retrieve the `user-metadata` Document associated with the instance_id that the ST4SD Runtime Service REST-API
        returns after it creates a new instance for some experiment.

        **Note**: The instance_id is stored as the `rest-uid` field in the associated `user-metadata` MongoDB Document.
        """
        return self._api.cdb_get_user_metadata_document_for_rest_uid(rest_uid=instance_id, _api_verbose=_api_verbose)

    def cdb_get_components_raw_files(self, instance_uri=None, stage=None, component=None, filename=None,
                                    _api_verbose=True):
        """Returns one tuple [ComponentDocument:Dict[str, str], Contents_of_file:str, file_URI:str] per matched file.

        Parameters set to None will not be used to filter results

        Args:
            instance_uri(str): file://$GATEWAY_ID/absolute/path/to/instance/dir
            stage(int): index of stage comprising component
            component(str): name of component
            filename(str): relative to the working dir of the component path
            _api_verbose(bool): if set to True, method will info messages when retrying

        Returns: Dict[file_uri:str, Tuple[ComponentDocument:Dict[str, Any], file_contents:str]]
        """
        return self._api.cdb_get_components_raw_files(
            instance=instance_uri, stage=stage, component=component, filename=filename,
            query={}, _api_verbose=True)

    def cdb_get_components_last_stdout(self, instance_uri=None, stage=None, component=None, _api_verbose=True):
        """Retrieves contents last recorded stdout of components

        Parameters set to None will not be used to filter results

        Args:
            instance_uri(str): file://$GATEWAY_ID/absolute/path/to/instance/dir
            stage(int): index of stage comprising component
            component(str): name of component
            _api_verbose(bool): if set to True, method will info messages when retrying

        Returns: Dict[stdout_uri:str, Tuple[ComponentDocument:Dict[str, Any], stdout_contents:str]]
        """
        return self.cdb_get_components_raw_files(instance_uri, stage, component, 'out.stdout', _api_verbose)

    def resolve_gateway(self, gateway_name, _api_verbose=True, _skip_cache=False):
        """Resolves a gateway id to a URL, may use cache and may reset cache if cache is old

        Arguments:
            gateway_name(str): unique identifier of gateway
            _api_verbose(bool): when True prints out more information about the HTTP request - useful for debugging
            _skip_cache(bool): if true attempt to resolve gateway without checking the cache at all

        Returns
            the gateway URL

        Raises
            experiment.errors.UnknownGateway - if GatewayRegistry does not have an entry for gateway id
            experiment.errors.EnhancedException - if unable to reach the gateway registry or decode the response
        """
        return self._api.resolve_gateway(gateway_name=gateway_name, _api_verbose=_api_verbose,
                                         _skip_cache=_skip_cache)

    def cdb_query_component_files_exist(self, instance_uri, stage_index, component_name):
        # type: (str, int, str) -> bool
        return self._api.cdb_query_component_files_exist(instance=instance_uri, stage=stage_index,
                                                         component=component_name)

    def cdb_download_component_files(self, instance_uri, stage_index, component_name, extract_to,
                                     delete_zip=True, _api_verbose=True):
        return self._api.cdb_download_component_files(
            instance=instance_uri, stage=stage_index, component=component_name, extract_to=extract_to,
            delete_zip=delete_zip, _api_verbose=_api_verbose)

    def cdb_upsert_user_metadata(self, documents, tags):
        # type: (List[Dict[str, Any]], Dict[str, Any]) -> None
        """Updates user-metadata @documents to include @tags and stores them on the ST4SD Datastore backend.
        Also updates associated `experiment` documents.

        Args:
            documents(List[Dict[str, Any]], Dict[str, Any]): Documents to update
            tags(Dict[str, Any]): Tags to upsert into the documents
        """
        return self._api.cdb_upsert_user_metadata(documents=documents, tags=tags)
