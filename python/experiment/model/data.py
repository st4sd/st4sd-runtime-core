# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

'''Class/functions for handling an experiments data.

Moving/staging etc.

The classes in this module are mostly data containers (the Model in MVC)
Instances describe how they should be run. The deployment and control is
handled by other classes.

The data encapsulated by an instance of a class usually resides on disk.'''

#Redisign
#Component composition of
#  -  ComponentSpecification which is a node of the graph (most current functionality) - move to graph
#  -  Working Dir
#  -  Engine
#  -  Will including merging functionality currently in workflow.ComponentState
#DataReference
#  -  Defines the edges of the graph. Move to graph. Remove need to access Experiment and working dir.
#  -  Not actually an edge as multiple DataReferences with different methods can exist on one-edge
#  -  But Graph needs access to them to work out the edges

# VV: Annotations switched on a feature which will be default for py3.10
# tl;dr it makes it easier to use type-hints
from __future__ import print_function, annotations

import errno
import copy
import datetime
import json
import logging
import os
import pprint
import re
import shutil
import sys
import tarfile
import time
import traceback
import uuid
import weakref
from typing import (TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set,
                    Tuple, Type, Union, cast)

import pandas
import pandas.core.series
import yaml
import yaml.parser
from future.utils import raise_with_traceback
from networkx import DiGraph

from six import string_types

import experiment.appenv
import experiment.model.codes
import experiment.model.errors
import experiment.model.executors
import experiment.model.frontends.flowir
import experiment.model.graph
import experiment.model.conf
import experiment.model.interface
import experiment.model.storage
import experiment.model.hooks.interface
import experiment.runtime.status


if TYPE_CHECKING:
    from experiment.model.executors import Command
    from experiment.model.graph import ComponentSpecification

    # VV: This is only a type-hint dependency, the module is not imported at execution time
    from experiment.runtime.workflow import ComponentState


# VV: Component documents are also complex so we'll assume Values can be "Any"
ComponentDocument = Dict[str, Any]


def ComponentNameFromPath(path):
    '''Given a path inside an experiment returns the component

    Exceptions:
        Raises ValueError if path is not a path to a component'''

    # First split the path and find the stage

    pathComponents = os.path.normpath(path).split(os.path.sep)
    component = None

    #NOTE: For nested experiments we need to find the LAST index in
    #the path where stages occurs
    #Otherwise we will find the name of the top-level component that led to the launch of the
    #actual experiment containing the file

    if "stages" in pathComponents:
        #THis can't raise an exeception as above if checks if theres at least one 'stages' in pathComponents
        index = max([ind for ind,component in enumerate(pathComponents) if component == 'stages'])
        if len(pathComponents) >= index + 2:
            component = pathComponents[index + 2]
        else:
            raise ValueError('Path not long enough to contain component. Sub stages path is %s' % os.sep.join(
                pathComponents[index:]))
    else:
        raise ValueError('Supplied path - %s - is not a valid component path. Does not include a stages/ dir' % path)

    return component


def ReplicaFromFilename(datafile):
    '''Returns the replica number of the component containing an input file

    Parameter:
        The path to a file in a component

    Returns:
        A tuple giving the concentrations of the molecules'''

    return ReplicaFromComponentName(ComponentNameFromPath(datafile))


def ReplicaFromComponentName(componentName):

    '''Given the name of a component returns it replica index

    Parameters:
        componentName: The name of a replicated experiment component

    Raises ValueError if no replica index can be determined'''

    exp = re.compile("[A-Za-z_]+([0-9]+)")
    m = exp.match(componentName)
    if m is None:
        raise ValueError('Cannot determine replica from component name: %s' % componentName)
    else:
        simIndex = int(m.group(1))

    return simIndex

def CheckFiles(dataReference):

    references = dataReference.paths

    #Check the references
    issues = []
    for ref in references:
        if not os.path.exists(ref):
            issues.append([dataReference, ref])

    if len(issues) != 0:        
        raise experiment.model.errors.DataReferenceFilesDoNotExistError(issues)

    return True


def StageReference(dataReference,  # type: experiment.model.graph.DataReference
                   location,  # type: experiment.model.storage.WorkingDirectory
                   graph,  # type: experiment.model.graph.WorkflowGraph
                   ):

    '''Stages the data referred to by a DataReference object
    
    This function stages the references based on the method specified on the DataReference
    objects creation. No data is staged unless all references exist.

    Know methods:
        copy/copyout - Copies reference files/directory to location
        link - Links references file/directory to location
        extract - Extracts references file (assumed to be an archive) to location
    
    Parameter:
        dataReference - An experiment.model.graph.DataReference object
        location - A storage.WorkingDirectory object detailing where the references
            will be staged to (only relevant for copy/link).
        graph - An experiment.model.graph.WorkflowGraph object use to resolve dataReference
    
    Exceptions:
        Raises DataReferenceFilesDoNotExistError if a file/directory specified by reference(s) do not exist.
        This exception contains a list of the refs that were detected not to exist.
        Note: More than this number may be present

        Raises DataReferenceCouldNotStageError if there is a problem when staging i.e while linkin/copying/extracting.
        This error could indicate a bug in the package reference/producer output or be filesystem related.
        Note: The staging ends the first time this problem is encountered'''

    reference = dataReference.resolve(graph)

    #Check the references
    issues = []

    path_methods = experiment.model.graph.DataReference.pathMethods
    if dataReference.method not in [experiment.model.graph.DataReference.LoopRef, experiment.model.graph.DataReference.LoopOutput]:
        if dataReference.method in path_methods and os.path.exists(reference) is False:
            issues.append([dataReference, reference])
    elif dataReference.method == experiment.model.graph.DataReference.LoopRef:
        # VV: AggregatedRef contains MULTIPLE dirs/files joined by a space character
        for individual_reference in reference.split(' '):
            if os.path.exists(individual_reference) is False:
                issues.append([dataReference, individual_reference])

    if len(issues) != 0:
        raise experiment.model.errors.DataReferenceFilesDoNotExistError(issues)

    try:
        if dataReference.method in [experiment.model.graph.DataReference.Copy, experiment.model.graph.DataReference.CopyOut]:
            dest = location.path
            if os.path.isdir(reference):
                destName = os.path.split(reference)[1]
                dest = os.path.join(dest, destName)
                shutil.copytree(reference, dest, symlinks=True)
            else:
                shutil.copy(reference, dest)
        elif dataReference.method == experiment.model.graph.DataReference.Link:
            name = os.path.split(reference)[1]
            dest = os.path.join(location.path, name)
            os.symlink(reference, dest)
        elif dataReference.method == experiment.model.graph.DataReference.Extract:
            archive = reference
            dest = location.path

            with tarfile.open(archive) as tar:
                #Check the contents will all be extract under location
                #Add / to dest to avoid commonprefix issue where /usr/var matches /usr/var2
                #(due to charactwise matching performed)
                target = os.path.join(os.path.realpath(dest), '')
                for f in tar.getmembers():
                    newPath = os.path.join(location.path, f.name)
                    #if target includes / then commonprefix will include it
                    if os.path.commonprefix([target, newPath]) != target:
                        raise tarfile.ReadError('Archive contains files that would be extracted outside of destination')

                tar.extractall(dest)
                tar.close()
    except (shutil.Error, tarfile.ReadError, OSError) as stageError:
        raise experiment.model.errors.DataReferenceCouldNotStageError(dataReference, stageError)


class FileReferenceInfo:
    def __init__(self, producer_id: str, path_rel: str, path_abs: str, filename: str, ref_uid: str):
        self.producer_id = producer_id
        self.path_rel = path_rel
        self.filename = filename if filename is not None else '.'
        self.path_abs = path_abs
        self.ref_uid = os.path.normpath(ref_uid)
        # VV: Need to generate appropriate hashes for building SET containing FileReferenceInfo
        self._hash = hash(frozenset(self.to_dict().items()))

    def to_dict(self):
        return {'producer_id': self.producer_id, 'path_rel': self.path_rel,
                'ref_uid': self.ref_uid, 'filename': self.filename}

    def __repr__(self):
        return str(self.to_dict())

    def __eq__(self, other: FileReferenceInfo):
        return self.to_dict() == other.to_dict()

    def __hash__(self):
        return self._hash


class DataReferenceInfo:
    def __init__(self, str_reference: str, instance_uri: str, consumer_stage: int, consumer_cmd_args: str,
                 app_deps: List[str]=None):
        self._str_ref = str_reference
        self._consumer_stage = consumer_stage
        self._consumer_cmd_args = consumer_cmd_args
        self._app_deps: List[str] = list(app_deps or [])
        self._instance_uri = instance_uri
        self._pid: Optional[experiment.model.graph.ComponentIdentifier] = None
        self.method = ""

        # VV: One DataReferenceInfo can point to multiple files - several instances of it in the cmd-args
        self.files: Set[FileReferenceInfo] = set()

        self.log = logging.getLogger(self._str_ref)

        self._parse()

    @property
    def pid(self) -> Optional[experiment.model.graph.ComponentIdentifier]:
        if self._pid is None:
            return None
        return experiment.model.graph.ComponentIdentifier(self._pid.componentName, self._pid.stageIndex)

    @classmethod
    def producer_id_to_absolute_path(cls, pid: experiment.model.graph.ComponentIdentifier, instance_uri: str) -> str:
        _, instance_location = experiment.model.storage.partition_uri(instance_uri)
        return os.path.join(instance_location, 'stages', pid.namespace, pid.componentName)

    @classmethod
    def producer_id_to_instance_relative_path(cls, pid: experiment.model.graph.ComponentIdentifier, instance_uri: str) -> str:
        instance_name = os.path.basename(instance_uri)
        return os.path.join(instance_name, 'stages', pid.namespace, pid.componentName)

    def _parse(self):
        ref = self._str_ref

        ref_stage, ref_name, ref_filename, method = experiment.model.frontends.flowir.FlowIR.ParseDataReferenceFull(
            ref, self._consumer_stage, application_dependencies=self._app_deps)
        filename = ref_filename
        self.method = method

        _, instance_location = experiment.model.storage.partition_uri(self._instance_uri)
        instance_name = os.path.basename(instance_location)
        pid = None

        if ref_stage is None:
            # VV: Reference to something that's not a Node of the workflow graph (input, data, app_dep, etc)
            if ref_name.startswith('/') is True:
                raise ValueError(f"Unsupported DataReference to absolute path", self._str_ref)

            # VV: This is a reference to data/input/app_dep, etc (e.g. data/pag_data.csv:ref)
            # ref_name example: `data/pag_data.csv`
            ref_uid = os.path.normpath(ref.rsplit(':')[0])
            path_rel = os.path.join(instance_name, ref_uid)
            path_abs = os.path.join(instance_location, ref_uid)

            producerid = ref_name.split('/', 1)[0]

            if '/' in ref_name:
                filename = os.path.basename(ref_name)
            else:
                filename = ''
        else:
            # VV: Reference to a Node of the workflow graph
            pid = experiment.model.graph.ComponentIdentifier(ref_name, ref_stage)
            producerid = pid.identifier

            path_rel = self.producer_id_to_instance_relative_path(pid, self._instance_uri)
            path_abs = self.producer_id_to_absolute_path(pid, self._instance_uri)

            if not filename and method == experiment.model.graph.DataReference.Output:
                filename = 'out.stdout'

            if filename and filename != '.':
                path_rel = os.path.join(path_rel, filename)
                path_abs = os.path.join(path_abs, filename)

            ref_uid = os.path.normpath(os.path.join(producerid, filename or '.'))

        filenameNotInArgsDataReferenceMethod = [
            experiment.model.graph.DataReference.Copy,
            experiment.model.graph.DataReference.CopyOut,
            experiment.model.graph.DataReference.Link,
            experiment.model.graph.DataReference.Extract,
            experiment.model.graph.DataReference.Output,
        ]

        self._pid = pid

        self.files.add(FileReferenceInfo(producerid, path_rel, path_abs, filename, ref_uid))
        if method in filenameNotInArgsDataReferenceMethod:
            return

        # VV: Need to look for all instances of the DataReference in the cmd args and then look for anything past
        # the reference string interpretation that looks like a filename

        if pid:
            # VV: If this was a reference to a component craft a regular expression that contains both relative and
            # absolute component references
            s = rf"({pid.namespace}\.)"
            if pid.stageIndex == self._consumer_stage:
                # VV: supports both absolute and relative ref - this is valid because both the producer and the
                # consumer are in the same stage
                s += r"?"
            s = s + f"{pid.componentName}"

            if ref_filename:
                s = os.path.join(s, ref_filename)
            s = ':'.join((s, method))
        else:
            # VV: This is your non-component DataReference, use the producerid as is
            # use a group here so that the 2nd group is always the matched file ...
            s = rf"({ref})"
        s += r"(([/]*([\w:.]*))*)"

        s = re.compile(s)

        for match in s.finditer(self._consumer_cmd_args):
            matched = match.groups()[1]
            if matched.startswith('/'):
                matched = matched[1:]

            if matched.endswith('*'):
                self.log.log(15, f"Pretending that {match.group()} does not match a glob expansion ending with an '*' "
                                 f"character:{matched} - trimming the *")
                matched = matched[:-1]

            # self.log.info(f"Found a match: {matched} in cmd args: {self._consumer_cmd_args}")
            if matched.endswith(';'):
                # VV: This character can sometimes follow data-references in bash cmdlines
                self.log.log(15, f"Removing suffix ; from {matched}")
                matched = matched[:-1]

            if matched.endswith("""')"""):
                # VV: This suffix is sometimes met in cmdlines of the python interpreter
                self.log.log(15, f"Removing suffix ') from {matched}")
                matched = matched[:-2]

            if matched.endswith('''")'''):
                # VV: This suffix is sometimes met in cmdlines of the python interpreter
                self.log.log(15, f"Removing suffix \") from {matched}")
                matched = matched[:-2]

            m_path_rel = os.path.join(path_rel, matched)
            m_path_abs = os.path.join(path_abs, matched)
            m_ref_uid = os.path.join(ref_uid, matched)

            # VV: Throw away leading '.' or '/' from the constructed paths

            m_path_abs = os.path.normpath(m_path_abs)
            m_path_rel = os.path.normpath(m_path_rel)
            m_ref_uid = os.path.normpath(m_ref_uid)

            filename = os.path.basename(m_path_abs)
            self.files.add(FileReferenceInfo(producerid, m_path_rel, m_path_abs, filename, m_ref_uid))


class Status:

    '''Instance of this class record the experiments state

    Instances are backed onto a file status.txt in the experiments output/ directory.
    Update of this file is handled by external monitors via this class'''

    defaults = {
        'stages': [],
        'current-stage': None,
        'stage-progress': 0,
        'total-progress': 0,
        'experiment-state': 'Initialising',
        'stage-state': 'Initialising',
        # VV: updated is deprecated and will be removed.
        # its timestamp format is %Y-%m-%dT%H%M%S.%f
        'updated': datetime.datetime.now(),
        'exit-status': 'N/A',
        'cost': 0,
        'created-on': 'N/A',
        'updated-on': 'N/A',
        'completed-on': 'N/A',
    }

    FORMAT_TIMESTAMP_ON_FIELDS = "%Y-%m-%dT%H:%M:%S.%f%z"

    @classmethod
    def statusFromFile(cls, filename):
        '''Can raise IOError if there is a problem reading the status file'''

        import ast

        with open(filename, 'r') as f:
            fileData = f.read()

        fileData = fileData.split('\n')
        # Tuple conversion just to supress an error detection ...
        fileData = dict(tuple([el.split('=', 1) for el in fileData if el.find('=') != -1]))

        # VV: here we are reversing the encodings we performed in self.writeToStream
        if 'error-description' in fileData:
            fileData['error-description'] = fileData['error-description'].encode('utf-8').decode('unicode_escape')

        # FIXME: StageWeights need to be written to file??
        # Or set by StatusMonitor on restart??
        return Status(filename, fileData, ast.literal_eval(fileData['stages']))

    def __init__(self, filename, data, stages):

        self.data = {}

        self.outputFile = filename
        self.outputDir = os.path.split(filename)[0]
        self.log = logging.getLogger("output.statusfile")

        self.data.update(Status.defaults)

        for key in list(data.keys()):
            self.data[key.strip().lower()] = data[key].strip()

        self.data['stages'] = stages

        # Getters

    def to_dict(self):
        # type: () -> Dict[str, str]
        """Returns a dictionary representation of the data where keys and values are strings.

        Values which are "datetime" instances are formatted using the string "%d%m%y-%H%M%S.%f"
        """
        ret = {}

        timestamp_formats = {'updated': "%Y-%m-%dT%H%M%S.%f"}
        for x, v in self.data.items():
            if not isinstance(v, datetime.datetime):
                v = '%s' % v
            else:
                v = v.strftime(timestamp_formats.get(x, Status.FORMAT_TIMESTAMP_ON_FIELDS))
            ret['%s' % x] = v
        return ret

    def stages(self):

        return self.data['stages']

    def currentStage(self):

        return self.data['current-stage']

    def totalProgress(self):

        return self.data['total-progress']

    def stageProgress(self):

        return self.data['stage-progress']

    def experimentState(self):

        '''Can be initialising, running, suspended, finished or resource-wait'''

        return self.data['experiment-state']

    def stageState(self):

        '''Can be initialising, running, suspended, finished or resource-wait'''

        return self.data['stage-state']

    def created(self) -> Union[str, datetime.datetime]:
        return self.data['created-on']

    def updated(self) -> Union[str, datetime.datetime]:
        return self.data['updated-on']

    def completed(self) -> Union[str, datetime.datetime]:
        return self.data['completed-on']

    def cost(self):

        return self.data['cost']

    # Setters

    def setTotalProgress(self, value):

        self.data['total-progress'] = value

    def setCurrentStage(self, stage):

        if stage not in self.data['stages']:
            raise ValueError('Unknown stage given %s (%s)' % (stage, self.data['stages']))

        self.data['current-stage'] = stage

    def setStageProgress(self, progress):

        self.data['stage-progress'] = progress
        # FIXME: total progress won't get updated in dict until this is called
        self.totalProgress()

    def setStageState(self, state):

        state = state.lower()

        if state not in experiment.model.codes.states:
            raise ValueError('Unknown state %s' % state)

        self.data['stage-state'] = state

    def setExperimentState(self, state):

        state = state.lower()

        if state not in experiment.model.codes.states:
            raise ValueError('Unknown state %s' % state)

        self.data['experiment-state'] = state

    def setCreated(self, value: Union[datetime.datetime, str]):
        self.data['created-on'] = value

    def setUpdated(self, value):
        self.data['updated'] = value
        self.data['updated-on'] = value

    def setCompleted(self, value: Union[datetime.datetime, str]):
        self.data['completed-on'] = value

    def setCost(self, cost):

        self.data['cost'] = cost

    def setExitStatus(self, stat):

        self.data['exit-status'] = stat

    def removeErrorDescription(self):
        """Removes the error description from status.

        This is useful when restarting a previously failed experiment.
        The new instance of the experiment should start with a clean slate.
        """

        if 'error-description' in self.data:
            del self.data['error-description']

    def setErrorDescription(self, desc: str):
        self.data['error-description'] = desc

    def writeToStream(self, stream):
        """Persists data to stream

        We want to have 1 line per "key" however some keys may contain newline characters (e.g. error-description).
        For these keys, we first escape characters (e.g. new lines) to get a bytes array and then we
        decode that into utf-8. This converts the string "hello\nworld" to the string "hello\\nworld"

        Args:
            stream: IO stream to store output
        """
        new_data = {x: self.data[x] for x in self.data}
        # VV: See method docstring
        if 'error-description' in self.data:
            self.data['error-description'] = self.data['error-description'].encode('unicode_escape').decode('utf-8')
        for key in sorted(new_data):
            stream.write("%s=%s\n" % (key, self.data[key]))

    # Output
    def update(self):

        '''Note: If update fails an error is not raised'''

        retval = True
        self.setUpdated(datetime.datetime.now())

        tempname = str(uuid.uuid4())
        tempname = os.path.join(self.outputDir, tempname)
        # FIXME: Accessing dir directly means various update code won't be called
        # However we are using the dict keys to know what attributed have been set ...
        try:
            with open(tempname, 'w+') as f:
                self.writeToStream(f)
        except IOError as error:
            self.log.warning("Error updating status file (could not write temp file)\n%s" % error)
            self.log.warning("Update failed")
            retval = False
        else:
            try:
                os.rename(tempname, self.outputFile)
            except OSError as error:
                self.log.warning("Error updating status file (cannot rename)\n%s" % error)
                self.log.warning("[STATUS MONITOR] Update failed")
                retval = False

        return retval

    def persistentUpdate(self, timeout=600):

        '''As update but will keep trying to write file if update fails

        Note: Does not raise exception on failure

        Parameters:
            timeout - How long to attempt to write the status file'''

        count = 0
        while count * 30 < timeout:
            # Update raises no exceptions
            if self.update():
                self.log.info("Persistent update succeeded after %d attempts" % count)
                break
            else:
                self.log.warning("Update failed will try in 30s (%d attempts so far)" % count)
                time.sleep(30)
                count += 1


def split_path_to_source_path_and_target_name(path: str) -> Tuple[str, str | None]:
    """Splits a path $sourcePath[:$targetPath] into sourcePath, targetName

    You can escape the `:` character as `\:` (and \ as \\)
    """
    # VV: 1st group is $sourcePath, 2nd group is the character after the last `\:`, 3rd group is targetName
    reg = re.compile(r"(([^:]|\\:)+[^\\]):(.+)")
    match = reg.match(path)

    if not match:
        rename_to = None
    else:
        groups = match.groups()
        path = groups[0]
        rename_to = groups[2]

    path = path.replace("\\:", ":").replace("\\\\", "\\")

    return path, rename_to


class Experiment:

    '''Class representing a computational experiment.

    Instances of this class correspond to a directory on disk.
    Later may try to abstract this.

    Note: Currently this class is a singleton - only one instance per application

    Attributes:
        An ordered list of the stages in the experiment.
        The experiment status.
        The experiment output.
        The experiment meta-data'''

    ReservedDocumentDescriptorKeys = ['instance', 'type', ]

    @classmethod
    def _populateApplicationSources(
            cls,
            application_dependencies,  # type: List[str]
            packagePath,
            instanceDirectory,
            ignore_existing=False,
            custom_application_sources=None
    ):
        """Creates links in the experiment instance to application packages specified in the experiments conf. file

        Private class method

        Application packages provided shared data and executables to the experiment instance.
        The ones to use are listed in the experiment.model.conf file
        
        Parameters:
            packagePath: Path to the experiment package being instantiated
                The directory this is in will be checked for application packages
            instanceDirectory: ExperimentInstanceDirectory object representing directory the app links will be created
                in
            ignore_existing(bool): If set to True will not raise an exception when the application dependency name
                conflicts with an existing path inside the root directory of the workflow instance
            custom_application_sources(Dict[str, str]): Overrides source of application dependencies, format is
               {"<application_dependency_entry_name>": "/absolute/path[:link/copy]"}
               The :link and :copy suffixes determine whether to link or copy the path under the instance directory.
               The suffix is optional and defaults to :link. If the path contains a ':' character,
               use '%3A' instead (i.e. url-encode the : character)"
        """

        custom_application_sources = custom_application_sources or {}

        log = logging.getLogger("data.experiment")

        if application_dependencies:
            packageLocations = []
            if "APPLICATION_PACKAGE_DIR" in os.environ:
                packageLocations.append(os.environ["APPLICATION_PACKAGE_DIR"])
            
            #If a path contains a trailing / split will create a pair of [path, ""]!
            packagePath = os.path.abspath(packagePath)
            if packagePath[-1] == os.path.sep:
                packagePath = packagePath[:-1]

            packageLocations.append(os.path.split(packagePath)[0])

            def firstExisting(locations):

                retval = None

                for location in locations:
                    if os.path.exists(location):
                        retval = location 

                return retval

            for package in application_dependencies:
                # VV: An applicationDependencySource may be created as either a link or a copy of the pointed path
                # The default is a link
                make_link = True
                if package in custom_application_sources:
                    # VV: Users have specified the source location of the application dependency in the
                    # commandline of elaunch.py
                    # this will override application dependencies even if they're defined as absolute paths!
                    # determine package folder name using the information in FlowIR
                    path = custom_application_sources[package]

                    try:
                        path, method = path.split(':', 1)
                    except ValueError:
                        pass
                    else:
                        if method == 'copy':
                            make_link = False
                        elif method != 'link':
                            raise experiment.model.errors.ExperimentInvalidConfigurationError(
                                f"Invalid destination method, given for applicationDependencySource "
                                f"{package}={custom_application_sources[package]}. Valid methods are :link and :copy "
                                f"(:link if omitted)")
                    # VV: Finaly, expand decode '%3A' to ':' to handle paths that contain the ':` character
                    path = path.replace('%3A', ':')
                    path = firstExisting([path])
                    package_folder = os.path.split(package)[1]
                    search_locations = [os.path.split(package)[0]]
                elif package.startswith(os.path.sep) is False:
                    path = firstExisting([os.path.join(el, package) for el in packageLocations])
                    package_folder = package
                    search_locations = packageLocations
                else:
                    # VV: This is an absolute path to a dependency
                    path = firstExisting([package])
                    package_folder = os.path.split(package)[1]
                    search_locations = [os.path.split(package)[0]]

                packageName = experiment.model.frontends.flowir.FlowIR.application_dependency_to_name(package_folder)
                path_dest = os.path.join(instanceDirectory.location, packageName)

                if packageName is None:
                    raise experiment.model.errors.ExperimentInvalidConfigurationError(
                        "Invalid name, %s, given for application package in conf file" % package)

                if os.path.isdir(path_dest):
                    log.warning("Will not create application link for %s because link already exists" % package)
                    continue

                if path is None:
                    raise experiment.model.errors.PackageLocationError("Cannot find application package %s in given locations: %s" % (
                        package_folder, ",".join(search_locations)))

                try:
                    if ignore_existing and os.path.isdir(path_dest):
                        log.info("Skipping creating application package %s->%s because it already exists" % (
                            path, path_dest))
                    elif make_link:
                        os.symlink(path, path_dest)
                    else:
                        try:
                            shutil.copytree(path, path_dest)
                        except OSError as data:
                            if data.errno != errno.EEXIST:
                                raise
                except Exception as error:
                    raise_with_traceback(experiment.model.errors.FilesystemInconsistencyError(
                        "Could not create %s to application package %s at %s" % (
                            "link" if make_link else "copy", path, path_dest), error))
                else:
                    log.info("Created application link %s to %s" % (packageName, path))
        else:   
            log.log(19, "Experiment uses no application packages")

    @classmethod
    def _createVirtualEnvLinks(
            cls,
            virtual_environments,  # type: List[str]
            packagePath,
            instanceDirectory,
            platform,
            ignore_existing=False,
    ):

        '''Private class method
        
        Creates links in the experiment instance to python virtual environments specified conf. file

        Parameters:
            packagePath: Path to the experiment package being instantiated
                The directory this is in will be checked for application packages
            instanceDirectory: ExperimentInstanceDirectory object representing directory the app links will be created in
            platform: The platform to create the configuration for'''

        log = logging.getLogger("data.experiment")

        if bool(virtual_environments) is False:
            log.log(19, "Experiment uses no virtual environments")
            return virtual_environments

        log.info("Conf contains virtual env definitions - %s" % ",".join(virtual_environments))

        #Remove trailing slashes - this is to allow comparison of these paths to VIRTUAL_ENV env var if set
        for i in range(len(virtual_environments)):
            if virtual_environments[i][-1] == os.path.sep:
                virtual_environments[i] = virtual_environments[i][:-1]

        for venvPath in virtual_environments:
            if os.path.exists(venvPath) and os.path.isdir(venvPath):
                venvName = os.path.split(venvPath)[1]
                linkName = os.path.join(instanceDirectory.location, venvName)
                try:
                    if ignore_existing and os.path.isdir(linkName):
                        log.warning("Ignore linking to virtual env %s->%s because it already exists" % (
                            venvPath, linkName
                        ))
                    else:
                        os.symlink(venvPath, linkName)
                except Exception as error:
                    log.warning("Could not create link to virtual env %s at %s" % (venvPath, linkName))
                    raise experiment.model.errors.FilesystemInconsistencyError(
                        "Could not create link to virtual env %s at %s" % (venvPath, linkName),
                        error)
                else:
                    log.info("Created virtual-env link %s to %s" % (linkName, venvPath))
            else:
                log.warning("Specified virtualenv %s does not exist (or is not a directory)" % venvPath)

        return virtual_environments

    @classmethod
    def _dump_cwl_auxiliary_files(cls, new_input_files, new_data_files, package_location):
        def dump_files(files, where):
            paths = {}

            for name in files:
                contents = files[name]
                path = os.path.join(where, name)

                paths[name] = path

                with open(path, 'w') as f:
                    f.write(contents)

            return paths

        new_input_files = {
            '%s.yaml' % key: yaml.safe_dump(new_input_files[key]) for key in new_input_files
        }

        new_data_files = {
            '%s.yaml' % key: yaml.safe_dump(new_data_files[key]) for key in new_data_files
        }

        log = logging.getLogger('CWL-to-FlowIR')

        log.info("Generating auxiliary CWL files under %s" % package_location)
        log.info("Generating input files %s" % list(new_input_files.keys()))
        log.info("Generating data files %s" % list(new_data_files.keys()))

        input_location = os.path.join(package_location, 'input')
        data_location = os.path.join(package_location, 'data')
        data_cwl_location = os.path.join(data_location, 'cwl')

        for path_dir in [input_location, data_location, data_cwl_location]:
            if os.path.exists(path_dir) is False:
                os.makedirs(path_dir)

        dump_files(new_input_files, input_location)
        dump_files(new_data_files, data_cwl_location)

    @classmethod
    def experimentFromPackage(cls,
                              experimentPackage: experiment.model.storage.ExperimentPackage,
                              location: str = os.getcwd(),
                              timestamp: bool = True,
                              platform: Optional[str] = None,
                              inputs: Optional[List[str]] = None,
                              data: Optional[List[str]] = None,
                              variable_files: Optional[Union[str, List[str]]] = None,
                              createApplicationLinks: bool = True,
                              createVirtualEnvLinks: bool = True,
                              format_priority: Optional[List[str]] = None,
                              instance_name: Optional[str] = None,
                              custom_application_sources: Optional[Dict[str, str]] = None,
                              ):

        '''Initialise new experiment based from an experiment.model.storage.ExperimentPackage.


        Parameters:
            experimentPackage: The experiment package to load the graph from.
            location: The path in which to create the experiment data directory
            timestamp: If true the instance directory name will include a timestamp
            platform: The platform to create the experiment instance for
            inputs: List of input files for the new experiment
                 (if path contains : then treat it as $sourceFile:$targetFile)
            data: List of data files for the new experiment - will replace package files of same name in data/ dir
                 (if path contains : then treat it as $sourceFile:$targetFile)
            variable_files: Instance specific variable file(s). If multiple files are provided then they are layered
                starting from the first and working towards the last. This means that the value of a variable that
                 exists in multiple layers will be the one that the last layer defines.
            createApplicationLinks: Process application specifications in the [SANDBOX] configuration section
            createVirtualEnvLinks: Process virtual environment specification in [SANDBOX] configuration section
            format_priority: Prioritized list of formats to use when parsing the configuration (
               see experiment.model.conf.ExperimentConfigurationFactory)
            instance_name: if not none instead of using Path(packagePath).with_suffix('.instance') name will be used
               can be combined with stamp
            custom_application_sources(Dict[str, str]): Overrides source of application dependencies, format is
               {"<application_dependency_entry_name>": "/absolute/path[:link/copy]"}
               The :link and :copy suffixes determine whether to link or copy the path under the instance directory.
               The suffix is optional and defaults to :link. If the path contains a ':' character,
               use '%3A' instead (i.e. url-encode the : character)"

        On return from this method a new directory will exist on disk
        which will contain the experiment data
        
        Exceptions:
            
        Raises errors.PackageLocationError if packagePath is invalid OR any application packages referred to are invalid
        Raises errors.PackageFormatError if the layout of the package is incorrect.
        Raises errors.InstanceLocationError if location is invalid
        Raises errors.InstanceCreateError if the instance data directory can't be created
        Raises errors.ExperimentSetupError if instance data can't be copied to instance dir
        Raises errors.ExperimentMissingConfigurationError if a required configuration file does not exist
        Raises errors.ExperimentInvalidConfigurationError if a configuration file is invalid.
        Raises errors.FilesystemInconsistencyError if a problem with the file-system is detected
                    after the experiments instance directory has been successfully created
        Raises errors.UndefinedPlatformError if no configuration files match the given platform.
        Raises errors.PackageStructureError if the structure of the package is invalid.

        On any exception if the instance dir has been created it is deleted'''

        log = logging.getLogger("data.experiment")

        inputs = inputs or []
        data = data or []

        #The ExperimentInstanceDirectory class encapsulates details about the
        #structure of the instance directory and the names of files
        #It will check the package at packagePath is valid
        instanceDirectory=None
        try:
            instanceDirectory = experiment.model.storage.ExperimentInstanceDirectory.newInstanceDirectory(
                location, experimentPackage, stamp=timestamp, name=instance_name)
        except experiment.model.errors.InstanceCreateError as error:
            log.warning('Error when creating instance dir')
            raise

        try:
            #Copy in any instance specific inputs
            #TODO: Perhaps move into newInstanceDirectory/ExperimentInstanceDirectory __init__
            if inputs:
                log.info('Copying specified input files to experiment')

            for path in inputs:
                try:
                    src_path, rename_to = split_path_to_source_path_and_target_name(path)

                    instanceDirectory.addInputFile(src_path, rename_to=rename_to)
                    log.info("Copied %s" % path)
                except (IOError, OSError) as underlyingError:
                    log.info("Failed copying %s" % path)
                    raise experiment.model.errors.ExperimentSetupError('Unable to copy input files', underlyingError,
                                                                       instance_dir=instanceDirectory)

            for path in data:
                log.info('Replacing specified data files')
                try:
                    src_path, rename_to = split_path_to_source_path_and_target_name(path)
                    instanceDirectory.updateDataFile(src_path, rename_to)
                    log.info("Updated data directory with %s" % path)
                except (IOError, OSError) as underlyingError:
                    log.info("Failed updating data directory with %s" % path)
                    raise experiment.model.errors.ExperimentSetupError('Unable to update data files', underlyingError,
                                                                       instance_dir=instanceDirectory)
            instance_variable_files = []
            if variable_files is not None:
                # VV: Aggregate all variable files from any variables-files schema, and store them in FlowIR schema
                # under $INSTANCE_DIR/input/variables.yaml
                if isinstance(variable_files, string_types):
                    variable_files = [variable_files]

                try:
                    user_vars = experiment.model.conf.FlowIRExperimentConfiguration.layer_many_variable_files(variable_files)
                except experiment.model.errors.FlowIRConfigurationErrors as e:
                    raise_with_traceback(
                        experiment.model.errors.ExperimentSetupError("Unable to read user variables file(s)",
                                                                     e, instanceDirectory))
                    raise # VV: keep linter happy

                log.info('Adding instance specific variables from %s' % variable_files)
                try:
                    path_agg_variables = os.path.join(instanceDirectory.inputDir, 'variables.yaml')
                    with open(path_agg_variables, 'w') as f:
                        experiment.model.frontends.flowir.yaml_dump(user_vars, f)
                    instance_variable_files.append(path_agg_variables)
                except (IOError, OSError) as underlyingError:
                    log.info("Failed updating data directory with %s" % variable_files)
                    raise experiment.model.errors.ExperimentSetupError('Unable to update data files', underlyingError,
                                                                       instance_dir=instanceDirectory)

            #TODO: The below method of providing executables to packages is limited and will be superseded.
            #It doesn't work well when the application needs to use tools that are naturally installed somewhere else
            #for example python scripts in a module.
            #The reason they exist is that we'd like the application to be self contained/sand-boxed
            #so all executables used should exist in the sandbox
            #A new method would involve a manifest that would copy/link executables into the sandbox at this point

            #Create links to application packages
            ##NOTE: This will raise errors.PackageLocationError if an application package can't be found
            confexp = experimentPackage.configuration

            confexp.parametrize(
                platform=platform, systemvars={},
                createInstanceFiles=False, primitive=True, variable_files=instance_variable_files,
                is_instance=False, manifest=experimentPackage.manifestData,
            )

            concrete = confexp.get_flowir_concrete(return_copy=False)

            if isinstance(confexp, experiment.model.conf.CWLExperimentConfiguration):
                # VV: This is an imported CWL rename the 'conf/cwl' directory to 'conf/archived-cwl' and store the
                #     automatically generated input/data files in the appropriate
                instance_location = instanceDirectory.location

                cwl_cwl = os.path.join(instance_location, 'conf', 'cwl')
                archived_cwl = os.path.join(instance_location, 'conf', 'archived-cwl')
                log.info('Archiving CWL package %s to %s' % (cwl_cwl, archived_cwl))
                if os.path.exists(archived_cwl):
                    shutil.rmtree(archived_cwl)
                os.rename(cwl_cwl, archived_cwl)
                confexp = cast(experiment.model.conf.CWLExperimentConfiguration, confexp)

                cls._dump_cwl_auxiliary_files(
                    confexp.new_input_files, confexp.new_data_files, instanceDirectory.location
                )
                # VV:Finally, store the FlowIR description of the package so that we don't go through CWL parsing
                #    again when we load the package later. Theoretically we can place the FlowIR description for
                #    all formats that we support but that might be too radical of a change for now.
                with open('%s/conf/flowir_package.yaml' % instanceDirectory.location, 'w') as f:
                    yaml.safe_dump(concrete.raw(), f)

            if createApplicationLinks:
                application_dependencies = concrete.get_application_dependencies()
                custom_application_sources = (custom_application_sources or {}).copy()

                unknown_sources = {x: custom_application_sources[x]
                                   for x in set(custom_application_sources).difference(application_dependencies)}

                if unknown_sources:
                    msg = "Unknown application dependencies %s" % unknown_sources
                    raise experiment.model.errors.ExperimentSetupError(
                        desc=msg, underlyingError=ValueError(msg), instance_dir=instanceDirectory)

                Experiment._populateApplicationSources(application_dependencies, experimentPackage.location, instanceDirectory,
                                                       custom_application_sources=custom_application_sources)

            #FIXME: This is a hack for python scripts needed by the application which have been installed in
            #standard python manner i.e. as a module
            #We create a link in the experiment to virtualenv specified in the configuration
            #Furthermore if VIRTUAL_ENV is set and the env was not in the configuration we add a link called "python" to it
            #If VIRTUAL_ENV is not set and no virtual environments were specified we create a link to the python ".local" directory
            if createVirtualEnvLinks:
                virtual_environments = concrete.get_virtual_environments()
                venvs = Experiment._createVirtualEnvLinks(virtual_environments, experimentPackage.location, instanceDirectory, platform)
            else:
                venvs = []

            if "VIRTUAL_ENV" in os.environ or 'CONDA_PREFIX' in os.environ:
                # VV: It's possible that a package contains a virtual-environment that points to
                #     `os.environ["VIRTUAL_ENV"]`. Instead of comparing virtual-environment paths,
                #     compare virtual-environment IDs
                venv_ids = list(map(experiment.model.frontends.flowir.FlowIR.extract_id_of_folder, venvs))
                if "python" not in venv_ids:
                    log.info("Detected use of virtual env - will link in python bin directory")
                    linkName = os.path.join(instanceDirectory.location, "python")
                    virtual_environment_source = os.environ.get("VIRTUAL_ENV", os.environ.get('CONDA_PREFIX'))
                    if not os.path.exists(linkName):
                        try:
                            os.symlink(virtual_environment_source, linkName)
                        except Exception as error:
                            raise experiment.model.errors.FilesystemInconsistencyError(
                                "Could not create link to python virtualenv %s at %s" % (
                                    virtual_environment_source, linkName), error)
                        else:
                            log.info("Created python link %s to %s" % (linkName, virtual_environment_source))
                    else:
                        log.info('VirtualEnv is sandboxed in experiment - no need to link')
            elif os.path.exists(os.path.join(os.path.expanduser("~"), '.local')) and len(venvs) == 0:
                log.info("Will link in ~/.local as python directory")
                path = os.path.join(os.path.expanduser("~"), '.local')
                linkName = os.path.join(instanceDirectory.location, "python")
                try:
                    os.symlink(path, linkName)
                except Exception as error:
                    raise experiment.model.errors.FilesystemInconsistencyError(
                        "Could not create link to application package %s at %s" % (path, linkName),
                        error)
                else:
                    log.info("Created python link %s to %s" % (linkName, path))

            e = Experiment(instanceDirectory, platform=platform, format_priority=format_priority
                           , is_instance=False, updateInstanceConfiguration=True)
        except Exception as error:
            log.warning('Exception during Experiment creation. Error was: %s' % error)
            if instanceDirectory is not None:
                try:
                    instanceDirectory.consolidate()
                    shutil.rmtree(instanceDirectory.location)
                except Exception as removalError:
                    log.warning(
                        'Unable to clean up instance dir after error during creation. Reason: %s' % removalError)
            raise_with_traceback(error)

        return e

    @classmethod
    def experimentFromInstance(cls, directory,
                               platform=None,
                               configPatches=None,
                               updateInstanceConfiguration=True,
                               format_priority=None,  # type: Optional[List[str]]
                               attempt_shadowdir_repair=True,
                               ):

        '''Returns an object representing the experiment instance at directory

        Args:
            format_priority: Prioritized list of formats to use when parsing the configuration (
               see experiment.model.conf.ExperimentConfigurationFactory)
        '''
        configPatches = configPatches or {}
        instanceDir = experiment.model.storage.ExperimentInstanceDirectory(directory,
                                                                           attempt_shadowdir_repair=attempt_shadowdir_repair)
        return cls(instanceDir,
                   platform=platform,
                   configPatches=configPatches,
                   updateInstanceConfiguration=updateInstanceConfiguration,
                   format_priority=format_priority,
                   is_instance=True,)

    def get_flow_option(self, option_name):
        concrete = self.experimentGraph._concrete

        # VV: FIXME Figure out a proper way to place the `reservation` variable
        concrete.get_global_variable(option_name)

    def __init__(self,
                 instanceDirectory: experiment.model.storage.ExperimentInstanceDirectory,
                 platform: str | None =None,
                 configPatches: Dict[int, List[str]] | None = None,
                 updateInstanceConfiguration: bool = True,
                 is_instance: bool | None = None,
                 format_priority: List[str] | None=None,
                 ):

        """Returns an object representing the experiment instance at ExperimentInstanceDirectory

        NB: The Experiment object returned will be based on the configuration defined in the instanceDirectory
        at the time this method is called.
        Subsequent changes to the configuration of components, including via the returned objects 
        configuration ivar (an ExperimentConfiguration object) will not have any impact.
        Only environment variables can be modified via the configuration post-instantiation.

        Note: The experiments configuration/workflow graph is not validated on instantiation.
        To do this call validate() on the returned object.

        The reason for this is because an Experiment object can be functional for a purpose without
        passing all possible validation tests.
        Decoupling the validation from instantiation allows the user to decide what tests
        need to be passed to ensure the returned object is suitable for a particular purpose.
        For example an Experiment object can be used to explore the data in an instance without
        having all the component executable paths present.

        At a later stage these difference usages of an Experiment may be separated into different classes.
        For example an experiment could be composed of a Graph, a Storage, a Sandbox and an Environment
        - Storage+Graph allows exploring data without checking environment details
        - Storage+Graph+Sandbox includes incorporating executables into the experiment and related checks
        - Storage+Graph+Sandbox+Environment includes a platform environment and related checks.

        Args:
            instanceDirectory: The instance directory
            configPatches: A dictionary of stageIndex/list of filenames pairs. It allows dynamic modification of the
                experiment configuration. See documentation for configPatches parameter of
                FlowIRExperimentConfiguration class for more.
            updateInstanceConfiguration: If True instance specific files are updated with current options passed
                e.g. platform. If they don't exist they are created
            is_instance: If True the experiment will load an Instance configuration, otherwise it will
                load a Package configuration. If is_instance is not provided the experiment will prioritize
                loading an Instance if the `experiment.instance.conf` file does not exist it will fall-back to loading
                a Package configuration.
            format_priority: Prioritized list of formats to use when parsing the configuration (
               see experiment.model.conf.ExperimentConfigurationFactory)

        Exceptions:
            
        Raises errors.ExperimentMissingConfigurationError if a required configuration file does not exist
        Raises errors.FilesystemInconsistencyError if a problem with the file-system is detected
        after the experiments instance directory has been successfully created

        Instance Configuration Files:
            The default behaviour of Experiment object is to create new instance configuration files on each call
            ignoring anything that exists.

            This behaviour can be changed via the is_instance and updateInstanceConfiguration keywords.
        """
        configPatches = configPatches or {}
        self.instanceDirectory = instanceDirectory  # type: experiment.model.storage.ExperimentInstanceDirectory
        self.log = logging.getLogger("data.experiment")

        # VV: When interface.inputSpec.extractionMethod.hooks (FlowIR) is set to True, runtime can invoke the
        # get_input_ids() hook in interface.py to extract the IDs of the input systems that this experiment processes.
        self._input_ids: List[str] | None = None
        # VV: If the experiment defines interface.inputSpec.additionalInputData=True, this dictionary maps
        # input ids to names of associated input files (assumes that there is no error running the
        # get_additional_input_data() hook)

        # VV: If the experiment defines at least 1 interface.propertiesSpec, then _measured_properties will contain
        # the Joined pandaFrames that represent the measured properties after calling
        # process_interface(validate_only=False, do_input_spec=True/False, do_properties_spec=True)
        # The above assumes that process_interface(validate_only=False, do_input_spec=True,...) has executed before
        # so that the experiment is aware of the input ids it processes
        self._measured_properties: pandas.DataFrame | None = None

        #Add resource specific envars
        systemvars={"INSTANCE_DIR": self.instanceDirectory.location}
        systemvars['FLOW_EXPERIMENT_NAME'] = self.instanceDirectory.name
        systemvars['FLOW_RUN_ID'] = str(uuid.uuid4())

        # VV: If the caller has not explicitly asked for a package/instance decide automatically
        if is_instance is None:
            # VV: If there is an experiment instance load that, otherwise load the Package definition
            is_instance = os.path.exists(self.instanceDirectory.experimentInstanceConfigurationFile)
            self.log.warning("Not explicitly instructed to load a package/instance in %s will load %s" % (
                self.instanceDirectory.location, {True: 'instance', False: 'package'}[is_instance]
            ))
        else:
            self.log.log(14, "Instructed to load a %s" % ('instance' if is_instance else 'package'))
        
        self.experimentGraph = None  # type: experiment.model.graph.WorkflowGraph

        #self.experimentGraph is a complete graph
        workflowGraph = experiment.model.graph.WorkflowGraph.graphFromExperimentInstanceDirectory(
            self.instanceDirectory,
            platform=platform,
            systemvars=systemvars,
            configPatches=configPatches,
            is_instance=is_instance,
            createInstanceConfiguration=updateInstanceConfiguration,
            updateInstanceConfiguration=updateInstanceConfiguration,
            primitive=False,
            format_priority=format_priority
        )
        # self.configuration: experiment.model.conf.FlowIRExperimentConfiguration | None = None
        self.switchWorkflowGraph(workflowGraph)

        #FIXME: Get stage index from config object
        #As it stands Stage objects need access to the Experiment object before it is initialised
        #to get this info (experiment is a parameter to Stage __init__)
        self._stages = []  # type: List[Stage]

        for i in range(self.experimentGraph.numberStageConfigurations):
            comp_names, stage_options = self.experimentGraph.get_stage_description(i)
            stage = Stage(self, i, comp_names, stage_options)
            self._stages.append(stage)

        # VV: Check that all Jobs in stage (i+1) with the 'migrated' job-type have a matching
        #     isMigratable Job in the previous stage.
        #     In the context of migration mathing specifications should share the same name.

        # VV: Contains all isMigratable components of the 'previous' stage
        previous_migratable = set()  # type: Set[Job]

        unmatched_migratable = []  # type: List[job]
        unmatched_migrated = []  # type: List[Job]

        for index, stage in enumerate(self._stages):
            current_migratable = set()  # type: Set[Job]
            for job in stage.jobs():
                if job.isMigratable:
                    current_migratable.add(job)
                if job.isMigrated:
                    matching = [j for j in previous_migratable if j.name == job.name]
                    if len(matching) == 0:
                        self.log.critical('Job %s does not have a matching isMigratable job in previous stage' %
                                          job.name)
                        unmatched_migrated.append(job)
                    elif len(matching) > 1:
                        self.log.critical('Job %s has multiple matching isMigratable jobs in previous stage' %
                                          job.name)
                        unmatched_migrated.append(job)
                    else:
                        previous_migratable.remove(matching[0])
            # VV: Keep track of all isMigratable job which were not matched by a migrated one
            unmatched_migratable += list(previous_migratable)
            previous_migratable = current_migratable

        error_msg = []

        if unmatched_migratable:
            references = [j.reference for j in unmatched_migratable]
            msg = "Experiment contains unmatched migratable components: [%s]" % (
                ', '.join(references)
            )
            error_msg.append(msg)

        if unmatched_migrated:
            references = [j.reference for j in unmatched_migrated]
            msg = "Experiment contains unmatched migrated components: [%s]" % (
                ', '.join(references)
            )
            error_msg.append(msg)

        if error_msg:
            raise experiment.model.errors.ExperimentInvalidConfigurationError('. '.join(error_msg))

        self._currentStage = 0

        #Create Status object (and status file)
        self._statusFile = None
        statusFileName = None
        try:
            statusFileName = os.path.join(self.instanceDirectory.absoluteOutputDirectory, "status.txt")
            if os.path.exists(statusFileName):
                self._statusFile = Status.statusFromFile(statusFileName)
            else:
                # VV: Sniff the "created-on" timestamp by looking at the $INSTANCE_DIR_NAME
                # The directory looks like <some-name-here>-%Y-%m-%dT%H%M%S.%f.instance
                stamp_format = "%Y-%m-%dT%H%M%S.%f"
                basename = os.path.split(self.instanceDirectory.instancePath)[1]
                basename = os.path.splitext(basename)[0]
                components = basename.split("-")

                created_on = datetime.datetime.now()
                if len(components) > 3:
                    stamp = '-'.join(components[-3:])
                    try:
                        created_on = datetime.datetime.strptime(stamp, stamp_format)
                    except ValueError:
                        # VV: stamp didn't match what we were hoping it to be, that's fine use 'now'
                        pass

                self._statusFile = Status(statusFileName, {}, [stage.name for stage in self._stages])
                self._statusFile.setCreated(created_on)
                self._statusFile.setUpdated(created_on)

                if updateInstanceConfiguration:
                    self.statusFile.update()
        except OSError as error:
            self.log.warning('Error (%s) creating status file at %s' % (error, statusFileName))
            self.log.warning('Continuing to allowing reading data but this may cause problems later')

        #Add hooks module in new package to sys.path
        sys.path.append(self.instanceDirectory.location)

    def get_input_ids(self, return_copy=True) -> List[str] | None:
        """Returns the list of input IDs that this experiment processes or None if there are no interface instructions
        to extract the input ids.

        Args:
            return_copy: Whether to return a copy of the input ids instead of the actual object. Default is true.

        Returns:
            If the experiment defines an extraction method for the input ids, returns the list of input ids. Otherwise,
                it returns None.
        """
        concrete = self.configuration.get_flowir_concrete(return_copy=False)
        interface = concrete.get_interface(return_copy=False)
        if not interface:
            return None

        input_ids = interface['inputs']
        if return_copy:
            return copy.deepcopy(input_ids)
        return input_ids

    def get_additional_input_data(self, return_copy=True) -> Dict[str, List[str]] | None:
        """Returns a dictionary whose keys are input ids and values are lists of strings, each of which is the name
        of an additional file associated with the input id.

        The runtime automatically invokes the hook get_additional_input_data when
        interface.inputSpec.additionalInputData is set to True.

        Args:
            return_copy: Whether to return a copy of the additionalInputData dictionary instead of the actual object.
                Default is true.

        Returns:
            If the experiment defines interface.inputSpec.additionalInputData=True, returns the association
                of additionalInputData to input ids. Otherwise, it returns None.
        """
        concrete = self.configuration.get_flowir_concrete(return_copy=False)
        interface = concrete.get_interface(return_copy=False)
        if not interface:
            return None

        additional_input_data = interface['additionalInputData']
        if return_copy:
            return copy.deepcopy(additional_input_data)
        return additional_input_data

    def get_measured_properties(self, return_copy=True) -> pandas.DataFrame | None:
        """Returns the Dataframe that holds all the properties that this experiment has measured.

        Make sure that you run process_interface(validate_only=False, do_input_spec=True, do_properties_spec=True)
        at least once: a) during the lifetime of this process and b) after the components that generate outputs have
        finished generating the outputs.

        If a key-output of the experiment has not been produced, the returned Pandas will not contain any properties
        that are measured using that key-output.

        Args:
            return_copy: Whether to return a copy of the measured properties Dataframe instead of the actual object.
                Default is true.

        Returns:
            The Dataframe that holds all the properties that this experiment has measured.
        """

        if self._measured_properties is not None:
            if return_copy:
                return copy.deepcopy(self._measured_properties)
            else:
                return self._measured_properties

    def set_measured_properties(self, properties: pandas.DataFrame, store_on_disk=True):
        """Register/Modify additional data for input ids

        Also updates configuration of experiment

        Args:
            properties: A Dataframe which contains the columns `input-id`, `$propertyName` (for each propertySpec entry)
                and other columns (experiment developers decide which other columns to include)
            store_on_disk: Whether to persist the changes to the disk. Default is true
        """
        self._measured_properties = properties

        concrete = self.configuration.get_flowir_concrete(return_copy=False)
        interface = concrete.get_interface(return_copy=False)

        if store_on_disk:
            output_files = interface.get('outputFiles', []) or []
            output_files.append(os.path.join('output', 'properties.csv'))
            output_files = sorted(set(output_files))
            interface['outputFiles'] = output_files

        # VV: We also want to update the unreplicated FlowIR because this is the flavour of FlowIR that we
        # store as flowir_instance.yaml
        unreplicated = self.configuration.get_unreplicated_flowir(return_copy=False)
        unreplicated.set_interface(interface)

        if store_on_disk:
            self._store_extracted_measured_properties()

    def switchWorkflowGraph(self, workflowGraph: experiment.model.graph.WorkflowGraph):
        self.experimentGraph = workflowGraph
        # self.configuration = self.experimentGraph.configuration
        self.experimentGraph.rootStorage = self.instanceDirectory

    @property
    def configuration(self) -> experiment.model.conf.FlowIRExperimentConfiguration:
        return self.experimentGraph.configuration

    def updateStages(self):
        log = logging.getLogger('Experiment')
        for i in range(self.experimentGraph.numberStageConfigurations):
            if i < len(self._stages):
                log.info("Switching graph of stage %s to new one" % i)
                self._stages[i].changeWorkflowGraph(self.experimentGraph)
            else:
                comp_names, stage_options = self.experimentGraph.get_stage_description(i)
                log.info("Creating new stage %s with components %s" % (
                    i, comp_names
                ))
                stage = Stage(self, i, comp_names, stage_options)
                self._stages.append(stage)

    def validateExperiment(self,
                           checkComponentReferences=True,
                           checkWorkflowOptions=True,
                           checkDataReferences=True,
                           checkExecutorOptions=True,
                           checkExecutables=True,
                           checkDirectReferences=True,
                           excludeInputReferences=False,
                           ignoreTestExecutablesError=False,
                           checkInterfaceSignatures=True,):

        '''Validates the experiment instance by performing a number of checks on the components

        Test Details:

        Graph Level (Requires only experiment configuration):
            - Checks component references are correct
               - via a check that the entity exists in the graph
            - Checks values component workflow keywords are valid
            - Checks values component executor keywords are valid
            - Checks migratable components have matching component in next stage
            - Checks executor options are correct

        Experiment Instance Level (Requires the instance dir to be created)
          - Checks component directories exist on disk
          - Checks input data is present on disk

        Deployment Level (Requires input data to be present)
            - Checks component direct data references exist
               - This test will fail if preformed before any experiment inputs are present.

        Deployment Level (Requires the correct filesystem/environment to be in place and possibly input data)
            - Checks component executables exist

        Parameters:
            checkComponentReferences: Checks that component references point to valid entities
                Note: componentReferences have been deprecated so this flag now does nothing
            checkDataReferences: Checks that component data references point to existing components in the graph
            checkWorkflowOptions: Checks a components workflow specific options are valid
            checkExecutorOptions: Checks acomponents executor specific options are valid
            checkExecutables: Checks that components executables exist and can be executed
            checkDirectReferences: If True performs existence checks on direct references (references to files not
              in components).
            excludeInputReferences: Excludes input references (those that are supplied by user running experiment)
              from the above check
            ignoreTestExecutablesError: Still attempts to check executables, but instead of raising an Exception it
              just prints a warning message on the console. This may be useful in scenarios where a backend is
              unavailable (e.g. kubernetes, LSF, etc) but there exist cached memoization candidates available which
              can be used instead of executing a task.
            checkInterfaceSignatures: Checks whether the signature of extractionMethods in the interface are valid
        Exceptions:
            Raises errors.ExperimentInvalidConfigurationError if there are issues'''

        configTests = []
        instanceTests = []

        #DON"T REQUIRE STORAGE
        import experiment.runtime.monitor

        if checkWorkflowOptions:
            configTests.append(experiment.model.graph.ComponentSpecification.checkWorkflowOptions)

        if checkDataReferences:
            configTests.append(experiment.model.graph.ComponentSpecification.checkDataReferences)

        if checkExecutorOptions:
            configTests.append(experiment.model.graph.ComponentSpecification.checkExecutorOptions)

        #REQUIRE STORAGE

        #Note: Although it requires storage for full testing, the test can be executed from ComponentSpecification
        if checkExecutables:
            configTests.append(lambda comp: experiment.model.graph.ComponentSpecification.checkExecutable(
                comp, ignoreTestExecutablesError=ignoreTestExecutablesError))

        if checkDirectReferences:
            instanceTests.append(lambda x: Job.checkDirectDataReferences(x, excludeInputReferences=excludeInputReferences))

        if checkInterfaceSignatures:
            interface = self.configuration.get_interface()
            if interface:
                do_interface = experiment.model.hooks.interface.Interface(interface, "FlowIR.interface")
                do_interface.validate(self)

        # VV: Make sure not to mix 'simulator' type jobs with any other kind of jobs
        #     This is just to guard against human error when moving from simulation/testing
        #     to production
        job_types = dict()  # type: Dict[str, List[Job]]
        try:
            workflowGraph = self.experimentGraph

            for node, data in self.graph.nodes(data=True):
                try:
                    spec: experiment.model.graph.ComponentSpecification = data['componentSpecification']
                except KeyError as e:
                    self.log.critical("\"componentSpecification\" missing from %s: %s" % (
                        node, experiment.model.frontends.flowir.yaml_dump(data)
                    ))
                    raise_with_traceback(e)

                try:
                    for test in configTests:
                        test(spec)
                except experiment.model.errors.DataReferenceFilesDoNotExistError as error:
                    # VV: It is ok for files to not exist as long as they are produced by Components
                    references = cast(List[Tuple[experiment.model.graph.DataReference, str]], error.referenceErrors)
                    actual_errors = [ref_path for ref_path in references if ref_path[0].isDirectReference(workflowGraph)]
                    if actual_errors:
                        raise experiment.model.errors.DataReferenceFilesDoNotExistError(actual_errors)
                except experiment.model.errors.DataReferenceInconsistencyError as error:
                    # Counting this as a configuration error
                    raise_with_traceback(experiment.model.errors.ExperimentInvalidConfigurationError(
                        "Problem with data references for component %s" % spec.identification, error))
                except experiment.model.errors.CircularComponentReferenceError as error:
                    # Counting this as a configuration error
                    raise_with_traceback(experiment.model.errors.ExperimentInvalidConfigurationError(
                        "Problem with component references for component %s" % spec.identification, error))
                except experiment.model.errors.ExecutorOptionsError as error:
                    raise_with_traceback(experiment.model.errors.ExperimentInvalidConfigurationError(
                        "Problem with values of backend executor options for component %s" % spec.identification,
                        error))
                backend = spec.resourceManager['config']['backend']
                if backend not in job_types:
                    job_types[backend] = [spec]
                else:
                    job_types[backend].append(spec)
        except Exception as e:
            if 'simulator' in job_types:
                if len(job_types) > 1:
                    e = experiment.model.errors.MixingSimulatorWithRealBackend(job_types)
                    self.log.critical(e)

            self.log.debug("Exception while validating %s" % traceback.format_exc())
            raise_with_traceback(e)
        else:
            if 'simulator' in job_types:
                if len(job_types) > 1:
                    raise experiment.model.errors.MixingSimulatorWithRealBackend(job_types)

        for i in range(self.configuration.numberStageConfigurations):
            stage = self._stages[i]
            for job in stage.jobs():
                try:
                    for test in instanceTests:
                        test(job)
                except experiment.model.errors.DataReferenceFilesDoNotExistError as error:
                    # Counting this as a configuration error
                    self.log.log(18, traceback.format_exc())

                    raise experiment.model.errors.ExperimentInvalidConfigurationError(
                        "Problem with data references for job %s" % job.identification, error)
                except ValueError as error:
                    #Don't raise error if the executable is actually an input
                    #TODO: An executable provided in this way won't be specified as a reference - should it?
                    #This is a bit hacky and leverages the fact that input references are direct
                    #Better if we had a checkInputs option and used that - requires filtering out inputs for directReferences
                    if "instance/input/" in job.executable and checkDirectReferences is False:
                        self.log.warning(
                            'Skipping existence check of executable of component %s which is also an input: %s',
                            (job.name, job.executable))
                    elif job.type == 'simulator':
                        self.log.critical('There might be a problem with executable references for job %s' % (
                            job.identification
                        ))
                    else:
                        raise experiment.model.errors.ExperimentInvalidConfigurationError(
                            "Problem with executable references for job %s" % job.identification, error)

    @property
    def name(self):

        '''Returns the name of the experiment

        Currently this is the package name minus any extension if there is one.
        If the package can't be determined its the instance minus any extension and time stamp'''

        return self.instanceDirectory.name

    @property
    def statusFile(self):
        # type: () -> Status
        '''Returns the data.Status object for the experiment'''

        return self._statusFile

    # @property
    # def globalEnvironment(self):
    #
    #     '''Returns a dictionary of variable:value pairs
    #     These are variables that must be set for any external process
    #     launched during the experiment'''
    #
    #     environment = os.environ.copy()
    #     variables = self.experimentGraph.defaultEnvironment()
    #     environment.update(variables)
    #     return environment

    def __str__(self):

        return "There are %d stages. Current stage %d" % (len(self._stages), self.currentStage())

    def incrementStage(self):
        self._currentStage += 1
        return self._currentStage

    def _decrementStage(self):

        self._currentStage -= 1

    def setCurrentStage(self, value):    

        '''Sets the current stage'''

        if value >= len(self._stages):
            raise IndexError("Trying to set stage to  %d when only %d stages (0 start) " % (value, len(self._stages)))
        else:
            self._currentStage = value

    def currentStage(self):

        '''Returns the current stage the experiment is at.

        This is the next stage that will be returned by stages()'''

        return self._currentStage

    def numStages(self):
        return len(self._stages)

    def getStage(self, stage_index):
        # type: (int) -> Stage
        return self._stages[stage_index]

    def stages(self):
        
        '''Generator: Returns the next stage to execute'''

        while self.currentStage() < len(self._stages):
            yield self._stages[self.currentStage()]
            self.incrementStage()

    def restage(self):

        '''Resets the next stage to last executed

        Existing data for the last executed stage is deleted.'''
        
        self.cleanStage()
        self._decrementStage()

    def cleanStage(self):

        '''Deletes all data for stage - cannot be undone'''

        log = logging.getLogger("data.experiment")

        log.warning("%s not implemented" % __name__)

    def findJob(self, stageIndex: int , jobName: str) -> "Job":

        '''Finds a Job in the experiment

        Parameters:
            stageIndex: The stage to look in
            jobName: Name of the job to find

        Returns:
            The matching Job object, or None if it could not be found.
            
        Exceptions:
            Raises IndexError if stageIndex > number of stages'''

        try:
            job = self._stages[stageIndex].jobWithName(jobName)
        except IndexError as error:    
            #As more context to index error
            raise IndexError("Searching for component '%s' in non-added stage %d. Underlying Error: %s" % (jobName, stageIndex, error))

        return job

    def findJobs(self, stageIndex, searchString):

        '''Finds all Jobs matching searchString

        The match condition is that the searchString is an initial substring of the jobName

        Parameters:
            stageIndex: The stage to look in
            searchString: Jobs whose name begins with "searchString" will be returned

        Returns:
            The matching Job objects, or None if none could not be found.
            
        Exceptions:
            Raises IndexError if stageIndex > number of stages'''

        try:
            components = [component for component in self._stages[stageIndex].jobs() if component.name[:len(searchString)] == searchString]
        except IndexError as error:    
            #As more context to index error
            raise IndexError("Searching for component '%s' in non-added stage %d. Underlying Error: %s" % (searchString, stageIndex, error))

        return components

    def addInput(self, path):

        '''Copies the file at path into the receivers input directory.

        Exceptions:
        
        Raises IOError or OSError if file cannot be copied'''

        self.instanceDirectory.addInputFile(path)

    def updateData(self, path):

        '''Copies the file at path into the receivers data directory.

        Note: A file with the same name must exist in the data directory

        Exceptions:
        
        Raises IOError or OSError if file cannot be copied'''

        self.instanceDirectory.updateDataFile(path)

    @classmethod
    def split_instance_location(cls, instance_location):
        # type: (str) -> Tuple[str, str]
        """Split an instance location file://$GATEWAY_ID/$INSTANCE_LOCATION and return
        $GATEWAY_ID, $INSTANCE_LOCATION

        $GATEWAY_ID can be ''
        """
        # VV: TODO Deprecate in favor of the experiment.model.storage API
        return experiment.model.storage.partition_uri(instance_location)

    def generate_instance_location(self, gateway_id):
        instance_location = self.instanceDirectory.location or ''
        return experiment.model.storage.generate_uri(gateway_id, instance_location)

    def annotate_component_documents(
            self, gateway_id: Optional[str],
            documents: List[ComponentDocument]) -> List[ComponentDocument]:
        """Updates instance/memoization-hash/uid of component Documents that a cluster with @GatewayID generated

        Updates documents in-place.

        Args:
            gateway_id: Optional id of the cluster gateway
            documents: Collection of documents

        Returns:
            The documents it modified (in-place changes).
        """
        instance = self.generate_instance_location(gateway_id)
        graph = self.graph

        db = None
        # VV: Attempt to augment Component documents with their current state
        try:
            db = experiment.runtime.status.StatusDB(location=self.instanceDirectory.statusDatabaseLocation)
            comp_states = db.get_components_state()
        except Exception as e:
            self.log.log(18, traceback.format_exc())
            self.log.info("Could not open database because of %s - will not record status of components" % e)
            for doc in documents:
                if doc.get('type') == 'component':
                    doc['component-state'] = 'missing'
        else:
            try:
                for doc in documents:
                    if doc.get('type') == 'component':
                        doc['memoization-hash'] = ''
                        doc['memoization-hash-fuzzy'] = ''
                        doc['component-state'] = 'unknown'
                        try:
                            cid = experiment.model.frontends.flowir.ComponentIdentifier(doc['name'], doc['stage'])
                            ref = cid.identifier
                            try:
                                # VV: Component-state field of component documentDescriptors may be state because the
                                # experiment.service.db.Status instance has not observed the emissions of the associated
                                # experiment.workflow.ComponentState object
                                comp_state_str = graph.nodes[ref]['component']().state
                            except Exception:
                                # VV: There's no `component` attribute, fallback to sqlite
                                comp_state_str = comp_states.get(doc['stage'], {}).get(doc['name'], 'unknown')
                            doc['component-state'] = comp_state_str
                        except Exception as e:
                            self.log.log(18, "err when setting component state of %s was %s" % (doc, e))
                            continue

                        if doc['component-state'] == experiment.model.codes.FINISHED_STATE:
                            try:
                                node = self.graph.nodes[ref]
                                spec = node['componentSpecification']  # type: "ComponentSpecification"
                            except Exception as e:
                                self.log.info("Cannot find node of %s because of %s - will"
                                              " set memoization hash to \"\"" % (ref, e))
                            else:
                                try:
                                    doc['memoization-hash'] = spec.memoization_hash or ''
                                    if doc['memoization-hash']:
                                        self.log.info("Memoization of %s is %s:%s" % (
                                            ref, doc['memoization-hash'], pprint.pformat(spec.memoization_info)
                                        ))
                                except Exception as e:
                                    self.log.info("Cannot compute memoization hash of %s because of %s - will"
                                                  " set it to \"\"" % (ref, e))
                                try:
                                    doc['memoization-hash-fuzzy'] = spec.memoization_hash_fuzzy or ''
                                    if doc['memoization-hash-fuzzy']:
                                        self.log.info("Fuzzy memoization of %s is %s:%s" % (
                                            ref, doc['memoization-hash-fuzzy'],
                                            pprint.pformat(spec.memoization_info_fuzzy)
                                        ))
                                except Exception as e:
                                    self.log.info("Cannot compute fuzzy memoization hash of %s because of %s - will"
                                                  " set it to None" % (ref, e))
            except Exception as e:
                self.log.info("Could not parse database because of %s - will not record status of components" % e)
        finally:
            if db:
                db.close()

        # Could add two-way ref here
        for doc in documents:
            Job.annotate_document(doc, instance)

        return documents

    @classmethod
    def _parse_outputs_file(cls, output_file: str) -> Dict[str, Dict[str, Union[str, int]]]:
        """Parses an output file (YAML) to produce the structure of the `output` key in experiment
        DocumentDescription

        Parses the output file and creates a dictionary with the following format:

        Args:
            output_file: Path to output_file, for example ${INSTANCE_DIR}/output/output.json
        <name of output:str>:
            creationtime: <datetime in format "%Y-%m-%dT%H%M%S.%f": str>
            description: <str>
            filename: <name of file:str>
            filepath: <absolute path to file:str>
            final: <yes/no:str>
            production: <yes/no:str>
            type: <the contents of the `type` field from the associated output entry in FlowIR:str>
            version: <times this output has been updated:int>

        Exceptions:
            Raises experiment.errors.InvalidFileError with appropriate underlying error for invalid contents
        """
        with open(output_file, 'rb') as f:
            outputs = yaml.load(f, yaml.FullLoader)  # type: Dict[str, Dict[str, str]]

        if isinstance(outputs, dict) is False:
            raise experiment.model.errors.InvalidFileError(
                output_file, TypeError("File contains a %s instead of a dictionary" % type(outputs)))

        ret = {}

        for output_name in outputs:
            if isinstance(output_name, string_types) is False:
                raise experiment.model.errors.InvalidFileError(
                    output_file, TypeError("Output name %s is %s instead of a string" % (
                        output_name, type(output_name))))
            bundle = outputs[output_name]  # type: Dict[str, str]
            entry = {}
            try:
                timestamp = float(bundle['creationtime'])
            except KeyError:
                raise experiment.model.errors.InvalidFileError(
                    output_file, TypeError("Output name %s does not have a creationtime timestamp" % output_name))
            except ValueError:
                raise experiment.model.errors.InvalidFileError(
                    output_file, TypeError("Output name %s has invalid timestamp (%s)" % (
                        output_name, bundle['creationtime'])))

            try:
                entry['creationtime'] = datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%dT%H%M%S.%f")
            except TypeError:
                raise experiment.model.errors.InvalidFileError(
                    output_file, TypeError("Output name %s has invalid timestamp (%s)" % (
                        output_name, timestamp)))

            for k in ['description', 'filename', 'filepath', 'final', 'production', 'type']:
                try:
                    entry[k] = str(bundle[k])
                except KeyError:
                    raise experiment.model.errors.InvalidFileError(
                        output_file, TypeError("Output name %s is missing key \"%s\"" % (output_name, k)))
            try:
                entry['version'] = int(bundle['version'])
            except TypeError:
                raise experiment.model.errors.InvalidFileError(
                    output_file, TypeError("Output name %s features a non integer \"version\" key (%s)" % (
                        output_name, bundle['version'])))
            except KeyError:
                raise experiment.model.errors.InvalidFileError(
                    output_file, TypeError("Output name %s is missing key \"version\"" % output_name))
            ret[output_name] = entry

        return ret

    def read_metadata_contents(self):
        # type: () -> Dict[str, Any]
        """Parses the elaunch.yaml file and raises an exception if it contains invalid keys

        If file does not exist, or cannot be YAML-loaded, method returns an empty dictionary. Invalid keys are
        those defined in Experiment.ReservedDocumentDescriptorKeys

        Returns a dictionary with the contents of the elaunch.yaml file
        Raises a ValueError exception indicating issues with the contents of elaunch.yaml
        """
        metadata_path = os.path.join(self.instanceDirectory.location, 'elaunch.yaml')
        metadata_contents = {}
        try:
            with open(metadata_path, 'r') as f:
                metadata_contents = experiment.model.frontends.flowir.yaml_load(f)
        except Exception as e:
            self.log.warning("Cannot access metadata file in path %s due to %s - will assume it' empty" %
                             (metadata_path, e))

        if metadata_contents:
            illegal_keys = [key for key in metadata_contents if key in self.ReservedDocumentDescriptorKeys]

            if illegal_keys:
                msg = "Metadata contents contains forbidden keys %s (keys cannot be %s)." % (
                    illegal_keys, self.ReservedDocumentDescriptorKeys)
                self.log.warning(msg)
                raise ValueError(msg)

        return metadata_contents

    def generate_experiment_document_description(self, gateway_id):
        # type: (Optional[str]) -> Dict[str, Any]
        """Generates an experiment documentDescription for experiment
        Args:
            gateway_id(Optional[str]): an optional gateway ID to use when building the instance URI for document

        Returns
            An experiment documentDescription
        """

        # Note: the output dir is a link while the experiment runs to avoid GPFS problems
        # However its possible it may not consoldiate properly at the end of an experiment
        # If subsequently the linked dir is deleted the link will be stale and unreadable
        # leading to an error here.
        # In this case we still want to be able to read the rest of the data so we
        # log a warning and continue
        try:
            output_file = os.path.join(self.instanceDirectory.outputDir, 'output.json')
            if os.path.isfile(output_file):
                outputContents = self._parse_outputs_file(output_file)
            else:
                self.log.info("Outputs file %s does not exist - will add an empty \"output\" key "
                              "to experiment DocumentDescription" % output_file)
                outputContents = {}
        except OSError as error:
            self.log.warning("Unable to access contents of output directory due: %s - will add an empty \"output\" key "
                              "to experiment DocumentDescription" % error)
            outputContents = {}

        instance = self.generate_instance_location(gateway_id)
        experiment_name = self.instanceDirectory.name

        try:
            status = self.statusFile.to_dict()
        except Exception as e:
            status = {}
            self.log.warning("Cannot insert `status` to experiment DocumentDescription due to %s - ignore error" % e)

        interface = self.configuration.get_interface() or {}

        return {
            "instance": instance,
            "interface": interface,
            # VV: we escape the symbols & and % as '%26' and '%25' respectively because we use the '&' character
            # as a delimiter in the user-metadata and the component documents
            'uid': instance.replace('%', '%25').replace('&', '%26'),
            "output": outputContents,
            "name": experiment_name,
            "type": "experiment",
            "status": status,
            "metadata": self.read_metadata_contents()
        }

    def generate_userMetadata_document_description(self, gateway_id: str | None) -> Dict[str, Any]:
        """Generates a user-metadata documentDescription for experiment
        Args:
            gateway_id(Optional[str]): an optional gateway ID to use when building the instance URI for document

        Returns
            A user-metadata documentDescription
        """
        instance = self.generate_instance_location(gateway_id)
        doc_userMetadata = self.read_metadata_contents().get('userMetadata', {})

        doc_userMetadata.update({
            "instance": instance,
            "type": "user-metadata",
            # VV: we escape the symbols & and % as '%26' and '%25' respectively because we use the '&' character
            # as a delimiter
            "uid": '&'.join((instance.replace('%', '%25').replace('&', '%26'), 'user-metadata'))
        })

        return doc_userMetadata

    def generate_document_description(self, gateway_id):
        # type: (Optional[str]) -> List[Dict[str, Any]]
        """Generates experiment, user-metadata and component documentDescriptions of experiment

        Args:
            gateway_id(Optional[str]): an optional gateway ID to use when building the instance URI for documents

        Returns
            A list of document descriptions
        """

        # VV: Create 1 experiment and 1 user-metadata documents
        doc_exp = self.generate_experiment_document_description(gateway_id)
        doc_userMetadata = self.generate_userMetadata_document_description(gateway_id)

        # VV: Generate component documents and annotate them with instance uri
        documents = []

        for stage in self._stages:
            documents.extend(stage.documentDescription)

        self.annotate_component_documents(gateway_id, documents)

        documents.append(doc_exp)
        documents.append(doc_userMetadata)

        return documents

    @property
    def documentDescription(self):
        # type: () -> List[Dict[str, Any]]
        """Returns a list of documents suitable for e.g. mongo db

        Each component is its own document, plus there is:
         1. an experiment document (containing experiment output/status other high level info)
         2. a user-metadata document (containing user provided key:value pairs)

        The reason is that for data access purposes a components is what we work with,
        i.e. We want the main result of a query to be sets of Component documents.

        From these components docs we then access CSV files.

        If the component docs are embedded as subdocs in experiments MongoDBs
        design, where the result of a query is by default the entire document
        (i.e. experiment) matching the query, makes things more difficult to use
        (in terms of constructing queries to return the desired results).
        There may also be a performance hit

        Since the primary result would be experiments each of these would contain
        ALL components (possibly hundreds) each with possible hundreds/thousands of files.
        More complex aggregation/filtering queries would have to be used to extract Components.

        This is despite the fact that the components are closely tied (or should only be
        interpreted) as part of a given experiment - a characteristic which is often
        cited as indicating they should be embedded in an experiment doc.

        Note: The descriptions purpose is for querying data"""

        return self.generate_document_description(None)

    @property
    def graph(self):
        # type: () -> DiGraph
        '''Returns a networkx.DiGraph object of the experiment'''

        return self.experimentGraph.graph

    #@property
    def primitiveGraph(self):

        '''Returns a networkx.Graph object of the experiments primitive graph'''

        return self.experimentGraph.primitive().graph

    def path(self, start, end):

        '''Returns all paths from start to end

        Param:
            start: Component reference
            end:   Component reference

        Returns:
            Ordered execution path betwen start and end'''

        return experiment.model.graph.ExecutionPath(start, end, self.graph)

    def set_interface_input_ids(self, input_ids: List[str], store_on_disk=True):
        """Register/Modify input ids

        Also updates configuration of experiment

        Args:
            input_ids: Array of input ids
            store_on_disk: Whether to persist the changes to the disk. Default is true
        """
        # VV: Only call this method if experiment has an interface
        if isinstance(input_ids, pandas.DataFrame):
            self.log.warning("Input ids are a DataFrame - will try to extract the `input-id` column")
            input_ids = input_ids['input-id'].to_list()
        elif isinstance(input_ids, pandas.core.series.Series):
            self.log.warning("Input ids are a pandas.core.series.Series - will try to convert to list of strings")
            input_ids = input_ids.to_list()

        concrete = self.configuration.get_flowir_concrete(return_copy=False)
        interface = concrete.get_interface(return_copy=False)
        interface['inputs'] = copy.deepcopy(input_ids)

        if store_on_disk:
            output_files = interface.get('outputFiles', []) or []
            output_files.append(os.path.join('output', 'input-ids.json'))
            output_files = sorted(set(output_files))
            interface['outputFiles'] = output_files

        # VV: We also want to update the unreplicated FlowIR because this is the flavour of FlowIR that we
        # store as flowir_instance.yaml
        unreplicated = self.configuration.get_unreplicated_flowir(return_copy=False)
        unreplicated.set_interface(interface)

        if store_on_disk:
            self._store_extracted_input_ids()

    def _store_extracted_input_ids(self):
        # VV: Storing these as JSON enable us to read them as either JSON or YAML
        path = os.path.join(self.instanceDirectory.outputDir, 'input-ids.json')
        with open(path, 'w') as f:
            json.dump(self.get_input_ids(return_copy=False), f, indent=4, separators=(',', ': '))

    def set_interface_additional_input_data(self, additional_input_data: Dict[str, List[str]], store_on_disk=True):
        """Register/Modify additional data for input ids

        Also updates configuration of experiment

        Args:
            additional_input_data: A dictionary of lists. Keys are input ids and values are absolute paths of files
                which are related to the input id.
            store_on_disk: Whether to persist the changes to the disk. Default is true
        """
        # VV: Only call this method if experiment has an interface
        concrete = self.configuration.get_flowir_concrete(return_copy=False)
        interface = concrete.get_interface(return_copy=False)

        interface['additionalInputData'] = copy.deepcopy(additional_input_data)

        # VV: We also want to update the unreplicated FlowIR because this is the flavour of FlowIR that we
        # store as flowir_instance.yaml
        unreplicated = self.configuration.get_unreplicated_flowir(return_copy=False)
        unreplicated.set_interface(interface)
        if store_on_disk:
            self._store_additional_input_data()

    def _store_additional_input_data(self):
        # VV: Storing these as JSON enable us to read them as either JSON or YAML
        path = os.path.join(self.instanceDirectory.outputDir, 'additional_input_data.json')
        with open(path, 'w') as f:
            json.dump(self.get_additional_input_data(return_copy=False), f, indent=4, separators=(',', ': '))

    def _store_extracted_measured_properties(self):
        path = os.path.join(self.instanceDirectory.outputDir, 'properties.csv')
        self._measured_properties.to_csv(path, index=False, sep=";")

    def load_extracted_measured_properties(self, return_copy=True):
        path = os.path.join(self.instanceDirectory.outputDir, 'properties.csv')
        self._measured_properties = pandas.read_csv(path, sep=None, engine="python")
        return self.get_measured_properties(return_copy=return_copy)

    def resolve_key_outputs(self) -> Dict[str, str]:
        """Maps the key outputs of the experiment to their absolute paths

        Returns:
            A dictionary whose keys are key-output names, and values are absolute paths of the output paths
        """
        output_spec = self.configuration.get_key_outputs()

        def find_fresh(name: str, output_spec: experiment.model.frontends.flowir.DictFlowIROutput) -> str | None:
            """Returns the absolute path to the file associated with the key-output

            If the file does not exist it returns None.

            If the file may be produced by components in multiple stages it prioritizes components that have executed
            in later stages.
            """
            output_spec = output_spec[name]
            try:
                data_in = output_spec['data-in']
            except KeyError:
                raise KeyError(f"Experiment key-output {name} does not have a data-in field")

            stages = output_spec.get('stages', [])
            # VV: data-in can point to files computed by multiple components with the same name but in different stages
            # we want to find the reference that points to a file which exists and has been produced by the component
            # with the largest stage.
            full_refs = []
            if len(stages) == 0:
                full_refs.append(data_in)
            else:
                for s in sorted(stages, reverse=True):
                    full_refs.append("stage%d.%s" % (
                        experiment.model.frontends.flowir.FlowIR.stage_identifier_to_stage_index(s), data_in))

            for ref in full_refs:
                dref = experiment.model.graph.DataReference(ref)
                workdir = self.instanceDirectory.workingDirectoryForComponent(dref.stageIndex, dref.producerName)
                path = os.path.join(workdir, dref.path or '')
                if os.path.exists(path):
                    return path

        return {k: find_fresh(k, output_spec) for k in output_spec}


class Job(experiment.model.interface.InternalRepresentationAttributes):

    '''Experiments are composed of Components .

    Components main attributes
    - A ComponentSpecifcation - the configuration of the component
    - A WorkingDirectory - where the component will run

    '''

    @classmethod
    def jobFromConfiguration(cls, componentIdentifier, #  type: experiment.model.graph.ComponentIdentifier
                             workflowGraph,  #  type: experiment.model.graph.WorkflowGraph
                             workingDirectory=None #  type: experiment.model.storage.WorkingDirectory
                             ):

        """Returns a Job instance created using the provided configuration

        Parameters
        ----------
        cls: Type[Job]
            The class type of this class method, can be a class which derives from Job
        componentIdentifier: experiment.model.graph.ComponentIdentifier
             The identifier of the component in the graph to create a Job for
        workflowGraph: experiment.model.graph.WorkflowGraph
            The WorkflowGraph instance detailing the experiment configuration and component relationships
        workingDirectory: Optional[experiment.model.storage.WorkingDirectory]
            A WorkingDirectory object that represents the run directory of the Job.
            Can be None

        Returns
        -------
        Job
            The job description

        Raises
        ------
        errors.ExperimentInvalidConfigurationError
            if there are any issues with the configuration.
        """

        try:
            job = Job(componentIdentifier,
                        workingDirectory,
                        workflowGraph)

        except Exception as error:
            #Combine DataReferenceFormatError,PathNotRelativeError and ValueErrors
            #into an ExperimentInvalidConfigurationError
            raise experiment.model.errors.ExperimentInvalidConfigurationError(
                "Error with configuration of job %s" % componentIdentifier,
                error)

        return job

    def __deepcopy__(self, memodict={}):

        #Dont support deepcopy
        return self

    def __init__(self, componentIdentfier: experiment.model.graph.ComponentIdentifier,
                 workingDirectory: experiment.model.storage.WorkingDirectory,
                 workflowGraph: experiment.model.graph.WorkflowGraph):

        """
        Initialises new Job object for a node in a workflow graph

        The created instance attaches itself to relevant node on the graph as 'componentInstance'

        Args:
            componentIdentifier: The node in the graph which will be instantiated
            workingDirectory: An optional WorkingDirectory instance representing the working directory for the Job.
                If not supplied a job cannot have copy/link input refs
                If a job is migratable this directory will be replaced with a link to the migrating jobs dir
            workflowGraph: The graph defining the workflow configuration
                - There must be a node in this graph called componentIdentifier
                - The node must have an experiment.model.graph.ComponentSpecification instance attached
                  (as 'componentSpecification')

        Exceptions
            - Raises experiment.errors.DataReferenceInconsistencyError if
                     -  a component without a working dir has references requiring a working dir (copy/link)
                     -  a migrated job has > 1 data reference
                     -  a migrated jobs data reference is not of type "link"
            - Raises KeyError if no component called componentIdentifier exists in the graph
            - Raises experiment.errors.ComponentNotFoundError if there node of the graph called componentIdentifier
            - Raises experiment.errors.ComponentSpecificationNotFoundError if there is no spec for the component stored in the relevant node of the graph
        """

        #Validate arguments e.g. all inputOptions match inputPackages
        #Executable exists and is executable?

        self.cid = componentIdentfier  # type: experiment.model.graph.ComponentIdentifier
        log = logging.getLogger("component.%s" % self.cid)
        self.componentSpecification = None  # type: "ComponentSpecification"
        self.workflowGraphRef = None  # type: Callable[[], experiment.model.graph.WorkflowGraph]

        # VV: this also sets the componentSpecification ivar
        self.setWorkflowGraph(workflowGraph)
        self.workingDirectory = workingDirectory  # type: Optional[experiment.model.storage.WorkingDirectory]
        self.isStaged = False
        #Check DataReferences - if we have no working directory they all must be ref
        if self.workingDirectory is None:
            for reference in self.componentSpecification.dataReferences:
                if reference.method != experiment.model.graph.DataReference.Ref:
                    msg = 'Reference must be of type ref for components without working directories: %s' % reference
                    log.critical(msg)
                    raise experiment.model.errors.DataReferenceInconsistencyError(msg)

        #Migrated jobs must have a single link data referece
        try:
            if self.isMigrated:
                if len(self.componentSpecification.dataReferences) != 1:
                    msg = 'Migrated job has %d data references - should be 1' % len(self.componentSpecification.dataReferences)
                    log.critical(msg)
                    raise experiment.model.errors.DataReferenceInconsistencyError(msg)

                if self.componentSpecification.dataReferences[0].method != experiment.model.graph.DataReference.Link:
                    msg = 'Migrated job reference method must be "link" - %s specified' % self.componentSpecification.dataReferences[0].method
                    log.critical(msg)
                    raise experiment.model.errors.DataReferenceInconsistencyError(msg)
        except experiment.model.errors.DataReferenceInconsistencyError as err:
            if self.type == 'simulator':
                log.critical('Some references are missing, but we are simulating')
            else:
                raise err

    def __str__(self):

        desc = "Job %s (Staged: %s, Program: %s)" % (self.cid, self.isStaged, self.componentSpecification.commandDetails.get('executable'))

        return desc

    @classmethod
    def annotate_document(
            cls,
            doc: ComponentDocument,
            instance: str,
    ):
        """Updates the fields of a component MongoDocument that depend on the instance URI

        Currently, these are:
        1. instance
        2. producers

        Args:
            doc: Document description of a Job
            instance: The file://$GATEWAY_ID/path/to/workflow/instance URI
        """
        logger = logging.getLogger('annotateJobDoc')
        gateway_id, location = experiment.model.storage.partition_uri(instance)

        if doc['type'] != 'component':
            logger.info(f"Will not annotate document {doc['type']}")
            return
        doc['instance'] = instance

        # VV: Component uids follow the format: <$INSTANCE_URI>&<$COMPONENT_REFERENCE>
        # we escape the symbols & and % as '%26' and '%25' respectively
        cid = experiment.model.graph.ComponentIdentifier(doc['name'], doc['stage'])
        doc['uid'] = cid.to_uid(instance)

        # VV: Finally update the UID of the component's producers to include the gateway
        producers: Dict[str, List[str]] = doc['producers']
        for prod_uid in list(producers):
            _, remaining = experiment.model.storage.partition_uri(prod_uid)
            files = producers.pop(prod_uid)
            uid_with_gw = experiment.model.storage.generate_uri(gateway_id, remaining)
            producers[uid_with_gw] = files

    def setWorkflowGraph(self, workflowGraph: experiment.model.graph.WorkflowGraph):
        try:
            node = workflowGraph.graph.nodes[self.cid.identifier]
        except KeyError:
            raise experiment.model.errors.ComponentNotFoundError(self.cid.identifier)

        try:
            comp_desc = node["componentSpecification"]  # type: ComponentSpecification
        except KeyError:
            raise experiment.model.errors.ComponentSpecificationNotFoundError(self.cid.identifier)

        self.componentSpecification = comp_desc  # type: ComponentSpecification
        self.workflowGraphRef = weakref.ref(workflowGraph)
        # Add  to the graph node
        workflowGraph.graph.nodes[self.cid.identifier]["componentInstance"] = self

    def consumesPredecessorPaths(self) -> Dict[str, List[str]]:
        """Estimates which paths that the predecessors produce, this job ends up referencing

        This method does not guarantee that it will discover all paths.

        A component UID is file://<gateway id>/path/to/instance&stage<stage index>.<component name>

        Returns:
            Dictionary, whose keys are UIDs of predecessors and values are the
            list of paths that exist under the working-directory of associated predecessor and
            this component references.

        """
        consumes: Dict[str, Set[str]] = dict()
        cmd_arg_str = self.componentSpecification.commandDetails.get('arguments', "")

        references = self.componentSpecification.rawDataReferences

        app_deps = self.workflowGraph.configuration.get_application_dependencies()
        dir_root = self.workflowGraph.rootStorage.location
        instance = experiment.model.storage.generate_uri(None, dir_root)

        for ref in references:
            dri = DataReferenceInfo(ref, dir_root, self.stageIndex, cmd_arg_str, app_deps)

            if dri.pid:
                uid = dri.pid.to_uid(instance)
                if uid not in consumes:
                    consumes[uid] = set()
                for f in dri.files:
                    consumes[uid].add(f.filename)

        # VV: Finally turn the set in a list (set is just there so that we don't have duplicates)
        return {x: list(consumes[x]) for x in consumes}

    @property
    def documentDescription(self):
        """Returns a dict suitable for e.g. mongo db

        Note: The description's purpose is for querying data

        Raises:
            KeyError: If the `output` dictionary contains an output which does not have a `data-in` key
        """
        try:
            flowir = self.componentSpecification.configuration
        except Exception as e:
            log = logging.getLogger(self.reference)
            log.log(18, traceback.format_exc())
            log.info("Could not generate flowir: %s - will include blank dictionary instead" % e)
            flowir = {}

        data = {
            "name": self.cid.componentName,
            "stage": self.stageIndex,
            "location": self.workingDirectory.path,
            "files": self.workingDirectory.allContents,
            "type": "component",
            "flowir": flowir,
            "memoization-hash": '',
            "component-identifier": self.cid.identifier,
            "producers": self.consumesPredecessorPaths(),
        }

        # FIXME: There is a problem when the description is created using an existing instance
        # as all files in the component will be treated as input
        # data["input"] = self.workingDirectory.inputs

        return data

    #FOLLOWING PROPERTIES ARE TO BE DEPRECATED

    @property
    def executable(self):

        return self.command.executable

    @property
    def arguments(self):

        return self.command.arguments

    @property
    def stageIndex(self):

        return self.cid.stageIndex

    @property
    def type(self):

        return self.componentSpecification.resourceManager['config']['backend']

    @property
    def reference(self):

        '''Returns the receivers references string'''

        return self.componentSpecification.identification.identifier

    @property
    def name(self):
        # type: (Job) -> str
        '''Returns the name of the receiver without the namespace (stage)'''

        return self.componentSpecification.identification.componentName

    @property
    def flowir_description(self):

        """ A Dictionary containing the full FlowIR description of this component
        """

        return copy.copy(self.componentSpecification.configuration)

    def options_modify(self, key, value):

        self.componentSpecification.setOption(key, value)

    def options_remove(self, key):

        self.componentSpecification.removeOption(key)


    #END DEPRECATION

    #Convenience Properties

    @property
    def directory(self):

        if self.workingDirectory is not None:
            return self.workingDirectory.path
        else:
            return None

    @property
    def isMigrated(self):

        '''Returns True if this job will be migrated from a previous stage.
        
        This means the jobs type is 'migrated'''

        return self.workflowAttributes['isMigrated']

    @property 
    def isMigratable(self):

        '''Returns True if this job will be migrated to the next stage'''

        return self.workflowAttributes['isMigratable']

    @property
    def isRepeat(self):

        return self.componentSpecification.workflowAttributes['isRepeat']

    def repeatInterval(self):

        if self.isRepeat:
            return self.componentSpecification.workflowAttributes['repeatInterval']
        else:
            raise ValueError("Receiver is not a repeating job")

    #
    # METHODS
    #

    def producersHaveOutputSinceDate(self, date):

        '''Returns True if any of the recievers producers have created output since date

        Exceptions:

        Raises FilesystemInconsistencyError if there is an issue in accessing the Jobs output dir'''

        hasOutput = False
        for producerInstance in self.producerInstances:
            # HACK for continuous remote jobs
            # These jobs will not produce any local output until termination
            # and local consumers will block until then
            # The problem doesn't occur with repeating jobs as they should continually stage-back
            # The hack removes the condition on
            if producerInstance.isRepeat is False or len(producerInstance.workingDirectory.outputSinceDate(date)) > 0:
                hasOutput = True
                break

        return hasOutput

    @property
    def producerInstances(self):
        # type: () -> List[Job]
        '''Returns a list of the Jobs the receiver consumes from
        
        This method can only be called after all the jobs in a stage
        have been created.

        Exceptions: 

        Unless an internal inconsistency is encountered this method should not raise any exceptions.
        If it does it is a bug.'''

        instances = []

        for ref in self.componentSpecification.componentDataReferences:
            try:
                p_id = ref.producerIdentifier.identifier

                placeholder = self.workflowGraph._placeholders.get(p_id)  # type: Dict[str, Any]

                if placeholder is not None:
                    # VV: If the DataReference points to a Placeholder, fetch the latest componentInstance
                    p_id = placeholder['latest']

                comp_instance = self.workflowGraph.graph.nodes[p_id]['componentInstance']
            except Exception:
                raise_with_traceback(ValueError("Component %s could not get 'componentInstance' from its producer "
                                                "%s for reference \"%s\"" % (
                    self.identification.identifier, ref.producerIdentifier.identifier, ref.stringRepresentation
                )))
            else:
                instances.append(comp_instance)

        return instances

    def checkDirectDataReferences(self, excludeInputReferences=False):

        '''Checks a components direct data references exist

        Note: It checks for complete reference existence not just directories

        Params:
            excludeInputReferences: If True, references to files in the input directory are omitted

        These are references to pre-existing files (files not in components).

        Raises:
            experiment.errors.DataReferenceFilesDoNotExistError if a direct data reference doesnt exist
            This is raised on first non-existent reference'''
        check_path_for = [
            experiment.model.graph.DataReference.Ref,
            experiment.model.graph.DataReference.Extract,
            experiment.model.graph.DataReference.Copy,
            experiment.model.graph.DataReference.CopyOut,
            experiment.model.graph.DataReference.Link
        ]

        log = logging.getLogger("component.%s" % self.cid)
        for reference in self.componentSpecification.dataReferences:
            if reference.isDirectReference(self.workflowGraph):
                # VV: An `output` reference can be directReference but it will *not* resolve to a path, it resolves
                #     to the contents of the File. Internally, it ensures that the path does exist before it
                #     attempts to read its contents so first resolve the directReference, and then only attempt to
                #     make sure that the path is correct for a subset of the supported DataReference types
                path = reference.resolve(self.workflowGraph)

                if reference.method not in check_path_for:
                    log.warning("Will not check if path for directReference %s exists"
                                % reference.stringRepresentation)
                elif not os.path.exists(path):
                    if excludeInputReferences is True and "input" in path:
                        log.info('Skipping check of input direct reference %s' % path)
                    elif self.type != 'simulator':
                        log.critical("File %s for reference %s is missing" % (path, reference))
                        raise experiment.model.errors.DataReferenceFilesDoNotExistError([(reference, path)])
                    else:
                        log.critical("File %s for reference %s is missing (simulating)" % (path, reference))

    def resolveArguments(self, unresolved=None):

        '''Returns the components argument string with references resolved

        Parameters:
            unresolved (list|None): If a list on return contains information on any
                possible unresolved references in the commandline '''

        # Resolve Arguments
        # Note - not setting ignoreErrors to True as if references cannot be resolved once this class
        # is instantiated it indicates an error
        return self.componentSpecification.resolveArguments(unresolved=unresolved)

    def resolveCommand(self):

        '''Returns the components command with references resolved'''

        return self.command.commandLine
    
    def stageIn(self, verbose=True):

        '''Performs all necessary work to run the job.

        - Copies 'copy' and 'copyout' refs
        - Creates 'link' refs
        - Updates input files list
        - Handles migrating jobs

        Notes:

        The job will fail to run if this method is not called.
        This method changes the job to 'staged' state it also
        may modify the value of the job arguments.

        Exceptions:

        Raises errors.DataReferenceFilesDoNotExistError if specified reference files do not exist
        Raises errors.DataReferenceCouldNotStageError if there was an error in copying/linking files
        Raises errors.FilesystemInconsistencyError if problem detected with filesystem.

        Note: DataReferenceFilesDoNotExistError and DataReferenceCouldNotStageError could both be due
        to an unstable file-system. However this cannot be distinguish from the case where the producers
        e.g. failed to create files with correct names.'''

        log = logging.getLogger("component.%s" % self.cid)

        log.debug('Job %s staging-in' % self.cid)
        log.debug('There are %d data packages to stage' % len(self.componentSpecification.dataReferences))

        if self.type != 'simulator':
            # VV: Non simulator jobs stage references
            def stage_reference_wrapper(dataReference, location, graph):
                return StageReference(dataReference, location, graph)
            stage_reference = stage_reference_wrapper
        else:
            # VV: Simulator jobs must *NOT* stage references
            def stage_reference(reference, location, graph):
                # VV: Leaving this as .info instead of .debug just to make sure that Flow output
                #     varies significantly between simulator and production modes
                #     it's ok for the simlator output to be ugly, it's mostly devs that'll be
                #     reading the logs after all
                log.debug('Simulating StageReference "%s" to "%s")' % (
                    reference,
                    location.path
                ))

        #Copy/Link Files
        if self.isMigrated is True:

            #Delete the 'hard' working directory created previously
            location = os.path.split(self.workingDirectory.path)[0]
            shutil.rmtree(self.workingDirectory.path)

            #There should be exactly one reference
            reference = self.componentSpecification.dataReferences[0].resolve(self.workflowGraph)
            name = os.path.split(reference)[1]
            dest = os.path.join(location, name)

            os.symlink(reference, dest)

            #Create new JobWorkingDirectory object for this instance
            #pointing to link dir
            self.workingDirectory = experiment.model.storage.JobWorkingDirectory(dest,
                                                                self.workingDirectory.stageIndex,
                                                                self.workingDirectory.experimentDirectory,
                                                                ignoreExisting=True)

        elif self.workingDirectory is not None:
            #Copy files in, in two phase
            #First: copy/link, Second: copyout
            #In this was copyout refs will not be counted as inputs

            # FIXME: Hack for hybrid jobs
            # Input files referenced from components may not exist on local side
            # This is true even if they are staged-out by the producer as that happens asynchronously
            # Solution implemented is to ignore ref references and wait for copy reference to appear (polling)

            # VV: @tag:CopyOut
            # VV: Temporarily skip copy-out data-references so that they are not considered `input` files
            #     by the call to `self.workingDirectory.updateInputs()` because that would prohibit the
            #     component of detecting it within its Engine.canConsume()/producersHaveOutputSinceDate()
            #     methods.

            non_copyout = [reference for reference in self.componentSpecification.inputDataReferences if reference.method != experiment.model.graph.DataReference.CopyOut]

            for reference in non_copyout:
                if reference.method in experiment.model.graph.DataReference.pathMethods:
                    stage_reference(reference, self.workingDirectory, self.workflowGraph)
                else:
                    log.debug("Skipping StageIn for direct reference %s" % reference.stringRepresentation)

            # VV: @tag:CopyOut
            non_copyout = [reference for reference in self.componentSpecification.componentDataReferences if reference.method != experiment.model.graph.DataReference.CopyOut]
            producers = self.componentSpecification.producers
            for reference in non_copyout:
                #TODO: This workaround may not be needed anymore!!
                # Hybrid jobs staging output back should not exit before the output is back.
                # Hybrid workaround method 1
                # We need to find if the producer of the data is configured to copy data back
                # Note: Treat references which are directories normally
                try:
                    producerSpecification = producers[reference]
                except KeyError:
                    msg = "(%s) Unknown producer \"%s\" for reference %s, known (prod, reference) are: %s" % (
                        self.cid.identifier, reference.producerIdentifier.identifier, reference.stringRepresentation,
                        [(r.producerIdentifier.identifier, r.stringRepresentation) for r in producers]
                    )
                    raise ValueError(msg)


                # VV: Iterate the List of post executors and look for an `lsf-dm-out` command

                outputStagedBack = any([executor.get('name') == 'lsf-dm-out' for executor in producerSpecification.executors['post']])
                if outputStagedBack:
                    if reference.method == experiment.model.graph.DataReference.Copy:
                        attempts = 0
                        success = False
                        while attempts < 5:
                            try:
                                stage_reference(reference, self.workingDirectory, self.workflowGraph)
                                success = True
                            except experiment.model.errors.DataReferenceFilesDoNotExistError as error:
                                log.warning("Files of remote job required for copy not present")
                                log.warning("Will sleep and wait. %d attempts remaining" % (5-attempts))
                                time.sleep(10.0)
                            finally:
                                attempts += 1

                        if success is False:
                            raise experiment.model.errors.DataReferenceCouldNotStageError(reference, None)

                    elif reference.method == experiment.model.graph.DataReference.Link:
                        log.critical("Unable to stage link reference to remote file (%s) in hybrid environment" % reference)
                    else:
                        log.warning("Found :ref reference (%s) to remote file. Assuming exists", reference)
                elif reference.method in experiment.model.graph.DataReference.pathMethods:
                    stage_reference(reference, self.workingDirectory, self.workflowGraph)
                else:
                    log.debug("Skipping StageIn for component reference %s" % reference.stringRepresentation)

            #Update the WorkingDirectory object inputs now they have been staged
            self.workingDirectory.updateInputs()

            # VV: @tag:CopyOut
            #Now process copyout refs i.e. those that will be treated as outputs
            copyout = [reference for reference in self.componentSpecification.dataReferences if reference.method == experiment.model.graph.DataReference.CopyOut]

            for reference in copyout:
                stage_reference(reference, self.workingDirectory, self.workflowGraph)

        self.isStaged = True

    #
    # InternalRepresentationAttributes methods
    #

    @property
    def producers(self):

        '''Returns a dict of DataReference:Job pairs for the receivers producers'''

        specs = {}
        for ref in self.componentDataReferences:
            specs[ref] = self.workflowGraph.graph.nodes[ref.producerIdentifier.identifier]['componentInstance']

        return specs


    @property
    def inputDataReferences(self):
        # type: () -> "List[experiment.model.graph.DataReference]"

        '''A receivers input data-references as a list of graph.DataReference objects

        Requires that the edges have been added to the WorkflowGraph the receiver has access t

        Returns:
            List of DataReferences'''

        return self.componentSpecification.inputDataReferences

    @property
    def componentDataReferences(self) -> List[experiment.model.graph.DataReference]:
        return self.componentSpecification.componentDataReferences

    @property
    def identification(self) -> experiment.model.graph.ComponentIdentifier:
        return self.cid

    @property
    def environment(self):

        '''Returns the environment defined for the job'''

        return self.componentSpecification.environment

    @property
    def workflowAttributes(self):

        '''Returns any workflow attributes defined for the component

        Will replace the following properties eventually

        isMigratable/migrated/repeatInterval/repeatCondition/isRepeat.

        Raises ValueError if any of the workflow-attributes are invalid.'''

        return self.componentSpecification.workflowAttributes

    @property
    def command(self):
        # type: () -> Command

        '''Returns an executor.Command object representing the receivers command line

        NOTE: Difference to graph.ComponentSpecification.Command

        This method differs to since it sets the correct working dir.
        This means the property:
        - will always resolve the raw argument string w.r.t the storage
        - can always resolve the executable w.r.t the correct location
        - It will use the correct directory to resolve shell substitutions
        '''

        commandDetails = self.componentSpecification.commandDetails

        arguments = self.resolveArguments()


        #Do not attempt to find pathless executables here (set findPath=True)
        #The default is to assuming pathless executable will be found in the environment and only check if explicitly asked
        #This happen on call to ComponentSpecification.checkExecutable
        #NOTE: If ComponentSpecification.checkExecutable results in a change in the path
        #Then it will be reflected here as the changed path is written back into the configuration
        #i.e. commandDetails['executable'] will change
        return experiment.model.executors.Command(commandDetails['executable'],
                                                        arguments=arguments,
                                                        workingDir=self.workingDirectory.path,
                                                        environment=self.componentSpecification.environment,
                                                        basePath=self.workflowGraph.rootStorage.location,
                                                        resolvePath=commandDetails.get('resolvePath'),
                                                        resolveShellSubstitutions=True,
                                                        useCommandEnvironment=True,
                                                        expandArguments=commandDetails.get('expandArguments', 'double-quote')
                                                        )

    @property
    def resourceRequest(self):

        '''Returns the ComponentSpecifications resource-request

        Makes explicit the resource-request details'''

        return self.componentSpecification.resourceRequest

    @property
    def resourceManager(self):
        return self.componentSpecification.resourceManager

    @property
    def executors(self):
        # type: () -> Dict[str, List[Dict[str, Dict[str, str]]]]
        '''Returns a Dictionary containing List of executor options for each stage'''

        return self.componentSpecification.executors

    @property
    def dataReferences(self) -> List[experiment.model.graph.DataReference]:
        return self.componentSpecification.dataReferences

    @property
    def workflowGraph(self) -> experiment.model.graph.WorkflowGraph:
        '''Returns the experiment.model.graph.WorkflowGraph object the receiver is part of'''

        return self.workflowGraphRef()

    @property
    def workingDir(self):
        return self.workingDirectory

    @property
    def customAttributes(self):

        return self.componentSpecification.customAttributes

    def setOption(self, key, value):

        self.componentSpecification.setOption(key, value)

    def removeOption(self, key):

        self.componentSpecification.removeOption(key)


class Stage:
    '''Class representing a stage in an experiment.

    An experiment usually has a number of stages.
    For example: pre-process, main, post-process.

    A stage will consist of a number of jobs which run concurrently.

    Attributes:
        An unordered list of jobs.
        An index indicating the stages order in the experiment
        A directory where the stages data is stored'''

    def _apply_options(self, stage_options):
        # type: (Dict[str, str]) -> None
        stage_options = stage_options or {}

        try:
            self._name = stage_options['stage-name']
        except KeyError:
            pass

        try:
            self._continueOnError = int(stage_options['continue-on-error'])
        except KeyError:
            pass

    def changeWorkflowGraph(self, workflowGraph):
        # type: (experiment.model.graph.WorkflowGraph) -> None
        concrete = workflowGraph.configuration.get_flowir_concrete(return_copy=False)
        comp_ids = concrete.get_component_identifiers(False)
        log = logging.getLogger('Stage%d' % self.index)

        for comp_id in comp_ids:
            comp_stage, comp_name = comp_id

            if comp_stage != self.index:
                continue

            if comp_name in self._jobs:
                job = self._jobs[comp_name]
                log.info("Updating workflowGraph for job %s " % job.identification.identifier)
                self._jobs[comp_name].setWorkflowGraph(workflowGraph)
            else:
                job = self._create_job(comp_name, workflowGraph)
                log.info("Patch in job %s " % job.identification.identifier)
                self._jobs[comp_name] = job

    def _create_job(self, name, workflowGraph):
        # type: (str, experiment.model.graph.WorkflowGraph) -> Job
        try:
            directory = self.experiment.instanceDirectory.createJobWorkingDirectory(self.index, name)
        except OSError as error:
            raise experiment.model.errors.FilesystemInconsistencyError(
                "Unable to create working directory for %s" % name, error
            )

        job = Job.jobFromConfiguration(experiment.model.graph.ComponentIdentifier(name, self.index),
                                       workflowGraph,
                                       directory)

        return job

    def _initialise_components(self, stage_components):
        # type: (List[str]) -> None

        '''Initialises the stages components

        Raises errors.FilesystemInconsistencyError if a components working dir cannot be created

        See jobFromConfiguration for other possible errors'''

        for name in stage_components:
            self._jobs[name] = self._create_job(name, self.experiment.experimentGraph)

    def add_job(self, job):
        # type: (Job) -> None

        self._jobs[job.identification.componentName] = job

    def __init__(
            self,  # type: Stage
            exp,  # type: Experiment
            index,  # type: int
            component_names,  # type: List[str]
            stage_options,  # type: Dict[str, str]
    ):
        """Initialise a stage object.

        Parameters
        ----------
        experiment: Experiment
            The experiment the stage is part of
        index: int
            The index of the stage in the experiment.
        component_names: List[str]
            Names of components in this stage

        Raises
        ------
        errors.FilesystemInconsistencyError
            if it cannot create on disk storage. This is an inconsistency as the experiment, and the
            overall instance directory must have already been created.
        errors.ExperimentInvalidConfigurationError
            if there is a problem with the configuration

        Notes
        =====
        A stage instance holds a weak-reference to its parent experiment.
        This allows it to search jobs in other stages
        """

        import collections

        self._jobs = collections.OrderedDict()  # type: Dict[str, Job]
        self.index = index
        self.weakExperiment = weakref.ref(exp)  # type: Callable[[], Experiment]
        self._continueOnError = 0
        self._name = 'stage%d' % index

        # Create directory
        try:
            self.directory = self.experiment.instanceDirectory.createStageDirectory(self.index)
        except OSError as error:
            raise experiment.model.errors.FilesystemInconsistencyError(
                "Unable to create working directory for stage  %s" % self.name, error
            )

        self._apply_options(stage_options)
        self._initialise_components(component_names)

    def __str__(self):

        '''Returns a string describing the stage'''

        relpath = self.experiment.instanceDirectory.relativePath(self.directory)
        return "Stage %d (%s) with %d jobs. Data located at %s" % (self.index, self.name, len(self._jobs), relpath)

    def jobNames(self):

        '''Returns a list of the names of the jobs in the stage - unordered'''

        return list(self._jobs.keys())

    def jobs(self):
        """Returns a list of the stages jobs - unordered

        Returns
        -------
        List[Job]
            The list of jobs (unordered)
        """

        return list(self._jobs.values())

    def jobWithName(self, name):

        '''Returns Job object with given name

        Or None if no Job called name is in the stage'''

        try:
            job = self._jobs[name]
        except KeyError:
            job = None

        return job

    @property
    def continueOnError(self):

        '''This property is True if the experiment should continue on encountering an error in this stage'''

        return self._continueOnError

    @property
    def name(self):
        """Return the "human readable" name of the stage

        Returns
        -------
        str
            The human readable name of the stage
        """

        return self._name

    @property
    def referenceName(self):

        '''Return the reference name for the stage

        This is of the form stage$i where $i is stage index.
        This form for this name should never change

        The allows other classes to use this name to refer to a
        stage without having to know the human-readable name (which may change)'''

        return "stage%d" % self.index

    @property
    def experiment(self):
        # type: () -> Experiment

        return self.weakExperiment()

    @property
    def documentDescription(self):
        """Returns a dict suitable for e.g. mongo db

        Note: The descriptions purpose is for querying data"""

        return [el.documentDescription for el in self.jobs()]


