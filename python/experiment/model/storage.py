# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston


'''Contains classes representing experiment on-disk storage'''

from __future__ import annotations
from __future__ import print_function

import pydantic.typing
from abc import ABCMeta, abstractmethod, abstractproperty
import six

import datetime
import errno
import getpass
import logging
import os
import pprint
import re
import shutil
import threading
import uuid
from typing import TYPE_CHECKING, List, Tuple, Dict, Optional, Union, Any

from future.utils import raise_with_traceback

import experiment.utilities.fsearch
import experiment.model.conf
import experiment.model.errors

if TYPE_CHECKING:
    from experiment.model.frontends.flowir import DictManifest


def partition_uri(uri: Optional[str]) -> Tuple[str, str]:
    """Split a URI to its components

    Args:
        uri(str): A file URI (file://$GATEWAY_ID/$ABSOLUTE_PATH)

    Returns:
        A tuple of 2 strings. The first value is the GATEWAY_ID component (can be '' if empty) and the second
        the $ABSOLUTE_PATH component.
    """

    if uri.startswith('file://'):
        uri = uri[7:]
        gateway, path = uri.split('/', 1)
        path = ''.join(('/', path))

        return gateway, path

    return '', uri


def generate_uri(gateway_id: Optional[str], path: str) -> str:
    """Generates a uri from its gateway and path components

    Args:
        gateway_id(str): A gateway id (name must conform to domain rules i.e. no / symbols)
        path(str): Absolute path to a file
    """
    if path.startswith('/'):
        path = path[1:]

    gateway_id = gateway_id or ''

    if '/' in gateway_id:
        raise ValueError("Gateway id \"%s\" is invalid (contains illegal character '/'")

    path = 'file://%s/%s' % (gateway_id, path)

    return path


def generate_elaunch_metadata_dictionary(
        platform: str, pid: int, cmdline: str, inputs: List[str], data: List[str],
        user_variables: Dict[str, Dict[str, Any]], variables: Dict[str, Dict[str, Any]],
        version: str, hybrid_platform: str, user_metadata: Dict[str, Any],
        instance_name: str):
    """Returns the contents of the Dictionary which workflow instances store under ${INSTANCE_DIR}/elaunch.yaml

    Args:
        platform: name of FlowIR platform
        pid: process ID of elaunch.py
        cmdline: commandline of elaunch.py
        inputs: list of input files to workflow instance
        data: list of files that override ones bundled in data of the workflow definition
        user_variables: user variables {'global':{name:value}, 'stages':{index:{name:value}}}
        variables: Workflow variables {'global':{name:value}, 'stages':{index:{name:value}}}
        version: version of elaunch
        hybrid_platform: Name of hybrid platform (e.g. paragon) - used to be used with LSF, deprecated
        user_metadata: A dictionary with key(str): Any value pairs that users can provide at launch
        instance_name: the name of the workflow instance directory
        interface: the interface of the virtual experiment
    """
    elaunch_state = {
        'pid': pid,
        'platform': platform,
        'arguments': cmdline,
        'inputs': inputs or [],
        'data': data or [],
        'userVariables': user_variables or {},
        'variables': variables or {},
        'version': version,
        'hybridPlatform': hybrid_platform or None,
        'userMetadata': user_metadata,
        'instanceName': instance_name,
    }

    return elaunch_state


def IsExperimentTopLevel(dirpath):

    '''Returns True if dirpath is the top level of an instance/package experiment dir'''

    confpath = os.path.join("conf", "experiment.conf")
    confpath = os.path.join(dirpath, confpath)

    flowirpath = os.path.join('conf', 'flowir_package.yaml')
    flowirpath = os.path.join(dirpath, flowirpath)

    flowir_instance_path = os.path.join('conf', 'flowir_instance.yaml')
    flowir_instance_path = os.path.join(dirpath, flowir_instance_path)

    exists = list(map(os.path.exists, [confpath, flowirpath, flowir_instance_path]))

    retval = any(exists)

    return retval

def IsExperimentInstanceDirectory(dirpath):

    '''Returns True if dirpath is the top level of an instance experiment dir'''

    stagePath = os.path.join(dirpath, 'stages')

    retval = False
    if os.path.exists(stagePath):
        retval = True

    return retval

def FindExperimentTopLevel():

    '''Find the top-level dir of an experiment package/instance from the current path

    If the current working dir is not inside an experiment this function returns None'''

    d = os.getcwd()
    components = d.split(os.path.sep)

    toplevel = None
    #Walk up components
    for i in range(len(components), 0, -1):
        d = os.path.sep.join(components[:i])
        if IsExperimentTopLevel(d):
            toplevel = d
            break

    return toplevel            

class ExperimentShadowDirectory:

    '''Represents a directory structure on a separate file-system that the experiment can use

    For robustness it can be useful for intermediate jobs to use working directories
    located on local file-system.'''

    @classmethod
    def temporaryShadow(cls, name):

        location = os.path.join('/tmp', 'chpc-%s-shadow' % getpass.getuser())
        if not os.path.exists(location):
            try:
                os.mkdir(location)
            except OSError as data:
                if data.errno != errno.EEXIST: #Another process beat us to creation since the check
                    raise experiment.model.errors.ShadowCreateError(data)

        return ExperimentShadowDirectory(name, location)

    def __init__(self, name, location):

        '''Creates experiment shadow directory at location 

        Parameters:
           location: Where to create the instance
           name: Name of the actual experiment instance

        Exceptions:

        Raises errors.ShadowLocationError if the specified location is invalid
        Raises errors.ShadowCreateError if the store can't be created at location'''

        if not os.path.isdir(location):
            raise experiment.model.errors.ShadowLocationError(location)

        self.instancePath = os.path.join(location, '%s.shadow' % name)
        self.containingDir = location
        currentDirectory = os.getcwd()

        os.mkdir(self.instancePath)

        #Create sub directories
        try:
            os.chdir(self.instancePath)
            os.mkdir("stages")
            os.mkdir("output")
            os.mkdir("temp")
        except OSError as data:
            raise experiment.model.errors.ShadowCreateError(data)
        finally:
            os.chdir(currentDirectory)

        self.stageDir = os.path.join(self.location, 'stages')      
        self.outputDir = os.path.join(self.location, 'output')      
        self.tempDir = os.path.join(self.location, 'temp')

    @property
    def location(self):

        '''Returns the receivers location'''

        return self.instancePath

    def directoryForStage(self, stageIndex):

        '''Returns the directory for stage \e stageIndex
        
        Parameters:

        stageIndex - The stageIndex of the stage'''

        return os.path.join(self.stageDir, 'stage%d' % stageIndex)

    def directoryForJob(self, stageIndex, jobName):

        '''Returns the directory for the job in the given stage
        
        Parameters:

        stageIndex - The stageIndex of the stage.
        jobName - The name of the job'''

        directory = self.directoryForStage(stageIndex)
        directory = os.path.join(directory, jobName)

        return directory

    def createStageDirectory(self, stageIndex):

        '''Creates the directory for stage \e stageIndex
        
        Parameters:
            stageIndex - The index of the stage.
        
        Returns:
            The path to the stage directory.
            Note: If the directory already exists this method does nothing.
            
        Exceptions:
            Raises OSError if the directory could not be created.'''

        directory = self.directoryForStage(stageIndex)
        if not os.path.exists(directory):
            os.mkdir(directory)

        return directory    

    def createJobDirectory(self, stageIndex, jobName):

        '''Creates the directory for a job in a given stage
        
        Parameters:

        stageIndex - The index of the stage the job is in
        jobName - The name of the job
        
        Returns:
            The path to the job directory.
            Note: If the directory already exists this method does nothing.
            
        Exceptions:
            Raises OSError if the directory could not be created.'''

        directory = self.directoryForJob(stageIndex, jobName)
        if not os.path.exists(directory):
            os.mkdir(directory)

        return directory    

    def temporaryFile(self):

        tempname = str(uuid.uuid4())
        tempname = os.path.join(self.tempDir, tempname)

        return tempname

    def clean(self):

        shutil.rmtree(self.instancePath)


@six.add_metaclass(ABCMeta)
class StorageStructurePathResolver:

    '''StorageStructurePathResolvers can be concrete or virtual.
    The paths returned by virtual implementations are not guaranteed to exist.
    The type of the receiver is given by the `isConcrete` property.
    This can be used to gate operations requiring paths to exist or consumers can simply reply on the fact that such
    operations will raise OSErrors to signal where a virtual  StorageStructurePathResolver is being used where a
    concrete one is required

    Methods used by WorkflowGraph operations when the need information on storage structure

    WorkflowGraph components can contain relative paths. Hence these need to be resolved w.r.t the
    storage the eventual storage structure of the workflow'''

    @abstractproperty
    def manifestData(self) -> Dict[str, str]:
        """Returns the contents of the manifest

        The manifest is a dictionary, with targetFolder: sourceFolder entries. Each
        sourceFolder will be copied or linked to populate the respective targetFolder. Source folders can be
        absolute paths, or paths relative to the path of the FlowIR YAML file. SourceFolders may also be
        suffixed with :copy or :link to control whether targetFolder will be copied or linked to sourceFolder
        (default is copy). TargetFolders are interpreted as relative paths under the instance directory. They
        may also be the resulting folder name of some applicationDependency. They may include a path-separator
        (.e.g /) but must not be absolute paths.
        """

    @abstractproperty
    def location(self):
        # type: ()-> str
        '''Returns the receivers location

        This is the "main" directory of the workflow storage

        This property should ONLY be used where a default directory is needed to perform some operation
        e.g. execute resolve bash expressions.

        To find resolve a relative path in a specification use resolvePath
        '''

    @abstractmethod
    def resolvePath(self, path):

        '''Resolves a relative path in a workflow specification to its actual location

        NOTE: As is this method is only used to get path to direct references

        Parameters:
            path - A path

        Returns:
            The absolute path'''

        pass

    @abstractmethod
    def workingDirectoryForComponent(self, stageIndex, componentName):

        '''Returns a path of a working directory for the component in the given stage

        Existence of the returned path depends on the receivers implementation.
        The `isConcrete` method can be used to check

        Parameters:
            stageIndex - The stageIndex of the stage.
            componentName - The name of the component'''
        pass

    @abstractproperty
    def isConcrete(self):

        '''Returns True if the paths returned by the receiver are expected to exist

        The storage structure represented by an object implementing StorageStructurePathResolver may be virtual.
        That is they represent a potential locations where workflows could store output.
        This is useful for validating and exploring the workflow graph w.r.t a storage structure without having
        to create it'''

        pass


class ExperimentPackage(StorageStructurePathResolver):

    def __init__(
            self,
            flow_ir_conf: experiment.model.conf.FlowIRExperimentConfiguration,
            manifest: Optional[DictManifest]
    ):
        """
        A package consists of a FlowIRConfiguration object and a manifest

        Args:
            flow_ir_conf:
            manifest:
        """
        # VV: FIXME This does not work for "yaml" definition of Workflows which also use DoWhile documents
        manifest = manifest or {}
        self.flow_ir_conf = flow_ir_conf

        # VV: use self.manifestData because that takes into account the manifest of the @flow_ir_conf ivar
        self._manifest = {str(x): str(manifest[x]) for x in manifest}

    @property
    def manifestData(self) -> Dict[str, str]:
        if self._manifest:
            manifest = self._manifest
        else:
            manifest = self.flow_ir_conf.manifestData

        return {str(x): str(manifest[x]) for x in manifest}

    def manifest_clear(self):
        self._manifest.clear()

    def manifest_update(self, delta: "DictManifest"):
        # VV: First copy the current manifestData (which could be coming straight from self.flow_ir_conf) then
        # update it with the delta
        self._manifest.clear()
        self._manifest.update(self.manifestData)
        self._manifest.update(delta.copy())

    @classmethod
    def packageFromLocation(
            cls,
            location: str,
            manifest: Optional[Union[str, experiment.model.frontends.flowir.DictManifest]] = None,
            platform: Optional[str] = None,
            validate: bool = True,
            createInstanceFiles: bool = False,
            updateInstanceFiles: bool = False,
            is_instance: bool = False,
            variable_substitute: bool = False,
            **kwargs
    ):
        """
        Returns an ExperimentPackage for the given location

        Args:
            location: Path to the the workflow definition
                e.g. A Package directory with dosini
                     A Package directory with conf/flowir_package.yaml
                     The path to a Yaml file containing FlowIR
                     ....
            manifest: Optional path to YAML file containing manifest OR contents of the manifest.
                The manifest is a dictionary, with targetFolder: sourceFolder entries. Each
                sourceFolder will be copied or linked to populate the respective targetFolder. Source folders can be
                absolute paths, or paths relative to the path of the FlowIR YAML file. SourceFolders may also be
                suffixed with :copy or :link to control whether targetFolder will be copied or linked to sourceFolder
                (default is copy). TargetFolders are interpreted as relative paths under the instance directory. They
                may also be the resulting folder name of some applicationDependency. They may include a path-separator
                (.e.g /) but must not be absolute paths.
            platform: Name of platform (defaults to `default`)
            validate: When True this method will raise exception if configuration is invalid
            createInstanceFiles: If set to True will auto-generate instance files provided that they do not
                already exist. Set updateInstanceFiles to True too to update existing files.
            updateInstanceFiles(bool): Set to true to update instance files if they already exist
            is_instance: Indicates whether to load configuration files from an existing instance directory.
            variable_substitute: Whether to perform variable substitution, optional for a primitive graph
                but required for a replicated one
            kwargs: Extra arguments to experiment.model.conf.ExperimentConfigurationFactory.configurationForExperiment

        Raises:
            experiment.errors.ExperimentUndefinedPlatformError: if the experiment does not have the requested platform
            experiments.errors.ExperimentMissingConfigurationError: If configuration does not exist and validate is True
        """

        conf = experiment.model.conf.ExperimentConfigurationFactory.configurationForExperiment(
            location, platform=platform, manifest=manifest, validate=validate,
            createInstanceFiles=createInstanceFiles, updateInstanceFiles=updateInstanceFiles,
            variable_substitut=variable_substitute,
            is_instance=is_instance, **kwargs)

        # VV: ExperimentPackage uses the manifest defined of the FlowIRExperimentConfiguration object conf
        return cls(conf, None)

    @property
    def configuration(self) -> "experiment.model.conf.FlowIRExperimentConfiguration":
        return self.flow_ir_conf

    @property
    def location(self) -> str:
        '''Returns the receivers location

        This is the "main" directory of the workflow storage

        This property should ONLY be used where a default directory is needed to perform some operation
        e.g. execute resolve bash expressions.

        To find resolve a relative path in a specification use resolvePath
        '''
        return self.flow_ir_conf.location

    def resolvePath(self, path):
        # type: (str) -> str
        '''Resolves path relative to the top level of the instance directory

        Parameters:
            path - A path

        Returns:
            The absolute path'''

        path = os.path.join(self.location, path)
        path = os.path.normpath(path)

        return path

    def workingDirectoryForComponent(self, stageIndex, componentName):
        '''Returns a path of a working directory for the component in the given stage

        Existence of the returned path depends on the receivers implementation.
        The `isConcrete` method can be used to check

        Parameters:
            stageIndex - The stageIndex of the stage.
            componentName - The name of the component'''
        pass

    def isConcrete(self):
        '''Returns True if the paths returned by the receiver are expected to exist

        The storage structure represented by an object implementing StorageStructurePathResolver may be virtual.
        That is they represent a potential locations where workflows could store output.
        This is useful for validating and exploring the workflow graph w.r.t a storage structure without having
        to create it'''

        pass

    def expandPackageToDirectory(
            self,
            targetPath: str,
            file_format: Optional[pydantic.typing.Literal["flowir", "dsl"]]  = None
    ):

        """Expands the contents of the package to a new directory

        Notes:

        If ExperimentPackage is not created by a FlowIR YAML file this method uses shutil.copytree

        Args:
            targetPath:
                Path of the instance directory
            file_format:
                The file format of a package which consists of a single file. Defaults to "flowir"

        """

        path = self.location
        manifest = self.manifestData
        logger = logging.getLogger('storage.ExperimentPackage.deploy')

        logger.info(f"Deploying {path} to {targetPath} - isExperimentPackageDirectory: "
                    f"{self.configuration.isExperimentPackageDirectory}, manifest: {self.manifestData}")

        if not self.configuration.isExperimentPackageDirectory:
            try:
                if manifest:
                    logger.info("Loaded manifest %s" % pprint.pformat(manifest))
                    flowir_dir = os.path.abspath(os.path.dirname(path))

                    for targetFolder in manifest:
                        sourceFolder = manifest[targetFolder]

                        try:
                            sourceFolder, method = sourceFolder.rsplit(':', 1)
                        except ValueError:
                            method = 'copy'

                        if os.path.isabs(sourceFolder) is False:
                            sourceFolder = os.path.join(flowir_dir, sourceFolder)
                        manifest[targetFolder] = ':'.join((sourceFolder, method))

                    logger.info("Rewrote manifest to %s" % pprint.pformat(manifest))
                else:
                    manifest = {}

                if not os.path.exists(targetPath):
                    os.makedirs(targetPath)

                # VV: now populate the targetPath using the targetFolder: sourceFolder instructions of manifest
                for targetFolder in manifest:
                    sourceFolder = manifest[targetFolder]

                    if os.path.isabs(targetFolder):
                        raise ValueError("Manifest entry %s (%s) should not be an absolute path" % (
                            targetFolder, sourceFolder))

                    sourceFolder, method = sourceFolder.rsplit(':', 1)
                    target_folder_path = os.path.join(targetPath, targetFolder)

                    if method == 'copy':
                        logger.info("Copying %s to %s" % (sourceFolder, targetFolder))
                        shutil.copytree(sourceFolder, target_folder_path)
                    elif method == 'link':
                        logger.info("Linking %s to %s" % (target_folder_path, targetFolder))
                        os.symlink(sourceFolder, target_folder_path)
                    else:
                        raise ValueError("Unknown manifest folder method for %s: %s" % (targetFolder, sourceFolder))

                conf_dir = os.path.join(targetPath, "conf")

                if 'conf' not in manifest:
                    # VV: It's OK for the conf folder to already exist, it could have commonly used pipeline definitions
                    # in it which the flowir we're copying into the conf dir $imports
                    os.makedirs(conf_dir)
                if file_format == "dsl":
                    shutil.copyfile(path, os.path.join(conf_dir, "dsl.yaml"))
                else:
                    shutil.copyfile(path, os.path.join(conf_dir, "flowir_package.yaml"))
            except OSError as e:
                raise_with_traceback(experiment.model.errors.PackageCreateError(e, targetPath, path))

        else:
            def ignore(dirname, contents):

                basename = os.path.split(dirname)[1]
                if 'deploy' in basename:
                    return contents

                return []

            shutil.copytree(self.location, targetPath, True, ignore)

# class ExperimentPackageDirectory(StorageStructurePathResolver):
#     '''Represents an experiment package directory
#
#     This class encapsulates details about the structure of the package
#     directory and the names of files'''
#
#
#     def __init__(self, packagePath, stageConfigurations=None,
#                            experimentConfigurations={},
#                            variableConfigurations = {}, manifest=None):
#
#         '''Instantiates an ExperimentPackageDirectory object based on a directory at packagePath
#
#         If stageConfigurations is None the receiver 'reads' an existing package dir.
#         In this case packagePath must exist
#
#         If stageConfigurations is not None the receiver creates a package dir
#         In this case packagePath cannot exist
#
#         Note: The other optional parameters require stageConfigurations to be non-None to have an effect.
#
#         Parameters:
#            packagePath: Path to the experiment package
#             stageConfigurations (list):
#                 A list of ConfigParser.SafeConfigParser objects. One for each stage, in order
#             experimentConfigurations (dictionary) : A dictionary of {platform:SafeConfigParser} pairs.
#                 Optional. The default object should be under the key "default"
#             variableConfigurations (dictionary) : A dictionary of {platform:ConfigParser.SafeConfigParser} pairs.
#                 Optional. The default object should be under the key "default"
#             manifest(str): Optional path to manifest YAML file to use when setting up package directory from a FlowIR
#                 YAML file. The manifest should contain a dictionary, with targetFolder: sourceFolder entries. Each
#                 sourceFolder will be copied or linked to populate the respective targetFolder. Source folders can be
#                 absolute paths, or paths relative to the path of the FlowIR YAML file. SourceFolders may also be
#                 suffixed with :copy or :link to control whether targetFolder will be copied or linked to sourceFolder
#                 (default is copy). TargetFolders are interpreted as relative paths under the instance directory. They
#                 may also be the resulting folder name of some applicationDependency. They may include a path-separator
#                 (.e.g /) but must not be absolute paths.
#
#         Exceptions:
#
#         Raises errors.InstanceLocationError if the specified location is invalid'''
#
#         self.log = logging.getLogger('storage.packagedir')
#
#         self.packagePath = GetAndSetupPackagePath(packagePath, manifest, self.log)
#         self.containingDir = os.path.split(self.packagePath)[0]
#
#         self.dataDir = os.path.join(self.location, 'data')
#         self.hooksDir = os.path.join(self.location, 'hooks')
#         self.binDir = os.path.join(self.location, 'bin')
#
#         if stageConfigurations is not None:
#             if  os.path.isdir(self.packagePath):
#                 raise errors.InstanceLocationError(self.packagePath)
#             else:
#                 os.mkdir(self.packagePath)
#                 os.mkdir(self.configurationDirectory)
#
#                 #Write experiment conf files
#                 for platform in list(experimentConfigurations.keys()):
#                     outfile = "experiment.conf" if platform == 'default' else "experiment.%d.conf" % platform
#                     with open(os.path.join(self.configurationDirectory, outfile), 'w') as f:
#                         experimentConfigurations[platform].write(f)
#
#                 os.mkdir(self.stageConfigurationDirectory)
#                 for i, conf in enumerate(stageConfigurations):
#                     path = os.path.join(self.stageConfigurationDirectory, 'stage%d.conf' % i)
#                     with open(path, 'w') as f:
#                         conf.write(f)
#
#                 #Create variables dirs and write variables conf files
#                 variablesDir = os.path.join(self.configurationDirectory, 'variables.d')
#                 os.mkdir(variablesDir)
#
#                 for platform in list(variableConfigurations.keys()):
#                     if platform == 'default':
#                         outfile = "variables.conf "
#                     else:
#                         outfile = os.path.join("variables.d/%d.conf" % platform)
#
#                     with open(os.path.join(self.configurationDirectory, outfile), 'w') as f:
#                         variableConfigurations[platform].write(f)
#
#                 os.mkdir(self.dataDir)
#                 os.mkdir(self.hooksDirectory)
#         else:
#             if not os.path.isdir(self.packagePath):
#                 raise errors.InstanceLocationError(self.packagePath)
#
#     @property
#     def name(self):
#         basename = os.path.split(self.packagePath)[1]
#         name = os.path.splitext(basename)[0]
#         return name
#
#     @property
#     def location(self):
#
#         '''Returns the receivers location'''
#
#         return self.packagePath
#
#     @property
#     def executablesDirectory(self):
#         # no usage
#         '''Returns the receivers directory for storing executables'''
#
#         return self.binDir
#
#
#     @property
#     def hooksDirectory(self):
#         # used in stageconfigurations
#         '''Returns the receivers hooks module location'''
#
#         return self.hooksDir
#
#     @property
#     def configurationDirectory(self):
#         # used in stageConfigurationDirectory
#         '''Returns the configuration directory'''
#
#         return os.path.join(self.location, 'conf')
#
#     @property
#     def stageConfigurationDirectory(self):
#         # used in stageconfigurations
#         '''Returns where the stage configurations are stored'''
#
#         return os.path.join(self.configurationDirectory, 'stages.d')
#
#     @property
#     def numberStageConfigurationFiles(self):
#         # no usage
#         '''Returns the number of stage configuration files in the instance'''
#
#         contents = os.listdir(self.stageConfigurationDirectory)
#         stageRe = re.compile(r"stage([0-9]+).conf")
#         return len([name for name in contents if stageRe.match(name) is not None])
#
#     def configurationFileForStage(self, stageIndex):
#         # no usage
#         '''Returns the path to the configuration file for given stageIndex.
#
#         Exceptions:
#
#         Raises ExperimentMissingConfiguration error if the no file exists for given index'''
#
#         path = os.path.join(self.stageConfigurationDirectory, 'stage%d.conf' % stageIndex)
#         if not os.path.exists(path):
#             raise errors.ExperimentMissingConfigurationError('No configuration file for stage %d (%s)' % (
#             stageIndex, path))
#
#         return path
#
#     def instanceConfigurationFileForStage(self, stageIndex):
#         # no usage
#         '''Returns the path to the  instance specific configuration file for the given stageIndex.
#
#         See experimentInstanceConfigurationFile for notes re: exceptions
#
#         Exceptions:
#
#         None'''
#
#         path = os.path.join(self.stageConfigurationDirectory, 'stage%d.instance.conf' % stageIndex)
#
#         return path
#
#     def configurationFileForPlatform(self, platform):
#         # no usage
#         '''Returns the path to the configuration file for platform.
#
#         Returns None if there is no such file'''
#
#         path = os.path.join(self.configurationDirectory, 'experiment.%s.conf' % platform)
#         if not os.path.exists(path):
#             path = None
#
#         return path
#
#     @property
#     def experimentConfigurationFile(self):
#         # no usage
#         '''Returns the path to the experiments configuration file
#
#         Exceptions:
#
#         Raises ExperimentMissingConfiguration if the main configuration file does not exist'''
#
#         path = os.path.join(self.configurationDirectory, 'experiment.conf')
#         if not os.path.exists(path):
#             raise errors.ExperimentMissingConfigurationError('Experiment configuration file missing (%s)' % path)
#
#         return path
#
#     @property
#     def outputConfigurationFile(self):
#         # no usage
#         '''Returns the path to the experiments output configuration file
#
#         Exceptions:
#
#         Raises ExperimentMissingConfiguration if the output configuration file does not exist'''
#
#         path = os.path.join(self.configurationDirectory, 'output.conf')
#         if not os.path.exists(path):
#             raise errors.ExperimentMissingConfigurationError('Experiment configuration file missing (%s)' % path)
#
#         return path
#
#     @property
#     def statusConfigurationFile(self):
#         # no usage
#         '''Returns the path to the experiments status configuration file
#
#         Exceptions:
#
#         Raises ExperimentMissingConfiguration if the status configuration file does not exist'''
#
#         path = os.path.join(self.configurationDirectory, 'status.conf')
#         if not os.path.exists(path):
#             raise errors.ExperimentMissingConfigurationError('Experiment configuration file missing (%s)' % path)
#
#         return path
#
#     @property
#     def variableDirectories(self):
#         # no usage
#         return [self.configurationDirectory]
#
#     @property
#     def platforms(self):
#         # used in epatch-apply.py
#         import re
#
#         regex = re.compile(r"\w+\.(\w+)\.conf")
#         return [regex.match(f).group(1) for f in os.listdir(self.configurationDirectory) if regex.match(f) is not None]
#
#     def resolvePath(self, path):
#         # used in epatch.py
#
#         '''Resolves path relative to the top level of the package directory
#
#         Parameters:
#             path - A path
#
#         Returns:
#             The absolute path'''
#
#         path = os.path.join(self.location, path)
#         path = os.path.normpath(path)
#
#         return path
#
#     def workingDirectoryForComponent(self, stageIndex, componentName):
#
#         '''Returns a path of a working directory for the component in the given stage
#
#         Existence of the returned path depends on the receivers implementation.
#         The `isConcrete` method can be used to check
#
#         Parameters:
#             stageIndex - The stageIndex of the stage.
#             componentName - The name of the component'''
#         pass
#
#     def isConcrete(self):
#
#         '''Returns True if the paths returned by the receiver are expected to exist
#
#                The storage structure represented by an object implementing StorageStructurePathResolver may be virtual.
#                That is they represent a potential locations where workflows could store output.
#                This is useful for validating and exploring the workflow graph w.r.t a storage structure without having
#                to create it'''
#
#         return False
#
#     def relativePath(self, path):
#         # no usage
#         '''Returns a relative filepath to path from package directory location.
#
#         Note: This method does not check that the path exists'''
#
#         return os.path.relpath(path, self.containingDir)


class ExperimentInstanceDirectory(StorageStructurePathResolver):

    '''Represents an experiment instance directory

    This class encapsulates details about the structure of the instance
    directory and the names of files'''

    @classmethod
    def newInstanceDirectory(cls,
                             location,  # type: str
                             package,  # type: ExperimentPackage
                             stamp=True,  # type: bool
                             name=None  # type: str
                             ):

        '''Creates experiment instance directory at location based on the package at packagePath

        This involves copying data from the package to a new directory created under location/

        Parameters:
           location: Where to create the instance
           package: The experiment package the instance is based on (assumes directory package)
           stamp: If True a timestamp is added to the instance directory name
           name: if not none instead of using Path(packagePath).with_suffix('.instance') name will be used
                 can be combined with stamp
        Exceptions:

        Raises:
            experiment.errors.PackageLocationError: if the package can't be found
            experiment.errors.PackageFormatError: if the layout of package is incorrect.
            experiment.errors.InstanceLocationError: if the specified location is invalid
            experiment.errors.InstanceCreateError: if the store can't be created at location'''

        # packageLocation = packagePath.location

        # if not packagePath.configuration.deployed():
        #     packagePath.deploy()
        #     packageLocation = packagePath.location

        if not os.path.isdir(location):
            raise experiment.model.errors.InstanceLocationError(location)

        if name is None:
            #Remove trailing / if present in path
            packageLocation = package.location
            if package.location[-1] == os.path.sep:
                packageLocation = package.location[:-1]

            #Instance location
            path, dirname = os.path.split(packageLocation)
            packageName = os.path.splitext(dirname)[0]
        else:
            packageName = name
        
        #Create timestamp without colons in time (isoformat) as
        #these are traditionally not expected in paths
        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H%M%S.%f")
        uid_name = '%s-%s' % (packageName, timestamp)
        if stamp is True:
            instancePath = os.path.join(location, '%s.instance' % uid_name)
        else:    
            instancePath = os.path.join(location, '%s.instance' % packageName)

        shadowDir = ExperimentShadowDirectory.temporaryShadow(uid_name)

        try:
            package.expandPackageToDirectory(instancePath, package.configuration.file_format)
        except Exception as data:
            raise experiment.model.errors.InstanceCreateError(data, location=instancePath)

        currentDirectory = os.getcwd()

        #Create sub directories
        try:
            os.chdir(instancePath)
            os.mkdir("input")
            os.mkdir("stages")
            #os.mkdir("output")
            os.symlink(shadowDir.outputDir, "output")
        except OSError as data:
            raise experiment.model.errors.InstanceCreateError(data, location=instancePath)
        finally:
            os.chdir(currentDirectory)

        try:
            directory = ExperimentInstanceDirectory(instancePath, package.location, shadowDir,
                                                    manifest=package.manifestData)
        except OSError as data:
            raise experiment.model.errors.InstanceCreateError(data, location=instancePath)

        return directory

    def __init__(self, instancePath, packagePath=None, shadowDir=None, ignoreExisting=False, touch=False,
                 attempt_shadowdir_repair=True, manifest=None):

        '''Instantiates an ExperimentInstanceDirectory object based on the directory at instancePath

        The instance directory should have be originally created via a call to ExperimentInstanceDirectory.newInstanceDirectory()

        Parameters:
           instancePath: Path to the experiment instance
           packagePath: The experiment package the instance is based on - can be None
           shadowDir: An ExperimentShadowDirectory representing a shadow of current - can be None
           ignoreExisting: If True then files/data present in job directories is not flagged as job input
           touch: If true a hidden file '.touch.txt' is created when a job working directory is created.
            The point of this parameter is to ensure JobWorkingDirectory are never empty, which has an
            impact for some data transfer methods.
            This parameter is for testing - it may be removed
           attempt_shadowdir_repair: Set to true to re-create broken symlinks to `output` shadow dir
           manifest: The manifest is a dictionary, with targetFolder: sourceFolder entries. Each
            sourceFolder will be copied or linked to populate the respective targetFolder. Source folders can be
            absolute paths, or paths relative to the path of the FlowIR YAML file. SourceFolders may also be
            suffixed with :copy or :link to control whether targetFolder will be copied or linked to sourceFolder
            (default is copy). TargetFolders are interpreted as relative paths under the instance directory. They
            may also be the resulting folder name of some applicationDependency. They may include a path-separator
            (.e.g /) but must not be absolute paths.

        Note:

        The ignoreExisting parameter is passed to each JobWorkingDirectory object on its creation.
        If True its meaning is "do not assume anything about files already in the directory" 

        By default JobWorkingDirectory objects assume that such files are job input files (ignoreExisting=Flase).
        This assumption affects the attributes, output/input, and the methods outputBeforeDate, outputSinceDate.
        
        This behaviour may not be what is required if ExperimentInstanceDirectory is instantiated using a directory
        which already contains output from jobs. 
        In this case the default value of ignoreExisting would cause the output from the jobs would be treated as input and 
        queries on output would not return any data.

        Exceptions:

        Raises errors.InstanceLocationError if the specified location is invalid'''
        self.attempt_shadowdir_repair = attempt_shadowdir_repair
        self.log = logging.getLogger('storage.instancedir')

        if not os.path.isdir(instancePath):
            raise experiment.model.errors.InstanceLocationError(instancePath)

        self.mtx_output = threading.RLock()
        self.packagePath = packagePath  
        self.instancePath = instancePath
        self.containingDir = os.path.split(self.instancePath)[0]
        self.shadowDir = shadowDir
        self.ignoreExisting = ignoreExisting

        self._manifest = experiment.model.frontends.flowir.Manifest(manifest)

        self.inputDir = os.path.join(self.location, 'input')      
        self.dataDir = os.path.join(self.location, 'data')      
        self.stageDir = os.path.join(self.location, 'stages')      
        self.outputDir = os.path.join(self.location, 'output')      
        self.hooksDir = os.path.join(self.location, 'hooks')

        #Check if output dir valid
        #i.e. if its a symlink check the path exists
        #NOTE: All following assumes nothing else will try to work on the link
        with self.mtx_output:
            if os.path.islink(self.outputDir):
                if not os.path.exists(os.readlink(self.outputDir)):
                    if self.attempt_shadowdir_repair:
                        self.log.warning('Output path %s is a broken symlink - attempt to fix' % self.outputDir)
                        try:
                            os.unlink(self.outputDir)
                            os.mkdir(self.outputDir)
                            self.log.warning('Created new output dir at %s' % self.outputDir)
                        except OSError as error:
                            self.log.warning('Unable to unlink and recreate output dir (%s) Continuing anyway.' % error)
                    else:
                        self.log.warning('Output path %s is a broken symlink - will not fix' % self.outputDir)

        #It may be necessary to mirror the directory structure on a remote machine
        #Some transfer/mirroring mechanisms only transfer directories containing data.
        #Since the job working directories are created empty they would not be transferred.
        #The touch parameter ensures the entire instance directory structure will be
        #transferred with these mechanisms by creating a hidden file in each job working dir
        self.touch = touch

        # VV: Maintain a record of top-level folders in instance directory, can be used to validate
        # the definition of a package
        self._top_level_folders = []  # type: List[str]
        try:
            for entry in os.listdir(self.location):
                fullpath = os.path.normpath(os.path.join(self.location, entry))
                if os.path.isdir(fullpath):
                    self._top_level_folders.append(entry)
        except OSError as e:
            self.log.warning("Cannot discover folders in instance directory %s" % self.location)

    def attempt_fix_hooks_directory(self):
        """Inspect hooks directory and if it contains .py files but no __init__.py file create an __init__.py file.
        """
        if os.path.exists(self.hooksDir) is False:
            return

        found_init = False
        found_other_files = False

        for de in os.scandir(self.hooksDir):
            de: os.DirEntry = de
            if de.name not in ['.', '..', '__init__.py']:
                found_other_files = True
            if de.name == '__init__.py' and de.is_dir():
                found_init = True
            if found_other_files and found_init:
                break

        if found_other_files and (found_init is False):
            path_init = os.path.join(self.hooksDir, '__init__.py')
            with open(path_init, 'w') as f:
                f.write("\n# Automatically generated by st4sd-runtime-core\n")

    @property
    def manifestData(self) -> Dict[str, str]:
        return self._manifest.manifestData

    @property
    def top_level_folders(self):
        # type: () -> List[str]
        """A list of top level folders in the package/instance directory"""

        return self._manifest.top_level_folders

    @property
    def name(self):

        '''Returns the name of the experiment

        Currently this is the package name minus any extension if there is one.
        If the package can't be determined its the instance name minus any extension and time stamp'''
        if self.packagePath is not None:
            basename = os.path.split(self.packagePath)[1]
            name = os.path.splitext(basename)[0]
        else:
            basename = os.path.split(self.instancePath)[1]
            basename = os.path.splitext(basename)[0]
            components = basename.split("-")
            # A timestamped instance dir will contain at least three dashes in the name
            # The three dashes will be at the end
            # Splitting on dashes should give ..., [Year], [Month], [Day+Time]
            # There will be an unknown number of initial components depending on if there are dashes in the
            # package name itself
            if len(components) > 3:
                components = components[:-3]

            name = "".join(components)

        return name

    @property
    def packageLocation(self):
        # type: () -> str
        '''Returns the location of the package the receiver is based on'''

        return self.packagePath
    
    @property
    def location(self):
        # type: ()-> str
        '''Returns the receivers location'''

        return self.instancePath
    
    @property
    def hooksDirectory(self):
        # no usage
        '''Returns the receivers hooks module location'''

        return self.hooksDir

    @property
    def configurationDirectory(self):

        '''Returns the configuration directory'''

        return os.path.join(self.location, 'conf')

    @property
    def stageConfigurationDirectory(self):

        '''Returns where the stage configurations are stored'''

        return os.path.join(self.configurationDirectory, 'stages.d')

    @property
    def absoluteOutputDirectory(self):

        '''Returns the fully resolved path to the output directory.

        During an experiment run the directory inside the instance may be a link.
        This method resolves it to its true location'''

        outputDirectory = os.path.realpath(self.outputDir)
        return os.path.abspath(outputDirectory)

    @property
    def statusDatabaseLocation(self):

        '''Returns the location of the instances status db

        Note: There will be no database at this location unless the instance is run'''

        return os.path.join(self.location, 'status.db')

    def directoryForStage(self, stageIndex):

        '''Returns the directory for stage with stageIndex
        
        Parameters:

        stageIndex - The stageIndex of the stage'''

        return os.path.join(self.stageDir, 'stage%d' % stageIndex)

    def workingDirectoryForComponent(self, stageIndex, componentName):

        '''Returns a path of a working directory for the component in the given stage

        Existence of the returned path depends on the receivers implementation.
        The `isConcrete` method can be used to check

        Parameters:
            stageIndex - The stageIndex of the stage.
            componentName - The name of the component'''

        directory = self.directoryForStage(stageIndex)
        directory = os.path.join(directory, componentName)

        return directory

    def isConcrete(self):

        '''Returns True if the paths returned by the receiver are expected to exist

       The storage structure represented by an object implementing StorageStructurePathResolver may be virtual.
       That is they represent a potential locations where workflows could store output.
       This is useful for validating and exploring the workflow graph w.r.t a storage structure without having
       to create it'''

        return True

    def createStageDirectory(self, stageIndex):

        '''Creates the directory for stage \e stageIndex
        
        Parameters:
            stageIndex - The index of the stage.
        
        Returns:
            The path to the stage directory.
            Note: If the directory already exists this method does nothing.
            
        Exceptions:
            Raises OSError if the directory could not be created.'''

        directory = self.directoryForStage(stageIndex)
        if not os.path.exists(directory):
            os.mkdir(directory)

        return directory    

    def createJobDirectory(self, stageIndex, jobName):
        # no usage
        '''Creates the directory for a job in a given stage
        
        Parameters:

        stageIndex - The index of the stage the job is in
        jobName - The name of the job
        
        Returns:
            The path to the job directory.
            Note: If the directory already exists this method does nothing.
            
        Exceptions:
            Raises OSError if the directory could not be created.'''

        directory = self.workingDirectoryForComponent(stageIndex, jobName)
        if not os.path.exists(directory):
            os.mkdir(directory)

        return directory    

    def createJobWorkingDirectory(self, stageIndex, jobName):

        '''Temporary name - will replace createJobDirectory

        Parameters:

        stageIndex - The index of the stage the job is in
        jobName - The name of the job
        
        Returns:
            A JobWorkingDirectory object representing the created directory.
            Note: If the directory already exists this method will not skip
            creation just return a StageWorkingDirectory object
            
        Exceptions:
            Raises OSError if the directory could not be created.'''

        directory = self.workingDirectoryForComponent(stageIndex, jobName)
        if not os.path.exists(directory):
            os.mkdir(directory)

        obj = JobWorkingDirectory(directory, stageIndex, self, ignoreExisting=self.ignoreExisting)

        #See __init__ for details on this step
        if self.touch:
            try:
                with open(os.path.join(obj.path, '.touch.txt'), 'w') as f:
                        print('Touched', file=f)
            except Exception as error:
                self.log.warning('Unable to create touch file in %s: %s' % (directory, error))

        return obj                    

    @property
    def numberStageConfigurationFiles(self):
        # no usage
        '''Returns the number of stage configuration files in the instance'''

        contents = os.listdir(self.stageConfigurationDirectory)
        stageRe = re.compile(r"stage([0-9]+).conf")
        return len([name for name in contents if stageRe.match(name) is not None])

    def configurationFileForStage(self, stageIndex):
        # no usage
        '''Returns the path to the configuration file for given stageIndex.
        
        Exceptions:
        
        Raises ExperimentMissingConfiguration error if the no file exists for given index'''

        path = os.path.join(self.stageConfigurationDirectory, 'stage%d.conf' % stageIndex)
        if not os.path.exists(path):
            raise experiment.model.errors.ExperimentMissingConfigurationError('No configuration file for stage %d (%s)' % (stageIndex, path))

        return path

    def instanceConfigurationFileForStage(self, stageIndex):
        # no usage
        '''Returns the path to the  instance specific configuration file for the given stageIndex.

        See experimentInstanceConfigurationFile for notes re: exceptions

        Exceptions:

        None'''

        path = os.path.join(self.stageConfigurationDirectory, 'stage%d.instance.conf' % stageIndex)

        return path

    def configurationFileForPlatform(self, platform):
        # no usage
        '''Returns the path to the configuration file for platform.

        Returns None if there is no such file'''

        path = os.path.join(self.configurationDirectory, 'experiment.%s.conf' % platform)
        if not os.path.exists(path):
           path = None

        return path

    @property
    def experimentConfigurationFile(self):    
        '''Returns the path to the experiments configuration file

        Exceptions:

        Raises ExperimentMissingConfiguration if the main configuration file does not exist'''

        path = os.path.join(self.configurationDirectory, 'flowir_package.yaml')
        if not os.path.exists(path):
            raise experiment.model.errors.ExperimentMissingConfigurationError('Experiment configuration file missing (%s)' % path)

        return path

    @property
    def experimentInstanceConfigurationFile(self):

        '''Returns the path to the experiments instance specific configuration file

        Note: This method does not check for existence as this files does not exist
        when the instance directory is created from a package.

        This allows callers to use this method to obtain where the file should be written
        '''
        path = os.path.join(self.configurationDirectory, 'flowir_instance.yaml')

        return path

    @property
    def outputConfigurationFile(self):    
        # no usage
        '''Returns the path to the experiments output configuration file

        Exceptions:

        Raises ExperimentMissingConfiguration if the output configuration file does not exist'''

        path = os.path.join(self.configurationDirectory, 'output.conf')
        if not os.path.exists(path):
            raise experiment.model.errors.ExperimentMissingConfigurationError('Experiment configuration file missing (%s)' % path)

        return path

    @property
    def statusConfigurationFile(self):    
        # no usage
        '''Returns the path to the experiments status configuration file

        Exceptions:

        Raises ExperimentMissingConfiguration if the status configuration file does not exist'''

        path = os.path.join(self.configurationDirectory, 'status.conf')
        if not os.path.exists(path):
            raise experiment.model.errors.ExperimentMissingConfigurationError('Experiment configuration file missing (%s)' % path)

        return path

    @property
    def variableDirectories(self):
        # no usage
        return [self.configurationDirectory, self.inputDir]

    @property
    def platforms(self):
        logging.getLogger('storage').warning('ExperimentInstanceDirectory.platforms property is deprecated')
        import re

        regex = re.compile(r"\w+\.(\w+)\.conf")
        return [regex.match(f).group(1) for f in os.listdir(self.configurationDirectory) if regex.match(f) is not None]

    def addInputFile(self, path, rename_to=None):

        '''Copies the file at path into the input directory. Optionally renames it to @rename_to

        Exceptions:

        Raises IOError if src/dst path does not exist
        '''

        path = os.path.abspath(path)
        name = rename_to or os.path.split(path)[1]
        #Note shutil.Error is only raised during multi-file operations
        shutil.copy(path, os.path.join(self.inputDir, name))

    def updateDataFile(self, path, rename_to: str | None = None):

        '''Copies the file at path into the data directory, replacing an existing file of the same name (or rename_to)

        Exceptions:

        Raises shutil.Error if copy cannot be completed'''

        path = os.path.abspath(path)
        name = rename_to or os.path.split(path)[1]

        dst = os.path.join(self.dataDir, name)

        if os.path.exists(dst):
            shutil.copyfile(path, dst)
        else:    
            raise experiment.model.errors.DataFileUpdateError(name)

    def resolvePath(self, path):

        '''Resolves path relative to the top level of the instance directory

        Parameters:
            path - A path

        Returns: 
            The absolute path'''    

        path = os.path.join(self.location, path)
        path = os.path.normpath(path)

        return path    

    def relativePath(self, path):

        '''Returns a relative filepath to path from instance directory location.

        That is the directory containing the experimen instance

        Note: This method does not check that the path exists'''

        return os.path.relpath(path, self.containingDir)

    def consolidate(self):

        '''Moves all shadow data to main storage 

        Deletes shadow directory if successful.
        
        Exceptions:
            Raise errors.FilesystemInconsistencyError if the shadow data cannot be moved to main storage.
            Raises errors.SystemError if the shadow directory could not be removed'''

        #FIXME: At the moment this method assumes only output dir is shadowed
        #FIXME: Make this thread safe i.e were changing output dir while other process might be writing to it ...

        #Create local directory to contain files
        #Don't unlink the shadow until the files have been copied to this directory successfully

        if self.shadowDir is None:
            self.log.warning("Will not consolidate because shadow dir does not exist. "
                             "This is normal if this experiment instance was restarted.")
            return

        with self.mtx_output:
            currentDirectory = os.getcwd()
            try:
                os.chdir(self.instancePath)
                shutil.copytree(self.shadowDir.outputDir, "output-local")
                # VV: maybe check whether outputDir is a link before attempting to unlink it ?
                os.unlink(self.outputDir)
                os.rename("output-local", "output")
            except (shutil.Error, OSError) as e:
                raise_with_traceback(
                    experiment.model.errors.FilesystemInconsistencyError("Filesystem error prevented consolidation", e))
            finally:
                os.chdir(currentDirectory)

        #If the above was successful delete the shadow dir
        try:
            shutil.rmtree(self.shadowDir.instancePath)
            self.shadowDir = None
        except (shutil.Error, OSError) as error:
            raise_with_traceback(experiment.model.errors.SystemError(error))


class WorkingDirectory:

    '''Gathers methods useful on job working directory.

    Instances of this class expect the directory to exist and be readable
    for the duration of their existence. If an exception to this assumption
    is encountered a FilesystemInconsistencyError is raised - see below

    Filesystem Errors:

    On instantiation the instance checks if the directory can be listed/exists.
    If it doesn't an OSError is raised. 

    Subsequently if the directory can't be queried the error is upgraded to 
    FilesystemInconsistencyError. 
    This is so this particular case i.e. the filesystem was working now isn't
    can be identified elsewhere.'''

    def __init__(self, directory, ignoreExisting=False):
    
        '''Initialises new OutputDirectory instance

        Parameters:
            directory: A path to the output directory
            ignoreExisting: If True existing files in the dir will not be treated as inputs

        Exceptions:

        Raises OSError if directory does not exist/is not readable etc'''

        self.directory = directory
        self.ignoreExisting = ignoreExisting
        if ignoreExisting is False:
            self._inputs = os.listdir(self.directory)
        else:
            self._inputs = []

    def _listdir(self, directory):

        '''Private wrapper for listdir that changes OSErrors to errors.FilesystemInconsistencyError'''

        try:
            contents = os.listdir(directory)
        except OSError as errorObject:
            desc = "Unable to read previous readable working directory %s" % directory
            raise experiment.model.errors.FilesystemInconsistencyError(desc, errorObject)

        return contents    

    def updateInputs(self, force=False):    

        '''Updates the input file list with the current contents of the directory.

        Note: If ignoreExisting was set to True on initialisation this method does nothing unless force=True.
        This is to be consistent as otherwise this method would change the behaviour expected
        by passing ignoreExisting=True on init

        FIXME: This behaviour could cause issues ... need to find better solution.
        The reason is exists is that after stagingIn A Job object will call this method.
        However a restart could be underway (flagged by passing ignoreExisting to ExperimentInstanceDirectory)
        , meaning we don't want the existing files treated as inputs, and calling this method break that.
        Probably need to fix this at the stageIn level ...
        
        "Exceptions":

        Raises errors.FilesystemInconsistencyError if the directory contents cannot be listed'''

        if self.ignoreExisting==False or force==True:
            self._inputs = self._listdir(self.directory)

    @property
    def path(self):

        '''Returns the path to the directory'''

        return self.directory

    @property
    def allContents(self):

        '''Returns a list containing all files in all subdirs of the working dir

        Exceptions:
            TBA
        '''
        #FIXME: Add exception details

        return experiment.utilities.fsearch.Search(self.directory, testFunc=lambda x: True)


    @property
    def contents(self):

        '''Returns a list containing the directory contents

        Notes:
            The elements of the list are full paths
            Only checks files in top-level of working dir

        Exceptions:

        Raises errors.FilesystemInconsistencyError if the directory contents cannot be listed'''

        return [os.path.join(self.directory, filename) for filename in self._listdir(self.directory)]

    @property
    def inputs(self):

        '''Returns a list containing the inputs in the directory

        Notes:
            The elements of the list are full paths
            Only checks files in top-level of working dir

        Exceptions:
        
        None. The inputs are set on initialisation or on a call to updateInputs().
        Any exceptions due to inability to read directory will be set there'''

        return [os.path.join(self.directory, filename) for filename in self._inputs]

    @property
    def output(self):

        '''Returns a list of the files output by the job so far

        Note: Only checks files in top-level of working dir

        Exceptions:

        Raises errors.FilesystemInconsistencyError if the directory contents cannot be listed'''

        return [os.path.join(self.directory, filename) for filename in self._listdir(self.directory) if filename not in self._inputs]

    def outputSinceDate(self, date):

        '''Returns a list of the files output by the job sinde date.
        
        Parameters: 
            date: A datetime.datetime object giving the date.

        Notes:
            This currently checks for modified files not new files
            Only checks files in top-level of working dir

        Exceptions: 

        Raises errors.FilesystemInconsistencyError if there is an error querying the directory'''

        result = []
        for filename in self.output:
            mtime = os.path.getmtime(filename)
            d1 = datetime.datetime.fromtimestamp(mtime)
            if d1 > date:
                result.append(filename)
               
        return result

    def isUpdatedSinceDate(self, date, filename):

        '''Returns True if a file with a given name has been updated by the job sinde date.

        Note: Only checks files in top-level of working dir

        Parameters: 
            date: A datetime.datetime object giving the date.
            filename: The file to check

        If the file does not exist in the directory this method returns False    

        Exceptions: 

        Raises errors.FilesystemInconsistencyError if there is an error querying the directory'''

        if filename in self.output:
            mtime = os.path.getmtime(filename)
            d1 = datetime.datetime.fromtimestamp(mtime)
            if d1 > date:
                return True
               
        return False

    def outputBeforeDate(self, date):

        '''Returns a list of the files created by the job before date 

        In this case "creation" time is the value returned by getctime

        Note: Only checks files in top-level of working dir

        Parameters: 
            date: A datetime.datetime object giving the date.

        Exceptions: 

        Raises errors.FilesystemInconsistencyError if there is an error querying the directory'''

        result = []
        for filename in self.output:
            ctime = os.path.getctime(filename)
            d1 = datetime.datetime.fromtimestamp(ctime)
            if d1 < date:
                result.append(filename)
               
        return result

class JobWorkingDirectory(WorkingDirectory):

    '''Extends WorkingDirectory with some Stage/Experiment specific features
    
    This is essentially a WorkingDirectory object that knows its part of an Experiment.

    Note: There can be multiple StageWorkingDirectory objects for the same directory.'''

    def __init__(self, directory, stageIndex, experimentInstance, ignoreExisting=False):

        '''Initialises new JobWorkingDirectory instance

        Parameters:
            directory: A path to the output directory
            stageIndex: The stage the working directory is in
            experimentInstance: The ExperimentInstanceDirectory object representing
                the directory this newly created object is under
            ignoreExisting: If True existing files in the dir will not be treated as inputs

        Exceptions:

        Raises OSError if directory does not exist/is not readable etc'''

        WorkingDirectory.__init__(self, directory, ignoreExisting=ignoreExisting)
        self._stageIndex = stageIndex
        self._experimentInstance = experimentInstance


    @property     
    def stageIndex(self):

        return self._stageIndex

    @property
    def experimentDirectory(self):

        '''Returns the ExperimentInstanceDirectory object the reciever is under'''

        return self._experimentInstance
