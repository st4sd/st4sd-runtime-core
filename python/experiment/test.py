# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

'''Classes/Functions for testing an experiment package'''
from __future__ import print_function
from __future__ import annotations

import logging
import os
import traceback
from typing import List, Optional, Dict, Union, Tuple

import experiment.model.conf
import experiment.model.data
import experiment.model.errors
import experiment.model.storage

rootLogger = logging.getLogger()


def ValidatePackage(
        packagePath: str,
        location: str | None = None,
        timestamp: bool = True,
        platform: str | None =None,
        inputs: List[str] | None = None,
        data: List[str] | None =None,
        variables: str | None = None,
        createApplicationLinks:bool = True,
        createVirtualEnvLinks:bool = True,
        format_priority: List[str] | None = None,
        instance_name: str | None = None,
        custom_application_sources: Dict[str, str] | None =None,
        manifest: str | Dict[str, str] | None = None,
        ) -> Tuple[bool, Exception | None, experiment.model.data.Experiment | None]:
    """Checks is packagePath refers to a valid package

    This package runs a number of tests on the package.
    The results of the test are written to stderr.

    Args:
        packagePath: The path to a computational experiment package.
        location: The path in which to create the experiment data directory
        timestamp: If true the instance directory name will include a timestamp
        platform: The platform to create the experiment instance for
        inputs: List of input files for the new experiment (if path contains : then treat it as $sourceFile:$targetFile)
        data: List of data files for the new experiment - will replace package files of same name in data/ dir
             (if path contains : then treat it as $sourceFile:$targetFile)
        variables: Instance specific variable file
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

    Returns:
         a tuple with three elements (isValid, error, Experiment)
            First element is True if package is valid, False otherwise.
            Second element is None if first is True,
                If first False it can be None or an Exception object detailing the error
            Third element is the Experiment object that was validated
                If first False it can be None
    """

    if inputs is None:
        inputs = []
    if data is None:
        data = []

    location = location or os.getcwd()
    valid = True
    error = None
    if not os.path.exists(packagePath):
        rootLogger.warning("Specified package path does not exist %s" % packagePath)
        valid = False
        error = experiment.model.errors.PackageLocationError(location=packagePath)

    experimentPackage = None

    #Check configuration
    if valid:
        try:
            experimentPackage = experiment.model.storage.ExperimentPackage.packageFromLocation(
                packagePath, platform=platform, manifest=manifest, validate=True)
            rootLogger.info(experimentPackage)
        except (experiment.model.errors.ExperimentInvalidConfigurationError,
                experiment.model.errors.ExperimentMissingConfigurationError) as e:
            return False, e, None

    e = None
    if valid:
        try:
            rootLogger.warning("TESTING EXPERIMENT FROM PACKAGE\n")
            #Raises errors.PackageLocationError if packagePath is invalid OR any application packages referred to
            # are invalid
            #Raises errors.PackageFormatError if the layout of the package is incorrect.
            #Raises errors.InstanceLocationError if location is invalid
            #Raises errors.InstanceCreateError if the instance data directory can't be created
            #Raises errors.ExperimentMissingConfigurationError if a required configuration file does not exist
            #Raises errors.ExperimentInvalidConfigurationError if a configuration file is invalid.
            #Raises errors.FilesystemInconsistencyError if a problem with the file-system is detected
            #
            #Note: ExperimentInvalidConfigurationError in this case also covers the following exceptions
            #DataReferenceFilesDoNotExistError, DataReferenceFormatError
            # VV: No need to provide manifest here as earlier call to configurationForPackage will have already
            # created a package directory and rewritten packagePath
            e = experiment.model.data.Experiment.experimentFromPackage(
                experimentPackage, location=location, timestamp=timestamp, platform=platform,
                format_priority=format_priority,
                inputs=inputs, data=data, variable_files=variables, createApplicationLinks=createApplicationLinks,
                createVirtualEnvLinks=createVirtualEnvLinks, instance_name=instance_name,
                custom_application_sources=custom_application_sources
            )
            rootLogger.info("Created experiment: %s" % e)
        except Exception as err:
            rootLogger.log(19, 'EXCEPTION: %s' % traceback.format_exc())
            rootLogger.warning("Exception while creating test experiment instance: %s" % err)
            valid = False
            error = err

    return valid, error, e

def CheckSystem():

    '''Checks the system/environment

    e.g. scheduler is up, have read/write permissions, envvars are set etc.''' 

    return True
