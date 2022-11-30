#
# coding=UTF-8
# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston


'''Module containing exception definitions'''
from __future__ import print_function, annotations

from typing import List, Dict, TYPE_CHECKING

from experiment.model.errors import EnhancedException, ExperimentError, FlowException, FilesystemInconsistencyError

if TYPE_CHECKING:
    import experiment.runtime.engine
    import experiment.model.storage


class TaskSubmissionError(EnhancedException):

    def __init__(self, desc, underlyingError=None):

        EnhancedException.__init__(self,
                '%s\nUnderlying error:\n(%s) %s' % (desc, type(underlyingError), str(underlyingError)),
                                   underlyingError)


class CannotRestartShutdownEngineError(Exception):

    def __init__(self, referenceString=None):

        msg = 'You cannot restart an engine once it is shutdown'
        if referenceString is not None:
            msg += 'Attempted by component %s' % referenceString

        Exception.__init__(self, msg)


class StageFailedError(ExperimentError):

    '''Error raised when a stage fails'''

    def __init__(self, stage, underlyingError):

        '''
        Parameters:
            underlyingError: Describes why the Job failed'''

        ExperimentError.__init__(self,
                'Stage %d failed. Reason:\n%s' % (stage.index, str(underlyingError)),
                                 underlyingError)


class UnexpectedExceptionError(EnhancedException):

    '''Raised when a exception caught that was not expected - indicates a error'''

    def __init__(self, underlyingError):

        EnhancedException.__init__(self,
                'Unexpected error encountered: %s. This indicates a programming bug' % str(underlyingError),
                                   underlyingError)


class SourceRestartError(Exception):
    def __init__(self,  source_component):
        super(SourceRestartError, self).__init__()
        self.source_component = source_component
        self.message = "Failed to restart Source component " \
                       "%s (instead of running it)" % source_component.specification.reference

    def __str__(self):
        return self.message


class JobLaunchError(EnhancedException):

    '''Raised when a job failed to launch'''

    def __init__(self, desc, underlyingError):

        EnhancedException.__init__(self,
                '%s. Underlying error:\n%s' % (desc, str(underlyingError)),
                                   underlyingError)


class UnexpectedJobFailureError(Exception):

    '''Error raised when one or more jobs fails unexpectedly'''

    def __init__(self, failedJobs=None):

        '''
        Parameters:
            failedJobs: A list of JobManager objects that failed'''

        Exception.__init__(self)
        failedJobs = failedJobs or []
        self.failed_engines = failedJobs  # type: List[experiment.runtime.engine.Engine]

        desc = ""
        for jobManager in failedJobs:
            desc += "Job: %s. Returncode %s. Reason %s\n" % (jobManager.job.reference,
                            str(jobManager.returncode()),
                            jobManager.exitReason())

        self.message = '%d jobs failed unexpectedly.\n%s' % (len(failedJobs), desc)

    def __str__(self):

        return self.message

# VV: Boto S3 Errors

class S3Exception(FlowException):
    pass


class S3Unauthorized(S3Exception):
    def __init__(self, end_point, underlying_exception=None):
        # type: (str, Exception) -> None
        self.end_point = end_point
        self.underlying_exception = underlying_exception
        self.message = "Not authorized to S3 end-point %s" % self.end_point

        if underlying_exception:
            self.message += ". Underlying exception %s" % underlying_exception

    def __str__(self):
        return self.message


class S3LocalStoragePathMissing(S3Exception):
    def __init__(self, path):
        """Raised when a file that was expected to exist locally doesn't

        Args:
            path(str): path to file/folder
        """

        self.path = path

        self.message = "Path %s does not exist" % path

    def __str__(self):
        return self.message


class S3UnableToUploadFile(S3Exception):
    def __init__(self, bucket, source_path, dest_key, underlying_exception):
        """Raised when unable to upload a file to a key under some bucket

        Args:
            bucket(str): Bucket identifier
            source_path(str): path to file/folder on local storage
            dest_key(str): destination key of file/folder in bucket
            underlying_exception(Exception): underlying exception
        """

        self.bucket = bucket
        self.source_path = source_path
        self.dest_key = dest_key
        self.underlying_exception = underlying_exception

        self.message = 'Unable to upload "%s" to bucket "%s" under key "%s"' % (source_path, bucket, dest_key)
        if underlying_exception:
            self.message += ". Underlying exception %s" % underlying_exception

    def __str__(self):
        return self.message


class FinalStageNoFinishedLeafComponents(FlowException):
    def __str__(self):
        return "None of the leaf components in the final stage finished successfully"


systemErrors = [SystemError, FilesystemInconsistencyError]
