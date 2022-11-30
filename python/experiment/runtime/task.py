#
# coding=UTF-8
# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

'''Defines the interfaces a backend must implement

A backed should have the following classes

Basic:
- Task: For creating an inspecting a task
- Backend Specific Executor Implementations: (If any required)

Above allows Task to be used standalone for workflow

Workflow Interface

Following classes/function required for integrating with workflow

- ConfigClass: For providing information on valid configuration parameters
    - resourceManager, executors
- TaskLauncher: Constructs the Task given a component specification
- Expose User-Side Executor+Enhancements:
    - List BackendSpecific executors a Component could use
    - Provide a Flow side initialisation/factory method to create them
'''

from __future__ import print_function

import sys
from abc import ABCMeta, abstractmethod, abstractproperty
import six

@six.add_metaclass(ABCMeta)
class Task:

    '''Generic interface for controlling a subprocess that may be launched via a 3rd party

    The interface is polymorphic to subprocess.Popen as much as possible.
    However for initialisation it utilises the classes in the executor module.
    This provides a richer way to initialise a class.

    The Popen init can be supported via a class method OR by creating a subclass
    which overrides __init__ and performs the necessary translation assembling
    internally the executor chain/stack

    In the latter the Task is instantiated with the ExecutorChain directly.

    Resource Requests

    The resourceRequest dictionary must support at least the following keys
    - numberProcesses
    - threadsPerCore
    - numberThreads
    - ranksPerNode


    '''

    def __init__(self,
                 executor,
                 preCommands=None,
                 postCommands=None,
                 options=None,
                 resourceRequest=None,
                 stdout=sys.stdout,
                 stderr=sys.stderr
                 ):

        '''Initialisation method for Tasks.

        Tasks may add other keyword options but the above are required

        Note: The working directory is set by the executorChain

        Parameters:
            executor - A Command or subclass defining the command line and environment
                The instance may have to inspect if this object is a Command or one of its subclasses e.g. Executor
            resources - A dictionary containing backend specific options as key:value pairs
                This is an option:value pair related to the backend that is not a resourceRequest
            resourceRequest - A dictionary containing the resource request
                See class docs for syntax
            stdout - The stdout stream for the job
            stderr - The stderr stream for the job
            waitOnOutput - If True '''

        pass

    @property
    def schedulerId(self):
        """Returns the ID of the task which the underlying scheduler decides

        Can be empty if scheduler does not generate task ids"""
        return ""

    @classmethod
    def verifyOptions(cls, options):

        '''Returns True if options contains valid option-values for the receiver

        Params:
            options (dict): A dictionary of option:value pairs. Only options known
            by the receiver are checked'''

        pass

    @abstractmethod
    def poll(self):

        '''Returns the returncode of the task if finished otherwise None

        Same as subprocess.Popen.poll - equivalent to returncode property'''

        pass

    @abstractmethod
    def wait(self):

        '''Blocking wait on process exit.

        NB: This method SHOULD NOT raise exceptions - this violates the Task API.
        All exceptions occurring inside this method must be caught and exitReason/returncode etc. set if it is
        due to an error'''

        pass

    @abstractmethod
    def isAlive(self):

        '''Returns True if the receiver is NOT finished

        NOT in FINISHED_STATE, FAILED_STATE or SHUTDOWNSTATE
        '''

        pass

    @abstractmethod
    def kill(self):

        '''Synonym for terminate'''

        pass

    @abstractmethod
    def terminate(self):

        '''Stop task with 'soft' kill

        e.g. SIGTERM/SIGINT or equivalent'''

        pass

    @abstractproperty
    def exitReason(self):

        '''Returns a string describing why the task exited.

        This must be one of the values in experiment.codes.exitReasons

        e.g. return experiment.codes.exitReasons["Killed"]

        This method should map exit-reasons of underlying backend to those in
        experiment.codes.exitReasons

        '''

        pass

    #This should be a property
    #However Popen exposes an Ivar with the same name
    #@abstractproperty
    #def returncode(self):
    #
    #    '''Return the exit code of the task'''
    #
    #    pass

    @abstractproperty
    def status(self):

        '''Returns the status of the task (a string)

        This must be one of the states defined in experiment.codes

        e.g. return experiment.codes.FINISHED_STATE'''

        pass

    @abstractproperty
    def performanceInfo(self):

        '''Return a 1 row matrix containing performance information.

        The columns in the matrix are not prescribed'''

        pass

    @classmethod
    def default_performance_info(cls):
        '''Return a 1 row matrix containing a default performance information report.

        The columns in the matrix are not prescribed'''
        pass
