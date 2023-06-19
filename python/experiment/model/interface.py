# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

'''Module for abstract interfaces'''

from __future__ import annotations

from abc import ABCMeta, abstractmethod, abstractproperty
import typing
import six

if typing.TYPE_CHECKING:
    from experiment.model.graph import WorkflowGraph, DataReference, ComponentIdentifier
    from experiment.model.executors import Command
    from experiment.model.storage import WorkingDirectory


@six.add_metaclass(ABCMeta)
class InternalRepresentationAttributes:

    @abstractproperty
    def workflowAttributes(self):
        # type: () -> typing.Dict[str, typing.Any]
        pass

    @abstractproperty
    def resourceManager(self):
        # type: () -> typing.Dict[str, typing.Any]
        pass

    @abstractproperty
    def resourceRequest(self):
        # type: () -> typing.Dict[str, typing.Any]
        pass

    @abstractproperty
    def environment(self):
        # type: () -> typing.Dict[str, str]
        pass

    @abstractproperty
    def dataReferences(self):
        # type: () -> typing.List[DataReference]
        pass

    @abstractproperty
    def customAttributes(self):
        # type: () -> typing.Dict[str, typing.Any]

        pass

    @abstractproperty
    def executors(self):
        # type: () -> typing.Dict[str, typing.List[typing.Dict[str, Command]]]
        pass

    @abstractproperty
    def identification(self):
        # type: () -> ComponentIdentifier
        pass

    @abstractproperty
    def producers(self):
        # type: () -> typing.Dict[DataReference, InternalRepresentationAttributes]

        '''Returns a dict of DataReference:<InternalRepresentationAttributes> pairs for the receivers producers

        <InternalRepresentationAttributes> denotes an object which conforms to this classes interface.
        This object must provide behaviour at the same graph level of the receiver.
        For example if the receiver is L2 then the producer object is L2
        If the receiver is L3 then the producer object is L3

        '''

        pass

    @abstractproperty
    def inputDataReferences(self):
        # type: () -> typing.List[DataReference]

        '''A receivers input data-references as a list of graph.DataReference objects

        Requires that the edges have been added to the WorkflowGraph the receiver has access t

        Returns:
            List of DataReferences'''

        pass

    @abstractproperty
    def componentDataReferences(self):
        # type: () -> typing.List[DataReference]

        pass

    @abstractproperty
    def workflowGraph(self):
        # type: () -> WorkflowGraph

        '''The workflow graph the reciever is part of'''

        pass

    @abstractproperty
    def workingDir(self):
        # type: () -> typing.Optional[WorkingDirectory]

        '''The receivers working dir instance if its an L3 object otherwise None'''

        pass

    @abstractproperty
    def command(self):
        # type: () -> Command
        pass

    ##TEMPORARY METHODS
    @abstractmethod
    def setOption(self, key, value):

        '''Sets the options key to value'''

        pass

    @abstractmethod
    def removeOption(self, key):

        '''Remove the value associated with the option

        Note: In some cases this will cause the option to assume a default value (e.g. you can't delete job-type)
        if accessed via the ComponentSpecification API'''

        pass

@six.add_metaclass(ABCMeta)
class ExecutableChecker:

    def findExecutable(self, command, resolvePath): # type: (Command, bool) -> str

        '''Finds the Commands executable

        A search is performed if
        - The Commands executable is pathless
        - resolvePath is True

        Note: This method does not check if the executable path exists or is executable.
        Its possible some calls to e.g. follow links, return some meaningless default if they don't work

        Parameters:
            command: An executors.Command instance
            resolvePath: If True the code will attempt to resolve the executable path if it is a link

        Raises:
            ValueError if the executable is:
            - relative

        Returns:
            The updated executable path
                '''

        pass

    def checkExecutable(self, command
                         ): # type: (Command) -> None

        '''Checks if the executable defined by command exists

        Params:
            command: An executors.Command instance

        Raise:
            ValueError if the executable is
            - pathless
            - relative
            - does not exist at absolute path
            - cannot be executed
        '''

        pass

    def findAndCheckExecutable(self, command: Command, resolvePath: bool = False) -> str:

        '''Finds and checks the Commands executable in a single step

        A search is performed if
        - The Commands executable is pathless
        - resolvePath is True

        Parameters:
            command: An executors.Command instance
            resolvePath: If True the code will attempt to resolve the executable path if it is a link

        Raises:
            ValueError if the executable is:
            - relative
            - does not exist at absolute path
            - cannot be executed

            TaskSubmissionError if unable to submit Task that resolves executable path

        Returns:
            The updated executable path
        '''

        pass
