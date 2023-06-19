# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

'''Module for functions/classes relating to executors

Executors are programs which launch other programs'''

from __future__ import print_function
from __future__ import annotations

import logging
import os


try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess

import copy
import sys
import tempfile
import threading
import re
from abc import ABCMeta, abstractmethod
from typing import (
    Dict,
    Optional,
    Tuple,
    Union,
    Any
)

import six

import experiment.model.errors
import experiment.model.frontends.flowir
import experiment.model.interface

moduleLogger = logging.getLogger("monitor")

#To lock resolving command lines
directoryLock = threading.Lock()


def read_popen_output(args, env=None, shell=False, cwd=None):
    with tempfile.TemporaryFile() as f_out, tempfile.TemporaryFile() as f_err:
        process = subprocess.Popen(
            args=args, env=env, shell=shell, cwd=cwd,
            stdout=f_out,
            stderr=f_err,
            universal_newlines=True,
        )
        process.wait()

        f_out.seek(0)
        f_err.seek(0)

        out = f_out.read().decode()
        err = f_err.read().decode()

    return out, err, process

class LocalExecutableChecker(experiment.model.interface.ExecutableChecker):
    cache = {}

    @classmethod
    def cache_command(cls, command, resolved_executable):
        # type: (Command, str) -> None
        cls.cache[cls.hash_command(command)] = resolved_executable

    @classmethod
    def hash_command(cls, command):
        # type: (Command) -> Tuple[Union[str, Tuple[str, str]], ...]
        the_hash = [command._executable, ]
        environment = command._environment

        sorted_keys = sorted(environment)
        for key in sorted_keys:
            the_hash.append((key, environment[key]))

        # VV: Vectors and dicts are not hash-able but Tuples are!
        return tuple(the_hash)

    @classmethod
    def is_command_hashed(cls, command):
        return cls.hash_command(command) in cls.cache

    @classmethod
    def get_hashed_command(cls, command):
        # type: (Command) -> str
        return cls.cache[cls.hash_command(command)]

    def __init__(self):

        self.log = logging.getLogger("executors.localchecker")

    def _findExecutablePath(self, command  # type:  Command
                            ):

        '''Internal method allowing subclass to switch how executables are located'''

        self.log.warning(
            "Found pathless executable (%s) - checking if resolvable in command environment" % command._executable)
        # Note: If PATH is not in the environment or is empty an error will have be raised previously
        interpreter, err, process = read_popen_output("which %s" % command._executable,
                                                      env=command._environment,
                                                      shell=True)

        if process.returncode == 0:
            executableWithPath = interpreter.strip("\n")
            self.log.info("Found executable in command environment - using %s" % executableWithPath)
        else:
            self.log.warning(
                "Unable to find location of executable %s using command environment %s" % (
                    command._executable, command._environment
                ))
            executableWithPath = None

        return executableWithPath

    def _checkExecutable(self, executable, environment):

        if not os.path.exists(executable):
            msg = "No file exists at '%s' (using command environment %s)" % (
                executable,
                environment)
            self.log.warning(msg)
            raise ValueError(msg)
        elif not os.access(executable, os.X_OK):
            msg = "Specified executable at '%s' is not executable by user (using environment %s)" % (
                executable,
                environment)
            self.log.warning(msg)
            raise ValueError(msg)

    def findExecutable(self, command, resolvePath=False):

        '''Finds the Commands executable

         A search is performed if
         - The Commands executable is pathless
         - resolvePath is True

         Parameters:
             command: An executors.Command instance
             resolvePath: If True the code will attempt to resolve the executable path if it is a link

         Raises:
             ValueError if the executable is:
             - Relative

         Returns:
            A string containing the full path to the executable if one can be constructed
            None otherwise (indicates a pathless executable could not be resolved)

         Notes:
             This method does not check for the existence of any paths it constructs
             Its possible some calls to e.g. follow links, return some meaningless default if they don't work
             If the executable path is absolute and resolvePath is False this method does nothing and will return the same path.
         '''

        if LocalExecutableChecker.is_command_hashed(command):
            executableWithPath = LocalExecutableChecker.get_hashed_command(command)
            return executableWithPath

        if os.path.split(command._executable)[0] == "":
            executableWithPath = self._findExecutablePath(command)
        elif not os.path.isabs(command._executable):
            raise ValueError('Executable must be pathless or absolute')
        else:
            executableWithPath = command._executable

        if bool(executableWithPath) is True:
            # realpath behaviour with strings that have no path separators (pathless executables)
            # Various behaviour depending on python version has been seen
            # It could raise AttributeError due to missing path separator
            # It could resolve it w.r.t CWD! (seen on Mac)
            # Because of these possibilities the safest is to LBYL if executable is pathless
            if resolvePath and os.path.split(executableWithPath)[0] != "":
                #Note realpath does not check if what the link resolves to exists
                #If the executable doesn't exists realpath just returns it with no error
                try:
                    executableWithPath = os.path.realpath(executableWithPath)
                except AttributeError as e:
                    #Analogously to above we don't want this method to raise any errors if we cannot find path
                    command.log.warning('Caught exception %s' % e)
                    executableWithPath = None
        else:
            self.log.warning("executableWithPath is '%s'" % executableWithPath)

        LocalExecutableChecker.cache_command(command, executableWithPath)
        return executableWithPath

    def findAndCheckExecutable(self, command: Command, resolvePath: bool = False):

        executable = self.findExecutable(command, resolvePath=resolvePath)

        if executable is None:
            msg = "Unable to find location of executable %s using command environment %s" % (
                command._executable, command._environment
            )
            raise ValueError(msg)

        self._checkExecutable(executable, command._environment)

        return executable

    def checkExecutable(self, command):

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

        self._checkExecutable(command._executable, command._environment)


def CheckSpecification(
                componentSpecification # typ    §e: experiment.model.interface.InternalRepresentationAttributes
                 ):

    '''Checks the pre/main/post Commands defined by a data.Job or graph.ComponentSpecification instance

    This function bridges the workflow graph and a specific backend.
    It inspects the backend and commands defined in a ComponentSpecification instance.
    It then uses the executor.Registry() to instantiate the correct syntax checking class for each command

    For example a ComponentSpecification defines the lsf backend.
    It also defines a DataManagerStageInCommand as a pre-command.

    This class will pass the ComponentSpecification to the syntax checker for
    the DataManagerStageInCommand in the LSF backend.

    Args:
        componentSpecification (experiment.interface.InternalRepresentationAttributes): Object containing the component options.
            Expected to have the following properties: executors, resourceManager

    Returns:
        None

    Exceptions:


    '''

    executorOpts = componentSpecification.executors
    backend = componentSpecification.resourceManager['config']['backend']

    for executor_types in ['main', 'pre', 'post']:
        for commandOpts in executorOpts[executor_types]:
            commandName = commandOpts['name']

            try:
                testFunction = Registry.registryManager().testerForName(commandName, backend)
            except KeyError:
                moduleLogger.warning("No tester for %s-command '%s' in backend '%s'." % (
                    executor_types, commandName, backend
                ))
                if backend != 'simulator' and Registry.registryManager().is_executor_known(commandName) is False:
                    raise experiment.model.errors.BackendExecutorMismatchError("%s executor %s, specified by %s, is not registered " \
                        "for use with backend %s and is not in defaults" % (
                        executor_types, commandName, componentSpecification.identification.componentName, backend
                    ))
            else:
                testFunction(commandOpts, componentSpecification)


def CommandsFromSpecification(componentSpecification  # type: experiment.model.interface.InternalRepresentationAttributes
                              ):

    '''Returns the pre/main/post Commands defined by a data.ComponentSpecification instance

    Some Commands may be chains

    This function bridges the workflow graph and specific backend.
    It inspects the backend and commands defined in a ComponentSpecification instance.
    It then uses the executor.Registry() to instantiate the correct backend class for each command

    Parameters:
        componentSpecification: A data.ComponentSpecification instance

    Returns:
        A tuple with three elements.
        The first and last are a list of Command instances
        The second will be an executor.Executor instance or executor.Command instance'''

    executorOpts = componentSpecification.executors
    backend = componentSpecification.resourceManager['config']['backend']
    phase_commands = {
        'pre': [],
        'post': [],
        'main': [componentSpecification.command],
    }

    for phase in ['main', 'pre', 'post']:
        for commandOpts in executorOpts[phase]:
            commandName = commandOpts['name']
            try:
                # VV: If the executor is not compatible with the selected backend skip it.
                #     The CheckSpecification() method has run before we ever attempt to build a command so
                #     skipping incompatible commands for Backends rather than raising an Exception is safe.
                cls = Registry.registryManager().executorForName(commandName, backend)
            except KeyError:
                moduleLogger.info("Skip command '%s' for backend '%s' of component %s "
                                  "because of command/backend incompatibility" % (
                    commandOpts, backend, componentSpecification.identification.identifier
                ))
            else:
                phase_commands[phase].append(cls.commandFromOptionsAndSpecification(commandOpts, componentSpecification))

        # VV: Some commands can be turned off, for example `lsf-dm-in` has to be switched off when the --hybrid
        #     of elaunch.py option is set to false. It is up to the command to figure out if it should
        #     actually do something or not
        phase_commands[phase] = [command for command in phase_commands[phase] if command is not None]

    # VV: FIXME Why is this not a list too ?
    target = phase_commands['main'][-1]

    return phase_commands['pre'], target, phase_commands['post']


def GetMPIRun(env, logger):

    if "LSF_MPIRUN" in env:
        logger.debug("Using LSF_MPIRUN defined by job environment")
        lsfmpirun = env["LSF_MPIRUN"]
    elif "LSF_MPIRUN" in os.environ:
        lsfmpirun = os.environ["LSF_MPIRUN"]
        logger.warning("No job specific LSF_MPIRUN - using LSF_MPIRUN defined by global environment: %s" % lsfmpirun)
    else:
        logger.warning("mpirun to use not specified by LSF_MPIRUN var in job or experiment env - searching")
        # process = subprocess.Popen("which mpirun", stdout=subprocess.PIPE, shell=True)
        interpreter, err, process = read_popen_output('which mpirun', shell=True)
        if process.returncode == 0:
            lsfmpirun = interpreter.strip("\n")
            logger.warning("Determined mpirun location using which: %s" % lsfmpirun)
        else:
            lsfmpirun = "/opt/openmpi/bin/mpirun"
            logger.warning("Unable to determine location of mpriun - using default (%s)." % lsfmpirun)

    return lsfmpirun

def GetMPIType(env, lsfmpirun, logger):

    if "LSF_MPITYPE" in env:
        logger.debug("Using LSF_MPITYPE defined by job environment")
        lsfmpitype = env["LSF_MPITYPE"].lower()
    elif "LSF_MPITYPE" in os.environ:
        logger.debug("No job specific LSF_MPITYPE - using LSF_MPITYPE defined by experiment environment")
        lsfmpitype = os.environ["LSF_MPITYPE"].lower()
    else:
        logger.warning("mpi type to use not specified by LSF_MPITYPE var in job or experiment env - determining from mpirun location")
        if "mpich" in lsfmpirun.lower():
            lsfmpitype = "mpich"
        elif "openmpi" in lsfmpirun.lower():
            lsfmpitype = "openmpi"
        else:
            lsfmpitype = "openmpi"
            logger.warning("Unable to determine mpi type - using default (%s). Note, process may fail to run" % lsfmpitype)

    if lsfmpitype not in ["mpich", "openmpi"]:
            logger.warning("Unknown mpitype specified - %s - defaulting to openmpi" % lsfmpitype)

    return lsfmpitype

#Currently Task objects take a command+arguments.
#Classes defined here allow greater flexibility in the specification of what a Task should run
#Particularly where
# 1. One command launches another command
# 2. Pre/post execution commands are required
# 3. The chain of commands to execute can be modified pre and post Task instantiation
# 4. Environment variables correspond to different parts of the chain e.g. LSF envs v MPIrun envs v Job envs
# 5. Control resolution of bash command subs

#This requires that Task object has the ability to be take a richer object than a string for specifying what to do.

# That is a Task executes a Command/ExecutorChain and possibly an ExecutionStack
# Internally a Task can modify the passed Command/ExecutorChain and create an ExecutionStack


#Environments and Variable Expansion
#
#Composition of environments and resolution of environment variables can get complicated.
#
#At the moment the rules are simplified - both to see how far that takes us and because configuration file
#format only gives certain granularity.
#
#1. The environment of all components in a chain is the same (GLOBAL ENVIRONMENT)
#2. This environment is composed of all the individual environments
#3. Composition rules: ??
#4. Expansion variables is done on the host by the Task instance running the chain
#5. The expansion assumes the environment is CLOSED i.e. it is evaluated w.r.t to itself only.
#(Is this actually true ...)
#
# Note: This does not mean that expansion does not take place elsewhere e.g. when configuration file is read.
# It only applies to environment as they were set when creating Command/Executor instances.

class Command(object):

    '''Represents a command line (command+args+environment) for a program.

    Note: Unlike Popen etc. this class does not represent a process ONLY a command line.
    Its purpose is to
    - Provide a way to store command lines as first-class objects
    - Be sub-classed to provide default command-lines and commandline
      options manipulation for complex commands e.g docker, mpirun, runjob etc.
    - Provide an interface for other objects to inspect elements of a command line'''

    @classmethod
    def commandFromOptionsAndSpecification(cls, options, specification):

        '''Return an command based on options and a ComponentSpecification

        Parameters:
            options: A dictionary of key-value pairs.
                Valid keys:values are subclass specific.
            specification: A experiment.data.ComponentSpecification instance the
            options relate to'''

        return Command.commandFromOptions(options)

    @classmethod
    def commandFromOptions(cls, options):

        '''Return an command based on options

        Parameters:
            options: A dictionary of key-value pairs.
                Valid keys:values are subclass specific.'''

        return Command(**options)

    def __init__(self,
                 executable,  # type: str
                 arguments="",  # type: str
                 workingDir=None,  # type: Optional[str]
                 environment=None,  # type: Optional[Dict[str, str]]
                 basePath=None, # type: Optional[str]
                 resolvePath=True,  # type: bool
                 resolveShellSubstitutions=True,  # type: bool
                 useCommandEnvironment=True,  # type: bool
                 executableChecker=LocalExecutableChecker(),
                 expandArguments='double-quote',
        ):

        '''

        Note: The default transformations applied to the value of the executable parameter is only to resolve it w.r.t working-dir if relative.

        By defaut this object does not
        - Attempt to follow it if its a link
        - Leave it if its pathless

        To do above call findPath/updatePath/checkExecutable separately

        Parameters:

        executable: The executable to execute (path or name), a string
        arguments: The executables arguments, a string
        workingDir: Where the command is intended to be executed.
            This is only used when resolving a command line argumente
        basePath: Path used to resolve relative executables. If None the workingDir is used.
        resolvePath: If True, and the executable is a path that exists, it will be resolved using os.realpath
                Note: This does not happen on instantiation - updatePath or updateAndCheckExecutable must be called
        resolveShellSubstitutions: If True the argument string is run through a shell when commandLine is accessed
        useCommandEnvironment: If True the defined environment variables are used when evaluating
            the argument string (if resolveArguments is True)
        environment: Dictionary of variable:value names
            Note: If the variable values contain variables they will NOT be expanded by the returned object.
        expandArguments: 'double-quote' or none'
            'double-quote'(default): instruct to envelop commandline in double-quotes and perform bash expansion by feeding
                the resulting string to `echo`
            'none': do not bash expand at all

        Note: Some executables behave differently if run via a link versus the linked executable.
        An example are ImageMagick tools like "montage" and "convert" which are links to "magick"
        The available command line options for "montage" are not valid for "magick" as "magick"
        detects it is being run via the "montage" link and applies different criteria to the command line in this case.

        '''

        self.rule = None
        self.log = logging.getLogger("executors.%s" % os.path.split(executable)[1])

        self.resolvePath = resolvePath
        self.executableChecker = executableChecker
        self.expandArguments = expandArguments

        valid_expand = ['double-quote', 'none']
        if expandArguments not in valid_expand:
            raise ValueError('Invalid option %s for expandArguments parameter, expected values are %s' % (
                expandArguments, valid_expand
            ))

        if environment is None:
            environment = {}

        self._environment = copy.deepcopy(environment)

        if executable:
            executable = experiment.model.frontends.flowir.expand_vars(executable, self._environment)
        self._executable = executable

        self._workingDir = workingDir
        if self._workingDir is None:
            self._workingDir = os.getcwd()

        self.basePath = basePath if basePath is not None else self._workingDir
        if not os.path.isabs(self._executable) and not os.path.split(self._executable)[0] == "":
            self.log.debug("Found relative executable %s - resolving w.r.t %s" % (self._executable, self.basePath))
            # Note does not raise exceptions
            executableWithPath = os.path.join(self.basePath, self._executable)
            executableWithPath = os.path.normpath(executableWithPath)
            self._executable = executableWithPath

        self.resolveShellSubstitutions=resolveShellSubstitutions
        self.useCommandEnvironment=useCommandEnvironment
        self._arguments = arguments

    @property
    def workingDir(self):

        return self._applyRewriteRule(self._workingDir)

    @property
    def executable(self):

        return self._applyRewriteRule(self._executable)

    @property
    def arguments(self):

        '''Returns the argument string'''

        return self._applyRewriteRule(self._arguments)

    @property
    def commandLine(self):
        '''Returns the command string'''
        return "%s %s" % (self.executable, self.commandArguments)

    @property
    def commandArguments(self):
        if self.resolveShellSubstitutions:
            return self.resolveArgumentString()
        else:
            return self.arguments

    @property
    def environment(self):

        '''Returns the a copy of the environment dictionary'''

        retval = copy.deepcopy(self._environment or {})

        if self.rule is not None and self._environment is not None:
            retval = [(key, self._applyRewriteRule(self._environment[key])) for key in self._environment]
            retval = dict(retval)

        return retval

    @property
    def environmentString(self):

        '''Returns the environment as a string

        String is a colon separated list of ENVAR=Value strings'''

        environment = self.environment
        envs = ["%s=%s" % (key, environment[key]) for key in environment]
        return ":".join(envs)

    def updatePath(self):

        '''Updates receivers executable property

        Tries to convert the exectuable to a full path if it is pathless
        Tries to resolve executables which are links (if command created with resolvePath=True)

        If a pathless executable cannot be resolved this method does nothing - the executable remains pathless
        If the current value of self._executable does not exist (and absolute or not) this method will have no effect


        Notes:
        This method does not perform any checks for existence of files at any absolute path it generates
        Its possible the absolute path returned when resolvePath is True do not exit

        If the recievers resolvePath attribute was set to True (and executable was found) then it will be changed to False after executing this method
        This is to prevent multiple potentially expensive (e.g. spawning containers) redundant operations

        '''

        #Rely on immutability of Command instances to only do this once
        p = self.executableChecker.findExecutable(self, resolvePath=self.resolvePath)
        self._executable = p if p is not None else self._executable

        #If we found and resolve the path try to resolve it again unless self.resolvePath is directly changed back to True
        if p is not None:
            if self.resolvePath is True:
                self.resolvePath = False

    def resolveArgumentString(self):

        '''Resolves shell command substitutions in the argument string

        The substitutions are potentially resolved in a shell depending on the value of self.expandArguments
        (see construction doc-string for more information).
        The environment used depends on if one was supplied and the value of useCommandEnvironment ivar
        The substitutions are resolved in the working dir of the receiver

        Environment:

        If the receiver has no environment, or useCommandEnvironment is False,
        the complete calling process environment is used.

        If the receiver has a defined environment, those variables appearing in the command line only are added.

        In both cases the resulting environment is evaluated in a shell.

        Rewrites:
            Any set rewrite rule does not
            - change the working dir used to resolve the arguments
            - change the argument string resolved
            - change the environment used

            i.e. all resolution is done with a local environment

           Rewrite rule WILL be applied to the resolved command
        '''

        retval = self._arguments

        if self.expandArguments != 'none'\
                and (not (self._arguments.strip(" ") == "" or self._arguments is None)):

            # NB: This part has to be executed in thread safe manner to preserve the working directory
            # If any other threads can change directory the results of the following are undefined
            # This is a problem as we can't really guarantee this
            # All we can say is DONT USE os.chdir in threads!
            try:
                self.log.debug('Acquiring directory lock')
                directoryLock.acquire()

                # Evaluate shell commands in correct working dir
                currentDir = None
                if self._workingDir is not None:
                    currentDir = os.getcwd()
                    os.chdir(self._workingDir)

                # Evaluate the arguments as a shell expression
                self.log.debug("Evaluating arg string, %s, via shell in %s" % (self._arguments, os.getcwd()))

                expansionVars = None
                if self.useCommandEnvironment is True:
                    expansionVars = {}
                    # Only place environment variables which are in the command line into the expansion environment
                    # This is to prevent unintended side-effects
                    for key in list(self._environment.keys()):
                        if key in self._arguments:
                            expansionVars[key] = self._environment[key]

                # FIXME: Issue with above - if you remove PATH/LD_LIBRARY_PATH then no virtualenv!
                for key in ["PATH", "LD_LIBRARY_PATH"]:
                    if key in self._environment:
                        expansionVars[key] = self._environment[key]

                self.log.debug("Using environment %s" % expansionVars)

                if self.expandArguments == 'double-quote':
                    str_to_expand = '"%s"' % self._arguments
                else:
                    raise NotImplementedError("Cannot expand command line string %s while respecting "
                                              "expandArguments=%s" %(self._arguments, self.expandArguments))

                out, err, process = read_popen_output(
                    'echo %s' % str_to_expand,
                    env=expansionVars,
                    shell=True,
                    cwd=self._workingDir,
                )

                if err != "":
                    self.log.debug("Error string returned: %s", err)

                retval = out.strip("\n")
                self.log.debug("Processed args are %s", retval)

                if currentDir is not None:
                    os.chdir(currentDir)
            finally:
                self.log.debug('Releasing directory lock')
                directoryLock.release()
                self.log.debug('Released directory lock')

        return self._applyRewriteRule(retval)

    def checkExecutable(self):

        '''Checks if the executable exists'''

        self.executableChecker.checkExecutable(self)

    def updateAndCheckExecutable(self):

        '''Updates the receivers executable and checks it exists in single step'''

        #This will raise an error if it cannot find a valid executable path hence we can just assign
        self._executable = self.executableChecker.findAndCheckExecutable(self, self.resolvePath)
        if self._executable is None:
            raise ValueError('Executable checker returned None - this is inconsistent')

    #Following are for polymorphism with Executor class
    @property
    def target(self):

        return self

    @target.setter
    def target(self, obj):

        '''Sets the target of the executor.

        Must be a command or executor'''

        pass

    @property
    def endChain(self):

        return self

    def _applyRewriteRule(self, string):

        '''Applies the current rule to str

        If no rule is set this method does nothing'''

        retval = string
        if self.rule is not None and string is not None:
            retval = re.sub(self.rule['pattern'], self.rule['replacement'], string)

        return retval

    def setRewriteRule(self, rule):

        '''Sets a rule that will be used to rewrite paths returned by the receiver

        This is for use when the Command is constructed locally for remote execution.

        Parameters:
            rule: A dictionary with two keys.
                'pattern': A regular expression that identifies strings to replace
                'replacement: The string to substitute
        '''

        self.rule = rule

@six.add_metaclass(ABCMeta)
class BackendCommandExtensions:
    @abstractmethod
    def setBackendOptions(self, options):

        '''Sets backend specific options

        Backend specific pre/post commands may need to set or modify
        backend options. For example they made need to flag to the backend
        that certain features are active

        Concrete Example.

        An LSF pre-executor for DataManager will add executions of
       'bstage in' for a set of files to the pre-job stage.
        However it also needs to add lsf.SUB4_DATA_STAGING_REQ to global lsf options,
        and needs to set the LSF 'dataSpecFile' option.

        To do this it needs access to the lsf.submitReq structure.

        Parameters:
            options: A backend specific options object

        Returns:
            Backend specific option structure'''

        pass

class AggregatedCommand(Command):

    '''A command which is a sequence of commands

    Allows resolving and executing a set of commands together'''

    def __init__(self, commands,
                 workingDir=None,
                 environment=None,
                 resolvePath=True,
                 resolveShellSubstitutions=True,
                 useCommandEnvironment=True):
        '''

        Parameters:

        commands: A list of command objects
        workingDir: Where the command is intended to be executed.
            This is only used when resolving a command line.
            If None defaults to current working dir.
        resolvePath: Not used - kept for polymorphism with Command init
        resolveShellSubstitutions: If True the argument string is run through a shell when commandLine is accessed
        useCommandEnvironment: If True the defined environment variables are used when evaluating
            the argument string (if resolveArguments is True)
        environment: Dictionary of variable:value names
            Note: If the variable values contain variables they will NOT be expanded by the returned object.'''

        self.commands = commands
        self.resolveShellSubstitutions = resolveShellSubstitutions
        self.useCommandEnvironment = useCommandEnvironment
        self.rule = None

        self._environment = environment
        if self._environment is None:
            self._environment = {}

        self._workingDir = workingDir
        if self._workingDir is None:
            self._workingDir = os.getcwd()

        self.log = logging.getLogger("aggregated.commmand")

    @property
    def arguments(self):

        '''Returns the command line without bash command substitution.

        Note; This property is not rewritten'''

        #Unlike AggregatedCommand has no interval argument ivar
        #Therefore we can not apply a rewrite here as this property is the only way to access the raw value

        return " ;".join(["%s %s" % (c.executable, c.arguments) for c in self.commands])

    @property
    def commandLine(self):

        '''Returns the command string'''

        if self.resolveShellSubstitutions:
            cl = self.resolveArgumentString()
        else:
            cl = self._applyRewriteRule(self.arguments)

        return cl

    def resolveArgumentString(self):

        '''Resolves shell command substitutions in the argument string

        The commands are resolved in a shell.

        The resolution can be global or local.
        In the first case all subcommands are resolved simultaneously,
        in the second they are resolved independently

        The resolution can use a complete or specific environment (useCommandEnvironment)

        Therefore there are four combinations, two of which are equivalent (global/complete and local/complete)
        - Global, Specific: environment is not None and useCommmandEnvironment is True
        - Local, Specific/Complete/Hybrid: environment is not None and useCommmandEnvironment is True
        - Global, Complete: environment is None and useCommandEnvironment is False

        Note: The result in the second case depends on the specific configuration of each command.
        Note Local/Complete and Global/Complete are equivalent => method 3 can be used

        If the receiver has a defined environment, those variables appearing in the command line only are added.

        In both cases the resulting environment is evaluated in a shell.'''

        retval = self.arguments
        if not (self.arguments.strip(" ") == "" or self.arguments is None):

            #
            # useCommandEnvironment is True and environment == None - use subcommands resolve method
            # Otherwise resolve here

            if self.useCommandEnvironment and self._environment is None:
                retval = " ;".join([c.commandLine for c in self.commands])
            else:

                # Evaluate the arguments as a shell expression
                self.log.debug("Evaluating arg string, %s, via shell in %s" % (self.arguments, os.getcwd()))

                expansionVars = None
                if self.useCommandEnvironment is True:
                    expansionVars = {}
                    # Only place environment variables which are in the command line into the expansion environment
                    for key in list(self._environment.keys()):
                        if key in self.arguments:
                            expansionVars[key] = self._environment[key]

                out, err, process = read_popen_output(
                    'echo "%s"' % self.arguments,
                    env=expansionVars,
                    shell=True,
                    cwd=self._workingDir,
                )

                # VV: There is an issue with communicate - see hartreechem/flow#220
                # out, err = process.communicate()
                if err != "":
                    self.log.debug("Error string returned: %s", err)

                retval = out.strip("\n")
                self.log.debug("Processed args %s are %s" % (self.arguments, retval))

        return self._applyRewriteRule(retval)

    def checkExecutable(self):

        '''Checks if the executable exists'''

        self.log.critical('Not implemented for AggregatedCommand')

    def addCommand(self, command, index=None):

        if index is None:
            index = len(self.commands)

        self.commands.insert(index, command)


class Executor(Command):

    '''A program which executes another program - the target.

    The target can be another command/executor.

    Environment and Targets:

    There are two possibilities for composing the environments of target and executor.
    1. GLOBAL ENVIRONMENT: The environment returned by an instance is the sum of the executor and target environments
        Both the executor and the target will see the same environment
    2. PARTITIONED ENVIRONMENTS: The environment returned by an instance is the executor environment.
        The target environment is set by the executor via command line options

    In the first case the responsibility for expanding environment variables is pushed up the stack.
    Usually it will be the Task instance executing the Executor chain that decides what to do.

    In the second case the executor will be responsible for expanding the variables of its target.
    There are three options
           - Resolve w.r.t. target environment only on the calling process host. (The environment set is closed)
           - Resolve w.r.t the calling process environment
           - Wait for run-time resolution

    Critically the last case is dependent on having a shell and furthermore the visibility of the envars
    on the command line (escaping)
        - No escaping. Envar expansion depends on the executing process of the full chain being a shell
        - Escaped. Envar expansion depends on this executors parent providing a shell.

    In either case if there is no shell there will be no expansion.

    Summary:

    Four resolution schemes

    Global
    Partitioned-Static
    Partitioned-Runtime-Global
    Partitioned-Runtime-Local

    We will only support Global at the moment as the others require the ability to partition environments
    in the configuration file.
    For example we set MPI environment for dl_meso which has to contain both dl_meso and mpi related envars
    which can't be separated.
    '''

    @classmethod
    def commandFromOptionsAndSpecification(cls, options, references):

        print('This class method is not implemented for Executor sublcass', file=sys.stderr)

    @classmethod
    def commandFromOptions(cls, options):

        print('This class method is not implemented for Executor sublcass', file=sys.stderr)

    @classmethod
    def executorFromOptions(cls, target, options):

        '''Return an executor based on options

        Parameters:
            target: An instance of an executor.Command subclass
            options: A dictionary of key-value pairs.
                Valid keys:values are subclass specific.'''

        return Executor(target, **options)

    def __init__(self,
                    target: experiment.model.interface.Command,
                    executable: str,
                    arguments=None,
                    workingDir=None,
                    environment=None,
                    resolveShellSubstitutions=True,
                    useCommandEnvironment=True):

        #If we pass None to superclass it will be set to cwd which is not
        #what we want
        if workingDir is None:
            workingDir = target.workingDir

        super(Executor, self).__init__(executable=executable,
                               arguments=arguments,
                               workingDir=workingDir,
                               environment=environment,
                               resolveShellSubstitutions=resolveShellSubstitutions,
                               useCommandEnvironment=useCommandEnvironment)
        self._target = target
        #We have to resolve any pathless executables as when the chain is executed
        #they will no longer be looked up in the execution environment (as they are not the
        #first argument)
        self.target.updatePath()

    @property
    def target(self) -> experiment.model.interface.Command:

        '''Returns the target of the executor'''

        return self._target

    @target.setter
    def target(self, obj):

        '''Sets the target of the executor.

        Must be a command or executor.

        Note: A set rewrite rule will not be propogated to the target.
        To do this explicitly reset the rule using setRewriteRule'''

        self._target = obj
        self.target.updatePath()

    @property
    def commandLine(self):

        '''Returns the command string'''

        if self.resolveShellSubstitutions:
            args = self.resolveArgumentString()
        else:
            args = self.arguments

        return "%s %s %s" % (self.executable, args, self.target.commandLine)

    def chainContainsExecutor(self, executorType):

        '''Checks if the chain contains an executor of the given type and returns it if so'''

        retval = None
        if isinstance(self, executorType):
            retval = self
        else:
            try:
                retval = self.target.chainContainsExecutor(executorType)
            except AttributeError:
                pass

        return retval

    @property
    def environment(self):

        '''Note: Returns the environment defined by all the executors in the chain'''

        env = copy.copy(self.executorEnvironment())
        env.update(self.target.environment)

        return env

    def executorEnvironment(self):

         '''Returns the environment defined by the receiver only'''

         retval = self._environment
         if self.rule is not None:
             retval = [(key, self._applyRewriteRule(self._environment[key])) for key in self._environment]
             retval = dict(retval)

         return retval

    @property
    def workingDir(self):

        '''Returns the first defined working dir in the chain'''

        #I don't think this can actually be None
        if self._workingDir is None:
            return self.target.workingDir
        else:
            return self._applyRewriteRule(self._workingDir)

    @property
    def endChain(self):

        '''Returns the final target of a chain of executors'''

        if type(self.target) == Command:
            retval = self.target
        else:
            retval = self.target.finalTarget

        return retval

    def setRewriteRule(self, rule):

        super(Executor, self).setRewriteRule(rule)
        #Apply rewrite to target
        if self.target is not None:
            self.target.setRewriteRule(rule)

class DockerRun(Executor):

    '''Executor for docker'''

    @classmethod
    def executorFromOptions(
            cls,
            target: experiment.model.interface.Command,
            options: Dict[str, Any],
    ) -> DockerRun:

        """Return an executor based on options

        Arguments:
            target: The executors target: A Command subclass instance
            options: A dictionary of key-value pairs.
                Valid keys:values are subclass specific.

                Required options:
                    docker-image: The container image to use

                Optional:
                    docker-executable(str):      The path to the docker-like executable, or name of the executable
                                                 if it exists in $PATH. Defaults to "docker" if unset
                    docker-use-entrypoint(bool): if set to True will use executable as entrypoint of container.
                                                 Defaults to False if unset
                    docker-args(str):            Arguments to docker run (i.e. docker run <runArguments>)
                    docker-platform(str):        the arguments to `--platform` for pulling and running containers.
                                                 If unset (or empty) then `--platform` will not be set. This means that
                                                 the docker-like executable decides which platform to pull.
                                                 For example, if the environment contains the DOCKER_DEFAULT_PLATFORM
                                                 env-var and the docker-like executable is actually docker then
                                                 it will try to pull the x86 flavour of the image for the linux os.

        Returns:
            An instance of DockerRun()
        """

        dockerRunArgs=""
        if 'docker-args' in options:
            dockerRunArgs = options['docker-args']

        executor = cls(
            target=target,
            executable=options.get('docker-executable', 'docker'),
            image=options['docker-image'],
            mounts=[(target.environment['INSTANCE_DIR'], target.environment['INSTANCE_DIR'])],
            environment=target.environment,
            resolveShellSubstitutions=False,
            use_entrypoint=options.get('docker-use-entrypoint', False),
            platform=options.get('docker-platform'),
            runArguments=dockerRunArgs
        )

        return executor

    def __init__(
        self,
        target,
        image,
        executable="docker",
        arguments="",
        mounts=[],
        runArguments="",
        workingDir=None,
        use_entrypoint = True,
        resolveShellSubstitutions=True,
        environment: Union[Dict[str, str], None] = None,
        platform: Union[str, None] = None,
        useCommandEnvironment=True,
        addUser=True,
    ):

        '''
        Args:
            target: the command to run inside the container
            executable: Docker executable to use - defaults to docker
            arguments: Docker arguments (i.e. docker <arguments> run)
            image: Docker image
            mounts: A list of local directories to mount as local dir:container dir
            workingDir: The working directory for the docker process
            use_entrypoint: if set to True will use executable as entrypoint of container
            runArguments: Arguments to docker run (i.e. docker run <runArguments>)
            docker-platform:  the arguments to `--platform` for pulling and running containers.
                If unset (or empty) then `--platform` will not be set. This means that
                the docker-like executable decides which platform to pull.
                For example, if the environment contains the DOCKER_DEFAULT_PLATFORM
                env-var and the docker-like executable is actually docker then
                it will try to pull the x86 flavour of the image for the linux os.
        '''

        super(DockerRun, self).__init__(target,
                            executable=executable,
                            arguments=arguments,
                            workingDir=workingDir,
                            environment=environment,
                            resolveShellSubstitutions=resolveShellSubstitutions,
                            useCommandEnvironment=useCommandEnvironment)

        self.platform = platform
        self.image = image
        self.mounts = mounts
        self.use_entrypoint = use_entrypoint

        runArguments = [runArguments] if runArguments else []

        if self.platform:
            runArguments.append(f"--platform={platform}")

        if addUser:
            uid = os.getuid()
            # VV: Set the user to the local user and the group to 0 so that containers do not run as root AND
            # those that follow OpenShift best practices just work out of the box
            #  https://docs.openshift.com/container-platform/4.12/openshift_images/create-images.html
            runArguments.append(f"--user {uid}:0")

        if environment:
            for name in environment:
                runArguments.append(f"--env {name}")

        if self.use_entrypoint:
            runArguments.append(f'--entrypoint={self.target.executable}')

        # VV: Ask <docker run> to allocate a pseudo-TTY
        runArguments.append('-t')
        runArguments.append(f'-w {self.workingDir}')

        # VV: Contains arguments to `docker run` (e.g. --rm)
        self.runArguments = ' '.join(runArguments)

    @property
    def arguments(self) -> str:
        # VV: the format is "<dockerArguments> run <runArguments> <target command-line>"
        args = [f"{self._arguments} run {self.runArguments}"]

        for mount in self.mounts:
            args.append("-v %s:%s" % tuple(mount))

        args.append(self.image)

        if self.use_entrypoint:
            # VV: The target executable is in --entrypoint - just use the target arguments here
            args.append(self.target.arguments)
        else:
            args.append(self.target.commandLine)

        return self._applyRewriteRule(' '.join(args))

    @property
    def commandLine(self) -> str:
        # VV: This is <docker> <run [--entrypoint] [--workdir] [-e ...]>
        cl = f"{self.executable} {self.arguments}"
        return cl


class OpenMPIRun(Executor):

    '''Executor for openmpi's mpirun'''

    classLog = logging.getLogger("executors.%s" % "OpenMPIRun")


    @classmethod
    def executorFromOptions(cls, target, options):

        coresPerProcess = options['numberThreads']/options['threadsPerCore']
        # Check if target environment defines any relevant information:
        mpirun = GetMPIRun(target.environment, cls.classLog)

        return OpenMPIRun(target,
                          executable=mpirun,
                          numberProcesses=options['numberProcesses'],
                          ranksPerNode=options['ranksPerNode'],
                          coresPerProcess=coresPerProcess)

    def __init__(self, target,
                 executable="/usr/bin/mpirun",
                 arguments="",
                 numberProcesses=1,
                 ranksPerNode=1,
                 coresPerProcess=1,
                 environment=None):

        super(OpenMPIRun, self).__init__(target,
                                        executable=executable,
                                        arguments=arguments,
                                        environment=environment,
                                        resolveShellSubstitutions=False)

        self.ranksPerNode = ranksPerNode
        self.numberProcesses = numberProcesses
        self.coresPerProcess = coresPerProcess

    @property
    def arguments(self):

        #Re:rewrite. No paths so not applying
        return "--allow-run-as-root --mca mpi_show_mca_params all -v -n %d --map-by ppr:%d:node:PE=%d -display-topo -display-map -display-allocation" % (
            self.numberProcesses, self.ranksPerNode, self.coresPerProcess)

    @property
    def commandLine(self):

        return "%s %s %s" % (self.executable, self.arguments, self.target.commandLine)

class MPICHMPIRun(Executor):

    '''MPICH Executor'''

    classLog = logging.getLogger("executors.%s" % "MPICHMPIRun")

    @classmethod
    def executorFromOptions(cls, target, options):

        coresPerProcess = options['numberThreads'] /options['threadsPerCore']

        #Check if target environment defines any relevant information:
        mpirun = GetMPIRun(target.environment, cls.classLog)

        return MPICHMPIRun(target,
                          executable=mpirun,
                          numberProcesses=options['numberProcesses'],
                          ranksPerNode=options['ranksPerNode'],
                          coresPerProcess=coresPerProcess)

    def __init__(self, target, executable="/usr/bin/mpirun", arguments="",
                 numberProcesses=1,
                 ranksPerNode=1,
                 coresPerProcess=1,
                 environment=None):

        super(MPICHMPIRun, self).__init__(target,
                                          executable=executable,
                                          arguments=arguments,
                                          environment=environment,
                                          resolveShellSubstitutions=False)
        self.ranksPerNode = ranksPerNode
        self.numberProcesses=numberProcesses
        self.coresPerProcess=coresPerProcess

    @property
    def arguments(self):

        return "-n %d -ppn %d" % (self.numberProcesses, self.ranksPerNode)

    @property
    def commandLine(self):

        return "%s %s %s" % (self.executable, self.arguments, self.target.commandLine)


class MPIExecutor(Executor):

    '''Note: Only use for class methods'''

    classLog = logging.getLogger("executors.%s" % "MPIExecutor")

    @classmethod
    def executorFromOptions(cls, target, options):

        if not 'numberProcesses' in options:
            cls.classLog.warning('Provided options does not specify numberProcesses - not creating MPI executor')
            return target

        coresPerProcess = options['numberThreads'] /options['threadsPerCore']

        # Check if target environment defines any relevant information:
        mpirun = GetMPIRun(target.environment, cls.classLog)

        if not os.path.exists(mpirun):
            cls.classLog.warning('Specified mpirun (%s) does not exist' % mpirun)
        else:
            cls.classLog.info("Using mpirun at %s" % mpirun)

        mpitype = GetMPIType(target.environment, mpirun, cls.classLog)
        cls.classLog.info("MPI type is %s" % mpitype)

        if mpitype == "openmpi":
            return OpenMPIRun(target,
                               executable=mpirun,
                               numberProcesses=options['numberProcesses'],
                               ranksPerNode=options['ranksPerNode'],
                               coresPerProcess=coresPerProcess)
        else:
            return MPICHMPIRun(target,
                           executable=mpirun,
                           numberProcesses=options['numberProcesses'],
                           ranksPerNode=options['ranksPerNode'],
                           coresPerProcess=coresPerProcess)

class ExecutionStack:

    '''Represents a set of Command/Executor  that should be executed in certain order.

    There are three levels pre-target, target, post-target '''

    def __init__(self, target=None):

        self.stack = {"pre":[], "target":None, "post":[]}
        self._target=target

    def add(self, newCommand, level="pre"):

        '''Adds a command to the pre/post execution stack

        Raises ValueError if level is not pre or post'''

        self.stack[level].append(newCommand)

    @property
    def target(self):

        return self._target

    @target.setter
    def target(self, obj):

        self._target = obj

    def preTarget(self):

        return self.stack['pre']

    def postTarget(self):

        return self.stack['post']

    def printCommands(self):

        for el in self.preTarget():
            print(el.commandLine)

        print(self.target.commandLine)

        for el in self.postTarget():
            print(el.commandLine)


#FIXME: Skeleton implementation
class Registry(object):

    singleton = None

    @classmethod
    def registryManager(cls):

        if cls.singleton is None:
            cls.singleton = Registry()

        return cls.singleton

    def __init__(self):

        self.register = {'default':{},
                         'testers':{}}

        self.testRegister = {'default':{}}

    def executorForName(self, name, namespace=None, searchDefault=True):

        '''
        Parameters:
            name: The name of the executor
            namespace: Its namespace e.g.  the name of the backend it is associated with
                If there is no executor for the namespace the default namespace is searched
            searchDefault: If False does not search the default namespace if the executor is not in given `namespace`,

        Exceptions:

            Raises KeyError if no executor with name'''

        if namespace is None:
            namespace = "default"

        try:
            executor = self.register[namespace][name]
        except KeyError:
            if searchDefault and namespace not in [None, 'default']:
                executor = self.register['default'][name]
            else:
                raise

        return executor

    def addExecutor(self, cls, name, namespace=None):

        if namespace is None:
            namespace = "default"

        if namespace not in self.register:
            self.register[namespace] = {}

        self.register[namespace][name] = cls

    def addTester(self, cls, name, namespace=None):

        if namespace is None:
            namespace = "default"

        if namespace not in self.testRegister:
            self.testRegister[namespace] = {}

        self.testRegister[namespace][name] = cls

    def testerForName(self, name, namespace=None, searchDefault=True):

        if namespace is None:
            namespace = "default"

        try:
            tester = self.testRegister[namespace][name]
        except KeyError:
            if searchDefault and namespace not in [None, 'default']:
                tester = self.testRegister['default'][name]
            else:
                raise

        return tester

    def is_executor_known(self, tester):
        for namespace in self.testRegister:
            if tester in self.testRegister[namespace]:
                return True

        return False


if __name__ == "__main__":

    FORMAT = '%(levelname)-9s %(threadName)-30s %(name)-30s: %(funcName)-20s %(asctime)-15s: %(message)s'
    logging.basicConfig(format=FORMAT)
    rootLogger = logging.getLogger()
    rootLogger.setLevel(30)

    #Create an execution chain
    #docker + mpirun + dpd with pre and post exec
    command = Command("ls", "-l $PWD", environment={"TEST_ENV":"test_val"})
    docker = DockerRun(target=command, image="test_image", arguments='$(echo $PWD)')#, resolveShellSubstitutions=False)
    mpirunEx = OpenMPIRun(target=command, numberProcesses=16, ranksPerNode=4, environment={"LSF_MPIRUN":"somepath"})
    #Test interposing mpirun in the docker chain
    docker.target=mpirunEx

    print('COMMAND')
    print(command.commandLine)
    print('\nMPIRUN EXECUTOR')
    print(mpirunEx.commandLine)
    print(mpirunEx.environment)
    print('\nDOCKER EXECUTOR (INTERPOSED MPIRUN)')
    print(docker.commandLine)
    print(docker.environment)

    stack = ExecutionStack(target=docker)
    stack.add(Command("cp", "$HOME/* /tmp/", useCommandEnvironment=False), level="pre")
    stack.add(Command("cp", "$HOME/* /tmp/", useCommandEnvironment=False), level="post")

    print('\nSTACK')
    stack.printCommands()

    #Apply rewrite
    print('Testing rewrite')
    docker.setRewriteRule({'pattern':'michaelj',
                           'replacement':'paragon'})

    print(docker.commandLine)

else:

    #Create registry
    registry = Registry.registryManager()

    #Add known "named" executors
    registry.addExecutor(DockerRun, 'docker')
    registry.addExecutor(OpenMPIRun, 'openmpi')
    registry.addExecutor(MPICHMPIRun, 'mpich')
    registry.addExecutor(MPIExecutor, 'mpi')
