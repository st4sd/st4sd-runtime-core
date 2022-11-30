#
# coding=UTF-8
# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston


from __future__ import print_function

import experiment.model.errors
import experiment.runtime.errors

try:
    from typing import Callable
except ImportError:
    pass

import datetime
import inspect
import logging
import sys
import threading
import time
import traceback

try:
    ThreadingEvent = threading._Event
except Exception:
    ThreadingEvent = threading.Event

moduleLogger = logging.getLogger("monitor")

#
#Note1: This module is deprecated
#
#
#Note2: Standardise monitors behaviour on exceptions from actions and lifecheck functions
# For example LifeMonitor/DeathMonitor die on an exception from LifeCheck()
# LifeAction does not (handles and sets another timer)
# The two possibly models for ensuring Monitors don't die
#  A) Monitors handle all exception from functions they call
#  B) Functions should be written to handle all exceptions that could be raised
# i.e. is the onus on the function writer or the monitor ....
#
# One issue at the moment is that in case B. where an exception escapses this kills the monitor
# but the corresponding action isn't called so triggers on the monitor ending are not guaranteed to be executed ...
# Should be guaranteed that if the monitor stops everything excpected to be executed is executed

moduleLock = threading.Lock()

def CallStack():

    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe, 2)
    i=0
    for frame in calframe:
        moduleLogger.info('caller name %d: %s' % (i, frame[3]))
        i+=1

def TotalSeconds(td):

    '''Returns total seconds from a timedelta object'''

    return (td.microseconds + (td.seconds + td.days * 24 * 3600.0) * 10**6) / 10**6

class MonitorActionError(experiment.model.errors.EnhancedException):
    
    '''Represents errors raised by a monitor action'''

    def __init__(self, function, underlyingError, tb=None):

        '''
        Parameters:
            function - The function that failed
            underlyingError - An exception object describing why the action failed'''

        desc = u'Failed executing monitor action function %s. Reason follows:\n%s' % (function, underlyingError)
        if tb is not None:
            desc += u"\nUnderlying Traceback:\n%s" % (u'\n'.join(traceback.format_tb(tb)))

        experiment.model.errors.EnhancedException.__init__(self, desc, underlyingError)

        MonitorExceptionTracker.defaultTracker().addException(self)

class MonitorTestError(experiment.model.errors.EnhancedException):

    '''Represents errors raised by a monitor test'''

    def __init__(self, desc, function, underlyingError):

        '''
        Parameters:
            desc - A string describing the monitors type
            function - The function that failed
            underlyingError - An exception object describing why the action failed'''

        experiment.model.errors.EnhancedException.__init__(self,
                '%s failed executing test function %s. Reason follows:\n%s' % (desc, function, str(underlyingError)),
                                                           underlyingError)

        MonitorExceptionTracker.defaultTracker().addException(self)

class MonitorExceptionTracker:

    default = None

    @classmethod
    def defaultTracker(cls):

        #moduleLogger.debug('Acquiring module lock')
        moduleLock.acquire()

        try:
            if MonitorExceptionTracker.default is None:
                MonitorExceptionTracker.default = MonitorExceptionTracker()
        except Exception as error:    
            moduleLogger.warning("Error while creating singleton exception tracker: %s" % error)
        finally:   
            #moduleLogger.debug('Releasing module lock')
            moduleLock.release()    
            #moduleLogger.debug('Released module lock')

        return MonitorExceptionTracker.default

    def __init__(self, lifespan=300):

        self.exceptions = []
        self.lifespan = lifespan
        self.lock = threading.Lock()
        self.log = logging.getLogger("monitor.exceptiontracker")

    def addException(self, exception):

        self.log.debug('Acquiring monitor lock')
        self.lock.acquire()
        try:
            self.exceptions.append({'date':datetime.datetime.now(), 
                'thread':threading.current_thread().name,
                'exception':exception})
            self.log.warning('The following exception was added to the tracker: %s %s' % (type(exception), exception))
        except Exception as error:    
            self.log.warning("Exception while trying to add exception: %s" % str(error))
        finally:
            #self.log.debug('Releasing monitor lock')
            self.lock.release()
            #self.log.debug('Released monitor lock')

    def prune(self):    

        now = datetime.datetime.now()
        r = list(range(len(self.exceptions)))
        r.reverse()
        #self.log.debug('Acquiring monitor lock')
        self.lock.acquire()
        try:
            for i in r:

                delta = now - self.exceptions[i]['date']
                if TotalSeconds(delta) > self.lifespan:
                    self.exceptions.pop(i)
                else:    
                    break
        except Exception as error:        
            self.log.warning("Exception while pruning exception list: %s" % str(error))
        finally:        
            #self.log.debug('Releasing monitor lock')
            self.lock.release()
            #self.log.debug('Released monitor lock')

    def isSystemStable(self, timeFrame=180):   

        self.prune()

        delta = datetime.timedelta(seconds=timeFrame)
        now = datetime.datetime.now()

        systemExceptions = []
        #self.log.debug('Acquiring monitor lock')
        self.lock.acquire()
        try:

            for e in self.exceptions:
                exceptionObj = e['exception']
                if type(exceptionObj) in experiment.runtime.errors.systemErrors:
                    systemExceptions.append(e)
                elif isinstance(exceptionObj, experiment.model.errors.EnhancedException):
                    underlyingErrors = exceptionObj.underlyingErrors()
                    underlyingSystemErrors = [ue for ue in underlyingErrors if type(ue) in experiment.runtime.errors.systemErrors]
                    if len(underlyingSystemErrors) != 0:
                        systemExceptions.append(e)
        except Exception as error:                
            self.log.warning("Exception while checking for stability: %s" % str(error))
        finally:                
            #self.log.debug('Releasing monitor lock')
            self.lock.release()
            #self.log.debug('Released monitor lock')

        systemExceptions = [e for e in systemExceptions if (e['date'] > now - delta)]
    
        if len(systemExceptions) > 0:
            self.log.warning("Found %d system exceptions in last %s seconds" % (len(systemExceptions), timeFrame))

        retval = True
        if len(systemExceptions) != 0:
            self.log.warning("System is not stable")
            retval = False

        return retval    

    def printStatus(self, details=True):    

        self.prune()
        #self.log.debug('Acquiring monitor lock')
        self.lock.acquire()
        try:
            if len(self.exceptions) > 1:
                self.log.warning('There have been %d exceptions in last %d seconds' % (len(self.exceptions), self.lifespan))
                minDate = min([e['date'] for e in self.exceptions])
                maxDate = max([e['date'] for e in self.exceptions])
                delta = maxDate-minDate
                stableTime = datetime.datetime.now() - maxDate
                self.log.warning('First at %s, last at %s, timespan %d. %d secs since last' % (minDate, maxDate, TotalSeconds(delta), TotalSeconds(stableTime)))

            count = 0
            if details is True:
                for e in self.exceptions:
                    self.log.info('%-4d: %s %s %s' % (count, e['date'], type(e['exception']), e['exception']))
                    count += 1 
        finally:                
            #self.log.debug('Releasing monitor lock')
            self.lock.release()
            #self.log.debug('Released monitor lock')

        if not self.isSystemStable():
            self.log.warning("System is not stable")

def CreateLocalProcessLifeCheck(process):

    '''Returns a function that can be called to check if a process is alive.

    Exceptions: 

    This function does not raise any exceptions.

    The returned function will raise ValueError if process is not a subproces.Popen object'''

    def IsAlive():

        retval = True
        process.poll() 
        if process.returncode is not None:
            retval = False

        return retval    

    return IsAlive


def CreateMonitor(interval,
                  action,  # type: Callable[[bool], None]
                  cancelEvent,  # type: threading.Event
                  lastAction=True,
                  name=None,
                  default_polling_time=5.0):

    '''Returns a function that performs an action periodically

    Once called the returned function will perform the action every interval seconds
    in a separate thread.

    Parameters:
        interval: The interval at which to check. Can be either a float/integer or
        a callable which accepts an argument "seconds_waiting". This is a float
        which contains the time that has passed since `action` completed
        last time (in seconds). interval(seconds_waiting) is expected to return
        True when action is to be executed again (or the monitor killed),
        False otherwise.
        action: A function that is called every interval seconds. Must take one argument - lastAction.
        cancelEvent: A threading.Event object that can be used to signal the
            action to stopi
        lastAction: If true when cancelled the monitor will perform the action a last time.
        default_polling_time: Time (in seconds) to poll for whether the action can be performed

    Exceptions: 

    This function raises ValueError if cancelEvent is not a threading.Event object.

    The returned function raises MonitorActionError if action() fails.

    The process calling the returned function will only recieves this exception
    if the first call to action() fails.

    A MonitorActionError does not cause the monitor to fail i.e it 
    will keep running, calling action() periodically.'''

    if cancelEvent is not None and not isinstance(cancelEvent, ThreadingEvent):
        raise ValueError("Incorrect type passed for cancelEvent parameter:").with_traceback(cancelEvent)

    if name is None:
        name = "Monitor (%s)" % action.__name__

    def Monitor(polling_time):
        #If true the monitors action is performed
        executeAction = True
        #If true a new monitor is started
        continueAction = True

        errors_with_filesystem = 5

        while continueAction:
            try:
                if cancelEvent is not None and cancelEvent.is_set():
                    continueAction = False
                    executeAction = lastAction
                    if executeAction:
                        moduleLogger.warning("Cancel event is set. Monitor will exit after last execution of %s" % name)
                    else:
                        moduleLogger.warning("Cancel event is set - monitor with action %s will exit now" % name)

                if executeAction:
                    try:
                        #The parameter should be True if we're are NOT going to continue
                        action(not continueAction)
                    except experiment.model.errors.FilesystemInconsistencyError as error:
                        # VV: Something seriously wrong is going on with the Filesystem, let's retry 5 times
                        #     and bail if we have the same issue over and over
                        moduleLogger.critical("Encountered error when accessing filesystem. "
                                              "Exception: %s" % traceback.format_exc())
                        if errors_with_filesystem > 0:
                            # VV: Wait 30 seconds, maybe the system recovers?
                            errors_with_filesystem = errors_with_filesystem - 1
                            ex_type, ex, tb = sys.exc_info()
                            new_exc = MonitorActionError(action, error, tb)
                            moduleLogger.critical("Monitor will sleep for 30 seconds to let the filesystem recover.")
                            time.sleep(30)
                            raise new_exc

                        moduleLogger.critical("The filesystem is not reliable at the moment,"
                                              " this monitor will terminate.")
                        return
                    except Exception as error:
                        ex_type, ex, tb = sys.exc_info()
                        raise MonitorActionError(action, error, tb)

                if continueAction:
                    beginning = datetime.datetime.now()
                    try:
                        condition = (lambda : not cancelEvent.is_set()) if cancelEvent is not None else (lambda: True)

                        while condition():
                            # VV: interval may return 'execute now' because cancelMonitor is set,
                            #     the next iteration will figure this out and act accordingly
                            execute_now = interval((datetime.datetime.now() - beginning).total_seconds())
                            # VV: there's no need to sleep if the condition() is False
                            #     (i.e. cancelEvent is set)
                            if not execute_now and condition():
                                time.sleep(polling_time)
                                continue
                            break
                    except TypeError:
                        # VV: This is not a dynamic interval, it's just a sleep duration
                        if cancelEvent:
                            cancelEvent.wait(interval)
                        else:
                            time.sleep(interval)
            except Exception:
                # VV: When a monitor raises an exception, log the exception as critical
                #     and carry on
                moduleLogger.critical('MONITOR EXCEPTION (%s):\n%s' % (name, traceback.format_exc()))
                time.sleep(5)

    assert default_polling_time > 0.0

    thread_closure = threading.Thread(target=lambda: Monitor(default_polling_time), name=name).start
    return thread_closure


def CreateEventAction(interval,
                      test,
                      action,  # type: Callable[[], None]
                      cancelEvent=None):

    '''Returns a function that performs an action once if test is true

    Parameters:
        interval: The interval at which to check
        test: A function that is called every interval seconds and that returns a bool
        action: A function that is called once when the test function returns true. Must take one arguments.
            This is used to indicate it will not be called again
        cancelEvent: An optional threading.Event object that can be used to signal the
            action to stop

    Exceptions: 

    This function raises ValueError on first call if cancelEvent is not
    a threading.Event object

    The returned function raises MonitorActionError if action() fails.
    The process calling the returned function will only recieve this exception if test() returns
    true on the first call to it. 

    The returned function raises MonitorTestError if test() fails.
    This exception will kill the monitor. 

    The process calling the returned function will only recieve MonitorTestError if
    the first calls to test() fails.'''

    if cancelEvent is not None and not isinstance(cancelEvent, ThreadingEvent):
        raise ValueError("Incorrect type passed for cancelEvent parameter:").with_traceback(cancelEvent)

    def EventAction():

        continueAction = True
        if cancelEvent is not None and cancelEvent.is_set():
            continueAction = False

        if continueAction is True:
            try:
                if test() is False:
                    timer = threading.Timer(interval, CreateEventAction(interval, test, action, cancelEvent))
                    timer.name="EventAction (%s, %s)" % (test.__name__, action.__name__)
                    timer.start()
                else:   
                    try: 
                        action()
                    except Exception as error:
                        ex_type, ex, tb = sys.exc_info()
                        raise MonitorActionError(action, error, tb)
            except MonitorActionError:
                raise
            except Exception as error:
                raise MonitorTestError('EventAction', test, error)

    return EventAction

def CreateDeathAction(LifeCheck, interval, action, cancelEvent=None, name=None):

    '''Returns a function that monitors if a process is alive in a separate thread.

    When it finds it is dead it calls action

    Parameters:
        LifeCheck: A function that returns True if the process is alive
        interval: The interval at which to check
        action: Action to perform - function that takes no arguments.
        name: A name for the monitor - if None a default is used
        cancelEvent: An optional threading.Event object that can be used to signal the
            monitor to stop

    Exceptions: 

    This function raises ValueError if cancelEvent is not a threading.Event object

    The returned function raises MonitorActionError if action() fails.

    The process calling the returned function will only recieve this exception if 
    LifeCheck() returns False on the first call.

    IMPORTANT: The returned function raises MonitorTestError if LifeCheck() raises an exception.
    This exception will kill the monitor i.e. the DeathAction will never be executed.
    If you don't want this to happen ensure no exceptions can escape from LifeCheck()

    The process calling the returned function will only recieve MonitorTestError if
    the first call to LifeCheck() fails.'''

    if cancelEvent is not None and not isinstance(cancelEvent, ThreadingEvent):
        raise ValueError("Incorrect type passed for cancelEvent parameter:").with_traceback(cancelEvent)

    def DeathAction():

        continueAction = True
        if cancelEvent is not None and cancelEvent.is_set():
            continueAction = False

        if continueAction is True:
            try:
                if LifeCheck() is True:
                    timer = threading.Timer(interval, CreateDeathAction(LifeCheck, interval, action, cancelEvent=cancelEvent, name=name))
                    if name is not None:
                        timer.name=name
                    else:    
                        timer.name="DeathAction (%s, %s)" % (LifeCheck.__name__, action.__name__)

                    timer.start()
                else:
                    try:
                        action()
                    except Exception as error:
                        ex_type, ex, tb = sys.exc_info()
                        raise MonitorActionError(action,error,tb)
            except MonitorActionError as error:
                moduleLogger.critical("DeathAction monitor exiting due to error while performing action - %s, %s, %s" % (LifeCheck, action, error))
                raise
            except Exception as error:
                moduleLogger.critical("DeathAction monitor exiting due to unexpected error - %s, %s, %s" % (LifeCheck, action, error))
                raise MonitorTestError('DeathAction', LifeCheck, error)

    return DeathAction
