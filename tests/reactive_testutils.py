# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

'''Contains functions useful when testing reactive observables'''
from __future__ import annotations

import datetime
import logging
import time

FORMAT = '%(levelname)-9s %(threadName)-30s %(name)-30s: %(funcName)-20s %(asctime)-15s: %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger()

def GatherClosure(alist, log=False):

    '''Returns a closure which will gather emissions into alist

    Set this as action of on_next when subscribing to observable

    '''

    def Gather(x, log=log):

        if log:
            logger.info(x)

        alist.append(x)

    return Gather

def Log(e):

    '''Logs e at WARN level'''

    logger.warning('%s Log value: %s' % (datetime.datetime.now(), str(e)))

def CompletedClosure(completed, log_msg:str | None = None):

    '''Returns a closure which will set the first element of completed to True on receiving an call

    Set this as action of 'on_completion' or 'on_error' when subscribing to observable
    Can also be set as action of on_next in which case the first emission will set completed to True.
    This will have side effect of printing the emission

    Params:
        completed (list): An empty list
            On return completed will contain one element which is False
        log_msg: An optional string, if set print an info message on_completed

    Returns:
        A closure which optionally takes a single parameter which is printed if present
    '''

    completed.append(False)

    def Flag(error=None):
        completed[0] = True
        if error is not None:
            logger.warning(f'Error in CompletedClosure: {error}')
        elif log_msg is not None:
            logger.info(f"Observable {log_msg} completed")

    return Flag

def WaitOnCompletion(completed, limit=40):

    '''Waits until the first element of completed is True

    Use in conjunction with `CompletedClosure` to wait until an observable completes on the main thread.

    Parameter:
        completed (list): A list with a least one element. The first element is expected to be a bool
            The function blocks until the first element is True
        limit: How long to wait in seconds until exiting
    '''

    counter = 0
    while not completed[0]:
        time.sleep(1.0)
        counter += 1
        if counter > limit:
            break

def WaitOnRun(emissions, limit=40):

    '''Waits while the emissions indicates an engine is not running

    Parameters:
        emissions (list): A list of tuples. Each tuple is an emission from an engine state/stateUpdate observable.
        limit (integer): How long to wait until isRunning is detected

    Use in conjunction with `Gather` to wait until an engine starts on the main thread.

    Notes: the emissions are tuples of (dict, engine). The value of the key isRunning in the dict
    is what is checked
    '''

    WaitOnEvent(emissions, event=lambda e: e[-1][0].get('isRunning') == False)

def WaitOnFirstTaskFinish(emissions, limit=40):

    '''Waits while the emissions indicates an engine first task launch has not finished

       Parameters:
           emissions (list): A list of tuples. Each tuple is an emission from an engine state observable.
           limit (integer): How long to wait until engine exit is detected

       Use in conjunction with `Gather` to wait on the main-thread until an engine has run its task at least ONCE

       Notes: the emissions are tuples of (dict, engine). The value of the key lastTaskFinishedData in the dict
       is what is checked to idenfity if the engine has run.
       '''

    WaitOnEvent(emissions, event=lambda e: e[-1][0].get('lastTaskFinishedDate') is None)

def WaitOnEvent(emissions, event, limit=40):

    '''Waits while the emissions indicates an engine is not running

    Parameters:
        emissions (list): A list of tuples. Each tuple is an emission from an engine state/stateUpdate observable.
        limit (integer): How long to wait until isRunning is detected
        event: A lambda taking emissions, that when it evaluates to false this function returns.

    Use in conjunction with `Gather` to wait until an engine starts on the main thread.

    Notes: the emissions are tuples of (dict, engine). The value of the key isRunning in the dict
    is what is checked
    '''

    counter = 0
    while len(emissions) == 0 or event(emissions):
        time.sleep(1.0)
        counter += 1
        if counter > limit:
            break
