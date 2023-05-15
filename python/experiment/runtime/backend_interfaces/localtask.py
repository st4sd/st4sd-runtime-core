#
# coding=UTF-8
# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston


'''Module containing classes/functions related to running a task locally'''
from __future__ import print_function

import datetime
import logging
import threading
import traceback

try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess

import experiment.utilities.data
import experiment.model.codes
import experiment.runtime.task


#Some IDEs will flag the LocalTask doesn't implement all the Task abstract methods
#However they don't recognise that it inherits the required implementations from Popen
class LocalTask(subprocess.Popen, experiment.runtime.task.Task):
    schedulingHeaders = [
        # VV: These 3 events can be emitted by Engines
        #     LocalTasks do not suffer from queueing times:
        #     their start time is the same as their submission time
        "epoch-submitted",
        "epoch-started",
        "epoch-finished",
    ]

    def __init__(self, args, cwd=None, env=None,
                 stdout=None, stderr=None, shell=False, **kwargs):
        # VV: Use a thread to keep track of when the process terminates so that
        #     we can safely fill in the performanceInfo details
        self._z_wait_event = threading.Event()

        self._z_finished_date = None
        self.args = args
        self.log = logging.getLogger('local')

        self.schedulingData = self.default_performance_info()

        subprocess.Popen.__init__(self, args,
                                  cwd=cwd, env=env,
                                  stdout=stdout, stderr=stderr,
                                  shell=shell, **kwargs)
        self._z_launched_date = datetime.datetime.now()
        epoch_submitted = self._z_launched_date.strftime("%d%m%y-%H%M%S")
        for key in ['epoch-submitted', 'epoch-started']:
            idx = self.schedulingData.indexOfColumnWithHeader(key)
            self.schedulingData.matrix[0][idx] = epoch_submitted

        threading.Thread(target=self._wait_task_and_set_epoch_finished).start()

    def _wait_task_and_set_epoch_finished(self):
        """Internal method that waits for task to finish and then sets epoch-finished

        You inherit localTask and override this method to add your custom logic for waiting the task to complete
        """
        # VV: Ensure that epoch-finished is reflected to the caller of self.wait() after they wake-up
        try:
            subprocess.Popen.wait(self)
        except Exception as e:
            self.log.warning("Failed to wait for termination of local task %s due to %s" % (self.args, e))
        self._z_finished_date = datetime.datetime.now()

        idx = self.schedulingData.indexOfColumnWithHeader('epoch-finished')
        self.schedulingData.matrix[0][idx] = self._z_finished_date.strftime("%d%m%y-%H%M%S")

        # VV: Wake up whoever is blocked at .wait()
        self._z_wait_event.set()

    def isAlive(self):

        retval = True
        self.poll()
        if self.returncode is not None:
            retval = False

        return retval

    def kill(self):

        #FIXME: temporary
        #The manager has to distinguish these two - currently kill is assumed to be "terminate" (SIGTERM)
        self.terminate()

    def wait(self):
        try:
            if self._z_finished_date is None:
                self._z_wait_event.wait()
            subprocess.Popen.wait(self)
        except:
            self.log.critical("Failed to wait for local task %s. EXCEPTION: %s\n" % (
                self.args, traceback.format_exc()
            ))
    @property
    def exitReason(self):

        retval = None
        # subprocess.Popen uses negative numbers to id signals
        if not self.isAlive():
            if self.returncode == 0:
                retval = experiment.model.codes.exitReasons["Success"]
            elif self.returncode == -9:
                retval = experiment.model.codes.exitReasons["Killed"]
            elif self.returncode in [-2, -15]:
                retval = experiment.model.codes.exitReasons["Cancelled"]
            elif self.returncode == -24:
                retval = experiment.model.codes.exitReasons["ResourceExhausted"]
            elif self.returncode < 0:
                retval = experiment.model.codes.exitReasons["UnknownIssue"]
            else:
                retval = experiment.model.codes.exitReasons["KnownIssue"]

        return retval

    @property
    def status(self):

        if self.isAlive():
            state = experiment.model.codes.RUNNING_STATE
        else:
            if self.returncode == 0:
                state = experiment.model.codes.FINISHED_STATE
            else:
                state = experiment.model.codes.FAILED_STATE

        return state

    @property
    def performanceInfo(self):
        return self.schedulingData[:]

    @classmethod
    def default_performance_info(cls):
        r = ['None'] * len(cls.schedulingHeaders)
        schedulingData = experiment.utilities.data.Matrix(rows=[r],
                                                    headers=cls.schedulingHeaders,
                                                    name='SchedulingData')
        return schedulingData
