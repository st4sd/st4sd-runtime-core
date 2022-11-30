#
# coding=UTF-8
# Copyright IBM Inc. 2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis




from __future__ import print_function
from __future__ import annotations

from typing import Union, List, Callable, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from experiment.model.interface import InternalRepresentationAttributes
    import experiment.model.graph

import datetime
import logging
import os
import random
import time
from threading import Condition, Event, Thread

import experiment.utilities.data
import experiment.model.codes
import experiment.model.executors
import experiment.runtime.task
import six
from abc import ABCMeta

class ReadOnly(type):
    def __setattr__(cls, attr, value):
        raise ValueError('Do not modify the attributes of %s' % cls.__name__)

@six.add_metaclass(ABCMeta)
class SimulatorTaskState(object):
    submitted = 0
    executing = 1
    finished = 3


class SimulatorTask(experiment.runtime.task.Task, object):
    schedulingHeaders = [
             # VV: These events can be emitted by the Engines
             "epoch-submitted",
             "epoch-started",
             "epoch-finished"
        ]

    def __init__(self, componentSpecification):
        # type: (InternalRepresentationAttributes) -> None
        self.log = logging.getLogger('simulator.%s' % componentSpecification.identification.componentName.lower())
        # VV: Just to force lint to relax
        experiment.runtime.task.Task.__init__(self, None)

        self.schedulingData = self.default_performance_info()
        self._real_epoch_submitted = None
        self._real_epoch_started = None
        self._real_epoch_finished = None

        self._job = componentSpecification
        self._real_state = SimulatorTaskState.submitted
        self._observed_state = SimulatorTaskState.submitted

        self._finished_event = Event()
        self._real_return_code = None
        self._observed_return_code = None
        self._exit_reason = None

        self._sim_condition = Condition()

        # MJ: Contains the experiment.model.graph.ComponentSpecifications for the jobs producers
        self._job_references: Set[experiment.model.graph.ComponentSpecifications] = set(self._job.producers.values())

        (range_schedule_overhead,
         range_execution_time,
         expected_exit_code,
         time_scale) = self._get_simulation_parameters()

        if expected_exit_code is None:
            expected_exit_code = 0

        if range_schedule_overhead is None:
            range_schedule_overhead = [0., 1.]

        if range_execution_time is None:
            range_execution_time = [2., 10.]

        if time_scale is None:
            time_scale = 1.0

        def pick_duration_from_range(time_range):
            low, high = time_range
            return random.random() * (high - low) + low

        self._exec_time = pick_duration_from_range(range_execution_time) / time_scale
        self._sched_overhead = pick_duration_from_range(range_schedule_overhead) / time_scale
        self._expected_exit_code = expected_exit_code

        self._sim_thread = Thread(target=self._run)
        self._sim_thread.start()
        self._sim_thread_poll = Thread(target=self.poll)
        self._sim_thread_poll.start()
        self.log.debug('Beginning submission (%s)' % self._sched_overhead)

    def _get_simulation_parameters(self):
        # VV: Inspect the 3 simulator options ('sim_scheduling_overhead', etc)
        def extract_range_from_key(key):
            line = str(self._job.customAttributes[key]).strip()
            tokens = line.split()
            next = tokens.pop(0)
            new_range = list(map(float, next.split(':')))

            if len(new_range) == 1:
                new_range.append(new_range[0])

            if len(tokens):
                rest = ' '.join(tokens)
                self._job.setOption(key, rest)

            return new_range

        range_scheduling_overhead = None
        range_execution_time = None
        expected_exit_code = None
        time_scale = None

        if 'sim_range_schedule_overhead' in self._job.customAttributes:
            range_scheduling_overhead = extract_range_from_key('sim_range_schedule_overhead')

        if 'sim_range_execution_time' in self._job.customAttributes:
            range_execution_time = extract_range_from_key('sim_range_execution_time')

        if 'sim_expected_exit_code' in self._job.customAttributes:
            # VV: It's possible to define a chain of exit codes. The exit codes are expected
            #     to be integers which are space separated. Once this chain of exit codes
            #     is consumed the process will start emitting exit codes of 0 for all future
            #     executions
            tokens = str(self._job.customAttributes['sim_expected_exit_code']).split()
            all_exit_codes = list(map(int, tokens))
            expected_exit_code = all_exit_codes.pop(0)

            if len(all_exit_codes):
                remaining_ret = ' '.join(map(str, all_exit_codes))
                self._job.setOption('sim_expected_exit_code', remaining_ret)
            else:
                self._job.setOption('sim_expected_exit_code', '0')

        if 'sim_time_scale' in self._job.customAttributes:
            time_scale = float(self._job.customAttributes['sim_time_scale'])

        return range_scheduling_overhead, range_execution_time, expected_exit_code, time_scale

    @property
    def expected_return_code(self):
        return self._expected_exit_code

    @property
    def expected_execution_time(self):
        return self._exec_time

    @property
    def expected_scheduling_time(self):
        return self._sched_overhead

    @property
    def input_dependencies(self):
        ret = set()
        for prod in [o for o in self._job_references if self._job.identification.stageIndex > o.identification.stageIndex]:
            output_file = os.path.join(prod.command.workingDir, 'finished.txt')
            ret.add(output_file)

        return ret

    @property
    def lifepartner_dependencies(self):
        ret = set()
        for prod in [o for o in self._job_references if self._job.identification.stageIndex == o.identification.stageIndex]:
            pre, executor, post = experiment.model.executors.CommandsFromSpecification(prod)
            output_file = os.path.join(executor.workingDir, 'started.txt')
            ret.add(output_file)

        return ret

    def _filter_not_existing_files(self, files):
        unmet = set()

        for output_file in files:
            if not os.path.exists(output_file):
                unmet.add(output_file)

        return unmet

    @property
    def _unmet_inputs(self):
        # FIXME VV: This is a bit weird for input dependencies which are Repeating jobs
        # because they will produce a 'finished.txt' file but we can't know if they
        # have actually performed their last action, we need information that exists
        # in a higher level
        return self._filter_not_existing_files(self.input_dependencies)

    @property
    def _unmet_lifepartners(self):
        return self._filter_not_existing_files(self.lifepartner_dependencies)

    def _run(self):
        self._real_epoch_submitted = datetime.datetime.now().strftime("%d%m%y-%H%M%S")
        time.sleep(self._sched_overhead)

        self.log.debug('Beginning execution (%s)' % self._exec_time)
        pre, executor, post = experiment.model.executors.CommandsFromSpecification(self._job)
        working_dir = executor.workingDir

        with self._sim_condition:
            if self._real_state == SimulatorTaskState.submitted:
                self._real_state = SimulatorTaskState.executing
                self._real_epoch_started = datetime.datetime.now().strftime("%d%m%y-%H%M%S")
                with open(os.path.join(working_dir, 'started.txt'), 'a') as f:
                    f.write('Just started at %s\n' % datetime.datetime.now())

                unmet = self._unmet_inputs | self._unmet_lifepartners
                start_time = time.time()
                if len(unmet) == 0:
                    self._sim_condition.wait(self._exec_time)
                else:
                    self._real_return_code = 1

                # VV: Update profiling information
                self._real_epoch_finished = datetime.datetime.now().strftime("%d%m%y-%H%M%S")
                self._real_state = SimulatorTaskState.finished
                if self._real_return_code is None:
                    self._real_return_code = self._expected_exit_code
                    with open(os.path.join(working_dir, 'finished.txt'), 'a') as f:
                        f.write('Just finished at %s with exit code %s\n' % (
                            datetime.datetime.now(),
                            self._real_return_code
                        )
                                )
                else:
                    with open(os.path.join(working_dir, 'killed.txt'), 'a') as f:
                        if unmet:
                            fail_message = "Just finished abruptly at %s with exit code " \
                                           "%s, unmet inputs: \n  %s" % (
                                               datetime.datetime.now(),
                                               self._real_return_code,
                                               '  \n'.join(unmet)
                                           )
                        else:
                            fail_message = 'Just finished abruptly at %s with exit code %s' % (
                                datetime.datetime.now(),
                                self._real_return_code,
                            )
                        self.log.critical(fail_message)
                        f.write('%s\n' % fail_message)

        self.log.debug('Done execution (%s)' % self._real_return_code)

    def poll(self):
        self._observed_return_code = self._real_return_code

        # VV: No wait-time, some pend-start-time, and no pre-execution-time
        idx = self.schedulingData.indexOfColumnWithHeader('epoch-submitted')
        self.schedulingData.matrix[0][idx] = self._real_epoch_submitted

        idx = self.schedulingData.indexOfColumnWithHeader('epoch-started')
        self.schedulingData.matrix[0][idx] = self._real_epoch_started

        idx = self.schedulingData.indexOfColumnWithHeader('epoch-finished')
        self.schedulingData.matrix[0][idx] = self._real_epoch_finished

        self._observed_state = self._real_state
        self._observed_return_code = self._real_return_code

        self.log.debug('Polling (%s, %s)' % (
            self._real_state,
            self._real_return_code
        ))

        if self.isAlive() is False:
            self._finished_event.set()
        else:
            time.sleep(1)
            self._sim_thread_poll = Thread(target=self.poll)
            self._sim_thread_poll.start()

    def wait(self):
        self._finished_event.wait()

    def isAlive(self):
        alive_states = [SimulatorTaskState.submitted,
                        SimulatorTaskState.executing]

        return self._observed_state in alive_states

    def kill(self):
        if self.isAlive():
            # VV: Emulate signal 9
            self._real_return_code = -9
            with self._sim_condition:
                if self._real_state == SimulatorTaskState.executing:
                    self._sim_condition.notify()

    def terminate(self):
        return self.kill()

    @property
    def returncode(self):
        return self._observed_return_code

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
            elif self.returncode < 0:
                retval = experiment.model.codes.exitReasons["UnknownIssue"]
            elif self.returncode == 24:
                retval = experiment.model.codes.exitReasons["ResourceExhausted"]
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
        return experiment.utilities.data.Matrix()

    @classmethod
    def default_performance_info(cls):
        r = ['None'] * 3

        schedulingData = experiment.utilities.data.Matrix(rows=[r],
                                                    headers=cls.schedulingHeaders,
                                                    name='SchedulingData')
        return schedulingData
