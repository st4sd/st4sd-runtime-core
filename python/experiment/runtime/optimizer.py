#
# coding=UTF-8
# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis

"""VV: Classes to optimize the execution of Workflow packages

This file contains 2 main classes: a) Modelist, and b) Optimizer. These classes do not have
any dependencies to other Flow code, in that sense they are generic to some extent. The
Modelist class keeps track of 4 task-events: queue-start, queue-stop, run-start, and run-stop.

In a previous version it would use these events to build models for queuing and execution
time as well as success/failure; said modules could then be used by the Optimizer to make
predictions regarding the future of Engines. After the latest major overhaul of the
Optimizer the generation of models has been removed. You can still find it by rolling
back to git commit c7fbb2b6e6a2d7a78c3b2e1e13b36ce2705fe4aa

Among other things, the modelist keeps track of information regarding the timestamps of
run-start, run-stop as well as the resulting exit-code of tasks. The Optimizer may then
use this information to build a connection between whether a Consumer succeeds and the
amount of time that its producers have executed since the last time it succeeded
(specifically, since the run-start time of its last successful run, will elaborate bellow).

The reason we pick the run-start timestamp instead of the run-stop timestamp is that
we expect the Optimizer to use this information just before deciding whether to schedule
a consumer Task. So, we can't be using information which became known to the Modelist
*after* the Consumer task run-start timestamps. To make matters worse, the Modelist might
observe task-events with some delay which is entirely dependent on the Task-backend that
is used. This means that there's some noise in the information that is eventually gathered.

Here's how the Modelist attributes producer-execution-time to a consumer
(see Modelist.compute_effective_delta_time() for more information)

~~~~~~~~~~~~~~~~~~~~~~~~~~~~Modelist.compute_effective_delta_time()~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    batch of tasks = all tasks between 2 successful tasks (does not include first success,
                     but includes second success)
    effective_delta_time = time that the producers have executed succesfully which is
                           attributed to this batch of tasks
    n = not accounted by this run
    y = accounted by this run
    c = time-step within this run (could be failed or successful)
    l = time-step within last successful run
    f = past FAILED run (there's a chance it doesn't exist because previous run was successful
                         doesn't change a thing in the algorithm; it's only shown for brevity)
    date_now = timestamp when Optimizer computes the effective delta time

    -nnnnnyyyyyyy---yyyynnnnnn----nnnnnnn--nn-  <- producer quantums (even future ones)
    ------llll---fffff--cccccccccccccc--------  <- desc
             ^          ^            ^
             |  when_run_started     |
             |                   date_now
    last_success_stopped
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Optimizer acts as a thread-safe wrapper to the Modelist, models the effective-repeat-interval
(more on that in a bit) of RepeatingEngines, and suggests repeat-intervals. It bases its decisions
on an Explore/Exploit algorithm.

So what is an effective-repeat-interval? The key idea behind this term is that in the eyes of a
Consumer time stands still while all of its Producers lay dormant (i.e. are not executing). So the
effective-repeat-interval is a function of the amount of time the ActiveProducers* of a consumer have
executed in the current consumer batch of tasks (see schematic above).
[ActiveProducers* = producers which have not finished before a task is started]

Currently, we consider producer-time-succeed-max to be a good enough approximation of the real
effective-repeat-interval of consumers. Producer-time-succeed-max is the accumulative amount of time
that a Producer has executed and resulted in a Success in the span of the Consumer's current
batch-of-tasks. Modelist.compute_effective_delta_time() computes a bunch of things, among which is
prod-time-succeed-max; there's a couple of corner-cases though. What about a Producer task which
has started sometime before `date_now` but has not finished before `date_now`. The modelist can't know
what the exit-code of said Producer task is.

What it can know is whether the Producer task is driven by a Source engine. Because Source engines are
probably going to terminate Successfully the Modelist may simply treat all Source-Engine tasks as
successful. Which leaves us with RepeatingEngine tasks; the modelist classifies this producer
execution time as 'unknown' and then assumes it will succeed. The rationale is that if the producer
succeeds we save time by not waiting for the producer-task to terminate thereby potentially overlapping
Producer with Consumer execution. On the other hand, if the producer-task fails after the decisions of
the Optimizer have lead the Consumer-task to execute the Consumer-task may still succeed which will
let the Optimizer know that the Consumer probably has a smaller effective-repeat-interval than previously
thought. If the Consumer does fail well that's also OK because the Optimiser will factor this in when
deciding the next repeat-interval.

What I explained above is more or less how the `Explore` part of the Optimizer functions. When Exploring
the Optimizer will assume that its current view of some Consumer's effective-repeat-interval is flawed and
it will instead pick a target effective-repeat-interval which is some fraction of the one it believes to
be true. If it is right then the next effective-repeat-interval it calculates for said Consumer will be
smaller than the current one (and probably closer to the true value). If the Consumer task fails that means
that the Optimizer has wasted `task-run-time` and computational resources.

So how is the `target-effective-repeat-interval` acted upon? Here's where the `Exploit` part of the
Optimizer comes in. Once the Optimizer has decided a `target-effective-repeat-interval` it will periodically
monitor the consumers progress (i.e. observed progress towards the target-effective-repeat-interval
for the current batch of tasks). If the progress is less than the target-effective-repeat-interval it will
inspect the state of the Consumer's producers. If at least one of them is running the Optimizer will
first determine which is the producer that has executed for the longest during the Consumer's current
batch-start. It will then suggest a repeat-interval such that when the Consumer schedules for execution
its progress will be greater-than/equal-to the target-effective-repeat-interval.
"""

from __future__ import print_function

from typing import List, Any, Dict, Tuple, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    pass

import datetime
import logging
import random
import threading


class TaskQuantum(object):
    def __init__(self, date_started, date_stopped, override_date_started=None, exit_code=None):
        self.date_started = date_started  # type: datetime.datetime
        self.date_stopped = date_stopped  # type: datetime.datetime
        self.override_date_started = override_date_started  # type: datetime.datetime
        self.exit_code = exit_code  # type: int

    @property
    def virtual_started(self):
        # type:() -> datetime.datetime
        return self.override_date_started or self.date_started

    @virtual_started.setter
    def virtual_started(self, value):
        # type: (datetime.datetime) -> None
        self.override_date_started = value

    @property
    def virtual_stopped(self):
        # VV: A task-quantum might still be in flight, assume that it will take
        #     less than 10 years to complete
        return self.date_stopped or (self.date_started + datetime.timedelta(days=365 * 10))


class ComponentDescriptor(object):
    def __init__(self, experiment, stage, component,
                 backend,
                 executable,
                 repeat_interval,
                 ranksPerNode, numberProcesses, numberThreads,
                 walltime, date_initialized, is_active):
        self.experiment = experiment  # type: str
        self.stage = stage  # type: str
        self.component = component  # type: str
        self.backend = backend  # type: str
        self.repeat_interval = repeat_interval  # type: bool
        self.ranksPerNode = ranksPerNode  # type: int
        self.numberProcesses = numberProcesses  # type: int
        self.numberThreads = numberThreads  # type: int
        self.walltime = walltime  # type: float
        self.executable = executable  # type: str
        self.date_initialized = date_initialized  # type: datetime.datetime
        self.is_active = is_active

    @property
    def is_repeat(self):
        return self.repeat_interval is not None

    @property
    def reference(self):
        return '%s.%s' % (self.stage, self.component)

    def to_dict(self):
        date_initialized = self.date_initialized.strftime("%d%m%y-%H%M%S.%f") if self.date_initialized else None
        return {
            'experiment': self.experiment,
            'stage': self.stage,
            'component': self.component,
            'backend': self.backend,
            'repeat-interval': self.repeat_interval,
            'ranksPerNode': self.ranksPerNode,
            'numberProcesses': self.numberProcesses,
            'numberThreads': self.numberThreads,
            'walltime': self.walltime,
            'executable': self.executable,
            'date-initialized': date_initialized
        }

    @staticmethod
    def from_dict(dictionary):
        experiment = dictionary['experiment']
        stage = dictionary['stage']
        component = dictionary['component']
        backend = dictionary['backend']
        repeat_interval = dictionary['repeat-interval']
        ranksPerNode = dictionary['ranksPerNode']
        numberProcesses = dictionary['numberProcesses']
        numberThreads = dictionary['numberThreads']
        walltime = dictionary['walltime']
        executable = dictionary['executable']
        date_initialized = dictionary['date-initialized']

        if date_initialized is not None:
            date_initialized = datetime.datetime.strptime(date_initialized, "%d%m%y-%H%M%S.%f")

        return ComponentDescriptor(experiment=experiment,
                                   stage=stage,
                                   component=component,
                                   backend=backend,
                                   executable=executable,
                                   repeat_interval=repeat_interval,
                                   ranksPerNode=ranksPerNode,
                                   numberProcesses=numberProcesses,
                                   numberThreads=numberThreads,
                                   walltime=walltime,
                                   date_initialized=date_initialized,
                                   is_active=False)


class Measurement(object):
    def __init__(self, component, date_last_exec, date_last_success, dur_last_exec,
                 time_execute=None,
                 time_queue=None,
                 exit_code=None,
                 date_queued=None,
                 cost_queue_pending=None,
                 cost_queue_running=None,
                 cost_queue_finished=None,
                 prod_time_avg=None,
                 prod_time_succeed_sum=None,
                 prod_time_succeed_max=None,
                 prod_percent_running=None,
                 times_failed_since_last_success=0,
                 exec_time_since_last_success=None):
        """Contains information for a single Run of some Component.

        This information is not all available at the same time. See parameter description
        for more information.

        RepeatingComponent life cycle:
        [I] <Wait till there's input> -> [QS] Queue -> [RS] Run [RD]-> Wait(repeat interval)
        -> Repeat

        * [I] -> Event instantiated (only emitted once)
        * [QS] -> Queueing started (emitted multiple times)
        * [RS] -> Running started (emitted multiple times, but possibly NOT when the
                actual event took place. We know for sure it will be emitted before
                RD (running done))
        * [RD] -> Emitted some time after the Run finishes (there might be a significant delay)

        Notice that events might be emitted with significant delays, so this class
        should NOT directly poll datetime to produce timestamps it should rely on
        method parameters.
        """
        self.time_execute = time_execute  # type: float
        self.time_queue = time_queue  # type: float
        self.component = component  # type: ComponentDescriptor
        self.date_last_exec = date_last_exec  # type: datetime.datetime
        self.date_last_success = date_last_success  # type: datetime.datetime
        self.exit_code = exit_code  # type: int
        self.dur_last_exec = dur_last_exec  # type: float

        self.date_queued = date_queued  # type: datetime.datetime

        self.cost_queue_finished = cost_queue_finished  # type: float
        self.cost_queue_running = cost_queue_running  # type: float
        self.cost_queue_pending = cost_queue_pending  # type: float

        self.original_cost_pending = cost_queue_pending  # type: float
        self.original_cost_running = cost_queue_running  # type: float
        self.original_cost_finished = cost_queue_finished  # type: float

        self.prod_time_avg = prod_time_avg  # type: float
        self.prod_time_succeed_sum = prod_time_succeed_sum # type: float
        self.prod_percent_running = prod_percent_running  # type: float
        self.prod_time_succeed_max = prod_time_succeed_max  # type: float

        self.times_failed_since_last_success = times_failed_since_last_success  # type: int
        self.exec_time_since_last_success = exec_time_since_last_success

        # VV: This is filled in IF the exit-code is not None once we find out the effective-repeat-interval
        #     ( which takes place in modelist.event_run_stop())
        self.task_quantum = None  # type: Optional[TaskQuantum]

    def dt_last_exec(self, to_date):
        # type: (datetime.datetime) -> float
        if to_date is not None:
            if self.date_last_exec is not None:
                return (to_date - self.date_last_exec).total_seconds()
            # VV: Assume that the last successful run finished just before this Component
            #     got initialized
            if self.component.date_initialized is not None:
                return (to_date - self.component.date_initialized).total_seconds()

    def dt_last_success(self, to_date):
        # type: (datetime.datetime) -> float
        if to_date is not None:
            if self.date_last_success is not None:
                return (to_date - self.date_last_success).total_seconds()

            # VV: Assume that the last run finished just before this Component
            #     got initialized
            if self.component.date_initialized is not None:
                return (to_date - self.component.date_initialized).total_seconds()

    @property
    def resource_cost_estimated(self):
        return (self.component.numberProcesses
                * self.component.numberThreads
                * self.component.walltime)

    @property
    def resource_cost_measured(self):
        return (self.component.numberProcesses
                * self.component.numberThreads
                * self.time_execute)

    @property
    def date_started(self):
        # type: () -> datetime.datetime
        if self.date_queued is not None and self.time_queue is not None:
            return self.date_queued + datetime.timedelta(seconds=self.time_queue)

    @property
    def date_stopped(self):
        # type: () -> datetime.datetime
        date_started_run = self.date_started
        if self.time_execute is not None and date_started_run is not None:
            return date_started_run + datetime.timedelta(seconds=self.time_execute)


class Modelist(object):
    def __init__(self):
        self.log = logging.getLogger('Modelist')

        self.metadata = {
        }  # type: Dict[ComponentDescriptor, EngineMetaData]

        self.producers_of = {

        }  # type: Dict[ComponentDescriptor, List[ComponentDescriptor]]

        self.consumers_of = {

        }  # type: Dict[ComponentDescriptor, List[ComponentDescriptor]]

        self.descriptors = {
            # VV: experiment_name : {
            #       stage_name : {
            #         component_name : ComponentDescriptor
            #     }
            # }
        }  # type: Dict[str, Dict[str, Dict[str, ComponentDescriptor]]]

        self.recorded_measurements = {
            # VV: component: { List[Measurement (chronological order)] }
        }  # type: Dict[ComponentDescriptor, List[Measurement]]

        self.backend_state = {
            # VV: FIXME add information about WHICH backend is used:
            'queued': {},  # type: Dict[ComponentDescriptor, datetime.datetime]
            'running': {},  # type: Dict[ComponentDescriptor, datetime.datetime]
            'finished': []  # type: List[Tuple[datetime.datetime, ComponentDescriptor, float]]
        }

    def desc_from_reference(self, reference, experiment_name):
        stage, name = reference.split('.')
        try:
            desc = self.descriptors[experiment_name][stage][name]
            return desc
        except KeyError:
            return None

    def register_component(self, experiment, stage, component,
                           backend,
                           executable,
                           repeat_interval,
                           ranksPerNode, numberProcesses, numberThreads,
                           walltime,
                           date_initialized,
                           producer_descriptors=None):
        desc = ComponentDescriptor(experiment, stage, component,
                                            backend,
                                            executable,
                                            repeat_interval,
                                            ranksPerNode, numberProcesses, numberThreads,
                                            walltime, date_initialized, True)

        if experiment not in self.descriptors:
            self.descriptors[experiment] = {}

        if stage not in self.descriptors[experiment]:
            self.descriptors[experiment][stage] = {}

        assert component not in self.descriptors[experiment][stage]

        assert desc not in self.producers_of

        self.producers_of[desc] = producer_descriptors

        if desc not in self.consumers_of:
            self.consumers_of[desc] = []

        for producer in producer_descriptors:
            if producer not in self.consumers_of:
                self.consumers_of[producer] = [desc]
            else:
                self.consumers_of[producer].append(desc)

        # VV: All we know about this desc is that this is the first time we come across it, and
        #     its repeat_interval (if any)
        meta = EngineMetaData(
            None,
            None,
            None,
            None,
            repeat_interval,
            0
        )
        assert desc not in self.metadata

        self.metadata[desc] = meta

        self.descriptors[experiment][stage][component] = desc

        self.recorded_measurements[desc] = []

        return desc

    def event_queue_start(self, experiment, stage, component, date_now):
        #  type: (str, str, str, datetime.datetime) -> None
        self.log.critical('event_queue_start %s %s %s' % (experiment, stage, component))
        desc = self.descriptors[experiment][stage][component]
        safe_to_increase_cost = True

        assert desc not in self.backend_state['queued']

        self.backend_state['queued'][desc] = date_now

        if self.recorded_measurements[desc]:
            # VV: find the last measurement that actually ran

            for last_meas in self.recorded_measurements[desc][::-1]:
                if last_meas.exit_code is not None:
                    break
            else:
                # VV: Should we ever reach this point, we just have to use the very first
                #     measurement even though it crashed because we can use ITS last success/last run
                #     (recall that we faked these values under register_component)
                last_meas = self.recorded_measurements[desc][0]

            last_success = None
            last_exec = None
            dur_last_exec = None
            exec_time_since_last_success = 0

            if last_meas.exit_code == 0:
                # VV: Last run was successful
                last_exec = last_meas.date_stopped
                dur_last_exec = last_meas.time_execute
                last_success = last_meas.date_stopped
                exec_time_since_last_success = 0
            elif last_meas.exit_code is not None:
                # VV: Last run was not successful
                last_exec = last_meas.date_stopped
                dur_last_exec = last_meas.time_execute
                last_success = last_meas.date_last_success
                exec_time_since_last_success = last_meas.exec_time_since_last_success + dur_last_exec
            elif last_meas.exit_code is None:
                # VV: Component did not launch the last time we tried. Pretend that this never
                #     happened and re-use whatever info the failed-to-launch run used
                last_exec = last_meas.date_last_exec
                dur_last_exec = last_meas.dur_last_exec
                last_success = last_meas.date_last_success

            # VV: Sanitize
            dur_last_exec = max(0.0, dur_last_exec)

            meas = Measurement(
                component=desc,
                date_last_exec=last_exec,
                date_last_success=last_success,
                dur_last_exec=dur_last_exec,
                date_queued=date_now,
                exec_time_since_last_success=exec_time_since_last_success
            )
        else:
            # VV: This is the beginning of the very first measurement,
            #     fake some data about the previous, and completely imagined, run
            meas = Measurement(
                component=desc,
                date_last_exec=desc.date_initialized,
                date_last_success=desc.date_initialized,
                dur_last_exec=0,
                date_queued=date_now,
                exec_time_since_last_success=0
            )

        self.recorded_measurements[desc].append(meas)

    def event_run_start(self, experiment, stage, component, date_now):
        #  type: (str, str, str, datetime.datetime) -> None
        desc = self.descriptors[experiment][stage][component]

        if desc not in self.backend_state['queued']:
            self.log.critical("%s is queued:%s, running: %s, finished:%s" % (
                desc.reference,
                desc in self.backend_state['queued'],
                desc in self.backend_state['running'],
                desc in self.backend_state['finished'],
            ))

        assert desc in self.backend_state['queued']

        del self.backend_state['queued'][desc]

        self.backend_state['running'][desc] = date_now

        # VV: We know that the component started running at "date_now",
        #    now we can update the queue time
        current = self.recorded_measurements[desc][-1]
        current.time_queue = (date_now - current.date_queued).total_seconds()

        # VV: Mark this TaskQuantum to be 'in-flight' (we don't know when it'll finish
        #    , see quantum.virtual_stopped)
        self.metadata[desc].task_quantums.append(
            TaskQuantum(date_started=date_now, date_stopped=None)
        )

    def compute_effective_delta_time(self, desc, to_date):
        # type: (ComponentDescriptor, datetime.datetime) -> Dict[str, float]
        if self.metadata[desc].last_success_stopped:
            """ VV: Schematic to understand the code 
                    (i.e earliest_date_lookup and task_quantum_to_exec_time)
            n = not accounted by this run
            y = accounted by this run
            c = time-step within this run (could be failed or successful)
            l = time-step within last successful run
            f = past FAILED run (there's a chance it doesn't exist because previous run was successful
                                 doesn't change a thing in the algorithm; it's only shown for brevity)

            VV: So what about producer runs which started WHILE the consumer was running ?
                Well we can't know about these before attempting to execute the consumer, in fact
                we may event want to dismiss information which we found out after we Submitted the
                Task to the backend because we didn't know about it when we made the decision to
                execute the Task at hand.
            VV: FIXME Look at the above comment --^

            -nnnnnyyyyyyy---yyyynnnnnn----nnnnnnn--nn-  <- producer quantums (even future ones) 
            ------llll---fffff--cccccccccccccc--------  <- desc       
                     ^          ^            ^
                     |  when_run_started     |
                     |                   date_now
            last_success_stopped
            """

            def earliest_date_lookup(task_quantum, to_date):
                # type: (TaskQuantum, datetime.datetime) -> bool
                assert task_quantum.date_stopped is None or (
                        task_quantum.virtual_started <= task_quantum.date_stopped
                )

                return (task_quantum.date_started < to_date) and (
                        task_quantum.date_stopped is None
                        or self.metadata[desc].last_success_started < task_quantum.date_stopped
                )
        else:
            def earliest_date_lookup(task_quantum, to_date):
                # type: (TaskQuantum, datetime.datetime) -> bool
                return task_quantum.date_started < to_date

        def quantum_to_observed_exec_time(task_quantum, to_date):
            # type: (TaskQuantum, datetime.datetime) -> Optional[Tuple[float, bool]]
            success = task_quantum.exit_code == 0

            if earliest_date_lookup(task_quantum, to_date):
                if task_quantum.date_stopped is None or task_quantum.date_stopped > to_date:
                    cutoff_high = to_date
                else:
                    cutoff_high = task_quantum.date_stopped

                if self.metadata[desc].last_success_started:
                    # VV: If we've run before then make sure to ONLY account for the time since the
                    #     beginning of this batch of tasks (which is after the last success started)
                    if self.metadata[desc].last_success_started > task_quantum.date_started:
                        cutoff_low = self.metadata[desc].last_success_started
                    else:
                        cutoff_low = task_quantum.date_started
                else:
                    # VV: This engine has never run before so there's no lower bound in the task-quantum
                    #     starting date
                    cutoff_low = task_quantum.date_started

                return (cutoff_high-cutoff_low).total_seconds(), success
            else:
                return None

        prod_time_min = 0
        prod_time_sum = 0
        prod_time_avg = 0
        prod_time_max = 0
        prod_time_unknown_sum = 0
        prod_time_succeed_sum = 0
        prod_time_succeed_max = 0
        prod_percent_running = 0
        prod_percent_done = 0

        def is_producer_relevant(producer):
            # type: (ComponentDescriptor) -> bool

            origin = self.metadata[desc].last_success_stopped

            if origin is None or self.metadata[producer].is_active(origin):
                return True

            return False

        relevant_producers = list(filter(is_producer_relevant, self.producers_of[desc]))

        for k in self.producers_of[desc]:
            if k not in relevant_producers:
                self.log.debug("Accumulated time (prod %s is irrelevant to %s)" % (
                    k.reference, desc.reference
                ))

        time_fail = {}  # type: Dict[ComponentDescriptor, float]
        time_succeed = {}  # type: Dict[ComponentDescriptor, float]
        time_total = {}  # type: Dict[ComponentDescriptor, float]
        time_unknown = {}  # type: Dict[ComponentDescriptor, Optional[float]]
        prod_running = {}  # type: Dict[ComponentDescriptor, bool]

        if relevant_producers:
            for prod in relevant_producers:
                time_fail[prod] = 0.0
                time_succeed[prod] = 0.0
                time_total[prod] = 0.0
                # VV: Leave this to None so that the assertion bellow checks for the correctness of the
                #     algorithm
                time_unknown[prod] = None
                prod_running[prod] = False
                for task_quantum in list(self.metadata[prod].task_quantums):
                    ret_tuple = quantum_to_observed_exec_time(task_quantum, to_date)
                    if ret_tuple is not None:
                        ret, success = ret_tuple

                        # VV: Backends might extremely small values
                        ret = max(0.5, ret)

                        # VV: Source Engines will always be treated as 'unknown' unless we force
                        #     them to register as Successes
                        if task_quantum.date_stopped is not None \
                                or prod.is_repeat is False:
                            # VV: Need to be able to inspect the task finish (date_stopped) BEFORE `to_date`
                            if success or prod.is_repeat is False:
                                time_succeed[prod] += ret
                            else:
                                time_fail[prod] += ret
                        else:
                            # VV: There can be ONLY one task in transit per producer
                            if time_unknown[prod] is not None:
                                raise ValueError("Producer %s of %s has multiple in-transit tasks" % (
                                    prod.reference, desc.reference
                                ))

                            time_unknown[prod] = ret

                        if task_quantum.date_stopped is None or task_quantum.date_stopped > to_date:
                            prod_running[prod] = True

                time_unknown[prod] = time_unknown[prod] or 0.0
                time_total[prod] = time_fail[prod] + time_succeed[prod] + time_unknown[prod]

            prod_percent_done = (
                    (len(self.producers_of[desc]) - len(relevant_producers)) / float(len(self.producers_of[desc]))
            )

            prod_percent_running = sum(prod_running.values()) / float(len(relevant_producers))
            prod_time_unknown_sum = sum(time_unknown.values())
            prod_time_sum = sum(time_total.values())
            prod_time_min = min(time_total.values())
            prod_time_max = max(time_total.values())
            prod_time_avg = prod_time_sum / float(len(relevant_producers))
            prod_time_succeed_max = max(time_succeed.values())
            prod_time_succeed_sum = sum(time_succeed.values())

        return {
            'prod-time-unknown-sum': prod_time_unknown_sum,
            'prod-time-sum': prod_time_sum,
            'prod-time-min': prod_time_min,
            'prod-time-max': prod_time_max,
            'prod-time-avg': prod_time_avg,
            'prod-time-succeed-sum': prod_time_succeed_sum,
            'prod-time-succeed-max': prod_time_succeed_max,
            'prod-percent-running': prod_percent_running,
            'prod-percent-done': prod_percent_done,
            'time-total': time_total,
            'time-succeed': time_succeed,
            'time-unknown': time_unknown,
            'prod-running': prod_running,
            'time-fail': time_fail
        }

    def _copy_delta_info_to_measurement(self, measurement, delta):
        # type: (Measurement, Dict[str, float]) -> None
        measurement.prod_percent_running = delta['prod-percent-running']
        measurement.prod_time_avg = delta['prod-time-avg']
        measurement.prod_time_succeed_sum = delta['prod-time-succeed-sum']
        measurement.prod_time_succeed_max = delta['prod-time-succeed-max']

    def event_run_stop(self, experiment, stage, component, date_now, exit_code):
        # type: (str, str, str, datetime.datetime, int) -> None
        desc = self.descriptors[experiment][stage][component]

        if desc not in self.backend_state['running']:
            self.log.critical("%s is queued:%s, running: %s, finished:%s" % (
                desc.reference,
                desc in self.backend_state['queued'],
                desc in self.backend_state['running'],
                desc in self.backend_state['finished'],
            ))

        assert desc in self.backend_state['running']

        date_start = self.backend_state['running'][desc]
        actual_walltime = (date_now - date_start).total_seconds()
        self.backend_state['running'][desc] = date_now
        del self.backend_state['running'][desc]

        entry = [date_now, desc, actual_walltime]
        self.backend_state['finished'].append(entry)

        current = self.recorded_measurements[desc][-1]
        time_execute = (date_now - current.date_started).total_seconds()
        current.exit_code = exit_code
        current.time_execute = time_execute
        current.exec_time_since_last_success += time_execute

        assert self.metadata[desc].task_quantums[-1].date_stopped is None
        self.metadata[desc].task_quantums[-1].date_stopped = date_now
        self.metadata[desc].task_quantums[-1].exit_code = exit_code

        if exit_code is None or time_execute is None:
            self.metadata[desc].run_stop(date_now, exit_code)
            return

        delta_info = self.compute_effective_delta_time(desc, self.metadata[desc].when_run_started)

        self.log.info("Exit-code:%s Sum:%.2f Min:%.2f Max:%.2f Avg:%.2f "
                      "- %%Fly:%.2f %%Done:%.2f for %s" % (
                          exit_code,
                          delta_info['prod-time-sum'], delta_info['prod-time-min'], delta_info['prod-time-max'],
                          delta_info['prod-time-avg'], delta_info['prod-percent-running'],
                          delta_info['prod-percent-done'],
                          desc.reference
                      ))

        self._copy_delta_info_to_measurement(current, delta_info)

        if desc.is_repeat and exit_code == 0:
            # VV: We've just had a successful run, we can now measure the 'effective-repeat-interval'

            # VV: TODO Decide a better measurement to use as the effective-repeat-interval ?
            effective_repeat_interval = delta_info['prod-time-succeed-max']
            self.log.info("Effective interval: %s for %s" % (
                effective_repeat_interval, desc.reference
            ))

            self.metadata[desc].observed_effective_repeat_intervals.append(effective_repeat_interval)

        self.metadata[desc].run_stop(date_now, exit_code)


class EngineMetaData(object):
    def __init__(self,
                 when_queue_started,
                 when_queue_stopped,
                 when_run_started,
                 when_run_stopped,
                 delay,
                 times_run=0,
                 times_successful_run=0,
                 went_inactive_at=None):
        self.task_quantums = []  # type: List[TaskQuantum]
        self.when_queue_started = when_queue_started  # type: datetime.datetime
        self.when_queue_stopped = when_queue_stopped  # type: datetime.datetime
        self.when_run_started = when_run_started  # type: datetime.datetime
        self.when_run_stopped = when_run_stopped  # type: datetime.datetime

        self.original_delay = delay
        self.times_run = times_run  # type: int

        self.times_failed_since_success = 0
        self.times_successful_run = times_successful_run
        self.last_run_stopped = when_run_stopped
        self.last_success_started = None  # type: datetime.datetime
        self.last_success_stopped = None  # type: datetime.datetime
        self.is_repeat = (delay is not None)
        self.next_delay = None
        self.next_queue_date = None
        self.went_inactive_at = went_inactive_at

        self.observed_effective_repeat_intervals = []  # type: List[float]

    def is_active(self, when):
        # type: (datetime.datetime) -> bool
        if self.went_inactive_at is None:
            return True

        return self.went_inactive_at >= when

    def queue_start(self, when):
        self.when_queue_started = when
        self.when_queue_stopped = None
        self.when_run_started = None
        self.when_run_stopped = None
        assert when is not None

    def queue_stop(self, when):
        # type: (datetime.datetime) -> None
        assert self.when_queue_started is not None
        assert self.when_queue_stopped is None
        assert self.when_run_started is None
        assert self.when_run_stopped is None
        assert when is not None

        self.when_queue_stopped = when

    def run_start(self, when):
        # type: (datetime.datetime) -> None
        assert self.when_queue_started is not None
        assert self.when_queue_stopped is not None
        assert self.when_run_started is None
        assert self.when_run_stopped is None
        assert when is not None

        self.when_run_started = when

    def run_stop(self, when, exit_code):
        # type: (datetime.datetime, Optional[int]) -> None
        assert self.when_queue_started is not None
        assert self.when_queue_stopped is not None
        assert self.when_run_started is not None
        assert self.when_run_stopped is None
        assert when is not None

        self.when_run_stopped = when
        self.last_run_stopped = when
        self.times_run += 1

        if exit_code == 0:
            self.times_successful_run += 1

            self.last_success_started = self.when_run_started
            self.last_success_stopped = self.when_run_stopped
            self.times_failed_since_success = 0
        else:
            self.times_failed_since_success += 1

    def register_next_delay(self, next_delay):
        assert self.is_repeat

        self.next_delay = next_delay

    def register_next_queue_date(self, next_queue_date):
        self.next_queue_date = next_queue_date


class OptimiserDecisionMetadata(object):
    def __init__(self, desc, exploit_chance, exploit_target, explore_target_low, explore_target_high):
        # type: (ComponentDescriptor, float, float, float, float) -> None
        self._last_recorded_times_run = None  # type: Optional[int]
        self._exploit_chance = exploit_chance

        # VV: True = Exploit, False = Explore
        self._should_exploit_next = False
        self._desc = desc

        self._exploit_target_factor = exploit_target
        self._explore_target_low = explore_target_low
        self._explore_target_high = explore_target_high
        self._date_last_stopped = None
        self._explore_target_factor = None

        self._date_to_submit = None  # type: Optional[datetime.datetime]

        self._is_dirty = True

        self.target_effective_repeat_interval = None  # type: Optional[float]
        self.attempts = 0

    def should_exploit(self, metadata):
        # type: (EngineMetaData) -> bool

        if metadata.times_run != self._last_recorded_times_run:
            self._should_exploit_next = (random.uniform(0.0, 1.0) <= self._exploit_chance)

            if self._should_exploit_next is False:
                self._explore_target_factor = random.uniform(
                    self._explore_target_low, self._explore_target_high
                )

        self._last_recorded_times_run = metadata.times_run

        return self._should_exploit_next

    def explore_target_factor(self):
        return self._explore_target_factor

    def exploit_target_factor(self):
        return self._exploit_target_factor

    def register_choice(self, date_to_submit, date_last_stopped):
        # type: (datetime.datetime, datetime.datetime) -> None
        self._date_to_submit = date_to_submit
        self._date_last_stopped = date_last_stopped
        self.is_dirty = False

    def next_repeat_interval(self):
        if self.is_dirty:
            return None

        return (self._date_to_submit - self._date_last_stopped).total_seconds()

    @property
    def is_dirty(self):
        # type: () -> bool
        return self._is_dirty

    @is_dirty.setter
    def is_dirty(self, value):
        # type: (bool) -> None
        self._is_dirty = value

        if value is True:
            self._date_to_submit = None


class Optimizer(object):
    def __init__(self, experiment_name, modelist=None):
        if modelist is None:
            modelist = Modelist()

        self.modelist = modelist

        # VV: All active ComponentDescriptors and their Consumer references
        #     ordered based on when they were submitted for scheduling (lower index = earlier)
        self.active_ordered = []  # type: List[ComponentDescriptor]

        self.lock_engine_meta_data = threading.RLock()

        # VV: @tag: OPTIMIZER-observe-engine-updates
        # VV: There's a small chance an engine emits a task-event twice!
        self.past_event_hashes = set()  # type: set[Tuple[int, str, str, datetime.datetime]]
        self.engine_metadata_pending = [
            # VV: stage, component_name,
            #     initialized|queue_start|queue_stop|run_start|run_stop|finished_completely,
            #     when,
            #     exit-code|None|[backend, executable, repeat_interval, ... (all from register_component)...]
        ]  # type: List[Tuple[ComponentDescriptor, str, datetime.datetime, Any]]

        self.engine_metadata = self.modelist.metadata
        self.producers_of = self.modelist.producers_of
        self.consumers_of = self.modelist.consumers_of

        self.experiment_name = experiment_name

        self.lock = threading.RLock()

        self.log = logging.getLogger('Optimizer')

        self.decisions = {

        }  # type: Dict[ComponentDescriptor, OptimiserDecisionMetadata]

    def desc_from_reference(self, reference):
        return self.modelist.desc_from_reference(reference, self.experiment_name)

    def register_component(self, stage, component,
                           backend,
                           executable,
                           repeat_interval,
                           ranksPerNode, numberProcesses, numberThreads,
                           walltime,
                           date_initialized,
                           producer_references,
                           exploit_chance,
                           exploit_target,
                           explore_target_low,
                           explore_target_high
                           ):

        exploit_chance = exploit_chance if exploit_chance is not None else 0.9
        exploit_target = exploit_target if exploit_target is not None else 0.75
        explore_target_low = explore_target_low if explore_target_low is not None else 0.25
        explore_target_high = explore_target_high if explore_target_high is not None else 0.5

        entry = [
            stage, component,
            'initialized',
            date_initialized,
            (backend,
             executable,
             repeat_interval,
             ranksPerNode, numberProcesses, numberThreads,
             walltime,
             date_initialized,
             producer_references,
             exploit_chance,
             exploit_target,
             explore_target_low,
             explore_target_high
             )
        ]

        with self.lock_engine_meta_data:
            self.engine_metadata_pending.append(entry)

    def _register_component(self, stage, component,
                            backend,
                            executable,
                            repeat_interval,
                            ranksPerNode, numberProcesses, numberThreads,
                            walltime,
                            date_initialized,
                            producer_references,
                            exploit_chance,
                            exploit_target,
                            explore_target_low,
                            explore_target_high):

        def stage_name_to_index(my_stage):
            # type: (str) -> int
            prefix = 'stage'
            assert my_stage.startswith(prefix)
            index = int(my_stage[len(prefix):])
            del prefix

            return index

        # VV: TODO Does it make sense to keep track of Source components too ?
        my_stage = stage_name_to_index(stage)

        in_stage_deps = [ref for ref in producer_references if my_stage <= stage_name_to_index(ref.split('.')[0])]

        producer_descs = []
        for dep in in_stage_deps:
            descr = self.desc_from_reference(dep)
            if descr is None:
                raise ValueError(
                    "InStageDependency %s of %s.%s cannot be mapped to an existing Producer description" % (
                        dep, stage, component,
                ))
            producer_descs.append(descr)

        desc = self.modelist.register_component(self.experiment_name, stage, component,
                                                backend,
                                                executable,
                                                repeat_interval,
                                                ranksPerNode, numberProcesses, numberThreads,
                                                walltime,
                                                date_initialized,
                                                producer_descriptors=producer_descs)

        assert desc not in self.active_ordered
        self.active_ordered.append(desc)

        self.decisions[desc] = OptimiserDecisionMetadata(desc,
                                                         exploit_chance,
                                                         exploit_target,
                                                         explore_target_low, explore_target_high)

    def event_queue_start(self, stage, component, when):
        entry = [
            stage, component,
            'queue_start',
            when,
            None
        ]
        event_hash = (stage, component, 'queue_start', when)

        with self.lock_engine_meta_data:
            if event_hash not in self.past_event_hashes:
                self.past_event_hashes.add(event_hash)
                self.engine_metadata_pending.append(entry)
            else:
                self.log.warning("Received a duplicate event %s" % entry)

    def _event_queue_start(self, stage, component, when):
        with self.lock:
            self.modelist.event_queue_start(self.experiment_name, stage, component, when)
            desc = self.modelist.descriptors[self.experiment_name][stage][component]
            self.engine_metadata[desc].queue_start(when)

    def event_queue_stop(self, stage, component, when):
        entry = [
            stage, component,
            'queue_stop',
            when,
            None
        ]

        event_hash = (stage, component, 'queue_stop', when)

        with self.lock_engine_meta_data:
            if event_hash not in self.past_event_hashes:
                self.past_event_hashes.add(event_hash)
                self.engine_metadata_pending.append(entry)
            else:
                self.log.warning("Received a duplicate event %s" % entry)

    def _event_queue_stop(self, stage, component, when):
        with self.lock:
            desc = self.modelist.descriptors[self.experiment_name][stage][component]
            self.engine_metadata[desc].queue_stop(when)

    def event_run_start(self, stage, component, when):
        entry = [
            stage, component,
            'run_start',
            when,
            None
        ]

        event_hash = (stage, component, 'run_start', when)

        with self.lock_engine_meta_data:
            if event_hash not in self.past_event_hashes:
                self.past_event_hashes.add(event_hash)
                self.engine_metadata_pending.append(entry)
            else:
                self.log.warning("Received a duplicate event %s" % entry)

    def _event_run_start(self, stage, component, when):
        with self.lock:
            self.modelist.event_run_start(self.experiment_name, stage, component, when)
            desc = self.modelist.descriptors[self.experiment_name][stage][component]
            self.engine_metadata[desc].run_start(when)

    def event_run_stop(self, stage, component, when, exit_code):
        entry = [
            stage, component,
            'run_stop',
            when,
            exit_code
        ]

        event_hash = (stage, component, 'run_stop', when)

        with self.lock_engine_meta_data:
            if event_hash not in self.past_event_hashes:
                self.past_event_hashes.add(event_hash)
                self.engine_metadata_pending.append(entry)
            else:
                self.log.warning("Received a duplicate event %s" % entry)

    def _decide_next_effective_repeat_interval(self, desc, date_last_started, date_last_stopped, exit_code):
        metadata = self.engine_metadata[desc]
        decision = self.decisions[desc]

        def guess_next_effective_repeat_interval():
            max_recent = 3
            values = metadata.observed_effective_repeat_intervals[-max_recent:]

            if len(values) > 1:
                weight_max = 2
                weight_min = 1

                weights = [idx * (weight_max - weight_min) / (len(values) - 1.0) + weight_min for idx in range(len(values))]

                guess = sum([w_v[0] * w_v[1] for w_v in zip(weights, values)]) / float(sum(weights))
            elif len(values) == 1:
                guess = values[0]
            else:
                guess = None

            return guess

        if exit_code == 0:
            # VV: Modelist populates observed_effective_repeat_interval

            guess = guess_next_effective_repeat_interval()

            if guess is None:
                self.log.critical("%s had a successful task but no observed "
                                  "effective-repeat interval" % desc.reference)
                assert False

            self.log.info("%s next-eri: %.2f because it has just succeeded on %s" % (
                desc.reference, guess, date_last_stopped
            ))
            return guess
        else:
            # VV: We've just failed find out how long we waited for the producer to execute
            #     for and increase that target eri accordingly
            prod_info = self.modelist.compute_effective_delta_time(desc, date_last_started)
            current_progress = prod_info['prod-time-succeed-max']

            guess = guess_next_effective_repeat_interval()

            if guess is not None and guess < current_progress:
                self.log.info("%s keeping next-eri: %.2f because it failed on %s %d times and tested out %.2f" % (
                    desc.reference, guess, date_last_stopped, decision.attempts, current_progress
                ))
                return guess

            if current_progress <= 10 * 60:
                delta = min(60.0, current_progress * 1.5)
            elif current_progress <= 60 * 60:
                delta = min(60.0 * 10, current_progress * 1.5)
            elif current_progress <= 12 * 60 * 60:
                delta = 60.0 * 30
            elif current_progress <= 24 * 60 * 60:
                delta = 60.0 * 60
            else:
                # VV: Looks like a huge job, try every 4 hours
                delta = min(60.0 * 60 * 4, current_progress * 1.5)

            delta = max(1.0, delta)

            guess = current_progress + delta

            self.log.info("%s next-eri: %.2f because it failed on %s %d times" % (
                desc.reference, guess, date_last_stopped, decision.attempts
            ))
            return guess

    def _event_run_stop(self, stage, component, when, exit_code):
        with self.lock:
            self.modelist.event_run_stop(self.experiment_name, stage, component, when, exit_code)

            desc = self.modelist.descriptors[self.experiment_name][stage][component]

            if desc.is_repeat:
                decision = self.decisions[desc]
                if exit_code == 0:
                    self.log.info("It took %d attempts before %s succeeded" % (
                        decision.attempts, desc.reference
                    ))

                    # VV: We have just completed successfully, reset the last decision
                    decision.is_dirty = True
                    decision.target_effective_repeat_interval = None
                    decision.attempts = 0
                else:
                    decision.attempts += 1
                metadata = self.engine_metadata[desc]
                if metadata.is_active(when):
                    # VV: Given that we've just succeeded/failed pick the next repeat-interval
                    target_eri = self._decide_next_effective_repeat_interval(desc, metadata.when_run_started,
                                                                             when, exit_code)
                    assert target_eri is not None
                    decision.target_effective_repeat_interval = target_eri
                else:
                    self.log.info("%s is not a candidate for optimization" % desc.reference)

    def event_finished_completely(self, stage, component, when):
        entry = [
            stage, component,
            'finished_completely',
            when,
            None # VV: No payload
        ]

        with self.lock_engine_meta_data:
            self.engine_metadata_pending.append(entry)

    def _event_finished_completely(self, stage, component, when):
        with self.lock:
            desc = self.modelist.descriptors[self.experiment_name][stage][component]
            self.engine_metadata[desc].went_inactive_at = when
            desc.is_active = False
            self.log.info("Optimizer will now stop optimizing %s (event received at: %s)" % (
                desc.reference, when
            ))

    def optimize(self, date_now):
        with self.lock:
            # self.log.info("Digesting pending engine events")
            if self._digest_events():
                # VV: Invalidate all past decisions because we've just got new information
                for desc in self.decisions:
                    decision = self.decisions[desc]
                    metadata = self.engine_metadata[desc]
                    decision.is_dirty = (decision._last_recorded_times_run != metadata.times_run)

                self.log.info("Running the Optimizer")
                self._optimize_kernel(date_now)
                self.log.info("Optimizer done running")

    def _digest_events(self):
        # type: () -> int
        with self.lock_engine_meta_data:
            pending = self.engine_metadata_pending[:]
            self.engine_metadata_pending = []

        processed = len(pending)

        for event in pending:
            stage, component_name, event_name, when, payload = event
            self.log.info("Digesting %s for %s.%s from %s payload: %s" % (
                event_name, stage, component_name, when, payload
            ))
            if event_name == 'initialized':
                (backend,
                 executable,
                 repeat_interval,
                 ranksPerNode, numberProcesses, numberThreads,
                 walltime,
                 date_initialized,
                 producer_references,
                 exploit_chance,
                 exploit_target,
                 explore_low,
                 explore_high
                 ) = payload

                self._register_component(
                    stage, component_name,
                    backend,
                    executable,
                    repeat_interval,
                    ranksPerNode, numberProcesses, numberThreads,
                    walltime,
                    date_initialized,
                    producer_references,
                    exploit_chance,
                    exploit_target,
                    explore_low,
                    explore_high
                )
            elif event_name == 'queue_start':
                self._event_queue_start(stage, component_name, when)
            elif event_name == 'queue_stop':
                self._event_queue_stop(stage, component_name, when)
            elif event_name == 'run_start':
                self._event_run_start(stage, component_name, when)
            elif event_name == 'run_stop':
                self._event_run_stop(stage, component_name, when, payload)
            elif event_name == 'finished_completely':
                self._event_finished_completely(stage, component_name, when)
            else:
                assert False

        return processed

    def _explore_desc(self, desc, date_now, factor):
        # type: (ComponentDescriptor, datetime.datetime, float) -> Optional[Tuple[float, float]]

        # VV: Use factor to search for larger/smaller effective-repeat-intervals
        #     (small factors will steer optimiser towards looking for a smaller
        #     effective-repeat-interval)

        ret = self._target_effective_repeat_interval(desc, date_now, factor)

        if ret is not None:
            repeat_interval, eri = ret
            execute_at = self.engine_metadata[desc].last_run_stopped + datetime.timedelta(seconds=repeat_interval)

            self.log.debug("Explore ri:%.2f, eri:%.2f*%.2f for %s at %s" % (
                repeat_interval, eri, factor, desc.reference, execute_at
            ))

            return repeat_interval, eri

        return None

    def _exploit_desc(self, desc, date_now, factor):
        # type: (ComponentDescriptor, datetime.datetime, float) -> Optional[Tuple[float, float]]
        ret = self._target_effective_repeat_interval(desc, date_now, factor)

        if ret is not None:
            repeat_interval, eri = ret
            execute_at = self.engine_metadata[desc].last_run_stopped + datetime.timedelta(seconds=repeat_interval)

            self.log.debug("Exploit ri:%.2f, eri:%.2f*%.2f for %s at %s" % (
                repeat_interval, eri, factor, desc.reference, execute_at
            ))

            return repeat_interval, eri

        return None

    def _target_effective_repeat_interval(self, desc, date_now, factor, backend_delay_offset=15):
        # type: (ComponentDescriptor, datetime.datetime, float, float) -> Optional[Tuple[float, float]]
        # VV: Returns (suggested repeat interval, expected effective-repeat-interval)

        # VV: Use factor to search for larger/smaller effective-repeat-intervals
        #     (small factors will steer optimiser towards looking for a smaller
        #     effective-repeat-interval)

        metadata = self.engine_metadata[desc]  # type: EngineMetaData
        decision = self.decisions[desc]

        alive_producers = [p for p in self.producers_of[desc] if self.engine_metadata[p].is_active(date_now)]

        eff_rep_int = decision.target_effective_repeat_interval
        prod_info = self.modelist.compute_effective_delta_time(desc, date_now)

        if eff_rep_int is None:
            self.log.critical("%s does not have a target effective repeat interval" % desc.reference)

        assert eff_rep_int is not None

        if not alive_producers:
            # VV: All of the producers are done, we can't make any more choices
            metadata.register_next_delay(None)
            self.log.info("All producers of %s have finished. Will revert to default repeat-interval" % (
                desc.reference
            ))

            metadata.went_inactive_at = date_now
            desc.is_active = False
            return metadata.original_delay, eff_rep_int

        if metadata.last_run_stopped is None:
            # VV: This engine has never run before we can't optimise its repeat-interval if it's not
            #     currently attempting to repeat
            return None

        current_delay = (date_now - metadata.last_run_stopped).total_seconds()
        current_progress = prod_info['prod-time-succeed-max']

        time_succeed = prod_info['time-succeed']  # type: Dict[ComponentDescriptor, float]
        prod_running = prod_info['prod-running']  # type: Dict[ComponentDescriptor, bool]

        # VV: Some backends (e.g. LSF) introduce significant delays so we can't know for sure
        #     whether a producer is actually running right now. Set the repeat interval to
        #     expire 30 seconds after what we now believe to be adequate time to wait for the
        #     producers to execute for. This means that the Optimizer puts an artificial lower
        #     threshold to the effective-repeat interval. The advantage is that it will make
        #     fewer mistakes (as in less faild-executions). If no message arrives within the
        #     next K seconds then the optimizer will assume that everything is fine. This is
        #     best used for RepeatingEngines because plain Engines will likely not fail.
        #     K depends on a) the backend, b) the priority of the job, for the time being
        #     we'll use an optimistic delay of 15 seconds.

        if current_progress == 0 or (current_progress <= eff_rep_int * factor) and prod_running:
            # VV: Our model suggests that we should not execute right now but some of our
            #     producers are running right now, let's pick a time in the future where
            #     our prod-time-succeed-max will be good-enough

            # VV: Find the producer with the highest prod-succeed-time which is currently running
            pair_succeed = [(p, time_succeed[p]) for p in list(prod_running.keys())]
            pair_succeed_sorted = sorted(pair_succeed, key=lambda p: p[1], reverse=True)
            top_prod, top_succeed_time = pair_succeed_sorted[0]

            # VV: If it's a repeating engine it might fail, but we expect that Source engines never fail
            offset = backend_delay_offset if top_prod.is_repeat else 0.0

            remaining_time = offset + eff_rep_int * factor - top_succeed_time

            # VV: TODO At this point we could predict WHICH of the producers will actually execute for long
            #     enough so that we reach the desired eff_rep_int (using the execute models)

            next_repeat = remaining_time
            self.log.debug("%s max producer is %s with %.2f remaining (progress:%.2f), "
                          "repeat-interval=%.2f (eri: %.2f*%.2f)" % (
                              desc.reference, top_prod.reference,
                              remaining_time, top_succeed_time,
                              next_repeat, eff_rep_int, factor
            ))

            return next_repeat + current_delay, eff_rep_int
        elif current_progress >= eff_rep_int * factor:
            # VV: looks like we should be already running, make it so we start immediately-ish
            self.log.debug("%s should be already repeating. Progress: %.2f, eri: %.2f*%.2f" % (
                desc.reference, current_progress, eff_rep_int, factor,
            ))

            # VV: If we've already thought of some repeat-interval then just use that one
            if decision.next_repeat_interval() is not None:
                return decision.next_repeat_interval(), eff_rep_int
            # VV: If this is the first time we are deciding the repeat interval then set it equal to
            #     the time that it would have been ideal to start executing
            return current_delay - (current_progress - eff_rep_int * factor), eff_rep_int
        else:
            assert not prod_running
            # VV: Producers are not running so it would be OK to assume that we can't run either
            #     but let's just pick a huge repeat-interval if nothing changes till that time we'll
            #     just do a bit of exploring in about 4 hours from the last time it executed
            #     or 10 * repeat_interval whichever comes further in the future. See comment
            #     in _guess_effective_repeat_interval for more information on why this is not a bad idea
            next_repeat = current_delay + max(4 * 3600, 10 * desc.repeat_interval)
            self.log.debug("%s producers are not running. repeat-interval:%.2f, eri: %.2f*%.2f" % (
                desc.reference, next_repeat, eff_rep_int, factor
            ))
            return next_repeat, eff_rep_int

    def _optimize_kernel(self, date_now):
        for desc in self.active_ordered:
            if desc.is_active and desc.is_repeat:
                self.optimize_single(desc, date_now)

    def optimize_single(self, desc, date_now):
        # type: (ComponentDescriptor, datetime.datetime) -> Optional[float]
        with self.lock:
            metadata = self.engine_metadata[desc]

            decision = self.decisions[desc]

            if metadata.last_run_stopped is None or decision.target_effective_repeat_interval is None:
                self.log.debug("Cannot decide because last_run_stopped: %s "
                              "and target_effective_repeat_interval %s "
                              "for %s.%s" %(
                        metadata.last_run_stopped, decision.target_effective_repeat_interval, desc.stage, desc.component
                ))
                return None

            # if producers_dirty or decision.is_dirty:
            if decision.should_exploit(metadata):
                next_repeat_interval, eri = self._exploit_desc(desc, date_now,
                                                               decision.exploit_target_factor())
            else:
                next_repeat_interval, eri = self._explore_desc(desc, date_now,
                                                               decision.explore_target_factor())

            self.log.debug("Came up with decision %s for %s.%s" % (
                next_repeat_interval, desc.stage, desc.component
            ))

            if next_repeat_interval is not None:
                decision.register_choice(metadata.last_run_stopped
                                         + datetime.timedelta(seconds=next_repeat_interval),
                                         metadata.last_run_stopped)

        return decision.next_repeat_interval()

    def suggest_next_delay(self, stage, component, date_now):
        # type: (str, str, datetime.datetime) -> Optional[float]
        with self.lock:
            try:
                desc = self.modelist.descriptors[self.experiment_name][stage][component]
            except KeyError as e:
                # VV: Haven't had the chance to digest the event message yet, this is not an error
                self.log.warning("I just got asked to Optimize an unknown Engine %s.%s" % (
                                 stage, component
                             ))
                return None

            return self.optimize_single(desc, date_now)
