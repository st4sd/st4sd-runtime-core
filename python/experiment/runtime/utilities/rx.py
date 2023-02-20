#
# coding=UTF-8
# Copyright IBM Inc. 2021. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis

import logging
import threading
import traceback
from typing import Any
from typing import Callable
from typing import Optional
from typing import Dict
import reactivex.scheduler

import enum
import experiment.settings

logger = logging.getLogger('rx_utils')


def report_exceptions(cb, log=None, label=None):
    # type: (Callable[[...], Any], Optional[logging.Logger], Optional[str]) -> Any
    """Report exceptions that take place in RX observers, and raises them

    Args:
        cb: callback function to wrap
    """

    log = log or logger

    def wrapper(*args, **kwargs):
        try:
            return cb(*args, **kwargs)
        except Exception as e:
            log.critical(traceback.format_exc())

            if label:
                log.critical("Exception raised by %s is dismissed: %s" % (label, e))
            else:
                log.critical("Exception dismissed: %s" % e)
            raise e

    return wrapper


class ThreadPoolGenerator:
    _mtx = threading.Lock()
    _pools: Dict[str, reactivex.scheduler.ThreadPoolScheduler] = {}

    class Pools(enum.Enum):
        BackendK8s = "BackendK8s"
        Controller = "Controller"
        ComponentState = "ComponentState"
        Engine = "Engine"
        EngineTrigger = "EngineTrigger"
        EngineTask = "EngineTask"

    # VV: this maps the known pool identifiers to their corresponding field names in experiment.settings.Orchestrator
    # that determine their size in terms of number of worker threads.
    _poolname_to_workers_options_field = {
        Pools.BackendK8s.value: "workers_backend_k8s",
        Pools.Controller.value: "workers_controller",
        Pools.ComponentState.value: "workers_component_state",
        Pools.Engine.value: "workers_engine",
        Pools.EngineTrigger.value: "workers_engine_trigger",
        Pools.EngineTask.value: "workers_engine_task",
    }

    @classmethod
    def get_pool(cls, pool: Pools) -> reactivex.scheduler.ThreadPoolScheduler:
        """Returns the RX workerpool - if pool doesn't exist it generates first

        The method may cause the orchestrator to parse and validate Environment Variables (to determine size of pools)
        in the scenario where no method has parsed the environment variables before (not feasible if using elaunch.py
        or cexecute.py)

        Arguments:
            pool: The identifier of a pool, see the class ThreadPoolGenerator.Pools for valid identifiers

        Returns: An reactivex.scheduler.ThreadPoolScheduler object
        Raises:
            KeyError:
                If @pool is an unknown Pool identifier
            experiment.model.errors.EnhancedException:
                If method is the first caller to experiment.settings.load_settings_orchestrator() and
                the parsed environment variables are wrong. The exception will explain which environment variables
                were invalid
        """
        if pool.value not in cls._poolname_to_workers_options_field:
            raise KeyError(f"Unknown pool {pool.value}")

        # VV: After the first few calls to this method all pools will have been generated, so try an erly-exit here
        if pool.value in cls._pools:
            return cls._pools[pool.value]

        with cls._mtx:
            if pool.value not in cls._pools:
                # VV: The first call to load_settings_orchestrator() will parse and validate environment variables.
                # Subsequent calls return a reference to the last parsed Settings
                # (provided that reuse_if_existing is set to True for those calls)
                orchestrator = experiment.settings.load_settings_orchestrator(reuse_if_existing=True)

                field = cls._poolname_to_workers_options_field[pool.value]
                workers = getattr(orchestrator, field)
                cls._pools[pool.value] = reactivex.scheduler.ThreadPoolScheduler(workers)

        return cls._pools[pool.value]
