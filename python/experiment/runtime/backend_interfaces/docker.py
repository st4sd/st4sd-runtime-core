#
# coding=UTF-8
# Copyright IBM Inc. 2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

"""Super simple wrapper to docker run"""

from __future__ import annotations

import datetime
import json
import logging
import os
import subprocess
import sys
import uuid
from typing import (
    Optional,
    Dict,
    Any
)

import experiment.appenv
import experiment.model.executors
import experiment.runtime.backend_interfaces.localtask


class FailedToPull(ValueError):
    pass


class DockerTask(experiment.runtime.backend_interfaces.localtask.LocalTask):
    def __init__(
        self,
        executor: experiment.model.executors.DockerRun,
        stdout=sys.stdout, stderr=sys.stderr,
        pull_policy: str = 'Always',
        shell: bool = False,
        label: Optional[str] = None, **kwargs
    ):
        log = logging.getLogger('docker')
        self.log = log

        self.executor = executor
        if not label:
            self._name = f"st4sd-{uuid.uuid1()}"
        else:
            self._name = label

        self.executor.runArguments += f" --name={self._name}"

        valid_pull_policies = ['Always', 'Never', 'IfNotPresent']
        if pull_policy not in valid_pull_policies:
            raise ValueError(f"ImagePullPolicy {pull_policy} is not one of {valid_pull_policies}")

        self.pull_policy = pull_policy

        if pull_policy == 'Always' or pull_policy == 'IfNotPresent':
            self.may_pull_image(self.log)

        # VV: this is initialized by experiment.runtime.backends.InitialiseBackendsForWorkflow()
        config = experiment.appenv.DockerConfiguration.defaultConf
        if not config:
            raise ValueError("docker backend has not been initialized via "
                             "experiment.runtime.backends.InitialiseBackendsForWorkflow() yet")

        if config.garbage_collect == "all" and '--rm' not in self.executor.runArguments:
            log.log(15, f"Will auto delete {self._name} because dockerConf.garbage_collect=all")
            self.executor.runArguments += " --rm "

        environment = self.executor.environment
        # VV: We use the PATH twice:
        # 1. For the local device to locate the "docker"-like executable
        # 2. For the executable running in the container to locate its executable
        # Here we are just ensuring that the PATH has the information from both the local device and the container
        # We use the local-device path last to reduce the chances of shadowing executable paths in the container
        if 'PATH' not in environment:
            from_env = []
        else:
            from_env = environment['PATH'].split(':')

        from_device = os.environ.get('PATH', '').split(':')
        to_merge = [x for x in from_device if x not in from_env]

        merged = ':'.join(from_env + to_merge)

        if merged:
            if environment.get("PATH") != merged:
                log.info(f"Updating PATH from {environment.get('PATH')} to {merged}")
            environment["PATH"] = merged

        super().__init__(
            self.executor.commandLine, cwd=self.executor.workingDir, env=environment,
            stdout=stdout, stderr=stderr, shell=shell, **kwargs)

        self.log = log
        self.log.log(15, "Dockerized command: %s" % self.executor.commandLine)

    def _docker_like_cmd_stdout_toggle(self, log: Optional[logging.Logger]) -> Dict[str, Any]:
        """Utility method to enable/disable logging of the stdout for the docker-like method depending
        on the log level of a python logger"""

        # VV: If the user expects a log-level that's below info also display the stdout of the docker-like cmds
        # else, hide it by redirecting the stream to /dev/null
        if log.getEffectiveLevel() >= logging.INFO:
            return {'stdout': subprocess.DEVNULL}

        return {}


    def may_pull_image(self, log: logging.Logger):
        if_not_present = (self.pull_policy=='IfNotPresent')
        do_pull = not if_not_present

        if if_not_present:
            do_pull = self.executor.image not in self.get_referenced_image_ids()

        if not do_pull:
            return

        try:
            log.info(f"Pulling image {self.executor.image}")
            start = datetime.datetime.now()
            if self.executor.platform:
                cmd_pull = f"{self.executor.executable} pull --platform={self.executor.platform} {self.executor.image}"
            else:
                cmd_pull = f"{self.executor.executable} pull {self.executor.image}"
            pull = subprocess.Popen( cmd_pull, shell=True, stderr=subprocess.PIPE,
                                     **self._docker_like_cmd_stdout_toggle(log))
            pull_code = pull.wait()
            if pull_code != 0:
                raise FailedToPull(
                    f"Failed to pull {self.executor.image} with exit-code {pull_code}. stderr follows: "
                    f"{pull.stderr.read()}")
            dt = datetime.datetime.now() - start
            log.info(f"Finished pulling image {self.executor.image} in {dt}")
        except FailedToPull:
            raise
        except Exception as e:
            raise ValueError(f"Failed to pull {self.executor.image} due to {e}")

    def _wait_task_and_set_epoch_finished(self):
        # VV: Ensure that epoch-finished is reflected to the caller of self.wait() after they wake-up
        exit_code = None
        try:
            exit_code = subprocess.Popen.wait(self)
        except Exception as e:
            self.log.warning("Failed to wait for termination of local task %s due to %s" % (self.args, e))
        self._z_finished_date = datetime.datetime.now()

        idx = self.schedulingData.indexOfColumnWithHeader('epoch-finished')
        self.schedulingData.matrix[0][idx] = self._z_finished_date.strftime("%d%m%y-%H%M%S")

        # VV: Garbage collect Containers based on the DockerConfiguration settings
        if "--rm" not in self.executor.runArguments:
            self._may_garbage_collect(exit_code=exit_code)

        # VV: Wake up whoever is blocked at .wait()
        self._z_wait_event.set()

    def kill(self):
        if self._name and self.returncode is None:
            subprocess.Popen(args=f"{self.executor.executable} stop {self._name}", shell=True,
                             **self._docker_like_cmd_stdout_toggle(self.log)).wait()
            self._may_garbage_collect(1, force=True)

    def _may_garbage_collect(self, exit_code: Optional[int], force: bool = False):
        # VV: this is initialized by experiment.runtime.backends.InitialiseBackendsForWorkflow()
        config = experiment.appenv.DockerConfiguration.defaultConf

        garbage_collect = "none"
        if config:
            garbage_collect = config.garbage_collect
        msg = f"Deleting container {self._name} because dockerConf.garbage_collect={garbage_collect} and force={force}"

        if (
            force or
            garbage_collect == "all" or
            (garbage_collect == "successful" and exit_code == 0) or
            (garbage_collect == "failed" and exit_code != 0)
        ):
            self.log.log(15, msg)
            subprocess.Popen(f"{self.executor.executable} rm -f {self._name}", shell=True,
                             **self._docker_like_cmd_stdout_toggle(self.log)).wait()


    def get_referenced_image_ids(self):
        image = self.executor.image

        if "@sha256:" in image:
            return {image: image}

        try:
            resolve = subprocess.Popen(args=f"{self.executor.executable} inspect {image}",
                                       stdout=subprocess.PIPE, shell=True)
            exit_code = resolve.wait()

            if exit_code == 0:
                docker_inspect = json.load(resolve.stdout)
                return {image:  docker_inspect[0]["RepoDigests"][0]}
        except Exception as e:
            self.log.info(f"Could not resolve image {image} because of {e} - ignoring exception")

        return {}

    @property
    def schedulerId(self):
        """Returns the ID of the Container"""
        return self._name
