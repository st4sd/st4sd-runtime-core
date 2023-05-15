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
import subprocess
import sys
import uuid
from typing import Optional

import experiment.appenv
import experiment.model.executors
import experiment.runtime.backend_interfaces.localtask


class DockerTask(experiment.runtime.backend_interfaces.localtask.LocalTask):
    def __init__(
        self, executor: experiment.model.executors.DockerRun,
        stdout=sys.stdout, stderr=sys.stderr, shell: bool = False,
        label: Optional[str] = None, **kwargs
    ):
        self.executor = executor
        if not label:
            self._name = f"st4sd-{uuid.uuid1()}"
        else:
            self._name = label

        self.executor.runArguments += f" --name={self._name}"

        # VV: this is initialized by experiment.runtime.backends.InitialiseBackendsForWorkflow()
        config = experiment.appenv.DockerConfiguration.defaultConf
        log = logging.getLogger('docker')
        if config and config.garbage_collect == "all" and '--rm' not in self.executor.runArguments:

            log.log(15, f"Will auto delete {self._name} because dockerConf.garbage_collect=all")
            self.executor.runArguments += " --rm "

        super().__init__(
            self.executor.commandLine, cwd=self.executor.workingDir, env=self.executor.environment,
            stdout=stdout, stderr=stderr, shell=shell, **kwargs)

        self.log = log
        self.log.log(15, "Dockerized command: %s" % self.executor.commandLine)

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

        # VV: this is initialized by experiment.runtime.backends.InitialiseBackendsForWorkflow()
        config = experiment.appenv.DockerConfiguration.defaultConf
        garbage_collect = "none"

        # VV: Garbage collect Containers based on the DockerConfiguration settings
        if "--rm" not in self.executor.runArguments:
            if config:
                garbage_collect = config.garbage_collect
            msg = f"Deleting container {self._name} because dockerConf.garbage_collect={garbage_collect}"
            if garbage_collect == "successful" and exit_code == 0:
                self.log.log(15, msg)
                subprocess.Popen(f"docker rm -f {self._name}", shell=True).wait()
            elif garbage_collect == "failed" and exit_code is not None and exit_code != 0:
                self.log.log(15, msg)
                subprocess.Popen(f"docker rm -f {self._name}", shell=True).wait()

        # VV: Wake up whoever is blocked at .wait()
        self._z_wait_event.set()

    def kill(self):
        if self._name:
            subprocess.Popen(args=f"docker stop {self._name}", shell=True).wait()

    def get_referenced_image_ids(self):
        image = self.executor.image

        if "@sha256:" in image:
            return {image: image}

        try:
            resolve = subprocess.Popen(args=f"docker inspect {image}", stdout=subprocess.PIPE, shell=True)
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
