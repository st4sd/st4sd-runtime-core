#
# coding=UTF-8
# Copyright IBM Inc. 2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

"""Super simple wrapper to docker run"""

from __future__ import annotations

import sys

import experiment.model.executors
import experiment.runtime.backend_interfaces.localtask


class DockerTask(experiment.runtime.backend_interfaces.localtask.LocalTask):
    def __init__(
        self, executor: experiment.model.executors.DockerRun,
        stdout=sys.stdout, stderr=sys.stderr, shell: bool = False, **kwargs
    ):
        super().__init__(
            executor.commandLine, cwd=executor.workingDir, env=executor.environment,
            stdout=stdout, stderr=stderr, shell=shell, **kwargs)
