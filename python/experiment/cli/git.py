# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio
import subprocess
from pathlib import Path

import typer
from rich.console import Console

from experiment.cli.exit_codes import STPExitCodes

stderr = Console(stderr=True)
stdout = Console()


def get_git_toplevel_path(path: Path):
    try:
        toplevel_path = subprocess.check_output(["git", "rev-parse", "--show-toplevel"],
                                                cwd=path.parent).decode().strip()
    except subprocess.CalledProcessError as e:
        stderr.print("Unable to retrieve top level path for the experiment via git.")
        stderr.print("Are you sure the path belongs to a git repository?")
        raise typer.Exit(code=STPExitCodes.GIT_ERROR)

    return toplevel_path


def get_git_origin_url(path: Path):
    try:
        origin_url = subprocess.check_output(["git", "remote", "get-url", "origin"],
                                             cwd=path.parent).decode().strip()
        if origin_url.endswith(".git"):
            origin_url = origin_url[:-4]
    except subprocess.CalledProcessError as e:
        stderr.print("Unable to retrieve origin url for the experiment via git.")
        stderr.print("Are you sure the experiment path belongs to a git repository?")
        raise typer.Exit(code=STPExitCodes.GIT_ERROR)

    return origin_url


def get_git_head_commit(path: Path):
    try:
        head_commit = subprocess.check_output(["git", "rev-parse", "HEAD"],
                                              cwd=path.parent).decode().strip()
    except subprocess.CalledProcessError as e:
        stderr.print("Unable to retrieve the head commit for the experiment via git.")
        stderr.print("Are you sure the experiment path belongs to a git repository?")
        raise typer.Exit(code=STPExitCodes.GIT_ERROR)

    return head_commit
