# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio
import subprocess
import sys
from pathlib import Path

import typer


def get_git_origin_url(path: Path):
    try:
        origin_url = subprocess.check_output(["git", "remote", "get-url", "origin"],
                                             cwd=path.parent).decode().strip()
        if origin_url.endswith(".git"):
            origin_url = origin_url[:-4]
    except subprocess.CalledProcessError as e:
        typer.echo("Unable to retrieve origin url for the experiment.")
        typer.echo("Are you sure the experiment path belongs to a git repository?")
        sys.exit(e.returncode)

    return origin_url


def get_git_head_commit(path: Path):
    try:
        head_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=path.parent).decode().strip()
    except subprocess.CalledProcessError as e:
        typer.echo("Unable to retrieve the head commit for the experiment.")
        typer.echo("Are you sure the experiment path belongs to a git repository?")
        sys.exit(e.returncode)
    return head_commit
