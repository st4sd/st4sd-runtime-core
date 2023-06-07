# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio

import subprocess
from pathlib import Path

import pydantic
import typer
from rich.console import Console

from experiment.cli.exit_codes import STPExitCodes

stderr = Console(stderr=True)
stdout = Console()


def get_git_toplevel_path(path: Path):
    try:
        toplevel_path = (
            subprocess.check_output(
                ["git", "rev-parse", "--show-toplevel"], cwd=path.parent
            )
            .decode()
            .strip()
        )
    except subprocess.CalledProcessError as e:
        stderr.print(
            "[red]Error:[/red]:\tUnable to retrieve top level path for the experiment via git.\n"
            "[italic]Tip:\tAre you sure the path belongs to a git repository?[/italic]"
        )
        raise typer.Exit(code=STPExitCodes.GIT_ERROR)

    return toplevel_path


def get_git_origin_url(path: Path):
    try:
        origin_url = (
            subprocess.check_output(
                ["git", "remote", "get-url", "origin"], cwd=path.parent
            )
            .decode()
            .strip()
        )
        if origin_url.endswith(".git"):
            origin_url = origin_url[:-4]

    except subprocess.CalledProcessError as e:
        stderr.print(
            "[red]Error:[/red]:\tUnable to retrieve origin url for the experiment via git.\n"
            "[italic]Tip:\tAre you sure the path belongs to a git repository?[/italic]"
        )
        raise typer.Exit(code=STPExitCodes.GIT_ERROR)

    return origin_url


def get_alternative_git_url(original_url: str):
    # SSH URIs look like:
    # git@github.com:foo/bar
    if original_url.startswith("git@"):
        semicolon_idx = original_url.index(":")
        host = original_url[4:semicolon_idx]
        path = original_url[semicolon_idx + 1 :]
        return f"https://{host}/{path}"
    # HTTP(s) URLs look like:
    # https://github.com/foo/bar
    else:
        url: pydantic.AnyHttpUrl = pydantic.parse_obj_as(
            pydantic.AnyHttpUrl, original_url
        )
        return f"git@{url.host}:{url.path.lstrip('/')}"


def get_git_head_commit(path: Path):
    try:
        head_commit = (
            subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=path.parent)
            .decode()
            .strip()
        )
    except subprocess.CalledProcessError as e:
        stderr.print(
            "[red]Error:[/red]:\tUnable to retrieve the head commit for the experiment via git."
            "[italic]Tip:\tAre you sure the path belongs to a git repository?[/italic]"
        )
        raise typer.Exit(code=STPExitCodes.GIT_ERROR)

    return head_commit
