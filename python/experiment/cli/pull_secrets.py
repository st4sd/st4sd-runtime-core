# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio
from __future__ import annotations

import pathlib
import sys
from typing import Optional, List, Dict

import pydantic
import typer
from pydantic import AnyHttpUrl
from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table

from experiment.cli.api import get_api
from experiment.cli.exit_codes import STPExitCodes

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from experiment.service.db import ExperimentRestAPI

app = typer.Typer(no_args_is_help=True)

stderr = Console(stderr=True)
stdout = Console()


def get_pull_secrets_list(api: ExperimentRestAPI) -> Dict[str, List[str]]:
    try:
        pull_secrets = api.api_image_pull_secrets_list(print_too=False)
    except Exception as e:
        stderr.print(f"[red]Error:[/red]\tFailed to retrieve image pull secrets: {e}")
        raise typer.Exit(code=STPExitCodes.IO_ERROR)
    return pull_secrets


def check_stack_has_pull_secrets_for_pvep_images(api: ExperimentRestAPI, pvep):
    pull_secrets = get_pull_secrets_list(api)
    registry_auths = set()
    for pull_secret_name in pull_secrets.keys():
        for registry in pull_secrets[pull_secret_name]:
            registry_auths.add(registry)

    container_images = (
        pvep.get("metadata", {}).get("registry", {}).get("containerImages", [])
    )
    registries = set()
    for image in container_images:
        url = image["name"]
        if not url.startswith("http"):
            url = f"https://{url}"
        pydantic_url = pydantic.parse_obj_as(AnyHttpUrl, url)
        registries.add(pydantic_url.host)

    for registry in registries:
        if registry not in registry_auths:
            stderr.print(
                f"[yellow]Warn:[/yellow]\tNo image pull secret is available for {registry}. "
                f"This will prevent the experiment from running if the images are not public.\n"
                "[italic]Tip:\tYou can add one with: "
                f"[blue]stp stack pull-secrets add --name {registry.split('.')[-2]} --registry {registry}[/blue]"
                "[/italic]\n"
                "[bold]Note:[/bold]\tIt is possible that a valid pull secret exists but was not added via ST4SD."
            )


@app.command("list", options_metavar="[--simple]")
def list_image_pull_secrets(
    ctx: typer.Context,
    simple: bool = typer.Option(
        False, help="Uses a simpler output style.", is_flag=True
    ),
):
    """
    Lists all the pull secrets that the ST4SD instance from the active context is configured to use.
    """
    api = get_api(ctx)
    pull_secrets = get_pull_secrets_list(api)

    if simple:
        for name, registry in pull_secrets.items():
            stdout.print(f"{name}\t{registry}")
        return

    pull_secrets_table = Table(
        "Name", "Supported image registries", title="Image pull secrets"
    )
    for name, registry in pull_secrets.items():
        pull_secrets_table.add_row(name, str(registry))

    stdout.print(pull_secrets_table)


@app.command(
    "add",
    options_metavar="--name <name> "
    "--registry <url> "
    "[--username <username>] "
    "[--token-file <file>] "
    "[-t | --access-token | --token <token>] "
    "[--username-not-required]",
    no_args_is_help=True,
)
def add_image_pull_secret(
    ctx: typer.Context,
    name: str = typer.Option(
        ..., help="Name to be used for the image pull secret.", show_default=False
    ),
    registry: str = typer.Option(
        ..., help="Image registry for the pull secret.", show_default=False
    ),
    username: str = typer.Option(
        "",
        help="Username to authenticate to the image registry with.",
        show_default=False,
    ),
    access_token: str = typer.Option(
        "",
        "--access-token",
        "--token",
        "-t",
        help="Token to authenticate to the image registry.",
        show_default=False,
    ),
    token_file: Optional[pathlib.Path] = typer.Option(
        None,
        "--token-file",
        help="File to read the token from. "
        "Must contain only the token in plaintext. "
        "[bold]NOTE: takes precedence over other token flags.[/bold]",
        show_default=False,
        exists=True,
        readable=True,
        resolve_path=True,
    ),
    username_not_required: bool = typer.Option(
        False,
        "--username-not-required",
        help="Flag if the image registry does not support or require usernames.",
        is_flag=True,
    ),
):
    """
    Adds an imagePullSecret to the ST4SD instance from the active context.
    """
    api = get_api(ctx)

    if username == "" and not username_not_required:
        stderr.print("You must provide a username to this command.")

        # If stdin is not a tty we are either in a pipe or in a
        # non-interactive terminal
        if not sys.stdin.isatty():
            stderr.print(
                "[red]Error:[/red]\tUnfortunately, the terminal is not interactive and we cannot proceed.\n"
                "[italic]Tip:\tYou can use the [blue]--username[/blue] flag.[/italic]"
            )
            raise typer.Abort()

        username = Prompt.ask(
            "Provide the username to authenticate to the image registry"
        )

        if username == "":
            stderr.print(
                "[red]Error:[/red]\tNo username was provided.\n"
                "[italic]Tip:\tYou can use the [blue]--username[/blue] flag.[/italic]"
            )
            raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    # Tokens read from a file take precedence over other flags
    if token_file is not None:
        access_token = token_file.read_text().strip()

    if access_token == "":
        stderr.print("You need to provide a token to this command.")

        # If stdin is not a tty we are either in a pipe or in a
        # non-interactive terminal
        if not sys.stdin.isatty():
            stderr.print(
                "[red]Error:[/red]\tUnfortunately, the terminal is not interactive and we cannot proceed.\n"
                "[italic]Tip:\tYou can use the [blue]--key[/blue] flag.[/italic]"
            )
            raise typer.Abort()

        access_token = Prompt.ask(
            "Provide the token to authenticate to the image registry",
            password=True,
        )

        if access_token == "":
            stderr.print(
                "[red]Error:[/red]\tNo token was provided.\n"
                "[italic]Tip:\tYou can use the --key flag.[/italic]"
            )
            raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    try:
        api.api_image_pull_secrets_upsert(
            secret_name=name,
            registry=registry,
            username=username,
            password=access_token,
        )
    except Exception as e:
        stderr.print(f"[red]Error:[/red]\tFailed to add image pull secret: {e}")
        raise typer.Exit(code=STPExitCodes.IO_ERROR)

    stdout.print("[green]Success![/green]")
