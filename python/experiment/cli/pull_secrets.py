# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio
import pydantic
import typer
from pydantic import AnyHttpUrl
from rich.console import Console
from rich.table import Table

from experiment.cli.api import get_api
from experiment.cli.exit_codes import STPExitCodes
from experiment.service.db import ExperimentRestAPI

app = typer.Typer(no_args_is_help=True)

stderr = Console(stderr=True)
stdout = Console()


def check_stack_has_pull_secrets_for_pvep_images(api: ExperimentRestAPI, pvep):
    try:
        pull_secrets = api.api_image_pull_secrets_list(print_too=False)
    except Exception as e:
        stderr.print(f"Failed to retrieve image pull secrets: {e}")
        raise typer.Exit(code=STPExitCodes.IO_ERROR)

    registry_auths = set()
    for pull_secret_name in pull_secrets.keys():
        for registry in pull_secrets[pull_secret_name]:
            registry_auths.add(registry)

    container_images = pvep.get("metadata", {}).get("registry", {}).get("containerImages", [])
    registries = set()
    for image in container_images:
        url = image['name']
        if not url.startswith("http"):
            url = f"https://{url}"
        pydantic_url = pydantic.parse_obj_as(AnyHttpUrl, url)
        registries.add(pydantic_url.host)

    for registry in registries:
        if registry not in registry_auths:
            stderr.print(f"[yellow]Warning[/yellow]: no image pull secret is available for {registry}. "
                         f"This will prevent the experiment from running if the images are not public.")
            stderr.print("[italic]Tip: you can add one with: "
                         f"[blue]stp stack pull-secrets add --name {registry.split('.')[-2]} --registry {registry}[/blue][/italic]\n")


@app.command("list")
def list_image_pull_secrets(
        ctx: typer.Context,
        simple: bool = typer.Option(False, help="Uses a simpler, less fancy output style", is_flag=True)
):
    """
    Lists all the pull secrets that the ST4SD instance from the active context is configured to use.

    Usage:
    stp stack pull-secret list [--simple]
    """
    api = get_api(ctx)
    try:
        pull_secrets = api.api_image_pull_secrets_list(print_too=False)
    except Exception as e:
        stderr.print(f"Failed to retrieve image pull secrets: {e}")
        raise typer.Exit(code=STPExitCodes.IO_ERROR)

    if simple:
        for name, registry in pull_secrets.items():
            stdout.print(f"{name}\t{registry}")
        return

    pull_secrets_table = Table("Name", "Supported image registries", title="Image pull secrets")
    for name, registry in pull_secrets.items():
        pull_secrets_table.add_row(name, str(registry))

    stdout.print(pull_secrets_table)


@app.command("add")
def add_image_pull_secret(
        ctx: typer.Context,
        name: str = typer.Option(..., help="Name to be used for the image pull secret"),
        registry: str = typer.Option(..., help="Image registry for the pull secret"),
        username: str = typer.Option(..., help="Username to authenticate to the image registry with",
                                     prompt="Insert the username to authenticate to the image registry with"),
        key: str = typer.Option(..., help="Password or API key for the image registry",
                                prompt="Insert password/API key to access the registry", hide_input=True)
):
    """
    Adds an imagePullSecret to the ST4SD instance from the active context.

    Usage: stp stack pull-secrets add --name <name> --registry <registry-url>
    --username <username> --key <password/apikey>
    """
    api = get_api(ctx)
    try:
        api.api_image_pull_secrets_upsert(secret_name=name, registry=registry, username=username, password=key)
    except Exception as e:
        stderr.print(f"Failed to add image pull secret: {e}")
        raise typer.Exit(code=STPExitCodes.IO_ERROR)

    stdout.print("[green]Success![/green]")
