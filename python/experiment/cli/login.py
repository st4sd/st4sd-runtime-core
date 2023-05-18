# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio
import keyring
import pydantic
import requests.exceptions
import rich.prompt
import typer
from pydantic import HttpUrl, ValidationError
from rich.console import Console
from rich.prompt import Prompt

import experiment.service.db
from experiment.cli.configuration import Configuration, Context
from experiment.cli.exit_codes import STPExitCodes
from experiment.service.errors import UnauthorisedRequest

stderr = Console(stderr=True)
stdout = Console()


def login(
        ctx: typer.Context,
        url: str = typer.Argument(...,
                                  help="URL of the ST4SD instance you want to log in to",
                                  show_default=False),
        context_name: str = typer.Option(default="",
                                         help="Name you want to use for this context",
                                         show_default=False),
        access_token: str = typer.Option("",
                                         help="Token to access this context",
                                         show_default=False),
        force: bool = typer.Option(default=False,
                                   help="Force overwriting of previous login details",
                                   is_flag=True),
        set_default: bool = typer.Option(default=True,
                                         help="Set context as default",
                                         is_flag=True)
):
    """
    Log in to an ST4SD Instance and save the details for future use.

    Usage:
    stp login [--access-token <token to access context>]
    [--context-name <name to use for the context>]
    [--force] [--no-set-default] url

    If the access token is not provided, the user will be prompted for it.
    In case the context-name isn't provided, it will be set to be the same as the URL.

    Unless --no-set-default is provided, the context you're logging in to will be set as default.
    """

    #
    config: Configuration = ctx.obj

    url = url.strip("/").lower()
    if not url.startswith("http"):
        url = f"https://{url}"

    # Rewrite the URL by stripping e.g., path and query
    try:
        pydantic_url = pydantic.parse_obj_as(HttpUrl, url)
        url = f"{pydantic_url.scheme}://{pydantic_url.host}"
        if pydantic_url.port not in ["80", "443"]:
            url += f":{pydantic_url.port}"
    except ValidationError as e:
        stderr.print(f"{url} is not valid a valid URL: [red]{e.errors()[0].get('msg')}[/red]")
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    # Ask the user to provide an access token once
    if access_token == "":
        authorization_url = f"{url}/authorisation/token"
        stderr.print("You need to provide an access token to connect to this instance.")
        stderr.print(f"You can get one at {authorization_url}")

        if not stdout.is_interactive:
            stderr.print("The terminal is not interactive and we cannot proceed.")
            raise typer.Abort()

        # Give the user the possibility to open the browser
        if rich.prompt.Confirm.ask("Open the URL in the default browser?"):
            typer.launch(authorization_url)

        access_token = Prompt.ask("Provide the access token to connect to this instance",
                                  password=True)

    # If the user still hasn't provided one, quit
    if access_token == "":
        stderr.print("No access token was provided.\n"
                     "[italic]Tip: you can use the --access-token flag.[/italic]")
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    context_name = context_name.lower()
    if context_name == "":
        context_name = url

    # Check if the context already exists
    if config.contexts.entries.get(context_name) is not None:
        if config.settings.verbose:
            stdout.print(f"Information is already present for context {context_name}")

        # Check if the URL saved matches
        if config.contexts.entries.get(context_name).url != url:
            if config.settings.verbose:
                stdout.print(f"The URL provided does not match the one that is saved")
            if not force:
                stderr.print(
                    f"Context {context_name} is already saved with url {config.contexts.entries.get(context_name).url}.")
                stderr.print("If this is not an error and you want to overwrite it, use the --force flag")
                raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    # Attempt login
    try:
        experiment.service.db.ExperimentRestAPI(url, max_retries=2, secs_between_retries=1,
                                                cc_auth_token=access_token)
    except experiment.service.errors.UnauthorisedRequest:
        stderr.print("[red]Login attempt failed.[/red] Ensure the URL and the access token are correct")
        raise typer.Exit(code=STPExitCodes.UNAUTHORIZED)
    except requests.exceptions.ConnectionError:
        stderr.print(f"Ran into an error while trying to connect to {url}")
        stderr.print("Make sure it's a correct, valid, and working URL")
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)
    else:
        if config.settings.verbose:
            stdout.print(f"Successfully logged into {url}")

    if keyring.get_password(url, context_name) is not None and not force:
        stderr.print(f"A password for URL: {url} and context name {context_name} already exists.")
        stderr.print(f"You can overwrite it by using [yellow] stp login --force {url}[/yellow]")
        stderr.print(f"Or by changing the context name using [yellow]stp login --context-name YOUR_NAME {url}[/yellow]")
        raise typer.Exit(code=STPExitCodes.CONTEXT_ERROR)

    # Save login details
    keyring.set_password(url, context_name, access_token)
    if config.settings.verbose:
        stdout.print(f"Saved access token for context {context_name}")

    # Update settings and contexts
    if config.contexts.entries.get('context') is None or set_default:
        if config.settings.verbose:
            stdout.print(f"Setting context {context_name} as default")
        config.settings.default_context = context_name

    config.contexts.entries[context_name] = Context(name=context_name, url=url)
    config.update()
    stdout.print("[green]Success![/green]")
