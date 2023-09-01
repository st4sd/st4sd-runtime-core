# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio

import pathlib
import sys
from typing import Optional

import keyring
import pydantic
import requests.exceptions
import rich.prompt
import typer
from pydantic import HttpUrl, ValidationError
from rich.console import Console
from rich.prompt import Prompt

from experiment.cli.configuration import Configuration, Context
from experiment.cli.exit_codes import STPExitCodes

stderr = Console(stderr=True)
stdout = Console()


def login(
    ctx: typer.Context,
    url: str = typer.Argument(
        ...,
        help="URL of the ST4SD instance you want to log in to",
        show_default=False,
    ),
    context_name: str = typer.Option(
        "",
        "--context-name",
        "--name",
        help="Name you want to use for this context",
        show_default=False,
    ),
    access_token: str = typer.Option(
        "",
        "--access-token",
        "--token",
        "-t",
        help="Token to access this context",
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
    force: bool = typer.Option(
        default=False, help="Force overwriting of previous login details", is_flag=True
    ),
    set_default: bool = typer.Option(
        default=True, help="Set context as default", is_flag=True
    ),
    verbose: bool = typer.Option(
        False, "-v", "--verbose", help="Use verbose output.", is_flag=True
    ),
):
    """
    Log in to an ST4SD Instance and save the details for future use.

    - If one of --token-file, --access-token, --token, or -t is not provided, the user will be prompted for it.
    - If one of --context-name or --name is not provided, it will be set to be the same as the URL.

    [bold]Unless --no-set-default is provided, the context you are logging in to will be set as default.[/bold]
    """

    #
    config: Configuration = ctx.obj
    if not config.settings.verbose:
        config.settings.verbose = verbose

    # The validation will fail if the url does not start with a correct schema
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
        stderr.print(
            f"[red]Error:[/red]\t{url} is not valid a valid URL.\n"
            f"\t{e.errors()[0].get('msg')}"
        )
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    # Tokens read from a file take precedence over other flags
    if token_file is not None:
        access_token = token_file.read_text().strip()

    # Ask the user to provide an access token once
    if access_token == "":
        authorization_url = f"{url}/authorisation/token"
        stderr.print("You need to provide an access token to connect to this instance.")
        stderr.print(f"You can get one at {authorization_url}")

        # If stdin is not a tty we are either in a pipe or in a
        # non-interactive terminal
        if not sys.stdin.isatty():
            stderr.print(
                "[red]Error:[/red]\tUnfortunately, the terminal is not interactive and we cannot proceed."
            )
            raise typer.Abort()

        # Give the user the possibility to open the browser
        if rich.prompt.Confirm.ask("Open the URL in the default browser?"):
            typer.launch(authorization_url)

        access_token = Prompt.ask(
            "Provide the access token to connect to this instance", password=True
        )

    # If the user still hasn't provided one, quit
    if access_token == "":
        stderr.print(
            "[red]Error:[/red]\tNo access token was provided.\n"
            "[italic]Tip:\tYou can pass one from a file using the [blue]--token-file[/blue] flag\n"
            "\tOr from the terminal with the [blue]--access-token[/blue] flag.[/italic]"
        )
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    context_name = context_name.lower()
    if context_name == "":
        context_name = url

    # Check if the context already exists
    if config.contexts.entries.get(context_name) is not None:
        if config.settings.verbose:
            stdout.print(
                f"[yellow]Warn:[/yellow]"
                f"\tInformation is already present for context [bold]{context_name}[/bold]"
            )

        # Check if the URL saved matches
        if config.contexts.entries.get(context_name).url != url:
            if config.settings.verbose:
                stdout.print(
                    "[yellow]Warn:[/yellow]\tThe URL provided does not match the one that is saved"
                )
            if not force:
                stderr.print(
                    f"[red]Error:[/red]\tContext {context_name} is already saved "
                    f"with url {config.contexts.entries.get(context_name).url}\n"
                    "[italic]"
                    "Tip:\tIf this is not an error and you want to overwrite it, use the [blue]--force[/blue] flag"
                    "[/italic]"
                )
                raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    # Attempt login
    import experiment.service.db
    from experiment.service.errors import UnauthorisedRequest
    try:
        experiment.service.db.ExperimentRestAPI(
            url, max_retries=2, secs_between_retries=1, cc_auth_token=access_token
        )
    except experiment.service.errors.UnauthorisedRequest:
        stderr.print(
            "[red]Error:[/red]\tLogin attempt failed.\n"
            "\tEnsure the URL and the access token are correct."
        )
        raise typer.Exit(code=STPExitCodes.UNAUTHORIZED)
    except requests.exceptions.ConnectionError:
        stderr.print(
            f"[red]Error:[/red]\tRan into an error while trying to connect to {url}\n"
        )
        stderr.print("\tMake sure it's a correct, valid, and working URL.")
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)
    else:
        if config.settings.verbose:
            stdout.print(f"[green]Successfully logged into {url}[/green]")

    if keyring.get_password(url, context_name) is not None and not force:
        stderr.print(
            f"[red]Error:[/red]\tA password for context [bold]{context_name}[/bold] and URL {url} already exists.\n"
            "[italic]Tip:\tYou can overwrite it by using "
            f"[blue]stp login --force --context-name {context_name} {url}[/blue]\n"
            "\tOr by changing the context name using "
            f"[blue]stp login --context-name A_DIFFERENT_NAME {url}[/blue][/italic]"
        )
        raise typer.Exit(code=STPExitCodes.CONTEXT_ERROR)

    # Save login details
    keyring.set_password(url, context_name, access_token)
    if config.settings.verbose:
        stdout.print(f"Saved access token for context {context_name}")

    # Update settings and contexts
    if config.contexts.entries.get("context") is None or set_default:
        if config.settings.verbose:
            stdout.print(f"Setting context {context_name} as default")
        config.settings.default_context = context_name

    config.contexts.entries[context_name] = Context(name=context_name, url=url)
    config.update()
    stdout.print("[green]Success![/green]")
