# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio
import os
import sys

import keyring
import typer

import experiment.service.db
from experiment.cli.configuration import Configuration, Context


def login(
        ctx: typer.Context,
        url: str = typer.Argument(...,
                                  help="URL of the ST4SD instance you want to log in to",
                                  show_default=False),
        context_name: str = typer.Option(default="",
                                         help="Name you want to use for this context",
                                         show_default=False),
        access_token: str = typer.Option(...,
                                         help="Token to access this context",
                                         prompt="Enter a valid access token for this instance",
                                         hide_input=True,
                                         show_default=False),
        force: bool = typer.Option(default=False,
                                   help="Force overwriting of previous login details",
                                   is_flag=True),
        set_default: bool = typer.Option(default=True,
                                         help="Set context as default",
                                         is_flag=True)
):
    """
    Logs in to an ST4SD Instance and saves the details for future use.

    Usage: stp login --access_token <token to access context> [--force] [--set-default] URL

    In case the context-name isn't provided, it will be set to be the same as the URL.
    """

    #
    config: Configuration = ctx.obj

    url = url.strip("/").lower()
    if not url.startswith("http"):
        url = f"https://{url}"

    context_name = context_name.lower()
    if context_name == "":
        context_name = url

    # Check if the context already exists
    if config.contexts.entries.get(context_name) is not None:
        if config.settings.verbose:
            typer.echo(f"Information is already present for context {context_name}")

        # Check if the URL saved matches
        if config.contexts.entries.get(context_name).url != url:
            if config.settings.verbose:
                typer.echo(f"The URL provided does not match the one that is saved")
            if not force:
                typer.echo(
                    f"Context {context_name} is already saved with url {config.contexts.entries.get(context_name).url}.")
                typer.echo("If this is not an error and you want to overwrite it, use the --force flag")
                sys.exit(os.EX_USAGE)

    # Attempt login
    try:
        experiment.service.db.ExperimentRestAPI(url, max_retries=2, secs_between_retries=1,
                                                cc_auth_token=access_token)
    except experiment.service.errors.UnauthorisedRequest as e:
        typer.echo(f"Login attempt failed. Ensure the URL and the access token are correct")
        sys.exit(os.EX_NOPERM)
    else:
        if config.settings.verbose:
            typer.echo(f"Successfully logged into {url}")

    if keyring.get_password(url, context_name) is not None and not force:
        typer.echo(f"A password for URL: {url} and context name {context_name} already exists.")
        typer.echo("You can overwrite it by using --force or by changing the context name using --context-name")
        sys.exit(os.EX_USAGE)

    # Save login details
    keyring.set_password(url, context_name, access_token)
    if config.settings.verbose:
        typer.echo(f"Saved access token for context {context_name}")

    # Update settings and contexts
    if config.contexts.entries.get('context') is None or set_default:
        if config.settings.verbose:
            typer.echo("Setting context as default")
        config.settings.default_context = context_name

    config.contexts.entries[context_name] = Context(name=context_name, url=url)
    config.update()
