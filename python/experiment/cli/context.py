# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio
import os
import sys

import keyring
import typer

from experiment.cli.configuration import Configuration

app = typer.Typer(no_args_is_help=True)


@app.command("list")
def list_contexts(ctx: typer.Context):
    """
    Lists available contexts
    """
    config: Configuration = ctx.obj
    if len(config.contexts.entries.keys()) == 0:
        typer.echo("No contexts are available")
        typer.echo("Use stp login to create one")
        return

    typer.echo("Available contexts:")
    for context in config.contexts.entries.keys():
        typer.echo(f"- {context.lower()}")


@app.command()
def activate(ctx: typer.Context,
             name: str = typer.Argument(..., help="The context to set as default")):
    """
    Sets a context as the active/default
    """
    config: Configuration = ctx.obj
    name = name.lower()
    if config.contexts.entries.get(name) is None:
        typer.echo(f"Context {name} does not exist")
        sys.exit(os.EX_UNAVAILABLE)

    config.settings.default_context = name
    config.update()


@app.command()
def show(ctx: typer.Context):
    """
    Prints the active/default context
    """
    config: Configuration = ctx.obj
    if config.settings.verbose:
        typer.echo("The current context is:")
    typer.echo(config.settings.default_context)


@app.command()
def delete(ctx: typer.Context, name: str = typer.Argument(..., help="The context to delete")):
    """
    Delete a context and its related credentials
    """
    config: Configuration = ctx.obj
    name = name.lower()
    context = config.contexts.entries.get(name)
    if context is None:
        typer.echo(f"Context {name} does not exist")
        sys.exit(os.EX_UNAVAILABLE)

    url = context.url
    if keyring.get_password(url, name) is not None:
        keyring.delete_password(url, name)

    # Update the configuration accordingly
    del config.contexts.entries[name]
    if config.settings.default_context == name:
        config.settings.default_context = None

    config.update()
