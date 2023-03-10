# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio

import keyring
import typer
from rich.console import Console
from rich.table import Table

from experiment.cli.configuration import Configuration
from experiment.cli.exit_codes import STPExitCodes

app = typer.Typer(no_args_is_help=True)

stderr = Console(stderr=True)
stdout = Console()


@app.command("list")
def list_contexts(ctx: typer.Context):
    """
    List available contexts
    """
    config: Configuration = ctx.obj
    if len(config.contexts.entries.keys()) == 0:
        stderr.print("No contexts are available")
        stderr.print("Use stp login to create one")
        raise typer.Exit(code=STPExitCodes.CONTEXT_ERROR)

    contexts_table = Table("Available contexts")
    for context in config.contexts.entries.keys():
        contexts_table.add_row(context.lower())
    stdout.print(contexts_table)


@app.command()
def activate(ctx: typer.Context,
             name: str = typer.Argument(..., help="The context to set as default")):
    """
    Set a context as the active/default
    """
    config: Configuration = ctx.obj
    name = name.lower()
    if config.contexts.entries.get(name) is None:
        stderr.print(f"Context {name} does not exist")
        stderr.print(f"Use [blue]stp login --context-name {name} YOUR_URL[/blue] to add it")
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    config.settings.default_context = name
    config.update()


@app.command()
def show(ctx: typer.Context):
    """
    Print the active/default context
    """
    config: Configuration = ctx.obj
    if config.settings.verbose:
        stdout.print("The current context is:")
    stdout.print(config.settings.default_context)


@app.command()
def delete(ctx: typer.Context, name: str = typer.Argument(..., help="The context to delete")):
    """
    Delete a context and its related credentials
    """
    config: Configuration = ctx.obj
    name = name.lower()
    context = config.contexts.entries.get(name)
    if context is None:
        stderr.print(f"Context {name} does not exist")
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    url = context.url
    if keyring.get_password(url, name) is not None:
        if config.settings.verbose:
            stdout.print(f"Deleting credentials for context {name}")
        keyring.delete_password(url, name)

    # Update the configuration accordingly
    del config.contexts.entries[name]
    if config.settings.default_context == name:
        if config.settings.verbose:
            stdout.print(f"{name} was the default context.")
            stdout.print("Use [blue]stp context activate[/blue] to set a new one as the default.")
            stdout.print("Or use [blue]stp login[/blue] to log in to a new context")
        config.settings.default_context = None

    config.update()
