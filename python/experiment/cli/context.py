# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio
from typing import Optional

import keyring
import typer
from pydantic import AnyHttpUrl
from rich.console import Console
from rich.table import Table

from experiment.cli.configuration import Configuration, Context, Contexts
from experiment.cli.exit_codes import STPExitCodes

app = typer.Typer(no_args_is_help=True)

stderr = Console(stderr=True)
stdout = Console()


def find_context_by_url(contexts: Contexts, url: AnyHttpUrl) -> Optional[str]:
    for context in contexts.entries.values():
        if context.url.host == url.host:
            return context.name

    return None


@app.command("list", options_metavar="[--show-url] [--simple]")
def list_contexts(
    ctx: typer.Context,
    show_url: bool = typer.Option(
        False,
        "--show-url",
        help="Print the URL for each of the contexts.",
        is_flag=True,
    ),
    simple: bool = typer.Option(
        False,
        "--simple",
        help="Use a simpler output style. Better suited for scripts.",
        is_flag=True,
    ),
):
    """
    List available contexts
    """
    config: Configuration = ctx.obj

    if not config.contexts.entries:
        stderr.print("No contexts are available.")
        stderr.print("[italic]Tip:\tUse [blue]stp login[/blue] to create one.[/italic]")
        raise typer.Exit(code=STPExitCodes.CONTEXT_ERROR)

    if simple:
        for context in config.contexts.entries.keys():
            if show_url:
                stdout.print(
                    f"{context.lower()}\t{config.contexts.entries[context].url}"
                )
            else:
                stdout.print(context.lower())
        return

    contexts_table = Table("Available contexts")
    if show_url:
        contexts_table.add_column("URL")
    for context in config.contexts.entries.keys():
        if show_url:
            contexts_table.add_row(
                context.lower(), config.contexts.entries[context].url
            )
        else:
            contexts_table.add_row(context.lower())
    stdout.print(contexts_table)


@app.command(no_args_is_help=True)
def activate(
    ctx: typer.Context,
    name: str = typer.Argument(
        ..., help="The context to set as default.", show_default=False
    ),
):
    """
    Set a context as the active/default
    """
    config: Configuration = ctx.obj

    name = name.lower()
    if config.contexts.entries.get(name) is None:
        stderr.print(
            f"[red]Error:[/red]\tContext [bold]{name}[/bold] does not exist.\n"
            f"[italic]Tip:\tList the available contexts with [blue]stp context list[/blue]\n"
            f"\tOr use [blue]stp login --context-name {name} YOUR_URL[/blue] to add it.[/italic]"
        )
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    config.settings.default_context = name
    config.update()


@app.command(options_metavar="[--show-url] [-v | --verbose]")
def show(
    ctx: typer.Context,
    show_url: bool = typer.Option(
        False, "--show-url", help="Print the URL of the context.", is_flag=True
    ),
    verbose: bool = typer.Option(
        False, "-v", "--verbose", help="Use verbose output.", is_flag=True
    ),
):
    """
    Print the active/default context
    """
    config: Configuration = ctx.obj
    if not config.settings.verbose:
        config.settings.verbose = verbose

    if config.settings.verbose:
        stdout.print("The current context is:")

    if show_url:
        stdout.print(
            f"{config.settings.default_context.lower()}\t{config.contexts.entries[config.settings.default_context].url}"
        )
    else:
        stdout.print(config.settings.default_context)


@app.command(options_metavar="--to <name>", no_args_is_help=True)
def rename(
    ctx: typer.Context,
    original_name: str = typer.Argument(
        ..., help="The context to rename.", show_default=False
    ),
    new_name: str = typer.Option(
        ..., "--to", help="The new name for the context.", show_default=False
    ),
):
    """
    Renames a context
    """
    config: Configuration = ctx.obj
    original_name = original_name.lower()
    new_name = new_name.lower()

    from_context = config.contexts.entries.get(original_name)
    if from_context is None:
        stderr.print(
            f"[red]Error:[/red]\tContext [bold]{original_name}[/bold] does not exist."
        )
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    to_context = config.contexts.entries.get(new_name)
    if to_context is not None:
        stderr.print(
            f"[red]Error:[/red]\tContext [bold]{new_name}[/bold] already exists."
            f"[italic]Tip:\tYou can delete it with [blue]stp context delete {new_name}[/blue][/italic]"
        )
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    # Ensure the credentials will be available for the new name
    url = from_context.url
    token = keyring.get_password(url, original_name)
    keyring.delete_password(url, original_name)
    keyring.set_password(url, new_name, token)

    # Update the configuration accordingly
    del config.contexts.entries[original_name]
    config.contexts.entries[new_name] = Context(name=new_name, url=url)
    if config.settings.default_context == original_name:
        config.settings.default_context = new_name

    config.update()
    stdout.print("[green]Success![/green]")


@app.command(options_metavar="[-v | --verbose]", no_args_is_help=True)
def delete(
    ctx: typer.Context,
    name: str = typer.Argument(..., help="The context to delete.", show_default=False),
    verbose: bool = typer.Option(
        False, "-v", "--verbose", help="Use verbose output.", is_flag=True
    ),
):
    """
    Delete a context and its related credentials
    """
    config: Configuration = ctx.obj
    if not config.settings.verbose:
        config.settings.verbose = verbose

    name = name.lower()

    context = config.contexts.entries.get(name)
    if context is None:
        stderr.print(f"[red]Error:[/red]\tContext [bold]{name}[/bold] does not exist.")
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    url = context.url
    if keyring.get_password(url, name) is not None:
        if config.settings.verbose:
            stdout.print(f"Deleting credentials for context {name}")
        keyring.delete_password(url, name)

    # Update the configuration accordingly
    del config.contexts.entries[name]
    if config.settings.default_context == name:
        stdout.print(
            f"[bold]NOTE:\t{name}[/bold] was the default context.\n"
            "[italic]Tip:\tUse [blue]stp context activate[/blue] to set a new one as the default.\n"
            "\tOr use [blue]stp login[/blue] to log in to a new context[/italic]"
        )
        config.settings.default_context = None

    config.update()
    stdout.print("[green]Success![/green]")
