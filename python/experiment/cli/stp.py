# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio

# Hack to hide the logs for missing lsf and tinydb
import logging

logging.getLogger().setLevel(logging.CRITICAL)

from pathlib import Path
from typing import Optional

import typer

import experiment.cli.configuration
import experiment.cli.context as context
import experiment.cli.login as login
import experiment.cli.package as package
import experiment.cli.stack as stack
from experiment.cli.configuration import Configuration

#
app = typer.Typer(
    context_settings={"help_option_names": ["-h", "--help"]},
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
    rich_markup_mode="rich",
    help="stp simplifies interactions with the Simulation Toolkit for Scientific Discovery (ST4SD)",
)

# Add single commands from different files
app.command(
    options_metavar="[--token-file <file>] "
    "[-t | --access-token | --token <token>] "
    "[--context-name | --name <name>] "
    "[--force] "
    "[--no-set-default] "
    "[-v | --verbose]",
    no_args_is_help=True,
)(login.login)

# Add subcommands from different file
app.add_typer(
    context.app, name="context", help="Activate, list, rename, or delete contexts."
)
app.add_typer(
    package.app,
    name="package",
    help="Create, update, push, import and test Parameterised Virtual Experiment Packages (PVEPs).",
)
app.add_typer(stack.app, name="stack", help="Manage the ST4SD stack.")


@app.callback()
def common_options(
    ctx: typer.Context,
    settings_file: Optional[Path] = typer.Option(
        None,
        help=f"Path to the {experiment.cli.configuration.DEFAULT_SETTINGS_FILE_NAME} file",
        envvar="STP_SETTINGS_FILE",
        exists=True,
        readable=True,
        resolve_path=True,
        show_default=False,
    ),
    contexts_file: Optional[Path] = typer.Option(
        None,
        help=f"Path to the {experiment.cli.configuration.DEFAULT_CONTEXTS_FILE_NAME} file",
        envvar="STP_CONTEXTS_FILE",
        exists=True,
        readable=True,
        resolve_path=True,
        show_default=False,
    ),
    verbose: bool = typer.Option(False, "-v", "--verbose", help="Use verbose output"),
):
    # Use the context to store the configuration
    ctx.obj = Configuration(settings_file, contexts_file, verbose)


if __name__ == "__main__":
    app()
