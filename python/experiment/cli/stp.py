# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio
from pathlib import Path
from typing import Optional

import typer

import experiment.cli.package as package
import experiment.cli.context as context
import experiment.cli.login as login

from experiment.cli.configuration import Configuration

#
app = typer.Typer(no_args_is_help=True)

# Add single commands from different files
app.command()(login.login)

# Add subcommands from different file
app.add_typer(context.app, name="context", help="Operations such as change and deletion of contexts")
app.add_typer(package.app, name="package", help="Operations related to packages")


@app.callback()
def common_options(ctx: typer.Context,
                   settings_file: Optional[Path] = typer.Option(None,
                                                                help="Path to the stp_settings.toml file",
                                                                envvar="STP_SETTINGS_FILE", exists=True, readable=True,
                                                                resolve_path=True),
                   contexts_file: Optional[Path] = typer.Option(None,
                                                                help="Path to the stp_contexts.toml file",
                                                                envvar="STP_CONTEXTS_FILE", exists=True, readable=True,
                                                                resolve_path=True),
                   verbose: bool = typer.Option(False, help="Use verbose output")

                   ):
    # Use the context to store the configuration
    ctx.obj = Configuration(settings_file, contexts_file, verbose)


if __name__ == "__main__":
    app()
