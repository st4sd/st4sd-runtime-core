# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio

import typer
from rich.console import Console

from experiment.cli import pull_secrets

app = typer.Typer(no_args_is_help=True)

stderr = Console(stderr=True)
stdout = Console()

app.add_typer(
    pull_secrets.app, name="pull-secrets", help="Manage stack-wide pull secrets"
)
