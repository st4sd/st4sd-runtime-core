import json
import os
import sys
from pathlib import Path

import typer
import yaml

from experiment.cli.api import get_api
from experiment.cli.configuration import Configuration

app = typer.Typer(no_args_is_help=True)


@app.command()
def push(
        ctx: typer.Context,
        path: Path = typer.Option(...,
                                  help="Path to the file containing the experiment",
                                  prompt=True, exists=True, readable=True, resolve_path=True)
):
    config: Configuration = ctx.obj
    api = get_api(ctx)
    with open(path) as f:
        pvep = yaml.safe_load(f)

        try:
            result = api.api_experiment_push(pvep)
        except Exception as e:
            typer.echo(f"Failed to push experiment: {e}")
            sys.exit(os.EX_TEMPFAIL)
        else:
            if config.settings.verbose:
                typer.echo("Experiment pushed successfully")
                typer.echo(json.dumps(result, indent=2))
