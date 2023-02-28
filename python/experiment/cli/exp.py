import json
import os
import sys
from pathlib import Path
from typing import Optional

import keyring
import requests
import typer
import yaml
from pydantic import HttpUrl, parse_obj_as, ValidationError

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


@app.command("import")
def import_experiment(
        ctx: typer.Context,
        from_url: str = typer.Option(...,
                                     help="URL to the  experiment"),
        from_context: Optional[str] = typer.Option(default=None,
                                                   help="Optional context to use for accessing the target endpoint")
):
    config: Configuration = ctx.obj

    # AP: typer doesn't currently support HttpUrl as a type,
    # so we need to perform validation later
    try:
        url: HttpUrl = parse_obj_as(HttpUrl, from_url)
    except ValidationError as e:
        typer.echo(e)
        sys.exit(os.EX_USAGE)

    # We assume the URL to be something like
    # https://host/some/path/exp-name#an-optional-anchor
    exp_name = url.path.split("/")[-1]

    # If we have from_context we can log into the API
    if from_context is not None:
        context_url = f"{url.scheme}://{url.host}"
        token = keyring.get_password(context_url, from_context)
        if token is None:
            typer.echo(f"Unable to get password for provided context {from_context}")
            sys.exit(os.EX_UNAVAILABLE)
        api = get_api(ctx, from_context)
        try:
            result = api.api_request_get(f"experiments/{exp_name}?outputFormat=json&hideMetadataRegistry=y&hideNone=y")
            pvep = result['entry']
        except Exception as e:
            typer.echo(f"Unable to retrieve experiment: {e}")
            sys.exit(os.EX_TEMPFAIL)
    # With no context we assume we're accessing a publicly available registry
    else:
        try:
            exp_def_address = f"{url.scheme}://{url.host}/registry-ui/backend/experiments/{exp_name}?outputFormat=json&hideMetadataRegistry=y&hideNone=y"
            result = requests.get(exp_def_address)
            if result.status_code != 200:
                typer.echo(f"Unable to retrieve experiment, the server returned error {result.status_code}")
                typer.echo("If you're trying to import from a private registry, use the --from-context option")
                typer.echo("To pass the name of the context you want to use to access the data.")
                sys.exit(os.EX_NOPERM)
            pvep = result.json()['entry']
        except Exception as e:
            typer.echo(f"Unable to retrieve experiment: {e}")
            sys.exit(os.EX_TEMPFAIL)

    api = get_api(ctx)
    try:
        result = api.api_experiment_push(pvep)
    except Exception as e:
        typer.echo(f"Failed to push experiment: {e}")
        sys.exit(os.EX_TEMPFAIL)
    else:
        if config.settings.verbose:
            typer.echo("Experiment pushed successfully")
            typer.echo(json.dumps(result, indent=2))
