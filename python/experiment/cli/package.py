# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio
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
from experiment.cli.git import get_git_origin_url, get_git_head_commit

app = typer.Typer(no_args_is_help=True)


def load_package_from_file(path: Path):
    try:
        with open(path) as f:
            pvep = yaml.safe_load(f)
    except Exception as e:
        typer.echo(e)
        sys.exit(os.EX_TEMPFAIL)
    return pvep


def write_package_to_file(pvep, path: Path):
    if path.suffix == '.json':
        to_write = json.dumps(pvep, indent=2)
    else:
        to_write = yaml.safe_dump(pvep, indent=2)

    path.write_text(to_write)


def update_commit_in_base_package_for_repo(pvep, origin_url, head_commit):
    belongs_to_repo = False
    for i in range(len(pvep['base']['packages'])):
        base_package = pvep['base']['packages'][i]
        git_source = base_package['source'].get('git')
        if origin_url in git_source.get("location").get("url"):
            belongs_to_repo = True
            if 'commit' not in git_source['location']:
                typer.echo(f"The base package {base_package['name']} does not use a pinned commit")
                sys.exit(os.EX_DATAERR)
            git_source['location']['commit'] = head_commit
            break

    if not belongs_to_repo:
        typer.echo(
            f"The repository {origin_url} is not referenced in the base packages of the input experiment")
        sys.exit(os.EX_USAGE)

    return pvep


@app.command()
def push(
        ctx: typer.Context,
        path: Path = typer.Argument(...,
                                    help="Path to the file containing the pvep",
                                    exists=True, readable=True, resolve_path=True, file_okay=True),
        use_latest_commit: bool = typer.Option(False,
                                               help="If this flag is present, an attempt will be made to update "
                                                    "the base package commit to the HEAD of the git repository. "
                                                    "Path must be pointing to a git repository.",
                                               is_flag=True),
        update_package_definition: bool = typer.Option(False,
                                                       help="If this flag is present, any changes to "
                                                            "the package definition, such as those performed "
                                                            "by --use-latest-commit, will result in "
                                                            "the package file being updated",
                                                       is_flag=True)
):
    config: Configuration = ctx.obj
    api = get_api(ctx)
    pvep = load_package_from_file(path)

    if use_latest_commit:
        origin_url = get_git_origin_url(path)
        head_commit = get_git_head_commit(path)
        pvep = update_commit_in_base_package_for_repo(pvep, origin_url, head_commit)

    try:
        result = api.api_experiment_push(pvep)
    except Exception as e:
        typer.echo(f"Failed to push experiment: {e}")
        sys.exit(os.EX_TEMPFAIL)
    else:
        if config.settings.verbose:
            typer.echo("Experiment pushed successfully")
            typer.echo(json.dumps(result, indent=2))

    if update_package_definition:
        write_package_to_file(pvep, path)


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


@app.command()
def update_definition(ctx: typer.Context,
                      path: Path = typer.Option(...,
                                                help="Path to the file containing the experiment",
                                                exists=True, readable=True, resolve_path=True, file_okay=True),
                      use_latest_commit: bool = typer.Option(False,
                                                             help="If this flag is present, an attempt will be made "
                                                                  "to update the base package commit to the HEAD "
                                                                  "of the git repository. "
                                                                  "Path must be pointing to a git repository.",
                                                             is_flag=True),
                      output: Optional[Path] = typer.Option(None, help="Path to output the file to. "
                                                                       "If not set, the changes will be done in place.",
                                                            writable=True, resolve_path=True)):
    config: Configuration = ctx.obj
    pvep = load_package_from_file(path)

    if use_latest_commit:
        origin_url = get_git_origin_url(path)
        head_commit = get_git_head_commit(path)
        pvep = update_commit_in_base_package_for_repo(pvep, origin_url, head_commit)

    if output is not None:
        write_package_to_file(pvep, output)
    else:
        write_package_to_file(pvep, path)
