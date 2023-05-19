# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio
import datetime
import json
import os
import pathlib
from pathlib import Path
from typing import Optional, List

import jsonschema
import keyring
import requests
import typer
import yaml
from pydantic import HttpUrl, parse_obj_as, ValidationError
from rich.console import Console

from experiment.cli.api import get_api
from experiment.cli.configuration import Configuration
from experiment.cli.exit_codes import STPExitCodes
from experiment.cli.git import get_git_origin_url, get_git_head_commit, get_git_toplevel_path
from experiment.cli.pull_secrets import check_stack_has_pull_secrets_for_pvep_images
from experiment.model.storage import ExperimentPackage

app = typer.Typer(no_args_is_help=True)

stderr = Console(stderr=True)
stdout = Console()


def load_package_from_file(path: Path):
    try:
        with open(path) as f:
            pvep = yaml.safe_load(f)
    except Exception as e:
        stderr.print(f"Unable to load package from file: {e}")
        typer.Exit(code=STPExitCodes.IO_ERROR)
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
                stderr.print(f"The base package {base_package['name']} does not use a pinned commit")
                raise typer.Exit(code=STPExitCodes.INPUT_ERROR)
            git_source['location']['commit'] = head_commit
            break

    if not belongs_to_repo:
        stderr.print(f"The repository {origin_url} is not referenced in the base packages of the input experiment")
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    return pvep


def get_pvep_from_url(ctx: typer.Context, url: HttpUrl, from_context: Optional[str]):
    config: Configuration = ctx.obj

    # We assume the URL to be something like
    # https://host/some/path/exp-name#an-optional-anchor
    exp_name = url.path.split("/")[-1]

    # If we have from_context we can log into the API
    if from_context is not None:
        context_url = f"{url.scheme}://{url.host}"
        token = keyring.get_password(context_url, from_context)
        if token is None:
            stderr.print(f"Unable to get password for provided context {from_context}")
            stderr.print(f"Try to log in again with:"
                         f" [yellow]stp login --context-name {from_context} {context_url} --force [/yellow]")
            raise typer.Exit(STPExitCodes.UNAUTHORIZED)
        api = get_api(ctx, from_context)
        try:
            result = api.api_request_get(f"experiments/{exp_name}?outputFormat=json&hideMetadataRegistry=y&hideNone=y")
            pvep = result['entry']
        except Exception as e:
            stderr.print(f"Unable to retrieve experiment: {e}")
            raise typer.Exit(STPExitCodes.IO_ERROR)
    # With no context we assume we're accessing a publicly available registry
    else:
        try:
            exp_def_address = f"{url.scheme}://{url.host}/registry-ui/backend/experiments/{exp_name}?outputFormat=json&hideMetadataRegistry=y&hideNone=y"
            result = requests.get(exp_def_address)
            if result.status_code != 200:
                stderr.print(f"The server returned error {result.status_code}")
                stderr.print("If you're trying to retrieve a PVEP from a private registry, add the [blue]--from-context[/blue] option "
                             "to pass the name of the context you want to use to access the data.")
                stderr.print(f"Example: [blue]--from-context {config.settings.default_context}[/blue]")
                raise typer.Exit(STPExitCodes.UNAUTHORIZED)
            pvep = result.json()['entry']
        except Exception as e:
            stderr.print(f"Unable to retrieve experiment: {e}")
            raise typer.Exit(code=STPExitCodes.IO_ERROR)

    return pvep


def output_format_callback(chosen_format: str):
    supported_formats = ['json', 'yaml', 'yml']
    if chosen_format not in supported_formats:
        raise typer.BadParameter(f"Output format can only be one of {supported_formats}")
    return chosen_format


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
        tag: List[str] = typer.Option([], help="Tag that the experiment should have. Can be used more than once to "
                                               "add multiple tags. "
                                               "e.g., --tag hello --tag world"),
        update_package_definition: bool = typer.Option(False,
                                                       help="If this flag is present, any changes to "
                                                            "the package definition, such as those performed "
                                                            "by --use-latest-commit, will result in "
                                                            "the package file being updated",
                                                       is_flag=True)
):
    """
    Pushes a package to the registry of the currently active context.

    Usage:
    stp package push
    [--tag <tag to add to the pvep>]
    [--use-latest-commit] [--update-package-definition] path
    """
    config: Configuration = ctx.obj
    api = get_api(ctx)
    pvep = load_package_from_file(path)

    if use_latest_commit:
        origin_url = get_git_origin_url(path)
        head_commit = get_git_head_commit(path)
        pvep = update_commit_in_base_package_for_repo(pvep, origin_url, head_commit)

    if len(tag) != 0:
        pvep['metadata']['package']['tags'] = list(set(tag))

    try:
        result = api.api_experiment_push(pvep)
    except Exception as e:
        stderr.print(f"Failed to push experiment: {e}")
        raise typer.Exit(code=STPExitCodes.IO_ERROR)
    else:
        if config.settings.verbose:
            stdout.print("Experiment pushed successfully")
            stdout.print(json.dumps(result, indent=2))

    check_stack_has_pull_secrets_for_pvep_images(api, result)

    if update_package_definition:
        write_package_to_file(pvep, path)


@app.command("download")
def download_experiment(
        ctx: typer.Context,
        from_url: str = typer.Option(...,
                                     help="URL to the  experiment."
                                          "Must be a URL to an ST4SD Registry UI entry. "
                                          "E.g.: https://registry.st4sd.res.ibm.com/experiment/band-gap-dft-gamess-us"),
        from_context: Optional[str] = typer.Option(default=None,
                                                   help="Optional context to use for accessing the target endpoint."
                                                        "Must be set if the target registry requires authorisation"),
        to_folder: pathlib.Path = typer.Option(".",
                                               help="Folder in which to save the PVEP",
                                               exists=True,
                                               resolve_path=True,
                                               writable=True,
                                               dir_okay=True),
        output_format: str = typer.Option("json",
                                          help="Output format in which to save the PVEP."
                                               "Currently supported values: json, yaml")
):
    """
    Downloads a package from an external registry.

    Usage:
    stp package download [--from-url <url to the package in another ST4SD's Registry UI>]
    [--from-context <context information to use in case the target registry requires authorisation>]
    [--to-folder <folder in which to save the PVEP>]
    [--output-format <json or yaml>]
    """

    # AP: typer doesn't currently support HttpUrl as a type,
    # so we need to perform validation later
    try:
        url: HttpUrl = parse_obj_as(HttpUrl, from_url)
    except ValidationError as e:
        stderr.print(f"{from_url} is not valid a valid URL: [red]{e.errors()[0].get('msg')}[/red]")
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    pvep = get_pvep_from_url(ctx, url, from_context)
    pvep_file_name = pathlib.Path(pvep['metadata']['package']['name'] + f".{output_format}")
    pvep_path = to_folder / pvep_file_name
    if pvep_path.exists():
        stderr.print(f"File {pvep_path} already exists.")
        stderr.print("Please use the --to-folder option to specify a different folder")
        raise typer.Exit(code=STPExitCodes.IO_ERROR)

    write_package_to_file(pvep, pvep_path)
    stdout.print(f"Experiment saved to {pvep_path}")


@app.command("import")
def import_experiment(
        ctx: typer.Context,
        from_url: str = typer.Option(...,
                                     help="URL to the  experiment."
                                          "Must be a URL to an ST4SD Registry UI entry. "
                                          "E.g.: https://registry.st4sd.res.ibm.com/experiment/band-gap-dft-gamess-us"),
        from_context: Optional[str] = typer.Option(default=None,
                                                   help="Optional context to use for accessing the target endpoint."
                                                        "Must be set if the target registry requires authorisation")
):
    """
    Imports a package from an external registry.

    Usage:
    stp package import [--from-url <url to the package in another ST4SD's Registry UI>]
    [--from-context <context information to use in case the target registry requires authorisation>]
    """
    config: Configuration = ctx.obj

    # AP: typer doesn't currently support HttpUrl as a type,
    # so we need to perform validation later
    try:
        url: HttpUrl = parse_obj_as(HttpUrl, from_url)
    except ValidationError as e:
        stderr.print(f"{from_url} is not valid a valid URL: [red]{e.errors()[0].get('msg')}[/red]")
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    pvep = get_pvep_from_url(ctx, url, from_context)
    exp_name = pvep['metadata']['package']['name']
    api = get_api(ctx)
    try:
        result = api.api_experiment_push(pvep)
    except Exception as e:
        stderr.print(f"Failed to push experiment: {e}")
        raise typer.Exit(code=STPExitCodes.IO_ERROR)
    else:
        if config.settings.verbose:
            stdout.print("Experiment pushed successfully")
            stdout.print(json.dumps(result, indent=2))

    check_stack_has_pull_secrets_for_pvep_images(api, result)

    active_context_name = config.settings.default_context
    url = config.contexts.entries.get(active_context_name).url
    stdout.print(f"[green]Success![/green] You can find the imported PVEP at:")
    stdout.print(f"{url}/registry-ui/experiment/{exp_name}")


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
                      tag: List[str] = typer.Option([], help="Tag that the experiment should have. Can be used "
                                                             "more than once to add multiple tags. "
                                                             "e.g., --tag hello --tag world"),
                      output: Optional[Path] = typer.Option(None, help="Path to output the file to. "
                                                                       "If not set, the changes will be done in place.",
                                                            writable=True, resolve_path=True)):
    """
    Updates the definition of a local PVEP.

    Usage:
    stp package update-definition <--path path-to-package>
    [--tag <tag to set>] [--output <path-to-output-to>] [--use-latest-commit]

    If --output is not specified, the update will be done in place.
    """
    config: Configuration = ctx.obj
    pvep = load_package_from_file(path)

    if use_latest_commit:
        origin_url = get_git_origin_url(path)
        head_commit = get_git_head_commit(path)
        pvep = update_commit_in_base_package_for_repo(pvep, origin_url, head_commit)

    if len(tag) != 0:
        pvep['metadata']['package']['tags'] = list(set(tag))

    if output is not None:
        write_package_to_file(pvep, output)
    else:
        write_package_to_file(pvep, path)


@app.command("create")
def create_package(ctx: typer.Context,
                   path: Path = typer.Option(Path("."),
                                             "--from",
                                             help="Path to the experiment configuration file or a directory "
                                                  "that contains a standalone project",
                                             exists=True, readable=True, resolve_path=True),
                   manifest: Optional[Path] = typer.Option(None,
                                                           help="Path to the file containing the manifest file",
                                                           exists=True, readable=True, resolve_path=True,
                                                           file_okay=True),
                   variables_as_options: bool = typer.Option(False,
                                                             help="Set variables in the experiment as executionOptions "
                                                                  "instead of presets",
                                                             is_flag=True),
                   output_format: str = typer.Option("json",
                                                     help="Output format to save the resulting PVEP in",
                                                     callback=output_format_callback)
                   ):
    """
    Creates a PVEP given an experiment workflow definition.

    Usage:
    stp package create <--from path-to-workflow-definition>
    [--manifest path-to-workflow-manifest] [--output-format format-to-output-in]
    [--variables-as-options]
    """

    config: Configuration = ctx.obj

    # If we're given a dir, we offer support for
    # - standalone projects (conf/flowir_package.yaml)
    # - flowir.conf files
    if path.is_dir():
        standalone_flowir_path = path / Path("conf/flowir_package.yaml")
        flowir_conf_path = path / Path("flowir.conf")
        if standalone_flowir_path.is_file():
            path = standalone_flowir_path
        elif flowir_conf_path.is_file():
            path = flowir_conf_path
        else:
            stderr.print("The path provided to --from does not point to a file or to a standalone project folder")
            raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    if manifest is None:
        experiment_package: ExperimentPackage = ExperimentPackage.packageFromLocation(location=str(path))
    else:
        experiment_package: ExperimentPackage = ExperimentPackage.packageFromLocation(location=str(path))

    # Base package
    origin_url = get_git_origin_url(path)
    commit_id = get_git_head_commit(path)
    toplevel_git_path = Path(get_git_toplevel_path(path))
    name = origin_url.split("/")[-1]

    pvep = {
        "base": {
            "packages": [
                {
                    "name": name,
                    "source": {
                        "git": {
                            "location": {
                                "url": origin_url,
                                "commit": commit_id
                            }
                        }
                    },
                    "config": {
                        "path": str(path.relative_to(toplevel_git_path)),
                        "manifestPath": str(manifest.relative_to(toplevel_git_path) if manifest is not None else "")
                    }
                }
            ],
        }
    }

    # Metadata
    pvep['metadata'] = {
        "package": {
            "name": name,
            "tags": [],
            "license": "",
            "maintainer": "",
            "description": "",
            "keywords": []
        }
    }

    # Parameterisation
    raw_flowir = experiment_package.configuration.get_flowir_concrete().raw()
    variables = raw_flowir.get("variables", {})
    platforms = raw_flowir.get("platforms", [])

    # The default platform is treated differently
    platforms.remove("default")

    # Set a preset for the platform only if there are less than two
    parameterisation = {"presets": {}, "executionOptions": {}}
    if len(platforms) == 0:
        parameterisation['presets'] = {"platform": "default"}
    elif len(platforms) == 1:
        parameterisation['presets'] = {"platform": platforms[0]}
    else:
        parameterisation['executionOptions'] = {"platform": platforms}

    # If some variables from the default platform are redefined in other
    # platforms, we want to ignore them
    vars_in_platforms = []
    for platform in platforms:
        vars_in_platforms.extend(list(variables.get(platform, {}).get("global", {}).keys()))
    vars_in_platforms = set(vars_in_platforms)

    # Variables not defined in the platforms go in the executionOptions
    preset_vars = []
    option_vars = []
    default_vars = variables.get("default", {}).get("global", {})
    for var_name in default_vars.keys():
        if var_name not in vars_in_platforms:
            if variables_as_options:
                option_vars.append({"name": var_name, "value": default_vars[var_name]})
            else:
                preset_vars.append({"name": var_name, "value": default_vars[var_name]})

    parameterisation['presets']['variables'] = preset_vars
    parameterisation['executionOptions']['variables'] = option_vars
    pvep['parameterisation'] = parameterisation

    #
    output_file = Path.cwd() / Path(f"{name}.{output_format}")
    if output_file.exists():
        output_file = Path.cwd() / Path(f"{name}-autogen-{datetime.datetime.now().isoformat()}.{output_format}")
    write_package_to_file(pvep, output_file)

    if config.settings.verbose:
        stdout.print(f"PVEP saved successfully as {output_file}")


@app.command()
def test(ctx: typer.Context,
         path: Path = typer.Argument(...,
                                     help="Path to the file containing the PVEP",
                                     exists=True, readable=True, resolve_path=True, file_okay=True),
         schema_path: Optional[Path] = typer.Option(None,
                                                    help="Path to the PVEP JSON Schema",
                                                    exists=True, readable=True, resolve_path=True, file_okay=True),
         ):
    """
    Tests the syntax of a PVEP definition.

    Usage:
    stp package test [--schema-path path-to-pvep-json-schema] <path>
    """
    pvep = load_package_from_file(path)

    if schema_path is None:
        schema_path = Path(os.path.abspath(os.path.split(__file__)[0])) / Path("pvep_schema.jsonschema")
        if not schema_path.is_file():
            stderr.print("Unable to load the JSON schema file for PVEPs")
            stderr.print("Use --schema-path to supply a valid location")
            raise typer.Exit(code=STPExitCodes.IO_ERROR)

    with open(schema_path) as s:
        schema = json.load(s)

    try:
        jsonschema.validate(pvep, schema)
    except jsonschema.exceptions.ValidationError as e:
        full_path = "instance"
        for subpath in e.path:
            full_path = f"{full_path}['{subpath}']"
        stderr.print(f"Validation error in {full_path}: {e.message}")
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)
