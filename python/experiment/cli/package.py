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
from experiment.cli.context import find_context_by_url
from experiment.cli.exit_codes import STPExitCodes
from experiment.cli.git import (
    get_git_origin_url,
    get_git_head_commit,
    get_git_toplevel_path,
    get_alternative_git_url,
)
from experiment.cli.pull_secrets import check_stack_has_pull_secrets_for_pvep_images

app = typer.Typer(no_args_is_help=True)

stderr = Console(stderr=True)
stdout = Console()


def load_package_from_file(path: Path):
    try:
        with open(path) as f:
            pvep = yaml.safe_load(f)
    except Exception as e:
        stderr.print(f"[red]Error:[/red]\tUnable to load package from file: {e}")
        typer.Exit(code=STPExitCodes.IO_ERROR)
    return pvep


def write_package_to_file(pvep, path: Path):
    if path.suffix == ".json":
        to_write = json.dumps(pvep, indent=2)
    else:
        to_write = yaml.safe_dump(pvep, indent=2)

    path.write_text(to_write)


def update_commit_in_base_package_for_repo(pvep, origin_url, head_commit):
    belongs_to_repo = False
    alternative_url = get_alternative_git_url(origin_url)
    for i in range(len(pvep["base"]["packages"])):
        base_package = pvep["base"]["packages"][i]
        git_source = base_package["source"].get("git")
        pkg_git_url = git_source.get("location").get("url")
        if origin_url in pkg_git_url or alternative_url in pkg_git_url:
            belongs_to_repo = True
            if "commit" not in git_source["location"]:
                stderr.print(
                    f"[red]Error:[/red]"
                    f"\tThe base package {base_package['name']} does not use a pinned commit"
                )
                raise typer.Exit(code=STPExitCodes.INPUT_ERROR)
            git_source["location"]["commit"] = head_commit
            break

    if not belongs_to_repo:
        stderr.print(
            f"[red]Error:[/red]"
            f"\tThe repository {origin_url} is not referenced in the base packages of the input experiment.\n"
            f"[italic]Tip:\tRun this command from the git repository you want to update the commit for.[/italic]"
        )
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    return pvep


def get_pvep_from_url(ctx: typer.Context, url: HttpUrl, from_context: Optional[str]):
    config: Configuration = ctx.obj
    pvep = None

    # We assume the URL to be something like
    # https://host/some/path/exp-name#an-optional-anchor
    exp_name = url.path.split("/")[-1]

    # With no context we assume we're accessing a publicly available registry
    if from_context is None:
        try:
            exp_def_address = f"{url.scheme}://{url.host}/registry-ui/backend/experiments/{exp_name}?outputFormat=json&hideMetadataRegistry=y&hideNone=y"
            result = requests.get(exp_def_address)
            if result.status_code == 200:
                pvep = result.json()["entry"]
            else:
                context_to_use = find_context_by_url(config.contexts, url)
                if context_to_use is not None:
                    stderr.print(
                        "[yellow]Warn:[/yellow]\tIt appears the package you are trying to download belongs to "
                        "a private registry but the [blue]--from-context[/blue] flag was not provided.\n"
                        f"\tBased on the URL, we will attempt to use the [blue]{context_to_use}[/blue] context "
                        f"for authentication.\n"
                        f"[italic]Tip:\tNext time include [blue]--from-context {context_to_use}[/blue][/italic]"
                    )
                    from_context = context_to_use
                else:
                    stderr.print(
                        f"[red]Error:[/red]\tThe server returned error {result.status_code}\n"
                        "[italic]Tip:\tIf you're trying to retrieve a PVEP from a private registry, "
                        "add the [blue]--from-context[/blue] flag and pass the name of the context "
                        "you want to use to access the data.\n"
                        "Example:\t[blue]--from-context YOUR_CONTEXT_NAME[/blue][/italic]"
                    )
                    raise typer.Exit(STPExitCodes.UNAUTHORIZED)
        except Exception as e:
            stderr.print(f"[red]Error:[/red]\tUnable to retrieve experiment: {e}")
            raise typer.Exit(code=STPExitCodes.IO_ERROR)

    #
    if from_context is not None and pvep is None:
        context_url = f"{url.scheme}://{url.host}"
        token = keyring.get_password(context_url, from_context)
        if token is None:
            stderr.print(
                f"[red]Error:[/red]\tUnable to get password for provided context [bold]{from_context}[/bold].\n"
                "[italic]Tip:\tYou can log in again with: "
                f"[blue]stp login --force --context-name {from_context} {context_url}[/blue]"
                "[/italic]"
            )
            raise typer.Exit(STPExitCodes.UNAUTHORIZED)
        api = get_api(ctx, from_context)
        try:
            result = api.api_request_get(
                f"experiments/{exp_name}?outputFormat=json&hideMetadataRegistry=y&hideNone=y"
            )
            pvep = result["entry"]
        except Exception as e:
            stderr.print(f"[red]Error:[/red]\tUnable to retrieve experiment: {e}")
            raise typer.Exit(STPExitCodes.IO_ERROR)

    return pvep


def output_format_callback(chosen_format: str):
    supported_formats = ["json", "yaml", "yml"]
    if chosen_format not in supported_formats:
        raise typer.BadParameter(
            f"Output format can only be one of {supported_formats}"
        )
    return chosen_format


@app.command(
    options_metavar="[--tag <name>] "
    "[--update-package-definition] "
    "[--use-latest-commit] "
    "[-v | --verbose]",
    no_args_is_help=True,
)
def push(
    ctx: typer.Context,
    path: Path = typer.Argument(
        ...,
        help="Path to the file containing the pvep.",
        exists=True,
        readable=True,
        resolve_path=True,
        file_okay=True,
        show_default=False,
    ),
    tag: List[str] = typer.Option(
        [],
        help="Tag that the experiment should have. "
        "Can be used more than once to add multiple tags. "
        "e.g., --tag hello --tag world",
    ),
    use_latest_commit: bool = typer.Option(
        False,
        "--use-latest-commit",
        help="If this flag is present, an attempt will be made to update the base package commit "
        "to the HEAD of the git repository. "
        "Path must be pointing to a git repository.",
        is_flag=True,
    ),
    update_package_definition: bool = typer.Option(
        False,
        "--update-package-definition",
        help="If this flag is present, any changes to the package definition, such as "
        "those performed by --use-latest-commit, will result in the package file being updated.",
        is_flag=True,
    ),
    verbose: bool = typer.Option(
        False, "-v", "--verbose", help="Use verbose output.", is_flag=True
    ),
):
    """
    Pushes a package to the registry of the currently active context.
    """
    config: Configuration = ctx.obj
    if not config.settings.verbose:
        config.settings.verbose = verbose

    api = get_api(ctx)
    pvep = load_package_from_file(path)

    if use_latest_commit:
        origin_url = get_git_origin_url(path)
        head_commit = get_git_head_commit(path)
        pvep = update_commit_in_base_package_for_repo(pvep, origin_url, head_commit)

    if len(tag) != 0:
        pvep["metadata"]["package"]["tags"] = list(set(tag))

    try:
        result = api.api_experiment_push(pvep)
    except Exception as e:
        stderr.print(f"[red]Error:[/red]\tFailed to push experiment: {e}")
        raise typer.Exit(code=STPExitCodes.IO_ERROR)
    else:
        stdout.print("[green]Experiment pushed successfully![/green]")
        if config.settings.verbose:
            stdout.print(json.dumps(result, indent=2))

    check_stack_has_pull_secrets_for_pvep_images(api, result)

    if update_package_definition:
        write_package_to_file(pvep, path)

    active_context_name = config.settings.default_context
    url = config.contexts.entries.get(active_context_name).url
    exp_name = pvep["metadata"]["package"]["name"]
    stdout.print(f"You can find the PVEP at:")
    stdout.print(f"{url}/registry-ui/experiment/{exp_name}")


@app.command(
    "download",
    options_metavar="[--from-context <name>] "
    "[--to-folder <path>] "
    "[--output-format <json|yaml>]",
    no_args_is_help=True,
)
def download_experiment(
    ctx: typer.Context,
    from_url: str = typer.Argument(
        ...,
        help="URL to the  experiment. "
        "Must be a URL to an ST4SD Registry UI entry. "
        "E.g.: https://registry.st4sd.res.ibm.com/experiment/band-gap-dft-gamess-us",
        show_default=False,
    ),
    from_context: Optional[str] = typer.Option(
        None,
        help="Optional context to use for accessing the target endpoint. "
        "Must be set if the target registry requires authorisation.",
        show_default=False,
    ),
    to_folder: pathlib.Path = typer.Option(
        ".",
        help="Folder in which to save the PVEP.",
        exists=True,
        resolve_path=True,
        writable=True,
        dir_okay=True,
    ),
    output_format: str = typer.Option(
        "json",
        "--output-format",
        help="Output format in which to save the PVEP. Currently supported values: json, yaml",
        callback=output_format_callback,
    ),
):
    """
    Downloads a package from an external registry.
    """

    # AP: typer doesn't currently support HttpUrl as a type,
    # so we need to perform validation later
    try:
        url: HttpUrl = parse_obj_as(HttpUrl, from_url)
    except ValidationError as e:
        stderr.print(
            f"[red]Error:[/red]\t{from_url} is not valid a valid URL.\n"
            f"\t{e.errors()[0].get('msg')}"
        )
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    pvep = get_pvep_from_url(ctx, url, from_context)
    pvep_file_name = pathlib.Path(
        pvep["metadata"]["package"]["name"] + f".{output_format}"
    )
    pvep_path = to_folder / pvep_file_name
    if pvep_path.exists():
        stderr.print(
            f"[red]Error:[/red]\tFile {pvep_path} already exists.\n"
            "[italic]Tip:\tYou can use the [blue]--to-folder[/blue] option to specify a different folder.[/italic]"
        )
        raise typer.Exit(code=STPExitCodes.IO_ERROR)

    write_package_to_file(pvep, pvep_path)
    stdout.print(f"Experiment saved to {pvep_path}")


@app.command(
    "import",
    options_metavar="[--from-context <name>] [-v | --verbose]",
    no_args_is_help=True,
)
def import_experiment(
    ctx: typer.Context,
    from_url: str = typer.Argument(
        ...,
        help="URL to the  experiment."
        "Must be a URL to an ST4SD Registry UI entry. "
        "E.g.: https://registry.st4sd.res.ibm.com/experiment/band-gap-dft-gamess-us",
        show_default=False,
    ),
    from_context: Optional[str] = typer.Option(
        default=None,
        help="Optional context to use for accessing the target endpoint. "
        "Must be set if the target registry requires authorisation",
        show_default=False,
    ),
    verbose: bool = typer.Option(
        False, "-v", "--verbose", help="Use verbose output.", is_flag=True
    ),
):
    """
    Imports a package from an external registry.
    """
    config: Configuration = ctx.obj
    if not config.settings.verbose:
        config.settings.verbose = verbose

    # AP: typer doesn't currently support HttpUrl as a type,
    # so we need to perform validation later
    try:
        url: HttpUrl = parse_obj_as(HttpUrl, from_url)
    except ValidationError as e:
        stderr.print(
            f"[red]Error:[/red]\t{from_url} is not valid a valid URL.\n"
            f"\t{e.errors()[0].get('msg')}"
        )
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    pvep = get_pvep_from_url(ctx, url, from_context)
    exp_name = pvep["metadata"]["package"]["name"]
    api = get_api(ctx)
    try:
        result = api.api_experiment_push(pvep)
    except Exception as e:
        stderr.print(f"[red]Error:[/red]\tFailed to push experiment: {e}")
        raise typer.Exit(code=STPExitCodes.IO_ERROR)
    else:
        stdout.print("[green]Experiment pushed successfully")
        if config.settings.verbose:
            stdout.print(json.dumps(result, indent=2))

    check_stack_has_pull_secrets_for_pvep_images(api, result)

    active_context_name = config.settings.default_context
    url = config.contexts.entries.get(active_context_name).url
    stdout.print(f"You can find the imported PVEP at:")
    stdout.print(f"{url}/registry-ui/experiment/{exp_name}")


@app.command(
    options_metavar="--path <path> "
    "[--tag <name>] "
    "[-o | --output <path>]"
    "[--use-latest-commit]",
    no_args_is_help=True,
)
def update_definition(
    ctx: typer.Context,
    path: Path = typer.Option(
        ...,
        help="Path to the file containing the experiment",
        exists=True,
        readable=True,
        resolve_path=True,
        file_okay=True,
        show_default=False,
    ),
    use_latest_commit: bool = typer.Option(
        False,
        "--use-latest-commit",
        help="If this flag is present, an attempt will be made to update the base package commit "
        "to the HEAD of the git repository. "
        "Path must be pointing to a git repository.",
        is_flag=True,
    ),
    tag: List[str] = typer.Option(
        [],
        help="Tag that the experiment should have. Can be used more than once to add multiple tags. "
        "E.g., --tag hello --tag world",
    ),
    output: Optional[Path] = typer.Option(
        None,
        "-o",
        "--output",
        help="Path to output the file to. [bold]If not set, the changes will be done in place.[/bold]",
        writable=True,
        resolve_path=True,
        show_default=False,
    ),
):
    """
    Updates the definition of a local PVEP.

    If --output is not specified, the update will be done in place.
    """
    config: Configuration = ctx.obj
    pvep = load_package_from_file(path)

    if use_latest_commit:
        origin_url = get_git_origin_url(path)
        head_commit = get_git_head_commit(path)
        pvep = update_commit_in_base_package_for_repo(pvep, origin_url, head_commit)

    if len(tag) != 0:
        pvep["metadata"]["package"]["tags"] = list(set(tag))

    if output is not None:
        write_package_to_file(pvep, output)
    else:
        write_package_to_file(pvep, path)


@app.command(
    "create",
    options_metavar="[--from <path>] "
    "[--manifest <path>] "
    "[--output-format <json|yaml>] "
    "[--variables-as-options] "
    "[-v | --verbose]",
)
def create_package(
    ctx: typer.Context,
    path: Path = typer.Option(
        Path("."),
        "--from",
        help="Path to the experiment configuration file or a directory that contains a standalone project",
        exists=True,
        readable=True,
        resolve_path=True,
    ),
    manifest: Optional[Path] = typer.Option(
        None,
        help="Path to the file containing the manifest file",
        exists=True,
        readable=True,
        resolve_path=True,
        file_okay=True,
        show_default=False,
    ),
    variables_as_options: bool = typer.Option(
        False,
        help="Set variables in the experiment as executionOptions instead of presets",
        is_flag=True,
    ),
    output_format: str = typer.Option(
        "json",
        help="Output format to save the resulting PVEP in.",
        callback=output_format_callback,
    ),
    verbose: bool = typer.Option(
        False, "-v", "--verbose", help="Use verbose output.", is_flag=True
    ),
):
    """
    Creates a PVEP given an experiment workflow definition.
    """

    config: Configuration = ctx.obj
    if not config.settings.verbose:
        config.settings.verbose = verbose

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
            stderr.print(
                "[red]Error:[/red]"
                "\tThe path provided to [blue]--from[/blue] does not point to a file or to a standalone project folder"
            )
            raise typer.Exit(code=STPExitCodes.INPUT_ERROR)

    from experiment.model.storage import ExperimentPackage
    if manifest is None:
        experiment_package: ExperimentPackage = ExperimentPackage.packageFromLocation(
            location=str(path)
        )
    else:
        experiment_package: ExperimentPackage = ExperimentPackage.packageFromLocation(
            location=str(path)
        )

    # Base package
    origin_url = get_git_origin_url(path)
    # We only support https URLs for cloning
    if origin_url.startswith("git@"):
        origin_url = get_alternative_git_url(origin_url)
        stderr.print(
            "[yellow]Warn:[/yellow]\t"
            "ST4SD does not support SSH URLs for cloning packages.\n"
            f"\tThe clone URL for the PVEP has been converted to: {origin_url}"
        )
    commit_id = get_git_head_commit(path)
    toplevel_git_path = Path(get_git_toplevel_path(path))
    name = origin_url.split("/")[-1]

    pvep = {
        "base": {
            "packages": [
                {
                    "name": name,
                    "source": {
                        "git": {"location": {"url": origin_url, "commit": commit_id}}
                    },
                    "config": {
                        "path": str(path.relative_to(toplevel_git_path)),
                        "manifestPath": str(
                            manifest.relative_to(toplevel_git_path)
                            if manifest is not None
                            else ""
                        ),
                    },
                }
            ],
        }
    }

    # Metadata
    pvep["metadata"] = {
        "package": {
            "name": name,
            "tags": [],
            "license": "",
            "maintainer": "",
            "description": "",
            "keywords": [],
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
        parameterisation["presets"] = {"platform": "default"}
    elif len(platforms) == 1:
        parameterisation["presets"] = {"platform": platforms[0]}
    else:
        parameterisation["executionOptions"] = {"platform": platforms}

    # If some variables from the default platform are redefined in other
    # platforms, we want to ignore them
    vars_in_platforms = []
    for platform in platforms:
        vars_in_platforms.extend(
            list(variables.get(platform, {}).get("global", {}).keys())
        )
    vars_in_platforms = set(vars_in_platforms)

    # Variables not defined in the platforms go in the executionOptions
    preset_vars = []
    option_vars = []
    default_vars = variables.get("default", {}).get("global", {})
    for var_name in default_vars.keys():
        if var_name not in vars_in_platforms:
            if variables_as_options:
                option_vars.append(
                    {"name": var_name, "value": str(default_vars[var_name])}
                )
            else:
                preset_vars.append(
                    {"name": var_name, "value": str(default_vars[var_name])}
                )

    parameterisation["presets"]["variables"] = preset_vars
    parameterisation["executionOptions"]["variables"] = option_vars
    pvep["parameterisation"] = parameterisation

    #
    output_file = Path.cwd() / Path(f"{name}.{output_format}")
    if output_file.exists():
        output_file = Path.cwd() / Path(
            f"{name}-autogen-{datetime.datetime.now().isoformat()}.{output_format}"
        )
    write_package_to_file(pvep, output_file)

    stdout.print(f"[green]Success![/green] PVEP saved as {output_file}")


@app.command(options_metavar="[--schema-path <path>]", no_args_is_help=True)
def test(
    ctx: typer.Context,
    path: Path = typer.Argument(
        ...,
        help="Path to the file containing the PVEP",
        exists=True,
        readable=True,
        resolve_path=True,
        file_okay=True,
        show_default=False,
    ),
    schema_path: Optional[Path] = typer.Option(
        None,
        help="Path to the PVEP JSON Schema",
        exists=True,
        readable=True,
        resolve_path=True,
        file_okay=True,
        show_default=False,
    ),
):
    """
    Tests the syntax of a PVEP definition.
    """
    pvep = load_package_from_file(path)

    if schema_path is None:
        schema_path = Path(os.path.abspath(os.path.split(__file__)[0])) / Path(
            "pvep_schema.jsonschema"
        )
        if not schema_path.is_file():
            stderr.print(
                "[red]Error:[/red]\tUnable to load the JSON schema file for PVEPs.\n"
                "[italic]Tip:\tUse [blue]--schema-path[/blue] to supply a valid location.[/italic]"
            )
            raise typer.Exit(code=STPExitCodes.IO_ERROR)

    with open(schema_path) as s:
        schema = json.load(s)

    try:
        jsonschema.validate(pvep, schema)
    except jsonschema.exceptions.ValidationError as e:
        full_path = "instance"
        for subpath in e.path:
            full_path = f"{full_path}['{subpath}']"
        stderr.print(f"[red]Validation error in {full_path}[/red]:\t{e.message}")
        raise typer.Exit(code=STPExitCodes.INPUT_ERROR)
