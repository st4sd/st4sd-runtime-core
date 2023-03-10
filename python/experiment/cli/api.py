# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio

import keyring
import typer
from rich.console import Console

import experiment
import experiment.service.db
import experiment.service.errors
from experiment.cli.configuration import Configuration
from experiment.cli.exit_codes import STPExitCodes

stderr = Console(stderr=True)
stdout = Console()


def get_api(ctx: typer.Context, for_context: str = None):
    # Help with autocomplete
    config: Configuration = ctx.obj

    # Check we have a valid context
    active_context_name = config.settings.default_context
    if for_context is None and active_context_name is None:
        stderr.print("There is no active context.")
        stderr.print("Run stp context activate to set one")
        stderr.print("Or run stp login to create a new one")
        raise typer.Exit(code=STPExitCodes.CONTEXT_ERROR)

    # Check we have a URL for the context
    if for_context is not None:
        active_context_name = for_context
    url = config.contexts.entries.get(active_context_name).url
    if url is None:
        stderr.print(f"No URL is defined for context ({active_context_name})")
        stderr.print("Run stp login to set one")
        raise typer.Exit(code=STPExitCodes.CONTEXT_ERROR)

    # Check the credentials are still working
    token = keyring.get_password(url, active_context_name)
    if token is None:
        stderr.print(f"No token could be found for context {active_context_name}")
        stderr.print("Run stp login to set one")
        raise typer.Exit(code=STPExitCodes.CONTEXT_ERROR)

    # Attempt login
    try:
        api = experiment.service.db.ExperimentRestAPI(url, max_retries=2, secs_between_retries=1,
                                                      cc_auth_token=token)
    except experiment.service.errors.UnauthorisedRequest:
        stderr.print("Login attempt failed. Ensure the URL and the access token are correct")
        stderr.print("Note that the credentials might have expired. You can update them with:")
        stderr.print(f"stp login --force --context-name {active_context_name} {url}")
        raise typer.Exit(code=STPExitCodes.UNAUTHORIZED)
    else:
        if config.settings.verbose:
            stdout.print(f"Successfully logged into {url}")

    return api
