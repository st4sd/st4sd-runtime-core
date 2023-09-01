# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio

import keyring
import typer
from rich.console import Console

from experiment.cli.configuration import Configuration
from experiment.cli.exit_codes import STPExitCodes

stderr = Console(stderr=True)
stdout = Console()


def get_api(ctx: typer.Context, for_context: str = None):
    #
    config: Configuration = ctx.obj

    # Check we have a valid context - either the default one
    # or the one that was passed to the function
    active_context_name = config.settings.default_context
    if for_context is None and active_context_name is None:
        stderr.print(
            "[red]Error:[/red]\tThere is no active context.\n"
            "[italic]Tip:\tRun [blue]stp context activate[/blue] to set one of the contexts as active\n"
            "\tOr run [blue]stp login[/blue] to create a new one[/italic]"
        )
        raise typer.Exit(code=STPExitCodes.CONTEXT_ERROR)

    if for_context is not None:
        active_context_name = for_context

    # Check we have a URL for the context
    url = config.contexts.entries.get(active_context_name).url
    if url is None:
        stderr.print(
            f"[red]Error:[/red]\tNo URL is defined for context {active_context_name}\n"
            "[italic]Tip:\tRun [blue]stp login[/blue] to set one[/italic]"
        )
        raise typer.Exit(code=STPExitCodes.CONTEXT_ERROR)

    # Check we have credentials for the context
    token = keyring.get_password(url, active_context_name)
    if token is None:
        stderr.print(
            f"[red]Error:[/red]\tNo token could be found for context {active_context_name}\n"
            "[italic]Tip:\tRun [blue]stp login[/blue] to set one[/italic]"
        )
        raise typer.Exit(code=STPExitCodes.CONTEXT_ERROR)

    # Attempt login to check credentials still work
    import experiment.service.db
    import experiment.service.errors
    try:
        api = experiment.service.db.ExperimentRestAPI(
            url, max_retries=2, secs_between_retries=1, cc_auth_token=token
        )
    except (
        experiment.service.errors.UnauthorisedRequest,
        experiment.service.errors.InvalidHTTPRequest,
    ):
        stderr.print(
            "[red]Error:[/red]\tLogin attempt failed. Ensure the URL and the access token are correct.\n"
            "[italic]Tip:\tThe credentials might have expired. You can update them with:\n"
            f"\t[blue]stp login --force --context-name {active_context_name} {url}[/blue][/italic]"
        )
        raise typer.Exit(code=STPExitCodes.UNAUTHORIZED)
    else:
        if config.settings.verbose:
            stdout.print(f"[green]Successfully logged into {url}[/green]")

    return api
