import os
import sys

import keyring
import typer

import experiment
import experiment.service.db
import experiment.service.errors
from experiment.cli.configuration import Configuration


def get_api(ctx: typer.Context):
    # Help with autocomplete
    config: Configuration = ctx.obj

    # Check we have a valid context
    active_context_name = config.settings.default_context
    if active_context_name is None:
        typer.echo("There is no active context.")
        typer.echo("Run stp context activate to set one")
        typer.echo("Or run stp login to create a new one")
        sys.exit(os.EX_NOHOST)

    # Check we have a URL for the context
    url = config.contexts.entries.get(active_context_name).url
    if url is None:
        typer.echo(f"No URL is defined for the active context ({active_context_name})")
        typer.echo("Run stp login to set one")
        sys.exit(os.EX_NOHOST)

    # Check the credentials are still working
    token = keyring.get_password(url, active_context_name)
    if token is None:
        typer.echo(f"No token could be found for context {active_context_name}")
        typer.echo("Run stp login to set one")
        sys.exit(os.EX_UNAVAILABLE)

    # Attempt login
    try:
        api = experiment.service.db.ExperimentRestAPI(url, max_retries=2, secs_between_retries=1,
                                                      cc_auth_token=token)
    except experiment.service.errors.UnauthorisedRequest:
        typer.echo(f"Login attempt failed. Ensure the URL and the access token are correct")
        sys.exit(os.EX_NOPERM)
    else:
        if config.settings.verbose:
            typer.echo(f"Successfully logged into {url}")

    return api
