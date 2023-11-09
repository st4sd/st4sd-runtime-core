# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio

from pathlib import Path
from typing import Dict, Optional

import pydantic
import typer
from pydantic import AnyHttpUrl, BaseModel
from pydantic_settings import BaseSettings

#
APP_NAME = "stp"
DEFAULT_SETTINGS_FILE_NAME = "stp_settings.json"
DEFAULT_CONTEXTS_FILE_NAME = "stp_contexts.json"


#
class Settings(BaseSettings):
    default_context: Optional[str] = pydantic.Field(
        None, description="Default/Active context", env="STP_DEFAULT_CONTEXT"
    )
    verbose: bool = pydantic.Field(
        False, description="Use verbose output", env="STP_VERBOSE"
    )


class Context(BaseModel):
    name: str = pydantic.Field(
        ..., description="Name of the context (defaults to the URL)"
    )
    url: AnyHttpUrl = pydantic.Field(
        ..., description="URL of the ST4SD instance to connect to"
    )


class Contexts(BaseModel):
    entries: Dict[str, Context] = pydantic.Field(
        {}, description="Dictionary containing all the contexts"
    )


class Configuration:
    def __init__(
        self,
        settings_file: Optional[Path],
        contexts_file: Optional[Path],
        verbose: bool,
    ):
        # Create files in case they don't exist, or the JSON decoder
        # will throw a JSONDecodeError
        if settings_file is None:
            settings_file: Path = (
                Path(typer.get_app_dir(APP_NAME)) / DEFAULT_SETTINGS_FILE_NAME
            )
            settings_file.parent.mkdir(exist_ok=True, parents=True)
            if not settings_file.exists():
                settings_file.write_text(Settings().json())

        if contexts_file is None:
            contexts_file: Path = (
                Path(typer.get_app_dir(APP_NAME)) / DEFAULT_CONTEXTS_FILE_NAME
            )
            contexts_file.parent.mkdir(exist_ok=True, parents=True)
            if not contexts_file.exists():
                contexts_file.write_text(Contexts().json())

        #
        self.settings_file = settings_file
        self.contexts_file = contexts_file

        # Remember to set the flags manually to make them available
        # to the rest of the app via the settings
        self.settings = Settings.parse_file(path=settings_file)
        self.settings.verbose = verbose

        #
        self.contexts = Contexts.parse_file(path=contexts_file)

    def update(self):
        self.settings_file.write_text(self.settings.json())
        self.contexts_file.write_text(self.contexts.json())
