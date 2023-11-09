
# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
import threading

import pydantic
from typing import Optional
from typing import Dict
import os

import experiment.model.errors
from pydantic import Field, ConfigDict
from typing_extensions import Annotated


class Orchestrator(pydantic.BaseModel):
    """
    Example environment variables: ST4SD_ORCHESTRATOR_WORKERS_DEFAULT_ALL
    Use the load_settings_orchestrator() method to parse and validate settings (method raises errors that
    include information about offending environment variables)
    """
    model_config = ConfigDict(extra=pydantic.Extra.forbid)

    workers_default_all: Optional[Annotated[int, Field(ge=1)]] = pydantic.Field(
        None, description="If set, sets the default value to all worker polls. This will override the default of "
                          "other individual fields. Users can still override those defaults by specifying an explicit "
                          "value.")
    workers_controller: Optional[Annotated[int, Field(ge=1)]] = pydantic.Field(
        40, description="Number of workers in the Controller threadPool")
    workers_component_state: Optional[Annotated[int, Field(ge=1)]] = pydantic.Field(
        50, description="Number of workers in the ComponentState threadPool")
    workers_engine: Optional[Annotated[int, Field(ge=1)]] = pydantic.Field(
        50, description="Number of workers in the Engine threadPool")
    workers_engine_trigger: Optional[Annotated[int, Field(ge=1)]] = pydantic.Field(
        20, description="Number of workers in the Engine threadPool that Engines use to trigger immediate emissions")
    workers_engine_task: Optional[Annotated[int, Field(ge=1)]] = pydantic.Field(
        100, description="Number of workers in the Engine threadPool that Engines use to wait for tasks to complete")
    workers_backend_k8s: Optional[Annotated[int, Field(ge=1)]] = pydantic.Field(
        50, description="Number of workers in the Backend.Kubernetes threadPool")

    @pydantic.root_validator(pre=True)
    def set_defaults(cls, value: Dict[str, str]) -> Dict[str, str]:
        """If workers_default_all is set (and it's a valid value) then copy it to all fields that are not set

        Argument:
            value: The dictionary that contains key: value definitions of the BaseModel

        Returns:
            A post-processed dictionary with key: value definitions of the BaseModel which may have fields replaced
            to the value of workers_default_all.
        """
        label = 'workers_default_all'
        if isinstance(value, dict) and isinstance(value.get(label), str):
            try:
                # VV: we don't care about the value other than it's a positive integer, we'll still need to set
                # the values of fields to strings. Pydantic runs this method before it parses the values of
                # the dictionary we return to it.
                workers_all = int(value[label])
            except TypeError:
                return value

            if workers_all < 1:
                return value

            for k in cls.__fields__:
                if k == label or k in value:
                    continue
                value[k] = value[label]

        return value


# VV: use load_settings_orchestrator() to parse environment variables and cache them under _OrchestratorCache
_OrchestratorCache: Orchestrator | None = None
_mtx = threading.Lock()


def load_settings_orchestrator(
        environ: Optional[Dict[str, str]] = None,
        env_prefix: str = "ST4SD_ORCHESTRATOR_",
        reuse_if_existing: bool = True,
) -> Orchestrator:
    """Loads orchestrator settings from Environment Variables and Caches the parsed value

    Note::

        The return value of this method is a reference to the pydantic BaseModel containing the orchestartor
        Settings

    Arguments:
        environ: A dictionary containing environment variables and their values. If None defaults to os.environ.
            If set to the empty dictionary ({}) the method loads the default values of the Orchestrator BaseModel.
            Read definition of @reuse_if_existing argument.
        env_prefix: The prefix for the orchestrator environment variable names. The default is ST4SD_ORCHESTRATOR_
            The names of the variables are ${env_prefix}${Orchestrator.Field.upper()}
        reuse_if_existing: If True (default) and the settings have already been loaded before, the method will return
            the existing Settings (i.e. _OrchestratorCache). If True, and settings have been cached, and environ
            has changed since then, the caller will still receive the old (and potentially stale) Settings

    Returns: An Orchestrator object, it also sets the global _OrchestratorCache variable
    Raises:
        experiment.model.errors.EnhancedException:
            If environment variables are wrong. The underlyingError is a modified pydantic.ValidationError that
            encapsulates the name and value of the invalid environment variable.
    """
    global _OrchestratorCache

    # VV: If caller wishes to reuse cached values try for an early-exit here (i.e. without locking the mutex)
    # This is the path that most callers will follow during the orchestration of experiment instances
    if _OrchestratorCache is not None and reuse_if_existing:
        return _OrchestratorCache

    # VV: past this point we need to parse and validate the environment variables
    with _mtx:
        # VV: Do the check one more time so that if multiple threads attempted to load the settings, all but the 1st
        # get an early exit (provided that they wish to reuse an existing collection of settings)
        if _OrchestratorCache is not None and reuse_if_existing:
            return _OrchestratorCache

        _OrchestratorCache = None
        environ = environ if environ is not None else os.environ

        from_environ = {}
        env_vars = {}
        for setting_name in Orchestrator.__fields__:
            env_var_name = "".join([env_prefix, setting_name.upper()])
            if env_var_name in environ:
                env_vars[setting_name] = environ[env_var_name]
                from_environ[env_var_name] = environ[env_var_name]

        try:
            _OrchestratorCache = Orchestrator(**env_vars)
            return _OrchestratorCache
        except pydantic.ValidationError as e:
            # VV: Update the Exception in-place to reflect which environment variables were invalid
            errors = e.errors()
            for problem in errors:
                if 'loc' in problem and len(problem['loc']) == 1:
                    setting_name = problem['loc'][0]

                    if setting_name in env_vars:
                        env_var_name = "".join([env_prefix, setting_name.upper()])
                        problem['loc'] = (f'{env_var_name}="{env_vars[setting_name]}"',)
            raise experiment.model.errors.EnhancedException(
                f"The environment {from_environ} is invalid. The errors are {json.dumps(errors, indent=2)}",
                underlyingError=e) from e
