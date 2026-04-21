# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Vassilis Vassiliadis

import os
from string import Template
from typing import Dict, Optional, Set, Tuple


class UndefinedEnvironmentVariablesError(Exception):
    """
    Exception raised when a PVEP references environment variables that are not defined.

    This exception is raised by update_pvep_with_environment_values() when one or more
    environment variables referenced in a PVEP string (using $VAR or ${VAR} syntax)
    do not have values in the provided environment dictionary or os.environ.

    Attributes:
        missing_variables: List of environment variable names that are not defined
        message: Human-readable error message
    """

    def __init__(self, missing_variables: list):
        """
        Initialize the exception.

        Args:
            missing_variables: List of environment variable names that are missing
        """
        self.missing_variables = sorted(missing_variables)
        vars_list = ", ".join(self.missing_variables)
        plural = "s" if len(self.missing_variables) > 1 else ""
        self.message = f"Missing environment variable{plural}: {vars_list}"
        super().__init__(self.message)


def update_pvep_with_environment_values(
    pvep_string: str, environment: Optional[Dict[str, str]] = None
) -> Tuple[str, list[str]]:
    """
    Replace environment variable references in a PVEP string with their values.

    This function searches for environment variables in both $NAME and ${NAME}
    syntax within the PVEP string representation. It validates that all referenced
    environment variables exist before performing substitution.

    Environment variables are escaped using $$NAME and $${NAME}.

    Args:
        pvep_string: String representation of a Parameterised Virtual Experiment
                     Package (PVEP), typically JSON or YAML format
        environment: Optional dictionary of environment variables. If not provided,
                     uses os.environ from the current process

    Returns:
        Updated PVEP string with all environment variable references replaced
        by their values
        The names of the environment variables found in the PVEP string

    Raises:
        ValueError: If one or more referenced environment variables are not found
                    in the environment. The exception message lists all missing
                    variables without exposing their values (for security).

    """
    if environment is None:
        environment = dict(os.environ)

    # VV: Use Template's own pattern to find variable references
    # This ensures we match exactly what Template.substitute() will process
    template = Template(pvep_string)

    # Find all matches using Template's pattern
    referenced_vars: Set[str] = set()
    for match in template.pattern.finditer(pvep_string):
        # Template pattern has groups: 'escaped', 'named', 'braced', 'invalid'
        # We only care about 'named' (for $VAR) and 'braced' (for ${VAR})
        var_name = match.group("named") or match.group("braced")
        if var_name:
            referenced_vars.add(var_name)

    missing_vars = sorted([var for var in referenced_vars if var not in environment])

    if missing_vars:
        raise UndefinedEnvironmentVariablesError(missing_vars)

    # VV: All variables are present, perform substitution using string.Template
    # Template.substitute() automatically handles $$NAME and $${NAME} as escape sequences
    return template.substitute(environment), sorted(referenced_vars)
