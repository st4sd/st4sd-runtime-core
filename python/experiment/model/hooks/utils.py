# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import importlib.util
import os
import types


def import_module_from_absolute_path(module_name: str, module_path: str) -> types.ModuleType:
    """Imports a module from an absolute path

    Args:
        module_name: the name of the module
        module_path: the path to the module

    Returns:
        A python module

    Raises:
        ImportError: When unable to import module
    """
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    new_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(new_module)
    return new_module


def import_hooks_status(hooks_dir: str, hook_file="status.py") -> types.ModuleType:
    """Imports the status module under the hooks directory

    Args:
        hooks_dir: The directory containing the hooks of the virtual-experiment
        hook_file: Status hooks are found by default in `status.py`

    Returns:
        A python module

    Raises:
        ImportError: When unable to import module
    """
    path_status = os.path.join(hooks_dir, hook_file)
    return import_module_from_absolute_path(f"hooks.{os.path.splitext(hook_file)[0]}", path_status)


def import_hooks_restart(hooks_dir: str, hook_file="restart.py") -> types.ModuleType:
    """Imports the restart module under the hooks directory

    Args:
        hooks_dir: The directory containing the hooks of the virtual-experiment
        hook_file: Restart hooks are found by default in `restart.py`

    Returns:
        A python module

    Raises:
        ImportError: When unable to import module
    """
    path_restart = os.path.join(hooks_dir, hook_file)
    return import_module_from_absolute_path(f"hooks.{os.path.splitext(hook_file)[0]}", path_restart)


def import_hooks_interface(hooks_dir: str, hook_file="interface.py") -> types.ModuleType:
    """Imports the interface module under the hooks directory

    Args:
        hooks_dir: The directory containing the hooks of the virtual-experiment
        hook_file: Interface hooks are found by default in `interface.py`
    Returns:
        A python module

    Raises:
        ImportError: When unable to import module
    """
    path_interface = os.path.join(hooks_dir, hook_file)
    return import_module_from_absolute_path(f"hooks.{os.path.splitext(hook_file)[0]}", path_interface)
