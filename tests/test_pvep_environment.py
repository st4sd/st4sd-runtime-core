# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Vassilis Vassiliadis

import os
import pytest

from experiment.utilities.pvep import (
    update_pvep_with_environment_values,
    UndefinedEnvironmentVariablesError,
)


class TestUpdatePVEPWithEnvironmentValues:
    """Test suite for update_pvep_with_environment_values function."""

    def test_no_environment_variables(self):
        """Test PVEP string with no environment variables."""
        pvep_string = '{"name": "test", "value": "static"}'
        result, vars_found = update_pvep_with_environment_values(pvep_string, {})
        assert result == pvep_string
        assert vars_found == []

    def test_single_dollar_sign_syntax(self):
        """Test substitution with $VAR syntax."""
        pvep_string = '{"url": "$REGISTRY_URL"}'
        env = {"REGISTRY_URL": "https://example.com"}
        result, vars_found = update_pvep_with_environment_values(pvep_string, env)
        assert result == '{"url": "https://example.com"}'
        assert vars_found == ["REGISTRY_URL"]

    def test_braced_syntax(self):
        """Test substitution with ${VAR} syntax."""
        pvep_string = '{"commit": "${COMMIT_ID}"}'
        env = {"COMMIT_ID": "abc123"}
        result, vars_found = update_pvep_with_environment_values(pvep_string, env)
        assert result == '{"commit": "abc123"}'
        assert vars_found == ["COMMIT_ID"]

    def test_mixed_syntax(self):
        """Test substitution with both $VAR and ${VAR} syntax."""
        pvep_string = '{"url": "$REGISTRY_URL", "commit": "${COMMIT_ID}"}'
        env = {"REGISTRY_URL": "https://example.com", "COMMIT_ID": "abc123"}
        result, vars_found = update_pvep_with_environment_values(pvep_string, env)
        assert result == '{"url": "https://example.com", "commit": "abc123"}'
        assert set(vars_found) == {"REGISTRY_URL", "COMMIT_ID"}

    def test_multiple_occurrences_same_variable(self):
        """Test that the same variable can be used multiple times."""
        pvep_string = '{"url1": "$BASE_URL", "url2": "$BASE_URL"}'
        env = {"BASE_URL": "https://example.com"}
        result, vars_found = update_pvep_with_environment_values(pvep_string, env)
        assert (
            result == '{"url1": "https://example.com", "url2": "https://example.com"}'
        )
        assert vars_found == ["BASE_URL"]

    def test_variable_in_middle_of_string(self):
        """Test variable substitution within a larger string."""
        pvep_string = '{"url": "https://$HOST/path"}'
        env = {"HOST": "example.com"}
        result, vars_found = update_pvep_with_environment_values(pvep_string, env)
        assert result == '{"url": "https://example.com/path"}'
        assert vars_found == ["HOST"]

    def test_multiple_variables_in_one_value(self):
        """Test multiple variables in a single value."""
        pvep_string = '{"url": "$PROTOCOL://$HOST:$PORT"}'
        env = {"PROTOCOL": "https", "HOST": "example.com", "PORT": "8080"}
        result, vars_found = update_pvep_with_environment_values(pvep_string, env)
        assert result == '{"url": "https://example.com:8080"}'
        assert set(vars_found) == {"PROTOCOL", "HOST", "PORT"}

    def test_missing_single_variable(self):
        """Test error when a single environment variable is missing."""
        pvep_string = '{"url": "$MISSING_VAR"}'
        with pytest.raises(UndefinedEnvironmentVariablesError) as exc_info:
            update_pvep_with_environment_values(pvep_string, {})
        assert "Missing environment variable: MISSING_VAR" in str(exc_info.value)
        assert exc_info.value.missing_variables == ["MISSING_VAR"]
        # Ensure the error message doesn't expose values
        assert (
            "value" not in str(exc_info.value).lower()
            or "variable" in str(exc_info.value).lower()
        )

    def test_missing_multiple_variables(self):
        """Test error when multiple environment variables are missing."""
        pvep_string = '{"url": "$VAR1", "commit": "$VAR2", "tag": "$VAR3"}'
        env = {"VAR2": "present"}  # Only VAR2 is present
        with pytest.raises(UndefinedEnvironmentVariablesError) as exc_info:
            update_pvep_with_environment_values(pvep_string, env)
        error_msg = str(exc_info.value)
        assert "Missing environment variables:" in error_msg
        assert "VAR1" in error_msg
        assert "VAR3" in error_msg
        assert "VAR2" not in error_msg  # VAR2 is present, shouldn't be in error
        # Verify the exception has the missing_variables attribute
        assert set(exc_info.value.missing_variables) == {"VAR1", "VAR3"}

    def test_uses_os_environ_by_default(self):
        """Test that function uses os.environ when no environment is provided."""
        # Set a test environment variable
        test_var_name = "TEST_PVEP_VAR_12345"
        test_var_value = "test_value_xyz"
        os.environ[test_var_name] = test_var_value

        try:
            pvep_string = f'{{"test": "${test_var_name}"}}'
            result, vars_found = update_pvep_with_environment_values(pvep_string)
            assert result == f'{{"test": "{test_var_value}"}}'
            assert vars_found == [test_var_name]
        finally:
            # Clean up
            del os.environ[test_var_name]

    def test_custom_environment_overrides_os_environ(self):
        """Test that custom environment dict is used instead of os.environ."""
        # Set a variable in os.environ
        test_var_name = "TEST_PVEP_VAR_67890"
        os.environ[test_var_name] = "os_environ_value"

        try:
            pvep_string = f'{{"test": "${test_var_name}"}}'
            custom_env = {test_var_name: "custom_value"}
            result, vars_found = update_pvep_with_environment_values(
                pvep_string, custom_env
            )
            # Should use custom_value, not os_environ_value
            assert result == '{"test": "custom_value"}'
            assert vars_found == [test_var_name]
        finally:
            # Clean up
            del os.environ[test_var_name]

    def test_variable_names_with_underscores(self):
        """Test variables with underscores in their names."""
        pvep_string = '{"var": "$MY_VAR_NAME"}'
        env = {"MY_VAR_NAME": "value"}
        result, vars_found = update_pvep_with_environment_values(pvep_string, env)
        assert result == '{"var": "value"}'
        assert vars_found == ["MY_VAR_NAME"]

    def test_variable_names_with_numbers(self):
        """Test variables with numbers in their names."""
        pvep_string = '{"var": "$VAR123"}'
        env = {"VAR123": "value"}
        result, vars_found = update_pvep_with_environment_values(pvep_string, env)
        assert result == '{"var": "value"}'
        assert vars_found == ["VAR123"]

    def test_lowercase_variable_names(self):
        """Test that lowercase variable names are supported."""
        pvep_string = '{"var": "$my_var"}'
        env = {"my_var": "value"}
        result, vars_found = update_pvep_with_environment_values(pvep_string, env)
        assert result == '{"var": "value"}'
        assert vars_found == ["my_var"]

    def test_mixed_case_variable_names(self):
        """Test that mixed case variable names are supported."""
        pvep_string = '{"var": "$MyVar"}'
        env = {"MyVar": "value"}
        result, vars_found = update_pvep_with_environment_values(pvep_string, env)
        assert result == '{"var": "value"}'
        assert vars_found == ["MyVar"]

    def test_empty_string(self):
        """Test with empty PVEP string."""
        result, vars_found = update_pvep_with_environment_values("", {})
        assert result == ""
        assert vars_found == []

    def test_yaml_format(self):
        """Test with YAML-formatted PVEP."""
        pvep_string = """
base:
  packages:
    - name: main
      source:
        git:
          location:
            url: $REPO_URL
            commit: ${COMMIT_ID}
"""
        env = {"REPO_URL": "https://github.com/example/repo", "COMMIT_ID": "abc123"}
        result, vars_found = update_pvep_with_environment_values(pvep_string, env)
        assert "https://github.com/example/repo" in result
        assert "abc123" in result
        assert set(vars_found) == {"REPO_URL", "COMMIT_ID"}

    def test_special_characters_in_values(self):
        """Test that special characters in values are preserved."""
        pvep_string = '{"url": "$URL"}'
        env = {"URL": "https://example.com/path?query=value&other=123"}
        result, vars_found = update_pvep_with_environment_values(pvep_string, env)
        assert result == '{"url": "https://example.com/path?query=value&other=123"}'
        assert vars_found == ["URL"]

    def test_empty_environment_variable_value(self):
        """Test that empty string values are handled correctly."""
        pvep_string = '{"var": "$EMPTY_VAR"}'
        env = {"EMPTY_VAR": ""}
        result, vars_found = update_pvep_with_environment_values(pvep_string, env)
        assert result == '{"var": ""}'
        assert vars_found == ["EMPTY_VAR"]

    def test_vars_found_sorted(self):
        """Test that returned variable names are sorted."""
        pvep_string = '{"z": "$ZULU", "a": "$ALPHA", "m": "$MIKE"}'
        env = {"ZULU": "z", "ALPHA": "a", "MIKE": "m"}
        result, vars_found = update_pvep_with_environment_values(pvep_string, env)
        assert vars_found == ["ALPHA", "MIKE", "ZULU"]

    def test_escaped_dollar_sign_double(self):
        """Test that $$VAR is treated as a literal $VAR (not expanded)."""
        pvep_string = '{"literal": "$$VAR", "expanded": "$VAR"}'
        env = {"VAR": "value"}
        result, vars_found = update_pvep_with_environment_values(pvep_string, env)
        # $$VAR should become $VAR (literal), $VAR should become "value"
        assert result == '{"literal": "$VAR", "expanded": "value"}'
        # Only the non-escaped VAR should be in vars_found
        assert vars_found == ["VAR"]

    def test_escaped_dollar_sign_braced(self):
        """Test that $${VAR} is treated as a literal ${VAR} (not expanded)."""
        pvep_string = '{"literal": "$${VAR}", "expanded": "${VAR}"}'
        env = {"VAR": "value"}
        result, vars_found = update_pvep_with_environment_values(pvep_string, env)
        # $${VAR} should become ${VAR} (literal), ${VAR} should become "value"
        assert result == '{"literal": "${VAR}", "expanded": "value"}'
        # Only the non-escaped VAR should be in vars_found
        assert vars_found == ["VAR"]

    def test_multiple_escaped_and_expanded(self):
        """Test mix of escaped and expanded variables."""
        pvep_string = '{"a": "$$ESCAPED", "b": "$EXPANDED", "c": "$${ALSO_ESCAPED}", "d": "${ALSO_EXPANDED}"}'
        env = {
            "ESCAPED": "val1",
            "EXPANDED": "val2",
            "ALSO_ESCAPED": "val3",
            "ALSO_EXPANDED": "val4",
        }
        result, vars_found = update_pvep_with_environment_values(pvep_string, env)
        # Escaped ones should remain as literals, expanded ones should be substituted
        assert "$ESCAPED" in result  # Literal $ESCAPED
        assert "val2" in result  # Expanded EXPANDED
        assert "${ALSO_ESCAPED}" in result  # Literal ${ALSO_ESCAPED}
        assert "val4" in result  # Expanded ALSO_EXPANDED
        # Only non-escaped variables should be in vars_found
        assert set(vars_found) == {"EXPANDED", "ALSO_EXPANDED"}

    def test_realistic_pvep_example(self):
        """Test with a realistic PVEP structure."""
        pvep_string = """{
  "base": {
    "packages": [
      {
        "name": "main",
        "source": {
          "git": {
            "location": {
              "url": "$GIT_REPO_URL",
              "commit": "${GIT_COMMIT}"
            }
          }
        }
      }
    ]
  },
  "metadata": {
    "package": {
      "name": "my-experiment",
      "description": "Experiment using $BACKEND backend"
    }
  },
  "parameterisation": {
    "presets": {
      "variables": [
        {
          "name": "registry",
          "value": "${CONTAINER_REGISTRY}"
        }
      ]
    }
  }
}"""
        env = {
            "GIT_REPO_URL": "https://github.com/example/repo",
            "GIT_COMMIT": "abc123def456",
            "BACKEND": "kubernetes",
            "CONTAINER_REGISTRY": "docker.io/myorg",
        }
        result, vars_found = update_pvep_with_environment_values(pvep_string, env)

        # Verify all substitutions occurred
        assert "https://github.com/example/repo" in result
        assert "abc123def456" in result
        assert "kubernetes" in result
        assert "docker.io/myorg" in result

        # Verify no variable references remain
        assert "$GIT_REPO_URL" not in result
        assert "${GIT_COMMIT}" not in result
        assert "$BACKEND" not in result
        assert "${CONTAINER_REGISTRY}" not in result

        # Verify all variables were found
        assert set(vars_found) == {
            "GIT_REPO_URL",
            "GIT_COMMIT",
            "BACKEND",
            "CONTAINER_REGISTRY",
        }
