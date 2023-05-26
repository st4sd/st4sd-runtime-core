# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Author: Alessandro Pomponio

from enum import IntEnum


class STPExitCodes(IntEnum):
    CONTEXT_ERROR = 1
    UNAUTHORIZED = 2
    INPUT_ERROR = 3
    GIT_ERROR = 4
    IO_ERROR = 5
