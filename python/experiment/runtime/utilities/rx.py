#
# coding=UTF-8
# Copyright IBM Inc. 2021. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis

from __future__ import print_function

import logging
import traceback
from typing import Any, Callable, Optional

logger = logging.getLogger('rx_utils')


def report_exceptions(cb, log=None, label=None):
    # type: (Callable[[...], Any], Optional[logging.Logger], Optional[str]) -> Any
    """Report exceptions that take place in RX observers, and raises them

    Args:
        cb: callback function to wrap
    """

    log = log or logger

    def wrapper(*args, **kwargs):
        try:
            return cb(*args, **kwargs)
        except Exception as e:
            log.critical(traceback.format_exc())

            if label:
                log.critical("Exception raised by %s is dismissed: %s" % (label, e))
            else:
                log.critical("Exception dismissed: %s" % e)
            raise e

    return wrapper
