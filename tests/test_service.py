# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# coding=UTF-8
#
# IBM Confidential
# OCO Source Materials
# 5747-SM3
# (c) Copyright IBM Corp. 2022
# The source code for this program is not published or otherwise
# divested of its trade secrets, irrespective of what has
# been deposited with the U.S. Copyright Office.
# Author(s): Vassilis Vassiliadis
# Contact: Vassilis.Vassiliadis@ibm.com

from __future__ import annotations

import logging

import experiment.service.db

logger = logging.getLogger()
logger.setLevel(20)


def test_map_outputs_to_properties():
    interface = {
        "propertiesSpec": [
            {
                "name": "LambdaMax",
                "source": {
                    "output": "molecule-properties",
                    "extractionMethod": {
                        "hook": True
                    }
                }
            },
            {
                "name": "BandGap",
                "source": {
                    "output": "molecule-properties",
                    "extractionMethod": {
                        "hook": True
                    }
                }
            },
            {
                "name": "Energy",
                "source": {
                    "output": "molecule-properties",
                    "extractionMethod": {
                        "hook": True
                    }
                }
            }
        ]
    }

    output_to_properties = experiment.service.db.ExperimentRestAPI.cdb_map_outputs_to_properties(interface)

    assert output_to_properties == {
        "molecule-properties": ["LambdaMax", "BandGap", "Energy"]
    }
