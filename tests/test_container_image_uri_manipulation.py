# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# coding=UTF-8
#
# IBM Confidential
# OCO Source Materials
# 5747-SM3
# (c) Copyright IBM Corp. 2021
# The source code for this program is not published or otherwise
# divested of its trade secrets, irrespective of what has
# been deposited with the U.S. Copyright Office.
# Author(s): Vassilis Vassiliadis
# Contact: Vassilis.Vassiliadis@ibm.com

import experiment.runtime.utilities.container_image_cache
import pytest


@pytest.fixture(params=[
    {'uri': 'http://domain/image:tag', 'schema': 'http://', 'domain': 'domain/', 'image': 'image', 'tag': ':tag'},
    {'uri': 'http://domain/image@tag', 'schema': 'http://', 'domain': 'domain/', 'image': 'image', 'tag': '@tag'},
    {'uri': 'http://domain/image@sha256:1', 'schema': 'http://', 'domain': 'domain/', 'image': 'image',
        'tag': '@sha256:1'},
    {'uri': 'http://a.b/c/image', 'schema': 'http://', 'domain': 'a.b/c/', 'image': 'image', 'tag': ''},
    {'uri': 'docker-pullable://a.b/c/image@sha256:abc', 'schema': 'docker-pullable://', 'domain': 'a.b/c/',
        'image': 'image', 'tag': '@sha256:abc'},
    {'uri': 'http://a.b/c/image@sha256:1', 'schema': 'http://', 'domain': 'a.b/c/', 'image': 'image', 'tag': '@sha256:1'},
    {'uri': 'domain/image@tag', 'schema': '', 'domain': 'domain/', 'image': 'image', 'tag': '@tag'},
    {'uri': 'image@tag', 'schema': '', 'domain': '', 'image': 'image', 'tag': '@tag'},
    {'uri': 'image', 'schema': '', 'domain': '', 'image': 'image', 'tag': ''},
])
def image_breakdown(request):
    return request.param


def test_partition_image_uri(image_breakdown):
    schema, domain, image, tag = experiment.runtime.utilities.container_image_cache.partition_image_uri(image_breakdown['uri'])

    assert schema == image_breakdown['schema']
    assert domain == image_breakdown['domain']
    assert image == image_breakdown['image']
    assert tag == image_breakdown['tag']

    # VV: Ensure that putting everything back together generates the original URI
    assert ''.join((schema, domain, image, tag)) == image_breakdown['uri']


def test_expand_image_uri_tag(image_breakdown):
    img = experiment.runtime.utilities.container_image_cache.expand_image_uri(image_breakdown['uri'])
    schema, domain, image, tag = experiment.runtime.utilities.container_image_cache.partition_image_uri(img)
    assert tag == image_breakdown['tag'] or ':latest'
