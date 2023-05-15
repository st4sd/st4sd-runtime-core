#
# coding=UTF-8
# Copyright IBM Inc. 2021. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis

"""A container image cache to map images that are referenced by workflow nodes to the image ids that the
backend (e.g. docker, kubernetes, lsf) resolves them into. Following the registration of an imageID to imageID
mapping, future nodes can use the image-cache to directly reference the fully resolved imageID.

This is protecting the nodes in the active workflow instance from unexpected updates to the container images that
they reference."""

from typing import Tuple


def partition_image_uri(img):
    # type: (str) -> Tuple[str, str, str, str]
    """Partitions an image uri to schema, registry-domain, image_name, and tag

    tag includes the delimiter character
       e.g. hello:latest has the `:latest` tag
       e.g. hello@sha256:abcd has the `@sha256:abcd` tag
    when a uri component does not exist the returned component is ''

    Returns:
        A tuple with the following components (in this order)
        str: URI schema also includes ://, may be empty string
        str: domain of the URL (includes trailing /), can be empty string e.g. `ubuntu:latest`
        str: name of image, should not be empty string
        str: the image tag (includes the @ and : delimiters), may be empty string

    Notice that all delimiters are preserved. This is so that joining the returned tuple via ''.join(tuple)
    generates the original image URI.
    """
    schema, delimiter, uri = img.partition('://')

    # VV: Format [<schema>://][<domain>/]<imageName>([:<tag>] | <@<hash>)
    if delimiter == '':
        schema = ''
        uri = img
    else:
        schema = '%s://' % schema

    domain_words = uri.split('/')
    domain = '/'.join(domain_words[:-1])
    if domain:
        domain = '%s/' % domain

    name = domain_words[-1]
    if '@' in name:
        name, tag = name.split('@', 1)
        tag = '@%s' % tag
    elif ':' in name:
        name, tag = name.split(':', 1)
        tag = ':%s' % tag
    else:
        tag = ''

    return schema, domain, name, tag


def expand_image_uri(img):
    # type: (str) -> str
    """Make sure that the image comes with a tag OR a specific hash

    Examples:

      ibm/drl -> ibm/drl:latest

      ibm/drl:tag -> ibm/drl:tag

      ibm/drl@sha256:.... -> ibm/drl@sha256:...
    """
    schema, domain, image, tag = partition_image_uri(img)
    tag = tag or ':latest'
    return ''.join((schema, domain, image, tag))


# VV: Maps expanded container URIs to the fully resolved, and expanded, image URI which is returned by
# kubernetes, in the form of pod.status.containerStatuses.imageID. In this context, expanded means that the
# image URI includes a tag (the default tag is `:latest`).
_image_cache = {}


def cache_lookup(image_uri):
    # type: (str) -> str
    """Returns the fully resolved and expanded form of an image URI assuming that it's been registered with the image
    cache before, otherwise returns fully expanded image uri (see expand_image_uri())
    """
    image_uri = expand_image_uri(image_uri)
    return _image_cache.get(image_uri, image_uri)


def cache_register(image_uri, resolved_image_uri):
    # type: (str, str) -> None
    """Registers an image_uri with its fully resolved image URI, both arguments are fully expanded
    (see expand_image_uri())
    """

    image_uri = expand_image_uri(image_uri)
    resolved_image_uri= expand_image_uri(resolved_image_uri)

    _image_cache[image_uri] = resolved_image_uri


def cache_invalidate(image_uri):
    # type: (str) -> None
    """Removes an URI mapping from the cache (also expands image_uri, see expand_image_uri()), does not raise exception
    if image_uri is not present in the cache"""
    try:
        del _image_cache[expand_image_uri(image_uri)]
    except KeyError:
        pass
