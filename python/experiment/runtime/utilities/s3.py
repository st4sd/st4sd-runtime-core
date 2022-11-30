#
# coding=UTF-8
# Copyright IBM Inc. 2021. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis

import boto3
import botocore.exceptions
import os
import logging

from typing import Optional, cast, Tuple

import experiment.runtime.errors


class S3Utility:
    @classmethod
    def from_boto_client(cls, boto_client):
        # type: (boto3.session.Session.client) -> "S3Utility"
        return S3Utility(access_key_id=None, secret_access_key=None, end_point=None, boto_client=boto_client)

    def __init__(self, access_key_id, secret_access_key, end_point, boto_client=None):
        self.log = logging.getLogger('S3')
        self._client = None  # type: Optional[boto3.session.Session.client]

        if boto_client:
            self._client = boto_client
        else:
            try:
                self._client = boto3.client(
                    's3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key,
                    endpoint_url=end_point)
            except Exception as e:
                raise experiment.runtime.errors.S3Unauthorized(end_point, e)

    @classmethod
    def make_s3_uri(cls, bucket, key):
        """Builds a S3 URI (s3://<bucket-name>/path/to/object)
        Args:
            bucket(str): bucket identifier
            key(str): key to object
        """

        if key.startswith('/'):
            key = key[1:]
        bucket = bucket.rstrip('/')
        return '/'.join(('s3:/', bucket, key))

    @classmethod
    def split_s3_uri(cls, s3_uri):
        # type: (str) -> Tuple[str, str]
        """Splits S3 uri into bucket name and path, raises ValueError exception on incorrect URI.

        Args:
            s3_uri(str): A s3 URI with the format  s3://<bucket-name>/path/to/object

        Returns a tuple of 2 strings. The first is the bucket name, the second is the path in the bucket.
        """
        msg = 'Invalid S3 URI "%s" format is s3://<bucket-name>/path/to/folder' % s3_uri

        if s3_uri and s3_uri.startswith('s3://'):
            try:
                bucket_name, bucket_path = s3_uri[5:].split('/', 1)
            except Exception:
                raise ValueError(msg)
            return bucket_name, bucket_path
        else:
            raise ValueError(msg)

    def key_exists(self, bucket, key):
        """Checks whether key exists in bucket

        Args:
            bucket(str): Bucket identifier
            key(str): Key in bucket

        Returns:
            True if key exists, False if it doesn't
        """
        try:
            self._client.head_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError as e:
            return False
        return True

    def upload_path_to_key(self, bucket, source_path, dest_key, update_existing=True):
        """Uploads file/folder to a key in a bucket

        Args:
            bucket(str): Bucket identifier
            source_path(str): path to file/folder on local storage
            dest_key(str): destination key of file/folder in bucket
            update_existing(bool): If true, will update existing keys, if False will not update existing files
        Returns:
            number of bytes uploaded
        """

        num_bytes = 0

        upload_files = []
        upload_folders = []

        if os.path.isfile(source_path):
            # VV: Remove training '/' so that we can reliably extract relative path of file wrt source_path
            if source_path.endswith('/'):
                source_path = source_path[:-1]
            upload_files.append(source_path)
        elif os.path.isdir(source_path):
            upload_folders.append(source_path)
        else:
            raise experiment.runtime.errors.S3LocalStoragePathMissing(source_path)

        while True:
            if upload_folders:
                folder = upload_folders.pop(0)

                for p in os.scandir(folder):
                    de = cast(os.DirEntry, p)
                    if de.is_file() is False:
                        upload_folders.append(de.path)
                    else:
                        upload_files.append(de.path)

            while upload_files:
                path = upload_files.pop(0)

                if path != source_path:
                    # VV: asked to upload 1 folder to dest_key, and this is one of the files under the folder tree
                    relpath = os.path.relpath(path, source_path)
                    key = os.path.join(dest_key, relpath)
                else:
                    # VV: asked to upload 1 file to dest_key
                    key = dest_key

                if update_existing or (self.key_exists(bucket, key) is False):
                    these_bytes = os.path.getsize(path)
                    num_bytes += these_bytes
                    self.log.info("Uploading %d bytes for %s to s3://%s/%s" % (these_bytes, path, bucket, key))
                    try:
                        self._client.upload_file(Filename=path, Bucket=bucket, Key=key)
                    except Exception as e:
                        raise experiment.runtime.errors.S3UnableToUploadFile(bucket, path, key, e)
                else:
                    self.log.info("Skip uploading %s because it already exists" % path)

            if (not upload_folders) and (not upload_files):
                return num_bytes
