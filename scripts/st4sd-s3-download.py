#!/usr/bin/env python

# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#  Vassilis Vassiliadis
#  Alessandro Pomponio

import argparse
import os

import boto3


# AP: adapted from: https://stackoverflow.com/a/62945526
def download(bucket, s3_key_prefix, output_dir, is_workflow, mode=0o777):

    if is_workflow:
        # AP: For workflows we might receive in input a path to:
        # - a "file": experiments/experiment/conf/dsl.yaml
        # - a "directory": experiments/experiment/ or experiments/experiment
        #
        # If we receive a "file" we want to save it with just its name: dsl.yaml
        # If we receive a "directory" we want to save its contents starting from
        # the os.path.basename of the "directory".
        # To do this, we'll have to remove part of the prefix from the object key
        prefix = s3_key_prefix[:-1] if s3_key_prefix.endswith("/") else s3_key_prefix
        prefix_len_to_be_removed = prefix.rindex("/") + 1
    else:
        prefix_len_to_be_removed = 0

    for obj in bucket.objects.filter(Prefix=s3_key_prefix):
        file_name = obj.key[prefix_len_to_be_removed:]
        target = os.path.join(output_dir, file_name)
        os.makedirs(os.path.dirname(target), mode=mode, exist_ok=True)
        if obj.key[-1] == '/':
            continue
        print(f" {file_name}")
        bucket.download_file(obj.key, target)
        os.chmod(target, mode=mode)


def main():
    parser = argparse.ArgumentParser()

    group = parser.add_argument_group('S3 credentials')
    group.add_argument('--accessKeyID', dest='access_key_id', required=True)
    group.add_argument('--secretAccessKey', dest='secret_access_key', required=True)
    group.add_argument('--endpoint', dest='endpoint', required=True)
    group.add_argument('--bucket', dest='bucket', required=True)

    parser.add_argument('-o', '--outputRoot', dest='output_root', required=True)

    group = parser.add_argument_group('Instructions to download files (can be repeated)')
    group.add_argument('-i', '--input', dest='input', action='append',
                       help='Relative path; file is stored under {OUTPUT_ROOT}/input')
    group.add_argument('-d', '--data', dest='data', action='append',
                       help='Relative path; file is stored under {OUTPUT_ROOT}/data')
    group.add_argument('--workflow', dest='workflow', action='append',
                       help='Relative path; Can be a folder or single file; '
                            'Will be stored under {OUTPUT_ROOT}/{workflow base name}')

    opts = parser.parse_args()

    s3 = boto3.resource('s3',
                        aws_access_key_id=opts.access_key_id,
                        aws_secret_access_key=opts.secret_access_key,
                        endpoint_url=opts.endpoint,
                        region_name=None)
    bucket = s3.Bucket(opts.bucket)

    dir_input = os.path.join(opts.output_root, 'input')
    dir_data = os.path.join(opts.output_root, 'data')

    for folder, files, is_workflow in [
        (dir_input, opts.input, False),
        (dir_data, opts.data, False),
        (opts.output_root, opts.workflow, True)
    ]:

        if not files:
            continue

        print(f"Downloading to {folder}")
        for file in files:
            download(bucket, file, folder, is_workflow)


if __name__ == '__main__':
    main()