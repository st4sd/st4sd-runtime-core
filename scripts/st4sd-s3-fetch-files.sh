#!/bin/bash

# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#  Vassilis Vassiliadis

missing=""

if [[ -z "${S3_ACCESS_KEY_ID}" ]]; then
    export missing="  S3_ACCESS_KEY_ID\n"
fi

if [[ -z "${S3_SECRET_ACCESS_KEY}" ]]; then
    export missing="${missing}  S3_SECRET_ACCESS_KEY\n"
fi

if [[ -z "${S3_ENDPOINT}" ]]; then
    export missing="${missing}  S3_ENDPOINT\n"
fi

if [[ -z "${S3_BUCKET}" ]]; then
    export missing="${missing}  S3_BUCKET\n"
fi

if [[ ! -z "${missing}" ]]; then
    echo "Missing environment variables:"
    echo -e "${missing}"
    exit 1
fi

export S3_MOUNT_POINT=${S3_MOUNT_POINT:-/s3-${S3_BUCKET}}
export ROOT_OUTPUT=${ROOT_OUTPUT:-/tmp/files}

which python3

if [[ $? -eq 0 ]]; then
    export cmd_python="python3"
else
    export cmd_python="python"
fi

st4sd-s3-download.py \
  --accessKeyID "${S3_ACCESS_KEY_ID}" \
  --secretAccessKey "${S3_SECRET_ACCESS_KEY}" \
  --endpoint "${S3_ENDPOINT}" \
  --bucket "${S3_BUCKET}" \
  --outputRoot "${ROOT_OUTPUT}" ${@:1}