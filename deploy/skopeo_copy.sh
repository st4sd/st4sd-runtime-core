#!/usr/bin/env sh

image_src="${1}"
image_dst="${2}"

skopeo login -u "${DOCKER_USERNAME}" -p "${DOCKER_TOKEN}" "${DOCKER_REGISTRY}" && \
skopeo copy --dest-precompute-digests --multi-arch all --preserve-digests  --retry-times 10 \
            "docker://${image_src}" "docker://${image_dst}"
