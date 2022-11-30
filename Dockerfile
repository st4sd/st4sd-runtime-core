# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

ARG base_image=ubuntu:bionic

FROM $base_image
# VV: Builder image
RUN apt-get update && \
    export DEBIAN_FRONTEND=noninteractive && \
    apt-get install -y \
       python3.7 python3-pip python3-tk git locales curl libffi-dev libssl-dev \
       libpng-dev libjpeg-dev libfreetype6-dev pkg-config libxml2-dev libxslt-dev libpython3.7-dev \
       libzmq3-dev
ENV LANGUAGE=en
ENV LC_ALL en_GB.UTF-8
ENV LANG en_GB.UTF-8

RUN locale-gen ${LC_ALL}

RUN python3 -m pip install --upgrade pip virtualenv setuptools six tox && \
    python3 -m pip install "pyzmq>=13,<=22.3.0" papermill 
RUN mkdir /venvs

ENV PIP_DEFAULT_TIMEOUT=120

COPY ./ st4sd-runtime-core

ENV VIRTUAL_ENV=/venvs/st4sd-runtime-core

RUN cd /st4sd-runtime-core && \
    export DEPLOY_VENV=${VIRTUAL_ENV} && \
    export TOX_ENV=py37-deploy && \
    tox -e $TOX_ENV -vv && \
    chmod a+rwx ${VIRTUAL_ENV}/* && \
    chmod a+rwx ${VIRTUAL_ENV}/*/*/*

RUN PATH=${VIRTUAL_ENV}/bin:${PATH} pip3 install twine
RUN cd /st4sd-runtime-core && \
    export PATH=${VIRTUAL_ENV}/bin:${PATH} && \
    python3 setup.py sdist bdist_wheel

# VV: Runtime image
FROM $base_image

ENV LANGUAGE=en
ENV LC_ALL en_GB.UTF-8
ENV LANG en_GB.UTF-8

# VV: Install system-wide pip and python-tk for matplotlib for consistency with moved virtual-env
RUN apt-get update && \
    apt-get upgrade -y && \
    export DEBIAN_FRONTEND=noninteractive && \
    apt-get install -y --no-install-recommends python3.7 python3-pip python3-tk libffi-dev python-rdkit vim-tiny \
       locales libzmq3-dev && \
    locale-gen ${LC_ALL} && \
    rm -rf /var/lib/apt/lists/*

RUN echo 'You can find the licenses of GPL packages in this container under \n\
/usr/share/doc/${PACKAGE_NAME}/copyright \n\
\n\
If you would like the source to the GPL packages in this image then \n\
send a request to this address, specifying the package you want and \n\
the name and hash of this image: \n\
\n\
IBM Research Ireland,\n\
IBM Technology Campus\n\
Damastown Industrial Park\n\
Mulhuddart Co. Dublin D15 HN66\n\
Ireland\n' >/gpl-licenses

ENV VIRTUAL_ENV=/venvs/st4sd-runtime-core
COPY --from=0 /venvs/ /venvs/
COPY --from=0 /st4sd-runtime-core/dist /st4sd-runtime-core/dist


# VV: Activate the virtual environment
ENV PIP_DEFAULT_TIMEOUT=120
ENV PATH=${VIRTUAL_ENV}/bin:${PATH}

RUN elaunch.py -h && \
    elaunch.py --version

CMD elaunch.py -h
