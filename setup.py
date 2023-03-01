# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import print_function

# To use a consistent encoding
from codecs import open
from os import path

# Always prefer setuptools over distutils
from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'README.MD'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='st4sd-runtime-core',

    use_scm_version={"root": ".", "relative_to": __file__, "local_scheme": "no-local-version"},
    setup_requires=['setuptools_scm'],

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version='1.0.0',

    description='A tool for creating and deploying computational experiments',
    long_description=long_description,
    long_description_content_type='text/markdown',

    # The project's main homepage.
    url='https://github.com/st4sd/st4sd-runtime-core',

    # Author details
    author='Michael A. Johnston',
    author_email='michaelj@ie.ibm.com',

    # Choose your license
    license='Apache 2.0',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],

    # What does your project relate to?
    keywords='hpc kubernetes openshift lsf workflows experiments computational-chemistry simulation science',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages("python", exclude=['contrib', 'docs', 'tests*']),
    package_dir={'':'python'},

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    # Notes:
    # rx version dep. Rx moved to 3.x after 1.6.1 which introduced new API
    install_requires=['reactivex>=4.0.0', 'pyyaml', 'pytest', 'pytest-xdist', 'pytest-timeout',
                      'networkx', 'matplotlib', 'requests', 'six', 'kubernetes', 'psutil', 'boto3',
                      'pyrsistent', 'js2py', 'pymongo>=4.0', 'papermill', 'pandas', 'future', 'pydantic',
                      'keyring', 'typer'], #, 'pygraphviz'],

    # List additional groups of dependencies here (e.g. development
    # dependencies). You can install these using the following syntax,
    # for example:
    # $ pip install -e .[dev,test]
    extras_require={
        'deploy': ['jupyter', 'paho-mqtt'],
    },

    # If there are data files included in your packages that need to be
    # installed, specify them here.  If using Python 2.6 or less, then these
    # have to be included in MANIFEST.in as well.
    #package_data={
    #    'sample': ['package_data.dat'],
    #},

    package_data={
        'experiment': ['runtime/backend_interfaces/stage-out.sh', 'rewrite-rules.json', 'resources/Template.package.tar']
    },

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    #entry_points={
    #    'console_scripts': [
    #        'sample=sample:main',
    #    ],
    #},

    scripts=[
        "scripts/launchexperiment.py",
        "scripts/elaunch.py",
        "scripts/ecreate.py",
        "scripts/einspect.py",
        "scripts/ccommand.py",
        "scripts/cexecute.py",
        "scripts/etest.py",
        "scripts/ctest.py",
        "scripts/epatch.py",
        "scripts/epatch-apply.py",
        "scripts/lsfsub.py",
        "scripts/eflowir.py",
        "scripts/ememo.py",
        "scripts/ewrap.py",
        "scripts/einputs.py",
        "scripts/stp",
    ]
)
