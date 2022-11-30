#! /usr/bin/env python

# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Michael Johnston

'''Creates a skeleton directory structure for a new experiment. '''
from __future__ import print_function

import logging
import optparse
import os
import shutil
import sys
import tarfile

try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess

import experiment.model.data

usage = "usage: %prog [options] [experiment-name]"

parser = optparse.OptionParser(usage=usage, version="% 0.1", description=__doc__)

parser.add_option("-d", "--description", dest="description",
                  help="A string in quotes or a file containing a string. This will be the brief description of the created experiment",
                  default='Description forthcoming',
                  metavar="DESCRIPTION")
parser.add_option("-g", "--initGitRepo", dest="initGitRepo",
                  help="The created experiment directory will be converted into a git repository",
                  action="store_true",
                  metavar="GITREPO")
parser.add_option("-l", "--logLevel", dest="logLevel",
                  help="The level of logging. Default %default",
                  type="int",
                  default=30,
                  metavar="LOGGING")

options,args = parser.parse_args()
if len(args) != 1:
    print('You must provide a name for your experiment', file=sys.stderr)

FORMAT = '%(levelname)-9s %(threadName)-30s %(name)-30s: %(funcName)-20s %(asctime)-15s: %(message)s'
logging.basicConfig(format=FORMAT)
rootLogger = logging.getLogger()
rootLogger.setLevel(options.logLevel)

name = '%s.package' % args[0]


modulePath = os.path.abspath(os.path.split(experiment.model.data.__file__)[0])
filename = modulePath + os.sep + 'resources' + os.sep + 'Template.package.tar'
with tarfile.open(filename) as tar:
    tar.extractall()
    tar.close()

shutil.move('Template.package', name)
os.chdir(name)
p = subprocess.Popen("find . -type f -exec sed -i \"\" 's/\<NAME\>/%s/' {} \;" % args[0], shell=True)
p.wait()
p = subprocess.Popen("find . -type f -exec sed -i \"\" 's/\<DESCRIPTION\>/%s/' {} \;" % options.description, shell=True)
p.wait()
os.chdir('../..')

if options.initGitRepo:
    try:
        import git
    except ImportError:
        print('You must install GitPython to initialise a new repository', file=sys.stderr)
    else:
        r = git.Repo.init(name)
        os.chdir(name)
        r.git.add('.')
        r.git.commit('.', m="'Initial commit of %s experiment'" % args[0])
