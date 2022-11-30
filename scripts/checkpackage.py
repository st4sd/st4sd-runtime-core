#! /usr/bin/env python

# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

'''Checks a compuational experiment package for errors etc.'''
from __future__ import print_function

import logging
import optparse
import sys

import experiment.test

usage = "usage: %prog [options] [package]"

parser = optparse.OptionParser(usage=usage, version="% 0.1", description=__doc__)

parser.add_option("-v", "--verbose", dest="verbose",
		help="Turns on debugging output",
		action="store_true",
		default=False,
		metavar="VERBOSE")

options, args = parser.parse_args()

FORMAT = '%(levelname)-9s %(threadName)-30s %(name)-30s: %(funcName)-20s %(asctime)-15s: %(message)s'
logging.basicConfig(format=FORMAT)
rootLogger = logging.getLogger() 
rootLogger.setLevel(20)

if len(args) > 1:
	rootLogger.critical("Too many arguments %s" % args)
	sys.exit(1)

if len(args) ==0:
	rootLogger.critical("No package specified")
	sys.exit(1)

packagePath = args[0]
rootLogger.info("Checking package %s " % packagePath)

if not experiment.test.ValidatePackage(packagePath)[0]:
	rootLogger.critical("Package validation failed")
	sys.exit(1)

if not experiment.test.CheckSystem():
	rootLogger.critical("System check failed")
	sys.exit(1)
