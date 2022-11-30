#! /usr/bin/env python
# coding=UTF-8

# Copyright IBM Inc. 2020. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis



from __future__ import print_function

import glob
import logging
import optparse
import os
import sys
from distutils.dir_util import copy_tree

import experiment.model.frontends.dosini as Dosini
import experiment.model.frontends.flowir as FlowIR


def extract_flowir(source_location):
    errors = []
    log = logging.getLogger('extract')
    log.setLevel(20)
    path_dosini = os.path.join(source_location, 'conf')
    log.info("Parsing DOSINI configuration from %s" % path_dosini)

    raw_flowir = Dosini.Dosini.load_from_directory(
        directory=path_dosini, variable_files=[], is_instance=False, out_errors=errors,
        component_definitions={},
    )

    if errors:
        log.critical("Cannot convert to FlowIR because DOSINI configuration is flawed (%d errors)" % (len(errors)))
        for e in errors:
            log.critical("  %s" % e)
        sys.exit(1)

    return raw_flowir


def print_flowir(source_location):
    log = logging.getLogger('print')
    log.setLevel(20)
    source_location = os.path.abspath(source_location)
    raw_flowir = extract_flowir(source_location)

    pretty_primitive = FlowIR.FlowIR.pretty_flowir_sort(raw_flowir)
    pretty_yaml = FlowIR.yaml_dump(pretty_primitive, sort_keys=False, default_flow_style=False)

    log.info("----FlowIR of package %s----\n%s" % (source_location, pretty_yaml))


def convert_to_flowir(source_location, output_location):
    log = logging.getLogger('convert')
    log.setLevel(20)
    output_location = os.path.abspath(output_location)
    source_location = os.path.abspath(source_location)

    if os.path.exists(output_location):
        raise ValueError("Directory %s already exists" % output_location)

    raw_flowir = extract_flowir(source_location)

    log.info("Populating %s" % output_location)
    copy_tree(source_location, output_location)

    conf_files = [os.path.split(x)[1] for x in glob.glob(os.path.join(output_location, 'conf', '*.conf'))]
    conf_folders = [os.path.split(x)[1] for x in glob.glob(os.path.join(output_location, 'conf', '*.d'))]
    log.info("Archiving stray DOSINI conf files %s and folders %s" % (conf_files, conf_folders))
    archive_path = os.path.join(output_location, 'conf', 'archive_dosini', '')
    os.makedirs(archive_path)
    for c in conf_files + conf_folders:
        os.rename(os.path.join(output_location, 'conf', c), os.path.join(archive_path, c))

    flowir_path = os.path.join(output_location, 'conf', 'flowir_package.yaml')

    pretty_primitive = FlowIR.FlowIR.pretty_flowir_sort(raw_flowir)
    with open(flowir_path, 'w') as f:
        FlowIR.yaml_dump(pretty_primitive, f, sort_keys=False, default_flow_style=False)

    log.info("You can find the FlowIR definition of the package at %s" % flowir_path)
    log.info("You can find the archived DOSINI configuration under %s" % archive_path)


def main():
    usage = "usage: %prog [options] [package]"

    parser = optparse.OptionParser(usage=usage, version="% 0.1", description=__doc__)
    parser.add_option('--convert', '-c', dest='convert', help='Convert DOSINI experiment to FlowIR and store it '
                                                              'in the specified location',
                      default=None)
    parser.add_option("-l", "--logLevel", dest="logLevel",
                      help="The level of logging. Default %default",
                      type="int",
                      default=30,
                      metavar="LOGGING")

    options, args = parser.parse_args()

    FORMAT = '%(levelname)-9s %(threadName)-30s %(name)-30s: %(funcName)-20s %(asctime)-15s: %(message)s'
    logging.basicConfig(format=FORMAT)
    rootLogger = logging.getLogger()
    rootLogger.setLevel(options.logLevel)

    if len(args) != 1:
        rootLogger.critical("Expected exactly 1 argument (path to source experiment)")
        sys.exit(1)

    if options.convert is not None:
        convert_to_flowir(args[0], options.convert)
    else:
        print_flowir(args[0])


if __name__ == '__main__':
    main()
