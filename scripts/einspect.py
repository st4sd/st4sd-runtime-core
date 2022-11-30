#! /usr/bin/env python
# coding=UTF-8

# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

'''Program for inspecting the state of the components in a running experiment.'''

from __future__ import print_function

import logging
import optparse
import sys

import experiment.model.data
import experiment.runtime.span
import experiment.runtime.status
import experiment.model.storage

usage = "usage: %prog [options] [instance]"

parser = optparse.OptionParser(usage=usage, version="% 0.1", description=__doc__)
filterGroup = optparse.OptionGroup(parser, "Define what information is returned")
spanGroup = optparse.OptionGroup(parser, "Span calculation options")

parser.add_option_group(filterGroup)
parser.add_option_group(spanGroup)

parser.add_option("-l", "--logLevel", dest="logLevel",
                  help="The level of logging. Default %default",
                  type="int",
                  default=30,
                  metavar="LOGGING")
parser.add_option("-s", "--stageIndex", dest="stages",
                  help="Output information for the given stage. "
                  "If this option is not given information for all stages is output",
                  default=[],
                  action="append",
                  metavar="STAGE")
parser.add_option("", "--definitions", help="Outputs definitions of some terms and exits",
                  action="store_true",
                  metavar="DEFINITIONS")
parser.add_option("", "--testSpan", help="Test span calculation",
                  action="store_true",
                  metavar="SPAN")
parser.add_option("", "--repairShadowDir", help="Attempt to repair shadow directory (default: False)",
                      action="store_true",
                      metavar="REPAIR_SHADOW_DIR",
                      default=False)

filterGroup.add_option('-c', '--categories', dest='categories',
                       help="Choose additional categories (sets of columns), for which information is output. "
                            "With each use of this option a user can specify one additional category from: "
                            "producer, task, engine",
                       type='choice',
                       action='append',
                       default=[],
                       choices=['producer', 'task', 'engine'],
                       metavar='CATEGORIES')
filterGroup.add_option('','--noDefaults', dest='noDefaults',
                       help="Omits output columns in the categories - useful if there are too many columns "
                            "being output",
                       action='store_true',
                       metavar='NODEFAULTS')
filterGroup.add_option('-f', '--filters', dest='filters',
                       help="Set filters that limit the components output to those exhibiting certain behaviour. "
                            "With each use of this option an additional filter can be specified. "
                            "Available filters are: failed, interesting, engine-exited, blocked-output, "
                            "blocked-resource, not-executed, executing and all. Default filters applied are: "
                            "failed, engine-exited, blocked-output, blocked-resource, not-executed. Specifying this "
                            "flag once overrides the defaults. Specifying 'all' override all other filters",
                       type='choice',
                       action='append',
                       default=[],
                       choices=['failed', 'not-finished', 'interesting', 'engine-exited', 'blocked-output',
                                'blocked-resource', 'not-executed', 'executing', 'all'],
                       metavar='FILTERS')

spanGroup.add_option('-m', '--mode', dest='mode',
                       help="Span calculation mode. any or all. Default %default.",
                       type='choice',
                       default='all',
                       choices=['any', 'all'],
                       metavar='MODE')
spanGroup.add_option("-i", "--interval", dest="interval",
                  help="Interval at which to calculate instantaneous span ",
                  default=60,
                  type='int',
                  metavar="INTERVAL")


options, args = parser.parse_args()

FORMAT = '%(levelname)-9s %(threadName)-30s %(name)-30s: %(funcName)-20s %(asctime)-15s: %(message)s'
logging.basicConfig(format=FORMAT)
rootLogger = logging.getLogger()
rootLogger.setLevel(options.logLevel)

if options.definitions is True:

    print('\nDEFINITIONS OF COMMON TERMS\n')
    print('Backend: An external agent responsible for executing a Task e.g. LSF, local-machine')
    print('Task:   An instance of an external program launched by an Engine on some Backend')
    print('Engine: An object that controls launching and monitoring a Task')
    print('Kernel: The part of a repeating engine that is run at the repeat interval. NOTE: May not '
          'always launch a Task')
    print('Component: A processing element in a workflow graph')
    print('\t- Uses an Engine to control processing')
    print('\t- Its state is separate to the Engine state i.e. a Component can be active even if its Engine has exited')
    print('Aliveness: Denotes if a given object is actively running')
    print('\nDEFINITIONS OF FILTERS\n')
    print('failed: Components which have failed. Will have caused the workflow to exit')
    print('interesting: Components which are in a state other than FINISHED or RUNNING')
    print('engine-exited: Components whose engines have stopped for a reason other than SUCCESS')
    print('blocked-output: Component which cannot launch their tasks as they are waiting on producer output')
    print('blocked-resource: Components whose tasks are waiting on compute resource')
    print('not-executed: Components which have never launched a task - will also be reported in blocked-output')
    print('executing: Components which have an actively running task')
    print('not-finished: Components which are not FAILED or FINISHED')
    sys.exit(0)

if len(args) != 1:
    rootLogger.warning("No instance given - checking if inside one")
    d = experiment.model.storage.FindExperimentTopLevel()
    if d is None:
        rootLogger.info("No experiment given and not within one - aborting")
        sys.exit(1)
    else:
        rootLogger.info("Identified instance at %s" % d)
        args.append(d)

#Check if its an instance before calling below

d = experiment.model.storage.ExperimentInstanceDirectory(args[0], attempt_shadowdir_repair=options.repairShadowDir)
e = experiment.model.data.Experiment(d, is_instance=True, updateInstanceConfiguration=False)

db = experiment.runtime.status.StatusDB(location=d.statusDatabaseLocation)
if db is None:
    print("No status database for instance at %s" % args[0], file=sys.stderr)
    sys.exit(1)

try:
    data = db.getWorkflowStatus()
finally:
    db.close()


if len(list(data)) == 0:
    print('No stage data in status database', file=sys.stderr)
    sys.exit(0)

if len(options.filters) == 0:
    options.filters = ['failed', 'engine-exited', 'blocked-output', 'blocked-resource', 'not-executed']
    print('Filters not specified - using defaults: %s' % options.filters, file=sys.stderr)


print(db.pretty_print_status_details_report(data, stages=[int(x) for x in options.stages or []],
                            categories=options.categories, no_defaults=options.noDefaults,
                            filters=options.filters))

if options.testSpan:
    options.stages = [int(el) for el in options.stages]
    stageIndexes = None if len(options.stages) == 0 else list(range(min(options.stages), max(options.stages)+1))
    if stageIndexes is None:
        print('Calculating span across entire workflow')
    else:
        print('Calculating span using stages from %d to %d' % (stageIndexes[0], stageIndexes[-1]))
    experiment.runtime.span.CalculateSpan(e.graph, stageIndexes, db, mode=options.mode, interval=options.interval)

#ADD SOME FILTERS
# - Failed repeating last task
# - Consistent failures (repeating)
# - Filter on Replica index
# - Filter on component name
