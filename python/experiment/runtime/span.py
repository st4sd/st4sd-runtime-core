#
# coding=UTF-8
# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Michael Johnston

from __future__ import print_function

import copy
import datetime
import logging
import math
from collections import OrderedDict
from functools import reduce
from typing import Tuple, Dict, Any, List

import dateutil.parser
import networkx
import numpy
from past.builtins import cmp

import experiment.utilities.data
from experiment.model.graph import ProducersForNode
from experiment.runtime.status import StatusDB


# VV: `all_check` and `any_check` are used in a `reduce()`
#     the very first time x is a node, every subsequent call
#     uses x to represent the accumulated value


def all_check(x, y):
    if isinstance(x, bool):
        return x and y[1]['colour'] == 'green'
    else:
        return x[1]['colour'] == 'green' and y[1]['colour'] == 'green'


def any_check(x, y):
    if isinstance(x, bool):
        return x or y[1].get('colour') == 'green'
    else:
        return x[1].get('colour') == 'green' and y[1].get('colour') == 'green'


conditions = {
    'any': any_check,
    'all': all_check
}


def GetLastCompletionTime(node, database):

    '''Returns the last timew the given node started and completed successfully

    Args:
        node (tuple): Contains (nodeLabel, nodeData)
        database (experiment.runtime.status.StatusDB): Use to access execution data for node

    Returns:
        A Tuple of datetime.dateimtes or None.
            The start and finish times (as a date) that the node last executed successfully'''

    data = database.executionDataForComponent(node[0]) #type: experiment.utilities.data.Matrix
    data.setKeyColumn('exit-code')
    #FIXME: Handle no key 0
    #need to unify next check which is doesn't work
    data = data.matrixForKey(0)

    lastCompletionEndTime = None  # type: datetime.datetime
    lastCompletionStartTime = None  # type: datetime.datetime
    if data.numberOfRows() > 0:
        #start will be a date-as-string
        #duration will be a time-as-string (h:m:s)
        start, duration = data.columns(['execution-date', 'execution-time'])[-1]
        duration = dateutil.parser.parse(duration)
        lastCompletionStartTime = dateutil.parser.parse(start, dayfirst=True)
        lastCompletionEndTime = lastCompletionStartTime+datetime.timedelta(hours=duration.hour, minutes=duration.minute, seconds=duration.second)

    return lastCompletionStartTime, lastCompletionEndTime


def ComponentResourcesUsed(node, startTime, endTime, database):

    '''Returns metrics related total resources used by a node between a start time and end time

    Args:
    node (tuple):  Must contain two elements, (nodeLabel, nodeData), of the node whose green-time is sought
        nodeLabel should be the name of the node as a reference string
        nodeData is a dictionary of node attributes
    startTime (datetime.datetime): Time to start accounting for resource usage
    endTime (datetime.datetime): Time to finish accounting for resource usage.
    database (experiment.runtime.status.StatusDB): Used to get executiong data .

    Resource usage is inclusive of partial runs at the start and exclusive at the end.
    That is if `node` was already running at `startTime` this execution is counted.
    If `node` was running at `endTime` this execution is not.

    Returns:
        metrics (dict): Contains metrics related to resources used'''

    executionTime = -1
    greenTime = None
    spanNode = None

    log = logging.getLogger('graph.span.resourcesused')

    metrics = OrderedDict([
        ('total-executions', 0),
        ('successful-executions', 0),
        ('failed-executions', 0),
        ('total-execution-time', 0),
        ('successful-execution-time', 0),
        ('failed-execution-time', 0),
    ])

    data = database.executionDataForComponent(node[0])
    for date, time, exitCode in data.columns(
            ['execution-date', 'execution-time', 'exit-code']):  # type: (str, str, int)
        execDate = dateutil.parser.parse(date, dayfirst=True)
        execTime = dateutil.parser.parse(time, dayfirst=True)
        duration = datetime.timedelta(hours=execTime.hour, minutes=execTime.minute, seconds=execTime.second)
        finishTime =  execDate+duration
        if startTime < finishTime < endTime:
            metrics['total-executions'] += 1
            metrics['total-execution-time'] += duration.total_seconds()
            if exitCode == 0:
                metrics['successful-executions'] += 1
                metrics['successful-execution-time'] += duration.total_seconds()
            else:
                metrics['failed-executions'] += 1
                metrics['failed-execution-time'] += duration.total_seconds()

    return metrics


def GraphResourcesUsed(g):

    '''Returns total resources used by graph to propagate data to leafs

    If nodes have no execution data they are silently ignored - assumed caller wants this

    Returns:
        metrics (dict): A dict containing resource information'''

    graphMetrics = OrderedDict([
        ('total-executions', 0),
        ('successful-executions', 0),
        ('failed-executions', 0),
        ('total-execution-time', 0),
        ('successful-execution-time', 0),
        ('failed-execution-time', 0),
    ])

    for node in networkx.topological_sort(g):
        nodeData = g.nodes[node]
        metrics = nodeData.get('resource-utilisation')
        if metrics is not None:
            for k in list(graphMetrics.keys()):
                graphMetrics[k] += metrics[k]

    return graphMetrics


def GetProfileDataForObserverWithSubjectsThatHaveLongShutdown(node, producers, database):
    """

    Args:
        node (tuple):  Must contain two elements, (nodeLabel, nodeData), of the node whose green-time is sought
            nodeLabel should be the name of the node as a reference string
            nodeData is a dictionary of node attributes
        producers (list): A list of tuples. Tuples have the same format as the node parameter
    """
    # VV: Get non-raw configuration
    node_config = node[1]['getConfiguration'](False)
    data = database.executionDataForComponent(node[0])
    log = logging.getLogger('graph.span.shutdowncorrection')

    if node_config['workflowAttributes']['isRepeat']:
        # VV: This is a repeating component, make sure that it reports success for the execution
        #     right after all of its producers finished
        log.debug("Repeating node is %s" % node[0])
        log.debug('Performance data: %s' % data.csvRepresentation())

        successes = [exit_code == 0 for exit_code in data.column('exit-code')]

        has_succeeded = any(successes)

        if has_succeeded:
            # VV: Find point
            max_producer_success_termination = None
            all_producers_have_run = True
            log.debug("%s has producers %s" % (node[0], [k[0] for k in producers]))

            for prod in producers:
                prod_data = database.executionDataForComponent(prod[0])

                runs = prod_data.columns(['execution-date', 'task-run-time', 'exit-code'])

                if runs:
                    last_run = runs[-1]
                    exec_date, exec_time, exit_code = last_run
                    exec_date = dateutil.parser.parse(exec_date, dayfirst=True)
                    try:
                        exec_time = float(exec_time)
                    except Exception as e:
                        all_producers_have_run = False
                        log.debug("Exception:Last execution of producer %s of %s was: when: %s, dur: %s, ret: %s" % (
                            prod[0], node[0], exec_date, exec_time, exit_code
                        ))
                        log.debug(e)
                    else:
                        exec_finished = exec_date + datetime.timedelta(seconds=exec_time)
                        log.debug("Last execution of producer %s of %s was: when: %s, dur: %s, ret: %s" % (
                            prod[0], node[0], exec_date, exec_time, exit_code
                         ))

                        if max_producer_success_termination is None or \
                                max_producer_success_termination < exec_finished:
                            max_producer_success_termination = exec_finished
                else:
                    all_producers_have_run = False
                    log.debug("Producer %s of %s has never run" % (prod[0], node[0]))

            log.debug("Last producer of %s finished at %s" % (node[0], max_producer_success_termination))

            if all_producers_have_run and max_producer_success_termination is not None:
                log.debug("Checking if first execution of %s after %s should be corrected to be a success" % (
                    node[0], max_producer_success_termination.strftime("%d%m%y-%H%M%S")
                 ))

                # VV: Create a copy of the performance data matrix
                headers = data.columnHeaders()
                all_rows = []
                for row_idx in range(data.numberOfRows()):
                    elements = data.getElements(row_idx)
                    all_rows.append([elements[header] for header in headers])

                for row in all_rows:
                    exec_date = row[data.indexOfColumnWithHeader('execution-date')]
                    try:
                        exec_date = dateutil.parser.parse(exec_date, dayfirst=True)
                    except Exception:
                        log.debug("Skipping row because exec_date=\"%s\"" % exec_date)
                        continue

                    if exec_date > max_producer_success_termination:
                        if row[data.indexOfColumnWithHeader('exit-code')] == 0:
                            log.debug("Execution at %s is already a success" % exec_date)
                            return data

                        exec_time = row[data.indexOfColumnWithHeader('task-run-time')]
                        try:
                            exec_time = float(exec_time)
                        except Exception:
                            row[data.indexOfColumnWithHeader('task-run-time')] = 0
                            exec_time = "%s" % datetime.timedelta(seconds=0)
                            row[data.indexOfColumnWithHeader('execution-time')] = exec_time

                        row[data.indexOfColumnWithHeader('exit-code')] = 0

                        ts = exec_date.strftime("%d%m%y-%H%M%S")
                        row[data.indexOfColumnWithHeader('epoch-started')] = ts
                        row[data.indexOfColumnWithHeader('epoch-submitted')] = ts
                        row[data.indexOfColumnWithHeader('epoch-finished')] = ts

                        log.info("Set fake success for %s at %s - after %s" % (node[0],
                            exec_date.strftime("%d%m%y-%H%M%S"),
                            max_producer_success_termination.strftime("%d%m%y-%H%M%S")))
                        break
                data = experiment.utilities.data.Matrix(rows=all_rows, headers=headers, name=data.name())
            else:
                log.warning("Some producers of %s have never finished" % node[0])
        else:
            log.warning("Node %s has never had a successful task execution" % node[0])

    return data


def FindGreenTime(
        node,  # type: Tuple[str, Dict[str, Any]]
        producers,  # type: List[Tuple[str, Dict[str, Any]]]
        mode,  # type: str
        database,  # type: StatusDB
    ):
    # type: (...) -> Tuple[float, datetime.datetime, Tuple[str, Dict[str, Any]]]
    '''Finds the green-time of node

    Args:
        node (tuple):  Must contain two elements, (nodeLabel, nodeData), of the node whose green-time is sought
            nodeLabel should be the name of the node as a reference string
            nodeData is a dictionary of node attributes
        producers (list): A list of tuples. Tuples have the same format as the node parameter
            The colour of producers is identified via the key 'colour' in its nodeData dictionary
            The greenTime via the key 'greenTime'
        mode (str): Either 'any' or 'all'. Specifies the conditions underwhich the producers are considered `green`
        database (experiment.runtime.status.StatusDB): Used to get executiong data .

    Returns:
        datetime.datetime:  Defines the time this node went green, or None if it could not be determined

    Notes
    For Observers we need to factor in that a producer may have long shut-down times thereby stop producing
    data N seconds before exiting. The algorithm should not assume that a subject *constantly* produces data.
    The fix is to automatically tag the first task of an Observer right after its producers have finished
    as successful (even though it might not be) as long as it has had at least 1 successful tasks before.
    '''

    executionTime = -1
    greenTime = None
    spanNode = None

    log = logging.getLogger('graph.span.findgreentime')

    if reduce(conditions[mode], producers):
        #They may not all be green depending on mode
        greenNodes = [n for n in producers if n[1]['colour'] == 'green']
        log.info('Green nodes are %s' % [el[0] for el in greenNodes])
        greenTimes = numpy.asarray([n[1]['greenTime'] for n in greenNodes])
        log.info('Green times are %s' % ",".join([str(g) for g in greenTimes]))

        if len(greenTimes) != 0:
            #Producers that are sources: `greenTime` is set via ColourSources
            #   If the mode is 'any' only producers that are active at the current timepoint are green
            #   The greenTime will be the last-emission before timepoint
            #Producers that are repeating: 'greenTime' is set via this function
            #   Thus source->repeating (any mode): greenTime should be the first completion after the last producer to go green
            #   Following above this is a function of "timepoint"

            #Identify the last node to go green BEFORE
            #Note: argmax returns an int not list in at least some version of numpy
            spanNode = greenNodes[numpy.argmax(greenTimes)]
            log.info('Span node is %s' % spanNode[0])

            data = GetProfileDataForObserverWithSubjectsThatHaveLongShutdown(node, producers, database)

            #TODO: If there is queueing we should look for the actual start time
            #TODO: Search from end?
            log.debug('greentimes: %s' % greenTimes)
            for date, time, exitCode in data.columns(['execution-date', 'execution-time', 'exit-code']):
                log.debug("Execution data -> Date: %s Duration: %s Exit-Code: %s", date, time, exitCode)
                if exitCode == 0:
                    execDate = dateutil.parser.parse(date, dayfirst=True)
                    # VV: TODO Why use execution-time instead of task-run-time ?
                    execTime = dateutil.parser.parse(time, dayfirst=True)
                    duration = datetime.timedelta(hours=execTime.hour, minutes=execTime.minute, seconds=execTime.second)

                    #We are looking for the first execution after the producers went green
                    #Two possibilities depending on mode
                    if getattr(numpy, mode)(greenTimes < execDate) :
                        executionTime = duration.total_seconds()  # type: float
                        greenTime = execDate+duration
                        log.info("Execution at %s -> After span node green time. Duration %s. Green-time: %s"
                                 % (execDate, duration, greenTime))
                        break
                    else:
                        log.info("Execution at %s -> Before span node green time. Skipping" % execDate)
                else:
                    log.info("Execution at %s -> Skipping as failed with exit-code %s" % (date, exitCode))

    return executionTime, greenTime, spanNode


def UpdateNodeSpanColour(node, graph, statusDatabase, timePoint, mode='any'):

    '''Updates the colour of a node and the span metrics

    Args:
        node (str): The node whose colour is to be updated
        graph (networkx.Digraph): The graph
        timePoint (datetime.datetime): The start time for this span calculation.
            Required for resource utilisation metrics
        mode (str): Either "one" or "all".
            "all": Node requires all its producers to be green before it can be green
            "any": A node can turn green if any of its producers is green
        statusDatabase (experiment.runtime.status.StatusDB): Used to access status of node.
    '''

    #Node is green if
    # - all/one input are green
    # - if has successfully executed since all inputs/one input went green

    nodeData = graph.nodes()[node]

    log = logging.getLogger('graph.span.colour.%s' % node)
    log.info('Span Mode is %s' % mode)

    #Determine producers in same stage
    inputEdges = networkx.reverse(graph).edges(node)
    producers = [el[1] for el in inputEdges]
    producers = [(n, graph.nodes[n]) for n in producers]
    producers = [n for n in producers if n[1]['stageIndex'] == nodeData['stageIndex']]

    #Find the green time and color the node as appropriate
    executionTime, greenTime, spanNode = FindGreenTime((node, nodeData), producers, mode, statusDatabase)
    nodeData['colour'] = 'green' if greenTime is not None else 'red'

    log.info('Node colour is: %s' % nodeData['colour'])

    #If the node is green we can calculate the span
    #producerSpan - time between producers producing all/any data and node consuming it
    #totalSpan - the total time from the start of the graph until the node conumes any/all producers data.
    if nodeData['colour'] == 'green':
        #Set absolute completion time
        nodeData['greenTime'] = greenTime  # type: datetime.datetime
        #Set nodeSpan - the time for the "last" piece of data produced to be processed
        nodeData['producerSpan'] = (greenTime - spanNode[1]['greenTime']).total_seconds() #type: int
        nodeData['totalSpan'] = spanNode[1]['totalSpan'] + nodeData['producerSpan']

        log.info('Green Time: %s', nodeData['greenTime'])
        log.info('Producer span: %lf', nodeData['producerSpan'])
        log.info('Total span: %lf', nodeData['totalSpan'])

        metrics = ComponentResourcesUsed((node, nodeData), timePoint, nodeData['greenTime'], statusDatabase)
        nodeData['resource-utilisation'] = metrics

    #Get/Estimate the latency along each edge
    try:
        repeatInterval = float(nodeData['componentSpecification'].workflowAttributes['repeatInterval'])
    except:
        repeatInterval = 60.0

    log.info('Nodes repeat-interval is: %lf' % repeatInterval)

    #lastCompletionTime will be the
    if greenTime is not None:
        lastCompletionEndTime = greenTime
        lastCompletionStartTime = None
    else:
        lastCompletionStartTime, lastCompletionEndTime = GetLastCompletionTime((node, nodeData), statusDatabase)
        log.info("Last completion: start %s. end %s" % (lastCompletionStartTime, lastCompletionEndTime))

    latencies = []
    for producer in producers:
        producerData = producer[1]
        log.info('Calculating latency from %s' % producer[0])
        log.info('Producer green time %s' % producerData.get('greenTime'))
        #NoteCan only estimate if producer is green ...
        if nodeData.get('greenTime') is not None and producerData.get('greenTime') is not None:
            latencyDelta = nodeData['greenTime'] - producerData['greenTime']  # type: datetime.timedelta
            log.info('The latency is seconds is %lf' % latencyDelta.total_seconds())
            latency = latencyDelta.total_seconds() #type: float
            log.info('Latency from %s: %lf (green->green)' % (producer[0], latency))
        elif producerData.get('greenTime') is not None:
            #If node is not green can still calculate edge latency as we may be in `all` mode
            #I.e. node may have consumed data from this producer successfully but not all producers
            #Three conditions:
            #1. node succesfully executed before the producer,
            #2. node successfully executed after
            #3. node did not successfully last time it tried
            if lastCompletionStartTime is None:
                #The last execution of the node was not successful
                #We dont know at the moment when the last successful one was
                latency = float('inf')
                log.info('Latency from %s: %lf (estimate green->unknown)' % (producer[0], latency))
            elif lastCompletionStartTime < producerData['greenTime']:
                #The consumer successfully executed but started BEFORE producer finished
                #The estimated success time is next execution of this node
                if lastCompletionEndTime < producerData['greenTime']:
                    #How long has elapsed since the lastCompletion
                    log.info('Consumer last successful execution finished before green time of %s' % producer[0])
                    latencyDelta = producerData['greenTime'] - lastCompletionEndTime # type: datetime.timedelta
                    log.info('The lower bound latency is seconds is %lf' % latencyDelta.total_seconds())
                    latency = math.ceil(latencyDelta.total_seconds()/float(repeatInterval))*repeatInterval #type: float
                    log.info('Latency from %s: %lf (estimate green->unknown)' % (producer[0], latency))
                else:
                    log.info('Consumers last successful execution started before but '
                             'finished after green time of %s' % producer[0])
                    #It started before the producer greenTime but finished sometime after it ...
                    #It wont consume all the emitted data unitl it repeats again at earliest
                    latencyDelta = (lastCompletionEndTime-producerData.get('greenTime'))
                    latency = latencyDelta.total_seconds() + repeatInterval  #type: float
                    log.info('Latency from %s: %lf (estimate green->unknown)' % (producer[0], latency))
                    lastCompletionEndTime = lastCompletionStartTime-datetime.timedelta(seconds=repeatInterval)
                    log.info('Estimated end of last successful execution that finished BEFORE '
                             'green time: %s', lastCompletionEndTime)
            else:
                #Here if lastCompletion > producerData['greenTime']:
                #This means the node successfully STARTED executing after the producer - not an estimate
                latencyDelta = lastCompletionEndTime - producerData['greenTime'] # type: datetime.timedelta
                latency = latencyDelta.total_seconds()
                log.info('Latency from %s: %lf (green->post-success)' % (producer[0], latency))

        else:
            latency = float('inf')

        #In rare case the latency may be zero
        #FIXME: Set it to one for the moment
        latency = latency if latency != 0 else 1

        print('\tedge-state',  producer[0], producerData.get('greenTime'), node, lastCompletionEndTime, nodeData['colour'], latency, 1/latency)

        if nodeData['colour'] != 'green':
            log.debug('Node is not green - execution data follows')
            log.debug('producer %s perf-data:' % producer[0])
            data = statusDatabase.executionDataForComponent(producer[0])
            log.debug(data.csvRepresentation())

            log.debug('consumer %s perf-data:' % node)

            data = statusDatabase.executionDataForComponent(node)
            log.debug(data.csvRepresentation())

        latencies.append(latency)

    flows = [1.0/x for x in latencies]

    log.info("Flows are %s" % flows)

    #Assign metrics
    #For each input edge calculate Consumption-Latency (CL) and flow
    #    - time since producer went green and consumer successfully executed (or estimate for red)
    #    - flow  = 1/CL
    #Total CL To Node = MAX({cl}) (same with Flow)
    #SpanTime

    #(node span time) = (Time since last successful execution) + (producers span time)

    networkx.set_node_attributes(graph, {node:nodeData})

    return nodeData['colour']


def NodeRunningAtTimePoint(node, statusDatabase, timePoint):

    '''Returns True if the Node was running at timepoint otherwise false

    Returns:
        (bool): True if node is running at timepoint'''

    isRunningTimePoint = False

    #executionDataForComponent only returns info for completed exectuions
    #Therefore to identify if timepoint is during a current execution we have to query current status
    componentStatus = statusDatabase.getComponentStatus(node)
    engineIsRunning = componentStatus['engine']['isRunning']
    lastLaunchedDate = componentStatus['engine']['lastTaskLaunchDate']
    lastFinishedDate = componentStatus['engine']['lastTaskFinishedDate']
    execDate = dateutil.parser.parse(lastLaunchedDate, dayfirst=True)
    finishDate = dateutil.parser.parse(lastFinishedDate, dayfirst=True)

    if timePoint > execDate:
        #The timePoint is after the last launch time of the task
        #Check if its before the time this task ended
        if lastFinishedDate == 'None':
            #Task may have died without recording a finishedDate - in this case we use engineIsRunning
            isRunningTimePoint = engineIsRunning
        else:
            isRunningTimePoint = True if timePoint < finishDate else False

    else:
        # If timePoint is before the run returned by getComponentStatus we have to look further back in the history
        data = statusDatabase.executionDataForComponent(node)
        for date, time, exitCode in data.columns(
                ['execution-date', 'execution-time', 'exit-code']):  # type: (str, str, int)
            execDate = dateutil.parser.parse(date, dayfirst=True)
            execTime = dateutil.parser.parse(time, dayfirst=True)
            duration = datetime.timedelta(hours=execTime.hour, minutes=execTime.minute, seconds=execTime.second)

            #If the timePoint is before execDate there's no point continuing
            if timePoint < execDate:
                break
            else:
                # If it started within 1 secs of the min we count it as starting at same time
                isRunningTimePoint = True if execDate <= timePoint + datetime.timedelta(seconds=1) <= execDate + duration else False
                if isRunningTimePoint is True:
                    break

    return isRunningTimePoint


def NodeFinishedAtTimePoint(node, statusDatabase, timePoint):

    '''Returns True if the Node is finished at timepoint otherwise false

    If no execution data is present for a Task this function will return it False becase:
    - Either it is actually not finished
    - If failed without launching -- it never finished.

    Returns:
        (bool): True if node is finished at timepoint'''

    isFinishedTimePoint = False
    data = statusDatabase.executionDataForComponent(node)

    if data is not None:
        #Get the last exec date
        date, time, exitCode = data.columns(['execution-date', 'execution-time', 'exit-code'])[-1]  # type: (str, str, int)
        execDate = dateutil.parser.parse(date, dayfirst=True)
        execTime = dateutil.parser.parse(time, dayfirst=True)
        duration = datetime.timedelta(hours=execTime.hour, minutes=execTime.minute, seconds=execTime.second)

        #Note: The extra second is required to be consistent with NodeRunningAtTimePoint
        isFinishedTimePoint = True if  execDate + duration < timePoint+  datetime.timedelta(seconds=1) else False
        finishedTime = execDate+duration
    else:
        isFinishedTimePoint = None

    return isFinishedTimePoint, execDate+duration


def ColourSources(graph, statusDatabase, timePoint, mode='all'):

    '''Colours the sources of graph

    Any mode: If they are active at timePoint they are green, otherwise red
    All mode: If they are active at timePoint OR finished they are green, otherwise red

    The difference is required because
    - All mode will block if any producers are red -> unless finished sources are green you cannot calculate all span
        once sources start finishing
    - Any mode uses first producer green time. If finished sources are green they will be used as the 'any' condition,
        and all spans once a producer finishes will be incorrect.
        That is, when checking which consumer execution consumes the producers, the one that consumes a finished
        producer will always be chosen.
    '''

    sources = [n for n in graph if not graph.nodes[n]['componentSpecification'].workflowAttributes['repeatInterval']]

    # Go through the sources
    for node in sources:

        isRunningTimePoint = NodeRunningAtTimePoint(node, statusDatabase, timePoint)
        isFinishedTimePoint, finishedTime = NodeFinishedAtTimePoint(node,statusDatabase, timePoint)

        if isRunningTimePoint or isFinishedTimePoint and mode =='all':
            lastCompletion = timePoint if isFinishedTimePoint is False else finishedTime
            colour = 'green'
        elif isRunningTimePoint and mode == 'any':
            lastCompletion = timePoint
            colour = 'green'
        else:
            lastCompletion = None
            colour = 'red'

        d = {'colour': colour,
             'greenTime': lastCompletion,
             'spanTime': 0,
             'totalSpan': 0}
        networkx.set_node_attributes(graph, {node: d})

        if colour == 'red':
            print('Source node %s is not active at timepoint %s' % (node, timePoint))
        elif isFinishedTimePoint:
            print('Source node %s is finished at timepoint %s' % (node, timePoint))
        else:
            print('Source node %s has started by timepoint %s - last emission %s' % (node, timePoint, lastCompletion))


def UpdateSpan(graph, statusDatabase, timePoint=None, mode='all'):

    '''Starts or updates a span for a workflow graph

    Note: A graph may have to be passed multiple times before the span is returned.
    This is because data may not have progressed through the graph by the time this function is called.

    To start a span calculation the sources nodes cannot be coloured.
    Starting == colouring the sources
    Updating == processing a graph whose sources are coloured.

    Args:
        graph (networkx.DiGraph): A (coloured) graph of a workflow stage
        statusDatabase (experiment.runtime.status.StatusDB): A database instance providing access to component status
        timePoint (datetime.datetime): Emission time to calculate span from.
            If workflow sources are coloured this parameter is ignored
            If it is None and the source-nodes of the graph are not coloured, behaviour depends on workflow state:
                workflow finished: timePoint defaults to the last emission time of the sources
                workflow not-finished: timePoint defaults to 'now'
        mode (str): 'All' or 'Any'. Defines when a node is considered green

    Note: graph is modified by this function

    Returns:
        (networkx.Digraph, bool, float, int):
            1. The graph (identical to the graph passed)
            2. True if the total span (all leaves are green) could be calculated.
            3. The total span if above is True else 0
            4. The number of leaves which are green
    '''

    #Set/update the colour of the sources at the timePoint
    ColourSources(graph,statusDatabase,timePoint, mode=mode)

    redNodes = []
    rg = networkx.reverse(graph)
    for node in networkx.topological_sort(graph):
        nodeData = graph.nodes[node]
        producers = ProducersForNode(node, rg, reversed=True)
        if (mode == 'all' or len(producers) ==1) and any(p in redNodes for p in producers):
            #print 'Skipping %s as it has red producer' % node
            #We add it to red nodes
            redNodes.append(node)
            graph.nodes[node]['colour'] = 'red'
            continue

        isRepeating = nodeData['componentSpecification'].workflowAttributes['isRepeat']
        if isRepeating:
            c = nodeData.get('colour')

            #green - get retrieve the spanTime to this node
            #red/None - Update-Node
            #     - UpdateNode
            #
            #UpdateNode
            #   - node is still red, producers all red - Nothing
            #   - node is still red, some producers green - Estimate flows for green->red
            #   - node is green - Update totalSpan, Get/Estimate all input flows

            if c != 'green':
                #This node is red, previous node is green (as we're still in the loop)
                #Check if color reassigned
                c = UpdateNodeSpanColour(node, graph, statusDatabase, timePoint, mode=mode)
                #We don't need to check nodes downstream of this red node
                #However we can't just break as not all nodes are downstream of it:wq
                if c == 'red':
                    redNodes.append(node)

    leafNodes = [node for node in graph.nodes() if graph.in_degree(node) != 0 and graph.out_degree(node) == 0]
    #This list can't be empty - there must be an entry for each leafNode
    leavesState = ['totalSpan' in graph.nodes[l] for l in leafNodes]

    finished = reduce(lambda x, y: x and y, leavesState)

    #Handle reduce behaviour with 1 element - won't be converted as the lambda won't be used
    if len(leafNodes) > 1:
        completedLeaves = reduce(lambda x,y: int(x)+int(y), leavesState)
    else:
        completedLeaves = int(leavesState[0])

    span = -1
    if finished:
        span = max([graph.nodes()[n]['totalSpan'] for n in leafNodes])

    return graph, finished, span, completedLeaves


def CalculateSpan(workflow, stageIndexes, statusDatabase, mode='all', window=1, interval=60):

    '''Calculates the span of the workflow versus time

    Args:
        workflow (networkx.Digraph): The full workflow graph
        stageIndexes (list): Indexes of the stages to calculate the span of. If None the span across the full graph is calculated.
        statusDatabase (experiment.runtime.status.StatusDB): A database instance providing access to component status
        mode (str): 'All' or 'Any'. Defines when a node is considered green
        window (int): Span will be block averaged over intervals of this length
            window will be centered on time-point.
            If window is even 1 will be added to make odd.
            The reported value at each time-point will be the average span of the instaneous values
            at time-points in range +-(0.5*window)
        interval (int): x-axis interval. Time in seconds at which to calculate the span

    Returns:
        experiment.utilities.data.Matrix: Contains the results'''

    #Contains map of time-index to graph and its properties (a dict)
    #The mapped dict has keys ('graph', 'finished', 'span')
    graphs = {}

    #Graphs which have active span calculations.
    #List of (graph, time-index) tuples
    activeGraphs = []

    #Get start time of workflow
    if stageIndexes is None:
        spanGraph = workflow
    else:
        spanGraph = workflow.subgraph([n[0] for n in workflow.nodes(data=True) if n[1]['stageIndex'] in stageIndexes])

    sources = [(n,spanGraph.nodes[n]) for n in spanGraph
                if not spanGraph.nodes[n]['componentSpecification'].workflowAttributes['repeatInterval']]


    print('Source component statuses', statusDatabase.getComponentStatus(sources[0][0]))
    startTimes = [statusDatabase.executionDataForComponent(n[0]).column('execution-date')[0] for n in sources]

    print('Source component start times', startTimes)

    startTime = min([dateutil.parser.parse(d, dayfirst=True) for d in startTimes])
    print('WORKFLOW START-TIME:', startTime)

    #Get current time or finish time of workflow if available
    leafNodes = [node for node in spanGraph.nodes() if spanGraph.in_degree(node) != 0 and spanGraph.out_degree(node) == 0]

    print('Graph leaf-nodes', leafNodes)

    if len(leafNodes) > 1:
        isRunning = reduce(lambda x,y: x and y, [('True' == statusDatabase.getComponentStatus(n)['engine']['isRunning']) for n in leafNodes])
    else:
        isRunning = ('True' == statusDatabase.getComponentStatus(leafNodes[0])['engine']['isRunning'])

    print('Is workflow running?:', isRunning)

    if isRunning:
        endTime = datetime.datetime.now()
    else:
        #The last time the span can be calculated from is when the last source finished
        dates = [statusDatabase.getComponentStatus(node[0])['engine']['lastTaskLaunchDate'] for node in sources]
        dates = [dateutil.parser.parse(d) for d in dates]
        print('Last launched dates for all sources', dates)
        runTimes = [statusDatabase.getComponentStatus(node[0])['engine']['lastTaskRunTime'] for node in sources]
        runTimes = [dateutil.parser.parse(d) for d in runTimes]
        print('Last run times for all sources', runTimes)
        finishedTimes = [d + datetime.timedelta(hours=r.hour, minutes=r.minute,
                                                       seconds=r.second) for d,r in zip(dates,runTimes)]
        print('Last finished times for all sources', finishedTimes)
        endTime = max(finishedTimes)

    print('WORKFLOW END-TIME:', endTime)
    print('WORKFLOW DURATION:', (endTime-startTime).total_seconds())

    #Form the sample points
    timePoints = list(range(0, int(math.ceil((endTime-startTime).total_seconds())), interval))
    print('The span will be calculated at the following time-points', timePoints)
    print('There are %d time-points in total' % len(timePoints))

    blockages = {
        # <delta:int>: List[name_of_blocked_component:str]
    }  # type: Dict[int, List[str]]

    for delta in timePoints:
        print('Calculating span for time point:', delta, startTime+datetime.timedelta(seconds=delta))
        # Note: If the experiment is finished there should be no active graphs as the span can be calculated immediately
        print('There are %d active graphs' % len(activeGraphs))
        # A. Propagate all active graphs
        completedGraphs = []
        for index, data in enumerate(activeGraphs):
            g, t = data
            print('Updating active timepoint', t)
            g, finished, span, completedLeaves = UpdateSpan(g, statusDatabase, mode=mode)
            if finished:
                resourceMetrics = GraphResourcesUsed(g)
                resourceString = ["%s %s" % (k, resourceMetrics[k]) for k in list(resourceMetrics.keys())]
                print('Timepoint %s is finished: SPAN %s. ' % (t, span), resourceString)
                completedGraphs.append(index)
                graphs[t]['span'] = span
                graphs[t]['finished'] = finished
                graphs[t]['resource-utilisation'] = resourceMetrics

        #B. Remove any graphs that have completed after B from active graphs
        completedGraphs.sort(cmp, reverse=True)
        for index in completedGraphs:
            activeGraphs.pop(index)

        #C. Start new span calculation for this time-point
        print('Starting new calculation for', delta)
        graph = copy.deepcopy(spanGraph)
        graph, finished, span, completedLeaves = UpdateSpan(graph, statusDatabase,
                timePoint=startTime+datetime.timedelta(seconds=delta),
                mode=mode)

        # If the workflow is finished and we can't calculate span it means some/all of the workflow was blocked at timepoint
        # This can either be because
        # A) some sources nodes have not turned green/started (so no data from theses, so no span where `all` sources have to reach leafs)
        # B) There is a permanent blockage i.e. all nodes are emitting but some consumers never successfull processed that output

        resourceMetrics = GraphResourcesUsed(graph)
        resourceString = ["%s %s" % (k, resourceMetrics[k]) for k in list(resourceMetrics.keys())]

        if finished:
            print('Timepoint %s. Data propagated to leafs: SPAN %s' % (delta, span), resourceString)
        elif not finished:
            if not isRunning:
                #Check if all sources are emitting at this time-point
                sources = [n for n in graph if not
                                graph.nodes[n]['componentSpecification'].workflowAttributes['repeatInterval']]
                if len(sources) > 1:
                    greenSources = reduce(lambda x, y: x + y, [graph.nodes[n]['colour'] == 'green' for n in sources])
                else:
                    greenSources = 1 if graph.nodes[sources[0]].get('colour') == 'green' else 0

                if greenSources == len(sources):
                    #the worklow is finished but we we're unable to calculate a span => some component never executed
                    #We can ignore the blocked component and report the span to non-blocked leaf nodes
                    leafNodes = [node for node in graph.nodes() if graph.in_degree(node) != 0 and graph.out_degree(node) == 0]
                    blockedNodes =  [l for l in leafNodes if 'totalSpan' not in graph.nodes[l] ]
                    if blockedNodes:
                        blockages[delta] = list(blockedNodes)
                    try:
                        span = max([graph.nodes()[n]['totalSpan'] for n in leafNodes if 'totalSpan' in graph.nodes()[n]])
                    except ValueError:
                        print('Blockage affects all leaf-nodes. Span N/A')
                        span = 'N/A'
                    print('Timepoint %s. Workflow has a permanent blockage to leaf nodes %s: SPAN %s' % (delta, blockedNodes, span), resourceString)
                else:
                    print('Timepoint %s. Workflow is blocked as only %d of %d sources are active: SPAN %s' % (delta, greenSources, len(sources), 'NA'), resourceString)
                    span = 'N/A'
            else:
                activeGraphs.append((graph, delta))

        graphs[delta] = {'graph': graph, 'span': span, 'finished': finished, 'resource-utilisation':resourceMetrics}

    #Calculate KPIs
    #Makespan - requires workflow to be finished
    makespan = -1
    if len(activeGraphs) == 0:
        #Two ways - last-execution of last leaf node - start
        #Or (time last emission - start) + last emission span
        #Start is 0
        try:
            makespan = timePoints[-1] + graphs[timePoints[-1]]['span']
            print('MAKESPAN:', makespan)
        except TypeError:
            #If last span was N/A
            print('MAKESPAN:', 'Unknown - last emission never propogated to any leaf')

    else:
        print('There are %d active graphs' % len(activeGraphs))
        for g,d in activeGraphs:
            print(d)

    #First Output Response
    firstGraph = None
    firstOutputResponse = None
    for timePoint in timePoints:
        if graphs[timePoint]['span'] != 'N/A':
            print('First complete span of graph achieved from emissions at timepoint %s' % timePoint)
            firstGraph = graphs[timePoint]['graph'] #  type:  networkx.DiGraph
            firstOutputResponse = graphs[timePoint]['span']
            break

    if firstGraph is None:
        print('ERROR: Data never propagated to leaf nodes. Review if there was a blockage in the workflow (all mode)')
        raise ValueError('Workflow permanently blocked - unable to calculate span')

    print('First Output Response All:', firstOutputResponse)
    leafNodes = [node for node in firstGraph.nodes() if firstGraph.in_degree(node) != 0 and firstGraph.out_degree(node) == 0]

    for n in leafNodes:
        try:
            print(n, firstGraph.nodes()[n]['totalSpan'])
        except KeyError:
            print('Route to node %s was blocked' % n)

    firstResponse = min([firstGraph.nodes()[n]['totalSpan'] for n in leafNodes if 'totalSpan' in firstGraph.nodes()[n]])
    print('First Output Repsonse Any:', firstResponse)

    #Output spans/latencies
    #emission time, leaf 1 span, leaf 2 span, total span, times ...
    m = experiment.utilities.data.Matrix(rows=list(zip(timePoints, [graphs[d]['span'] for d in timePoints])), headers=['emission-time', 'span'])
    for i, t in enumerate(timePoints):
        metrics = graphs[t]['resource-utilisation']
        if i == 0:
            for h in list(metrics.keys()):
                m.addColumn([0]*len(timePoints), header=h)

        m.setElements(metrics, i)

    print(m.csvRepresentation())

    return {
        'firstResponseAll': firstOutputResponse,
        'firstResponseAny': firstResponse,
        'profileMatrix': m,
        'blockages': blockages,
    }

    #Output rates
    #Can calcuate for t_o+(0.5*windot) to t-1-(0.5*window)
    #for t in averagePoints:
    #    rate = (spans[t][t+window] - spans[t][t-window])/(window*interval)