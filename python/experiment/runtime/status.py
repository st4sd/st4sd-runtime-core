#
# coding=UTF-8
# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

from __future__ import annotations

import collections
import logging
import os
import sqlite3
import threading
import traceback
from typing import Optional, Any, Callable, Dict, List

import reactivex
import reactivex.operators as op
import reactivex.scheduler

import experiment.utilities.data
import experiment.model.codes
import experiment.model.conf
import experiment.model.errors
import experiment.model.storage
import experiment.model.executors
import experiment.model.graph
import experiment.model.data
import experiment.model.frontends.flowir
import experiment.runtime.errors
import experiment.runtime.monitor
import experiment.runtime.engine
import experiment.runtime.workflow

class StatusDB(object):

    '''An interface to a database for storing component status during execution

    Backed by an SQLLite db.
    The underlying db contains three tables,
    - component state
    - non-repeating engine state
    - repeating engine state

    You must call close() to dispose of instances of this class

    '''

    #Must be in same order as table headers

    allowedEngineKeys = [
        'reference',
        'isAlive',
        'backend',
        'creationDate',
        'isWaitingOnOutput',
        'outputWaitTime',
        'isRunning',
        'runDate',
        'lastTaskLaunchDate',
        'lastTaskRunTime',
        'lastTaskRunState',
        'lastTaskFinishedDate',
        'lastTaskExitCode',
        'lastTaskExitReason',
        'engineExitCode',
        'engineExitReason',
        'schedulerId',
    ]

    allowedRepeatingEngineKeys = allowedEngineKeys[:]
    allowedRepeatingEngineKeys += [
        'repeatInterval',
        'lastKernelLaunchDate',
        'timeToNextKernelLaunch',
        'numberTaskLaunches',
        'timeSinceLastTaskLaunch']

    allowedComponentKeys = [
        'reference',
        'consumesFrom',
        'isAlive',
        'state',
        'engine']

    def __init__(self, location):

        '''Initialises a database for storing status at location

        If the database doesn't exist it is created

        Param:
            location: A path. Usually inside an experiment'''

        self.log = logging.getLogger('db.status')
        #For use only in transaction thread
        self.conn = None
        self.cursor = None
        self.location = location

        self.transactionMap = {'component': self.setComponentStatus,
                               'engine': self.setEngineStatus,
                               'repeatingEngine': self.setEngineStatus}

        conn = sqlite3.connect(self.location)
        cursor = conn.cursor()

        self.log.debug('Initialise with database at %s' % self.location)
        self.log.debug('Creating tables')
        try:
            cursor.execute('''CREATE TABLE componentState
                            (id integer primary key, reference text unique,
                            consumesFrom text, isAlive text, state text, engine text)''')

            cursor.execute('''CREATE TABLE engineState
                (id integer primary key, reference text unique, isAlive text, backend text, creationDate text,
                 isWaitingOnOutput integer, outputWaitTime real,
                 isRunning integer,  runDate text, lastTaskLaunchDate text,
                 lastTaskRunTime real, lastTaskRunState text, lastTaskFinishedDate text, lastTaskExitCode integer, 
                 lastTaskExitReason text, engineExitCode integer, engineExitReason text, schedulerId text)''')

            cursor.execute('''CREATE TABLE repeatingEngineState
                (id integer primary key, reference text unique, isAlive text, backend text, creationDate text,
                 isWaitingOnOutput integer, outputWaitTime real,
                 isRunning integer,  runDate text,
                 lastTaskLaunchDate text, lastTaskRunTime real, lastTaskRunState text,
                 lastTaskFinishedDate text, lastTaskExitCode integer, lastTaskExitReason text,
                 engineExitCode integer, engineExitReason text, schedulerId text,
                 repeatInterval integer, lastKernelLaunchDate integer, timeToNextKernelLaunch real,
                 numberTaskLaunches integer, timeSinceLastTaskLaunch real)''')
        except sqlite3.Error as e:
            self.log.info(e)
            self.log.debug('Encountered warning - assuming tables already exist')
        else:
            conn.commit()
        finally:
            cursor.close()
            conn.close()

        # VV: The transactional queue will set this event when it terminates
        self.event_finished = threading.Event()

        def on_completed(err: Exception | None = None):
            if err:
                self.log.warning(f"Exception {err} while processing transaction. Traceback: {traceback.format_exc()}")
            else:
                self.log.debug("Status db transaction queue got terminate message")

            try:
                self.log.debug("Shutting down the transaction subject now")
                self._transaction_subject.on_completed()
            except Exception as e:
                self.log.warning(f"Unable to shutdown transaction subject due to {type(e)}: {e} - will ignore error")

            try:
                self._closeConnection()
            except Exception as e:
                self.log.warning(f"Unable to close connection due to {type(e)}: {e} - will ignore error")
            
            self.log.debug("Reporting that the statusDB is now complete")
            self.event_finished.set()

        self._serialise_thread = reactivex.scheduler.ThreadPoolScheduler(1)
        self._emit_pool = reactivex.scheduler.ThreadPoolScheduler(1)

        self._transaction_subject =  reactivex.subject.Subject()
        self.transanctionDisposable = self._transaction_subject.pipe(
            op.observe_on(self._serialise_thread),
            op.take_while(lambda what: what is not None),
        ).subscribe(on_next=lambda x: self._processUpsertTransaction(x),
                  on_error=on_completed,
                  on_completed=on_completed)

    def _enqueue_item(self, what: Dict[str, Any] | None):
        # VV: The convention is that if @what is `None` the disposable will complete after it processes it
        def to_emit(what=what):
            def closure(what=what, self=self):
                self._transaction_subject.on_next(what)
            return closure

        # VV: Causes the database to injest `what` asynchronously
        reactivex.just(what).pipe(
            op.observe_on(self._emit_pool),
        ).subscribe(on_next=to_emit(what))

    def close(self):
        self._enqueue_item(None)

    def _closeConnection(self):

        self.log.info('Received completed message from transaction queue')
        if self.conn is not None:
            self.log.info("Shutting down connection")
            try:
                self.cursor.close()
                self.conn.close()
            except Exception as error:
                self.log.warning('Exception when shutting down status db connection: %s' % error)

        self.log.info('Connection closed')

    def _processUpsertTransaction(self, e):

        '''Process an Upsert transaction

        e should contain type of transaction.

        Ignore empty `e` as signal to cycle queue'''

        self.log.debug("Processing transaction from queue: %s" % e)
        if len(e) != 0:
            self._upsert(**e)

    def _upsert(self,  table, allowedKeys, data):

        '''Performs an upsert for reference in table

        Upsert == UPDATE OR INSERT

        Params:
            reference: A component reference containing stage and name. Ids the row to upsert
            table: The name of the table to upsert
            data: The data to upsert
        '''

        data = {k:data[k] for k in list(data.keys()) if k in allowedKeys}

        if 'reference' not in data:
            self.log.warning('UPSERT requires component reference string')
            raise ValueError

        reference = data['reference']

        if self.conn is None:
            self.conn = sqlite3.connect(self.location)
            self.cursor = self.conn.cursor()

        updateString = ",".join(["%s='%s'" % (x, data[x]) for x in data])
        updateQuery = '''UPDATE %s SET %s WHERE reference='%s' ''' % (table, updateString, reference)
        self.log.debug("Trying update query %s" % updateQuery)

        try:
            self.cursor.execute(updateQuery)
        except sqlite3.Error as e:
            self.log.warning(f'Error during update of query {updateQuery}: {e}')

        # Check the update worked - if not insert
        if self.cursor.rowcount in [0,-1]:
            keys = list(data.keys())
            insertQuery = '''INSERT INTO %s (%s) VALUES (%s)''' % \
                          (table, ",".join(keys), ",".join(["'%s'" % x for x in [data[k] for k in keys]]))

            self.log.debug("Update didn't work. Trying insert %s" % updateQuery)
            try:
                self.cursor.execute(insertQuery)
            except sqlite3.Error as e:
                self.log.warning('Error during insert: %s' % e)
        else:
            self.log.debug('Update worked')

        self.conn.commit()


    def setEngineStatus(self, stateDictionary):

        '''Sets/Updates the status of a components engine.

        Params:
            stateDictionary: The engines stateDictionary

            Must include the key 'sourceType' indicating the type of engine.
            Must include the correct keys for engine type - see method body for details.
            '''

        self.log.debug('Received engine status %s. Adding to transaction queue' % stateDictionary)

        if stateDictionary['sourceType'] == 'engine':
            self._enqueue_item({'table':'engineState',
                                       'allowedKeys': StatusDB.allowedEngineKeys,
                                       'data':stateDictionary})
        elif stateDictionary['sourceType'] == 'repeatingEngine':
            self._enqueue_item({'table': 'repeatingEngineState',
                                       'allowedKeys': StatusDB.allowedRepeatingEngineKeys,
                                       'data': stateDictionary})

    def setComponentStatus(self, stateDictionary):

        '''Sets/Updates the status of a components'''

        self.log.debug('Received component status %s. Adding to transaction queue' % stateDictionary)
        self._enqueue_item({'table': 'componentState',
                                   'allowedKeys': StatusDB.allowedComponentKeys,
                                   'data': stateDictionary})

    def getComponentStatus(self, reference):

        '''Returns the status of a components - including engine status

        Parameter:
            reference: A component reference

        Returns:
            A dictionary with the components status.'''

        #Get component state

        #Get engine state

        conn = sqlite3.connect(self.location)
        cursor = conn.cursor()

        # VV: Don't swallow errors here - we're trying to retrieve results from the DB
        cursor.execute('''SELECT * from componentState WHERE reference='%s' ''' % reference)

        retval = cursor.fetchall()

        componentResults = collections.OrderedDict(list(zip(['rowid'] + StatusDB.allowedComponentKeys, retval[0])))

        self.log.debug('Initial component query returned %s' % componentResults)

        if 'repeatingEngine' in retval[0]:
            headers = ['rowid'] + StatusDB.allowedRepeatingEngineKeys
            table = 'repeatingEngineState'
        else:
            headers = ['rowid'] + StatusDB.allowedEngineKeys
            table = 'engineState'

        self.log.debug('Table %s. Headers %s' % (table, headers))

        try:
            selectString = '''SELECT * from %s WHERE reference='%s' ''' % (table, reference)
            self.log.debug(selectString)
            cursor.execute('''SELECT * from %s WHERE reference='%s' ''' % (table, reference))
        except sqlite3.Error as e:
            self.log.warning(e)

        retval = cursor.fetchall()
        engineResults = collections.OrderedDict(list(zip(headers, retval[0])))
        self.log.debug('Engine query returned %s' % engineResults)

        cursor.close()
        conn.close()

        return {'component':componentResults, 'engine':engineResults}

    def executionDataForComponent(self, componentName):

        '''Returns the execution history for the given component

        Args:
            componentName (str). The name of the component as a reference string

        Returns:
            experiment.utilities.data.Matrix: A matrix where each row corresponds to an execution of the component
            or None if no execution data present

        The matrix will have the following columns
            execution-date: The date the component was executed as a string (parseable with dateutil.parser
                without format info)
            execution-time: The time the component ran for as a string (parseable with dateutil.parser without
                format info)
            exit-code: 0 if execution was successful 1 otherwise

            Currently execution-date is the date the component was submitted to the backend - it does not queuing time
            execution-time is the time from this submission to return - it does include queuing time
            exit-code currently only indicates if the job could be launched (data available).
                It does not capture successful execution

            '''

        #Find the working directory of the component
        #FIXME: This is a hack
        #We don't have access to the experiment in this class or from the typical caller (in graph.py)
        #Hence no access to storage for a given component
        #The hack is to
        #A) Assume the location of the db is in the top level of the instance
        #B) Components working dirs are at `stages/stage$X/$name
        #   where the component name is of form stage$X.$name

        stageIndex, componentName, hasIndex = experiment.model.conf.ParseProducerReference(componentName)

        if not hasIndex:
            raise ValueError('Unable to determine stage from provided component name %s' % componentName)

        workingDir = 'stages/stage%d/%s' % (stageIndex, componentName)
        workingDir = os.path.join(os.path.split(self.location)[0], workingDir)
        performanceFile = os.path.join(workingDir, 'component_performance.csv')

        try:
            performanceData = experiment.utilities.data.matrixFromCSVFile(performanceFile)
        except IOError as error:
            performanceData = None
        else:
            #Edit the headers
            #The current standard headers are
            # 'component-launch-time',
            #  'can-consume',
            #  'output-available',
            #  'delta-last-task',
            #   'task-run-time',
            #   'data-produced',
            #   'task-production-rate',
            #   'component-production-rate'
            # And then the backend parameters from lsf
            # - wait-time
            # - pend-start-time
            # - pre-execution-time
            # - main-execution-time
            # - completion-time
            # - transfer-out-time-estimate
            # - total-time
            #
            #We want
            #'execution-date', 'execution-time', 'exit-code'
            #'execution-date' == 'component-launch-time' - (format "%d%m%y-%H%M%S", dateutil parses it correctly)
            #    - Could also set it to 'component-launch-time' + (wait)
            #'execution-time' == 'task-run-time' - although we have a number of options here also
            #'exit-code' == use 'can-consume' and 'output-available' although this is not accurate.
            import datetime

            performanceData.addColumn(performanceData.column('component-launch-time'), header='execution-date')
            performanceData.addColumn(["%s" % datetime.timedelta(seconds=el)
                                       for el in performanceData.column('task-run-time')],
                                       header='execution-time')
            new_rows = []
            headers = performanceData.columnHeaders()

            # VV: Convert `None` exit-code to `1`
            for row_idx in range(performanceData.numberOfRows()):
                elements = performanceData.getElements(row_idx)
                if not elements['can-consume'] or not elements['output-available']:
                    elements['exit-code'] = 1

                # VV: Ensure that `exit-code` is always an integer (below will also convert `None` to 1)
                if str(elements['exit-code']).lower() == 'none':
                    elements['exit-code'] = 1
                else:
                    elements['exit-code'] = int(elements['exit-code'])

                row = [elements[header] for header in headers]
                new_rows.append(row)

            performanceData = experiment.utilities.data.Matrix(rows=new_rows, headers=headers, name=performanceData.name())

        return performanceData

    def get_components_state(self):
        """Returns state of components

        Returns
            A dictionary whose structure is
            stageIndex:
                componentName: component state (a la experiment.codes.*_STATE)
        """
        workflow_status = self.getWorkflowStatus()
        comp_states = {}
        for stage in workflow_status:
            stage_status = workflow_status[stage]
            comp_states[stage] = {}
            for key in ['engine', 'repeatingEngine']:
                if key in stage_status:
                    matrix = stage_status[key]['componentStates']  # type: experiment.utilities.data.Matrix
                    idx_state = matrix.indexOfColumnWithHeader('state')
                    idx_reference = matrix.indexOfColumnWithHeader('reference')
                    for row in matrix.matrix:
                        ref = row[idx_reference]
                        state = row[idx_state]

                        _, name, _ = experiment.model.frontends.flowir.FlowIR.ParseProducerReference(ref)
                        comp_states[stage][name] = state

        return comp_states

    def getWorkflowStatus(self, json_friendly=False):

        '''Returns all recorded status entries

        For each stage components are divided into those with repeating engines or not.
        For each engine type the component and engine states are kept separate due to
        header conflict (currently)

        Arguments:
            json_friendly(bool): If set to True will return CSV representations of matrices, instead of the actual
                experiment.utilities.data.Matrix object

        Returns:
            A dictionary whose structure is
            stageIndex:
              engine:
                    - componentStates: experiment.utilities.data.Matrix OR CSV of matrix
                    - engineStates: experiment.utilities.data.Matrix OR CSV of matrix
              repeatingEngine:
                    - componentStates: experiment.utilities.data.Matrix OR CSV of matrix
                    - engineStates: experiment.utilities.data.Matrix OR CSV of matrix
        '''

        conn = sqlite3.connect(self.location)
        cursor = conn.cursor()
        retval = {}

        # VV: Don't swallow errors here - we're trying to retrieve results from the DB
        cursor.execute('''SELECT reference from componentState''')

        data = cursor.fetchall()
        componentList = [r[0] for r in data]

        cursor.close()
        conn.close()

        self.log.debug('Component list %s' % componentList)

        #Get all references from component list
        for ref in componentList:
            d = self.getComponentStatus(ref)
            ref = d['component']['reference']
            engine = d['component']['engine']
            stageIndex, name, hasIndex = experiment.model.conf.ParseProducerReference(ref)

            if stageIndex not in retval:
                retval[stageIndex] = {}

            if engine not in retval[stageIndex]:
                retval[stageIndex][engine] = {'componentStates': experiment.utilities.data.Matrix(),
                                              'engineStates': experiment.utilities.data.Matrix()}

            m = retval[stageIndex][engine]['componentStates']  # type: experiment.utilities.data.Matrix
            m.addElements(d['component'])

            m = retval[stageIndex][engine]['engineStates']  # type: experiment.utilities.data.Matrix
            m.addElements(d['engine'])

        if json_friendly:
            for stageIndex in retval:
                for engine in retval[stageIndex]:
                    for k in retval[stageIndex][engine]:
                        csv = retval[stageIndex][engine][k].csvRepresentation(sep='; ').rstrip()
                        retval[stageIndex][engine][k] = csv

        return retval

    @classmethod
    def pretty_print_status_details_report(
            cls,
            data: Dict[str, Any],
            stages: List[int] | None = None,
            categories: List[str] | None = None,
            no_defaults: bool = False,
            filters: List[str] = None) -> str:
        """Generates a detailed status report for workflow, can apply filtering

        Args:
            data: A dictionary that was generated by experiment.service.db.getWorkflowStatus()
            stages: An optional list of stage indices, when provided status report will
                only contain entries for components in those stages
            categories: Optional list of categories for which information is output.
                Available categories are producer, task, and engine
            no_defaults: Omits output columns in the  categories - useful if there are too many columns
                being output
            filters: Set filters that limit the components output to those exhibiting certain behaviour.
                With each use of this option an additional filter can be specified. Available filters are:
                failed, interesting, engine-exited, blocked-output, blocked-resource, not-executed, executing, and all.
                Default filters applied are: failed, engine-exited, blocked-output, blocked-resource, and not-executed.
        Returns:
         A string which can either be a pretty report OR the text "No stage data in status database"
        """
        if len(list(data.keys())) == 0:
            return "No stage data in status database"

        def SetColumnSelectionFilters(selectors, selection):
            '''

            Args:
                selectors: A dictionary of category:functions pairs.
                    The function returns entries that match the category defintion
                selection: A list of categories to select from

            Returns:
                A list of column headers which match the given selection
            '''

            if 'all' in selection:
                columnSelectionFilters = list(selectors.values())
            else:
                columnSelectionFilters = []
                columnSelectionFilters.extend([selectors[sel] for sel in selection])

            selectedColumns = []
            for selector in columnSelectionFilters:
                selectedColumns = selector(selectedColumns)

            return selectedColumns

        statusColumns = [
            {'name': 'reference', 'category': 'component', 'filters': ['label']},
            {'name': 'consumesFrom', 'category': 'component', 'filters': ['producer']},
            {'name': 'isAlive', 'category': 'component', 'filters': ['producer']},
            {'name': 'state', 'category': 'component', 'filters': ['standard']},
            {'name': 'engine', 'category': 'component', 'filters': []},
            # Omit isAlive on engine as component has same column and its aliveness is deducible from other columns
            #  {'name':'isAlive', 'category':'engine', 'filters':['standard']},
            {'name': 'backend', 'category': 'engine', 'filters': ['standard']},
            {'name': 'creationDate', 'category': 'engine', 'filters': ['engine']},
            {'name': 'runDate', 'category': 'engine', 'filters': ['engine']},
            {'name': 'isWaitingOnOutput', 'category': 'engine', 'filters': ['standard', 'engine']},
            {'name': 'outputWaitTime', 'category': 'engine', 'filters': ['engine']},
            {'name': 'isRunning', 'category': 'engine', 'filters': ['engine']},
            {'name': 'engineExitCode', 'category': 'engine', 'filters': ['engine']},
            {'name': 'engineExitReason', 'category': 'engine', 'filters': ['standard']},
            {'name': 'repeatInterval', 'category': 'repeating-engine', 'filters': ['standard-repeating']},
            {'name': 'lastKernelLaunchDate', 'category': 'repeating-engine', 'filters': ['engine']},
            {'name': 'timeToNextKernelLaunch', 'category': 'repeating-engine', 'filters': ['standard-repeating']},
            {'name': 'numberTaskLaunches', 'category': 'repeating-engine', 'filters': ['standard-repeating']},
            {'name': 'timeSinceLastTaskLaunch', 'category': 'repeating-engine', 'filters': ['engine']},
            {'name': 'lastTaskLaunchDate', 'category': 'task', 'filters': ['task']},
            {'name': 'lastTaskRunTime', 'category': 'task', 'filters': ['standard', 'task']},
            {'name': 'lastTaskRunState', 'category': 'task', 'filters': ['standard', 'task']},
            {'name': 'lastTaskFinishedDate', 'category': 'task', 'filters': ['task']},
            {'name': 'lastTaskExitCode', 'category': 'task', 'filters': ['task']},
            {'name': 'lastTaskExitReason', 'category': 'task', 'filters': ['task']},
            {'name': 'schedulerId', 'category': 'task', 'filters': ['task']},
        ]
        ret = []

        stages = stages if stages is not None else []
        categories = list(categories) if categories is not None else []
        if 'label' not in categories:
            categories.append('label')
        if not no_defaults:
            categories.extend(('standard', 'standard-repeating'))

        filterStages = False if len(stages) == 0 else True
        select = lambda x: [el['name'] for el in statusColumns if x in el['filters']]

        selectors = collections.OrderedDict()
        selectors['label'] = lambda x: x + select('label')
        selectors['standard'] = lambda x: x + select('standard')
        selectors['standard-repeating'] = lambda x: x + select('standard-repeating')
        selectors['engine'] = lambda x: x + select('engine')
        selectors['task'] = lambda x: x + select('task')
        selectors['producer'] = lambda x: x + select('producer')

        selectedColumns = SetColumnSelectionFilters(selectors, categories)

        # Row selection filters
        # Default filters are failed, non-repeating exited engine, blocked (includes neverExecuted)
        failedComponents = lambda x: x.filter(columnHeader='state',
                                              filterFunction=lambda y: y == experiment.model.codes.FAILED_STATE)
        interestingComponents = lambda x: x.filter(columnHeader='state',
                                                   filterFunction=lambda y: y not in [experiment.model.codes.FINISHED_STATE,
                                                                                      experiment.model.codes.RUNNING_STATE])
        engineExited = lambda x: x.filter(columnHeader='engineExitReason',
                                          filterFunction=lambda y: y not in [
                                              experiment.model.codes.exitReasons['Success'],
                                                                             'None'])
        waiting = lambda x: x.filter(columnHeader='isWaitingOnOutput',
                                     filterFunction=lambda y: y == True)
        neverExecuted = lambda x: x.filter(columnHeader='numberTaskLaunches',
                                           filterFunction=lambda y: y == 0)
        resource = lambda x: x.filter(columnHeader='lastTaskRunState',
                                      filterFunction=lambda y: y == experiment.model.codes.RESOURCE_WAIT_STATE)
        executing = lambda x: x.filter(columnHeader='lastTaskRunState',
                                       filterFunction=lambda y: y == experiment.model.codes.RUNNING_STATE)
        notFinished = lambda x: x.filter(columnHeader='state',
                                         filterFunction=lambda y: y not in [experiment.model.codes.FINISHED_STATE,
                                                                            experiment.model.codes.FAILED_STATE])
        selectedFilters = None
        all_filters = {'failed': failedComponents,
                       'not-finished': notFinished,
                       'engine-exited': engineExited,
                       'not-executed': neverExecuted,
                       'interesting': interestingComponents,
                       'blocked-output': waiting,
                       'blocked-resource': resource,
                       'executing': executing}
        filters = filters if filters is not None else ['failed', 'engine-exited', 'blocked-output',
                                                       'blocked-resource', 'not-executed']
        if 'all' not in filters:
            selectedFilters = [(el, all_filters[el]) for el in filters]

        for stageIndex in list(data.keys()):
            if not filterStages or (filterStages and stageIndex in stages):
                ret.extend(['', "%s STAGE %s %s" % (10 * "=", stageIndex, 10 * "="), ''])

                for engineType in data[stageIndex]:
                    value = data[stageIndex][engineType]

                    # Combine the selected columns of the matrices
                    componentInfo = value['componentStates']
                    engineInfo = value['engineStates']

                    combinedTable = experiment.utilities.data.Matrix()
                    for c in [x for x in selectedColumns if x in componentInfo.columnHeaders()]:
                        if c not in combinedTable.columnHeaders():
                            combinedTable.addColumn(componentInfo.column(c), header=c)

                    for c in [x for x in selectedColumns if x in engineInfo.columnHeaders()]:
                        if c not in combinedTable.columnHeaders():
                            combinedTable.addColumn(engineInfo.column(c), header=c)
                    final_table = combinedTable if selectedFilters is None else None
                    # Apply filters
                    if selectedFilters is not None:
                        ret.append('Filtering components using engine-type: %s' % engineType)
                        for filterType, filterFunction in selectedFilters:
                            # FIXME: Hack for the moment as only repeating engine records numberTaskLaunches
                            if engineType == 'engine' and filterType == 'not-executed':
                                continue

                            filteredTable = filterFunction(combinedTable)  # type: Optional[experiment.utilities.data.Matrix()]
                            if filteredTable:
                                final_table = filteredTable
                    else:
                        ret.append('Components using engine-type: %s' % engineType)

                    if final_table is None:
                        continue
                    ret.append(final_table.csvRepresentation().rstrip())

        return '\n'.join(ret)

    def get_detailed_status_report(self, stages=None, categories=None, no_defaults=False, filters=None):
        """Generates a detailed status report for workflow, can apply filtering

        Arguments:
            stages(List[int]): An optional list of stage indices, when provided dictionary will
                only contain entries for components in those stages
            categories(List[str]): Optional list of categories for which information is output.
                Available categories are producer, task, and engine
            no_defaults(bool): Omits output columns in the  categories - useful if there are too many columns
                being output
            filters(List[str]): Set filters that limit the components output to those exhibiting certain behaviour.
                With each use of this option an additional filter can be specified. Available filters are:
                failed, interesting, engine-exited, blocked-output, blocked-resource, not-executed, executing, and all.
                Default filters applied are: failed, engine-exited, blocked-output, blocked-resource, and not-executed.
        Returns
         A string which can either be a pretty report OR the text "No stage data in status database"
        """
        data = self.getWorkflowStatus()
        return self.pretty_print_status_details_report(data, stages=stages, categories=categories,
                                                       no_defaults=no_defaults, filters=filters)

    def monitorComponent(self, component):
        # type: ("experiment.workflow.ComponentState") -> Any

        self.setComponentStatus(component.stateDictionary)
        self.setEngineStatus(component.engine.stateDictionary)

        def on_completed_closure(component):
            # type: (experiment.runtime.workflow.ComponentState) -> Callable[[Any], Any]
            def on_completed():
                self.setComponentStatus(component.stateDictionary)
                self.setEngineStatus(component.engine.stateDictionary)

                self.log.log(19,
                             'Received completed message from component %s status' % component.specification.reference)

            return on_completed

        #Components emit component and engine updates
        return component.combinedStateUpdates.\
            subscribe(on_next=lambda x: self.transactionMap[x[0]['sourceType']](x[0]),
                on_error=lambda e: self.log.warning('Error when monitoring component %s: %s. Traceback: %s' % (
                    component.specification.reference, e, traceback.format_exc())),
                on_completed=on_completed_closure(component))

    def monitorEngine(self, engine):
        # type: (experiment.runtime.engine.Engine) -> Any
        self.setEngineStatus(engine.stateDictionary)

        def on_completed_closure(engine):
            def on_completed():
                self.setEngineStatus(engine.stateDictionary)
                self.log.log(19, 'Received completed message from engine status %s' % engine.job.reference)
            return on_completed

        #We could also push this up to the component so it emits (self, engine) states until finished
        #TODO: Only subscribe as long as the components state is not finished
        return engine.stateUpdates.\
            subscribe(on_next=lambda x: self.setEngineStatus(x[0]),
                on_error=lambda e: self.log.warning('Received error from engine status observable %s: %s' % (
                    engine.job.reference, e)),
                on_completed=on_completed_closure(engine))

