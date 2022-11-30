# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import print_function

import os
import shutil
import tempfile
import uuid

import reactivex
import pytest

import experiment.model.codes
import experiment.model.data
import experiment.model.errors
import experiment.model.frontends.dosini
import experiment.model.storage
import experiment.runtime.engine
import experiment.runtime.workflow

from .reactive_testutils import *

from typing import Dict, Any

# VV: Next 2 lines remove artificial delays in Engine between component becoming ready
# and launching its first task
experiment.runtime.engine.ENGINE_RUN_START_DELAY_SECONDS = 0.0
experiment.runtime.engine.ENGINE_LAUNCH_DELAY_SECONDS = 0.0

conf='''
[DEFAULT]
name=stage0

[Source]
executable=sleep
arguments=15
replicate=2
job-type=local

[Repeating]
#executable = python
#arguments= -c 'import time, os; time.sleep(5); os.listdir(\\"\\"\\"Source:ref\\"\\"\\")'
executable= ls
arguments = /tmp/
repeat-interval=5
job-type=local
check-producer-output=false

references=Source:ref

[Aggregating]
executable=ls
arguments= Repeating:ref
repeat-interval=5
aggregate=True
job-type=local
check-producer-output=false

references=Repeating:ref

'''

experimentConf = '''
[DEFAULT]
name='Test'
'''

@pytest.fixture(scope="function")
def output_dir():
    path = tempfile.mkdtemp()

    print(('Output dir is', path))
    yield path

    try:
        shutil.rmtree(path)
    except:
        pass


@pytest.fixture
def test_absolute_identifier():

    return {'identifier':'stage2.ClusterAnalysis', 'index':None, 'results':{'identifier':'stage2.ClusterAnalysis', 'namespace':'stage2', 'name':'ClusterAnalysis', 'index':2}}

@pytest.fixture
def test_relative_identifier():

    return {'identifier':'ClusterAnalysis', 'index':8, 'results':{'identifier':'stage8.ClusterAnalysis', 'namespace':'stage8', 'name':'ClusterAnalysis', 'index':8}}

@pytest.fixture(params=['absolute', 'relative'])
def test_identifier(test_relative_identifier, test_absolute_identifier, request):

    if request.param == 'absolute':
        test = test_absolute_identifier
    else:    
        test = test_relative_identifier

    return test        

@pytest.fixture(params=["ref", "copy", "link", "copyout"])
def reference_method(request):

    return request.param

@pytest.fixture (params=[None, "/filepath"])
def file_path(request):

    return request.param

@pytest.fixture 
def test_direct_reference(multicomponent_file_path, reference_method):
    test_identifier = {}
    test_identifier['results'] = {}

    test_identifier['identifier'] = "%s:%s" % (multicomponent_file_path, reference_method)
    test_identifier['results']['method'] = reference_method
    test_identifier['results']['identifier'] = os.path.split(multicomponent_file_path)[0]
    #The name will be the directory containing the file
    test_identifier['results']['name'] = os.path.split(multicomponent_file_path)[0]
    test_identifier['results']['filePath'] = os.path.split(multicomponent_file_path)[1].lstrip(os.path.sep)

    return test_identifier

@pytest.fixture
def multicomponent_file_path():

    return '/multi/component/path.txt'

@pytest.fixture
def test_reference(test_identifier, reference_method, file_path):

    if file_path is None:
        ref = "%s:%s" % (test_identifier['identifier'], reference_method)
    else:    
        ref = "%s%s:%s" % (test_identifier['identifier'], file_path, reference_method)

    test_identifier['results']['method'] = reference_method
    test_identifier['results']['filePath'] = file_path.lstrip(os.path.sep) if file_path is not None else file_path
    test_identifier['identifier'] = ref

    return test_identifier

@pytest.fixture
def test_multicomponent_path_reference(test_absolute_identifier, reference_method, multicomponent_file_path):

    if file_path is None:
        ref = "%s:%s" % (test_absolute_identifier['identifier'], reference_method)
    else:    
        ref = "%s%s:%s" % (test_absolute_identifier['identifier'], multicomponent_file_path, reference_method)

    test_absolute_identifier['results']['method'] = reference_method
    test_absolute_identifier['results']['filePath'] = multicomponent_file_path.lstrip(os.path.sep)
    test_absolute_identifier['identifier'] = ref

    return test_absolute_identifier

#Fixtures

@pytest.fixture
def stageConfig():
    import experiment.model.conf
    import uuid


    filename = os.path.join('/tmp', "%s.conf" % uuid.uuid4())

    with open(filename, 'w') as f:
        f.write(conf)

    cfg = experiment.model.frontends.dosini.FlowConfigParser()
    cfg.read([filename])
    yield cfg

    os.remove(filename)

@pytest.fixture
def experimentConfig():

    import configparser

    config = configparser.RawConfigParser(defaults={'name':'Test'})

    return config

@pytest.fixture
def packagedir(stageConfig,experimentConfig):

    directory = '/tmp/Test-%s.package' % uuid.uuid4()
    if os.path.exists(directory):
        shutil.rmtree(directory)

    #FIXME: decide what to do with stageConfigurations and experimentConfigurations
    stageConfigurations = [stageConfig]
    experimentConfigurations = {'default': experimentConfig}

    if stageConfigurations is not None:
        if os.path.isdir(directory):
            raise experiment.model.errors.InstanceLocationError(directory)
        else:
            os.mkdir(directory)
            configurationDirectory = os.path.join(directory, 'conf')
            os.mkdir(configurationDirectory)

            #Write experiment conf files
            for platform in list(experimentConfigurations.keys()):
                outfile = "experiment.conf" if platform == 'default' else "experiment.%d.conf" % platform
                with open(os.path.join(configurationDirectory, outfile), 'w') as f:
                    experimentConfigurations[platform].write(f)

            stageConfigurationDirectory = os.path.join(configurationDirectory, 'stages.d')
            os.mkdir(stageConfigurationDirectory)
            for i, conf in enumerate(stageConfigurations):
                path = os.path.join(stageConfigurationDirectory, 'stage%d.conf' % i)
                with open(path, 'w') as f:
                    conf.write(f)

            #Create variables dirs and write variables conf files
            variablesDir = os.path.join(configurationDirectory, 'variables.d')
            os.mkdir(variablesDir)
            os.mkdir(os.path.join(directory, 'data'))
            os.mkdir(os.path.join(directory, 'hooks'))
    else:
        if not os.path.isdir(directory):
            raise experiment.model.errors.InstanceLocationError(directory)

    expPackage = experiment.model.storage.ExperimentPackage.packageFromLocation(directory)

    yield expPackage

    try:
        shutil.rmtree(expPackage.location)
    except:
        pass


@pytest.fixture(scope='function')
def instancedir(packagedir):

    #Ensure we are in a dir that exists before starting to create a new dir
    os.chdir(os.path.expanduser('~'))
    w = experiment.model.storage.ExperimentInstanceDirectory.newInstanceDirectory('/tmp',
                                                                                  package=packagedir)

    w.touch = True

    yield w

    # VV: Try to cleanup, don't crash and burn if the instance dir is not there.
    #     This should be safe because of the scope option passed to the fixture
    #     decorator. This fixture is invoked once for each test function that uses it
    try:
        shutil.rmtree(w.location)
    except OSError:
        pass

@pytest.fixture
def experimentInstance(instancedir):

    return experiment.model.data.Experiment(instancedir, is_instance=True)



@pytest.fixture
def componentSpecification(componentName, experimentInstance):

    return experimentInstance.findJob(0, componentName)


@pytest.fixture
def engine(componentSpecification):

    e = experiment.runtime.engine.Engine.engineForComponentSpecification(componentSpecification)
    yield e
    e.kill()

    #Shutdown the engine if it isn't already
    logger.info('Tearing down %s' % componentSpecification.identification)
    if not e.isShutdown:
        logger.info('Engine not shutdown - doing so now')
        logger.warning(e.state)
        stateUpdatesCompletion = []
        emissions = []
        stateUpdatesCompletedFunction = CompletedClosure(stateUpdatesCompletion)
        b = e.stateUpdates.subscribe(on_next=GatherClosure(emissions),
            on_completed=stateUpdatesCompletedFunction)
        #kill is asynchronous so need to wait until the engine is dead before it is shutdown
        def is_still_alive(emissions):
            emission_engine = emissions[-1]
            emissions: Dict[str, Any] = emission_engine[0]
            engine: experiment.runtime.engine.Engine = emission_engine[1]

            isAlive = emissions.get('isAlive', engine.isAlive())
            logger.info(f"Engine {engine.job.reference} isAlive {isAlive}")
            return isAlive

        if e.isAlive():
            logger.info('Wait on isAlive=False')
            WaitOnEvent(emissions, event=is_still_alive)

        logger.info('Shutting down now')
        e.shutdown()
        logger.info('Waiting for shutdown to propagate')
        WaitOnCompletion(stateUpdatesCompletion)
        b.dispose()
        logger.info('Complete %s' % stateUpdatesCompletion)

@pytest.fixture
def nxGraph(experimentInstance):

    return experimentInstance.graph


@pytest.fixture
def components(experimentInstance):
    components = []
    for stage in experimentInstance.stages():
        for spec in stage.jobs():
            component = experiment.runtime.workflow.ComponentState(spec, experimentInstance.experimentGraph)
            components.append(component)

    yield components
    logger.debug("!!!!!!! Tearing down @components !!!!!!!")

    #Create observable for components finished notifications
    aliveComponents =  [c for c in components if c.isAlive() is True]
    if len(aliveComponents) > 0:
        notifyFinished = reactivex.merge(*[c.notifyFinished for c in aliveComponents])
        def FinishedLog(e):
            logger.warning("COMPONENT CLEANUP %s" % str(e))

        completed = []
        #Subscribe to notify finished to check for FAILED components
        notifyFinished.subscribe(on_next=FinishedLog,
                                 on_error=FinishedLog,
                                 on_completed=CompletedClosure(completed))

        for component in aliveComponents:
            logger.info('SENDING FINISH TO: %s %s %s' % (component.name, component.state, component.isAlive()) )
            component.finish(experiment.model.codes.FINISHED_STATE)

        WaitOnCompletion(completed)
        logger.info('FINAL COMPONENT CLEANUP: Completed is %s' % completed)
        assert completed[0]
