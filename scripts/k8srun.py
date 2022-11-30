#
# coding=UTF-8
# Copyright IBM Inc. 2019. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import print_function

import logging
import pprint
import time
import uuid

import experiment.model.codes
import experiment.model.executors
import experiment.runtime.backend_interfaces.k8s
import experiment.runtime.errors

FORMAT = '%(levelname)-9s %(threadName)-30s %(name)-30s: %(funcName)-20s %(asctime)-15s: %(message)s'
logging.basicConfig(format=FORMAT)
rootLogger = logging.getLogger()
rootLogger.setLevel(20)

#TO TEST BEHAVIOUR ON CONTAINER FAILURE CHANGE ARGS TO $PWD
executor = experiment.model.executors.Command(executable="ls",
                                                    arguments="-lth .",
                                                    workingDir="/tmp/test",
                                                    environment={},
                                                    resolvePath=False,
                                                    resolveShellSubstitutions=False)

options = {}
#TO TEST BEHAVIOUR ON PULL FAILURE MANGLE THIS
#Required
options['k8s-image'] = "hermes-cluster.icp:8500/flow/flow"

#optional
options['k8s-walltime'] = 60

#Flow specific options
options['k8s-flow-id'] = "flow-%s" % str(uuid.uuid4())
options['k8s-name'] = "flow-test-%s" % str(uuid.uuid4())

#Options we should be able to get from flow
options['k8s-namespace'] = "default"
options['k8s-image-pull-secret'] = 'regcred'
#+ volume options
options['k8s-claim-name'] = "vv-synthetic-claim"
options['k8s-volume-name'] = "vv-synthetic-workflow-volume"


#Options that can be read from local config if availabe
#options['k8s-api-key-var'] = 'HERMES_API_KEY'
options['k8s-host'] = 'http://localhost:8080'

#MAPPING - TO BE MOVED TO expeirment.backend.KubernetesTaskGenerator

options_k8s = {}
options_k8s['configuration'] = {
    'host': options['k8s-host'],
    'api-key-var': options.get('k8s-api-key-var')
}
options_k8s['job'] = {
    'namespace': options['k8s-namespace'],
    'name': options['k8s-name'],
    'flow-id': options['k8s-flow-id'],
    'walltime': options['k8s-walltime']
}

options_k8s['job']['pod'] = {'imagePullSecrets': options['k8s-image-pull-secret']}
options_k8s['job']['pod']['containers'] = {'image': options['k8s-image']}

options_k8s['job']['pod']['volumes'] = [
    {
        'name': options['k8s-volume-name'],
        'type': 'persistent_volume_claim',
        'options': {
            'claim_name': options['k8s-claim-name']
        }
    }
]
options_k8s['job']['pod']['containers']['volumemounts'] = [
    {
        'name': options['k8s-volume-name'],
        'mount-path': '/tmp/flow'
    }
]

print('K8s options')
pprint.pprint(options_k8s)

try:
    task = experiment.runtime.backend_interfaces.k8s.NativeScheduledTask(executor=executor, options=options_k8s, resourceRequest={'numberThreads':1, 'threadsPerCore':1})
except experiment.runtime.errors.JobLaunchError as e:
    print(e)
else:
    try:
        poll = True
        count = 0
        while poll:
            status =  task.status
            print('Updated Status:', status)
            print('Count', count)
            if status in [experiment.model.codes.FAILED_STATE, experiment.model.codes.FINISHED_STATE]:
                break
            else:
                time.sleep(10.0)
                count +=1
                if count == 30:
                    task.terminate()

                if count == 36:
                    poll = False
    except KeyboardInterrupt as e:
        print(e)
        task.terminate()
    finally:
        print('Task Final Status:', task.status)
        print('Task Exit Reason:', task.exitReason)
        print('Task Return Code:', task.returncode)
        task.terminate()

    print("Performance Information")
    print(task.performanceInfo.csvRepresentation())
