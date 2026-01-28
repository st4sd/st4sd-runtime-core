#! /usr/bin/env python
# coding=UTF-8

# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis

import ast
import datetime
import getpass
import json
import os
import pprint
import time
from pathlib import Path
from typing import List

import yaml
from kubernetes import config, client
from kubernetes.client.rest import ApiException

K8S_WORKFLOW_GROUP = os.environ.get("K8S_WORKFLOW_GROUP", "st4sd.ibm.com")
K8S_WORKFLOW_VERSION = os.environ.get("K8S_WORKFLOW_VERSION", "v1alpha1")
K8S_WORKFLOW_PLURAL = os.environ.get("K8S_WORKFLOW_PLURAL", "workflows")


def discover_instance_dir(shadow_base_dir, workdir, last_known_instance_dir):
    from_env_var = os.environ.get('INSTANCE_DIR_NAME')
    if from_env_var is not None:
        return os.path.join(workdir, from_env_var)

    # VV: Flow now creates exactly 1 shadow directory per experiment, but it used to create 2.
    # Determining the instance dir involves picking the most recent shadow dir under base_dir
    try:
        instances = os.listdir(shadow_base_dir)
        if len(instances) == 0:
            # VV: the shadow-dir is deleted once flow terminates, reuse the last known instance_dir if
            #     there're no shadow dirs but there used to be at least one
            return last_known_instance_dir
        else:
            instance_id = sorted(instances)[-1]
            working_instance = Path(instance_id).with_suffix('.instance')
            return working_instance
    except FileNotFoundError:
        return None
    except Exception as e:
        print("Could not list instance directories under %s. Error: %s" % (base_dir, e))
        return None


def get_elaunch_exit_code(pod_name, namespace):
    api = client.CoreV1Api(client.ApiClient())
    status = api.read_namespaced_pod(pod_name, namespace).status  # type: client.models.V1PodStatus
    cont_status = status.container_statuses  # type: List[client.models.V1ContainerStatus]

    for c in cont_status:
        if c.name == 'elaunch-primary':
            elaunch_status = c  # type: client.models.V1ContainerStatus
            break
    else:
        raise ValueError("Could not find status of container elaunch-primary")

    terminated = elaunch_status.state.terminated  # type: client.models.V1ContainerStateTerminated
    if terminated:
        return terminated.exit_code

    return None


def parse_elaunch_status_report(working_instance):
    metafile = os.path.join(str(working_instance), "elaunch.yaml")
    status_file = os.path.join(str(working_instance), "output", "status.txt")
    outputJson = os.path.join(str(working_instance), "output", "output.json")

    if os.path.isfile(status_file) is False:
        print("Status file %s does not exist" % status_file)
        return {}

    status = {}

    try:
        with open(status_file, 'r') as file:
            lines = file.readlines()
            for line in lines:
                key, value = line.split("=", 1)
                if key == "stages":
                    arr = ast.literal_eval(value)
                    status[key] = arr
                elif key in ["current-stage", "exit-status", "experiment-state", "stage-state"]:
                    status[key.replace("-", "")] = value[:-1].strip()
                elif key == "updated":
                    # TODO can we make it actually date and not string?
                    # experimentEntry[key] = dateparser.parse(value,settings={'TIMEZONE': 'UTC'})
                    status[key] = value.strip()
                elif key in ["cost", "stage-progress", "total-progress", "error-description"]:
                    status[key.replace("-", "")] = value.strip()

            try:
                if 'errordescription' in status:
                    err = status['errordescription']
                    status['errordescription'] = err.encode('utf-8').decode('unicode_escape')
            except Exception as e:
                print(f"Could not prettify the error description due to {e} will keep it as is")

            if os.path.isfile(outputJson):
                status["outputfiles"] = json.load(open(outputJson, 'r'))
            else:
                status['outputfiles'] = None

            if os.path.isfile(metafile):
                with open(metafile, 'r') as myfile:
                    data = myfile.read()
                    status["meta"] = data
            else:
                status['meta'] = None

    except Exception as e:
        print("Could not parse status because of %s -- assume transient error" % e)
        return {}

    return status


def get_workflow():
    max_retries = 5
    retries = 0

    while retries < max_retries:
        if retries:
            time.sleep(5)
        try:
            return api_instance.get_namespaced_custom_object(K8S_WORKFLOW_GROUP, K8S_WORKFLOW_VERSION, namespace,
                                                             K8S_WORKFLOW_PLURAL, name)
        except client.rest.ApiException as e:
            if (e.reason or '').lower() == 'not found':
                print("Could not get workflow object it does not exist")
            else:
                print("Exception when calling CustomObjectsApi->get_namespaced_custom_object: %s\n" % e)
            retries += 1
        except Exception as e:
            print("Exception when calling CustomObjectsApi->get_namespaced_custom_object: %s\n" % e)
            retries += 1

    raise ValueError("Could not fetch workflow - abort")


def update_workflow_status(status):
    workflow = get_workflow()
    workflow['status'] = status
    status['updated'] = str(datetime.datetime.now())

    try:
        _ = api_instance.patch_namespaced_custom_object(
            K8S_WORKFLOW_GROUP, K8S_WORKFLOW_VERSION, namespace, K8S_WORKFLOW_PLURAL, name, workflow)
    except Exception as e:
        print("Could not update workflow status due to %s - will assume transient error" % e)


if __name__ == '__main__':
    base_dir = "/tmp/chpc-"+getpass.getuser()+"-shadow"
    workdir = "/tmp/workdir"
    working_instance = None

    with open("/etc/podinfo/flow-k8s-conf.yml", 'r') as stream:
        try:
            parsed = (yaml.safe_load(stream))
        except Exception as exc:
            print("Could not parse flow-k8s-conf.yml file - %s" % exc)
            raise

    objectmeta = parsed["objectmeta"]
    namespace = objectmeta["namespace"]
    ownerreferences = objectmeta["ownerreferences"]
    name = (ownerreferences[0]["name"])
    old_instance = None
    last_known_state = None
    config.load_incluster_config()

    status = {}
    exit_code = None
    api_instance = client.CustomObjectsApi(client.ApiClient())

    while exit_code is None:
        time.sleep(1)

        working_instance = discover_instance_dir(base_dir, workdir, working_instance)

        if old_instance != working_instance:
            print("Latest working instance is %s" % working_instance)

        old_instance = working_instance

        if working_instance is None:
            continue
        working_instance_path = os.path.join(workdir, str(working_instance))

        # VV: First check whether elaunch has terminated, and then parse the status report file
        try:
            exit_code = get_elaunch_exit_code(name, namespace)
        except Exception as e:
            print("Could not determine exit-code of elaunch because of %s -- will assume transient error" % e)
            continue

        status = parse_elaunch_status_report(working_instance_path)
        if exit_code not in [None, 0] and status.get('experimentstate', '').lower() not in ['finished', 'failed']:
            # VV: elaunch has terminated with some kind of error but the status does not reflect that
            status['currentstage'] = ""
            status['exitstatus'] = "Failed"
            status['outputfiles'] = {}
            status['currentstage'] = ""
            status['stageprogress'] = ""
            status['stagestate'] = "initialising"

            if exit_code != 2:
                status['errordescription'] = ("workflow has terminated with exit error %s but there is no detailed "
                                              "status report.This is an unexpected issue. Please check the logs" %
                                              exit_code)
            else:
                status['errordescription'] = ("workflow has terminated with exit error %s but there is no detailed "
                                              "status report, this indicates invalid arguments to "
                                              "workflow. Please check the logs" % exit_code)
            status['stages'] = []
            status['totalprogress'] = ""
            status['meta'] = ""

            status['exitstate'] = 'failed'
            status['experimentstate'] = 'failed'
        elif exit_code == 0 and status.get('experimentstate', '').lower() not in ['finished', 'failed']:
            print("Elaunch has terminated, but status does not contain experiment state, will sleep for 10 seconds "
                  "and try reading status report again")
            pprint.pprint(status)
            time.sleep(10)
            status = parse_elaunch_status_report(working_instance_path)
        elif exit_code == 0:
            if status.get('outputfiles') is None or status.get('meta') is None:
                print("Elaunch has terminated, but status does not contain outputfiles or meta field, "
                      "will sleep for 10 seconds and try reading status report again")
                pprint.pprint(status)
                time.sleep(10)
                status = parse_elaunch_status_report(working_instance_path)

        if status.get('meta') is None:
            status['meta'] = ""
            print("No meta field, generating an empty one")
        if status.get('outputfiles') is None:
            status['outputfiles'] = {}
            print("No outputfiles field, generating an empty one")

        update_workflow_status(status)

    print("Flow terminated with exit code %s" % exit_code)
    print("Final status")
    pprint.pprint(status)