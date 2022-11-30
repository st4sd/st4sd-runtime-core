# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# coding=UTF-8
#
# IBM Confidential
# OCO Source Materials
# 5747-SM3
# (c) Copyright IBM Corp. 2019
# The source code for this program is not published or otherwise
# divested of its trade secrets, irrespective of what has
# been deposited with the U.S. Copyright Office.
# Author(s): Vassilis Vassiliadis
# Contact: Vassilis.Vassiliadis@ibm.com

from __future__ import print_function

import logging

import experiment.runtime.backend_interfaces.k8s
import kubernetes.client
import yaml

from .utils import experiment_from_flowir

logger = logging.getLogger()


def test_generate_template_podspec(output_dir):
    flowir = r"""
    components:
    - name: override
      command:
        executable: echo
        arguments: world
      resourceManager:
        config:
          backend: kubernetes
        kubernetes:
          image: ubuntu
          podSpec:
            schedulerName: kubeflux
            volumes:
            - name: dhsm
              emptyDir:
                medium: memory
            containers:
              # st4sd-runtime-core will drop everything after containers[0]
              - volumeMounts:
                - name: dhsm
                  mountPath: /dev/shm
                env:
                - name: hello
                  valueFrom:
                    secretKeyRef:
                      name: the-secret
                      key: the-key
        """

    exp = experiment_from_flowir(flowir, output_dir, checkExecutables=False)
    conf = exp.configuration.get_flowir_concrete().get_component_configuration((0, 'override'))

    template = experiment.runtime.backend_interfaces.k8s.NativeScheduledTask.parse_pod_spec_template(
        conf['resourceManager']['kubernetes']['podSpec'])

    assert template.scheduler_name == "kubeflux"
    assert template.volumes[0] == kubernetes.client.V1Volume(
        name="dhsm", empty_dir=kubernetes.client.V1EmptyDirVolumeSource(medium="memory"))

    client = kubernetes.client.ApiClient()
    tidy_dict = client.sanitize_for_serialization(template)

    assert tidy_dict == {
        'containers': [{
            'env': [{
                'name': 'hello',
                'valueFrom': {
                    'secretKeyRef': {
                        'key': 'the-key',
                        'name': 'the-secret'
                    }
                }
            }],
            'name': 'ignored',
            'volumeMounts': [{
                'mountPath': '/dev/shm',
                'name': 'dhsm'
            }]
        }],
        'schedulerName': 'kubeflux',
        'volumes': [{
            'emptyDir': {'medium': 'memory'},
            'name': 'dhsm'
        }]
    }


def test_generate_template_podspec_override(output_dir):
    template = yaml.load("""
    schedulerName: kubeflux
    volumes:
    - name: dhsm
      emptyDir:
        medium: memory
    containers:
      # st4sd-runtime-core will drop everything after containers[0]
      - volumeMounts:
        - name: dhsm
          mountPath: /dev/shm
        env:
        - name: hello
          valueFrom:
            secretKeyRef:
              name: the-secret
              key: the-key
    """, Loader=yaml.FullLoader)

    layer = yaml.load("""
    volumes:
    - name: working-volume
      persistentVolumeClaim:
        claimName: hello
    containers:
      - volumeMounts:
        - name: working-volume
          mountPath: /tmp/workdir
        env:
        - name: hi
          value: world
    """, Loader=yaml.FullLoader)

    client = kubernetes.client.ApiClient()

    template = experiment.runtime.backend_interfaces.k8s.NativeScheduledTask.parse_pod_spec_template(template)
    layer = experiment.runtime.backend_interfaces.k8s.NativeScheduledTask.parse_pod_spec_template(layer)
    template = experiment.runtime.backend_interfaces.k8s.NativeScheduledTask.merge_pod_specs(template, layer)

    tidy_dict = client.sanitize_for_serialization(template)

    assert tidy_dict == {
        'containers': [{
            'name': 'ignored',
            'env': [
                {'name': 'hello', 'valueFrom': {'secretKeyRef': {'name': 'the-secret', 'key': 'the-key', }}},
                {'name': 'hi', 'value': 'world'},
            ],
            'volumeMounts': [
                {'mountPath': '/dev/shm', 'name': 'dhsm'},
                {'mountPath': '/tmp/workdir', 'name': 'working-volume'},
            ]

        }],
        'schedulerName': 'kubeflux',
        'volumes': [
            {'emptyDir': {'medium': 'memory'}, 'name': 'dhsm'},
            {'name': 'working-volume', 'persistentVolumeClaim': {'claimName': 'hello'}}
        ]}


def test_generate_template_podspec_override_nearly_empty(output_dir):
    template = yaml.load("""
    schedulerName: kubeflux
    """, Loader=yaml.FullLoader)

    layer = yaml.load("""
    volumes:
    - name: working-volume
      persistentVolumeClaim:
        claimName: hello
    containers:
      - volumeMounts:
        - name: working-volume
          mountPath: /tmp/workdir
        env:
        - name: hi
          value: world
    """, Loader=yaml.FullLoader)

    client = kubernetes.client.ApiClient()

    template = experiment.runtime.backend_interfaces.k8s.NativeScheduledTask.parse_pod_spec_template(template)
    layer = experiment.runtime.backend_interfaces.k8s.NativeScheduledTask.parse_pod_spec_template(layer)
    template = experiment.runtime.backend_interfaces.k8s.NativeScheduledTask.merge_pod_specs(template, layer)

    tidy_dict = client.sanitize_for_serialization(template)

    assert tidy_dict == {
        'containers': [{
            'name': 'ignored',
            'env': [
                {'name': 'hi', 'value': 'world'},
            ],
            'volumeMounts': [
                {'mountPath': '/tmp/workdir', 'name': 'working-volume'},
            ]

        }],
        'schedulerName': 'kubeflux',
        'volumes': [
            {'name': 'working-volume', 'persistentVolumeClaim': {'claimName': 'hello'}}
        ]}
