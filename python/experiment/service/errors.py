#
# coding=UTF-8
# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis

from __future__ import annotations

from typing import Optional, List, Dict, Any

import requests

import json
import experiment.model.errors
    

class UnknownGateway(experiment.model.errors.FlowException):
    def __init__(self, gateway_name, gateway_registry_url):
        # type: (str, str) -> None
        self.gateway_name = gateway_name
        self.gateway_registry_url = gateway_registry_url


class InvalidHTTPRequest(Exception):
    def __init__(self, url, http_method, response, info):
        # type: (str, str, requests.Response, Optional[str]) -> None
        """Raised for an invalid HTTP Request by experimebt.db.ExperimentRESTApi
        """
        self.url = url
        self.http_method = http_method
        self.info = info
        self.response = response

        super(InvalidHTTPRequest, self).__init__()

        self.message = "Invalid %s request to %s (HTTP status code %s)" % (http_method, url, response.status_code)
        try:
            json_dict = response.json()
            message = json_dict['message']
            self.message += message + '.'
        except Exception:
            pass

        if info:
            self.message += ' %s' % info

        if response is not None and response.status_code == 504:
            self.message += ' - HTTP status code 504 TIMEOUT and may indicate a transient network error'

    def __str__(self):
        return self.message

    def __repr__(self):
        return '%s:%s' % (type(self), self.message)


class ProblematicEntriesError(experiment.model.errors.FlowExceptionWithMessageError):
    def __init__(self, problems: List[Dict[str, Any]], extra: str | None = None):
        self.problems = problems
        self.extra = extra
        
        if extra is None:
            extra = "REST-API reported problems"

        msg = extra + f" - problems: {json.dumps(problems, indent=2)}"

        super(ProblematicEntriesError, self).__init__(msg)


class HTTPUnknownExperimentId(InvalidHTTPRequest):
    def __init__(self, url, http_method, response, experiment_id):
        # type: (str, str, requests.Response, str) -> None
        self.experiment_id = experiment_id
        super(HTTPUnknownExperimentId, self).__init__(
            url=url, http_method=http_method, response=response, info=None)

        self.message = 'Unknown experiment "%s"' % experiment_id


class HTTPDataInputFilesConflicts(InvalidHTTPRequest):
    def __init__(self, url, http_method, response, dataInputConflicts):
        # type: (str, str, requests.Response, List[str]) -> None
        self.dataInputConflicts = list(dataInputConflicts or [])

        super(HTTPDataInputFilesConflicts, self).__init__(
            url=url, http_method=http_method, response=response, info=None)

        self.message = 'Following Data and Input files have the same name: %s' % self.dataInputConflicts


class HTTPExperimentInaccessibleContainerRegistries(InvalidHTTPRequest):
    def __init__(self, url, http_method, response, containerRegistries):
        # type: (str, str, requests.Response, List[str]) -> None
        self.containerRegistries = list(containerRegistries or [])

        super(HTTPExperimentInaccessibleContainerRegistries, self).__init__(
            url=url, http_method=http_method, response=response, info=None)

        self.message = "There is no known imagePullSecret entry for referenced " \
                       "container registries %s" % self.containerRegistries


class HTTPInvalidExperimentID(InvalidHTTPRequest):
    def __init__(self, url, http_method, response, experiment_id, naming_rule):
        # type: (str, str, requests.Response, str, str) -> None
        self.experiment_id = experiment_id
        self.naming_rule = naming_rule

        super(HTTPInvalidExperimentID, self).__init__(
            url=url, http_method=http_method, response=response, info=None)
        self.message = 'Invalid experiment id "%s". %s' % (experiment_id, naming_rule)


class HTTPInvalidVirtualExperimentDefinitionError(InvalidHTTPRequest):
    def __init__(self, url, http_method, response, validation_errors: str):
        self.validation_errors = validation_errors
        super(HTTPInvalidVirtualExperimentDefinitionError, self).__init__(
            url=url, http_method=http_method, response=response, info=validation_errors
        )


class HTTPInvalidExperimentStartPayload(InvalidHTTPRequest):
    def __init__(self, url, http_method, response, validation_errors: str):
        self.validation_errors = validation_errors
        super(HTTPInvalidExperimentStartPayload, self).__init__(
            url=url, http_method=http_method, response=response, info=validation_errors
        )


class HTTPExperimentIDAlreadyExists(InvalidHTTPRequest):
    def __init__(self, url, http_method, response, experiment_id):
        # type: (str, str, requests.Response, str) -> None
        self.experiment_id = experiment_id

        super(HTTPExperimentIDAlreadyExists, self).__init__(
            url=url, http_method=http_method, response=response, info=None)
        self.message = 'Experiment id "%s" already exists' % experiment_id


class UnauthorisedRequest(InvalidHTTPRequest):
    def __init__(self, url, http_method, response, info):
        # type: (str, str, requests.Response, Optional[str]) -> None
        """Raised for an unauthorised @http_method request to a @url by experimebt.db.ExperimentRESTApi
        """
        super(UnauthorisedRequest, self).__init__(url, http_method, response, info)

        self.message = "Unauthorised %s request to %s" % (http_method, url)
        if info:
            self.message += ' %s' % info


class CDBException(experiment.model.errors.FlowException):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class InvalidMongoQuery(experiment.model.errors.FlowException):
    def __init__(self, query):
        self.query = query

        self.message = 'Invalid Mongo query %s' % str(self.query)

    def __str__(self):
        return self.message


class MongoQueryContainsComponentReferenceAndExtraStage(experiment.model.errors.FlowException):
    def __init__(self, component, stage):
        super(MongoQueryContainsComponentReferenceAndExtraStage, self).__init__(None)
        self.component = component
        self.stage = stage

        self.message = "Mongo Query contains both component reference (%s) and a stage (%s)" % (component, stage)

    def __str__(self):
        return self.message


class NoInstanceError(CDBException):
    def __init__(self, query: Dict[str, Any]):
        self.query = query

        super(NoInstanceError, self).__init__(f"No instance for query {query}")


class NoPropertyError(CDBException):
    def __init__(self, instance_uri: str, property_name: str, known_properties: List[str] | None):
        self.instance_uri = instance_uri
        self.property_name = property_name
        self.known_properties = known_properties

        msg = f"Instance {instance_uri} has not computed property {property_name}"
        if known_properties:
            msg = f"{msg}. Known properties are {known_properties}"

        super(NoPropertyError, self).__init__(msg)


class NoOutputError(CDBException):
    def __init__(self, instance_uri: str, output_name: str, known_outputs: List[str] | None):
        self.instance_uri = instance_uri
        self.output_name = output_name

        self.known_outputs = known_outputs
        msg = f"Instance {instance_uri} has not computed output {output_name}"
        if known_outputs:
            msg = f"{msg}. Known outputs are {known_outputs}"

        super(NoOutputError, self).__init__(msg)


class BadCallError(CDBException):
    def __init__(self, reason: str):
        self.reason = reason

        super(BadCallError, self).__init__(f"Call is malformed, reason: {reason}")


class NoMatchingDocumentError(CDBException):
    def __init__(self, query: Dict[str, Any]):
        self.query = query
        super(NoMatchingDocumentError, self).__init__(f"No matching document for query {query}")
