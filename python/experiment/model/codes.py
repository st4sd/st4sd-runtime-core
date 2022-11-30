# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Michael Johnston


'''Contains various objects/globals defining e.g. exitCodes, jobStates etc'''
from __future__ import print_function

#Current reasons for job termination 
#The reason for the dictionary is to enforce use of correctly spelled versions of these names
#i.e. if the wrong name is used it will raise KeyError
#An enum would be useful here
exitReasons = {"Success":"Success",    #exit(0)
               "KnownIssue":"KnownIssue", #exit(non-zero) from application
               "SystemIssue":"SystemIssue", #exit(non-zero) not due to application (usually 128+)
               "SubmissionFailed":"SubmissionFailed", #Refers to specific case where job submission failed i.e. it never ran 
               "UnknownIssue":"UnknownIssue",   #non SIGKILL/SIGTERM/SIGINT/SIGXCPU signal
               "Killed":"Killed", #SIGKILL
               "Cancelled":"Cancelled", # SIGTERM, SIGINT or any other cancel reason
               "ResourceExhausted":"ResourceExhausted" #Usually indicates SIGXCPU
               }

#Jobs States
INITIALISING_STATE = "initialising"
FINISHED_STATE = "finished"
FAILED_STATE = "failed"
RUNNING_STATE = "running"
SUSPENDED_STATE = "suspended"
RESOURCE_WAIT_STATE = "waiting_on_resource"
POSTMORTEM_STATE = "checking"
OUTPUT_DATA_WAIT_STATE = "waiting_on_output_data_transfer"
SHUTDOWN_STATE= "component_shutdown"
states = [INITIALISING_STATE, SHUTDOWN_STATE, FINISHED_STATE, SUSPENDED_STATE, RUNNING_STATE, RESOURCE_WAIT_STATE, FAILED_STATE, POSTMORTEM_STATE, OUTPUT_DATA_WAIT_STATE]


#Restart contexts - returned by the RestartHook
restartContexts = {"RestartContextRestartPossible": "RestartContextRestartPossible",  #Should restart
                "RestartContextHookNotAvailable":"RestartContextHookNotAvailable",  # There is no specific hook, try to restart
                "RestartContextRestartNotRequired":"RestartContextRestartNotRequired",  #A restart is not necessary
                "RestartContextRestartNotPossible":"RestartContextRestartNotPossible",  #A restart is not poossible
                "RestartContextHookFailed":"RestartContextHookFailed",  #Tried to restart but something went wrong
                "RestartContextRestartConditionsNotMet": "RestartContextRestartConditionsNotMet"  # Conditions for restart not meet
}

#Restart codes - returned by the restart() method of engine
restartCodes = {"RestartInitiated": "RestartInitiated",  #Engine has initiated restart  (RestartContextRestartPossible, RestartContextHookNotAvailable)
                "RestartNotRequired":"RestartNotRequired",  #A restart is not necessary (RestartContextRestartNotRequired)
                "RestartCouldNotInitiate":"RestartCouldNotInitiate",  #A restart is not possible (RestartPrepFailed, RestartNotPossible)
                "RestartMaxAttemptsExceeded": "RestartMaxAttemptsExceeded", # A restart is not possible because some limit has been exceeded
                }
