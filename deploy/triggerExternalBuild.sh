#!/usr/bin/env bash

# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

## Process ipnut arguments for instance-specifics
    ## Parse inputs
    for arg in "$@"
    do
        key=`echo $arg | cut -f1 -d=`
        value=`echo $arg | cut -f2 -d=`

        ## Parse Key:Values against expected list
        ## Create 'input_' versions of variables used to populate against defaults
        case "${key}" in 
            ## Repository to trigger
            repo)
                input_repo=${value} ;;

            ## TravisAPI key to use
            token)
                input_token=${value} ;;
           
            ## Optional: triggering repository. Name added to commit on triggered repo
            trigger_src)
                input_triggersrc=${value} ;;

            *)
                echo "[${key}] is an unrecognised argument. Ignoring"
        esac
    done

    ## Set script arguments. If appropriate, setting default values
    ## Checks any required values are populated
        ## Repository to trigger
        if [ -z ${input_repo} ] ;
        then
            echo "No repository is selected"
            echo 'Select with <repo=${ORG}/${REPO}> syntax on command-line'
            exit 54
        else
            repo=${input_repo}
            echo "Triggering [${repo}]"
        fi

        ## Travis API Token: needed to authenticate
        if [ -z ${input_token} ] ;
        then
            echo "No Travis API token entered"
            echo 'Enter with <token=${TOKEN}> syntax on command-line'
            exit 55
        else
            apiToken=${input_token}
        fi

        ## Source of API trigger; marked in triggering commit. Useful to see what caused jobs to trigger
        triggersrc=${input_triggersrc:='untracked shell script'}
        


## Adjust repo slug for TravisAPI
## org/name format needs to be: <org>2%F<name>, for some weird reason
    repoAPI=`echo ${repo} | sed -e "s/\//%2F/g"`


## Define message body
body=\''{
    "request": {
        "branch": "main",
        "message": "Build triggered by ['"${triggersrc}"'] via Travis API call"
        }
    }'\'



## Send CURL request
curl -s -X POST \
    -H "Context-Type: application/json" \
    -H "Accept: application/json"       \
    -H "Travis-API-Version: 3"          \
    -H "Authorization: token ${apiToken}" \
    -d "${body}" \
    https://travis.ibm.com/api/v3/repo/${repoAPI}/requests
