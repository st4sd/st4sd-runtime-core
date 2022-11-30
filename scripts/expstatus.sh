#! /usr/bin/env bash

# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

files=$(find $1 -name status.txt)

echo "File, Stage, Progress, State, Exit-Status, Last Updated"

base="/output/status.txt"

for file in $files; do
	currentStage=$(grep current-stage $file | cut -d"=" -f2)
	exitStatus=$(grep exit-status $file | cut -d"=" -f2)
	experimentState=$(grep experiment-state $file | cut -d"=" -f2)
	updated=$(grep updated $file | cut -d"=" -f2)
	progress=$(grep total-progress $file | cut -d"=" -f2)

	if [ -n "$2" ] && [ "$2" = "1" ]; then
		if [ "$exitStatus" != "Success" ]; then
			echo ${file%${base}}, $currentStage, $progress, $experimentState, $exitStatus, $updated
		fi	
	else		
		echo ${file%${base}}, $currentStage, $progress, $experimentState, $exitStatus, $updated
	fi		
	
done
