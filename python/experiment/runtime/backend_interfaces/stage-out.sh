#! /bin/bash

# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

#The purpose of this script is to get around:
#A) The limitation that you cannot bstage out a directory
#B) The limitation that bstage out will not identify already existing files in a cache
#
#Given an SRC directory on a remote host this script will stage all its contents to the
#LSF_OUTDIR of the calling LSF job.
#There are two ways to do this - this script uses (B)
#A) Using the basename of each file in the $SRC dir as the $DST i.e -src $PATH -dst $(basename $PATH)
#B) Executing from the $SRC dir and using -src only  i.e. -src $(basename $PATH)
#
# Both are difficult to use with finds -exec command as they require manipulating the paths returned by find
# The solution used here is
# 1. first dump the files without dirs to a file
# 2. then run over the file with xargs using method B
#
# This is to avoid having another wrapper which is executed by find ...


SRC=$1

exec > $SRC/stage-out.log 2>&1 
#exec > $LS_SUBCWD/stage-out.log 2>&1 

set -x

#Get time since submission plus padding
#TODO: Find format of LSB TIMES (seconds since epoch?)
time=$(python -c "print int(round(($LSB_JOB_END_TIME-$LSB_JOB_SUBMIT_TIME)/60.0,0))+1")

cd $SRC

#Find and stage out new files
find . -type f -mmin -$time -printf "%f\n" > stage-out-selection.txt
xargs -a stage-out-selection.txt -n 1 -d "\n" bstage out -src

cd -