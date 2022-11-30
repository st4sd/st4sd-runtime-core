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

from typing import Optional

from experiment.model.frontends.dosini import GenerateDosini

description_process_inputs = """
job-type=lsf
queue=%(defaultq)s
environment=%(pythonenv)s
expandArguments=none
executable=concentration_curve_points.py
arguments= -n %(numberPoints)s input/field.conf:ref > concentration_curve_points.csv 
references=input/field.conf:ref

rstage-out=all
george=of the jungle
"""

stage_0 = """
[META]
stage-mame=CheckInputs

[ProcessInputs]

%s


""" % description_process_inputs

stage_1 = """
[META]
stage-name=Setup

[FIELD]

references=stage0.ProcessInputs:ref %(moleculeLibrary)s:ref %(parameterList)s:ref
replicate=%(numberPoints)s

executable=%(hpc-venv)s/bin/generatefield.py
arguments=-v stage0.ProcessInputs:ref/field.conf -c `sed -n "$((%(replica)s+2)),+0p" stage0.ProcessInputs:ref/concentration_curve_points.csv` -s 40,40,40 --moleculeLibrary=%(moleculeLibrary)s:ref --interactionLibrary=%(parameterList)s:ref

[CONTROL]

references=stage0.ProcessInputs:ref %(moleculeLibrary)s:ref data/CONTROL:ref %(parameterList)s:ref 
replicate=%(numberPoints)s

executable=%(hpc-venv)s/bin/modifycontrol.py
arguments= stage0.ProcessInputs:ref/field.conf -r %(replica)s -c data/CONTROL:ref -e spme --exactReciprocal --moleculeLibrary=%(moleculeLibrary)s:ref --automateCutoff -a 0.1  --interactionLibrary=%(parameterList)s:ref


"""

stage_2 = """
[META]
stage-name=Generating


[CAFSimulation]

job-type=%(backend)s
queue=%(exclusiveq)s
environment=%(mpienv)s
executable=%(dpdexe)s
shutdown-on: KnownIssue SystemIssue
restart-hook-on: UnknownIssue
#Resources
numberProcesses=%(dlmeso-number-processes)s
numberThreads=2
ranksPerNode=%(dlmeso-ranks-per-node)s
#For LSF
threadsPerCore=2
walltime=240
#Copy the output of the FIELD job stage1
references=stage1.FIELD/FIELD:copy stage1.CONTROL/CONTROL:copy

rstage-in=stage1.FIELD/FIELD:copy stage1.CONTROL/CONTROL:copy
rstage-out=OUTPUT

# Check serialization/deserialization of k8s, lsf, and optimizer options
lsf-docker-options=Dummy_a_0
lsf-docker-image=Dummy_a_1
lsf-docker-profile-app=Dummy_a_2

k8s-image=Dummy_b_0
k8s-host=Dummy_b_1
k8s-api-key-var=Dummy_b_2
k8s-image-pull-secret=Dummy_b_3
k8s-namespace=Dummy_b_4
k8s-cpu-units-per-core=9999
k8s-grace-period=4

memoization-disable-strong=True
memoization-disable-fuzzy=true
optimizerDisable=true
optimizerExploitChance=0.99
optimizerExploitTarget=0.98
optimizerExploitTargetHigh=0.97
optimizerExploitTargetLow=0.96
restart-hook-file=custom-restart-hook.py

memory = 1Mi

[ClusterAnalysis]
job-type = %(backend)s
queue = %(defaultq)s
numberThreads = 4

environment = ummap

executable = %(ummapexe)s
arguments=-f CAFSimulation:ref/HISTORY -t ALL -g {Tail} RESATOMID 1 2 3 4 AND NOT RESNAME NACL -m C [+d 1 +t 1  +pm 5 +hist COUNT  +c +o FIXED +b N ] S [ +snap 4 ] -par data/ummap_parameters.inp:ref -o UMMAPOUT -continue

references = data/ummap_parameters.inp:ref CAFSimulation:ref

repeat-interval = 60.0

[CAFObservableCalculation]

job-type=%(backend)s
queue=%(defaultq)s
environment=%(pythonenv)s
#UMMAP produces output in nested folders while the output checking method assumes it is produced in the top-level
#Hence we have to disable producer output checking for this component
check-producer-output=false

executable=%(hpc-venv)s/bin/calculate_caf_observables.py
arguments= -d  -f input/field.conf:ref -t 192000  -w 150 --moleculeLibrary=%(moleculeLibrary)s:ref ClusterAnalysis:ref/UMMAPOUT/Data/Basic_Cluster/HIST_Num_Res_in_Cluster/ -r 'HIST.*?([0-9]+)\.dat' -n 20
references=ClusterAnalysis:ref %(moleculeLibrary)s:ref input/field.conf:ref

repeat-interval=300.0

rstage-out=all

[AggregateCAFObservables]

job-type=%(backend)s
queue=%(defaultq)s
environment=%(pythonenv)s

aggregate=yes
executable=%(hpc-venv)s/bin/aggregate_caf_data.py
arguments= -r 0 -m stage0.ProcessInputs/concentration_curve_points.csv:ref -o caf_observables_scan.csv CAFObservableCalculation:ref/caf_combined_observables.csv

references=CAFObservableCalculation:ref stage0.ProcessInputs/concentration_curve_points.csv:ref

repeat-interval=180.0

rstage-out=caf_observables_scan.csv caf_observables_scan.csv.md5

[PlotAggregationNumber]

environment=%(pythonenv)s
executable=bin/mean_structure_size_saltcurve.py
arguments=AggregateCAFObservables:ref/caf_observables_scan.csv

references=AggregateCAFObservables:ref

repeat-interval=180.0

[DetermineShape]

job-type=%(backend)s
environment=%(pythonenv)s
queue=%(defaultq)s

#UMMAP produces output in nested folders while the output checking method assumes it is produced in the top-level
#Hence we have to disable producer output checking for this component
check-producer-output=false

executable=%(hpc-venv)s/bin/calculate_shape.py 
arguments= --new -w 200 ClusterAnalysis:ref/UMMAPOUT/Data/Micelle_Cluster/TIME_Cluster_stats_[Tail].dat

references=ClusterAnalysis:ref

repeat-interval=60.0

rstage-out=all

[AggregateShapeAnalysis]

job-type=%(backend)s
environment=%(pythonenv)s
queue=%(defaultq)s

aggregate=yes
executable=%(hpc-venv)s/bin/aggregate_caf_data.py 
arguments=-o caf_shape_scan.csv -m stage0.ProcessInputs:ref/concentration_curve_points.csv DetermineShape:ref/dominant.csv --useMapHeaders

references=DetermineShape:ref stage0.ProcessInputs:ref

repeat-interval=30.0

rstage-out=all

[Render]

environment=viz
job-type=%(backend)s
queue=%(defaultq)s

executable=viz/bin/%(arch)s/render_latest_pdb_salt.sh
arguments=ClusterAnalysis:ref/UMMAPOUT/Snaps/Cluster_Shape/
references=ClusterAnalysis:ref

repeat-interval=30.0
resourceString=%(resourceSelection)s

rstage-out=lastframe.out.rgb

[VIZRender]

environment=viz
job-type=%(backend)s
queue=%(defaultq)s

# executable=bin/viz_render.py
# arguments=-i ClusterAnalysis:ref/UMMAPOUT/Snaps/Cluster_Shape/ -o ./ -e png -c \"$VMD_PATH -e data/viz_vmd_tcl/viz_vmd_driver.tcl:ref -args \"
# references=ClusterAnalysis:ref data/viz_vmd_tcl/viz_vmd_driver.tcl:ref

# VV: Render 4K frames (3840x2160)
executable=bin/viz_render_annotations.py
arguments=--width 3840 --height 2160 -c \"$VMD_PATH -e data/viz_vmd_tcl/viz_vmd_multiple_mols.tcl:ref -args\" -r data/viz_vmd_tcl/viz_vmd_ummap_template.tcl:ref -b ClusterAnalysis:ref/UMMAPOUT/Snaps/Cluster_Shape/
references=ClusterAnalysis:ref data/viz_vmd_tcl/viz_vmd_multiple_mols.tcl:ref data/viz_vmd_tcl/viz_vmd_ummap_template.tcl:ref

repeat-interval=30.0
resourceString=%(resourceSelection)s

rstage-out=all

# viz_type=VIZRender

[VIZStream]

environment=viz
job-type=%(backendLocal)s

kill-after-producers-done-delay=60.0
references=VIZRender:ref
executable=bin/viz_streamer.py
arguments=-i VIZRender:ref/ -b $FFMPEG_PATH -e png -o rtmp://127.0.0.1:10004/live_4k -l rtmp://127.0.0.1:10004/live_720p -w http://127.0.0.1:10003/web -c VIZRender:ref -p $FLOW_RUN_ID -u \"%(VIZuserClaimToken)s\"

repeat-interval=600.0

[Convert]

environment=viz

executable=viz/bin/convert_salt_render_to_png.sh
arguments=Render:ref AggregateShapeAnalysis:ref/caf_shape_scan.csv %(replica)s
references=Render:ref AggregateShapeAnalysis:ref

repeat-interval=30.0

[Montage]

aggregate=True
executable=montage
environment=viz
resolvePath=False
arguments=Convert:ref/lastframe.png data/Key.png:ref -mode Concatenate -tile "$((1+%(numberPoints)s))"x1 -gravity center SaltCurve.jpg
references=Convert:ref data/Key.png:ref

repeat-interval=30.0


"""

variables_default = """
[GLOBAL]

#The default molecule library to use
moleculeLibrary: caf/data/MOLECULE_LIBRARY_trumbull
parameterList: caf/data/PARAMETER_LIST_trumbull
ummapexe=/gpfs/cds/local/HCRI003/rla09/shared/bin/ummap
numberPoints: 3
resourceSelection =
VIZuserClaimToken=
OverrideFromInput=Hello world
"""

variables_paragon = """
[GLOBAL]

backend=lsf
backendLocal=local
pythonenv=pythonlsf
mpienv=mpi
arch=power8
hpc-venv=pycaf-fen
defaultq=paragon
dpdexe=/gpfs/cds/local/HCRI003/rla09/shared/bin/dpd_xl_spectrum_omp.exe
dlmeso-number-processes=64
dlmeso-ranks-per-node=16
statusRequestInterval=60
walltime=480
exclusiveq=paragon

ffdir= /gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/ForceFieldUnilever
moleculeLibrary= %(ffdir)s/MOLECULE_LIBRARY
parameterList= %(ffdir)s/PARAMETER_LIST

[STAGE2]

resourceSelection = rusage[ngpus_physical=4.00]

"""

environment_default = """
#Experiment Master Configuration File
[DEFAULT]
name=SaltCurve
version=1.0


#The section contains options that try to make configuration files platform/path independent
[SANDBOX]
##Applications are packages of exectuables that are in the same directory as the experiment
#These directories will be linked into the top level of the experiment instance
applications=CAF.application,DPD.application,Viz.application

#Environments
#
#The environment sections (starting [ENVXXXX] define the environment that a job will be executed with
#
#Environment variables i.e. variables prefixed with $, can be specified in these sections.
#The rules for expansion are as follows
#
#ENVIRONMENT HAS NO KEY "DEFAULTS"
#
#If an environment variable appears it is expanded using the launch environment of the script
#
#ENVIRONMENT HAS KEY "DEFAULTS"
#
#e.g. DEFAULTS=PATH:LD_LIBRARY_PATH (note NO $)
#
#DEFAULTS is a list of variables whose values are to be taken from the [ENVIRONMENT] section
#
#For example if in [ENVIRONMENT] there is a line
#PATH=some/path
#
#Then the environment [ENV-MPI] is defined with:
#DEFAULTS=PATH
#PATH=/some/other/path:$PATH
#
#Then $PATH will be expanded with the value of PATH from [ENVIRONMENT] NOT the launch environment
#
#This allows construction of "clean" environments which are not contaminated by the launch environment


[ENVIRONMENT]
OMP_NUM_THREADS=4

[ENV-GNUPLOT]
PATH=$HOME/../shared/bin/:$PATH

"""

environment_paragon = """
[DEFAULT]
venvpath=/gpfs/cds/local/HCRI003/rla09/shared/virtualenvs

[SANDBOX]
#This is to allow executables in venvs to be referred to without full path
#Links to the venvs will be created in the top-level of the instance
#Since the environment may not be active the executable won't be in PATH
#In some cases it will never be active (e.g. on BGQ) 
virtualenvs=%(venvpath)s/%(hpc-venv)s

[ENV-PYTHONLSF]
#Path is to get python virtual-env for  python jobs 
PATH=%(venvpath)s/%(hpc-venv)s/bin:$PATH
LD_LIBRARY_PATH=/gpfs/paragon/local/apps/gcc/lapack/3.7.0/lib64:$LD_LIBRARY_PATH
#The above PATH call doesn't set the pythonpath
#As launched from ipad the first jobs fails although it uses the correct python
#However launched directly this environment works without adding venv site-packages to pythonpath?!
#Have to find out why
PYTHONPATH=%(venvpath)s/%(hpc-venv)s/lib/python2.7/site-packages:/gpfs/paragon/local/apps/python2/2.7.8/lib/python2.7/site-packages
OMP_NUM_THREADS=4
PYTHON_EGG_CACHE=$HOME

[ENV-MPI]
DEFAULTS=PATH:LD_LIBRARY_PATH
LSF_MPIRUN=/gpfs/paragon/local/apps/ibm/spectrum_mpi/10.1.0/bin/mpirun
PATH=/gpfs/paragon/local/apps/ibm/spectrum_mpi/10.1.0/bin/:$PATH
LD_LIBRARY_PATH=/gpfs/paragon/local/apps/cuda/8.0/lib64:/opt/ibm/lib:/gpfs/paragon/local/apps/ibm/spectrum_mpi/10.1.0/lib/:$LD_LIBRARY_PATH
LD_RUN_PATH=/gpfs/paragon/local/apps/cuda/8.0/lib64:/gpfs/paragon/local/apps/ibm/spectrum_mpi/10.1.0/lib/:$LD_RUN_PATH

[ENV-GNUPLOT]
DEFAULTS=PATH:LD_LIBRARY_PATH
LD_LIBRARY_PATH=/gpfs/paragon/local/apps/gcc/utilities/lib/:LD_LIBRARY_PATH
PATH=/gpfs/paragon/local/apps/gcc-7.1.0/gnuplot/5.2.2/bin/:$PATH
#Gnuplot CL calls a python script that accesses the venv
#For the same reason as detailed in ENV-PYTHON this is required
PYTHONPATH=%(venvpath)s/%(hpc-venv)s/lib/python2.7/site-packages:/gpfs/paragon/local/apps/python2/2.7.8/lib/python2.7/site-packages

[ENV-VIZ]
#Adding paths to spack installed image-magick due to policy.xml issues with system install
DEFAULTS=PATH:LD_LIBRARY_PATH
LD_LIBRARY_PATH=/opt/ibm/lib:/gpfs/paragon/local/HCRI003/rla09/shared/src/spack/opt/spack/linux-rhel7-ppc64le/gcc-4.8.5/image-magick-7.0.2-7-mpwuezn65ybc7fv4caxmkxhkirivlfwy/lib:$LD_LIBRARY_PATH
FFMPEG_PATH=/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/Viz.application/bin/ffmpeg-3.4.4/bin/ffmpeg
TACHYON_PATH=/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/Viz.application/bin/power8/tachyon
VMD_PATH=/gpfs/cds/local/HCRI003/rla09/shared/src/ExperimentsProduction/Viz.application/bin/power8/vmd
PATH=%(venvpath)s/%(hpc-venv)s/bin:$PATH
PATH=/gpfs/paragon/local/HCRI003/rla09/shared/src/spack/opt/spack/linux-rhel7-ppc64le/gcc-4.8.5/image-magick-7.0.2-7-mpwuezn65ybc7fv4caxmkxhkirivlfwy/bin:$PATH
PYTHONPATH=%(venvpath)s/%(hpc-venv)s/lib/python2.7/site-packages:/gpfs/paragon/local/apps/python2/2.7.8/lib/python2.7/site-packages

[ENV-UMMAP]
DEFAULTS = PATH
LD_LIBRARY_PATH=/opt/ibm/lib:$LD_LIBRARY_PATH

"""

output = """
#References can be given either as normal format i.e. [stage].JobName:[ref]
#or JobName:[ref] with an explicit list of stages the output file is in as a stage key
#The latter is useful when the same output files exist in multiple stages

[CAFMetrics]

stages=stage2
data-in=AggregateCAFObservables/caf_observables_scan.csv:copy
description="CAF Metrics"
type=csv

[MeanAggregationScan]

stages=stage2
data-in=PlotAggregationNumber/mean_aggregation_number_scan.png:copy
description="Mean aggregation number"
type=png

[SaltCurve]
stages=stage2
data-in=Montage/SaltCurve.jpg:copy
description="Salt curve images"
type=jpg
"""

status = """
[STAGE0]
stage-weight=0.01

[STAGE1]
stage-weight=0.04

[STAGE2]
stage-weight=0.95
executable=bin/status.py
arguments=-c total-samples -s 4000 stage2.AggregateShapeAnalysis:ref/caf_shape_scan.csv
references=stage2.AggregateShapeAnalysis:ref
"""


def SaltCurveGenerator(is_instance, root_path=None, path_name=None, delete_after=True):
    # type: (bool, Optional[str], Optional[str], bool) -> GenerateDosini

    return GenerateDosini(
        stages={
            0: stage_0,
            1: stage_1,
            2: stage_2
        },
        variables={
            None: variables_default,
            'paragon': variables_paragon,
        },
        environments={
            None: environment_default,
            'paragon': environment_paragon,
        },
        inputs={
            'variables.conf': """
[GLOBAL]
OverrideFromInput=

"""
        },
        output=output,
        status=status,
        root_path=root_path,
        path_name=path_name,
        is_instance=is_instance,
        delete_after=delete_after
    )
