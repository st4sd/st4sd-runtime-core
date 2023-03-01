# ST4SD Runtime Core

This repository contains the runtime-core of the [Simulation Toolkit for Scientific Discovery (ST4SD)](https://github.ibm.com/st4sd/overview).
The ST4SD-Runtime is a python framework, and associated services, for creating and deploying virtual-experiments - data-flows which embody the measurement of properties of systems.

A data-flow is a workflow which allows consumers to run concurrently with their producers if desired. 

Developers describe their data-flows using a YAML configuration file, which is interpreted and executed by the ST4SD-Runtime.

ST4SD-Runtime supports multiple execution-backends including Kubernetes and LSF and a single YAML file can support multiple-archs and multiple run-modes via overlays.

The ST4SD-Runtime also interacts with the [ST4SD-Datastore](https://github.com/st4sd/st4sd-datastore), a database which allows querying of executed virtual-experiments and retrieval of their data.

There are three parts to the ST4SD-Runtime
- `st4sd-runtime-core`: The core python framework (this git repository) for describing and executing virtual experiments
- [`st4sd-runtime-k8s`](https://github.com/st4sd/st4sd-runtime-k8s): Extensions which enable to running and managing virtual-experiments on k8s clusters  
- [`st4sd-runtime-service`](https://github.com/st4sd/st4sd-runtime-service): A RESTapi based service allowing users to add, start, stop and query virtual-experiments

## Features

* Cross-platform data-flows
  * Supports multiple backends  (LSF, OpenShift/Kubernetes, local)
  * Abstracts differences between backends allowing a single component description to be used
  * Variables can be used to encapsulate platform specific options
  * Can define component and platform specific environments
* Co-processing model
  * Consumers can be configured to run repeatedly while their producers are alive
* Simple to replicate workflow sub-graphs over sets of inputs 
* Supports `do-while` constructs
* Handles task persistence across backend allocation windows and allows user customisable restarts
* Deploy workflows directly from github (Kubernetes stack)
* Store and retrieve data and metadata from st4sd-datastore

## Lightning Start

If you have 

1. `python3` with `virtualenv` 
2. Have `ssh` access to  GitHub set up

The following snippet will install `st4sd-runtime-core` and run a toy-workflow on your laptop

```bash
virtualenv -p python3 $HOME/st4sd-runtime-test
source $HOME/st4sd-runtime-test/bin/activate
pip install st4sd-runtime-core[deploy]
git clone git@github.com:st4sd/sum-numbers.git
elaunch.py --nostamp -l40 sum-numbers
```

This will create a new virtualenv called `st4sd-runtime-test` at `$HOME/st4sd-runtime-test` and install `st4sd-runtime-core` into it.  It will also clone a repository into a directory called `sum-numbers` in whatever directory you run the above commands in. It will then run a toy-workflow that takes a couple of minutes to run. The toy workflow output will be in a directory called `sum-numbers.instance`

You can learn more about the toy-workflow, and  workflow specification, [here.](https://github.com/st4sd/sum-numbers) 

## More Information

Our [documentation website](https://st4sd.github.io/overview) contains detailed information on installing ST4SD, 
writing and running virtual-experiments, along with much more. 
