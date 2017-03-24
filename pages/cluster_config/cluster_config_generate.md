---
title: Cluster Generate
tags: [generate]
keywords: generate
last_updated: March 17, 2017
datatable: true
summary: "cluster generate creates optimized configurations and saves them to a JSON file"
sidebar: cluster_config_sidebar
permalink: cluster_config_generate.html
folder: cluster_config
---

## Purpose

The cluster-generate command allows you to create optimized configurations. The resulting configuration is saved to the local filesystem for review.

Saving the optimized configurations is invaluable when developing a new formula and when modifying the behavior of the formula with its parameters.


## Command line options

Here are command line options for cluster-generate:

```shell
$ cluster-generate --help
usage: cluster-generate [-h] [--formula FORMULA] [--formula-args FORMULA_ARGS]
                        --host HOST [--port PORT] [--username USERNAME]
                        [--password PASSWORD] [--cluster CLUSTER]
                        [--path PATH] [--log {INFO,DEBUG,WARNING,FATAL,ERROR}]

Auto generate various CDH configurations based on system resources

Optional arguments:
  -h, --help            show this help message and exit
  --formula FORMULA     Auto generation formula file. Defaults to
                        /usr/local/lib/python2.7/dist-packages/cluster_config-
                        0-py2.7.egg/cluster_config/formula.py
  --formula-args FORMULA_ARGS
                        formula arguments to possibly override constants in
                        /usr/local/lib/python2.7/dist-packages/cluster_config-
                        0-py2.7.egg/cluster_config/formula.py.
  --host HOST           Cloudera Manager Host
  --port PORT           Cloudera Manager Port
  --username USERNAME   Cloudera Manager User Name
  --password PASSWORD   Cloudera Manager Password
  --cluster CLUSTER     Cloudera Manager Cluster Name if more than one cluster
                        is managed by Cloudera Manager.
  --path PATH           Directory where we can save/load configurations files.
                        Defaults to working directory /home/rodorad/Documents
                        /cluster-config/pages/cluster_config
  --log {INFO,DEBUG,WARNING,FATAL,ERROR}
                        Log level [INFO|DEBUG|WARNING|FATAL|ERROR]

```

### Unique Options

**cluster-generate** only has two unique options
 
 * 	**formula** path to formula file if not using the default packaged formula
 *  **formula-args** path to formula arguments to tweak some of the formulas output


## Examples

### Execution phases

The important phases in the running of a cluster generate script are:

 * Acquiring a Cloudera Manager connection
 * Retrieving all the configurations for every service
 * Running the formula
 * Saving the calculated configuration
 * Creating snapshots




### Running with minimal options

This sample command runs cluster-generate with default parameters:


```
$ cluster-generate --host CLOUDERA_HOST
What is the Cloudera manager password? 
--INFO cluster selected: CLUSTER_NAME
--INFO using formula: formula.py
--INFO NUM_THREADS could be as large as 42 for multi-tenacty
--INFO Wrote CDH configuration file to: cdh.json
--INFO Creating file snapshots
--INFO Creating snapshot folder: CLOUDERA_HOST-2017_03_20_23_44_47
--INFO Snapshotting: cdh.json 
--INFO Snapshotting: generated.conf 
--INFO Snapshotting: before-ALL-CLUSTER-CONFIGURATIONS.json 
--INFO Snapshotting: formula.py 

```

#### Password

```
What is the Cloudera manager password? 
```
Before connecting to Cloudera Manager, you will be prompted for a password if one was not provided on the command line.

#### Formula logs
```
--INFO NUM_THREADS could be as large as 42 for multi-tenacty
```
You can get log messages from the formula. In this case it's a tip, but they can also be error messages.


#### Saving the calculated configurations
```
--INFO Wrote CDH configuration file to: cdh.json
```
The optimized configurations are saved to **_cdh.json_** to allow users to view and verify the configurations before they get saved to CDH with the **cluster-push** command.


#### Saving snapshots
```
--INFO Snapshotting: cdh.json 
--INFO Snapshotting: before-ALL-CLUSTER-CONFIGURATIONS.json 
--INFO Snapshotting: formula.py 
```

For auditing purposes, many files will be saved in a snapshots folder.

 * The calculated configurations, cdh.json.
 * All CDH configurations, before-ALL-CLUSTER-CONFIGURATIONS.json.
 * The formula used to calculate the configurations, formula.py. 


### Specify formula arguments

The most common option to be passed to **cluster-generate** will be the formula-args.yaml file.

An example formula arguments file  for the default formula can be found in the [repository](https://github.com/tapanalyticstoolkit/cluster-config/formula-args.yaml.tpl).

```
# User defined parameters, please don't modify unless you are an advanced user
OVER_COMMIT_FACTOR: 1.30 # Overcommit factor for JVM
MEM_FRACTION_FOR_HBASE: 0.125  # Suggested Max Memory fraction for HBase
MEM_FRACTION_FOR_OTHER_SERVICES: 0.125 # Suggested Memory fraction for other system resources
MAPREDUCE_JOB_COUNTERS_MAX: 500
SPARK_DRIVER_MAXPERMSIZE: 512 # This is required for running in yarn-cluster mode
YARN_SCHEDULER_MINIMUM_ALLOCATION_MB: 1024 # increase this in 512 increaments but usualy it should not be changed
ENABLE_SPARK_SHUFFLE_SERVICE: true # Set this to "true" in order to enable this service for large workloads
ZOOKEEPER_IS_EXTERNAL: true # Set this to "false" in order to run Zookeeper in local mode

NUM_THREADS: 1  # This should be set to the maximum number of munti-tenant users
MAPREDUCE_MINIMUM_AM_MEMORY_MB: 4096 # increase this in 512 increaments to create larger containers
MAPREDUCE_MINIMUM_EXECUTOR_MEMORY_MB: 4096 # increase this in 512 increaments to create larger containers
ENABLE_DYNAMIC_ALLOCATION_FOR_SPARK: true # set this to false in order to disable SPARK dynamic allocation 
```

The formula arguments file lets users change how the formula  calculates optimized configurations.

Let’s update MEM_FRACTION_FOR_HBASE to lower the amount of memory allocated to HBASE, because our jobs will not use it and we would rather have the extra memory allocated to YARN. Let’s reduce the value to 10% and run cluster-generate again.

```
# User defined parameters, please don't modify unless you are an advanced user
OVER_COMMIT_FACTOR: 1.30 # Overcommit factor for JVM
MEM_FRACTION_FOR_HBASE: 0.10  # Suggested Max Memory fraction for HBase
MEM_FRACTION_FOR_OTHER_SERVICES: 0.125 # Suggested Memory fraction for other system resources
MAPREDUCE_JOB_COUNTERS_MAX: 500
SPARK_DRIVER_MAXPERMSIZE: 512 # This is required for running in yarn-cluster mode
YARN_SCHEDULER_MINIMUM_ALLOCATION_MB: 1024 # increase this in 512 increaments but usualy it should not be changed
ENABLE_SPARK_SHUFFLE_SERVICE: true # Set this to "true" in order to enable this service for large workloads
ZOOKEEPER_IS_EXTERNAL: true # Set this to "false" in order to run Zookeeper in local mode

NUM_THREADS: 1  # This should be set to the maximum number of munti-tenant users
MAPREDUCE_MINIMUM_AM_MEMORY_MB: 4096 # increase this in 512 increaments to create larger containers
MAPREDUCE_MINIMUM_EXECUTOR_MEMORY_MB: 4096 # increase this in 512 increaments to create larger containers
ENABLE_DYNAMIC_ALLOCATION_FOR_SPARK: true # set this to false in order to disable SPARK dynamic allocation 
```


The options in the formula arguments file are entirely up to the author of the formula and the options they think should be modified.

To provide a formula-args.yaml  file to the tool, simply provide the path to the file.


```
$ cluster-generate --host CLOUDERA_HOST --formula-args formula-args.yaml
What is the Cloudera manager password? 
--INFO cluster selected: cluster
--INFO using formula: formula.py
--INFO Reading formula args config file: formula-args.yaml
--INFO NUM_THREADS could be as large as 42 for multi-tenacty
--INFO Wrote CDH configuration file to: cdh.json
--INFO Creating file snapshots
--INFO Creating snapshot folder:CLOUDERA_HOST-2017_03_17_15_37_49
--INFO Snapshotting: cdh.json 
--INFO Snapshotting: before-ALL-CDH-CLUSTER-CONFIGURATIONS.json 
--INFO Snapshotting: formula.py 
--INFO Snapshotting: formula-args.yaml 

```
Not much changed from the last time we ran cluster-generate. 

The only differences are the two extra messages involving loading and snapshot of formula-args.yaml

To see the changes in the configuration, you will need to diff cdh.json with a previous run.
In this diff, you can clearly see memory being removed from ‘HBASE_REGIONSERVER_JAVA_HEAPSIZE’ and the resulting memory being added to the various YARN configurations.

```
<     "HBASE_REGIONSERVER_JAVA_HEAPSIZE": 25331261952
---
>     "HBASE_REGIONSERVER_JAVA_HEAPSIZE": 20265009561

<     "MAPREDUCE_MAP_JAVA_OPTS_MAX_HEAP": 2818572288,
<     "MAPREDUCE_MAP_MEMORY_MB": 3584,
<     "MAPREDUCE_REDUCE_JAVA_OPTS_MAX_HEAP": 5637144576,
<     "MAPREDUCE_REDUCE_MEMORY_MB": 7168,
<     "YARN_APP_MAPREDUCE_AM_MAX_HEAP": 805306368,
---
>     "MAPREDUCE_MAP_JAVA_OPTS_MAX_HEAP": 3221225472,
>     "MAPREDUCE_MAP_MEMORY_MB": 4096,
>     "MAPREDUCE_REDUCE_JAVA_OPTS_MAX_HEAP": 6442450944,
>     "MAPREDUCE_REDUCE_MEMORY_MB": 8192,
>     "YARN_APP_MAPREDUCE_AM_MAX_HEAP": 3221225472,

....

```

