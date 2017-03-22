---
title: Cluster Explore
tags: [explore]
keywords: explore
last_updated: March 21, 2017
datatable: true
summary: "cluster explore will set all the calculated optimized configurations in cloudera manager."
sidebar: cluster_config_sidebar
permalink: cluster_config_explore.html
folder: cluster_config
---

## Purpose

The cluster-explore command enables quick searching of Cloudera service configurations by redirecting their names and descriptions to the command line.

## Command line options

Here are command line options for cluster-explore.

```shell
cluster-explore --help
usage: cluster-explore [-h] [--dump DUMP] --host HOST [--port PORT]
                       [--username USERNAME] [--password PASSWORD]
                       [--cluster CLUSTER] [--path PATH]
                       [--log {INFO,DEBUG,WARNING,FATAL,ERROR}]

Process cl arguments to avoid prompts in automation

optional arguments:
  -h, --help            show this help message and exit
  --dump DUMP           If you want to dump all configs without asking pass
                        'yes'. Defaults to 'no'.
  --host HOST           Cloudera Manager Host
  --port PORT           Cloudera Manager Port
  --username USERNAME   Cloudera Manager User Name
  --password PASSWORD   Cloudera Manager Password
  --cluster CLUSTER     Cloudera Manager Cluster Name if more than one cluster
                        is managed by Cloudera Manager.
  --path PATH           Directory where we can save/load configurations files.
                        Defaults to working directory /tmp/cluster-config
  --log {INFO,DEBUG,WARNING,FATAL,ERROR}
                        Log level [INFO|DEBUG|WARNING|FATAL|ERROR]
```

### Unique Options

**cluster-explore** has one unique option
 
 * **--dump**: will output all configurations for every service to standard out allowing to pipe out to the CLI tool of your choosing. 
 

 If the option to dump all configurations to the screen is kept at the default 'no' the user will have to interactively decided what service and configuration group he wants to look at. 

## Examples

### Execution phases

The important phases of cluster-explore script

 * Acquiring Cloudera manager connection
 * Retrieving all the configurations for every service
 * Check --dump option
 * output all configurations to the screen or ask the user




### Running with minimal options

This sample command prints all the Cloudera configurations to the command line


```
$ cluster-explore --host CLOUDERA_HOST
What is the Cloudera manager password? 
--INFO cluster selected: cluster
dump all configs[yes or no]: yes

config: 
- name: ROLE_HEALTH_SUPPRESSION_REGION_SERVER_HOST_HEALTH
- description: Whether to suppress the results of the Host Health heath test. The results of suppressed health tests are ignored when computing the overall health of the associated host, role or service, so suppressed health tests will not generate alerts.
- key: HBASE.REGIONSERVER.REGIONSERVER_BASE.ROLE_HEALTH_SUPPRESSION_REGION_SERVER_HOST_HEALTH
- value: false

config: 
- name: MGMT_NAVIGATOR_FAILURE_THRESHOLDS
- description: The health test thresholds for failures encountered when monitoring audits within a recent period specified by the mgmt_navigator_failure_window configuration for the role. The value that can be specified for this threshold is the number of bytes of audits data that is left to be sent to audit server.
- key: HBASE.REGIONSERVER.REGIONSERVER_BASE.MGMT_NAVIGATOR_FAILURE_THRESHOLDS
- value: {"critical":"any","warning":"never"}

config: 
- name: HBASE_REGIONSERVER_HLOG_WRITER_IMPL
- description: The HLog file writer implementation.
- key: HBASE.REGIONSERVER.REGIONSERVER_BASE.HBASE_REGIONSERVER_HLOG_WRITER_IMPL
- value: None

config: 
- name: ROLE_CONFIG_SUPPRESSION_HBASE_REGIONSERVER_CODECS
- description: Whether to suppress configuration warnings produced by the built-in parameter validation for the RegionServer Codecs parameter.
- key: HBASE.REGIONSERVER.REGIONSERVER_BASE.ROLE_CONFIG_SUPPRESSION_HBASE_REGIONSERVER_CODECS
- value: false


```

### Interactively

Running interactively will produce the same output but for a specific configuration group like yarn node managers. Here is a sample of what it looks like interactively.

```
$ cluster-explore --host CLOUDERA_HOST
What is the Cloudera manager password? 
--INFO cluster selected: cluster
dump all configs[yes or no]: no
Available service types on cluster: 'cluster'
Pick a service
Id 1 service: HDFS
Id 2 service: SPARK_ON_YARN
Id 3 service: ZOOKEEPER
Id 4 service: HIVE
Id 5 service: YARN
Id 6 service: HBASE
Enter service Id : 5
Selected YARN
Available role types on service: 'YARN'
Pick a role
Id 1 role: NODEMANAGER
Id 2 role: JOBHISTORY
Id 3 role: RESOURCEMANAGER
Id 4 role: GATEWAY
Enter role Id : 1
Selected NODEMANAGER
Available config group types on role: 'NODEMANAGER'
Pick a config group
Id 1 config group: NODEMANAGER_1
Id 2 config group: NODEMANAGER_2
Id 3 config group: NODEMANAGER_BASE
Enter config group Id : 1
Selected NODEMANAGER_1
config: 
- name: NODE_MANAGER_JAVA_OPTS
- description: These arguments will be passed as part of the Java command line. Commonly, garbage collection flags, PermGen, or extra debugging flags would be passed here.
- key: YARN.NODEMANAGER.NODEMANAGER_1.NODE_MANAGER_JAVA_OPTS
- value: -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled

config: 
- name: MAX_LOG_BACKUP_INDEX
- description: The maximum number of rolled log files to keep for NodeManager logs.  Typically used by log4j or logback.
- key: YARN.NODEMANAGER.NODEMANAGER_1.MAX_LOG_BACKUP_INDEX
- value: 10

config: 
- name: YARN_NODEMANAGER_CONTAINER_MANAGER_THREAD_COUNT
- description: Number of threads container manager uses.
- key: YARN.NODEMANAGER.NODEMANAGER_1.YARN_NODEMANAGER_CONTAINER_MANAGER_THREAD_COUNT
- value: 20

```
