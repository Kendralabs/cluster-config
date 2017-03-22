---
title: Cluster Push
tags: [push]
keywords: push
last_updated: March 17, 2017
datatable: true
summary: "cluster push will set all the calculated optimized configurations in Cloudera manager."
sidebar: cluster_config_sidebar
permalink: cluster_config_push.html
folder: cluster_config
---

## Purpose

The cluster-push command will allow you to save all the calculated configurations in Cloudera manager and if desired the cluster will be restarted.

## Command line options

Here are command line options for cluster-push.

```shell
$ cluster-push --help
usage: cluster-push [-h] --update-cdh {no,yes} --restart-cdh {no,yes}
                    [--conflict-merge {interactive,user,generated}] --host
                    HOST [--port PORT] [--username USERNAME]
                    [--password PASSWORD] [--cluster CLUSTER] [--path PATH]
                    [--log {INFO,DEBUG,WARNING,FATAL,ERROR}]

Update CDH with cdh.json/user-cdh.json configuration values.

optional arguments:
  -h, --help            show this help message and exit
  --update-cdh {no,yes}
                        Should we update CDH with all configurations in
                        cdh.json/user-cdh.json?
  --restart-cdh {no,yes}
                        Should we restart CDH services after configuration
                        changes
  --conflict-merge {interactive,user,generated}
                        When encountering merge conflicts between the
                        generated configuration() and the user configuration()
                        what value should we default to? The 'user',
                        'generated', or 'interactive'resolution
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

**cluster-push** only has three unique options
 
 * **--update-cdh**: Whether you would like to update CDH configurations. Either yes or no. This is a required option.
 * **--restart-cdh**: Whether you would like to restart CDH after updating its configuration. Either yes or no. This is a required option.
 * **--conflict-merge**: Conflict resolution preference when encountering key conflicts between cdh.json and user-cdh.json. Defaults to user-cdh.json. Valid values [user, generated, interactive]

The update-cdh and restart-cdh options exist mainly to make the user running tool aware of what they are doing. Since updating and restarting the cluster can be pretty destructive actions i wanted to administrator to acknowledge the update. 

## Examples

### Execution phases

The important phases in the runing of a cluster push script are

 * Acquiring Cloudera manager connection
 * Retrieving all the configurations for every service
 * Reading cdh.json & user-cdh.json
 * Pushing the calculated configuration to Cloudera manager
 * Creating snapshots




### Running with minimal options

This sample command pushes configurations to Cloudera manager that were saved in cdh.json from the cluster-generate command.


```
$ cluster-push --host CLOUDERA_HOST --update-cdh yes --restart-cdh yes
What is the Cloudera manager password? 
--INFO cluster selected: cluster
--INFO Reading CDH config file: /cdh.json
--INFO Reading user CDH config file: /user-cdh.json
--WARNING Couldn't open file: /user-cdh.json
--INFO Updating config group: REGIONSERVER_BASE
--INFO Updated 1 configuration/s.
--INFO Updating config group: NODEMANAGER_BASE
--INFO Updated 4 configuration/s.
--INFO Updating config group: GATEWAY_BASE
--INFO Updated 8 configuration/s.
--INFO Updating config group: RESOURCEMANAGER_BASE
--INFO Updated 6 configuration/s.
--INFO Updating config group: JOBHISTORY_BASE
--INFO Updated 2 configuration/s.
--INFO Trying to restart cluster : "cluster"
--INFO Waiting for Restart
--INFO Waiting for Restart
--INFO Waiting for Restart
--INFO Waiting for Restart
...
--INFO Waiting for Restart
--INFO Waiting for Restart
--INFO Waiting for Restart
--INFO Waiting for Restart


--INFO Done with Restart.
--INFO Creating file snapshots
--INFO Creating snapshot folder:/CLOUDERA_HOST-2017_03_17_16_08_42
--INFO Snapshotting: /before-ALL-CDH-CLUSTER-CONFIGURATIONS.json 
--INFO Snapshotting: /after-ALL-CDH-CLUSTER-CONFIGURATIONS.json 
--INFO Snapshotting: /cdh.json 
--INFO Snapshotting: /user-cdh.json 
--WARNING Couldn't create snapshot for: /user-cdh.json

```

#### Opening configuration files

```
--INFO Reading CDH config file: /cdh.json
--INFO Reading user CDH config file: /user-cdh.json
```
cluster-push will first try to open the json configuration files.


#### Saving configurations
```
--INFO Updating config group: REGIONSERVER_BASE
--INFO Updated 1 configuration/s.
--INFO Updating config group: NODEMANAGER_BASE
--INFO Updated 4 configuration/s.
--INFO Updating config group: GATEWAY_BASE
--INFO Updated 8 configuration/s.
--INFO Updating config group: RESOURCEMANAGER_BASE
--INFO Updated 6 configuration/s.
--INFO Updating config group: JOBHISTORY_BASE
--INFO Updated 2 configuration/s.
```
You will be notified with configuration updates with the configuration group name and the number of configurations that got updated. 


#### Restarting the cluster
```
--INFO Trying to restart cluster : "cluster"
--INFO Waiting for Restart
--INFO Waiting for Restart
--INFO Waiting for Restart
--INFO Waiting for Restart
...
--INFO Waiting for Restart
--INFO Waiting for Restart
--INFO Waiting for Restart
--INFO Waiting for Restart
--INFO Done with Restart.
```
If you opted to restart the cluster the request will be sent to the server and polling will check its progress.


#### Saving snapshots
```
--INFO Snapshotting: /before-ALL-CDH-CLUSTER-CONFIGURATIONS.json 
--INFO Snapshotting: /after-ALL-CDH-CLUSTER-CONFIGURATIONS.json 
--INFO Snapshotting: /cdh.json 
--INFO Snapshotting: /user-cdh.json 
```

The following Snapshots of will be saved

 * The calculated configurations, cdh.json.
 * All CDH configurations, before-ALL-CLUSTER-CONFIGURATIONS.json.
 * The formula used to calculate the configurations, formula.py. 


Snapshots come in handy when you need to compare configurations. The sorted json output allows for easy comparisons with command line tools like diff. Cloudera manager lets you see historical configuration changes by service while the snapshots will show changes across services.
