---
title: Cluster Generate Push
tags: [generate push]
keywords: generate push
last_updated: March 17, 2017
datatable: true
summary: "cluster generate push combines generate and push."
sidebar: cluster_config_sidebar
permalink: cluster_config_gen_push.html
folder: cluster_config
---

## Purpose

The cluster-gen-push command combines the functionality of cluster-generate and **cluster-push**. First the script generates an optimized configuration, and 
then it sets the configuration in CDH. This is useful when you know the optimized service configurations work and there is no need to review them. 

## Command line options

Here are command line options for cluster-push:

```shell
$ cluster-generate-push --help
usage: cluster-generate-push [-h] [--formula FORMULA]
                             [--formula-args FORMULA_ARGS] --update-cdh
                             {no,yes} --restart-cdh {no,yes}
                             [--conflict-merge {interactive,user,generated}]
                             --host HOST [--port PORT] [--username USERNAME]
                             [--password PASSWORD] [--cluster CLUSTER]
                             [--path PATH]
                             [--log {INFO,DEBUG,WARNING,FATAL,ERROR}]

Auto generate various CDH configurations based on system resources

optional arguments:
  -h, --help            show this help message and exit
  --formula FORMULA     Auto generation formula file. Defaults to
                        /usr/local/lib/python2.7/dist-packages/cluster_config-
                        0.1.0-py2.7.egg/cluster_config/formula.py
  --formula-args FORMULA_ARGS
                        formula arguments to possibly override constants in
                        /usr/local/lib/python2.7/dist-packages/cluster_config-
                        0.1.0-py2.7.egg/cluster_config/formula.py.
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

**cluster-generate-push** has all the unique options of **cluster-generate** and **cluster-push**, as follows:
 
 * **formula** path to formula file if not using the default packaged formula.
 * **formula-args** path to formula arguments to tweak some of the formulas output.
 * **--update-cdh**: Whether you would like to update CDH configurations. Either yes or no. This is a required option.
 * **--restart-cdh**: Whether you would like to restart CDH after updating its configuration. Either yes or no. This is a required option.
 * **--conflict-merge**: Conflict resolution preference when encountering key conflicts between cdh.json and user-cdh.json. Defaults to user-cdh.json. Valid values are [user, generated, interactive]



## Examples

See **cluster-generate** **cluster-push** for examples.
