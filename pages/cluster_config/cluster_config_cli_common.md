---
title: Common CLI options
tags: [cli]
keywords: cli
last_updated: March 17, 2017
datatable: true
summary: "Common command line options"
sidebar: cluster_config_sidebar
permalink: cluster_config_cli_common.html
folder: cluster_config
---


## Options

No matter what command line script you run they will all share the following options.

* **--host**: Cloudera manager IP or host name, always required
* **--port**: Cloudera manager port, default 7180, required if your CDH manager UI port is not 7180
* **--username**: Cloudera manager username, default 'admin', required if your CDH manager UI username is not 'admin'
* **--password**: Cloudera manager password, if not provided on the command line you will be prompted
* **--cluster**: Cloudera manager cluster name  if more than one cluster is managed by cloudera manager.
* **--path**: The path we will save and load files, like cdh.json, default is working directory
* **--log**: Log level, defaults to INFO