---
title: Common CLI options
tags: [cli]
keywords: cli
last_updated: March 17, 2017
datatable: true
summary: "Common command line options."
sidebar: cluster_config_sidebar
permalink: cluster_config_cli_common.html
folder: cluster_config
---


## Options

No matter what command line script you run, they all use the following options:

* **--host**: Cloudera Manager IP or host name, always required
* **--port**: Cloudera Manager port, default 7180, required if your CDH Manager UI port is not 7180
* **--username**: Cloudera Manager username, default 'admin', required if your CDH manager UI username is not 'admin'
* **--password**: Cloudera Manager password, if not provided on the command line, you will be prompted
* **--cluster**: Cloudera Manager cluster name  if more than one cluster is managed by Cloudera Manager
* **--path**: The path for saving and load files, like cdh.json, default is the working directory
* **--log**: Log level, defaults to INFO