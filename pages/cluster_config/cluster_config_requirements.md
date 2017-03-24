---
title: Install
tags: [intall]
keywords: install
last_updated: March 17, 2017
datatable: true
summary: "Installation requirements"
sidebar: cluster_config_sidebar
permalink: cluster_config_requirements.html
folder: cluster_config
---

## Requirements

* Python 2.7
* [argparse](https://pypi.python.org/pypi/argparse) >= 1.3.0
* [cm-api](https://pypi.python.org/pypi/cm-api) >= 9.0.0,<=12.0.0
* [pyyaml](https://pypi.python.org/pypi/PyYAML) >= 3.11
* Working CDH cluster for running the tool.

A common issue that occurs is a mismatch in cm-api versions. If you run Cluster-config from a node that has Cloudera Manager installed it will likely have the latest version cm-api Python module,
which is not compatible with cm-api versions on other nodes. You have two options:
 * The preferred option is to install cluster-config in a Python virtual environment so it does not interfere with system dependencies.
 * You can also try downgrading to cm-api to a version that removes the mismatch. 

