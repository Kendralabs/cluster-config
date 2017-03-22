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

* python 2.7
* [argparse](https://pypi.python.org/pypi/argparse) >= 1.3.0
* [cm-api](https://pypi.python.org/pypi/cm-api) >= 9.0.0,<=12.0.0
* [pyyaml](https://pypi.python.org/pypi/PyYAML) >= 3.11
* Working CDH cluster for running the tool.

The common issue i run into is a mismatch in cm-api version. If you run the tool from a node that has Cloudera manager installed it will likely have the latest version cm-api python module. Downgrading to a version within range should work or even better install cluster-config in a python virtual environment so it doesn’t interfere with the system dependencies. 

