---
title: Install
tags: [intall]
keywords: install
last_updated: March 17, 2017
datatable: true
summary: "Install from git or pip"
sidebar: cluster_config_sidebar
permalink: cluster_config_install.html
folder: cluster_config
---

## Install

Install can be done directly directly from the repository or from pip.

### Repo install

To install the tool directly from source, run git clone, then cd to the checkout directory, and finally run setup.py.
 
```bash
git clone https://github.com/tapanalyticstoolkit/cluster-config.git
cd cluster-config
[sudo] python setup.py install 
```

### Pip install

Pip is the easier and preferred installation method. 

```bash
[sudo] pip install cluster-config
```

Once the installation is complete you will have 4 scripts added to your path:

 * cluster-generate
 * cluster-push
 * cluster-explore
 * cluster-generate-push