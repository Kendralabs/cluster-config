---
title: Supported features
tags:
  - getting_started
keywords: "features, capabilities, benefits"
last_updated: "March 16, 2017"
summary: "Review the feature list to see if Cluster-config supports your requirements. New feature request are always welcome."
published: true
sidebar: cluster_config_sidebar
permalink: cluster_config_supported_features.html
folder: cluster_config
---

If you would like to see new supported features please submit a pull request or issue so we can discuss its implementation. 

## Supported features

Features | Notes
--------|-----------
Cloudera Manager Integration | Integrated with the Cloudera manager through cm-api for extracting and saving configurations.
Configuration Snapshots | Takes a snapshot of all service configurations in Cloudera manager.
Configuration Rollback | Reverted to a previous known good configuration snapshot. 
Explore Configurations | Browse all the service configurations from the command line. Easily grep all configurations to help find what you are looking for.
Restart Cluster | Allows the restarting of the Cloudera cluster from the command line.
User Configurations | you can set custom user configurations that are not part of formula calculations.
Pluggable formulas | Users can provide their own formulas to suit their needs and workloads.
Flexible formulas | Optimization formulas can be modified at runtime. Formulas can be written to support multiple or single threads depending on the workload.



## Features not available

The following features are not available but we are thinking about adding them at some point. If you interested in one issue versus another please voice your support so we can prioritize based on interest.


Features | Notes
--------|-----------|-----------
[HDP integration](https://github.com/tapanalyticstoolkit/cluster-config/issues/2) | While HDP is not planned we do have ideas on how to abstract and support formulas on multiple platforms. 
[Heterogeneous clusters](https://github.com/tapanalyticstoolkit/cluster-config/issues/1) | All the services configured by the formula must have the same hardware profile. The tool assume their will only be a single CDH configuration group per service role.

{% include links.html %}


