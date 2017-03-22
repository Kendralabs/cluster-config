---
title: Cluster-config
keywords: homepage
tags: [overview]
sidebar: cluster_config_sidebar
permalink: index.html
---

## What is it?

Cluster-config is a command line tool that will automatically optimize the service configurations in your Cloudera cluster.

## Why build it?

We needed a tool that would quickly and consistently optimize CDH services to have a stable development and QA environments. 
We also needed a tool that would help with future optimization efforts as well as help administrators debug configuration issues.


## How does it work?

The tool works my running pluggable formulas that are written in python. 
The formula is given all the cluster details and it's up to the formula author to decide what information they will use for calculations.
A default formula provided with the tool primarily optimizes yarn, and spark. New formulas can be specified on the command line.

## Who should use it?

Anyone interested in optimizing CDH services.


