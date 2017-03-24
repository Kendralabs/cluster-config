---
title: Cluster-config
keywords: homepage
tags: [overview]
sidebar: cluster_config_sidebar
permalink: index.html
toc: false
---

## What is it?

Cluster-config is a command line tool that automatically optimizes the service configurations in your Cloudera cluster.

## Why build it?

We needed a tool to quickly and consistently optimize CDH services to provide stable development and QA environments. 
We also needed a tool to help with future optimization efforts as well as help administrators debug configuration issues.


## How does it work?

The tool works by running pluggable formulas written in Python. 
The formula is given all the cluster details; it's up to the formula author to decide what information to use for calculations.
A default formula provided with the tool primarily optimizes YARN, and Spark. New formulas can be specified on the command line.

## Who should use it?

Anyone interested in optimizing CDH services.


