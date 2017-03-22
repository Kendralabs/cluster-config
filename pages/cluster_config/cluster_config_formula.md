---
title: Formulas
tags: [formula]
keywords: formula
last_updated: March 17, 2017
datatable: true
#summary: "Common command line options"
sidebar: cluster_config_sidebar
permalink: cluster_config_formula.html
folder: cluster_config
---


It is possible to create your own optimization formulas or copy the default formula and make your own tweaks. The formula files are regular python scripts. While the default formula is called **'formula.py'** there are no naming restrictions.

The formula files are executed with [python's exec file function](https://docs.python.org/2/library/functions.html#execfile) and it's broken up into two methods **constants** and **formula**  


### Constants

Constants definition takes cluster object and logging reference.

```
def constants(cluster, log):
```

Constants is used to define any values needed to calculate the formula. This is where you will define any arguments that can be influence by the user through the formula-args.yaml file.


### Formula 

Formula definition takes cluster object, logging reference, and constants dictionary

```
def formula(cluster, log, constants):
```




### Cluster

The cluster variable will contain all the configuration for all the installed services including, roles, and config groups from the Cloudera manager you connected to.


Let's look at a code sample from **_[formula.py](https://github.com/tapanalyticstoolkit/cluster-config/blob/master/cluster_config/formula.py)_** to see how the cluster variable is being used.

```
...
"NUM_NM_WORKERS": len(cluster.yarn.nodemanager.hosts.all()),
"NM_WORKER_CORES": cluster.yarn.nodemanager.hosts.max_cores(),
"NM_WORKER_MEM": cluster.yarn.nodemanager.hosts.max_memory(),
...
```

On the first line, **cluster.yarn.nodemanager.hosts.all()**, we are retrieving all the details for every host running the yarn node manager role. The returned object will be a list of Cloudera [apihosts](http://cloudera.github.io/cm_api/apidocs/v10/ns0_apiHost.html). 

With the same notation you can retrieve the all the details for hosts running the HBASE region server role or all ZOOKEEPER server roles.

* **cluster.hbase.regionserver.hosts.all()**
* **cluster.zookeeper.server.hosts.all()**

You can access all CDH services, roles, configs, and hosts with the same notation.

The pattern you want to follow for retrieving configurations **cluster.SERVICE.ROLE.CONFIG_GROUP.CONFIG** and for retrieving host details **cluster.SERVICE.ROLE.hosts.all** 


* **SERVICE** is any valid [CDH service](http://cloudera.github.io/cm_api/apidocs/v10/path__clusters_-clusterName-_services.html)
* **ROLE** is any valid [CDH role](http://cloudera.github.io/cm_api/apidocs/v10/path__clusters_-clusterName-_services_-serviceName-_roles.html)
* **CONFIG_GROUP** is any valid config group from [cluster-explore](Exploring CDH Configurations) script. A good example of a configuration group would be 'gateway_base' just about every service has this role.
* **CONFIG** is any valid config from [cluster-explore](Exploring CDH Configurations) script.

When accessing CDH services all attributes are lowercase while the keys displayed by the [cluster-explore](Exploring CDH Configurations) script are uppercase. 


### Accessing Individual configurations

Accessing individual configurations is easy but you must know the configuration name. You will need to run the [cluster-explore](Exploring CDH Configurations) script to find the full configuration name.

Here is a config key that was found with (cluster-explore](Exploring CDH Configurations).

```
- name: HBASE_REGIONSERVER_JAVA_HEAPSIZE
- description: Maximum size in bytes for the Java Process heap memory.  Passed to Java -Xmx.
- key: HBASE.REGIONSERVER.REGIONSERVER_BASE.HBASE_REGIONSERVER_JAVA_HEAPSIZE
- value: 26430567975
```

To retrieve the following value with the cluster variable make the key all lower case and prefix cluster.

**cluster.hbase.regionserver.regionserver_base.hbase_regionserver_java_heapsize.value**

If you look at the implementation for the 'cluster' objects class you will notice that you can set all CDH configurations directly i would caution against it. Saving the computed configurations to the 'cdh' dictionary will allow the user to review the configurations when they get saved to **_cdh.json_** and allow them to be saved to a snapshot for future administrative purposes. 

### Log

The log variable has four methods available for sending log messages to the command line
- log.info
- log.error
- log.warning
- log.fatal: does a sys.exit(1) after logging it's message.

All the methods take a single string as an argument.

### Constants

The dictionary that gets returned by the constants method is passed along to the formula method with any user overrides defined in the [formula-args](Formula args) file.

If we take an example from the [formula.py](https://github.intel.com/gao/cluster-config/blob/master/cluster_config/formula.py) file you can see the many of the constants are defined as lambdas.

```
"NUM_THREADS": lambda x: x if x is not None and x > 0 else 1,
"OVER_COMMIT_FACTOR": lambda x: x if x is not None and x >= 1 else 1,
"MEM_FRACTION_FOR_HBASE": lambda x: x if x is not None and x >= 0 and x < 1 else 0.125,
```

The lambdas will be replaced later either by a user value form formula-args or the default defined in the lambda.

### Tips

When saving optimized configurations be aware of the format you should be saving. A memory setting for yarn, HBASE, and spark may require different formats. Yarn might expect it's configurations in megabytes while HBASE and spark expect theirs in bytes.

Unfortunate, the CDH rest api doesn't always throw formatting errors. If you run into formatting issues login to CDH console and you should see configuration warnings for the affected services. 

