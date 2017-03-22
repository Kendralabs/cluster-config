---
title: Usage
tags: [design]
keywords: cdh design
last_updated: March 16, 2017
datatable: true
summary: "Understanding how Cluster-config interacts with the cluster."
sidebar: cluster_config_sidebar
permalink: cluster_config_architecture.html
folder: cluster_config
---

## Design

Cluster config never talks directly to any server node. All communication is done dire

Cluster config is sperated into several modules. The three top level modules that most users will interact with are  generating configurations, pushing configurations, and exploring configurations.
The CDH module is used for wrapping many of the cm-api objects into more digestable function calls and laslty the util modul
Three main functions can be perfomed with Cluster-config, generating configurations, pushing configurations, and exploring configurations.

Cluster-config interacts with CDH clusters solely through the [cm-api](https://cloudera.github.io/cm_api/). 
Formula authors don't need worry about underlying connection to Cloudera manager beacuse it will settled long before the formula is executed. 
All configurations for every service will be avaible.


Use fenced code blocks with the language specified, like this:

    ```js
    console.log('hello');
    ````

**Result:**

```js
console.log('hello');
```

For the list of supported languages you can use (similar to `js` for JavaScript), see [Supported languages](https://github.com/jneen/rouge/wiki/list-of-supported-languages-and-lexers).
