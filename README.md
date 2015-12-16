cluster_config
==========
The cluster configuration tool allows easy updating and optimization of any CDH cluster from the command line. With the use of optimization formulas you can have an optimized CDH cluster in a matter of seconds.

The tool does 3 things:

1. Generates optimized configurations based on your cluster hardware
2. Automates updating, restarting, and configuration deployment of CDH services
3. Simplifies exploration of CDH cluster configurations

##Requirements
1. Python 2.7
2. [argparse](https://docs.python.org/2.7/library/argparse.html) >= 1.3.0
3. [cm-api](https://github.com/cloudera/cm_api) == 10.0.0
4. Working CDH cluster for running the script

##installation

Until the pip package is hosted, you will need to clone and install directly from source.

```
git clone git@github.com:trustedanalytics/cluster-config.git
cd cluster-config
[sudo] python setup.py install
```

##wiki docs

[Look over the repos wiki if you have any questions](../../wiki)

