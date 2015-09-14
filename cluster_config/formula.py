def constants(cluster, log):
    """
    Sets the constants needed for calculating the formula.
    :param cluster: Cluster configuration connection
    :param log: simple log interface with log.info, log.error, log.warning, log.fatal, log.debug
    :return: a dictionary with all constants
    """
    const = {
        "NUM_NM_WORKERS": len(cluster.yarn.nodemanager.hosts.all()),
        "NM_WORKER_CORES": cluster.yarn.nodemanager.hosts.max_cores(),
        "NM_WORKER_MEM": cluster.yarn.nodemanager.hosts.max_memory(),
        "MIN_NM_MEMORY": gb_to_bytes(8),
        # lambdas are cleaner
        "OVER_COMMIT_FACTOR": lambda x: x if x is not None and x >= 1 else 1,
        "MEM_FRACTION_FOR_HBASE": lambda x: x if x is not None and x >= 0 and x < 1 else 0.125,
        "MEM_FRACTION_FOR_OTHER_SERVICES": lambda x: x if x is not None and x >= 0 and x < 1 else 0.125,
        "MAPREDUCE_JOB_COUNTERS_MAX": lambda x: x if x is not None and x >= 120 else 500,
        "SPARK_DRIVER_MAXPERMSIZE": lambda x: x if x is not None and x >= 512 else 512,
        "ENABLE_SPARK_SHUFFLE_SERVICE": lambda x: False if str(x).lower() == "false" else True,
        "ZOOKEEPER_IS_EXTERNAL": lambda x: False if str(x).lower() == "false" else True,
        "NUM_THREADS": lambda x: x if x is not None and x > 0 else 1,
        "YARN_SCHEDULER_MINIMUM_ALLOCATION_MB": lambda x: x if x is not None and x >= 1024 else 1024,
        "MAPREDUCE_MINIMUM_AM_MEMORY_MB": lambda x: (x / 512) * 512 if x is not None and x >= 1024 else 1024,
        "MAPREDUCE_MINIMUM_EXECUTOR_MEMORY_MB": lambda x: (x / 512) * 512 if x is not None and x >= 1024 else 1024,
        "ENABLE_DYNAMIC_ALLOCATION_FOR_SPARK": lambda x: False if str(x).lower() == "false" else True
    }

    if (const["NM_WORKER_MEM"] < (const["MIN_NM_MEMORY"])):
        log.fatal(
            "Running the toolkit with less than {0}GB memory for YARN is not supported.".format(const["MIN_NM_MEMORY"]))
    elif (const["NM_WORKER_MEM"] <= (gb_to_bytes(256))):
        # Java Heap Size should not go over 25% of total memory per node manager
        const["MAX_JVM_MEMORY"] = const["NM_WORKER_MEM"] / 4
    else:
        # for node managers with greater than 256 GB RAM, JVM memory should still be at most 64GB
        const["MAX_JVM_MEMORY"] = gb_to_bytes(64)

    return const


def formula(cluster, log, constants):
    """
    Houses the formula for calculating the optimized formula
    :param cluster: Cluster configuration connection
    :param  log: simple log interface with log.info, log.error, log.warning, log.fatal, log.debug
    :param constants: the calculated constants with any user overrides from formula-args
    :return: a dictionary with cdh and atk configurations
    """
    cdh = {}
    atk = {}

    # Validate user defined parameters in forumual-args.yaml file
    if (bytes_to_mb(constants["MAX_JVM_MEMORY"]) < constants["MAPREDUCE_MINIMUM_EXECUTOR_MEMORY_MB"]):
        log.warning("Container larger than {0}MB are not supported".format(constants["MAX_JVM_MEMORY"]))

    if (constants["MEM_FRACTION_FOR_OTHER_SERVICES"] < 0 or (
                constants["MEM_FRACTION_FOR_OTHER_SERVICES"] >= (1 - constants["MEM_FRACTION_FOR_HBASE"]))):
        log.fatal("{0} must be non-nagative and smaller than {1}".format("MEM_FRACTION_FOR_OTHER_SERVICES",
                                                                         1 - constants["MEM_FRACTION_FOR_HBASE"]))
    constants["SPARK_YARN_DRIVER_MEMORYOVERHEAD"] = max(384, constants["MAPREDUCE_MINIMUM_AM_MEMORY_MB"] * 0.07)
    constants["SPARK_YARN_EXECUTOR_MEMORYOVERHEAD"] = max(384, constants["MAPREDUCE_MINIMUM_EXECUTOR_MEMORY_MB"] * 0.07)

    ###### These values are gathered by the tool from Cluster ######
    if (constants["ZOOKEEPER_IS_EXTERNAL"]):
        atk["trustedanalytics.atk.engine.giraph.giraph.zkIsExternal"] = "true"
        ZK_LIST = map(lambda x: x.hostname, cluster.zookeeper.server.hosts.all().values())
        atk["trustedanalytics.atk.engine.giraph.giraph.zkList"] = "\"%s\"" % (",".join(ZK_LIST))

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.driver.maxPermSize"] = \
        "\"%dm\"" % (constants["SPARK_DRIVER_MAXPERMSIZE"])

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.yarn.driver.memoryOverhead"] = \
        "\"%d\"" % (constants["SPARK_YARN_DRIVER_MEMORYOVERHEAD"])

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.yarn.executor.memoryOverhead"] = \
        "\"%d\"" % (constants["SPARK_YARN_EXECUTOR_MEMORYOVERHEAD"])

    cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.YARN_NODEMANAGER_RESOURCE_CPU_VCORES"] = constants["NM_WORKER_CORES"]

    cdh["YARN.GATEWAY.GATEWAY_BASE.YARN_APP_MAPREDUCE_AM_RESOURCE_CPU_VCORES"] = 1

    cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_JOB_COUNTERS_LIMIT"] = constants["MAPREDUCE_JOB_COUNTERS_MAX"]

    cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.NODEMANAGER_MAPRED_SAFETY_VALVE"] = \
        "<property><name>mapreduce.job.counters.max</name><value>%d</value></property>" % (
            constants["MAPREDUCE_JOB_COUNTERS_MAX"])

    cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.RESOURCEMANAGER_MAPRED_SAFETY_VALVE"] = \
        cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.NODEMANAGER_MAPRED_SAFETY_VALVE"]

    cdh["YARN.JOBHISTORY.JOBHISTORY_BASE.JOBHISTORY_MAPRED_SAFETY_VALVE"] = \
        cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.NODEMANAGER_MAPRED_SAFETY_VALVE"]

    MEM_FOR_OTHER_SERVICES = int(constants["NM_WORKER_MEM"] * constants["MEM_FRACTION_FOR_OTHER_SERVICES"])
    MEM_FOR_HBASE_REGION_SERVERS = min(gb_to_bytes(32),
                                       int(constants["NM_WORKER_MEM"] * constants["MEM_FRACTION_FOR_HBASE"]))
    MEM_PER_NM = constants["NM_WORKER_MEM"] - MEM_FOR_OTHER_SERVICES - MEM_FOR_HBASE_REGION_SERVERS

    cdh["HBASE.REGIONSERVER.REGIONSERVER_BASE.HBASE_REGIONSERVER_JAVA_HEAPSIZE"] = \
        int(MEM_FOR_HBASE_REGION_SERVERS * 0.75)

    cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"] = 512

    cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_MAXIMUM_ALLOCATION_MB"] = \
        (
            int(bytes_to_mb(MEM_PER_NM) -
                max(
                    constants["SPARK_DRIVER_MAXPERMSIZE"],
                    constants["SPARK_YARN_DRIVER_MEMORYOVERHEAD"],
                    constants["SPARK_YARN_EXECUTOR_MEMORYOVERHEAD"]
                ) * 3
            ) / cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"]
        ) * cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"]

    cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.YARN_NODEMANAGER_RESOURCE_MEMORY_MB"] = \
        cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_MAXIMUM_ALLOCATION_MB"]

    cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"] = \
        max(
            min(
                (
                    (
                        cdh[
                            "YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_MAXIMUM_ALLOCATION_MB"] /
                        constants["NM_WORKER_CORES"])
                    / cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"]
                ) * cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"],
                bytes_to_mb(constants["MAX_JVM_MEMORY"])
            ), constants["MAPREDUCE_MINIMUM_EXECUTOR_MEMORY_MB"]
        )

    cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_REDUCE_MEMORY_MB"] = \
        2 * min(
            cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"],
            bytes_to_mb(constants["MAX_JVM_MEMORY"])
        )

    cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_JAVA_OPTS_MAX_HEAP"] = \
        mb_to_bytes(int(cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"] * 0.75))

    cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_REDUCE_JAVA_OPTS_MAX_HEAP"] = \
        2 * cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_JAVA_OPTS_MAX_HEAP"]

    cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_MINIMUM_ALLOCATION_MB"] = \
        constants["YARN_SCHEDULER_MINIMUM_ALLOCATION_MB"]

    cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_MAXIMUM_ALLOCATION_VCORES"] = \
        cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.YARN_NODEMANAGER_RESOURCE_CPU_VCORES"]

    cdh["YARN.GATEWAY.GATEWAY_BASE.YARN_APP_MAPREDUCE_AM_RESOURCE_MB"] = \
        constants["MAPREDUCE_MINIMUM_AM_MEMORY_MB"]

    cdh["YARN.GATEWAY.GATEWAY_BASE.YARN_APP_MAPREDUCE_AM_MAX_HEAP"] = \
        mb_to_bytes(
            int(cdh["YARN.GATEWAY.GATEWAY_BASE.YARN_APP_MAPREDUCE_AM_RESOURCE_MB"] * 0.75))

    CONTAINERS_ACCROSS_CLUSTER = \
        int(cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.YARN_NODEMANAGER_RESOURCE_MEMORY_MB"] \
            / (
            (
                cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"] + (
                    2 *
                    max(
                        constants["SPARK_YARN_DRIVER_MEMORYOVERHEAD"],
                        constants["SPARK_YARN_EXECUTOR_MEMORYOVERHEAD"],
                        cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"]
                    ) / cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"]
                ) * cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"]
            )
        ) * constants["NUM_NM_WORKERS"])

    if constants["NUM_THREADS"] > (CONTAINERS_ACCROSS_CLUSTER / 2):
        log.fatal("Number of concurrent threads should be at most {0}"
                  .format((min(CONTAINERS_ACCROSS_CLUSTER, CONTAINERS_ACCROSS_CLUSTER) / 2))
        )

    log.info("{0} could be as large as {1} for multi-tenacty".format("NUM_THREADS", (CONTAINERS_ACCROSS_CLUSTER / 2)))

    EXECUTORS_PER_THREAD = int((CONTAINERS_ACCROSS_CLUSTER - constants["NUM_THREADS"]) / constants["NUM_THREADS"])

    if not constants["ENABLE_DYNAMIC_ALLOCATION_FOR_SPARK"]:
        atk["trustedanalytics.atk.engine.spark.conf.properties.spark.dynamicAllocation.enabled"] = "false"
        atk["trustedanalytics.atk.engine.spark.conf.properties.spark.executor.instances"] = EXECUTORS_PER_THREAD
    else:
        atk["trustedanalytics.atk.engine.spark.conf.properties.spark.dynamicAllocation.enabled"] = "true"

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.dynamicAllocation.minExecutors"] = 1
    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.dynamicAllocation.maxExecutors"] = \
        CONTAINERS_ACCROSS_CLUSTER - constants["NUM_THREADS"]

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.executor.memory"] = \
        "\"%dm\"" % (
            cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"] -
            int(atk["trustedanalytics.atk.engine.spark.conf.properties.spark.yarn.executor.memoryOverhead"].strip("\""))
        )

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.driver.cores"] = 1

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.executor.cores"] = \
        (cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.YARN_NODEMANAGER_RESOURCE_CPU_VCORES"] * constants["NUM_NM_WORKERS"] -
         constants["NUM_THREADS"]) \
        / (constants["NUM_THREADS"] * EXECUTORS_PER_THREAD)

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.driver.memory"] = \
        "\"%dm\"" % (
            cdh["YARN.GATEWAY.GATEWAY_BASE.YARN_APP_MAPREDUCE_AM_RESOURCE_MB"] -
            int(atk["trustedanalytics.atk.engine.spark.conf.properties.spark.yarn.driver.memoryOverhead"].strip("\""))
        )

    atk["trustedanalytics.atk.engine.giraph.mapreduce.map.memory.mb"] = \
        cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"]

    atk["trustedanalytics.atk.engine.giraph.giraph.maxWorkers"] = \
        EXECUTORS_PER_THREAD - 1

    atk["trustedanalytics.atk.engine.giraph.mapreduce.map.java.opts.max.heap"] = \
        "\"-Xmx%sm\"" % (bytes_to_mb(cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_JAVA_OPTS_MAX_HEAP"]))

    atk["trustedanalytics.atk.engine.auto-partitioner.broadcast-join-threshold"] = "\"2048MB\""

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.driver.maxResultSize"] = "\"2g\""

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.shuffle.io.preferDirectBufs"] = "false"

    if (constants["ENABLE_SPARK_SHUFFLE_SERVICE"]):
        cdh["YARN.JOBHISTORY.JOBHISTORY_BASE.JOBHISTORY_CONFIG_SAFETY_VALVE"] = \
            "{0}\n{1}".format(
                "<property>"
                "<name>""yarn.nodemanager.aux-services""</name>"
                "<value>""spark_shuffle,mapreduce_shuffle""</value>"
                "</property>",
                "<property>"
                "<name>yarn.nodemanager.aux-services.spark_shuffle.class</name>"
                "<value>""org.apache.spark.network.yarn.YarnShuffleService</value>"
                "</property>")
        cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.NODEMANAGER_CONFIG_SAFETY_VALVE"] = \
            cdh["YARN.JOBHISTORY.JOBHISTORY_BASE.JOBHISTORY_CONFIG_SAFETY_VALVE"]
        cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.RESOURCEMANAGER_CONFIG_SAFETY_VALVE"] = \
            cdh["YARN.JOBHISTORY.JOBHISTORY_BASE.JOBHISTORY_CONFIG_SAFETY_VALVE"]

        atk["trustedanalytics.atk.engine.spark.conf.properties.spark.shuffle.service.enabled"] = "true"
    else:
        atk["trustedanalytics.atk.engine.spark.conf.properties.spark.shuffle.service.enabled"] = "false"

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.yarn.am.waitTime"] = "1000000"

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.driver.extraJavaOptions"] = \
        "\"-Xmx%sm\"" % (bytes_to_mb(cdh["YARN.GATEWAY.GATEWAY_BASE.YARN_APP_MAPREDUCE_AM_MAX_HEAP"]))

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.executor.extrajavaoptions"] = \
        "\"-Xmx%sm\"" % (bytes_to_mb(cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_JAVA_OPTS_MAX_HEAP"]))

    return {"cdh": cdh, "atk": atk}
