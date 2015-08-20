# from cluster_config import log

def constants(cluster, log):
    ###### These values are gathered by the tool from Cluster ######
    NUM_NM_WORKERS = len(cluster.yarn.nodemanager.hosts.all())
    NM_WORKER_CORES = cluster.yarn.nodemanager.hosts.max_cores()
    NM_WOKER_MEM = cluster.yarn.nodemanager.hosts.max_memory()

    const = {
        #lambdas are cleaner
        "NUM_THREADS": lambda x: x if x is not None and x > 0 else 1,
        # but if you need something more robust define a def to assign to the dict member
        "OVER_COMMIT_FACTOR": lambda x: x if x is not None and x > 0 else 1,
        "MEM_FRACTION_FOR_HBASE": lambda x: x if x is not None and x > 0 else 0.2,
        "MEM_FRACTION_FOR_OTHER_SERVICES": lambda x: x if x is not None and x > 0 else 1 - const["MEM_FRACTION_FOR_HBASE"],
        "MAPREDUCE_JOB_COUNTERS_MAX": lambda x: x if x is not None and x > 0 else 120,
        "SPARK_DRIVER_MAXPERMSIZE": lambda x: x if x is not None and x > 0 else 512,
        "MAPREDUCE_MAP_MINIMUM_MEMORY_MB": lambda x: x if x is not None and x > 0 else 512,
        "NUM_NM_WORKERS": NUM_NM_WORKERS,
        "NM_WORKER_CORES": NM_WORKER_CORES,
        "NM_WOKER_MEM": NM_WOKER_MEM,
        "SPARK_YARN_DRIVER_MEMORYOVERHEAD": 384,
        "SPARK_YARN_EXECUTOR_MEMORYOVERHEAD": 384,
        "MIN_NM_MEMORY": GB_to_bytes(8)
    }

    if (NM_WOKER_MEM < (const["MIN_NM_MEMORY"])):
        log.fatal("Running the toolkit with less than {0}GB memory for YARN is not supported.".format(const["MIN_NM_MEMORY"]))
    elif (NM_WOKER_MEM <= (GB_to_bytes(256))):
        MAX_JVM_MEMORY = NM_WOKER_MEM / 4  # Java Heap Size should not go over 25% of total memory per node manager
    else:
        MAX_JVM_MEMORY = GB_to_bytes(64)# for node managers with greater than 256 GB RAM, JVM memory should still be at most 64GB

    #if (bytes_to_MB(MAX_JVM_MEMORY) < const["MAPREDUCE_MAP_MINIMUM_MEMORY_MB"]):
        #log.warning("Container larger than {0}MB are not supported".format(MAX_JVM_MEMORY))

    const["MAX_JVM_MEMORY"] = MAX_JVM_MEMORY

    return const

def formula(cluster, log, constants):
    cdh = {}
    atk = {}

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
        "<property><name>mapreduce.job.counters.max</name><value>%d</value></property>" % (constants["MAPREDUCE_JOB_COUNTERS_MAX"])
    cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.RESOURCEMANAGER_MAPRED_SAFETY_VALVE"] = \
        cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.NODEMANAGER_MAPRED_SAFETY_VALVE"]
    cdh["YARN.JOBHISTORY.JOBHISTORY_BASE.JOBHISTORY_MAPRED_SAFETY_VALVE"] = \
        cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.NODEMANAGER_MAPRED_SAFETY_VALVE"]

    MEM_FOR_OTHER_SERVICES = int(constants["NM_WOKER_MEM"] * constants["MEM_FRACTION_FOR_OTHER_SERVICES"])
    MEM_FOR_HBASE_REGION_SERVERS = min(GB_to_bytes(32), int(constants["NM_WOKER_MEM"] * constants["MEM_FRACTION_FOR_HBASE"]))
    MEM_PER_NM = constants["NM_WOKER_MEM"] - MEM_FOR_OTHER_SERVICES - MEM_FOR_HBASE_REGION_SERVERS

    cdh["HBASE.REGIONSERVER.REGIONSERVER_BASE.HBASE_REGIONSERVER_JAVA_HEAPSIZE"] = \
        int(MEM_FOR_HBASE_REGION_SERVERS / constants["OVER_COMMIT_FACTOR"])

    cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"] = 512

    cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_MAXIMUM_ALLOCATION_MB"] = \
        (
            int(bytes_to_MB(MEM_PER_NM) -
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
                            "YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_MAXIMUM_ALLOCATION_MB"] / constants["NM_WORKER_CORES"])
                    / cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"]
                ) * cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"],
                bytes_to_MB(constants["MAX_JVM_MEMORY"])
            ), constants["MAPREDUCE_MAP_MINIMUM_MEMORY_MB"]
        )

    cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_REDUCE_MEMORY_MB"] = \
        min(
            2 * cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"],
            bytes_to_MB(constants["MAX_JVM_MEMORY"])
        )

    cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_JAVA_OPTS_MAX_HEAP"] = \
        int(bytes_to_MB(cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"] / constants["OVER_COMMIT_FACTOR"]))

    cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_REDUCE_JAVA_OPTS_MAX_HEAP"] = \
        2 * cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_JAVA_OPTS_MAX_HEAP"]

    cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_MINIMUM_ALLOCATION_MB"] = \
        cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"]

    cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_MAXIMUM_ALLOCATION_VCORES"] = \
        cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.YARN_NODEMANAGER_RESOURCE_CPU_VCORES"]

    cdh["YARN.GATEWAY.GATEWAY_BASE.YARN_APP_MAPREDUCE_AM_RESOURCE_MB"] = \
        cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"]

    cdh["YARN.GATEWAY.GATEWAY_BASE.YARN_APP_MAPREDUCE_AM_MAX_HEAP"] = \
        cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_JAVA_OPTS_MAX_HEAP"]

    CONTAINERS_ACCROSS_CLUSTER = \
        cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.YARN_NODEMANAGER_RESOURCE_MEMORY_MB"] \
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
        ) * constants["NUM_NM_WORKERS"]

    if constants["NUM_THREADS"] > (CONTAINERS_ACCROSS_CLUSTER / 2):
        log.fatal("Number of concurrent threads should be at most {0}"
                  .format((min(CONTAINERS_ACCROSS_CLUSTER, CONTAINERS_ACCROSS_CLUSTER) / 2))
        )

    log.info("{0} could be as large as {1} for multi-tenacty".format("NUM_THREADS", (CONTAINERS_ACCROSS_CLUSTER / 2)))

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.yarn.numExecutors"] = \
        int((CONTAINERS_ACCROSS_CLUSTER - constants["NUM_THREADS"]) / constants["NUM_THREADS"])

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.executor.memory"] = \
        "\"%dm\"" % (cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"])

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.executor.cores"] = \
        (cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.YARN_NODEMANAGER_RESOURCE_CPU_VCORES"] * constants["NUM_NM_WORKERS"] - constants["NUM_THREADS"]) \
        / (constants["NUM_THREADS"] * atk["trustedanalytics.atk.engine.spark.conf.properties.spark.yarn.numExecutors"])

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.driver.memory"] = \
        "\"%dm\"" % (cdh["YARN.GATEWAY.GATEWAY_BASE.YARN_APP_MAPREDUCE_AM_RESOURCE_MB"])

    atk["trustedanalytics.atk.engine.giraph.mapreduce.map.memory.mb"] = \
        cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"]

    atk["trustedanalytics.atk.engine.giraph.giraph.maxWorkers"] = \
        atk["trustedanalytics.atk.engine.spark.conf.properties.spark.yarn.numExecutors"]

    atk["trustedanalytics.atk.engine.giraph.mapreduce.map.java.opts.max.heap"] = \
        "\"-Xmx%sm\"" % (bytes_to_MB(cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_JAVA_OPTS_MAX_HEAP"]))

    return {"cdh": cdh, "atk": atk}