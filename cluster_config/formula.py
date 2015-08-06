# from cluster_config import log

def constans(cluster, constant, log):
    hosts = cluster.yarn.nodemanager.hosts.all()
    max_cores = 0
    max_memory = 0
    for key in hosts:
        if hosts[key].numCores > max_cores:
            max_cores = hosts[key].numCores
        if hosts[key].totalPhysMemBytes > max_memory:
            max_memory = hosts[key].totalPhysMemBytes

    ###### These values are gathered by the tool from Cluster ######
    NUM_NM_WORKERS = len(hosts)
    NM_WORKER_CORES = max_cores
    NM_WOKER_MEM = max_memory

    def num_threads(x, log):
        if x and x > 0:
            return x
        else:
            log.fatal("The number of threads must be at least 1")

    constant.add("NUM_THREADS", lambda x: x if x > 0 else 1)
    constant.add("NUM_THREADS1", *1)

    const = {
        #lambdas are cleaner
        "NUM_THREADS": lambda x: x if x > 0 else 1,
        # but if you need something more robust define a def to assign to the dict member
        "NUM_THREADS1": num_threads,
        "OVER_COMMIT_FACTOR": lambda x: x if x > 0 else 1,
        "MEM_FRACTION_FOR_HBASE": lambda x: x if x > 0 else 0.2,
        "MEM_FRACTION_FOR_OTHER_SERVICES": lambda x: x if x > 0 else 1 - const["MEM_FRACTION_FOR_HBASE"],
        "MAPREDUCE_JOB_COUNTERS_MAX": lambda x: x if x > 0 else 120,
        "SPARK_DRIVER_MAXPERMSIZE": lambda x: x if x > 0 else 512,
        "MAPREDUCE_MAP_MINIMUM_MEMORY_MB": lambda x: x if x > 0 else 512,
        "NUM_NM_WORKERS": NUM_NM_WORKERS,
        "NM_WORKER_CORES": NM_WORKER_CORES,
        "NM_WOKER_MEM": NM_WOKER_MEM,
        "KIB": pow(2,10),
        "MIB": pow(2,20),
        "GIB": pow(2,30),
        "TIB": pow(2,40),
        "MIN_NM_MEMORY": 8 * pow(2,30)
    }

#    const.NUM_THEADS = lambda x: x if x > 0 else 1


    if (max_memory < (const["MIN_NM_MEMORY"])):
        log.fatal("Running the toolkit with less than {0}GB memory for YARN is not supported.".format(const["MIN_NM_MEMORY"]))
    elif (max_memory <= (256 * const["GIB"])):
        MAX_JVM_MEMORY = max_memory / 4  # Java Heap Size should not go over 25% of total memory per node manager
    else:
        MAX_JVM_MEMORY = 64 * const["GIB"]  # for node managers with greater than 256 GB RAM, JVM memory should still be at most 64GB

    if ((MAX_JVM_MEMORY / const["MIB"]) < const["MAPREDUCE_MAP_MINIMUM_MEMORY_MB"]):
        log.fatal("Container larger than {0}MB are not supported".format(MAX_JVM_MEMORY))

def formula():
    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.driver.maxPermSize"] = \
        "\"%dm\"" % (SPARK_DRIVER_MAXPERMSIZE)

    SPARK_YARN_DRIVER_MEMORYOVERHEAD = 384
    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.yarn.driver.memoryOverhead"] = \
        "\"%d\"" % (SPARK_YARN_DRIVER_MEMORYOVERHEAD)

    SPARK_YARN_EXECUTOR_MEMORYOVERHEAD = 384
    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.yarn.executor.memoryOverhead"] = \
        "\"%d\"" % (SPARK_YARN_EXECUTOR_MEMORYOVERHEAD)

    cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.YARN_NODEMANAGER_RESOURCE_CPU_VCORES"] = NM_WORKER_CORES

    cdh["YARN.GATEWAY.GATEWAY_BASE.YARN_APP_MAPREDUCE_AM_RESOURCE_CPU_VCORES"] = 1

    cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_JOB_COUNTERS_LIMIT"] = MAPREDUCE_JOB_COUNTERS_MAX
    cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.NODEMANAGER_MAPRED_SAFETY_VALVE"] = \
        "<property><name>mapreduce.job.counters.max</name><value>%d</value></property>" % (MAPREDUCE_JOB_COUNTERS_MAX)
    cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.RESOURCEMANAGER_MAPRED_SAFETY_VALVE"] = \
        cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.NODEMANAGER_MAPRED_SAFETY_VALVE"]
    cdh["YARN.JOBHISTORY.JOBHISTORY_BASE.JOBHISTORY_MAPRED_SAFETY_VALVE"] = \
        cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.NODEMANAGER_MAPRED_SAFETY_VALVE"]

    MEM_FOR_OTHER_SERVICES = int(NM_WOKER_MEM * MEM_FRACTION_FOR_OTHER_SERVICES)
    MEM_FOR_HBASE_REGION_SERVERS = min(32 * GiB, int(NM_WOKER_MEM * MEM_FRACTION_FOR_HBASE))
    MEM_PER_NM = NM_WOKER_MEM - MEM_FOR_OTHER_SERVICES - MEM_FOR_HBASE_REGION_SERVERS

    cdh["HBASE.REGIONSERVER.REGIONSERVER_BASE.HBASE_REGIONSERVER_JAVA_HEAPSIZE"] = \
        int(MEM_FOR_HBASE_REGION_SERVERS / OVER_COMMIT_FACTOR)

    cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"] = 512

    cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_MAXIMUM_ALLOCATION_MB"] = \
        (
            int(MEM_PER_NM / MiB -
                max(
                    SPARK_DRIVER_MAXPERMSIZE,
                    SPARK_YARN_DRIVER_MEMORYOVERHEAD,
                    SPARK_YARN_EXECUTOR_MEMORYOVERHEAD
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
                            "YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_MAXIMUM_ALLOCATION_MB"] / NM_WORKER_CORES)
                    / cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"]
                ) * cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"],
                MAX_JVM_MEMORY / MiB
            ), MAPREDUCE_MAP_MINIMUM_MEMORY_MB
        )

    cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_REDUCE_MEMORY_MB"] = \
        min(
            2 * cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"],
            MAX_JVM_MEMORY / MiB
        )

    cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_JAVA_OPTS_MAX_HEAP"] = \
        int(cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"] / OVER_COMMIT_FACTOR) * MiB

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
                        SPARK_YARN_DRIVER_MEMORYOVERHEAD,
                        SPARK_YARN_EXECUTOR_MEMORYOVERHEAD,
                        cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"]
                    ) / cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"]
                ) * cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"]
            )
        ) * NUM_NM_WORKERS

    if NUM_THREADS > (CONTAINERS_ACCROSS_CLUSTER / 2):
        log.fatal("Number of concurrent threads should be at most {0}"
                  .format((min(CONTAINERS_ACCROSS_CLUSTER, CONTAINERS_ACCROSS_CLUSTER) / 2))
        )

    log.info("{0} could be as large as {1} for multi-tenacty".format("NUM_THREADS", (CONTAINERS_ACCROSS_CLUSTER / 2)))

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.yarn.numExecutors"] = \
        int((CONTAINERS_ACCROSS_CLUSTER - NUM_THREADS) / NUM_THREADS)

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.executor.memory"] = \
        "\"%dm\"" % (cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"])

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.executor.cores"] = \
        (cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.YARN_NODEMANAGER_RESOURCE_CPU_VCORES"] * NUM_NM_WORKERS - NUM_THREADS) \
        / (NUM_THREADS * atk["trustedanalytics.atk.engine.spark.conf.properties.spark.yarn.numExecutors"])

    atk["trustedanalytics.atk.engine.spark.conf.properties.spark.driver.memory"] = \
        "\"%dm\"" % (cdh["YARN.GATEWAY.GATEWAY_BASE.YARN_APP_MAPREDUCE_AM_RESOURCE_MB"])

    atk["trustedanalytics.atk.engine.giraph.mapreduce.map.memory.mb"] = \
        cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"]

    atk["trustedanalytics.atk.engine.giraph.giraph.maxWorkers"] = \
        atk["trustedanalytics.atk.engine.spark.conf.properties.spark.yarn.numExecutors"]

    atk["trustedanalytics.atk.engine.giraph.mapreduce.map.java.opts.max.heap"] = \
        "\"-Xmx%sm\"" % (cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_JAVA_OPTS_MAX_HEAP"] / MiB)

