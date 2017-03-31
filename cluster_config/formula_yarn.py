'''
Calculate an optimized configuration based on overall memory, cpu, and sensible defaults.

Will configure spark and yarn.

The formula will be run on 5 node t2.large cluster hosted in AWS. t2.large nodes have 2 vcpus, and 8gb of memory each.
4 nodes are designed as yarn node managers. The formula will run with default values.
'''

def constants(cluster, log):
    '''
    Sets the constants needed for calculating the formula.
    :param cluster: Cluster configuration connection
    :param log: simple log interface with log.info, log.error, log.warning, log.fatal, log.debug
    :return: a dictionary with all constants
    '''

    const = {
        "NUM_NM_WORKERS": len(cluster.yarn.nodemanager.hosts.all),
        #The number of workers with the node manager role
        #4 in our sample cluster

        "NM_WORKER_CORES": cluster.yarn.nodemanager.hosts.max_cores,
        #The max number of cores for any node manager host
        # 2 in our sample cluster

        "NM_WORKER_MEM": cluster.yarn.nodemanager.hosts.max_memory,
        #The max amount of memory available on any node manager host
        # 4gb or 7933714432 bytes in our sample cluster

        "MIN_NM_MEMORY": gb_to_bytes(2),


        "MEM_FRACTION_FOR_OTHER_SERVICES": lambda x: x if x is not None and x >= 0 and x < 1 else 0.125,
        #percentage of overall system memory that should be preserved for other services besides yarn.
        #Default is 12.5 %

        "MAPREDUCE_JOB_COUNTERS_MAX": lambda x: x if x is not None and x >= 120 else 500,
        #max number number of map reduce counters. The yarn default is 120, we recommend 500.


        "SPARK_DRIVER_MAXPERMSIZE": lambda x: x if x is not None and x >= 512 else 512,
        #The spark drivers max perm size
        #spark-submit --driver-java-options " -XX:MaxPermSize=SOME_SIZE M "

        "ENABLE_SPARK_SHUFFLE_SERVICE": lambda x: False if str(x).lower() == "false" else True,
        #Enable spark shuffle service so we can use dynamic allocation

        "NUM_THREADS": lambda x: x if x is not None and x > 0 else 1,
        #The number of threads being supported by this configuration

        "YARN_SCHEDULER_MINIMUM_ALLOCATION_MB": lambda x: x if x is not None and x >= 1024 else 1024,


        "MAPREDUCE_MINIMUM_AM_MEMORY_MB": lambda x: (x / 512) * 512 if x is not None and x >= 1024 else 1024,
        #The amount of memory the MR AppMaster needs.
        #https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml

        "MAX_HEAP_PERCENT": lambda x: x if x is not None else 0.75,
        #how much memory should be allocated to java heap. i.e. mapreduce.map.memory.mb * 0.75,
        # mapreduce.reduce.java.opts.max.heap * 0.75

        "MAPREDUCE_MINIMUM_EXECUTOR_MEMORY_MB": lambda x: (x / 512) * 512 if x is not None and x >= 1024 else 1024,
        #minimum executor memory

        "SPARK_MEMORY_OVERHEAD_MIN": 384,
        #minimum amount of memory allocated for spark overhead

        "MEMORY_OVERHEAD": 0.10,
        #The amount of off-heap memory (in megabytes) to be allocated for
        # spark.yarn.executor.memoryOverhead, spark.yarn.driver.memoryOverhead, spark.yarn.am.memoryOverhead.
        # variable from 6 to 10 percent
        # http://spark.apache.org/docs/latest/running-on-yarn.html#spark-properties

        "YARN_INCREMENT_ALLOCATION_MB": 512
        #memory increment when requesting containers
    }

    if (const["NM_WORKER_MEM"] < (const["MIN_NM_MEMORY"])):
        log.fatal(
            "Running the toolkit with less than {0}GB memory for YARN is not supported.".format(const["MIN_NM_MEMORY"]))
    elif (const["NM_WORKER_MEM"] <= (gb_to_bytes(256))):

        const["MAX_JVM_MEMORY"] = const["NM_WORKER_MEM"] / 4
        '''
        Java Heap Size should not go over 25% of total memory per node manager
        In our sample cluster this would be 7933714432 / 4 = 1983428608
        '''
    else:
        # for node managers with greater than 256 GB RAM, JVM memory should still be at most 64GB
        const["MAX_JVM_MEMORY"] = gb_to_bytes(64)

    return const

def formula(cluster, log, constants):
    """
    formula for calculating the optimized configuration
    :param cluster: Cluster configuration connection
    :param  log: simple log interface with log.info, log.error, log.warning, log.fatal, log.debug
    :param constants: the calculated constants with any user overrides from formula-args
    :return: a dictionary with cdh configurations
    """
    cdh = {}

    # Validate user defined parameters in forumual-args.yaml file
    if (bytes_to_mb(constants["MAX_JVM_MEMORY"]) < constants["MAPREDUCE_MINIMUM_EXECUTOR_MEMORY_MB"]):
        '''
        Make sure the user provided value is within the bounds of the cluster resources.
        '''
        log.warning("Container larger than {0}MB are not supported".format(constants["MAX_JVM_MEMORY"]))

    if constants["MEM_FRACTION_FOR_OTHER_SERVICES"] < 0:
        log.fatal("{0} must be non-nagative".format("MEM_FRACTION_FOR_OTHER_SERVICES"))

    constants["SPARK_YARN_DRIVER_MEMORYOVERHEAD"] = \
        max(constants["SPARK_MEMORY_OVERHEAD_MIN"], constants["MAPREDUCE_MINIMUM_AM_MEMORY_MB"] * constants["MEMORY_OVERHEAD"])
    '''
    6-10 percent of driver memory with a minimum of 384

    in our sample cluster
    max(384, 1024 * 0.10) = 384
    '''

    constants["SPARK_YARN_EXECUTOR_MEMORYOVERHEAD"] = \
        max(constants["SPARK_MEMORY_OVERHEAD_MIN"], constants["MAPREDUCE_MINIMUM_EXECUTOR_MEMORY_MB"] * constants["MEMORY_OVERHEAD"])
    '''
    6-10 percent of executor memory with a minimum of 384

    in our sample cluster
    max(384, 1024 * 0.10  ) = 384
    '''

    cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.YARN_NODEMANAGER_RESOURCE_CPU_VCORES"] = constants["NM_WORKER_CORES"]
    '''
    yarn.nodemanager.resource.cpu-vcores

    Number of CPU cores that can be allocated for containers. Typically set to the number of physical cores on each machine.

    in our sample cluster
    2
    '''

    cdh["YARN.GATEWAY.GATEWAY_BASE.YARN_APP_MAPREDUCE_AM_RESOURCE_CPU_VCORES"] = 1
    '''
    yarn.app.mapreduce.am.resource.cpu-vcores

    The number of virtual cores the mapreduce application master needs
    '''

    cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_JOB_COUNTERS_LIMIT"] = constants["MAPREDUCE_JOB_COUNTERS_MAX"]
    '''
    mapreduce.job.counters.limit

    yarns defaults is 120 our default is 500.

    Limit on the number of user counters allowed per job.
    (https://hadoop.apache.org/docs/r2.6.0/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml)
    '''

    cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.NODEMANAGER_MAPRED_SAFETY_VALVE"] = \
        "<property><name>mapreduce.job.counters.max</name><value>%d</value></property>" % (
            constants["MAPREDUCE_JOB_COUNTERS_MAX"])
    '''
    mapreduce.job.counters.max

    yarns defaults is 120 our default is 500. This sets the old max counters value from hadoop 1.x.

    Limit on the number of user counters allowed per job.
    (https://hadoop.apache.org/docs/r2.4.1/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml)
    '''

    cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.RESOURCEMANAGER_MAPRED_SAFETY_VALVE"] = \
        cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.NODEMANAGER_MAPRED_SAFETY_VALVE"]
    '''
    Set the same old counters max value for resource managers
    '''

    cdh["YARN.JOBHISTORY.JOBHISTORY_BASE.JOBHISTORY_MAPRED_SAFETY_VALVE"] = \
        cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.NODEMANAGER_MAPRED_SAFETY_VALVE"]
    '''
    Set the same old counters max value for job history server
    '''

    MEM_FOR_OTHER_SERVICES = int(constants["NM_WORKER_MEM"] * constants["MEM_FRACTION_FOR_OTHER_SERVICES"])
    '''
    Memory reserved for other cluster services.
    the default will be calucation is total worker memory * 0.125

    in our sample cluster
    (7933714432 * 0.125 )= 991714304
    '''


    MEM_PER_NM = constants["NM_WORKER_MEM"] - MEM_FOR_OTHER_SERVICES
    '''
    Total worker memory minus the percentage reserved for other services

    in our sample cluster
    7933714432 - 991714304 = 6942000128
    '''

    cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"] = constants["YARN_INCREMENT_ALLOCATION_MB"]
    '''
    yarn.scheduler.increment-allocation-mb

    https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-common/yarn-default.xml

    in our sample cluster
    512
    '''

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
    '''
    yarn.scheduler.maximum-allocation-mb

    take available worker memory account for memory overheads and round to the nearest yarn.scheduler.increment-allocation-mb.

    (https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-common/yarn-default.xml)

    in our sample cluster
    ( int( 6620 - max( 512, 512, 384) * 3 )/ 512 ) * 512 = 4608
    '''

    cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.YARN_NODEMANAGER_RESOURCE_MEMORY_MB"] = \
        cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_MAXIMUM_ALLOCATION_MB"]
    '''
    yarn.nodemanager.resource.memory-mb

    Amount of physical memory, in MB, that can be allocated for containers. we use the previous calculation from
    yarn.scheduler.maximum-allocation-mb

    https://hadoop.apache.org/docs/r2.6.1/hadoop-yarn/hadoop-yarn-common/yarn-default.xml

    in our sample cluster
    4608
    '''

    cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"] = \
        max(
            min((
                    (cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_MAXIMUM_ALLOCATION_MB"] /constants["NM_WORKER_CORES"]) /
                    cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"]) *
                cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_INCREMENT_ALLOCATION_MB"],
                bytes_to_mb(constants["MAX_JVM_MEMORY"])
            ),
            constants["MAPREDUCE_MINIMUM_EXECUTOR_MEMORY_MB"]
        )
    '''
    mapreduce.map.memory.mb

    The amount of memory to request from the scheduler for each map task.

    https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml

    result,MAPREDUCE_MINIMUM_EXECUTOR_MEMORY_MB

    In our sample cluster
    max( min( ( ( 4608/4 ) / 512 ) * 512, 1891), 1024) = 1891
    '''


    cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_REDUCE_MEMORY_MB"] = \
        2 * min(
            cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"],
            bytes_to_mb(constants["MAX_JVM_MEMORY"])
        )
    '''
    mapreduce.reduce.memory.mb

    The amount of memory to request from the scheduler for each reduce task.

    (https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml)

    in our sample cluster
    2 * min( 1891, 1891 ) = 3782
    '''

    cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_JAVA_OPTS_MAX_HEAP"] = \
        mb_to_bytes(int(cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"] * constants["MAX_HEAP_PERCENT"]))
    '''
    mapreduce.map.java.opts.max.heap

    max java heap size for the map task. Standard practice is to make this 75 percent of mapreduce.reduce.memory.mb.

    in our sample cluster
    mb_to_bytes(int( 1891 * 0.75 )) = 1486880768 bytes or 1418 MB
    '''


    cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_REDUCE_JAVA_OPTS_MAX_HEAP"] = \
        2 * cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_JAVA_OPTS_MAX_HEAP"]
    '''
    mapreduce.reduce.java.opts.max.heap

    max java heap size for the reduce task. Standard practice is to make this 75 percent of mapreduce.reduce.java.opts.max.heap

    in our sample cluster
    2 * 1486880768 = 2973761536 or 2836
    '''

    cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_MINIMUM_ALLOCATION_MB"] = \
        constants["YARN_SCHEDULER_MINIMUM_ALLOCATION_MB"]
    '''
    yarn.scheduler.minimum-allocation-mb

    The minimum allocation for every container request at the RM, in MBs.
    Memory requests lower than this won't take effect, and the specified value will get allocated at minimum.

    https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-common/yarn-default.xml

    in our cluster
    1024
    '''

    cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.YARN_SCHEDULER_MAXIMUM_ALLOCATION_VCORES"] = \
        cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.YARN_NODEMANAGER_RESOURCE_CPU_VCORES"]
    '''
    yarn.scheduler.maximum-allocation-vcores

    The maximum allocation for every container request at the RM, in terms of virtual CPU cores.
    Requests higher than this won't take effect, and will get capped to this value.

    https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-common/yarn-default.xml

    in our cluster
    2
    '''

    cdh["YARN.GATEWAY.GATEWAY_BASE.YARN_APP_MAPREDUCE_AM_RESOURCE_MB"] = \
        constants["MAPREDUCE_MINIMUM_AM_MEMORY_MB"]
    '''
    yarn.app.mapreduce.am.resource.mb

    The amount of memory the MR AppMaster needs.

    https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml

    in our sample cluster
    1024
    '''

    cdh["YARN.GATEWAY.GATEWAY_BASE.YARN_APP_MAPREDUCE_AM_MAX_HEAP"] = \
        mb_to_bytes(
            int(cdh["YARN.GATEWAY.GATEWAY_BASE.YARN_APP_MAPREDUCE_AM_RESOURCE_MB"] * constants["MAX_HEAP_PERCENT"]))
    '''
    YARN_APP_MAPREDUCE_AM_MAX_HEAP

    The maximum heap size, in bytes, of the Java MapReduce ApplicationMaster.

    https://www.cloudera.com/documentation/enterprise/5-7-x/topics/cm_props_cdh570_yarn_mr2included_.html

    in our sample cluster
    mb_to_bytes(int( 1024 * 0.75)) = 805306368
    '''

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


    '''
    calculate the total number of containers that can run across the cluster. This value is not used in this formula but
    it's useful as a reference if you want to create yarn client configuration and need to know how many containers
    you can request.

    in our sample cluster
    int(4608 / ( (1891 + (2 * max(384, 384, 512) / 512) * 512 ) ) * 4) = 4
    '''

    '''
    Check if the user provided num threads falls with in the capacity of the cluster.
    '''
    if constants["NUM_THREADS"] > (CONTAINERS_ACCROSS_CLUSTER / 2):
        log.fatal("Number of concurrent threads should be at most {0}"
                  .format((min(CONTAINERS_ACCROSS_CLUSTER, CONTAINERS_ACCROSS_CLUSTER) / 2)))


    log.info("{0} could be as large as {1} for multi-tenacty".format("NUM_THREADS", (CONTAINERS_ACCROSS_CLUSTER / 2)))

    EXECUTORS_PER_THREAD = int((CONTAINERS_ACCROSS_CLUSTER - constants["NUM_THREADS"]) / constants["NUM_THREADS"])

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
        '''
        Set the configuration for the spark shuffle service if enabled
        '''

        cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.NODEMANAGER_CONFIG_SAFETY_VALVE"] = \
            cdh["YARN.JOBHISTORY.JOBHISTORY_BASE.JOBHISTORY_CONFIG_SAFETY_VALVE"]

        cdh["YARN.RESOURCEMANAGER.RESOURCEMANAGER_BASE.RESOURCEMANAGER_CONFIG_SAFETY_VALVE"] = \
            cdh["YARN.JOBHISTORY.JOBHISTORY_BASE.JOBHISTORY_CONFIG_SAFETY_VALVE"]

    return {"cdh": cdh }