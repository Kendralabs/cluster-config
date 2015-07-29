# from cluster_config import log

# Simple MB, GB and TB to Bytes calculator
KiB = 1024
MiB = KiB * 1024
GiB = MiB * 1024
TiB = GiB * 1024
MIN_NM_MEMORY = 8 * GiB

# Validate user defined parameters in forumual-args.yaml file
NUM_THREADS = args['NUM_THREADS']
if (NUM_THREADS < 1):
    log.fatal("{0} must be at least 1".format("NUM_THREADS"))

OVER_COMMIT_FACTOR = args['OVER_COMMIT_FACTOR']
if (OVER_COMMIT_FACTOR < 1):
    log.fatal("{0} must be at least 1".format("OVER_COMMIT_FACTOR"))

MEM_FRACTION_FOR_HBASE = args['MEM_FRACTION_FOR_HBASE']
if (MEM_FRACTION_FOR_HBASE < 0 or MEM_FRACTION_FOR_HBASE >= 1):
    log.fatal("{0} must be non-nagative and smaller than 1".format("MEM_FRACTION_FOR_HBASE"))

MEM_FRACTION_FOR_OTHER_SERVICES = args['MEM_FRACTION_FOR_OTHER_SERVICES']
if (MEM_FRACTION_FOR_OTHER_SERVICES < 0 or (MEM_FRACTION_FOR_OTHER_SERVICES >= (1 - MEM_FRACTION_FOR_HBASE))):
    log.fatal("{0} must be non-nagative and smaller than {1}".format("MEM_FRACTION_FOR_OTHER_SERVICES",
                                                                     1 - MEM_FRACTION_FOR_HBASE))

MAPREDUCE_JOB_COUNTERS_MAX = args['MAPREDUCE_JOB_COUNTERS_MAX']
if (MAPREDUCE_JOB_COUNTERS_MAX < 120):
    log.fatal("{0} must be greater or equal {1}".format("MAPREDUCE_JOB_COUNTERS_MAX", 120))

SPARK_DRIVER_MAXPERMSIZE = args['SPARK_DRIVER_MAXPERMSIZE']
if (SPARK_DRIVER_MAXPERMSIZE < 512):
    log.fatal("{0} must be at least {1}MB.".format("SPARK_DRIVER_MAXPERMSIZE", 512))

MAPREDUCE_MAP_MINIMUM_MEMORY_MB = args['MAPREDUCE_MAP_MINIMUM_MEMORY_MB']
if (MAPREDUCE_MAP_MINIMUM_MEMORY_MB < 512):
    log.fatal("Containers less than {0}MB are not supported".format(MAPREDUCE_MAP_MINIMUM_MEMORY_MB))
elif (MAPREDUCE_MAP_MINIMUM_MEMORY_MB != (MAPREDUCE_MAP_MINIMUM_MEMORY_MB / 512) * 512):
    log.fatal("{0} must be dividable by {1}".format('MAPREDUCE_MAP_MINIMUM_MEMORY_MB', 512))

hosts = cluster.yarn.nodemanager.hosts.all()
max_cores = 0
max_memory = 0
for key in hosts:
    if hosts[key].numCores > max_cores:
        max_cores = hosts[key].numCores
    if hosts[key].totalPhysMemBytes > max_memory:
        max_memory = hosts[key].totalPhysMemBytes

cdh = {}
atk = {}

###### These values are gathered by the tool from Cluster ######
NUM_NM_WORKERS = len(hosts)
NM_WORKER_CORES = max_cores
NM_WOKER_MEM = max_memory

if (max_memory < (MIN_NM_MEMORY)):
    log.fatal("Running the toolkit with less than {0}GB memory for YARN is not supported.".format(MIN_NM_MEMORY))
elif (max_memory <= (256 * GiB)):
    MAX_JVM_MEMORY = max_memory / 4  # Java Heap Size should not go over 25% of total memory per node manager
else:
    MAX_JVM_MEMORY = 64 * GiB  # for node managers with greater than 256 GB RAM, JVM memory should still be at most 64GB

if ((MAX_JVM_MEMORY / MiB) < MAPREDUCE_MAP_MINIMUM_MEMORY_MB):
    log.fatal("Container larger than {0}MB are not supported".format(MAX_JVM_MEMORY))

atk["intel.taproot.analytics.engine.spark.conf.properties.spark.driver.maxPermSize"] = \
    "\"%dm\"" % (SPARK_DRIVER_MAXPERMSIZE)

SPARK_YARN_DRIVER_MEMORYOVERHEAD = 384
atk["intel.taproot.analytics.engine.spark.conf.properties.spark.yarn.driver.memoryOverhead"] = \
    "\"%d\"" % (SPARK_YARN_DRIVER_MEMORYOVERHEAD)

SPARK_YARN_EXECUTOR_MEMORYOVERHEAD = 384
atk["intel.taproot.analytics.engine.spark.conf.properties.spark.yarn.executor.memoryOverhead"] = \
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

atk["intel.taproot.analytics.engine.spark.conf.properties.spark.yarn.numExecutors"] = \
    int((CONTAINERS_ACCROSS_CLUSTER - NUM_THREADS) / NUM_THREADS)

atk["intel.taproot.analytics.engine.spark.conf.properties.spark.executor.memory"] = \
    "\"%dm\"" % (cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"])

atk["intel.taproot.analytics.engine.spark.conf.properties.spark.executor.cores"] = \
    (cdh["YARN.NODEMANAGER.NODEMANAGER_BASE.YARN_NODEMANAGER_RESOURCE_CPU_VCORES"] * NUM_NM_WORKERS - NUM_THREADS) \
    / (NUM_THREADS * atk["intel.taproot.analytics.engine.spark.conf.properties.spark.yarn.numExecutors"])

atk["intel.taproot.analytics.engine.spark.conf.properties.spark.driver.memory"] = \
    "\"%dm\"" % (cdh["YARN.GATEWAY.GATEWAY_BASE.YARN_APP_MAPREDUCE_AM_RESOURCE_MB"])

atk["intel.taproot.analytics.engine.giraph.mapreduce.map.memory.mb"] = \
    cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_MEMORY_MB"]

atk["intel.taproot.analytics.engine.giraph.giraph.maxWorkers"] = \
    atk["intel.taproot.analytics.engine.spark.conf.properties.spark.yarn.numExecutors"]

atk["intel.taproot.analytics.engine.giraph.mapreduce.map.java.opts.max.heap"] = \
    "\"-Xmx%sm\"" % (cdh["YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_MAP_JAVA_OPTS_MAX_HEAP"] / MiB)

