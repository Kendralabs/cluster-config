hosts = cluster.yarn.nodemanager.hosts.all()
max_cores = 0
max_memory = 0
for key in hosts:
    if hosts[key].numCores > max_cores:
        max_cores = hosts[key].numCores
    if hosts[key].totalPhysMemBytes > max_memory:
        max_memory = hosts[key].totalPhysMemBytes

CORES_PER_WORKER_NODE = max_cores
TOTAL_MEMORY_PER_WORKER_NODE_GIB = max_memory
NUMBER_OF_WORKER_NODES = len(hosts)
MAX_HBASE_MEMORY_MiB = cluster.hbase.regionserver.regionserver_base.hbase_regionserver_java_heapsize.value

MEMORY_FRACTION_RESERVED_FOR_HBASE=0.2
MEMORY_FRACTION_RESERVED_FOR_SPARK=0
NUMBER_OF_CONCURRENT_THREADS=1



TOTAL_MEMORY_PER_WORKER_NODE_MiB = TOTAL_MEMORY_PER_WORKER_NODE_GIB * 1024
RESOURCE_FRACTION_RESERVED_FOR_All_OTHER_SERVICES = 0.2
RESOURCE_OVERCOMMIT_VALIDATION_THRESHOLD=0.8
CORES_RESERVED_FOR_All_OTHER_SERVICES = CORES_PER_WORKER_NODE * RESOURCE_FRACTION_RESERVED_FOR_All_OTHER_SERVICES
MEMORY_RESERVED_FOR_All_OTHER_SERVICES = TOTAL_MEMORY_PER_WORKER_NODE_MiB * RESOURCE_FRACTION_RESERVED_FOR_All_OTHER_SERVICES
TOTAL_USABLE_CORES_PER_WORKER_NODE = CORES_PER_WORKER_NODE - CORES_RESERVED_FOR_All_OTHER_SERVICES
TOTAL_USABLE_MEMORY_PER_WORKER_NODE_MiB = TOTAL_MEMORY_PER_WORKER_NODE_MiB - MEMORY_RESERVED_FOR_All_OTHER_SERVICES
TOTAL_ALLOCATABLE_CORES_PER_WORKER_NODE_MiB = TOTAL_USABLE_CORES_PER_WORKER_NODE
TOTAL_ALLOCATABLE_MEMORY_PER_WORKER_NODE_MiB = TOTAL_USABLE_MEMORY_PER_WORKER_NODE_MiB * RESOURCE_OVERCOMMIT_VALIDATION_THRESHOLD
MEMORY_FRACTION_RESERVED_FOR_YARN = 0.8
TOTAL_ALLOCATABLE_MEMORY_FOR_HBASE_PER_WORKER_NODE_MiB = TOTAL_ALLOCATABLE_MEMORY_PER_WORKER_NODE_MiB * MEMORY_FRACTION_RESERVED_FOR_HBASE
TOTAL_ALLOCATABLE_MEMORY_PER_WORKER_NODE_FINAL_MiB = TOTAL_ALLOCATABLE_MEMORY_PER_WORKER_NODE_MiB + (TOTAL_ALLOCATABLE_MEMORY_FOR_HBASE_PER_WORKER_NODE_MiB - MAX_HBASE_MEMORY_MiB if TOTAL_ALLOCATABLE_MEMORY_FOR_HBASE_PER_WORKER_NODE_MiB >MAX_HBASE_MEMORY_MiB else 0)
TOTAL_ALLOCATABLE_MEMORY_FOR_SPARK_PER_WORKER_NODE_MiB = TOTAL_ALLOCATABLE_MEMORY_PER_WORKER_NODE_MiB * MEMORY_FRACTION_RESERVED_FOR_SPARK
TOTAL_ALLOCATABLE_MEMORY_FOR_YARN_PER_WORKER_NODE_MiB = TOTAL_ALLOCATABLE_MEMORY_PER_WORKER_NODE_FINAL_MiB * MEMORY_FRACTION_RESERVED_FOR_YARN


#TOTAL_ALLOCATABLE_MEMORY_PER_WORKER_NODE_FINAL_MiB
#TOTAL_ALLOCATABLE_MEMORY_FOR_YARN_PER_WORKER_NODE_MiB= *MEMORY_FRACTION_RESERVED_FOR_YARN
cdh = {}
cdh["yarn.gateway.gateway_base.mapreduce_map_java_opts_max_heap"] = float(cluster.yarn.gateway.gateway_base.mapreduce_map_memory_mb.value) * RESOURCE_OVERCOMMIT_VALIDATION_THRESHOLD
cdh["yarn.gateway.gateway_base.mapreduce_reduce_java_opts_max_heap"] = 2 * cdh["yarn.gateway.gateway_base.mapreduce_map_java_opts_max_heap"]
cdh["yarn.gateway.gateway_base.mapreduce_map_memory_mb"] = TOTAL_ALLOCATABLE_MEMORY_FOR_YARN_PER_WORKER_NODE_MiB/TOTAL_ALLOCATABLE_CORES_PER_WORKER_NODE_MiB
cdh["yarn.gateway.gateway_base.mapreduce_reduce_memory_mb"] = 2 * cdh["yarn.gateway.gateway_base.mapreduce_map_memory_mb"]
cdh["yarn.resourcemanager.resourcemanager_base.yarn_scheduler_minimum_allocation_mb"] = cdh["yarn.gateway.gateway_base.mapreduce_map_memory_mb"]
cdh["yarn.resourcemanager.resourcemanager_base.yarn_scheduler_maximum_allocation_mb"] = TOTAL_ALLOCATABLE_MEMORY_FOR_YARN_PER_WORKER_NODE_MiB
cdh["yarn.nodemanager.nodemanager_base.yarn_nodemanager_resource_memory_mb"] = TOTAL_ALLOCATABLE_MEMORY_FOR_YARN_PER_WORKER_NODE_MiB

CONTAINERS_ACCROSS_CLUSTER = ((cdh["yarn.nodemanager.nodemanager_base.yarn_nodemanager_resource_memory_mb"]/cdh["yarn.gateway.gateway_base.mapreduce_map_memory_mb"]) - 2) * NUMBER_OF_WORKER_NODES - 2
GIRAPH_WORKERS_ACCROSS_CLUSTER = ((cdh["yarn.nodemanager.nodemanager_base.yarn_nodemanager_resource_memory_mb"]/cdh["yarn.gateway.gateway_base.mapreduce_map_memory_mb"]) - 2) * NUMBER_OF_WORKER_NODES - 2

atk = {}
atk["intel.analytics.engine.spark.conf.properties.spark.executor.memory"] = cdh["yarn.gateway.gateway_base.mapreduce_map_memory_mb"]
atk["intel.analytics.engine.spark.conf.properties.spark.yarn.numExecutors"] = CONTAINERS_ACCROSS_CLUSTER/NUMBER_OF_CONCURRENT_THREADS
atk["intel.analytics.api.giraph.giraph.maxWorkers"] = CONTAINERS_ACCROSS_CLUSTER/NUMBER_OF_CONCURRENT_THREADS
atk["intel.analytics.api.giraph.mapreduce.map.memory.mb"] = cdh["yarn.gateway.gateway_base.mapreduce_map_memory_mb"]
atk["intel.analytics.api.giraph.mapreduce.map.java.opts.max.heap"] = cdh["yarn.gateway.gateway_base.mapreduce_map_java_opts_max_heap"]



