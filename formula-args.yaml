# User defined parameters, please don't modify unless you are an advanced user
OVER_COMMIT_FACTOR: 1.30 # Overcommit factor for JVM
MEM_FRACTION_FOR_HBASE: 0.125  # Suggested Max Memory fraction for HBase
MEM_FRACTION_FOR_OTHER_SERVICES: 0.125 # Suggested Memory fraction for other system resources
MAPREDUCE_JOB_COUNTERS_MAX: 500
SPARK_DRIVER_MAXPERMSIZE: 512 # This is required for running in yarn-cluster mode
YARN_SCHEDULER_MINIMUM_ALLOCATION_MB: 1024 # increase this in 512 increaments but usualy it should not be changed
ENABLE_SPARK_SHUFFLE_SERVICE: true # Set this to "true" in order to enable this service for large workloads
ZOOKEEPER_IS_EXTERNAL: true # Set this to "false" in order to run Zookeeper in local mode

NUM_THREADS: 1  # This should be set to the maximum number of munti-tenant users
MAPREDUCE_MINIMUM_AM_MEMORY_MB: 8192 # increase this in 512 increaments to create larger containers
MAPREDUCE_MINIMUM_EXECUTOR_MEMORY_MB: 8192 # increase this in 512 increaments to create larger containers
ENABLE_DYNAMIC_ALLOCATION_FOR_SPARK: true # set this to false in order to disable SPARK dynamic allocation 
