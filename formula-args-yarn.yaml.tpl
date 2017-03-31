# User defined parameters, please don't modify unless you are an advanced user
MEM_FRACTION_FOR_OTHER_SERVICES: 0.125 # Suggested Memory fraction for other system resources
MAPREDUCE_JOB_COUNTERS_MAX: 500

SPARK_DRIVER_MAXPERMSIZE: 512 # This is required for running in yarn-cluster mode
ENABLE_SPARK_SHUFFLE_SERVICE: true # Set this to "true" in order to enable this service for large workloads

NUM_THREADS: 1  # This should be set to the maximum number of munti-tenant users
YARN_SCHEDULER_MINIMUM_ALLOCATION_MB: 1024 # increase this in 512 increaments but usualy it should not be changed

MAPREDUCE_MINIMUM_AM_MEMORY_MB: 1024 # increase this in 512 increaments to create larger containers
MAX_HEAP_PERCENT: 0.75

MAPREDUCE_MINIMUM_EXECUTOR_MEMORY_MB: 1024 # increase this in 512 increaments to create larger containers
SPARK_MEMORY_OVERHEAD_MIN: 384 #minimum amount of memory allocated for spark overhead

MEMORY_OVERHEAD: 0.10 # The amount of off-heap memory (in megabytes) to be allocated for, ie spark.yarn.executor.memoryOverhead = --executor-memory * .10

YARN_INCREMENT_ALLOCATION_MB: 512 #memory increment when requesting containers
