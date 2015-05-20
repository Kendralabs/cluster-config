from atk_config import cdh
from pprint import pprint

cluster =  cdh.Cluster("10.54.8.175", 7180, "admin", "wolverine", "cluster")

## print cluster.services["HDFS"]

#cluster.hdfs.namenode.host.hostname
print cluster.hdfs.namenode.hosts.memoryTotal()
print cluster.hdfs.namenode.hosts.memory()
print cluster.hdfs.datanode.hosts.memoryTotal()
print cluster.hdfs.datanode.hosts.memory()

print ""
#cdh.host.cores()
#cdh.host.memory()


