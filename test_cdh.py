from atk_config import cdh
from pprint import pprint

cluster =  cdh.Cluster("10.54.8.175", 7180, "admin", "wolverine", "cluster")

#print cluster.hdfs.namenode.hosts.memoryTotal()
#print cluster.hdfs.namenode.hosts.memory()
#print cluster.hdfs.datanode.hosts.memoryTotal()
#print cluster.hdfs.datanode.hosts.memory()
hosts = cluster.yarn.nodemanager.hosts.all()
for key in hosts:
    print hosts[key].numCores
    print hosts[key].numPhysicalCores


print ""

pprint(vars(cluster.yarn.nodemanager))

print ""
pprint(vars(cluster.hdfs.datanode))
print ""
pprint(vars(cluster.hdfs.datanode.hdfs_datanode_base))
print ""
pprint(vars(cluster.hdfs.datanode.hdfs_datanode_base.dfs_datanode_port))

# cluster.services["HDFS"].roles["DATANODE"].configGroups["hdfs_datanode_base"].configs["dfs_datanode_failed_volumes_tolerated"].get
print cluster.hdfs.datanode.datanode_base.dfs_datanode_failed_volumes_tolerated.get()
print cluster.hdfs.datanode.datanode_base.set({"dfs_datanode_failed_volumes_tolerated": 5})
print cluster.hdfs.datanode.datanode_base.dfs_datanode_failed_volumes_tolerated.get()
#print cluster.hdfs.datanode.hdfs_datanode_base.dfs_datanode_failed_volumes_tolerated.set(6)
#print cluster.hdfs.datanode.hdfs_datanode_base.dfs_datanode_failed_volumes_tolerated.get()

#print cluster.hdfs.namenode.hdfs_datanode_base.dfs_datanode_port.get()
#cluster.hdfs.namenode.hdfs_datanode_base.dfs_datanode_port.set
#cluster.hdfs.namenode.hdfs_datanode_base.set
#cluster.hdfs.namenode.hdfs_datanode_base.get

"""
cluster =  cdh.Cluster("10.54.8.175", 7180, "admin", "wolverine")
hosts = cluster.yarn.nodemanager.hosts.all()
for key in hosts:
    print hosts[key].numCores
    print hosts[key].numPhysicalCores
"""


