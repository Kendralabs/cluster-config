from cm_api.endpoints import hosts


class Hosts(object):
    def __init__(self, cdh_resource_root, hostId):
        self._cdh_resource_root = None
        self._hosts = {}

        self.cdh_resource_root = cdh_resource_root
        self.add(hostId)

    def add(self, hostId):
        self.hosts[hostId] = hosts.get_host(self.cdh_resource_root, hostId)

    def memory(self):
        memory = {}
        for key in self.hosts:
            memory[self.hosts[key].hostname] = self.hosts[key].totalPhysMemBytes
        return memory

    def memoryTotal(self):
        memory = 0L
        for key in self.hosts:
            memory = memory + self.hosts[key].totalPhysMemBytes
        return memory

    def virtualCoresTotal(self):
        cores = 0
        for key in self.hosts:
            cores = cores + self.hosts[key].numCores
        return cores

    def physicalCoresTotal(self):
        cores = 0
        for key in self.hosts:
            cores = cores + self.hosts[key].numPhysicalCores
        return cores

    def all(self):
        nodes = {}
        for key in self.hosts:
            nodes[self.hosts[key].hostname] = self.hosts[key]
        return nodes

    @property
    def cdh_resource_root(self):
        return self._cdh_resource_root

    @cdh_resource_root.setter
    def cdh_resource_root(self, cdh_resource_root):
        self._cdh_resource_root = cdh_resource_root

    @property
    def hosts(self):
        return self._hosts

    @hosts.setter
    def hosts(self, hosts):
        self._hosts = hosts

