from cm_api.endpoints import hosts


class Hosts(object):
    def __init__(self, cdh_resource_root, hostId):
        self._cdh_resource_root = None
        self._hosts = {}

        self.cdh_resource_root = cdh_resource_root
        self.add(hostId)

    def add(self, hostId):
        self.hosts[hostId] = hosts.get_host(self.cdh_resource_root, hostId)

    def max_cores(self, physical=False):
        max = 0
        for host in self.hosts:
            if physical:
                if self.hosts[host].numPhysicalCores > max:
                    max = self.hosts[host].numPhysicalCores
            else:
                if self.hosts[host].numCores > max:
                    max = self.hosts[host].numCores
        return max

    def max_memory(self):
        max = 0
        for host in self.hosts:
            if self.hosts[host].totalPhysMemBytes > max:
                max = self.hosts[host].totalPhysMemBytes

        return max

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

    def hostnames(self):
        nodes = []
        for host in self.hosts:
            nodes.append(self.hosts[host].hostname)
        return nodes

    def ipAddresses(self):
        ips = []
        for host in self.hosts:
            ips.append(self.hosts[host].ipAddress)
        return ips

    def all(self):
        return self.hosts

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

