from cm_api.endpoints import hosts
from cluster_config.cdh import CDH

class Hosts(CDH):
    '''
    Hosts is used for grouping many hosts. They will be grouped by role
    '''
    def __init__(self, cdh, hostId):
        super(Hosts, self).set_resources(cdh)

        self._hosts = {}

        self.add(hostId)

    def add(self, hostId):
        '''
        add an additional host
        :param hostId: cmapi host id
        '''
        self.hosts[hostId] = hosts.get_host(self.cmapi_resource_root, hostId)

    @property
    def max_cores(self):
        max = 0
        for host in self.hosts:
            if self.hosts[host].numCores > max:
                max = self.hosts[host].numCores
        return max


    @property
    def max_memory(self):
        max = 0
        for host in self.hosts:
            if self.hosts[host].totalPhysMemBytes > max:
                max = self.hosts[host].totalPhysMemBytes

        return max

    @property
    def memory(self):
        memory = {}
        for key in self.hosts:
            memory[self.hosts[key].hostname] = self.hosts[key].totalPhysMemBytes
        return memory

    @property
    def memoryTotal(self):
        memory = 0L
        for key in self.hosts:
            memory = memory + self.hosts[key].totalPhysMemBytes
        return memory

    @property
    def virtualCoresTotal(self):
        cores = 0
        for key in self.hosts:
            cores = cores + self.hosts[key].numCores
        return cores

    @property
    def physicalCoresTotal(self):
        cores = 0
        for key in self.hosts:
            cores = cores + self.hosts[key].numPhysicalCores
        return cores

    @property
    def hostnames(self):
        nodes = []
        for host in self.hosts:
            nodes.append(self.hosts[host].hostname)
        return nodes

    @property
    def ipAddresses(self):
        ips = []
        for host in self.hosts:
            ips.append(self.hosts[host].ipAddress)
        return ips

    @property
    def all(self):
        return self.hosts

    @property
    def hosts(self):
        return self._hosts

    @hosts.setter
    def hosts(self, hosts):
        self._hosts = hosts

