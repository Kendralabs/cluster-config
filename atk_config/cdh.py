from cm_api.api_client import ApiResource
from cm_api.endpoints import hosts
from cm_api.endpoints import role_config_groups
from subprocess import call
from os import system
import hashlib,re, time, argparse, os, time, sys, getpass
from types import MethodType
import codecs
from pprint import pprint


class Hosts(object):
    api = None
    hosts = None

    def __init__(self, api, hostId):
        self.hosts = {}
        self.api = api
        self.add(hostId)

    def add(self, hostId):
        self.hosts[hostId] = hosts.get_host(self.api, hostId)

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
            nodes[self.hosts[key].hostname] = {"memory": self.hosts[key].totalPhysMemBytes,
                                               "virtualcores" : self.hosts[key].numCores,
                                               "physicalcores": self.hosts[key].numPhysicalCores}
        return nodes

class Roles(object):
    api = None
    roles = None
    hosts = None

    def __init__(self, api, role):
        self.roles = {}
        self.roles[role.name] = role
        self.api = api
        self.__get_host(role)

    def add(self,role):
        self.roles[role.name] = role
        self.__get_host(role)

    def __get_host(self, role):
        if self.hosts:
            self.hosts.add(role.hostRef.hostId)
        else:
            self.hosts = Hosts(self.api, role.hostRef.hostId)

    def type(self):
        return self.role.type

class Service(object):
    api = None
    apiService = None


    def __init__(self, api, service):
        self.api = api
        self.apiService = service
        self.__get_roles()

    def __get_roles(self):
        self.roles = {}
        for role in self.apiService.get_all_roles():
            if hasattr(self, role.type.lower()):
                getattr(self, role.type.lower(), Roles(self.api, role)).add(role)
            else:
                setattr(self, role.type.lower(), Roles(self.api, role))


    def type(self):
        return self.apiService.type




class Cluster(object):
    #save are cloudera manager api reference
    api = None
    cluster = None
    clusters = None
    services = None
    def __init__(self, host, port, username, password, cluster):
        #initialize cloudera manager connection
        self.api = ApiResource(host, server_port=port, username=username, password=password)
        #save the cluster name, might need if we have more than one cluster in cloudera manager
        self.cluster = cluster
        #get the cluster
        self.__get_cluster()
        self.__get_services()

    def __get_services(self):
        self.services = {}

        for service in self.cluster.get_all_services():
            setattr(self, service.type.lower(), Service(self.api, service))


    def __get_cluster(self):
        #get all the clusters managed by cdh
        self.clusters = self.api.get_all_clusters()
        #if we have more than one cluster we need to pick witch one to configure against
        if len(self.clusters) > 1:
            self.__select_cluster()
        #else pick the only cluster
        elif len(self.clusters) == 1:
            cluster = self.clusters[0]

    def __select_cluster(self):
        if self.cluster:
            for c in self.clusters:
                if c.displayName == self.cluster or c.name == self.cluster:
                    self.cluster = c
        else:
            count = 1
            for c in self.clusters:
                print c
                print(": Cluster Name: {0:20} Version: {1}".format(c.name, c.version))
                count += 1
            cluster_index = input("Enter the clusters index number: ")
            print ("You picked cluster " + str(cluster_index))
            self.cluster = self.clusters[(cluster_index-1)]

    def get_host_memory(self):
        print "get_host_memory"

    def get_physical_cores(self):
        print "phy cores"

    def get_virtual_cores(self):
        print "get host cores"

    def get_hosts(self):
        print "get hosts"

    def set(self,type,group,config):
        print("set cdh")

    def get(self,type,group,config):
        print("get cdh")