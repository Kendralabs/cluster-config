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
            nodes[self.hosts[key].hostname] = self.hosts[key]
        return nodes



class Config(object):
    group = None
    config = None

    def __init__(self, group, config):
        self.group = group
        self.config = config
        #pprint(self.conf)

    def get(self):
        if self.config.value:
            return self.config.value
        else:
            return self.config.default

    def set(self, value):
        self.config.value = self.group.update_config({self.config.name: value})[self.config.name]


class ConfigGroup(object):
    group = None
    def __init__(self, group):
        self.group = group
        self.config = {}
        self.__get_configs()

    def __get_configs(self):
        for name, config in self.group.get_config(view='full').items():
            #print config
            setattr(self, name.lower().replace("-", "_"), Config(self.group, config))
            #print config.name
            #print config.roleType

    def __update(self):
        self.__get_configs()

    def set(self, configs):
        print self.group.update_config(configs)
        self.__update()

    #group.update_config({"SPARK_WORKER_role_env_safety_valve": updated_class_path})

class Roles(object):
    api = None
    service = None
    cluster = None
    roles = None
    hosts = None
    type = None
    def __init__(self, api, service, cluster, role):
        self.api = api
        self.service = service
        self.cluster = cluster
        self.roles = {}
        self.type = role.type
        self.roles[role.name] = role

        self.__get_host(role)
        self.__get_all_config_groups()

    def __get_all_config_groups(self):
        for group in role_config_groups.get_all_role_config_groups(self.api, self.service.name, self.cluster.name):
            if group.roleType == self.type:
                #setattr(self, group.name.lower().replace("-", "_").replace(self.apiService.name.lower()+"_", ""), ConfigGroup(group))
                """
                print group.name
                print "group role type:" , group.roleType
                print group.base
                print group.config
                print group.displayName
                print group.serviceRef
                """
                setattr(self, group.name.lower().replace("-", "_"), ConfigGroup(group))

    def add(self, role):
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
    cluster = None
    groups = None
    def __init__(self, api, cluster, service):
        self.groups = {}
        self.api = api
        self.cluster = cluster
        self.apiService = service
        self.__get_roles()
        #self.__get_all_config_groups()

    """
    def __get_all_config_groups(self):
        for group in role_config_groups.get_all_role_config_groups(self.api, self.apiService.name, self.cluster.name):
            print group
            #setattr(self, group.name.lower().replace("-", "_").replace(self.apiService.name.lower()+"_", ""), ConfigGroup(group))

            print group.name
            print group.roleType
            print group.base
            print group.config
            print group.displayName
            print group.serviceRef
            setattr(self, group.name.lower().replace("-", "_"), ConfigGroup(group))
    """
    def __get_roles(self):
        self.roles = {}
        for role in self.apiService.get_all_roles():
            if hasattr(self, role.type.lower()):
                getattr(self, role.type.lower(), Roles(self.api, self.apiService, self.cluster, role)).add(role)
            else:
                setattr(self, role.type.lower(), Roles(self.api, self.apiService, self.cluster, role))


    def type(self):
        return self.apiService.type




class Cluster(object):
    #save are cloudera manager api reference
    api = None
    clusterName = None
    cluster = None
    clusters = None
    services = None
    def __init__(self, host, port, username, password, cluster=None):
        #initialize cloudera manager connection
        self.api = ApiResource(host, server_port=port, username=username, password=password)
        #save the cluster name, might need if we have more than one cluster in cloudera manager
        self.clusterName = cluster
        #get the cluster
        self.__get_cluster()
        self.__get_services()

    def __get_services(self):
        self.services = {}
        #print self.cluster.get_all_services()
        for service in self.cluster.get_all_services():
            setattr(self, service.type.lower(), Service(self.api, self.cluster, service))


    def __get_cluster(self):
        #get all the clusters managed by cdh
        self.clusters = self.api.get_all_clusters()
        #if we have more than one cluster we need to pick witch one to configure against
        if len(self.clusters) > 1:
            print("More than one Cluster Detected.")
            self.__select_cluster()
        #else pick the only cluster
        elif len(self.clusters) == 1:
            cluster = self.clusters[0]

    def __select_cluster(self):
        if self.clusterName:
            print("Trying to find cluster: '{0}'.".format(self.clusterName))
            for c in self.clusters:
                if c.displayName == self.clusterName or c.name == self.clusterName:
                    self.cluster = c
            if self.cluster is None:
                raise Exception("Couldn't find cluster: '{0}'".format(self.clusterName))
        else:
            print("Please choose a cluster.")
            count = 0
            for c in self.clusters:
                print("Id {0}: Cluster Name: {1:20} Version: {2}".format((count+1), c.name, c.version))
                count += 1
            cluster_index = input("Enter the clusters Id : ") - 1
            print type(self.clusters)
            if cluster_index < 0 or cluster_index > (count-1):
                raise Exception("Not a valid cluster Id")
            print ("You picked cluster {0}: Cluster Name: {1:20} Version: {2}".format(cluster_index, self.clusters[cluster_index].name, self.clusters[cluster_index].version))
            self.cluster = self.clusters[cluster_index]

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