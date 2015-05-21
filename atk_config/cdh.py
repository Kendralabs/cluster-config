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
        #print config.name

    def get(self):
        if self.config.value:
            return self.config.value
        else:
            return self.config.default

    def set(self, value):
        self.config.value = self.group.update_config({self.config.name: value})[self.config.name]


class ConfigGroup(object):
    group = None
    configs = None
    def __init__(self, group):
        self.group = group
        self.config = {}
        self.__get_configs()
        #print "group ", group.name

    def __get_configs(self):
        self.configs = {}
        for name, config in self.group.get_config(view='full').items():
            #print config
            setattr(self, name.lower().replace("-", "_"), Config(self.group, config))
            self.configs[name] = getattr(self, name.lower().replace("-", "_"))
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
    configGroups = None
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
        self.configGroups = {}
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
                self.configGroups[group.name] = getattr(self, group.name.lower().replace("-", "_"))

    def add(self, role):
        self.roles[role.name] = role
        self.__get_host(role)

    def __get_host(self, role):
        if self.hosts:
            self.hosts.add(role.hostRef.hostId)
        else:
            self.hosts = Hosts(self.api, role.hostRef.hostId)
    def set(self, configGroup, configs):
        print ""
        print "roles.set"
        print configGroup
        print configs
        getattr(self, configGroup.lower().replace("-", "_")).set(configs)

    def type(self):
        return self.role.type

class Service(object):
    api = None
    apiService = None
    cluster = None
    groups = None
    roles = None
    roleTypes = None
    roleNames = None
    def __init__(self, api, cluster, service):
        self.groups = {}
        self.api = api
        self.cluster = cluster
        self.apiService = service
        self.__get_roles()
        #self.__get_all_config_groups()

    def __get_roles(self):
        self.roles = {}
        self.roleNames = []
        self.roleTypes = []
        for role in self.apiService.get_all_roles():
            self.roleNames.append(role.name)
            self.roleTypes.append(role.type)
            if hasattr(self, role.type.lower()):
                getattr(self, role.type.lower(), Roles(self.api, self.apiService, self.cluster, role)).add(role)
            else:
                setattr(self, role.type.lower(), Roles(self.api, self.apiService, self.cluster, role))
                self.roles[role.type] = getattr(self, role.type.lower())
        self.roleTypes = set(self.roleTypes)


    def restart(self):
        self.apiService.restart()
        self.poll_commands("Restart")
        self.deployConfig()

    def deployConfig(self):
        self.apiService.deploy_client_config(*self.roleNames)
        self.poll_commands("deployClientConfig")

    def poll_commands(self, command_name):
        """
        poll the currently running commands to find out when the config deployment and restart have finished

        :param service: service to pool commands for
        :param command_name: the command we will be looking for, ie 'Restart'
        """
        while True:
            time.sleep(1)
            print(" . "),
            sys.stdout.flush()
            commands = self.apiService.get_commands(view="full")
            if commands:
                for c in commands:
                    if c.name == command_name:
                        #active = c.active
                        break
            else:
                break
        print "\n"

    def set(self,role,configGroup,configs):
        getattr(self, role.lower()).set(configGroup, configs)

    def get(self,role,configGroup=None,configs=None):

        getattr(self, role.lower()).get(configGroup, configs)

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
            #self.services.append(service.type)
            setattr(self, service.type.lower(), Service(self.api, self.cluster, service))
            self.services[service.type] = getattr(self, service.type.lower())



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
            if cluster_index <= 0 or cluster_index > (count-1):
                raise Exception("Not a valid cluster Id")
            print ("You picked cluster {0}: Cluster Name: {1:20} Version: {2}".format(cluster_index, self.clusters[cluster_index].name, self.clusters[cluster_index].version))
            self.cluster = self.clusters[cluster_index]

    def deployConfig(self,service):
        print "deploy config cluster"
        getattr(self,service.lower()).deployConfig()

    def restart(self, service):
        getattr(self,service.lower()).restart()

    def set(self,service,role,configGroup,configs):
        getattr(self,service.lower()).set(role, configGroup, configs)

    def get(self, service, role=None, configGroup=None, config=None):
        if role is None:
            return getattr(self,service.lower())
        else:
            return getattr(self,service.lower()).get(role, configGroup, config)
