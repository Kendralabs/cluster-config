from cm_api.api_client import ApiResource
from cm_api.endpoints import hosts, roles, role_config_groups
from urllib2 import URLError
import time
import sys
import re
import atk_config as atk
from pprint import pprint


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


class Config(object):
    def __init__(self, cdh_group, cdh_config):
        self._cdh_group = None
        self._cdh_config = None

        self.cdh_group = cdh_group
        self.cdh_config = cdh_config

    def get(self):
        if self.cdh_config.value:
            return self.cdh_config.value
        else:
            return self.cdh_config.default

    def set(self, value):
        self.cdh_config.value = self.cdh_group.update_config({self.cdh_config.name: value})[self.cdh_config.name]

    @property
    def key(self):
        name = self.cdh_config.name.upper()
        for r in ["-", "/"]:
            name = name.replace(r, "_")
        return name

    @property
    def name(self):
        return self.key.lower()

    @property
    def cdh_group(self):
        return self._cdh_group

    @cdh_group.setter
    def cdh_group(self, cdh_group):
        self._cdh_group = cdh_group

    @property
    def cdh_config(self):
        return self._cdh_config

    @cdh_config.setter
    def cdh_config(self, cdh_config):
        self._cdh_config = cdh_config

    @property
    def value(self):
        if self.cdh_config.value:
            return self.cdh_config.value
        else:
            return self.cdh_config.default

    @value.setter
    def value(self, value):
        self.cdh_config.value = self.cdh_group.update_config({self.cdg_config.name: value})[self.cdh_config.name]

class Config_Group(object):
    def __init__(self, cdh_service, cdh_group):
        #cdh service object
        self._cdh_service = None
        #cdh config_group object
        self._cdh_group = None
        #cdh configs items
        self._cdh_configs = {}

        self.cdh_service = cdh_service
        self.cdh_group = cdh_group

        self.__get_configs()

    def __get_configs(self):
        for name, config in self.cdh_group.get_config(view='full').items():
            temp = Config(self.cdh_group, config)
            setattr(self, temp.name, temp)
            self.configs[temp.key] = temp


    def __update(self):
        self.__get_configs()

    def set(self, configs):
        #re key our configs to use actual CDH config key
        temp = {}
        for config in configs:
            if config in self.configs:
                temp[self.configs[config].cdh_config.name] = configs[config]
            else:
                atk.log.warning("No configuration key found for: {0}".format(config))
        atk.log.info("Updating config group: {0}".format(self.key))
        self.cdh_group.update_config(temp)
        self.__update()
        return temp

    @property
    def key(self):
        return self.name.upper()

    @property
    def name(self):
        find = re.compile('[A-Z]*-[A-Z]*$')
        found = find.search(self.cdh_group.name)
        if found is None:
            return self.cdh_group.name.replace(self.cdh_service.name + "-", "").lower().replace("-", "_")
        else:
            return found.group().lower().replace("-", "_")

    @property
    def cdh_service(self):
        return self._cdh_service

    @cdh_service.setter
    def cdh_service(self, cdh_service):
        self._cdh_service = cdh_service

    @property
    def cdh_group(self):
        return self._cdh_group

    @cdh_group.setter
    def cdh_group(self, cdh_group):
        self._cdh_group = cdh_group

    @property
    def configs(self):
        return self._cdh_configs

    @configs.setter
    def configs(self, cdh_config):
        self._cdh_configs = cdh_config


class Roles(object):
    def __init__(self, cdh_resource_root, cdh_cluster, cdh_service, cdh_role, cdh_role_type=None, cdh_role_name=None, active=True):
        #save are cloudera manager resource root reference
        self.cdh_resource_root = cdh_resource_root
        #cdh cluster object
        self.cdh_cluster = cdh_cluster
        #cdh service object
        self.cdh_service = cdh_service
        #cdh role object
        self._cdh_roles = {}
        #config groups for the role
        self._config_groups = {}
        #list of hosts the role is installed on
        self._hosts = None


        #store the type of the roles
        self._type = None

        self.active = active

        if cdh_role_type:
            self.type = cdh_role_type
        else:
            self.type = cdh_role.type

        if cdh_role_name:
            self.cdh_roles[cdh_role_name] = cdh_role
        else:
            self.cdh_roles[cdh_role.name] = cdh_role

        if self.active:
            self.__get_host(cdh_role)

        self.__get_all_config_groups()

    def __get_all_config_groups(self):
        for group in role_config_groups.get_all_role_config_groups(self.cdh_resource_root, self.cdh_service.name, self.cdh_cluster.name):
            if group.roleType == self.type:
                config_group = Config_Group(self.cdh_service, group)
                setattr(self, config_group.name, config_group)
                self.config_groups[config_group.key] = config_group
#                self.config_groups[config_group.key] = config_group


    def add(self, cdh_role):
        if cdh_role.type == self.type:
            self.cdh_roles[cdh_role.name] = cdh_role
            self.__get_host(cdh_role)

    def __get_host(self, role):
        if self.hosts:
            self.hosts.add(role.hostRef.hostId)
        else:
            self.hosts = Hosts(self.cdh_resource_root, role.hostRef.hostId)

    def set(self, configs):
        updated = {}
        for config_group in configs:
            if config_group in self.config_groups:
                update = self.config_groups[config_group].set(configs[config_group])
                updated[config_group] = update
                atk.log.info("Updated {0} configuration/s.".format(len(update)))
            else:
                atk.log.warning("Config group: \"{0}\" doesn't exist for role: \"{1}\"".format(config_group, self.name))



    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, type):
        self._type = type

    @property
    def key(self):
        return self.type.upper()

    @property
    def name(self):
        return self.type.lower()

    @property
    def cdh_resource_root(self):
        return self._cdh_resource_root

    @cdh_resource_root.setter
    def cdh_resource_root(self, cdh_resource_root):
        self._cdh_resource_root = cdh_resource_root

    @property
    def cdh_cluster(self):
        return self._cdh_cluster

    @cdh_cluster.setter
    def cdh_cluster(self, cdh_cluster):
        self._cdh_cluster = cdh_cluster

    @property
    def cdh_service(self):
        return self._cdh_service

    @cdh_service.setter
    def cdh_service(self, cdh_service):
        self._cdh_service = cdh_service

    @property
    def cdh_roles(self):
        return self._cdh_roles

    @cdh_roles.setter
    def cdh_roles(self, roles):
        self._cdh_roles

    @property
    def config_groups(self):
        return self._config_groups

    @config_groups.setter
    def config_groups(self, config_groups):
        self._config_groups = config_groups

    @property
    def hosts(self):
        return self._hosts

    @hosts.setter
    def hosts(self, hosts):
        self._hosts = hosts

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, active):
        self._active = active


class Service(object):
    def __init__(self, cdh_resource_root, cdh_cluster, cdh_service):
        #save are cloudera manager resource root reference
        self._cdh_resource_root = None
        #cdh cluster object
        self._cdh_cluster = None
        #cdh service object
        self._cdh_service = None
        #all the cdh roles
        self._cdh_roles = {}

        self.cdh_resource_root = cdh_resource_root
        self.cdh_cluster = cdh_cluster
        self.cdh_service = cdh_service

        self.__get_roles()

    def __get_roles(self):

        #get all roles assigned to hosts
        for role in self.cdh_service.get_all_roles():
            if hasattr(self, role.type.lower()):
                getattr(self, role.type.lower()).add(role)
            else:
                temp = Roles(self.cdh_resource_root, self.cdh_cluster, self.cdh_service, role, role.type, role.name)
                setattr(self, temp.name, temp)
                self.roles[temp.key] = temp

        #get all roles that have no assigned hosts
        for config_group in role_config_groups.get_all_role_config_groups(self.cdh_resource_root, self.cdh_service.name, self.cdh_cluster.name):
            temp = Roles(self.cdh_resource_root, self.cdh_cluster,  self.cdh_service, None, config_group.roleType, config_group.roleType, False)
            if hasattr(self, temp.name) is False:
                setattr(self, temp.name, temp)
                self.roles[temp.key] = temp


    def restart(self):
        print("Restarting service : \"{0}\"".format(self.service.type))
        self.service.restart()
        self.poll_commands("Restart")
        self.deployConfig()

    def deployConfig(self):
        self.service.deploy_client_config(*self.roleNames)
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
            commands = self.service.get_commands(view="full")
            if commands:
                for c in commands:
                    if c.name == command_name:
                        #active = c.active
                        break
            else:
                break
        print "\n"

    def set(self, configs, restart=False):
        for role in configs:
            if role in self.roles:

                self.roles[role].set(configs[role])

                if restart:
                    self.restart()
            else:
                atk.log.warning("Role: \"{0}\" doesn't exist for service: \"{1}\"".format(role, self.name))

    def get(self,role,configGroup=None,configs=None):

        getattr(self, role.lower()).get(configGroup, configs)

    @property
    def key(self):
        return self.cdh_service.type.upper()

    @property
    def name(self):
        return self.cdh_service.type.lower()

    @property
    def cdh_resource_root(self):
        return self._cdh_resource_root

    @cdh_resource_root.setter
    def cdh_resource_root(self, cdh_resource_root):
        self._cdh_resource_root = cdh_resource_root

    @property
    def cdh_cluster(self):
        return self._cdh_cluster

    @cdh_cluster.setter
    def cdh_cluster(self, cdh_cluster):
        self._cdh_cluster = cdh_cluster

    @property
    def cdh_service(self):
        return self._cdh_service

    @cdh_service.setter
    def cdh_service(self, cdh_service):
        self._cdh_service = cdh_service

    @property
    def roles(self):
        return self._cdh_roles

    @roles.setter
    def roles(self, cdh_role):
        self._cdh_roles = cdh_role


class Cluster(object):

    def __init__(self, host, port, username, password, cluster=None):
        #save are cloudera manager resource root reference
        self._cdh_resource_root = None
        #the selected cluster either by default because we only have one or the one that matches user_given_cluster_name
        self._cdh_cluster = None
        #all the cluster services
        self._cdh_services = None
        #users cluster name from the command line
        self._user_cluster_name = None

        if host is None or port is None or username is None or password is None:
            return None

        #initialize cloudera manager connection
        self.cdh_resource_root = ApiResource(host, server_port=port, username=username, password=password)
        #save the cluster name, might need if we have more than one cluster in cloudera manager
        self.user_cluster_name = cluster

        #get the cluster
        self.__get_cluster()

        #get cluster services
        self.__get_services()

    def __get_services(self):
        self.cdh_services = {}

        for service in self.cluster.get_all_services():
            temp = Service(self.cdh_resource_root, self.cluster, service)
            setattr(self, temp.name, temp)
            self.cdh_services[temp.key] = temp



    def __get_cluster(self):
        #get all the clusters managed by cdh
        try:
            self.clusters = self.cdh_resource_root.get_all_clusters()
        except URLError:
            atk.log.fatal("couldn't connect to cluster management node.")

        #if we have more than one cluster we need to pick witch one to configure against
        if len(self.clusters) > 1:
            atk.log.info("More than one Cluster Detected.")
            self.__select_cluster()
        #else pick the only cluster
        elif len(self.clusters) == 1:
            atk.log.info("cluster selected: {0}".format(self.clusters[0].name))
            self.cluster = self.clusters[0]



    def __select_cluster(self):
        if self.user_given_cluster_name:
            print("Trying to find cluster: '{0}'.".format(self.user_given_cluster_name))
            for c in self.clusters:
                if c.displayName == self.user_given_cluster_name or c.name == self.user_given_cluster_name:
                    self.cluster = c
            if self.cluster is None:
                atk.log.fatal("Couldn't find cluster: '{0}'".format(self.user_given_cluster_name))

        else:
            print("Please choose a cluster.")
            count = 0
            for c in self.clusters:
                print("Id {0}: Cluster Name: {1:20} Version: {2}".format((count+1), c.name, c.version))
                count += 1
            cluster_index = input("Enter the clusters Id : ") - 1
            if cluster_index <= 0 or cluster_index > (count-1):
                atk.log.fatal("Invalid cluster id selected.")

            print ("You picked cluster {0}: Cluster Name: {1:20} Version: {2}".format(cluster_index, self.clusters[cluster_index].name, self.clusters[cluster_index].version))
            self.cluster = self.clusters[cluster_index]

    def update_configs(self, configs, restart=False):
        for service in configs:
            #iterate through roles
            if service in self.cdh_services:
                self.cdh_services[service].set(configs[service], restart)
            else:
                atk.log.error("Service: \"{0}\" doesn't exist in cluster: \"{1}\"".format(service, self.user_cluster_name))


    @property
    def cdh_resource_root(self):
        return self._cdh_resource_root

    @cdh_resource_root.setter
    def cdh_resource_root(self, cdh_resource_root):
        self._cdh_resource_root = cdh_resource_root

    @property
    def cdh_cluster(self):
        return self._cdh_cluster

    @cdh_cluster.setter
    def cdh_cluster(self, cluster):
        self._cdh_cluster = cluster

    @property
    def cdh_services(self):
        return self._cdh_services

    @cdh_services.setter
    def cdh_services(self, services):
        self._cdh_services = services

    @property
    def user_cluster_name(self):
        return self._user_cluster_name

    @user_cluster_name.setter
    def user_cluster_name(self, cluster_name):
        self._user_cluster_name = cluster_name

