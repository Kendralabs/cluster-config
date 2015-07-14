from cluster_config.cdh.config_group import Config_Group
from cluster_config.cdh.hosts import Hosts
from cm_api.endpoints import role_config_groups



class Role(object):
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
