from cm_api.endpoints import role_config_groups

from cluster_config.cdh.config_group import Config_Group
from cluster_config.cdh import CDH
from cluster_config.cdh.hosts import Hosts
from cluster_config.utils import log


class Role(CDH):
    def __init__(self, cdh, cdh_role, cdh_role_type=None, cdh_role_name=None, active=True):
        super(Role, self).set_resources(cdh)

        self._cdh_roles = {}
        '''cdh role object'''

        self._config_groups = {}
        '''config groups for the role'''

        self._hosts = None
        '''list of hosts the role is installed on'''

        self._type = None
        '''type of role'''

        self.active = active
        '''wather or not the role is active, if a role doesn't have a host assigne it will not be active'''

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
        '''
        Get all configuration groups for the role

        '''
        for group in role_config_groups.get_all_role_config_groups(self.cmapi_resource_root, self.cmapi_service.name, self.cmapi_cluster.name):
            if group.roleType == self.type:
                config_group = Config_Group(self, group)
                setattr(self, config_group.name, config_group)
                self.config_groups[config_group.key] = config_group


    def add(self, cdh_role):
        '''
        add more host's to this role
        :param cdh_role: cmapi role object from a particular host
        '''
        if cdh_role.type == self.type:
            self.cdh_roles[cdh_role.name] = cdh_role
            self.__get_host(cdh_role)

    def __get_host(self, role):
        '''
        Get host details for the role
        :param role: cmapi role object
        '''
        if self.hosts:
            self.hosts.add(role.hostRef.hostId)
        else:
            self.hosts = Hosts(self, role.hostRef.hostId)

    def set(self, configs):
        updated = {}
        for config_group in configs:
            if config_group in self.config_groups:
                update = self.config_groups[config_group].set(configs[config_group])
                updated[config_group] = update
                log.info("Updated {0} configuration/s.".format(len(update)))
            else:
                log.warning("Config group: \"{0}\" doesn't exist for role: \"{1}\"".format(config_group, self.name))


    def get_role_names(self):
        '''
        Get all the role names. this is used when deploying configurations after updates. The role names are very long and
        look like this 'spark-SPARK_WORKER-207e4bfb96a401eb77c8a78f55810d31'. Used by the Cloudera api to know where the
        config is going to get deployed

        :param roles: list of roles from a service. example SPARK_WORKER, SPARK_MASTER
        :return: only the service names. will list of something like this 'spark-SPARK_WORKER-207e4bfb96a401eb77c8a78f'
        '''
        temp = []
        for role in self.cdh_roles:
            if self.cdh_roles[role] and self.cdh_roles[role].hostRef:
                temp.append(role)
        return temp

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
