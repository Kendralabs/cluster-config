import sys
import time

from cm_api.endpoints import role_config_groups

import cluster_config.utils.log as log
from cluster_config.cdh import CDH
from cluster_config.cdh.role import Role


class Service(CDH):
    '''
    Single Cloudera manager api service object
    '''

    def __init__(self, cdh, cdh_service):

        super(Service, self).set_resources(cdh)

        self.cmapi_service = cdh_service

        #all the cdh roles
        self._cdh_roles = {}

        self._get_roles()

    def _get_cdh_roles(self):
        return self.cmapi_service.get_all_roles()

    def _get_all_role_config_groups(self):
        return role_config_groups.get_all_role_config_groups(self.cmapi_resource_root, self.cmapi_service.name, self.cluster.name)

    def _role(self, role, role_type, role_name, active=True):
        return Role(self, role, role_type, role_name, active)

    def _get_roles(self):

        #get all roles assigned to hosts
        #a role type will have mulitple hosts
        for role in self._get_cdh_roles():
            if hasattr(self, role.type.lower()):
                getattr(self, role.type.lower()).add(role)
            else:
                temp = self._role(role, role.type, role.name)
                setattr(self, temp.name, temp)
                self.roles[temp.key] = temp

        #get all roles that have no assigned hosts
        for config_group in self._get_all_role_config_groups():
            temp = self._role(None, config_group.roleType, config_group.roleType, False)
            #temp = Role(self.cmapi_resource_root, self.cdh_cluster,  self.cdh_service, None, config_group.roleType, config_group.roleType, False)
            if hasattr(self, temp.name) is False:
                setattr(self, temp.name, temp)
                self.roles[temp.key] = temp


    def restart(self):
        '''
        Restart the service and deploy configurations
        '''
        print("Restarting service : \"{0}\"".format(self.service.type))
        self.service.restart()
        self.poll_commands("Restart")
        self.deployConfig()

    def _get_role_names(self):
        '''
        Get all the role names. this is used when deploying configurations after updates. The role names are very long and
        look like this 'spark-SPARK_WORKER-207e4bfb96a401eb77c8a78f55810d31'. Used by the Cloudera api to know where the
        config is going to get deployed

        :param roles: list of roles from a service. example SPARK_WORKER, SPARK_MASTER
        :return: only the service names. will list of something like this 'spark-SPARK_WORKER-207e4bfb96a401eb77c8a78f'
        '''
        temp = []
        for role in self.roles:
            for name in self.roles[role].get_role_names():
                temp.append(name)
        return temp

    def deployConfig(self):
        print("Deploying configuration for all {0} roles".format(self.service.type))
        self.service.deploy_client_config(*self._get_role_names())
        self.poll_commands("deployClientConfig")

    def poll_commands(self, command_name):
        '''
        poll the currently running commands to find out when the config deployment and restart have finished

        :param service: service to pool commands for
        :param command_name: the command we will be looking for, ie 'Restart'
        '''
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

            else:
                log.warning("Role: \"{0}\" doesn't exist for service: \"{1}\"".format(role, self.name))

    def get(self,role,configGroup=None,configs=None):

        getattr(self, role.lower()).get(configGroup, configs)

    @property
    def key(self):
        return self.cmapi_service.type.upper()

    @property
    def name(self):
        return self.cmapi_service.type.lower()

    @property
    def service(self):
        return self.cmapi_service

    @service.setter
    def service(self, cdh_service):
        self.cmapi_service = cdh_service

    @property
    def roles(self):
        return self._cdh_roles

    @roles.setter
    def roles(self, cdh_role):
        self._cdh_roles = cdh_role
