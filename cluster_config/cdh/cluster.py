import sys
import time
from urllib2 import URLError

from cm_api.api_client import ApiResource

from cluster_config.cdh import CDH
from cluster_config import CDH_CLUSTER_CONFIGS
from cluster_config.cdh.service import Service
from cluster_config.utils import log
from cluster_config.utils.file import file_path, write_json_conf


class Cluster(CDH):
    '''
    Wraps some of the method calls to cm-api.Cluster obj
    '''

    # noinspection PyPackageRequirements
    def __init__(self, host, port, username, password, cluster=None):
        self.cdh_services = None
        '''will hold all the cluster services'''

        super(Cluster, self).__init__(host, port, username, password, cluster)
        '''Get cm-api resource root'''

        #get cluster services
        self._set_services()

    def _get_services(self):
        '''
        Call cm-api to get all the cluster services
        :return: return list of cluster services
        '''
        return self.cluster.get_all_services()

    def _create_service(self, service):
        '''
        Create a service object, ie YARN, SPARK_ON_YARN, ...
        :param service:
        :return: new service object
        '''
        return Service(self, service)

    def _set_services(self):
        '''
        Get all the cluster services
        The service is wrapped in cluster_config.cdh.Service class which is assigned to an attribute and appended to a list.
        services will be acccesible with
        self.yarn or self.cdh_services["YARN"]
        '''
        self.cdh_services = {}

        for service in self._get_services():
            temp = self._create_service(service)
            setattr(self, temp.name, temp)
            self.cdh_services[temp.key] = temp

    def update_configs(self, configs, restart=False):
        '''
        Update cluster configurations

        :param configs: nested dictionary with all desired configuration
        :param restart: force restart after updating configurations?
        '''
        for service in configs:
            #iterate through roles
            if service in self.cdh_services:
                self.cdh_services[service].set(configs[service], restart)
            else:
                log.error("Service: \"{0}\" doesn't exist in cluster: \"{1}\"".format(service, self.user_cluster_name))
        if restart:
            self.restart()

    def restart(self):
        '''
        Restart the cluster and deploy configurations but only services with stale configurations.
        '''
        log.info("Trying to restart cluster : \"{0}\"".format(self.cluster.name))


        time.sleep(10)
        '''We need to sleep for a small amount of time so CDH notices it's configurations have been updated.'''

        self.cluster.restart(True, True)

        self.poll_commands("Restart")

    def poll_commands(self, command_name):
        '''
        poll the currently running commands to find out when the config deployment and restart have finished

        :param service: service to pool commands for
        :param command_name: the command we will be looking for, ie 'Restart'
        '''
        while True:
            time.sleep(2)
            log.info("Waiting for {0}".format(command_name)),
            sys.stdout.flush()
            commands = self.cluster.get_commands(view="full")
            if commands:
                for c in commands:
                    if c.name == command_name:
                        #active = c.active
                        break
            else:
                break
        print "\n"
        log.info("Done with {0}.".format(command_name))

    @property
    def services(self):
        return self._cdh_services

    @services.setter
    def services(self, services):
        self._cdh_services = services

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


def save_to_json(cluster, path, name=None):
    '''
    Save cluster configuration to json file
    :param cluster: Cluster object
    :param path: file location for saving json file
    :param name: name of the json file
    :return:
    '''
    if name is None:
        name = CDH_CLUSTER_CONFIGS
    else:
        name = "{0}-{1}".format(name, CDH_CLUSTER_CONFIGS)
    cdh_json_path = file_path(name, path)
    write_json_conf(obj_to_dict(cluster), cdh_json_path)
    return cdh_json_path

#TODO: rename to _dict
def obj_to_dict(cluster):
    '''
    Convert a cluster object to dictionary so i can save it as a json file
    :param cluster: cluster object
    :return: dictionay with all the cluster configurations
    '''
    configs = {}
    for service in cluster.services:
        configs[service] = {}
        for role in cluster.services[service].roles:
            configs[service][role] = {}
            for config_group in cluster.services[service].roles[role].config_groups:
                configs[service][role][config_group] = {}
                for config in cluster.services[service].roles[role].config_groups[config_group].configs:
                    configs[service][role][config_group][config] = cluster.services[service].roles[role].config_groups[config_group].configs[config].value
    return configs
