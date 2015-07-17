from cm_api.api_client import ApiResource
from urllib2 import URLError
from cluster_config.cdh.service import Service
from cluster_config import log



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

        self._get_api_resource(host, port, username, password)
        #save the cluster name, might need if we have more than one cluster in cloudera manager
        self.user_cluster_name = cluster

        #get the cluster
        self._get_cluster()

        #get cluster services
        self._set_services()

    def _get_api_resource(self, host, port, username, password):
        """
        Instantiate the cm_api resource, verify the host,port,username and password is not empty or None
        :param host:
        :param port:
        :param username:
        :param password:
        """
        if host is None or host is "":
            log.fatal("host init parameter can't be None or empty")
        if port is None or port is "":
            log.fatal("port init parameter can't be None or empty")
        if username is None or username is "":
            log.fatal("username init parameter can't be None or empty")
        if password is None or password is "":
            log.fatal("password init parameter can't be None or empty")

        #initialize cloudera manager connection
        log.debug("Connection to cluster {0}:{1} with user {2}:{3}".format(host,port,username, password))
        self.cdh_resource_root = ApiResource(host, server_port=port, username=username, password=password)

    def _cdh_get_clusters(self):
        """
        retrieve all the clusters managed by cdh
        :return: list of cluster objects
        """
        clusters = None
        try:
            clusters = self.cdh_resource_root.get_all_clusters()
        except URLError:
            log.fatal("couldn't connect to cluster management node.")
        return clusters

    def _cdh_get_services(self):
        return self.cluster.get_all_services()

    def _service(self, service):
        return Service(self.cdh_resource_root, self.cluster, service)

    def _set_services(self):
        """
        Get all the cluster services
        The service is wrapped in cluster_config.cdh.Service class which is assigned to an attribute and appended to a list.
        services will be acccesible with
        self.yarn or self.cdh_services["YARN"]
        """
        self.cdh_services = {}

        for service in self._cdh_get_services():
            temp = self._service(service)
            setattr(self, temp.name, temp)
            self.cdh_services[temp.key] = temp

    def _get_cluster(self):
        """
        Try to select a cluster if we have more than one cluster we need to ask the user. If we have 0 through fatal.
        if we have exactly one make it the default selection

        """
        self.clusters = self._cdh_get_clusters()

        #if we have more than one cluster we need to pick witch one to configure against
        if len(self.clusters) > 1:
            log.info("More than one Cluster Detected.")
            self._select_cluster()
        #else pick the only cluster
        elif len(self.clusters) == 1:
            log.info("cluster selected: {0}".format(self.clusters[0].name))
            self.cluster = self.clusters[0]
        elif len(self.clusters) <= 0:
            log.fatal("No clusters to configure")

    def _select_cluster(self):
        """
        Select a cluster either by trying to match self.user_cluster_name to the cluster name or display name or by
        prompting the user to select a cluster.
        """
        if self.user_cluster_name:
            print("Trying to find cluster: '{0}'.".format(self.user_cluster_name))

            for c in self.clusters:
                if c.displayName == self.user_cluster_name or c.name == self.user_cluster_name:
                    self.cluster = c

            if self.cluster is None:
                log.fatal("Couldn't find cluster: '{0}'".format(self.user_cluster_name))

        else:
            print("Please choose a cluster.")

            count = 0
            for c in self.clusters:
                print("Id {0}: Cluster Name: {1:20} Version: {2}".format((count+1), c.name, c.version))
                count += 1

            cluster_index = input("Enter the clusters Id : ")
            if cluster_index <= 0 or cluster_index > count:
                log.fatal("Invalid cluster id selected: {0}.".format(cluster_index))
            else:
                cluster_index -= 1
                print ("You picked cluster {0}: Cluster Name: {1:20} Version: {2}".format(cluster_index, self.clusters[cluster_index].name, self.clusters[cluster_index].version))
                self.cluster = self.clusters[cluster_index]

    def update_configs(self, configs, restart=False):
        for service in configs:
            #iterate through roles
            if service in self.cdh_services:
                self.cdh_services[service].set(configs[service], restart)
            else:
                log.error("Service: \"{0}\" doesn't exist in cluster: \"{1}\"".format(service, self.user_cluster_name))


    @property
    def cdh_resource_root(self):
        return self._cdh_resource_root

    @cdh_resource_root.setter
    def cdh_resource_root(self, cdh_resource_root):
        self._cdh_resource_root = cdh_resource_root

    @property
    def cluster(self):
        return self._cdh_cluster

    @cluster.setter
    def cluster(self, cluster):
        self._cdh_cluster = cluster

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

