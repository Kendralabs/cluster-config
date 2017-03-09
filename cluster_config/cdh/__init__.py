from urllib2 import URLError

from cm_api.api_client import ApiResource

from cluster_config.utils import log

class CDH(object):
    '''
    CDH base object class used to define common options needed to perform many of the cm-api requests.
    '''



    def __init__(self, host, port, username, password):
        self.cmapi_resource_root = None
        self.get_api_resource(host, port, username, password)
        self.cmapi_cluster = self.get_cluster()

    def set_resources(self, cdh):
        self.cmapi_resource_root = cdh.cmapi_resource_root
        self.cmapi_cluster = cdh.cmapi_cluster
        if hasattr(cdh, "cmapi_service"):
            self.cmapi_service = cdh.cmapi_service
        else:
            self.cmapi_service = None

    """
        def __init__(self, cmapi_resource_root, cmapi_cluster):
            self.cmapi_resource_root = cmapi_resource_root
            self.cmapi_cluster = cmapi_cluster
            self.cmapi_service = None

        def __init__(self, cmapi_resource_root, cmapi_cluster, cmapi_service):
            self.cmapi_resource_root = cmapi_resource_root
            self.cmapi_cluster = cmapi_cluster
            self.cmapi_service = cmapi_service
    """

    def get_api_resource(self, host, port, username, password):
        '''
        Instantiate the cm_api resource, verify the host,port,username and password is not empty or None
        :param host:
        :param port:
        :param username:
        :param password:
        '''
        if host is None or host is "":
            log.fatal("host init parameter can't be None or empty")
        if port is None or port is "":
            log.fatal("port init parameter can't be None or empty")
        if username is None or username is "":
            log.fatal("username init parameter can't be None or empty")
        if password is None or password is "":
            log.fatal("password init parameter can't be None or empty")

        #initialize cloudera manager connection
        log.debug("Connection to cluster {0}:{1} with user {2}:{3}".format(host, port, username, password))
        self.cdh_resource_root = ApiResource(host, server_port=port, username=username, password=password)

    def get_cluster(self):
        '''
        Try to select a cluster if we have more than one cluster we need to ask the user. If we have 0 through fatal.
        if we have exactly one make it the default selection

        '''
        clusters = self.cdh_get_clusters()

        #if we have more than one cluster we need to pick witch one to configure against
        if len(clusters) > 1:
            log.info("More than one Cluster Detected.")
            return self.select_cluster(clusters)
        #else pick the only cluster
        elif len(clusters) == 1:
            log.info("cluster selected: {0}".format(clusters[0].name))
            return clusters[0]
        elif len(clusters) <= 0:
            log.fatal("No clusters to configure")


    def select_cluster(self, clusters):
        '''
        Select a cluster either by trying to match self.user_cluster_name to the cluster name or display name or by
        prompting the user to select a cluster.
        :return: selected cluster
        '''
        if self.user_cluster_name:
            print("Trying to find cluster: '{0}'.".format(self.user_cluster_name))

            for c in clusters:
                if c.displayName == self.user_cluster_name or c.name == self.user_cluster_name:
                    return c

        else:
            print("Please choose a cluster.")

            count = 0
            for c in clusters:
                print("Id {0}: Cluster Name: {1:20} Version: {2}".format((count+1), c.name, c.version))
                count += 1

            cluster_index = input("Enter the clusters Id : ")
            if cluster_index <= 0 or cluster_index > count:
                log.fatal("Invalid cluster id selected: {0}.".format(cluster_index))
            else:
                cluster_index -= 1
                print ("You picked cluster {0}: Cluster Name: {1:20} Version: {2}".format(cluster_index, clusters[cluster_index].name, clusters[cluster_index].version))
                return clusters[cluster_index]

    def cdh_get_clusters(self):
        '''
        retrieve all the clusters managed by cdh
        :return: list of cluster objects
        '''
        clusters = None
        try:
            clusters = self.cmapi_resource_root.get_all_clusters()
        except URLError:
            log.fatal("couldn't connect to cluster management node.")
        return clusters

    @property
    def cdh_resource_root(self):
        return self.cmapi_resource_root

    @cdh_resource_root.setter
    def cdh_resource_root(self, cmapi_resource_root):
        self.cmapi_resource_root = cmapi_resource_root

    @property
    def cluster(self):
        return self.cmapi_cluster

    @cluster.setter
    def cluster(self, cluster):
        self.cmapi_cluster = cluster

