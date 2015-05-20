services = None

def get(services):
    """
    Find a service handle, right now we only look for HDFS, ZOOKEEPER, and SPARK

    :param services: The list of services on the cluster
    :param type: the service we are looking for
    :return: service handle or None if the service is not found
    """
    for service in services:
        print service

    return None