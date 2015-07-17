import unittest
import contextlib
from mock import MagicMock
import mock as mock
import cm_api.api_client as cm
from cluster_config.cdh.cluster import Cluster
from cluster_config.cdh.service import Service
import cluster_config as cc

class TestCdhCluster(unittest.TestCase):
    def test_missing_host(self):
        with mock.patch("cluster_config.log.fatal") as log_fatal,\
                mock.patch("cluster_config.cdh.cluster.Cluster._get_cluster") as get_cluster, \
                mock.patch("cluster_config.cdh.cluster.Cluster._set_services") as get_service:
            log_fatal.return_value = MagicMock()
            get_cluster.return_value = MagicMock()
            get_service.return_value = MagicMock()

            Cluster(None, "7777", "username", "password", "cluster_name")

            assert log_fatal.call_count == 1
            log_fatal.assert_called_with("host init parameter can't be None or empty")

    def test_missing_port(self):
        with mock.patch("cluster_config.log.fatal") as log_fatal, \
                mock.patch("cluster_config.cdh.cluster.Cluster._get_cluster") as get_cluster, \
                mock.patch("cluster_config.cdh.cluster.Cluster._set_services") as get_service:
            log_fatal.return_value = MagicMock()
            get_cluster.return_value = MagicMock()
            get_service.return_value = MagicMock()

            Cluster("some host", None, "username", "password", "cluster_name")

            assert log_fatal.call_count == 1
            log_fatal.assert_called_with("port init parameter can't be None or empty")

    def test_missing_username(self):
        with mock.patch("cluster_config.log.fatal") as log_fatal, \
                mock.patch("cluster_config.cdh.cluster.Cluster._get_cluster") as get_cluster, \
                mock.patch("cluster_config.cdh.cluster.Cluster._set_services") as get_service:
            log_fatal.return_value = MagicMock()
            get_cluster.return_value = MagicMock()
            get_service.return_value = MagicMock()

            Cluster("some host", "port", None, "password", "cluster_name")

            assert log_fatal.call_count == 1
            log_fatal.assert_called_with("username init parameter can't be None or empty")

    def test_missing_password(self):
        with mock.patch("cluster_config.log.fatal") as log_fatal, \
                mock.patch("cluster_config.cdh.cluster.Cluster._get_cluster") as get_cluster, \
                mock.patch("cluster_config.cdh.cluster.Cluster._set_services") as get_service:
            log_fatal.return_value = MagicMock()
            get_cluster.return_value = MagicMock()
            get_service.return_value = MagicMock()
            Cluster("host", "Port", "username", None, "cluster_name")

            assert log_fatal.call_count == 1
            log_fatal.assert_called_with("password init parameter can't be None or empty")

    def test_get_clusters_one_cluster(self):
        with mock.patch("cluster_config.log.info") as log_info, \
                mock.patch("cluster_config.cdh.cluster.Cluster._set_services") as get_service,\
                mock.patch("cluster_config.cdh.cluster.Cluster._get_api_resource") as api_source,\
                mock.patch("cluster_config.cdh.cluster.Cluster._cdh_get_clusters") as all_clusters:
            log_info.return_value = MagicMock()
            get_service.return_value = MagicMock()
            api_source.return_value = MagicMock()
            all_clusters.return_value = [type('obj', (object,), {'name' : "123"})]

            cluster = Cluster("host", "Port", "username", "password", "cluster_name")


            assert cluster.cluster.name == "123"
            log_info.assert_called_with("cluster selected: 123")

    def test_get_clusters_zero_clusters(self):
        with mock.patch("cluster_config.log.fatal") as log, \
                mock.patch("cluster_config.cdh.cluster.Cluster._set_services") as get_service,\
                mock.patch("cluster_config.cdh.cluster.Cluster._get_api_resource") as api_source,\
                mock.patch("cluster_config.cdh.cluster.Cluster._cdh_get_clusters") as all_clusters:
            log.return_value = MagicMock()
            get_service.return_value = MagicMock()
            api_source.return_value = MagicMock()
            all_clusters.return_value = []

            Cluster("host", "Port", "username", "password", "cluster_name")

            log.assert_called_with("No clusters to configure")
            assert log.call_count == 1

    def test_get_clusters_two_clusters(self):
        with mock.patch("cluster_config.log.fatal") as log, \
                mock.patch("cluster_config.cdh.cluster.Cluster._set_services") as get_service,\
                mock.patch("cluster_config.cdh.cluster.Cluster._get_api_resource") as api_source,\
                mock.patch("cluster_config.cdh.cluster.Cluster._cdh_get_clusters") as all_clusters:
            log.return_value = MagicMock()
            get_service.return_value = MagicMock()
            api_source.return_value = MagicMock()
            all_clusters.return_value = [type('obj', (object,), {'name' : "123", "displayName": "123"}),
                                         type('obj', (object,), {'name' : "1234", "displayName": "1234"})]

            cluster = Cluster("host", "Port", "username", "password", "123")

            assert cluster.cluster.name == "123"

            cluster = Cluster("host", "Port", "username", "password", "1234")

            assert cluster.cluster.name == "1234"

    def test_get_clusters_two_clusters_invalid_cluster_name(self):
        with mock.patch("cluster_config.log.fatal") as logg, \
                mock.patch("cluster_config.cdh.cluster.Cluster._set_services") as get_service,\
                mock.patch("cluster_config.cdh.cluster.Cluster._get_api_resource") as api_source,\
                mock.patch("cluster_config.cdh.cluster.Cluster._cdh_get_clusters") as all_clusters:
            logg.return_value = MagicMock()
            get_service.return_value = MagicMock()
            api_source.return_value = MagicMock()
            all_clusters.return_value = [type('obj', (object,), {'name' : "123", "displayName": "123"}),
                                         type('obj', (object,), {'name' : "1234", "displayName": "1234"})]

            cluster = Cluster("host", "Port", "username", "password", "1231")


            assert logg.call_count == 1
            logg.assert_called_with("Couldn't find cluster: '1231'")

    def test_get_clusters_two_clusters_cluster_name(self):
        with mock.patch("cluster_config.log.fatal") as logg, \
                mock.patch("cluster_config.cdh.cluster.Cluster._set_services") as get_service,\
                mock.patch("cluster_config.cdh.cluster.Cluster._get_api_resource") as api_source,\
                mock.patch("cluster_config.cdh.cluster.Cluster._cdh_get_clusters") as all_clusters,\
                mock.patch("__builtin__.input") as input_mock:
            logg.return_value = MagicMock()

            get_service.return_value = MagicMock()
            api_source.return_value = MagicMock()
            input_mock.return_value = 1
            all_clusters.return_value = [type('obj', (object,), {'name' : "123", "displayName": "first", "version": "1"}),
                                         type('obj', (object,), {'name' : "1234", "displayName": "second", "version": "2"})]

            cluster = Cluster("host", "Port", "username", "password", None)


            assert cluster.cluster.name == "123"
            assert cluster.cluster.displayName == "first"
            assert logg.call_count == 0

            input_mock.return_value = 2

            cluster = Cluster("host", "Port", "username", "password", None)


            assert cluster.cluster.name == "1234"
            assert cluster.cluster.displayName == "second"

    def test_get_clusters_two_clusters_invalid_index(self):
        with mock.patch("cluster_config.log.fatal") as logg, \
                mock.patch("cluster_config.cdh.cluster.Cluster._set_services") as get_service,\
                mock.patch("cluster_config.cdh.cluster.Cluster._get_api_resource") as api_source,\
                mock.patch("cluster_config.cdh.cluster.Cluster._cdh_get_clusters") as all_clusters,\
                mock.patch("__builtin__.input") as input_mock:
            logg.return_value = MagicMock()

            get_service.return_value = MagicMock()
            api_source.return_value = MagicMock()
            input_mock.return_value = 9
            all_clusters.return_value = [type('obj', (object,), {'name' : "123", "displayName": "first", "version": "1"}),
                                         type('obj', (object,), {'name' : "1234", "displayName": "second", "version": "2"})]

            Cluster("host", "Port", "username", "password", None)

            assert logg.call_count == 1

    def test_set_services(self):
        cc.cdh.Service = MagicMock()
        with mock.patch("cluster_config.cdh.cluster.Cluster._get_api_resource") as api_source,\
                mock.patch("cluster_config.cdh.cluster.Cluster._cdh_get_clusters") as all_clusters,\
                mock.patch("cluster_config.cdh.cluster.Cluster._cdh_get_services") as cdh_services,\
                mock.patch("cluster_config.cdh.cluster.Cluster._service") as services:
            api_source.return_value = MagicMock()
            all_clusters.return_value = [type('obj', (object,), {'name' : "123"})]
            cdh_services.return_value = [type('obj', (object,), {'name' : "yarn", "key": "YARN"})]

            services.return_value = return_value=type('obj', (object,), {'name' : "yarn", "key": "YARN"})
            #services.return_value = {}



            cluster = Cluster("host", "Port", "username", "password", "cluster_name")


            assert cluster.yarn
            assert cluster.services["YARN"]

