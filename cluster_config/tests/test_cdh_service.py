import unittest
import contextlib
from mock import MagicMock
import mock as mock
from cluster_config.cdh.service import  Service

class TestCdhService(unittest.TestCase):


    def test_roles(self):
        with mock.patch("cluster_config.cdh.service.Service._get_cdh_roles") as get_cdh_roles,\
                mock.patch("cluster_config.cdh.service.Service._role") as role,\
                mock.patch("cluster_config.cdh.service.Service._get_all_role_config_groups") as config_groups:
            get_cdh_roles.return_value = [type('obj', (object,), {'type' : "NODEMANAGER"}),
                                          type('obj', (object,), {'type' : "RESOURCEMANAGER"})]
            role.side_effect = [type('obj', (object,), {'name' : "nodemanager", "key": "NODEMANAGER"}),
                            type('obj', (object,), {'name' : "resourcemanager", "key": "RESOURCEMANAGER"})]

            mocked = MagicMock()

            service = Service( mocked, mocked, mocked)

            assert service.nodemanager
            assert service.roles["NODEMANAGER"]
            assert service.resourcemanager
            assert service.roles["RESOURCEMANAGER"]
