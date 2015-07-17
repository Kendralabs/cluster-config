import unittest
import logging
from mock import MagicMock
import mock as mock
import argparse
from cluster_config import cli
from cluster_config import log


class TestCli(unittest.TestCase):
    def test_host(self):
        parser = argparse.ArgumentParser(description="sample parser")
        parser.add_argument = MagicMock()

        cli.add_cluster_connection_options(parser)

        parser.add_argument.assert_any_call("--host", type=str, help="Cloudera Manager Host", required=True)


    def test_port(self):
        parser = argparse.ArgumentParser(description="sample parser")
        parser.add_argument = MagicMock()

        cli.add_cluster_connection_options(parser)

        parser.add_argument.assert_any_call("--port", type=int, help="Cloudera Manager Port", default=7180)

    def test_username(self):
        parser = argparse.ArgumentParser(description="sample parser")
        parser.add_argument = MagicMock()

        cli.add_cluster_connection_options(parser)

        parser.add_argument.assert_any_call("--username", type=str, help="Cloudera Manager User Name", default="admin")

    def test_password(self):
        parser = argparse.ArgumentParser(description="sample parser")
        parser.add_argument = MagicMock()

        cli.add_cluster_connection_options(parser)

        parser.add_argument.assert_any_call("--password", type=str, help="Cloudera Manager Password")

    def test_cluster(self):
        parser = argparse.ArgumentParser(description="sample parser")
        parser.add_argument = MagicMock()

        cli.add_cluster_connection_options(parser)

        parser.add_argument.assert_any_call("--cluster", type=str, help="Cloudera Manager Cluster Name if more than one cluster is managed by "
                                                    "Cloudera Manager.", default="cluster")


    def test_log(self):
        parser = argparse.ArgumentParser(description="sample parser")
        parser.add_argument = MagicMock()

        cli.add_cluster_connection_options(parser)

        parser.add_argument.assert_any_call("--log", type=str, help="Log level [INFO|DEBUG|WARNING|FATAL|ERROR]", default="INFO",
                            choices=["INFO", "DEBUG", "WARNING", "FATAL", "ERROR"])

    def test_for_only_default(self):
        parser = argparse.ArgumentParser(description="sample parser")
        cli.add_cluster_connection_options(parser)

        assert len(parser._actions) == 8


    def test_parse(self):
        with mock.patch("cluster_config.cli.get_cluster_password") as password:
            password.return_value = MagicMock()

            parser = argparse.ArgumentParser(description="sample parser")
            parser.parse_args = MagicMock(return_value= type('obj', (object,), {'log' : "DEBUG", "password": "123"}))

            parser.log = "DEBUG"

            args = cli.parse(parser)

            assert log.logger.level == logging.DEBUG

    def test_parse_invalid_log_level(self):
        with mock.patch("cluster_config.log.error") as log,\
                mock.patch("cluster_config.cli.get_cluster_password") as password:
            log.return_value = MagicMock()
            password.return_value = MagicMock()

            parser = argparse.ArgumentParser(description="sample parser")
            parser.parse_args = MagicMock(return_value= type('obj', (object,), {'log' : "123", "password": "123"}))

            parser.log = "DEBUG"

            args = cli.parse(parser)

            assert log.call_count == 1
            log.assert_called_with("Invalid log level: 123")
