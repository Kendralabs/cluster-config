import unittest
import logging
from mock import MagicMock
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

        parser.add_argument.assert_any_call("--log", type=str, help="Log level [INFO|DEBUG|WARNING|FATAL|ERROR]", default="INFO")

    def test_for_only_default(self):
        parser = argparse.ArgumentParser(description="sample parser")
        cli.add_cluster_connection_options(parser)

        assert len(parser._actions) == 7


    def test_parse(self):
        parser = argparse.ArgumentParser(description="sample parser")
        parser.parse_args = MagicMock(return_value= type('obj', (object,), {'log' : "DEBUG"}))

        cli.get_cluster_password = MagicMock()
        parser.log = "DEBUG"

        args = cli.parse(parser)

        assert log.logger.level == logging.DEBUG

    def test_parse_invalid_log_level(self):
        parser = argparse.ArgumentParser(description="sample parser")
        parser.parse_args = MagicMock(return_value= type('obj', (object,), {'log' : "123"}))
        log.error = MagicMock()
        cli.get_cluster_password = MagicMock()
        parser.log = "DEBUG"

        args = cli.parse(parser)

        assert log.error.call_count == 1
        assert log.error.assert_call("Invalid log level: {0}".format("123"))
