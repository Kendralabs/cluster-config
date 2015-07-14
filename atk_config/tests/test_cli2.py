import unittest
from mock import MagicMock
import argparse
from atk_config import cli

class TestCli(unittest.TestCase):
    def test_host(self):
        parser = argparse.ArgumentParser(description="sample parser")
        parser.add_argument = MagicMock()

        cli.add_cdh_command_line_options(parser)

        parser.add_argument.assert_any_call("--host", type=str, help="Cloudera Manager Host", default="127.0.0.1")


    def test_port(self):
        parser = argparse.ArgumentParser(description="sample parser")
        parser.add_argument = MagicMock()

        cli.add_cdh_command_line_options(parser)

        parser.add_argument.assert_any_call("--port", type=int, help="Cloudera Manager Port", default=7180)

    def test_username(self):
        parser = argparse.ArgumentParser(description="sample parser")
        parser.add_argument = MagicMock()

        cli.add_cdh_command_line_options(parser)

        parser.add_argument.assert_any_call("--username", type=str, help="Cloudera Manager User Name", default="admin")

    def test_password(self):
        parser = argparse.ArgumentParser(description="sample parser")
        parser.add_argument = MagicMock()

        cli.add_cdh_command_line_options(parser)

        parser.add_argument.assert_any_call("--password", type=str, help="Cloudera Manager Password", default="admin")

    def test_cluster(self):
        parser = argparse.ArgumentParser(description="sample parser")
        parser.add_argument = MagicMock()

        cli.add_cdh_command_line_options(parser)

        parser.add_argument.assert_any_call("--cluster", type=str, help="Cloudera Manager Cluster Name if more than one cluster is managed by "
                                                    "Cloudera Manager.", default="cluster")


    def test_log(self):
        parser = argparse.ArgumentParser(description="sample parser")
        parser.add_argument = MagicMock()

        cli.add_cdh_command_line_options(parser)

        parser.add_argument.assert_any_call("--log", type=str, help="Cloudera Manager Password", default="admin")

    def test_for_only_default(self):
        parser = argparse.ArgumentParser(description="sample parser")
        cli.add_cdh_command_line_options(parser)

        assert len(parser._actions) == 7
