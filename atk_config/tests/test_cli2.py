import unittest
from mock import MagicMock
import argparse
from atk_config import cli

class MyTest(unittest.TestCase):
    def test(self):
        parser = argparse.ArgumentParser(description="sample parser")
        parser.add_argument = MagicMock()

        cli.add_cdh_command_line_options(parser)

        parser.add_argument.assert_any_call("--host", type=str, help="Cloudera Manager Host", default="127.0.0.1")