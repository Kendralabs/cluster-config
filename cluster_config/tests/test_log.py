import unittest
import logging
import sys

from mock import MagicMock
from cluster_config import log

class TestCli(unittest.TestCase):
    def test_info(self):
        log.logger.log = MagicMock()

        log.info("info")

        log.logger.log.assert_called_with(logging.INFO, "info")

    def test_warning(self):
        log.logger.log = MagicMock()

        log.warning("warning")

        log.logger.log.assert_called_with(logging.WARNING, "warning")

    def test_debug(self):
        log.logger.log = MagicMock()

        log.debug("debug")

        log.logger.log.assert_called_with(logging.DEBUG, "debug")

    def test_fatal(self):
        log.logger.log = MagicMock()
        sys.exit = MagicMock()
        log.fatal("fatal")

        log.logger.log.assert_called_with(logging.FATAL, "fatal")
        sys.exit.assert_called_with(1)

    def test_error(self):
        log.logger.log = MagicMock()

        log.error("error")

        log.logger.log.assert_called_with(logging.ERROR, "error")

