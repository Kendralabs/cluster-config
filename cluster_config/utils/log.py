import logging
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)
out = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('--%(levelname)s %(message)s')
out.setFormatter(formatter)
logger.addHandler(out)


def info(msg):
    write(logging.INFO, msg)


def warning(msg):
    write(logging.WARNING, msg)


def debug(msg):
    write(logging.DEBUG, msg)


def fatal(msg):
    write(logging.FATAL, msg)
    sys.exit(1)


def error(msg):
    write(logging.ERROR, msg)


def write(log_lvl, msg):
    logger.log(log_lvl, msg)

