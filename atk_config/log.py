import logging
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)
out = logging.StreamHandler(sys.stdout)
out.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s %(message)s')
out.setFormatter(formatter)
logger.addHandler(out)

#any info message
def info(msg):
    logger.log(logging.INFO, msg)

#warning to user
def warning(msg):
    logger.log(logging.WARNING, msg)

#verbose error logging for dev purposes
def debug(msg):
    logger.log(logging.DEBUG, msg)

#fatal message, can't continue
def fatal(msg):
    logger.log(logging.FATAL,msg)
    sys.exit(1)

#any error that is recoverable like trying to save config for a non existent server, role, etc...
def error(msg):
    logger.log(logging.ERROR, msg)
