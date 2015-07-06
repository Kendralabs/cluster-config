import logging
import sys
from atk_config import file
from atk_config import dict
from atk_config import log
from atk_config import cli
from atk_config import cdh

CONFIG_DIR = "conf"
HOCON_EXT = "conf"
JSON_EXT = "json"
CONFIG_USER = "user"
TAPROOT_CONFIG_NAME = "generated"

CDH_CONFIG_NAME = "cdh"
CDH_CONFIG_FILE = "{0}/{1}.{2}".format(CONFIG_DIR, CDH_CONFIG_NAME, JSON_EXT)

TAPROOT_CONFIG_FILE = "{0}/{1}.{2}".format(CONFIG_DIR, TAPROOT_CONFIG_NAME, HOCON_EXT)

TAPROOT_USER_CONFIG_FILE = "{0}/{1}.{2}".format(CONFIG_DIR, CONFIG_USER, HOCON_EXT)

CDH_USER_CONFIG_NAME = "user-cdh"

CDH_USER_CONFIG_FILE = "{0}/{1}.{2}".format(CONFIG_DIR, CDH_USER_CONFIG_NAME, JSON_EXT)

SINGLE = ["user", "auto"]
PERSISTANT = ["au", "ag"]
RETRY = 5

CONFLICT_RESOLUTION = ["interactive", "first", "second"]




