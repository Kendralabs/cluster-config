from pprint import pprint
import os
import io
import json
import sys
import shutil
from pyhocon import ConfigFactory
import cluster_config as atk


def copy_generated_conf():
    shutil.copyfile(atk.TAPROOT_CONFIG_FILE,atk.TAPROOT_USER_CONFIG_FILE)


def get_cdh_auto_config():
    return open_json_conf(atk.CDH_CONFIG_FILE)


def get_chd_user_config():
    return open_json_conf(atk.CDH_USER_CONFIG_FILE)


def get_atk_user_config():
    return open_hocon(atk.TAPROOT_USER_CONFIG_FILE)


def open_hocon(path):
    try:
        return ConfigFactory.parse_file(path)
    except IOError as e:
        atk.log.error("No hocon file found at: ".format(path))
        return None


def write_cdh_conf(dictionary):
    atk.file.write_json_conf(dictionary, atk.CDH_CONFIG_FILE)
    atk.log.info("Wrote {0} json file".format(atk.CDH_CONFIG_FILE))


def check_dir_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)


def open_json_conf(path):
    conf = None
    atk.log.debug("open_json_conf: {0}".format(path))
    try:
        configJsonOpen = io.open(path, encoding="utf-8", mode="r")
        conf = json.loads(configJsonOpen.read())
        configJsonOpen.close()
    except IOError as e:
        print("Couldn't open json file: {0}".format(path))
    return conf


def write_json_conf(json_dict, path):
    try:
        configJsonOpen = io.open(path, encoding="utf-8", mode="w")
        configJsonOpen.write(unicode(json.dumps(json_dict, indent=True, sort_keys=True)))
        configJsonOpen.close()
    except IOError as e:
        print("couldn't write {0}".format(path))
        sys.exit(1)

