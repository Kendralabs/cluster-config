import base

from pprint import pprint
import os, io, json, sys, shutil
from pyhocon import ConfigFactory


def copy_generated_conf():
    shutil.copyfile("conf/{0}.{1}".format(base.CONFIG_GEN, base.CONFIG_EXT),"conf/{0}.{1}".format(base.CONFIG_USER, base.CONFIG_EXT))


def open_hocon(path):
    try:
        return ConfigFactory.parse_file(path)
    except IOError as e:
        print("no {0}.{1}".format(base.CONFIG_USER, base.CONFIG_EXT))
        return None


def set_conf(json_dict, type):
    base.check_dir_exists("conf")
    return base.write_json_conf(json_dict, "conf/{0}.{1}".format(type, base.CONFIG_EXT))


def get_conf(type):
    base.check_dir_exists("conf")
    return open_hocon("conf/{0}.{1}".format(type, base.CONFIG_EXT))


def get_user_conf():
    return get_conf("user")


def set_user_conf(json_dict):
    return set_conf(json_dict, "user")


def set_generated_conf(json_dict):
    return set_conf(json_dict, "generated")