from pprint import pprint
import os, io, json, sys, shutil
from pyhocon import ConfigFactory

CONFIG_FILE_NAME = "application.json"

CONFIG_DIR = "conf"
CONFIG_EXT = "conf"
JSON_EXT = "json"
CONFIG_USER = "user"
CONFIG_GEN = "generated"

CDH_CONFIG_NAME = "cdh"
CDH_USER_CONFIG_NAME = "user-cdh"

def open_json_conf(path):
    conf = None
    print path
    try:
        configJsonOpen = io.open(path, encoding="utf-8", mode="r")
        conf = json.loads(configJsonOpen.read())
        configJsonOpen.close()
    except IOError as e:
        print("Couldn't open json file: {0}".format(path))
    return conf

#def merge_dicts( first, second):


def find_dict_conflicts(user_dict, auto_generated, config_key=[]):
    conflict_keys = []
    for key in user_dict:
        if type(user_dict[key]) == dict and auto_generated[key]:
            config_key.append(key)
            for add_key in find_dict_conflicts(user_dict[key], auto_generated[key], config_key):
                conflict_keys.append(add_key)
            config_key.pop()
        elif hasattr(auto_generated,key) is False or auto_generated[key] is None:
            continue
        else:
            temp = [key]
            conflict_keys.append(config_key + temp)
    return conflict_keys
