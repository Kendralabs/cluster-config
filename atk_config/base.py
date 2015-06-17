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


def merge_dicts(first, second):

    conflicts = find_dict_conflicts(first, second)
    print "conflicts"
    pprint(conflicts)
    resolved = resolve_conflicts(conflicts, first, second)
    pprint(resolved)
    temp = _merge_dicts(first, second)
    print "DONE first second"
    pprint(temp)
    temp = _merge_dicts(second, first)
    print "DONE second first"
    pprint(temp)
    set_resolved(resolved, temp)
    pprint(temp)


def _recurse_type_check(dictionary, key):
    return type(dictionary[key]) is dict or type(dictionary[key]) is list

def _merge_dicts(dict_one, dict_two):
    temp = {}
    #print ""
    #print "merge dicts"
    #pprint(dict_one)
    #pprint(dict_two)
    #print "--"
    temp = dict_one.copy()
    #pprint(temp)
    for key in dict_two:
        #print "dict one: {0}".format(hasattr(dict_one,key))
        if _recurse_type_check(dict_two, key) and key in dict_one:
            #print "recurse: {0}".format(key)
            temp[key] = _merge_dicts(dict_one[key], dict_two[key])
        elif key not in dict_one:
            #print "no matching key: {0}".format(key)
            temp[key] = dict_two[key]
            #print "continue"
        #pprint(temp)
    return temp

def find_dict_conflicts(user_dict, auto_generated, config_key=[]):
    conflict_keys = []
    for key in user_dict:
        #if the value of the key type is dict and the key exists in auto_generated recurse
        print "key: ", key, type(user_dict[key])
        if _recurse_type_check(user_dict, key) and key in auto_generated:
            config_key.append(key)
            for add_key in find_dict_conflicts(user_dict[key], auto_generated[key], config_key):
                conflict_keys.append(add_key)
            config_key.pop()
        elif auto_generated.has_key(key) is None or auto_generated.get(key) is None:
            continue
        else:
            temp = [key]
            conflict_keys.append(config_key + temp)
    return conflict_keys


def resolve_conflict(conflict, user_configs, auto_configs, keep=None):
    print("\nKey merge conflict: {0}".format('.'.join(conflict)))
    user_value = get_value(conflict, user_configs)

    print("[u|user]User value: {0}".format(user_value))
    auto_value = get_value(conflict, auto_configs)
    print("[a|auto]Auto gen value: {0}".format(auto_value))
    print("Optionally you can accept [au] all user values or [ag] all auto generated values.")
    if keep is None:
        keep = raw_input("Would you like to keep the user value or the auto generated value?[[user|au][auto|ag]]: ").strip()
    if keep == "user" or keep == "au":
        return (keep, user_value)
    else:
        return (keep, auto_value)

def resolve_conflicts(conflicts, user_configs, auto_configs):
    resolved = []
    keep = None
    for conflict in conflicts:
        keep, value = resolve_conflict(conflict, user_configs, auto_configs, keep)
        resolved.append((conflict, value))

    return resolved

def set_value(value, config_keys, configs):
    for key in config_keys:
        if key in configs and _recurse_type_check(configs, key):
            return set_value(value, config_keys[1:], configs[key])
        elif key in configs:
            configs[key] = value


def get_value(config_keys, configs):
    for key in config_keys:
        #if configs[key] and type(configs[key]) == dict:
        if key in configs and _recurse_type_check(configs, key):
            return get_value(config_keys[1:], configs[key])
        elif configs[key]:
            return configs[key]

def set_resolved(resolved, dictionary):
    for key, value in resolved:
        print "key: ", key, "value: " ,value
        set_value(value, key, dictionary)