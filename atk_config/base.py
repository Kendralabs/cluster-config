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

SINGLE = ["user", "auto"]
PERSISTANT = ["au", "ag"]
RETRY = 5

CONFLICT_RESOLUTION = ["interactive", "first", "second"]


def check_dir_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)

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


def merge_dicts(first_dictionary, second_dictionary, conflict_resolution_preference="first"):

    conflicts = find_dict_conflicts(first_dictionary, second_dictionary)

    resolved = resolve_conflicts(conflicts, first_dictionary, second_dictionary, conflict_resolution_preference)

    temp = _merge_dicts(first_dictionary, second_dictionary)

    set_resolved(resolved, temp)

    return temp

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

def find_dict_conflicts(first_dictionary, second_dictionary, config_key=[]):
    conflict_keys = []
    for key in first_dictionary:
        #if the value of the key type is dict and the key exists in auto_generated recurse
        if _recurse_type_check(first_dictionary, key) and key in second_dictionary:
            config_key.append(key)
            for add_key in find_dict_conflicts(first_dictionary[key], second_dictionary[key], config_key):
                conflict_keys.append(add_key)
            config_key.pop()
        elif key not in second_dictionary or second_dictionary.get(key) is None or second_dictionary.get(key) == first_dictionary.get(key):
            continue
        else:
            temp = [key]
            conflict_keys.append(config_key + temp)
    return conflict_keys


def resolve_conflict(conflict, user_configs, auto_configs, keep=None):
    print("\nKey merge conflict: {0}".format('.'.join(conflict)))
    user_value = get_value(conflict, user_configs)

    print("[{0}]User value: {1}".format(SINGLE[0], user_value))
    auto_value = get_value(conflict, auto_configs)
    print("[{0}]Auto gen value: {1}".format(SINGLE[1], auto_value))
    print("Optionally you can accept [{0}] all user values or [{1}] all auto generated values.".format(PERSISTANT[0], PERSISTANT[1]))
    while keep is None:
        if RETRY <= 0:
            print("Too many retries no valid entry.")
            sys.exit(1)

        keep = raw_input("Would you like to keep the user value or the auto generated value?[[{0}|{1}][{2}|{3}]]: ".format(SINGLE[0], PERSISTANT[0], SINGLE[1], PERSISTANT[1])).strip()
        if keep not in SINGLE and keep not in PERSISTANT:
            print("Not a valid answer please try again.")
            keep = None
            RETRY -= 1

    if keep in SINGLE:
        return None, user_value if keep == SINGLE[0] else auto_value
    else:
        return keep, user_value if keep == PERSISTANT[0] else auto_value

def resolve_conflicts(conflicts, first_dictionary, second_dictionary, conflict_resolution_preference):
    resolved = []
    if conflict_resolution_preference is "first":
        keep = PERSISTANT[0]
    elif conflict_resolution_preference is "second":
        keep = PERSISTANT[1]
    else:
        keep = None
    for conflict in conflicts:
        keep, value = resolve_conflict(conflict, first_dictionary, second_dictionary, keep)
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
        set_value(value, key, dictionary)

def write_json_conf(json_dict, path):
    try:
        configJsonOpen = io.open(path, encoding="utf-8", mode="w")
        configJsonOpen.write(unicode(json.dumps(json_dict, indent=True, sort_keys=True)))
        configJsonOpen.close()
    except IOError as e:
        print("couldn't write {0}".format(path))
        sys.exit(1)