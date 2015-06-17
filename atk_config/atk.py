import base
from pprint import pprint
import os, io, json, sys, shutil
from pyhocon import ConfigFactory





def check_dir_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)


def write_json_conf(json_dict, path):
    try:
        configJsonOpen = io.open(path, encoding="utf-8", mode="w")
        configJsonOpen.write(unicode(json.dumps(json_dict, indent=True, sort_keys=True)))
        configJsonOpen.close()
    except IOError as e:
        print("couldn't write {0}".format(path))
        sys.exit(1)


#def open_json_conf(path):
#    conf = None
#    try:
#        configJsonOpen = io.open(path, encoding="utf-8", mode="r")
#        conf = json.loads(configJsonOpen.read())
#        configJsonOpen.close()
#    except IOError as e:
#        print("Couldn't open json file: {0}".format(path))
#    return conf


def copy_generated_conf():
    shutil.copyfile("conf/{0}.{1}".format(base.CONFIG_GEN, base.CONFIG_EXT),"conf/{0}.{1}".format(base.CONFIG_USER, base.CONFIG_EXT))


def open_hocon(path):
    try:
        return ConfigFactory.parse_file(path)
    except IOError as e:
        print("no {0}.{1}".format(base.CONFIG_USER, base.CONFIG_EXT))
        return None


def set_conf(json_dict, type):
    check_dir_exists("conf")
    return write_json_conf(json_dict, "conf/{0}.{1}".format(type, base.CONFIG_EXT))


def get_conf(type):
    check_dir_exists("conf")
    return open_hocon("conf/{0}.{1}".format(type, base.CONFIG_EXT))


def get_user_conf():
    return get_conf("user")


def set_user_conf(json_dict):
    return set_conf(json_dict, "user")


def set_generated_conf(json_dict):
    return set_conf(json_dict, "generated")


def save_config(user_configs, auto_generated):
    #copy auto_generated configs
    tempConfigs = auto_generated.copy()
    #update with user configs overriding conflicts
    tempConfigs.update(user_configs)

    #cond key conflicts
    conflicts = find_conflicts(user_configs, auto_generated)
    pprint(conflicts)
    #ask user to resolve conflicts
    resolved = resolve_conflicts(conflicts, user_configs, auto_generated)
    #update resolved keys
    pprint(resolved)
    update_configs(resolved, user_configs, auto_generated)


def find_conflicts(user_dict, auto_generated, config_key=[]):
    conflict_keys = []
    for key in user_dict:
        if type(user_dict[key]) == dict and auto_generated[key]:
            config_key.append(key)
            for add_key in find_conflicts(user_dict[key], auto_generated[key], config_key):
                conflict_keys.append(add_key)
            config_key.pop()
        elif hasattr(auto_generated,key) is False or auto_generated[key] is None:
            continue
        else:
            temp = [key]
            conflict_keys.append(config_key + temp)
    return conflict_keys


def get_value(config_keys, configs):
    for key in config_keys:
        if configs[key] and type(configs[key]) == dict:
            return get_value(config_keys[1:], configs[key])
        elif configs[key]:
            return configs[key]

def merge_dicts(dict_one, dict_two):
    temp = {}
    print ""
    print "merge dicts"
    pprint(dict_one)
    pprint(dict_two)
    print "--"
    temp = dict_one.copy()
    pprint(temp)
    for key in dict_two:
        print "dict one: {0}".format(hasattr(dict_one,key))
        if key in dict_one and type(dict_two[key]) == dict:
            print "recurse: {0}".format(key)
            temp[key] = merge_dicts(dict_one[key], dict_two[key])
        elif key not in dict_one:
            print "no matching key: {0}".format(key)
            temp[key] = dict_two[key]
            print "continue"
        pprint(temp)
    return temp

def set_value(value, config_keys, configs):
    for key in config_keys:
        if configs[key] and type(configs[key]) == dict:
            return set_value(value, config_keys[1:], configs[key])
        elif configs[key]:
            configs[key] = value

def resolve_conflict(conflict, user_configs, auto_configs, keep=None):
    print("\nKey merge conflict: {0}".format('.'.join(conflict)))
    user_value = get_value(conflict, user_configs)

    print("[u|user]User value: {0}".format(user_value))
    auto_value = get_value(conflict, auto_configs)
    print("[a|auto]Auto gen value: {0}".format(auto_value))
    print("Optionally you can accept [au|alluser]all user values or [aa|allauto]all auto generated values.")
    if keep is None:
        keep = raw_input("Would you like to keep user value or the auto gen value?[[user|au][all|aa]]: ").strip()
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






def update_configs(resolved_configs, user_configs, auto_generated):
    temp = merge_dicts(auto_generated, user_configs)
    print "DONE"
    pprint(temp)
    temp = merge_dicts(user_configs, auto_generated)
    print "DONE"
    pprint(temp)

    #for resolved in resolved_configs:
    #    pprint(resolved)
    #    pprint(resolved[0])
    #    pprint(resolved[1])
    #    configs = set_value(resolved[1], resolved[0], configs)
    #    pprint(configs)



