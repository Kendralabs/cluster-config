import sys
import atk_config as atk

def to_string(dictionary):
    dictionary_string = ""
    for key in dictionary:
        if type(dictionary[key]) is dict:
            dictionary_string += "{0} {{".format(key)

        if atk.base._recurse_type_check(dictionary, key):
            dictionary_string += to_string(dictionary[key])

        if type(dictionary[key]) is dict:
            dictionary_string += "}}".format(key)
    return dictionary_string



def nest(nested, split, value):
    if len(split) > 0:
        if split[0] not in nested:
            nested[split[0]] = {}
        if len(split) == 1:
            nested[split[0]] = value
        elif len(split) > 1:
            nest(nested[split[0]], split[1:], value)

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

    print("[{0}]User value: {1}".format(atk.SINGLE[0], user_value))
    auto_value = get_value(conflict, auto_configs)
    print("[{0}]Auto gen value: {1}".format(atk.SINGLE[1], auto_value))
    print("Optionally you can accept [{0}] all user values or [{1}] all auto generated values.".format(atk.PERSISTANT[0], atk.PERSISTANT[1]))
    while keep is None:
        if atk.RETRY <= 0:
            print("Too many retries no valid entry.")
            sys.exit(1)

        keep = raw_input("Would you like to keep the user value or the auto generated value?[[{0}|{1}][{2}|{3}]]: ".format(atk.SINGLE[0], atk.PERSISTANT[0], atk.SINGLE[1], atk.PERSISTANT[1])).strip()
        if keep not in atk.SINGLE and keep not in atk.PERSISTANT:
            print("Not a valid answer please try again.")
            keep = None
            atk.RETRY -= 1

    if keep in atk.SINGLE:
        return None, user_value if keep == atk.SINGLE[0] else auto_value
    else:
        return keep, user_value if keep == atk.PERSISTANT[0] else auto_value

def resolve_conflicts(conflicts, first_dictionary, second_dictionary, conflict_resolution_preference):
    resolved = []
    if conflict_resolution_preference is "first":
        keep = atk.PERSISTANT[0]
    elif conflict_resolution_preference is "second":
        keep = atk.PERSISTANT[1]
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