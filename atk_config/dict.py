import sys
import atk_config as atk
from pprint import pprint

def nest(nested, keys, value):
    """
    turns a list of keys into a nested dictionary with the last key getting value assigned to it.
    given
    temp = {}
    split = [ "one", "two", "three" ]
    value = 3
    you will end up with
    temp["one"]["two"]["three"] = 3
    :param nested: some dictionary
    :param keys: the key list
    :param value: the value of the last split element
    """
    if keys and len(keys) > 0:
        if keys[0] not in nested:
            nested[keys[0]] = {}

        if len(keys) == 1:
            nested[keys[0]] = value
        elif len(keys) > 1:
            nest(nested[keys[0]], keys[1:], value)


def merge_dicts(first_dictionary, second_dictionary, conflict_resolution_preference="first"):
    """
    Takes to dictionaries and does a nested merge. Key conflicts are resolved either by defaulting to
    first or second dictionary or interactively.


    :param first_dictionary: The first dictionary to merge
    :param second_dictionary: the second dictionary to merge
    :param conflict_resolution_preference: The conflict resolution preference[ "first", "second", "interactive"]
        "first" defaults the value of any conflict to the first dictionary
        "second" defaults the value of any conflict to the second dictionary
        "interactive" no defaults you are asked at runtime
    :return: merged dictionary with resolved conflicts
    """
    if first_dictionary is None or second_dictionary is None:
        return None

    conflicts = find_dict_conflicts(first_dictionary, second_dictionary)

    resolved = resolve_conflicts(conflicts, first_dictionary, second_dictionary, conflict_resolution_preference)

    temp = _merge_dicts(first_dictionary, second_dictionary)

    set_resolved(resolved, temp)

    return temp


def _recurse_type_check(dictionary, key):
    """
    Checks of the dictionary key is a list or another dictionary
    :param dictionary: The dictionary that has the key
    :param key: the key we are checking
    :return:
    """
    if dictionary and key:
        return type(dictionary[key]) is dict or type(dictionary[key]) is list
    else:
        return False


def _merge_dicts(first_dictionary, second_dictionary):
    """
    merge two nested dictionaries. Non dictionary types will be ignored. Conflicting keys are ignored.
    We want a merged dictionary with all the same keys. Conflicting dictionary values will be delt with later.

    :param first_dictionary: first dictionary to merge
    :param second_dictionary: second dictionary to merge
    :return:
    """
    if type(first_dictionary) and type(second_dictionary) is not dict:
        return None

    temp = {}
    temp = first_dictionary.copy()

    for key in second_dictionary:

        if _recurse_type_check(second_dictionary, key) and key in first_dictionary:

            temp[key] = _merge_dicts(first_dictionary[key], second_dictionary[key])
        elif key not in first_dictionary:

            temp[key] = second_dictionary[key]

    return temp


def find_dict_conflicts(first_dictionary, second_dictionary, config_key=[]):
    """
    find key conflicts between two dictionaries.
    A key is not considered conflicting if it's the same value in both dictionaries

    :param first_dictionary:
    :param second_dictionary:
    :param config_key: list of of our current nested key [ lvl0, lvl1, lvl3, ...]
    :return:
    """

    conflict_keys = []

    for key in first_dictionary:
        #if the value of the key type is dict and the key exists in auto_generated recurse
        if _recurse_type_check(first_dictionary, key) and key in second_dictionary:
            print "recurse: ", key
            config_key.append(key)
            for add_key in find_dict_conflicts(first_dictionary[key], second_dictionary[key], config_key):
                conflict_keys.append(add_key)
            config_key.pop()
        elif key not in second_dictionary or second_dictionary.get(key) is None or second_dictionary.get(key) == first_dictionary.get(key):
            print "continue: ", key
            continue
        else:
            print "temp: key :", key
            temp = [key]
            conflict_keys.append(config_key + temp)
    return conflict_keys


def resolve_conflict(conflict, user_configs, auto_configs, keep=None):
    """
    resolve conflicts either by asking or going forward with defaults form command line
    :param conflict: conflicting kye
    :param user_configs: value for first conflicting key
    :param auto_configs: value for second conflicting key
    :param keep: answer for subsequent request
    :return: resolved value for key
    """
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
        atk.log.info("Auto resolving conflicts. Defaulting to {0} value".format("user" if keep == atk.PERSISTANT[0] else "auto generated"))
        return keep, user_value if keep == atk.PERSISTANT[0] else auto_value


def resolve_conflicts(conflicts, first_dictionary, second_dictionary, conflict_resolution_preference):
    """

    :param conflicts:
    :param first_dictionary:
    :param second_dictionary:
    :param conflict_resolution_preference:
    :return:
    """
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