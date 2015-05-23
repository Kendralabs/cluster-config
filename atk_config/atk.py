
from pprint import pprint

def find_conflicts_re(user_dict, auto_generated, config_key=[]):
    print ""
    print "re"
    pprint(config_key)
    pprint(user_dict)
    print config_key
    if config_key is None:
        config_key = []
    conflict_keys = []
    for key in user_dict:
        print key
        print user_dict[key]
        if type(user_dict[key]) == dict and auto_generated[key]:
            print "recurse"
            #find_conflicts_re(user_dict[key], auto_generated[key], [key] if config_key is None or len(config_key) == 0 else config_key.append(key))
            config_key.append(key)
            print config_key
            for conflict_key in find_conflicts_re(user_dict[key], auto_generated[key], config_key):
                conflict_keys.append(config_key)
        elif auto_generated[key] is None:
            print "continue"
            continue
        else:
            temp = [key]
            conflict_keys.append(config_key + temp)
    return conflict_keys





def find_conflicts(user_config, auto_generated):
    for intel in user_config:
            print intel
            print user_config[intel]
            for analytics in user_config[intel]:
                print analytics
                print user_config[intel][analytics]
                for group in user_config[intel][analytics]:
                    print group
                    print user_config[intel][analytics][group]
                    for key in user_config[intel][analytics][group]:
                        print key
                        print user_config[intel][analytics][group][key]
                        if auto_generated[intel][analytics][group][key] and \
                                        auto_generated[intel][analytics][group][key] != \
                                        user_config[intel][analytics][group][key]:
                            user_config[intel][analytics][group][key] = \
                                resolve_conflict([intel,analytics,group,key],
                                user_config[intel][analytics][group][key],
                                auto_generated[intel][analytics][group][key])


    print"fc"

def resolve_conflict(key, user_value, auto_value ):
    print("Key merge conflict: {0}".format('.'.join(key)))
    print("(user)User value: {0}".format(user_value))
    print("(auto)Auto gen value: {0}".format(auto_value))
    keep = raw_input("Would you like to keep user value or the auto gen value?[user/auto]: ").strip()
    if keep == "user":
        return user_value
    else:
        return auto_value