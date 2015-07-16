from cluster_config import cli
from cluster_config.cdh.cluster import Cluster
import hashlib, re, time, os, time, sys, getpass
from pprint import pprint
import argparse

parser = argparse.ArgumentParser(description="Process cl arguments to avoid prompts in automation")
parser.add_argument("--dump", type=str, help="If you want to dump all configs without asking pass 'yes'. Defaults to 'no'.", default="no")

args = cli.parse(parser)


cluster = Cluster(args.host, args.port, args.username, args.password, args.cluster)

def main():



    def pick(parentService, childService, parentServiceName, serviceList):
        print("Available {0} types on {1}: '{2}'".format(childService, parentService,parentServiceName))
        print("Pick a {0}".format(childService))
        count = 0
        list = []
        for service in serviceList:
            print("Id {0} {1}: {2}".format(count+1, childService, service))
            list.append(service)
            count += 1
        service_index = input("Enter {0} Id : ".format(childService))
        print service_index, len(serviceList)
        if service_index <= 0 or service_index > len(serviceList):
            run_again()
        service_index -= 1
        return list[service_index]

    dump = "no"
    if args.dump != "yes":
        dump = raw_input("dump all configs[yes or no]: ").strip()

    if dump == "yes" or args.dump == "yes":
        for service in cluster.cdh_services:
            for role in cluster.cdh_services[service].roles:
                for config_group in cluster.cdh_services[service].roles[role].config_groups:
                    for config in cluster.cdh_services[service].roles[role].config_groups[config_group].configs:
                        print_details(config, service, role, config_group)
        run_again()

    service_index = pick("cluster", "service", cluster.user_cluster_name, cluster.cdh_services)
    print service_index

    pprint(cluster.cdh_services[service_index].roles)
    role_index = pick("service", "role", service_index, cluster.cdh_services[service_index].roles)
    print role_index

    config_group_index = pick("role", "config group", role_index,
                             cluster.cdh_services[service_index].roles[role_index].config_groups)
    print config_group_index


    print ""
    for config in cluster.cdh_services[service_index].roles[role_index].config_groups[config_group_index].configs:
        print_details(config, service_index, role_index, config_group_index)

    run_again()

def print_details(config, service_index, role_index, config_group_index):
    print("- config name: {0} config description: {1}".format(config,
                                                            cluster.cdh_services[service_index].
                                                            roles[role_index].
                                                            config_groups[config_group_index].
                                                            configs[config].
                                                            cdh_config.description))
    print("- config key: {0}.{1}.{2}.{3}".format(service_index,role_index,config_group_index,cluster.cdh_services[service_index].
                                                            roles[role_index].
                                                            config_groups[config_group_index].
                                                            configs[config].key))
    print("- config value: {0}".format(cluster.cdh_services[service_index].
                                                            roles[role_index].
                                                            config_groups[config_group_index].
                                                            configs[config].value))
    print("")

def run_again():
    if args.dump == "no":
        if raw_input("Would you like to run the script again?[yes or no]: ").strip() == "yes":
            main()
        else:
            sys.exit(0)
    else:
        sys.exit(0)