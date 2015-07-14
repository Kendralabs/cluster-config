from atk_config import cli
from atk_config.cdh.cluster import Cluster
import hashlib, re, time, argparse, os, time, sys, getpass
from pprint import pprint

parser = argparse.ArgumentParser(description="Process cl arguments to avoid prompts in automation")
cli.add_cdh_command_line_options(parser)

args = parser.parse_args()

def main():

    cluster = Cluster(args.host, args.port, args.username, args.password, args.cluster)

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
        if service_index <= 0 or service_index > len(cluster.cdh_services):
            raise Exception("Not a valid {0} Id".format(childService))
        service_index -= 1
        return list[service_index]

    dump = raw_input("dump all configs[yes]: ").strip()
    if dump == "yes":
        for service in cluster.cdh_services:
            for role in cluster.cdh_services[service].roles:
                for config_group in cluster.cdh_services[service].roles[role].config_groups:
                    for config in cluster.cdh_services[service].roles[role].config_groups[config_group].configs:

                        print("config name: {0} config description: {1}".format(config,
                                                                cluster.cdh_services[service].
                                                                roles[role].
                                                                config_groups[config_group].
                                                                configs[config].
                                                                cdh_config.description))
                        print("\tconfig key: {0}.{1}.{2}.{3}".format(service,role,config_group,cluster.cdh_services[service].
                                                                roles[role].
                                                                config_groups[config_group].
                                                                configs[config].name))
                        print("")
        sys.exit(0)

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
        print("config name: {0} config description: {1}".format(config,
                                                                cluster.cdh_services[service_index].
                                                                roles[role_index].
                                                                config_groups[config_group_index].
                                                                configs[config].
                                                                cdh_config.description))
        print("\tconfig key: {0}.{1}.{2}.{3}".format(service_index,role_index,config_group_index,cluster.cdh_services[service_index].
                                                                roles[role_index].
                                                                config_groups[config_group_index].
                                                                configs[config].name))
        print("")