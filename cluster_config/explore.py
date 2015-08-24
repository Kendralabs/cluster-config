import cluster_config as cc
from cluster_config.cdh.cluster import Cluster
import sys
import argparse


def cli(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser(description="Process cl arguments to avoid prompts in automation")

    parser.add_argument("--dump", type=str, help="If you want to dump all configs without asking pass 'yes'. Defaults to 'no'.", default="no")

    return parser


def main():
    run(cc.cli.parse(cli()))


def run(args, cluster=None):
    if cluster is None:
        cluster = Cluster(args.host, args.port, args.username, args.password, args.cluster)

    dump = "no"
    if args.dump != "yes":
        dump = raw_input("dump all configs[yes or no]: ").strip()

    if dump == "yes" or args.dump == "yes":
        for service in cluster.services:
            for role in cluster.services[service].roles:
                for config_group in cluster.services[service].roles[role].config_groups:
                    for config in cluster.services[service].roles[role].config_groups[config_group].configs:
                        print_details(cluster, config, service, role, config_group)

    service_index = pick("cluster", "service", cluster.user_cluster_name, cluster.services)

    role_index = pick("service", "role", service_index, cluster.services[service_index].roles)

    config_group_index = pick("role", "config group", role_index,
                             cluster.services[service_index].roles[role_index].config_groups)


    print("")

    for config in cluster.services[service_index].roles[role_index].config_groups[config_group_index].configs:
        print_details(cluster, config, service_index, role_index, config_group_index)

    run_again(args)

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
        if service_index <= 0 or service_index > len(serviceList):
            run_again()
        service_index -= 1
        print("Selected {0}".format(list[service_index]))
        return list[service_index]

def print_details(cluster, config, service_index, role_index, config_group_index):
    print("config: ")
    print("- name: {0}".format(config,))
    print("- description: {0}".format(cluster.services[service_index].
                                                            roles[role_index].
                                                            config_groups[config_group_index].
                                                            configs[config].
                                                            cdh_config.description))
    print("- key: {0}.{1}.{2}.{3}".format(service_index,role_index,config_group_index,cluster.services[service_index].
                                                            roles[role_index].
                                                            config_groups[config_group_index].
                                                            configs[config].key))
    print("- value: {0}".format(cluster.services[service_index].
                                                            roles[role_index].
                                                            config_groups[config_group_index].
                                                            configs[config].value))
    print("")

def run_again(args):
    if args.dump == "no":
        if raw_input("Would you like to run the script again?[yes or no]: ").strip() == "yes":
            main()
        else:
            sys.exit(0)
    else:
        sys.exit(0)

