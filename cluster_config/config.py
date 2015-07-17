import argparse
from pprint import pprint
import os.path

import cluster_config as cc
from cluster_config import log
from cluster_config import file
from cluster_config.cdh.cluster import Cluster


parser = argparse.ArgumentParser(description="Process cl arguments to avoid prompts in automation")
parser.add_argument("--update-cdh", type=str, help="Should we set all the CDH configs keys in config.json?", default="no")
parser.add_argument("--restart", type=str, help="Weather or not to restart CDH services after config changes", default="no")

args = cc.cli.parse(parser)

def main():
    #get the cluster reference
    cluster = Cluster(args.host, args.port, args.username, args.password, args.cluster)

    if cluster:
        #Read the generated configs

        cdh_config_path = file.file_path(cc.CDH_CONFIG, args.path)
        log.info("Reading CDH config file: {0}".format(cdh_config_path))
        cdh_configs = file.open_json_conf(cdh_config_path)

        user_cdh_config_path = file.file_path(cc.USER_CDH_CONFIG, args.path)
        log.info("Reading user CDH config file: {0}".format(user_cdh_config_path))
        user_configs = file.open_json_conf(user_cdh_config_path)

        if user_configs:
            #merge config dictionaries and resolve conflicts
            configs = cc.dict.merge_dicts(user_configs, cdh_configs)
        else:
            configs = cdh_configs


        #if update cdh is "yes" then we iterate and update all the specified keys
        if args.update_cdh == "yes":
            #iterate through services, set cdh configs and possibly restart services
            cluster.update_configs(configs, False if args.restart == "no" else True)

    else:
        log.fatal("Couldn't connect to the CDH cluster")