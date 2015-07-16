import argparse
from pprint import pprint
import cluster_config as cc
from cluster_config.cdh.cluster import Cluster


parser = argparse.ArgumentParser(description="Process cl arguments to avoid prompts in automation")
parser.add_argument("--update-cdh", type=str, help="Should we set all the CDH configs keys in config.json?", default="no")
parser.add_argument("--restart", type=str, help="Weather or not to restart CDH services after config changes", default="no")
parser.add_argument("--user-cdh-json-path", type=str, help="Directory where we can find the user generated cdh configs({0}). Defaults to working directory".format(cc.USER_CDH_CONFIG))
parser.add_argument("--atk-user-conf-path", type=str, help="Directory for the {0} file. Defaults to working directory".format(cc.USER_CDH_CONFIG))

args = cc.cli.parse(parser)

def main():
    #get the cluster reference
    cluster = Cluster(args.host, args.port, args.username, args.password, args.cluster)

    if cluster:
        #Read the generated configs

        cdh_configs = cc.file.open_json_conf(cc.file.file_path(cc.CDH_CONFIG, args.cdh_json_path))
        user_configs = cc.file.open_json_conf(cc.file.file_path(cc.USER_CDH_CONFIG, args.user_cdh_json_path))
        if user_configs:
            #merge config dictionaries and resolve conflicts
            configs = cc.dict.merge_dicts(user_configs, cdh_configs)


        #if update cdh is "yes" then we iterate and update all the specified keys
        if args.update_cdh == "yes":
            #iterate through services, set cdh configs and possibly restart services
            cluster.update_configs(configs, False if args.restart is "no" else True)

        user_conf = cc.file.get_atk_user_config()
        if user_conf is None:
            #copy generated.conf to user.conf
            cc.file.copy_generated_conf()

    else:
        print("Couldn't connect to the CDH cluster")