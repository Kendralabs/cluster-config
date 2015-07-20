import argparse

import cluster_config as cc
from cluster_config import log
from cluster_config import file
from cluster_config.cdh.cluster import Cluster


parser = argparse.ArgumentParser(description="Process cl arguments to avoid prompts in automation")
parser.add_argument("--update-cdh", type=str,
                    help="Should we update CDH with all configurations in {0}/{1}?".format(cc.CDH_CONFIG,cc.USER_CDH_CONFIG),
                    choices=["no", "yes"], required=True)
parser.add_argument("--restart-cdh", type=str, help="Should we restart CDH services after configuration changes",
                    choices=["no", "yes"], required=True)
parser.add_argument("--conflict-merge", type=str, help="When encountering merge conflicts between the generated "
                                                       "configuration() and the user configuration() what value "
                                                       "should we default to? The 'user', 'generated', or 'interactive'"
                                                       "resolution", default="first", choices=cc.CONFLICT_RESOLUTION)

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
            log.info("conflict resolution: {0}".format(args.conflict_merge))
            configs = cc.dict.merge_dicts(user_configs, cdh_configs, args.conflict_merge)
            merged_config_path = file.file_path(cc.MERGED_CDH_CONFIG, args.path)
            log.info("Writting merged CDH config file: {0}".format(merged_config_path))
            file.write_json_conf(configs, merged_config_path)
        else:
            configs = cdh_configs


        #if update cdh is "yes" then we iterate and update all the specified keys
        if args.update_cdh == "yes":
            #iterate through services, set cdh configs and possibly restart services
            cluster.update_configs(configs, False if args.restart_cdh == "no" else True)


    else:
        log.fatal("Couldn't connect to the CDH cluster")

