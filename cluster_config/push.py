import argparse

import cluster_config as cc
from cluster_config.cdh.cluster import Cluster, save_to_json
from cluster_config.utils import file, log


def cli(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser(description="Update CDH with {0}/{1} configuration values.".
                                         format(cc.CDH_CONFIG,cc.USER_CDH_CONFIG))

    parser.add_argument("--update-cdh", type=str,
                    help="Should we update CDH with all configurations in {0}/{1}?".
                        format(cc.CDH_CONFIG,cc.USER_CDH_CONFIG),
                    choices=["no", "yes"], required=True)
    parser.add_argument("--restart-cdh", type=str, help="Should we restart CDH services after configuration changes",
                    choices=["no", "yes"], required=True)
    parser.add_argument("--conflict-merge", type=str, help="When encountering merge conflicts between the generated "
                                                       "configuration() and the user configuration() what value "
                                                       "should we default to? The 'user', 'generated', or 'interactive'"
                                                       "resolution", default="user", choices=cc.CONFLICT_RESOLUTION)

    return parser


def main():
    run(cc.utils.cli.parse(cli()))


def run(args, cluster=None, dt=None):
    if cluster is None:
        cluster = Cluster(args.host, args.port, args.username, args.password, args.cluster)

    if cluster:
        cluster_before = save_to_json(cluster, args.path, "before")
        #Read the generated configs
        cdh_config_path = file.file_path(cc.CDH_CONFIG, args.path)
        log.info("Reading CDH config file: {0}".format(cdh_config_path))
        cdh_configs = file.open_json_conf(cdh_config_path)

        user_cdh_config_path = file.file_path(cc.USER_CDH_CONFIG, args.path)
        log.info("Reading user CDH config file: {0}".format(user_cdh_config_path))
        user_configs = file.open_json_conf(user_cdh_config_path)

        merged_config_path = None
        if user_configs:
            #merge config dictionaries and resolve conflicts
            log.info("conflict resolution: {0}".format(args.conflict_merge))
            configs = cc.utils.dict.merge_dicts(user_configs, cdh_configs, convert_conflict_merge(args.conflict_merge))
            merged_config_path = file.file_path(cc.MERGED_CDH_CONFIG, args.path)
            log.info("Writting merged CDH config file: {0}".format(merged_config_path))
            file.write_json_conf(configs, merged_config_path)
        else:
            configs = cdh_configs


        #if update cdh is "yes" then we iterate and update all the specified keys
        if args.update_cdh == "yes":
            #iterate through services, set cdh configs and possibly restart services
            cluster.update_configs(configs, False if args.restart_cdh == "no" else True)

        cluster_after = save_to_json(cluster, args.path, "after")
        file.snapshots(args.host, "push", args.path, dt, cluster_before, cluster_after, cdh_config_path,
                       user_cdh_config_path, merged_config_path)

    else:
        log.fatal("Couldn't connect to the CDH cluster")


def convert_conflict_merge(conflict_merge_preference):
    """
    convert between user, generated, interactive to first, second, interactive options
    :param conflict_merge_preference: conflict merge pref from command line
    :return: the conflict merge preference that dictionary merging will understand
    """
    if conflict_merge_preference == "user":
        return "first"
    elif conflict_merge_preference == "generated":
        return "second"
    elif conflict_merge_preference == "interactive":
        return "interactive"
