import argparse
from pprint import pprint
import atk_config as atk


parser = argparse.ArgumentParser(description="Process cl arguments to avoid prompts in automation")
parser.add_argument("--update-cdh", type=str, help="Should we set all the CDH configs keys in config.json?", default="no")
parser.add_argument("--restart", type=str, help="Weather or not to restart CDH services after config changes", default="no")
atk.cli.add_cdh_command_line_options(parser)
args = parser.parse_args()

def main():
    #get the cluster reference
    cluster = atk.cdh.Cluster(args.host, args.port, args.username, args.password, args.cluster)

    if cluster:
        #Read the generated configs

        configs = atk.file.get_cdh_auto_config()
        user_config = atk.file.get_chd_user_config()
        if user_config:
            #merge config dictionaries and resolve conflicts
            configs = atk.dict.merge_dicts(user_config, configs)


        #if update cdh is "yes" then we iterate and update all the specified keys
        if args.update_cdh == "yes":
            #iterate through services, set cdh configs and possibly restart services
            cluster.update_configs(configs, False if args.restart is "no" else True)

        user_conf = atk.file.get_atk_user_config()
        if user_conf is None:
            #copy generated.conf to user.conf
            atk.file.copy_generated_conf()

    else:
        print("Couldn't connect to the CDH cluster")