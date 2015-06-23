from atk_config import cdh, cli
import argparse

parser = argparse.ArgumentParser(description="Auto generate configurations for CDH based resources available to your cluster")
cli.add_cdh_command_line_options(parser)
args = parser.parse_args()

cluster = cdh.Cluster(args.host, args.port, args.username, args.password, args.cluster)


##

def get_generated_config():
    return base.open_json_conf("{0}/{1}.{2}".format(base.CONFIG_DIR,base.CDH_CONFIG_NAME, base.JSON_EXT))

def get_user_config():
    return base.open_json_conf("{0}/{1}.{2}".format(base.CONFIG_DIR,base.CDH_USER_CONFIG_NAME, base.JSON_EXT))
