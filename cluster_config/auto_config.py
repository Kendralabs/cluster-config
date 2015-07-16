from __future__ import print_function
import os
import argparse
import cluster_config as atk
from cluster_config import log
from cluster_config import file
from cluster_config.cdh.cluster import Cluster

parser = argparse.ArgumentParser(description="Auto generate various CDH configurations based on system resources")
parser.add_argument("--formula", type=str, help="Auto generation formula file.", required=True)
#parser.add_argument("--atk-conf-path", type=str, help="Directory for the atk.conf file. Defaults to working directory")
args = atk.cli.parse(parser)


def main():

    if args.formula:
        #get the cluster reference
        cluster = Cluster(args.host, args.port, args.username, args.password, args.cluster)

        if cluster:
            #execute formula global variables
            vars = {"cluster": cluster, "cdh": {}, "atk": {}}
            try:
                execfile(args.formula, vars)
            except IOError as e:
                log.fatal("formula file: {0} doesn't exist".format(args.formula))

            if len(vars["cdh"]) > 0:
                temp = {}
                path = file.file_path(atk.CDH_CONFIG, args.path)
                for key in vars["cdh"]:
                    key_split = key.split(".")
                    atk.dict.nest(temp, key_split, vars["cdh"][key])
                file.write_json_conf(temp, path)
                log.info("Wrote CDH config file to: {0}".format(path))
            else:
                log.warning("No CDH configurations to save.")

            if len(vars["atk"]) > 0:
                path = file.file_path(atk.ATK_CONFIG, args.path)
                f = open(path, "w+")
                for key in vars["atk"]:

                    print("{0}={1}".format(key, vars["atk"][key]), file=f)

                f.close()
                log.info("Wrote ATK generated config file to: {0}".format(path))
            else:
                log.warning("No ATK configurations to save")

        else:
            atk.log.fatal("Couldn't connect to the CDH cluster")
    else:
            atk.log.fatal("formula path must be specified")

