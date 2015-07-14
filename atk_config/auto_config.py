from __future__ import print_function

import argparse

from pprint import pprint
import atk_config as atk

parser = argparse.ArgumentParser(description="Auto generate various CDH configurations based on system resources")
parser.add_argument("--formula", type=str, help="Auto generation formula file.")
atk.cli.add_cdh_command_line_options(parser)
args = parser.parse_args()


def main():


    if args.formula:
        #get the cluster reference
        cluster = atk.cdh.Cluster(args.host, args.port, args.username, args.password, args.cluster)

        if cluster:
            #execute formula
            vars = {"cluster": cluster, "cdh": {}, "atk": {}}
            try:
                execfile(args.formula, vars)
            except IOError as e:
                atk.log.fatal("formula file: {0} doesn't exist".format(args.formula))

            if len(vars["cdh"]) > 0:
                temp = {}
                for key in vars["cdh"]:
                    key_split = key.split(".")
                    atk.dict.nest(temp, key_split, vars["cdh"][key])
                atk.file.write_cdh_conf(temp)

            else:
                atk.log.warning("No CDH configurations to save.")

            if len(vars["atk"]) > 0:
                f = open(atk.TAPROOT_CONFIG_FILE, "w+")
                for key in vars["atk"]:

                    print("{0}={1}".format(key, vars["atk"][key]), file=f)

                f.close()
            else:
                atk.log.warning("No ATK configurations to save")

        else:
            atk.log.fatal("Couldn't connect to the CDH cluster")
    else:
            atk.log.fatal("formula path must be specified")