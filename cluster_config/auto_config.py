from __future__ import print_function
import os
import argparse
import cluster_config as cc
from cluster_config import log
from cluster_config import file
from cluster_config.cdh.cluster import Cluster
import sys


def cli(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser(description="Auto generate various CDH configurations based on system resources")

    default_formula = file.file_path(cc.DEFAULT_FORMULA,os.path.split(__file__)[0])

    parser.add_argument("--formula", type=str,
                        help="Auto generation formula file. Defaults to {0}".format(default_formula),
                        default=default_formula)

    parser.add_argument("--formula-args", type=str,
                        help="formula arguments to possibly override constants.".
                        format(default_formula),
                        default=default_formula)

    return parser


def main():
    run(cc.cli.parse(cli()))


def run(args, cluster=None):
    if cluster is None:
        cluster = Cluster(args.host, args.port, args.username, args.password, args.cluster)

    if args.formula:
        #execute formula global variables
        vars = exec_formula(cluster, args)

        save_cdh_configuration(vars, args)

        save_atk_configuration(vars, args)

    else:
        cc.log.fatal("Formula file must be specified")


def exec_formula(cluster, args):
    log.info("using formula: {0}".format(args.formula))

    user_formula_args_path = file.file_path(cc.FORMULA_ARGS, args.path)
    log.info("Reading CDH config file: {0}".format(user_formula_args_path))
    user_formula_args = file.open_yaml_conf(user_formula_args_path)

    #execute formula global variables
    vars = {"cluster": cluster, "cdh": {}, "atk": {}, "log": log, "args": user_formula_args}
    try:
        execfile(args.formula, vars)
    except IOError:
        log.fatal("formula file: {0} doesn't exist".format(args.formula))

    return vars


def save_cdh_configuration(vars, args):
    if len(vars["cdh"]) > 0:
        temp = {}
        path = file.file_path(cc.CDH_CONFIG, args.path)
        for key in vars["cdh"]:
            key_split = key.split(".")
            cc.dict.nest(temp, key_split, vars["cdh"][key])
        file.write_json_conf(temp, path)
        log.info("Wrote CDH configuration file to: {0}".format(path))
    else:
        log.warning("No CDH configurations to save.")


def save_atk_configuration(vars, args):
    if len(vars["atk"]) > 0:
        path = file.file_path(cc.ATK_CONFIG, args.path)
        f = open(path, "w+")
        for key in vars["atk"]:

            print("{0}={1}".format(key, vars["atk"][key]), file=f)

        f.close()
        log.info("Wrote ATK generated configuration file to: {0}".format(path))
    else:
        log.warning("No ATK configurations to save")