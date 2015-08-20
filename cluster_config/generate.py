from __future__ import print_function
import os
import sys
import argparse
import cluster_config as cc
from cluster_config import log
from cluster_config import file
from cluster_config.const import Const
from cluster_config.cdh.cluster import Cluster, save_config
import cluster_config.cdh as cdh
import time, datetime
from pprint import pprint


def cli(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser(description="Auto generate various CDH configurations based on system resources")

    default_formula = file.file_path(cc.DEFAULT_FORMULA,os.path.split(__file__)[0])

    parser.add_argument("--formula", type=str,
                        help="Auto generation formula file. Defaults to {0}".format(default_formula),
                        default=default_formula)

    parser.add_argument("--formula-args", type=str,
                        help="formula arguments to possibly override constants.".
                        format(default_formula))

    return parser


def main():
    run(cc.cli.parse(cli()))


def run(args, cluster=None):
    if cluster is None:
        cluster = Cluster(args.host, args.port, args.username, args.password, args.cluster)

    if args.formula:
        #dt = datetime.datetime.now()
        cluster_config_json_path = save_config(cluster, args.path, "before")
        #execute formula global variables
        #__import__("cluster_config.formula")
        #pprint(sys.modules)
        #sys.exit(1)
        #mymodule = sys.modules[module_name]

        vars = exec_formula(cluster, args)

        save_cdh_configuration(vars, args)

        save_atk_configuration(vars, args)

        file.snapshots(args.host, "generate", args.path, None, cluster_config_json_path, args.formula, args.formula_args)
    else:
        cc.log.fatal("Formula file must be specified")


def exec_formula(cluster, args):
    log.info("using formula: {0}".format(args.formula))

    user_formula_args_path = file.file_path(cc.FORMULA_ARGS, args.path)
    log.info("Reading CDH config file: {0}".format(user_formula_args_path))
    user_formula_args = file.open_yaml_conf(user_formula_args_path)

    #execute formula global variables
    vars = { "log": log, "KB_to_bytes": KB_to_bytes, "bytes_to_KB": bytes_to_KB, "MB_to_bytes": MB_to_bytes,
             "bytes_to_MB": bytes_to_MB,"GB_to_bytes": GB_to_bytes, "bytes_to_GB": bytes_to_GB, "TR_to_bytes": TR_to_bytes,
             "bytes_to_TR": bytes_to_TR }
    local = {}
    config = {}
    try:
        execfile(args.formula, vars, local)
        constants = local["constants"](cluster, log)
        for member in constants:
            if hasattr(constants[member], '__call__'):
                if member in user_formula_args:
                    constants[member] = constants[member](user_formula_args[member])
                else:
                    constants[member] = constants[member](None)

        config = local["formula"](cluster, log, constants)

    except IOError:
        log.fatal("formula file: {0} doesn't exist".format(args.formula))

    return config


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

def bytes_to_KB(bytes):
    return bytes_to_x(bytes, "KB")

def KB_to_bytes(K):
    return x_to_bytes(K, "KB")

def bytes_to_MB(bytes):
    return bytes_to_x(bytes, "MB")


def MB_to_bytes(M):
    return x_to_bytes(M, "MB")


def bytes_to_GB(bytes):
    return bytes_to_x(bytes, "GB")


def GB_to_bytes(G):
    return x_to_bytes(G, "GB")


def bytes_to_TR(bytes):
    return bytes_to_x(bytes, "TB")


def TR_to_bytes(T):
    return x_to_bytes(T, "TB")

lookup = {
    "KB": pow(2,10),
    "MB": pow(2,20),
    "GB": pow(2,30),
    "TB": pow(2,40)
}

def bytes_to_x(bytes, size):
    return bytes / lookup[size]

def x_to_bytes(x, size):
    return x * lookup[size]