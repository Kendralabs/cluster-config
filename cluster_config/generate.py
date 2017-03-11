from __future__ import print_function

import argparse
import inspect
import os

import cluster_config as cc
from cluster_config.utils import convert
from cluster_config.cdh.cluster import Cluster, save_to_json
from cluster_config.utils import file, log


def cli(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser(description="Auto generate various CDH configurations based on system resources")

    default_formula = file.file_path(cc.DEFAULT_FORMULA, os.path.split(__file__)[0])

    parser.add_argument("--formula", type=str,
                        help="Auto generation formula file. Defaults to {0}".format(default_formula),
                        default=default_formula)

    parser.add_argument("--formula-args", type=str,
                        help="formula arguments to possibly override constants in {0}.".
                        format(default_formula))

    return parser


def main():
    run(cc.utils.cli.parse(cli()))


def run(args, cluster=None, dt=None):
    if cluster is None:
        cluster = Cluster(args.host, args.port, args.username, args.password, args.cluster)

    if args.formula:
        cluster_config_json_path = save_to_json(cluster, args.path, "before")

        vars = exec_formula(cluster, args)

        cdh_config_path = save_cdh_configuration(vars, args)

        file.snapshots(args.host, "generate", args.path, dt, cdh_config_path, cluster_config_json_path,
                       args.formula, args.formula_args)
    else:
        cc.utils.log.fatal("Formula file must be specified")


def exec_formula(cluster, args):
    log.info("using formula: {0}".format(args.formula))
    user_formula_args = None
    if args.formula_args:
        user_formula_args_path = file.file_path(cc.FORMULA_ARGS, args.path)
        log.info("Reading formula args config file: {0}".format(user_formula_args_path))
        user_formula_args = file.open_yaml_conf(user_formula_args_path)

    #execute formula global variables
    vars = { "log": log, "kb_to_bytes": convert.kb_to_bytes, "bytes_to_kb": convert.bytes_to_kb, "mb_to_bytes": convert.mb_to_bytes,
             "bytes_to_mb": convert.bytes_to_mb,"gb_to_bytes": convert.gb_to_bytes, "bytes_to_gb": convert.bytes_to_gb, "tr_to_bytes": convert.tr_to_bytes,
             "bytes_to_tr": convert.bytes_to_tr}
    local = {}
    config = {}
    try:
        execfile(args.formula, vars, local)
        constants = local["constants"](cluster, log)
        for member in constants:
            if hasattr(constants[member], '__call__'):
                if user_formula_args and member in user_formula_args:
                    if constants[member](user_formula_args[member]) != user_formula_args[member]:
                        log.warning("Formula arg value '{0}' was ignored using default '{1}'. Formula arg value must adhere to these rules {2}  ".format(user_formula_args[member], constants[member](user_formula_args[member]), inspect.getsource(constants[member])))

                    constants[member] = constants[member](user_formula_args[member])
                else:
                    constants[member] = constants[member](None)



        config = local["formula"](cluster, log, constants)

    except IOError:
        log.fatal("formula file: {0} doesn't exist".format(args.formula))

    return config


def save_cdh_configuration(vars, args):
    #TODO: think of a better name

    if len(vars["cdh"]) > 0:
        temp = {}
        path = file.file_path(cc.CDH_CONFIG, args.path)
        for key in vars["cdh"]:
            key_split = key.split(".")
            cc.utils.dict.nest(temp, key_split, vars["cdh"][key])
        file.write_json_conf(temp, path)
        log.info("Wrote CDH configuration file to: {0}".format(path))
        return path
    else:
        log.warning("No CDH configurations to save.")
        return None


