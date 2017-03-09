import getpass
import logging
import os

import cluster_config.utils.log as log
from cluster_config.utils.log import logger


def add_cluster_connection_options(arg_parser):
    '''
    Add common CLI options to the various executable scripts
    :param arg_parser: argparse.ArgumentParser object
    :return: arg_parser with the added options
    '''

    arg_parser.add_argument("--host", type=str, help="Cloudera Manager Host", required=True)
    arg_parser.add_argument("--port", type=int, help="Cloudera Manager Port", default=7180)
    arg_parser.add_argument("--username", type=str, help="Cloudera Manager User Name", default="admin")
    arg_parser.add_argument("--password", type=str, help="Cloudera Manager Password")

    arg_parser.add_argument("--cluster", type=str, help="Cloudera Manager Cluster Name if more than one cluster is "
                                                        "managed by Cloudera Manager.", default="cluster")
    arg_parser.add_argument("--path", type=str,
                            help="Directory where we can save/load configurations files. "
                                 "Defaults to working directory {0}".format(os.getcwd()))
    arg_parser.add_argument("--log", type=str, help="Log level [INFO|DEBUG|WARNING|FATAL|ERROR]", default="INFO",
                            choices=["INFO", "DEBUG", "WARNING", "FATAL", "ERROR"])

    return arg_parser


def parse(argparse):
    '''
    Parse CLI options in a single place so we can check for CDH password if it's not included.

    :param argparse: argparse.ArgumentParser object
    :return:  parsed arguments
    '''
    add_cluster_connection_options(argparse)

    args = argparse.parse_args()

    set_logging(args.log)

    if not args.password:
        args.password = get_cluster_password()

    return args


def set_logging(log_lvl):
    '''
    Set log level
    :param log_lvl: log lvl to set
    '''
    if log_lvl:
        numeric_level = getattr(logging, log_lvl.upper(), None)
        if not isinstance(numeric_level, int):
            log.error("Invalid log level: {0}".format(log_lvl))
        else:
            logger.setLevel(numeric_level)


def get_cluster_password():
    '''
    Prompt the user for password
    :return: the user's cdh password
    '''
    return getpass.getpass(prompt="What is the Cloudera manager password? ")


