import getpass
import argparse
import logging

"""
add the cloudera connection arguments to the cli parser
"""
LOG=logging.INFO

def add_cluster_connection_options(argparser):
    argparser.add_argument("--host", type=str, help="Cloudera Manager Host", required=True)
    argparser.add_argument("--port", type=int, help="Cloudera Manager Port", default=7180)
    argparser.add_argument("--username", type=str, help="Cloudera Manager User Name", default="admin")
    argparser.add_argument("--password", type=str, help="Cloudera Manager Password")

    argparser.add_argument("--cluster", type=str, help="Cloudera Manager Cluster Name if more than one cluster is managed by "
                                                "Cloudera Manager.", default="cluster")
    argparser.add_argument("--log", type=str, help="log level", default="admin")

    return argparser

def parse(argparse):
    add_cluster_connection_options(argparse)

    args = argparse.parse_args()
    print "log", getattr(logging, "INFO")

    set_logging(args.log)
    args.password = get_cluster_password()
    return args

def set_logging(log_lvl):
    numeric_level = getattr(logging, log_lvl.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % log_lvl)
    LOG = numeric_level

def get_cluster_password():
    return getpass.getpass(prompt="What is the password to the cluster? ")