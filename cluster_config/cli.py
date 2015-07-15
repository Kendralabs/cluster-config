import getpass
import argparse

"""
add the cloudera connection arguments to the cli parser
"""
def add_cluster_connection_options(argparser):
    argparser.add_argument("--host", type=str, help="Cloudera Manager Host", default="127.0.0.1")
    argparser.add_argument("--port", type=int, help="Cloudera Manager Port", default=7180)
    argparser.add_argument("--username", type=str, help="Cloudera Manager User Name", default="admin")
    argparser.add_argument("--password", type=str, help="Cloudera Manager Password", default="admin")

    argparser.add_argument("--cluster", type=str, help="Cloudera Manager Cluster Name if more than one cluster is managed by "
                                                "Cloudera Manager.", default="cluster")
    argparser.add_argument("--log", type=str, help="Cloudera Manager Password", default="admin")

    return argparser

def parse(argparse):
    add_cluster_connection_options(argparse)

    return argparse.parse_args()

def get_cluster_password():
    return getpass.getpass(prompt="What is the password to the cluster?")