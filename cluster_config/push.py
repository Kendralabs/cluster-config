from __future__ import print_function
import os
import argparse
import cluster_config as cc
from cluster_config import log
from cluster_config import file
from cluster_config.cdh.cluster import Cluster
from cluster_config import auto_config, config



def cli(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser(description="Auto generate various CDH configurations based on system resources")

    parser = auto_config.cli(parser)
    parser = config.cli(parser)

    return parser


def main(args, cluster=None):
    if cluster is None:
        cluster = Cluster(args.host, args.port, args.username, args.password, args.cluster)

    auto_config.main(args, cluster)
    config.main(args, cluster)
