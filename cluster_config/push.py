from __future__ import print_function
import argparse
from cluster_config.cdh.cluster import Cluster
from cluster_config import auto_config, config


def cli(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser(description="Auto generate various CDH configurations based on system resources")

    parser = auto_config.cli(parser)
    parser = config.cli(parser)

    return parser


def main():
    from cluster_config.cdh.cluster import Cluster
    from cluster_config import push
    from cluster_config.cli import parse
    args = parse(push.cli())
    cluster = Cluster(args.host, args.port, args.username, args.password, args.cluster)
    push.run(args, cluster)


def run(args, cluster=None):
    if cluster is None:
        cluster = Cluster(args.host, args.port, args.username, args.password, args.cluster)

    auto_config.run(args, cluster)
    config.run(args, cluster)
