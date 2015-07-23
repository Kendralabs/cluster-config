#! /usr/bin/env python

if __name__ == "__main__":
    from cluster_config.cdh.cluster import Cluster
    from cluster_config import push
    from cluster_config.cli import parse
    args = parse(push.cli())
    cluster = Cluster(args.host, args.port, args.username, args.password, args.cluster)
    push.main(args, cluster)

