from cm_api.api_client import ApiResource
from cm_api.endpoints import hosts
from cm_api.endpoints import role_config_groups
from subprocess import call
from os import system
import hashlib, re, time, argparse, os, io, time, sys, getpass
import codecs
import json
from pprint import pprint
import atk_config as atk


parser = argparse.ArgumentParser(description="Auto generate various CDH configurations based on system resources")
parser.add_argument("--formula", type=str, help="Auto generation formula file.")
atk.cli.add_cdh_command_line_options(parser)
args = parser.parse_args()

#get the cluster reference
cluster = atk.cdh.Cluster(args.host, args.port, args.username, args.password, args.cluster)

if cluster:
    #open file read line by line
    vars = {"cluster": cluster, "cdh": {}, "atk": {}}
    execfile("default.py", vars)
    pprint(vars["cdh"])
    pprint(vars["atk"])
#    print configs.configs

else:
    print("Couldn't connect to the CDH cluster")