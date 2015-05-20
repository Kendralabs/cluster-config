from cm_api.api_client import ApiResource
from cm_api.endpoints import hosts
from cm_api.endpoints import role_config_groups
from subprocess import call
from os import system
import hashlib, re, time, argparse, os, time, sys, getpass
import codecs
import atk_config as ac

parser = argparse.ArgumentParser(description="Process cl arguments to avoid prompts in automation")
parser.add_argument("--host", type=str, help="Cloudera Manager Host", default="127.0.0.1")
parser.add_argument("--port", type=int, help="Cloudera Manager Port", default=7180)
parser.add_argument("--username", type=str, help="Cloudera Manager User Name", default="admin")
parser.add_argument("--password", type=str, help="Cloudera Manager Password", default="admin")
parser.add_argument("--cluster", type=str, help="Cloudera Manager Cluster Name if more than one cluster is managed by "
                                                "Cloudera Manager.", default="cluster")

args = parser.parse_args()

cluster = ac.cdh.Cluster("10.54.8.175", 7180, "admin", "wolverine", "cluster")

if cluster:
    configJson = None
    try:
        configJsonOpen = codecs.open("config.json", encoding="utf-8", mode="r")
        configJson = configJsonOpen.read()
        configJsonOpen.close()
    except IOError:
        print "cloudn open json file"

else:
    print("Couldn't connect to the CDH cluster")