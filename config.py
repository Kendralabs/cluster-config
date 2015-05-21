from cm_api.api_client import ApiResource
from cm_api.endpoints import hosts
from cm_api.endpoints import role_config_groups
from subprocess import call
from os import system
import hashlib, re, time, argparse, os, io, time, sys, getpass
import codecs
import json
from pprint import pprint
from atk_config import cdh

parser = argparse.ArgumentParser(description="Process cl arguments to avoid prompts in automation")
parser.add_argument("--host", type=str, help="Cloudera Manager Host", default="127.0.0.1")
parser.add_argument("--port", type=int, help="Cloudera Manager Port", default=7180)
parser.add_argument("--username", type=str, help="Cloudera Manager User Name", default="admin")
parser.add_argument("--password", type=str, help="Cloudera Manager Password", default="admin")
parser.add_argument("--cluster", type=str, help="Cloudera Manager Cluster Name if more than one cluster is managed by "
                                                "Cloudera Manager.", default="cluster")
parser.add_argument("--restart", type=str, help="Weather or not to restart CDH services after config changes", default="no")

args = parser.parse_args()

cluster = cdh.Cluster(args.host, args.port, args.username, args.password, args.cluster)

if cluster:
    configJson = None
    try:
        configJsonOpen = codecs.open("config.json", encoding="utf-8", mode="r")
        configJson = configJsonOpen.read()
        configJsonOpen.close()
    except IOError:
        print("Couldn't find config.json file")

    configJson = json.loads(configJson)
    pprint(configJson)

    #iterate through services, set cdh configs and possibly restart services
    for service in configJson["cdh"]:
        #iterate through roles
        for role in configJson["cdh"][service]:
            #iterate through config groups
            for configGroup in configJson["cdh"][service][role]:
                cluster.set(service, role, configGroup, configJson["cdh"][service][role][configGroup])
        if args.restart == "yes":
            cluster.restart(service)

    #set atk configs
    with io.open("application.json",'w', encoding='utf-8') as f:
        f.write(unicode(json.dumps(configJson["atk"])))

else:
    print("Couldn't connect to the CDH cluster")