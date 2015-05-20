from cm_api.api_client import ApiResource
from cm_api.endpoints import hosts
from cm_api.endpoints import role_config_groups
from subprocess import call
from os import system
import hashlib, re, time, argparse, os, time, sys, getpass
import codecs

parser = argparse.ArgumentParser(description="Process cl arguments to avoid prompts in automation")
parser.add_argument("--host", type=str, help="Cloudera Manager Host")
parser.add_argument("--port", type=int, help="Cloudera Manager Port")
parser.add_argument("--username", type=str, help="Cloudera Manager User Name")
parser.add_argument("--password", type=str, help="Cloudera Manager Password")
parser.add_argument("--cluster", type=str, help="Cloudera Manager Cluster Name if more than one cluster is managed by "
                                                "Cloudera Manager.")