import os
import io
import json
import yaml
import cluster_config as cc
from cluster_config import log
import cluster_config.cdh as cdh
import time
import shutil


def file_path(file_name, path):
    """
    get the entire file path for file_name
    :param file_name: The base file name, my_file.ext| myfile
    :param path: some default path
    :return: full path with file name, path/file_name or working/directory/file_name
    """
    return path.rstrip('\/') + "/{0}".format(file_name) if path else os.getcwd() + "/{0}".format(file_name)


def open_json_conf(path):
    """
    Open json files
    :param path: the full file path including file name
    :return: dictionary of the json file
    """
    conf = None
    log.debug("open_json_conf: {0}".format(path))
    try:
        config_json_open = io.open(path, encoding="utf-8", mode="r")
        conf = json.loads(config_json_open.read())
        config_json_open.close()
    except IOError:
        log.warning("Couldn't open json file: {0}".format(path))
    return conf


def snapshots(cluster, host, action, path, *args):
    log.info("Creating file snapshots")
    prefix = "{0}-{1}".format(host, time.strftime("%Y-%m-%d %H:%M:%S"))
    snapshot_folder = file_path(prefix, path)
    log.info("Creating snapshot folder:{0}".format(snapshot_folder))
    check_dir_exists(snapshot_folder)
    for arg in args:
        if arg:
            log.info("Snapshotting: {0} ".format(arg))
            shutil.copy(arg, "{0}/{1}-{2}-{3}".format(snapshot_folder, prefix, action, os.path.basename(arg)))

    cdh_json_path = file_path(cc.ALL_CLUSTER_CONFIGS, path)
    write_json_conf(cdh.json(cluster), cdh_json_path)
    shutil.copy(cdh_json_path, "{0}/{1}-{2}-{3}".format(snapshot_folder, prefix, action, os.path.basename(cdh_json_path)))


def check_dir_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)


def write_json_conf(json_dict, path):
    """
    save a dictionary as a json file
    :param json_dict: some dictionary
    :param path: full file path including file name
    """
    try:
        config_json_open = io.open(path, encoding="utf-8", mode="w")
        config_json_open.write(unicode(json.dumps(json_dict, indent=True, sort_keys=True)))
        config_json_open.close()
    except IOError:
        log.fatal("couldn't write {0}".format(path))


def open_yaml_conf(path):
    args = None
    log.debug("open yaml file: {0}". format(path))
    try:
        config_yaml_open = io.open(path, encoding="utf-8", mode="r")
        args = yaml.load(config_yaml_open)
        config_yaml_open.close()
    except IOError:
        log.warning("Couldn't open yaml file: {0}".format(path))
    return args

