import os
import io
import json
import yaml
import cluster_config as cc
from cluster_config import log
import cluster_config.cdh as cdh
import time, datetime
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
    log.debug("open_json_conf: {0}".format(path))
    conf = None
    try:
        f = io.open(path, encoding="utf-8", mode="r")
        conf = json.load(f)
    except IOError:
        log.warning("Couldn't open file: {0}".format(path))

    return conf


def snapshots(host, action, path, gen_dt=None, *args):
    log.info("Creating file snapshots")
    if gen_dt is None:
        gen_dt = datetime.datetime.now()

    prefix = "{0}-{1}".format(host, gen_dt.strftime("%Y_%m_%d_%H_%M_%S"))
    snapshot_folder = file_path(prefix, path)
    log.info("Creating snapshot folder:{0}".format(snapshot_folder))
    check_dir_exists(snapshot_folder)
    for arg in args:
        if arg:
            log.info("Snapshotting: {0} ".format(arg))
            try:
                shutil.copy(arg, "{0}/{1}-{2}".format(snapshot_folder, action, os.path.basename(arg)))
            except IOError:
                log.warning("Couldn't create snapshot for: {0} ".format(arg))


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


def open_file(path):
    file_string = None
    log.debug("opening file: {0}". format(path))
    try:
        file_pointer = io.open(path, encoding="utf-8", mode="r")
        file_string = file_pointer.read()
        file_pointer.close()
    except IOError:
        log.warning("Couldn't open yaml file: {0}".format(path))
    return file_string
