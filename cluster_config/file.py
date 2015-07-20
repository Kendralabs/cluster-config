import os
import io
import json
from cluster_config import log


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

