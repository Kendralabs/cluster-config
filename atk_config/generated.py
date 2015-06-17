import base


def get_generated_config():
    return base.open_json_conf("{0}/{1}.{2}".format(base.CONFIG_DIR,base.CDH_CONFIG_NAME, base.JSON_EXT))

def get_user_config():
    return base.open_json_conf("{0}/{1}.{2}".format(base.CONFIG_DIR,base.CDH_USER_CONFIG_NAME, base.JSON_EXT))
