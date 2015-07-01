
from atk_config import base
from atk_config import cli
from atk_config import cdh
from atk_config import atk
from atk_config import auto


def get_auto_config():
    return base.open_json_conf("{0}/{1}.{2}".format(base.CONFIG_DIR,base.CDH_CONFIG_NAME, base.JSON_EXT))

def get_user_config():
    return base.open_json_conf("{0}/{1}.{2}".format(base.CONFIG_DIR,base.CDH_USER_CONFIG_NAME, base.JSON_EXT))
