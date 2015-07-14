import re
from cluster_config.cdh.config import Config
import cluster_config.log as log
from pprint import pprint


class Config_Group(object):
    def __init__(self, cdh_service, cdh_group):
        #cdh service object
        self._cdh_service = None
        #cdh config_group object
        self._cdh_group = None
        #cdh configs items
        self._cdh_configs = {}

        self.cdh_service = cdh_service
        self.cdh_group = cdh_group

        self.__get_configs()

    def __get_configs(self):
        for name, config in self.cdh_group.get_config(view='full').items():
            temp = Config(self.cdh_group, config)
            setattr(self, temp.name, temp)
            self.configs[temp.key] = temp


    def __update(self):
        self.__get_configs()

    def set(self, configs):
        #re key our configs to use actual CDH config key
        temp = {}
        for config in configs:
            if config in self.configs:
                temp[self.configs[config].cdh_config.name] = configs[config]
            else:
                log.warning("No configuration key found for: {0}".format(config))
        log.info("Updating config group: {0}".format(self.key))
        self.cdh_group.update_config(temp)
        self.__update()
        return temp

    @property
    def key(self):
        return self.name.upper()

    @property
    def name(self):
        find = re.compile('[A-Z]*-[A-Z]*$')
        found = find.search(self.cdh_group.name)
        if found is None:
            return self.cdh_group.name.replace(self.cdh_service.name + "-", "").lower().replace("-", "_")
        else:
            return found.group().lower().replace("-", "_")

    @property
    def cdh_service(self):
        return self._cdh_service

    @cdh_service.setter
    def cdh_service(self, cdh_service):
        self._cdh_service = cdh_service

    @property
    def cdh_group(self):
        return self._cdh_group

    @cdh_group.setter
    def cdh_group(self, cdh_group):
        self._cdh_group = cdh_group

    @property
    def configs(self):
        return self._cdh_configs

    @configs.setter
    def configs(self, cdh_config):
        self._cdh_configs = cdh_config

