import re

import cluster_config.utils.log as log
from cluster_config.cdh.config import Config
from cluster_config.cdh import CDH

class Config_Group(CDH):
    def __init__(self, cdh, cdh_group):
        super(Config_Group, self).set_resources(cdh)

        self._cdh_configs = {}
        '''cdh configurations '''

        self.cdh_group = cdh_group
        '''cm-api config group'''

        self.__get_configs()

    def __get_configs(self):
        '''
        get all the config for this group from cm-api
        '''
        for name, config in self.cdh_group.get_config(view='full').items():
            temp = Config(self.cdh_group, config)
            setattr(self, temp.name, temp)
            self.configs[temp.key] = temp


    def __update(self):
        '''
        update the configurations for this config group
        :return:
        '''
        self.__get_configs()

    def set(self, configs):
        '''
        re key our configs to use actual CDH config key
        :param configs: dictionary of configs that were parsed from json.
        :return:
        '''
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
            return self.cdh_group.name.replace(self.cmapi_service.name + "-", "").lower().replace("-", "_")
        else:
            return found.group().lower().replace("-", "_")

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

