from cm_api.api_client import ApiResource
from cm_api.endpoints import hosts, roles, role_config_groups
from urllib2 import URLError
import time
import sys
import re
from pprint import pprint


class Config(object):
    def __init__(self, cdh_group, cdh_config):
        self._cdh_group = None
        self._cdh_config = None

        self.cdh_group = cdh_group
        self.cdh_config = cdh_config

    def get(self):
        if self.cdh_config.value:
            return self.cdh_config.value
        else:
            return self.cdh_config.default

    def set(self, value):
        self.cdh_config.value = self.cdh_group.update_config({self.cdh_config.name: value})[self.cdh_config.name]

    @property
    def key(self):
        name = self.cdh_config.name.upper()
        for r in ["-", "/"]:
            name = name.replace(r, "_")
        return name

    @property
    def name(self):
        return self.key.lower()

    @property
    def cdh_group(self):
        return self._cdh_group

    @cdh_group.setter
    def cdh_group(self, cdh_group):
        self._cdh_group = cdh_group

    @property
    def cdh_config(self):
        return self._cdh_config

    @cdh_config.setter
    def cdh_config(self, cdh_config):
        self._cdh_config = cdh_config

    @property
    def value(self):
        if self.cdh_config.value:
            return self.cdh_config.value
        else:
            return self.cdh_config.default

    @value.setter
    def value(self, value):
        self.cdh_config.value = self.cdh_group.update_config({self.cdg_config.name: value})[self.cdh_config.name]