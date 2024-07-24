import operator
import os
from functools import reduce

import yaml

# import json
# from yaml.loader import SafeLoader


class Config:
    def __init__(self):
        config_file = os.getenv("CONFIG_PATH", "config")
        with open(os.path.join(config_file, "service_config.yml")) as config_file:
            self.config = yaml.safe_load(config_file)

    def find(self, path):
        element_value = reduce(operator.getitem, path.split("."), self.config)
        # if (isinstance(element_value, list)):
        #     element_value = json.dumps(element_value)
        # print("element_value = {0}".format(element_value))
        return element_value
