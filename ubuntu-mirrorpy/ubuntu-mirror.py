from __future__ import print_function
import yaml
import os
import sys
from mirror import Mirror


try:
    config_file = sys.argv[1]

except:
    config_file = "config.yaml"

try:
    with open(config_file, "r") as file_stream:
        config = yaml.load(file_stream)

except:
    print("failed to load the config file")
    sys.exit(1)

mirror = Mirror(mirror_path=config['mirror_path'],
                mirror_url=config['mirror_url'])
mirror.sync()
