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

mirror_path = config['mirror_path']

if not os.path.exists(mirror_path):
    print("Mirror path does not exist, please fix it")
    sys.exit(1)

if not os.path.exists(os.path.join(mirror_path, 'ubuntu')):
    os.makedirs(os.path.join(mirror_path, 'ubuntu'))

mirror_path = os.path.join(mirror_path, 'ubuntu')

mirror = Mirror(mirror_path=mirror_path,
                mirror_url=config['mirror_url'])

mirror.sync()
