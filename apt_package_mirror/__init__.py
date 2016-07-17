from __future__ import print_function
import yaml
import os
import sys

def main():
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

    try:
        temp_indices = config['temp_files_path']
    except:
        temp_indices = None

    if temp_indices:
        mirror = Mirror(mirror_path=mirror_path,
                        mirror_url=config['mirror_url'],
                        temp_indices=temp_indices)
    else:
        mirror = Mirror(mirror_path=mirror_path,
                        mirror_url=config['mirror_url'])

    mirror.sync()

if __name__ == '__main__':
    main()
