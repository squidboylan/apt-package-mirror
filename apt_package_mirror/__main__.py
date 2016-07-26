from __future__ import print_function
import yaml
import os
from apt_package_mirror.mirror import Mirror
import sys
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-U', '--update-packages-only', dest='update_packages_only', action='store_true',
                        default=False, help='Grab new packages only')

    parser.add_argument('config_file',  default='config.yaml', nargs='?',
                        help='yaml config file that describes what mirror to copy and where to store the data')

    args = parser.parse_args()

    try:
        with open(args.config_file, "r") as file_stream:
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

    try:
        log_file = config['log_file']
        f = open(log_file, 'a')
        f.close()
    except:
        log_file = None

    mirror = Mirror(mirror_path=mirror_path,
                    mirror_url=config['mirror_url'],
                    temp_indices=temp_indices,
                    log_file=log_file)

    if args.update_packages_only:
        mirror.update_pool()

    else:
        mirror.sync()

if __name__ == '__main__':
    main()
