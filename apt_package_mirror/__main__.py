from __future__ import print_function
import yaml
import os
from apt_package_mirror.mirror import Mirror
import sys
import argparse


def main():
    # When files are created make them with a 022 umask
    os.umask(022)

    # Add commandline options and help text for them
    parser = argparse.ArgumentParser()
    parser.add_argument('-U', '--update-packages-only',
                        dest='update_packages_only', action='store_true',
                        default=False, help='Grab new packages only')

    config_file_help = ('yaml config file that describes what mirror to copy '
                        'and where to store the data')
    parser.add_argument(
            'config_file',  default='config.yaml', nargs='?',
            help=config_file_help
        )

    args = parser.parse_args()

    # Check if the config file exists, if it doesnt fail with a message
    try:
        with open(args.config_file, "r") as file_stream:
            config = yaml.load(file_stream)

    except:
        print("failed to load the config file")
        sys.exit(1)

    # Check if the mirror path defined in the config file exists
    mirror_path = config['mirror_path']

    if not os.path.exists(mirror_path):
        print("Mirror path does not exist, please fix it")
        sys.exit(1)

    # Check if the directory for temp files is defined
    try:
        temp_indices = config['temp_files_path']
    except:
        temp_indices = None

    # Check if a log_level is defined
    try:
        log_level = config['log_level']
    except:
        log_level = None

    # Check if a package_ttl is defined
    try:
        package_ttl = config['package_ttl']
    except:
        package_ttl = None

    # Check if a hash_function is defined
    try:
        hash_function = config['hash_function']
    except:
        hash_function = None

    # Create a file for logging in the location defined by the config file
    try:
        log_file = config['log_file']
        f = open(log_file, 'a')
        f.close()
    except:
        log_file = None

    mirror = Mirror(mirror_path=mirror_path,
                    mirror_url=config['mirror_url'],
                    temp_indices=temp_indices,
                    log_file=log_file, log_level=log_level,
                    package_ttl=package_ttl, hash_function=hash_function)

    # If a -U option is used, only update the 'pool' directory. This only grabs
    # new packages
    if args.update_packages_only:
        mirror.update_pool()

    # If a -U option is not used, attempt to update the whole mirror
    else:
        mirror.sync()

if __name__ == '__main__':
    main()
