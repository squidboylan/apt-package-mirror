from __future__ import print_function
import bz2
from ftplib import FTP
import gzip
import hashlib
import logging
import os
import pickle
import re
from subprocess import Popen, STDOUT, PIPE
import sys
import time
import urllib
import yaml

class MirrorException(Exception):
    def __init__(self, val):
        self.val = val

    def __str__(self):
        return repr(self.val)

class Mirror:

    # Setup class vars and logger
    def __init__(self, mirror_path, mirror_url,
                 temp_indices=None, log_file=None, log_level=None,
                 package_ttl=None, hash_function=None):

        if not temp_indices:
            self.temp_indices = '/tmp/dists-indices'

        if log_level is None:
            log_level = 'INFO'

        if package_ttl is None:
            package_ttl = 10800

        if hash_function is None:
            self.hash_function = "SHA256"
        else:
            self.hash_function = hash_function.upper()

        self.package_ttl = package_ttl
        self.mirror_path = mirror_path
        self.mirror_url = mirror_url
        self.temp_indices = temp_indices
        self.indexed_packages = set()

        self.logger = logging.getLogger()
        if log_level.upper() == 'DEBUG':
            self.logger.setLevel(logging.DEBUG)

        elif log_level.upper() == 'INFO':
            self.logger.setLevel(logging.INFO)

        elif log_level.upper() == 'WARNING':
            self.logger.setLevel(logging.WARNING)

        elif log_level.upper() == 'ERROR':
            self.logger.setLevel(logging.ERROR)

        elif log_level.upper() == 'CRITICAL':
            self.logger.setLevel(logging.CRITICAL)

        else:
            print("Bad log level entered, defaulting to 'INFO'")
            self.logger.setLevel(logging.INFO)

        log_format = "%(asctime)s [%(levelname)-5.5s]  %(message)s"
        logFormatter = logging.Formatter(log_format)

        console = logging.StreamHandler()
        console.setFormatter(logFormatter)

        fileHandler = logging.FileHandler(filename=log_file)
        fileHandler.setFormatter(logFormatter)

        self.logger.addHandler(fileHandler)
        self.logger.addHandler(console)

    # Sync the whole mirror
    def sync(self):
        self.lock_file = os.path.join(self.temp_indices, 'sync_in_progress')
        if os.path.exists(self.lock_file):
            self.logger.info("Sync already in progress")
            sys.exit(1)

        f = open(self.lock_file, 'w')
        f.close()
        try:
            self.logger.info("=======================================")
            self.logger.info("= Starting Sync of Mirror             =")
            self.logger.info("=======================================")
            self.update_pool()
            self.get_dists_indices()
            self.get_zzz_dists()
            self.check_release_files()
            self.check_indices()
            self.update_mirrors()
            self.update_indices()
            self.clean()
            self.update_project_dir()
            self.gen_lslR()
            os.remove(self.lock_file)
        except:
            self.logger.info("Exception caught, removing lock file")
            os.remove(self.lock_file)
            raise

    # Update the pool directory of the mirror
    # NOTE: This does not delete old packages, so it is safe to run at any time
    def update_pool(self):
        rsync_command = "rsync --recursive --times --links --hard-links \
                --contimeout=10 --timeout=10 --no-motd --stats \
                --progress \
                -vz rsync://{mirror_url}/pool {mirror_path}/"
        rsync_command = rsync_command.format(
                mirror_url=self.mirror_url,
                mirror_path=self.mirror_path
            )

        self.logger.info("Downloading new packages")
        rsync_status = Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                             shell=True)

        for line in rsync_status.stdout:
            self.logger.debug(line)

    # Update the entire mirror, excluding package, source, and release indices
    def update_mirrors(self):
        rsync_command = "rsync --recursive --times --links --hard-links \
                --exclude 'Packages*' --exclude 'Sources*' \
                --exclude 'Release*' --exclude 'ls-lR.gz' --exclude 'pool' \
                --contimeout=10 --timeout=10 --no-motd --delete --stats \
                --delay-updates --progress \
                -vz rsync://{mirror_url}/ {mirror_path}/"
        rsync_command = rsync_command.format(
                mirror_url=self.mirror_url,
                mirror_path=self.mirror_path
            )

        self.logger.info("Downloading all new files except indices")
        rsync_status = Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                             shell=True)

        for line in rsync_status.stdout:
            self.logger.debug(line)

    # Download the 'dists' directory and place it in a
    # temporary place so it can be checked to make sure it is accurate
    def get_dists_indices(self):
        rsync_command = "rsync --recursive --times --links --hard-links \
                --exclude 'installer*' --delete --no-motd --stats\
                --progress \
                -vz rsync://{mirror_url}/dists {temp_indices}/"
        rsync_command = rsync_command.format(
                mirror_url=self.mirror_url,
                temp_indices=self.temp_indices
            )

        self.logger.info(
                ("Downloading dist indices and storing them "
                 "in a temporary place")
            )
        rsync_status = Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                             shell=True)

        for line in rsync_status.stdout:
            self.logger.debug(line)

    # Download the 'zzz-dists' directory and place it in a
    # temporary place so it can be checked to make sure it is accurate
    # NOTE: This is for Debian compatibility, this should do nothing in an
    #       ubuntu mirror because they do not have the 'zzz-dists' dir, but
    #       debian symlinks some things in the 'dists' dir to 'zzz-dists'
    def get_zzz_dists(self):
        rsync_command = "rsync --recursive --times --links --hard-links \
                --exclude 'installer*' --delete --no-motd --stats\
                --progress \
                -vz rsync://{mirror_url}/zzz-dists {temp_indices}/"
        rsync_command = rsync_command.format(
                mirror_url=self.mirror_url,
                temp_indices=self.temp_indices
            )

        self.logger.info(
                "Downloading zzz-dists and storing them in a temporary place"
            )
        rsync_status = Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                             shell=True)

        for line in rsync_status.stdout:
            self.logger.debug(line)

    # Update the 'project' directory, delete the files that do not exist on the
    # mirror you are cloning from, then add an entry for our mirror in
    # project/trace
    def update_project_dir(self):
        rsync_command = "rsync --recursive --times --links --hard-links \
                --progress --delete -vz --stats --no-motd \
                rsync://{mirror_url}/project {mirror_path}/ && date -u \
                > ${mirror_path}/project/trace/$(hostname -f)"

        rsync_command = rsync_command.format(
                mirror_url=self.mirror_url,
                mirror_path=self.mirror_path
            )

        self.logger.info("Updating 'project' directory")
        rsync_status = Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                             shell=True)

        for line in rsync_status.stdout:
            self.logger.debug(line)

    # Check that each index is accurate (Packages.gz and Sources.gz files)
    def check_indices(self):
        dists_path = self.temp_indices
        self.logger.info("Gathering Indices")
        indices = self._get_indices(dists_path)
        dict_indices = {}
        for index in indices:
            split_path = os.path.split(index)
            dir_name = split_path[0]
            file_name = split_path[1]
            if dir_name not in dict_indices.keys():
                dict_indices[dir_name] = [file_name]
            else:
                dict_indices[dir_name] = dict_indices[dir_name] + [file_name]

        for key in dict_indices.keys():
            if "Sources" in dict_indices[key]:
                index = os.path.join(key, "Sources")
                self.check_index(index)

            elif "Sources.gz" in dict_indices[key]:
                index = os.path.join(key, "Sources.gz")
                self.check_index(index)

            elif "Sources.bz2" in dict_indices[key]:
                index = os.path.join(key, "Sources.bz2")
                self.check_index(index)

            if "Packages" in dict_indices[key]:
                index = os.path.join(key, "Packages")
                self.check_index(index)

            elif "Packages.gz" in dict_indices[key]:
                index = os.path.join(key, "Packages.gz")
                self.check_index(index)

            elif "Packages.bz2" in dict_indices[key]:
                index = os.path.join(key, "Packages.bz2")
                self.check_index(index)

    # Find all of the 'Packages.gz' files and 'Sources.gz' files in the 'dists'
    # directory so the check_index() function can check their integrity
    def _get_indices(self, dir):
        if not os.path.isfile(dir):
            indices = []
            for item in os.listdir(dir):
                file_path = os.path.join(dir, item)
                indices = indices + self._get_indices(file_path)

            return indices

        else:
            if re.match(".*(Packages|Sources)(\.gz|\.bz2)?$", dir):
                return [dir]
            else:
                return []

    # Check that the index is accurate and all the files it says exist in our
    # mirror actually exist (do not check the checksum of the file though as
    # that will take too much time)
    def check_index(self, file_name):
        if not re.match(".*(\.gz|\.bz2)$", file_name):
            with open(file_name, 'r') as f_stream:
                f_contents = f_stream.read()

        elif re.match(".*\.gz$", file_name):
            with gzip.open(file_name, 'r') as f_stream:
                f_contents = f_stream.read()

        elif re.match(".*\.bz2$", file_name):
            with bz2.BZ2File(file_name, 'r') as f_stream:
                f_contents = f_stream.read()

        self.logger.debug("Checking index " + file_name)

        if re.match(".*Packages(\.gz|\.bz2)?$", file_name):
            for line in f_contents.split('\n'):
                if line.startswith("Package:"):
                    package = line.split()[1]

                if line.startswith("Filename:"):
                    file_name = line.split(" ")[1]
                    file_path = os.path.join(self.mirror_path, file_name)
                    self.indexed_packages.add(file_name)
                    if not os.path.isfile(file_path):
                        self.logger.error("Missing file: " + file_path)
                        raise MirrorException("Missing file: " + file_path)

        if re.match(".*Sources(\.gz|\.bz2)?$", file_name):
            lines_to_check = []

            for line in f_contents.split('\n'):
                if line.startswith("Package:"):
                    package = line.split()[1]

                elif line.startswith("Directory:"):
                    dir_name = line.split()[1]

                elif line.startswith("Files:"):
                    hash_type = "MD5Sum"

                elif line.startswith(" ") and hash_type == "MD5Sum":
                    lines_to_check = lines_to_check + [line]

                elif line == "":
                    for i in lines_to_check:
                        line_contents = i.split()
                        file_name = line_contents[2]
                        self.indexed_packages.add(
                            os.path.join(dir_name, file_name)
                            )
                        md5Sum = line_contents[0]
                        file_path = os.path.join(self.mirror_path,
                                                 dir_name, file_name)
                        if not os.path.isfile(file_path):
                            self.logger.error("Missing file: " + file_path)
                            raise MirrorException("Missing file: " + file_path)

                elif not line.startswith(" "):
                    hash_type = None
                    dir_name = None
                    lines_to_check = []

    # Check each release file to make sure it is accurate
    def check_release_files(self):
        self.logger.info("Gathering Release Files")
        release_files = self._get_release_files(self.temp_indices)
        for file in release_files:
            self.check_release_file(file)

    # Find all the 'Release' files in the 'dists' directory
    def _get_release_files(self, dir):
        if not os.path.isfile(dir):
            indices = []
            for item in os.listdir(dir):
                file_path = os.path.join(dir, item)
                indices = indices + self._get_release_files(file_path)

            return indices

        else:
            if dir.endswith("/Release"):
                return [dir]
            else:
                return []

    # Check that each index the release file says our mirror has actually
    # exists in our mirror and that the hash_values match. If they are
    # inconsistent it will lead to a broken mirror.
    def check_release_file(self, file_name):
        current_hash_type = None

        self.logger.debug("Checking release file " + file_name)
        with open(file_name) as f_stream:
            f_contents = f_stream.read()

        dir = os.path.split(file_name)[0]

        hash_type = None
        for line in f_contents.split('\n'):
            if line.startswith("MD5Sum"):
                current_hash_type = "MD5SUM"

            elif line.startswith("SHA1"):
                current_hash_type = "SHA1"

            elif line.startswith("SHA256"):
                current_hash_type = "SHA256"

            elif line.startswith(" ") and self.hash_function == current_hash_type:
                file_to_check = line.split()[2]
                hash_val = line.split()[0]
                file_path = os.path.join(dir, file_to_check)

                if os.path.isfile(file_path):

                    with open(file_path, 'r') as f_stream:
                        file_path_contents = f_stream.read()

                    if self.hash_function == "MD5SUM":
                        actual_md5sum = hashlib.md5(file_path_contents).hexdigest()
                        if hash_val != actual_md5sum:
                            self.logger.debug(
                                    actual_md5sum + ' does not match ' +
                                    hash_val + ' for file ' + file_path +
                                    ' (MD5Sum)'
                                )
                            raise MirrorException(
                                    actual_md5sum + ' does not match ' +
                                    hash_val + ' for file ' + file_path +
                                    ' (MD5Sum)'
                                )

                    elif self.hash_function == "SHA1":
                        actual_sha1 = hashlib.sha1(file_path_contents).hexdigest()
                        if hash_val != actual_sha1:
                            self.logger.debug(
                                    actual_sha1 + ' does not match ' +
                                    hash_val + ' for file ' + file_path +
                                    ' (SHA1)'
                                )
                            raise MirrorException(
                                    actual_sha1 + ' does not match ' +
                                    hash_val + ' for file ' + file_path +
                                    ' (SHA1)'
                                )

                    elif self.hash_function == "SHA256":
                        actual_sha256 = hashlib.sha256(file_path_contents).hexdigest()
                        if hash_val != actual_sha256:
                            self.logger.debug(
                                    actual_sha256 + ' does not match ' +
                                    hash_val + ' for file ' + file_path +
                                    ' (SHA256)'
                                )
                            raise MirrorException(
                                    actual_sha256 + ' does not match ' +
                                    hash_val + ' for file ' + file_path +
                                    ' (SHA256)'
                                )

    # Move the 'dists' and 'zzz-dists' into the mirror from their temporary
    # location
    def update_indices(self):
        rsync_command = "rsync --recursive --times --links --hard-links \
                --delay-updates --progress -vz \
                {temp_indices}/dists {mirror_path}/"
        rsync_command = rsync_command.format(
                mirror_path=self.mirror_path,
                temp_indices=self.temp_indices
            )

        self.logger.info("updating 'dists' directory")
        rsync_status = Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                             shell=True)

        rsync_command = "rsync --recursive --times --links --hard-links \
                --delay-updates --progress -vz {temp_indices}/zzz-dists \
                {mirror_path}/"
        rsync_command = rsync_command.format(
                mirror_path=self.mirror_path,
                temp_indices=self.temp_indices
            )

        self.logger.info("updating 'zzz-dists' directory")
        rsync_status = Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                             shell=True)

    # Generate a new 'ls-lR.gz' file
    def gen_lslR(self):
        self.logger.info("Generating ls -lR file")
        ls_status = Popen("ls -lR {mirror_path} > {mirror_path}/ls-lR.new && \
                gzip {mirror_path}/ls-lR.new && \
                mv {mirror_path}/ls-lR.new.gz {mirror_path}/ls-lR.gz".format(
                    mirror_path=self.mirror_path
                ), stdout=PIPE, stderr=PIPE, shell=True
            )

    def clean(self):
        file_name = os.path.join(self.temp_indices, 'files_to_delete')
        rsync_command = "rsync --recursive --times --links --hard-links \
                --contimeout=10 --timeout=10 --no-motd --stats --delete \
                --progress -nvz rsync://{mirror_url}/pool {mirror_path}/"
        rsync_command = rsync_command.format(
            mirror_url=self.mirror_url, mirror_path=self.mirror_path
        )

        self.logger.info("Checking for files to delete")
        rsync_status = Popen(rsync_command, stdout=PIPE, stderr=PIPE, shell=True)

        now_num = int(time.time())
        now = str(now_num)
        yaml_file = os.path.join(self.temp_indices, 'files_to_delete')

        file_contents = None
        try:
            with open(yaml_file, 'r') as f_stream:
                file_contents = yaml.load(f_stream)
                f_stream.close()

        except:
            pass

        if not file_contents:
            file_contents = {}

        file_contents[now] = []

        for line in rsync_status.stdout:
            if re.match('^deleting', line):
                package = line.split()[1]
                file_contents[now].append(package)

        for key in file_contents.keys():
            for package in file_contents[key]:
                if package not in self.indexed_packages:
                    file_path = os.path.join(self.mirror_path, package)
                    if int(now) - int(key) >= self.package_ttl:
                        if os.path.exists(file_path):
                            if os.path.isfile(file_path):
                                self.logger.debug("Removing " + file_path)
                                os.remove(file_path)
                            elif os.path.isdir(file_path):
                                try:
                                    os.rmdir(file_path)
                                    self.logger.debug("Removing " + file_path)
                                except:
                                    self.logger.debug(
                                        "Would have removed " + file_path +
                                        " but it is not empty"
                                    )

                        file_contents[key].remove(package)
                else:
                    file_contents[key].remove(package)

        for x in file_contents.keys():
            if file_contents[x] == []:
                del file_contents[x]

        with open(yaml_file, 'w') as f_stream:
            f_stream.write(yaml.dump(file_contents))
            f_stream.close()
