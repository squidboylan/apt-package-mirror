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
    def __init__(self, config, mirror_path, mirror_url,
                 temp_indices=None, log_file=None, log_level=None,
                 package_ttl=None, hash_function=None):

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

        if 'distributions' in config.keys():
            if type(config['distributions']) is list:
                self.distributions = config['distributions']
            else:
                self.logger.error("the 'distributions' option must be a list")
        else:
            self.distributions = ['*']

        if 'architectures' in config.keys():
            if type(config['architectures']) is list:
                self.architectures = config['architectures']
            else:
                self.logger.error("the 'architectures' option must be a list")
        else:
            self.architectures = ['*']

        if 'repos' in config.keys():
            if type(config['architectures']) is list:
                self.repos = config['repos']
            else:
                self.logger.error("the 'repos' option must be a list")
        else:
            self.repos = ['*']

        self.parallel_downloads = 8
        if 'parallel_downloads' in config.keys():
            self.parallel_downloads = int(config['parallel_downloads'])

        self.package_ttl = package_ttl
        self.mirror_path = mirror_path
        self.mirror_url = mirror_url
        self.temp_indices = temp_indices
        self.indexed_packages = set()

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
            self.update_dists()
            self.check_release_files()
            self.check_indices()
            self.update_top_level_dirs()
            self.update_indices()
            self.gen_lslR()
            self.remove_old_packages()
            os.remove(self.lock_file)
        except:
            self.logger.info("Exception caught, removing lock file")
            os.remove(self.lock_file)
            raise

    def update_dists(self):
        self.logger.debug("Downloading dists\nrepos: " + str(self.repos) +
                "\narchitectures: " + str(self.architectures) + "\ndistributions: " + str(self.distributions))
        rsync_template_dist = "rsync --recursive --times --links --hard-links \
                --contimeout=10 --timeout=10 --no-motd --stats \
                --progress -vzR \
                rsync://{mirror_url}/./dists/{dist}/Release* \
                rsync://{mirror_url}/./dists/{dist}/InRelease \
                rsync://{mirror_url}/./dists/{dist}/Changelog* \
                {mirror_path}/"

        rsync_template_zzz_dists = "rsync --recursive --times --links --hard-links \
                --contimeout=10 --timeout=10 --no-motd --stats \
                --progress -vzR \
                rsync://{mirror_url}/./zzz-dists/{dist} \
                {mirror_path}/"

        rsync_template_repo = "rsync --recursive --times --links --hard-links \
                --contimeout=10 --timeout=10 --no-motd --stats \
                --progress -vzR \
                rsync://{mirror_url}/./dists/{dist}/{repo}/by-hash \
                rsync://{mirror_url}/./dists/{dist}/{repo}/binary-all \
                rsync://{mirror_url}/./dists/{dist}/{repo}/*source* \
                rsync://{mirror_url}/./dists/{dist}/{repo}/i18n \
                rsync://{mirror_url}/./dists/{dist}/{repo}/Release* \
                rsync://{mirror_url}/./dists/{dist}/{repo}/InRelease \
                rsync://{mirror_url}/./dists/{dist}/{repo}/dep11/by-hash \
                rsync://{mirror_url}/./dists/{dist}/{repo}/dep11/icons* \
                {mirror_path}/"

        rsync_template_arch = "rsync --recursive --times --links --hard-links \
                --contimeout=10 --timeout=10 --no-motd --stats \
                --progress -vzR \
                rsync://{mirror_url}/./dists/{dist}/{repo}/debian-installer/*{arch}* \
                rsync://{mirror_url}/./dists/{dist}/{repo}/dep11/*{arch}* \
                rsync://{mirror_url}/./dists/{dist}/{repo}/*{arch}* \
                {mirror_path}/"

        for dist in self.distributions:
            rsync_command_dist = rsync_template_dist.format(
                    mirror_url=self.mirror_url,
                    mirror_path=self.temp_indices,
                    dist=dist,
                )
            rsync_status = Popen(rsync_command_dist, stdout=PIPE, stderr=PIPE, shell=True)
            for line in rsync_status.stdout:
                self.logger.debug(line)

            rsync_command_zzz_dists = rsync_template_zzz_dists.format(
                    mirror_url=self.mirror_url,
                    mirror_path=self.temp_indices,
                    dist=dist,
                )
            rsync_status = Popen(rsync_command_zzz_dists, stdout=PIPE, stderr=PIPE, shell=True)
            for line in rsync_status.stdout:
                self.logger.debug(line)

            for repo in self.repos:
                rsync_command_repo = rsync_template_repo.format(
                        mirror_url=self.mirror_url,
                        mirror_path=self.temp_indices,
                        dist=dist,
                        repo=repo,
                    )
                rsync_status = Popen(rsync_command_repo, stdout=PIPE, stderr=PIPE, shell=True)
                for line in rsync_status.stdout:
                    self.logger.debug(line)

                for arch in self.architectures:
                    rsync_command_arch = rsync_template_arch.format(
                            mirror_url=self.mirror_url,
                            mirror_path=self.temp_indices,
                            dist=dist,
                            repo=repo,
                            arch=arch,
                        )
                    rsync_status = Popen(rsync_command_arch, stdout=PIPE, stderr=PIPE, shell=True)
                    for line in rsync_status.stdout:
                        self.logger.debug(line)

    # Update everything in the top level dir not including dists/, zzz-dists/,
    # pool/, and ls-lR.gz
    def update_top_level_dirs(self):
        rsync_command = "rsync --recursive --times --links --hard-links \
                --progress --delete -vz --stats --no-motd \
                --exclude='dists' --exclude='zzz-dists' --exclude 'pool' \
                --exclude='ls-lR.gz' \
                rsync://{mirror_url}/./* {mirror_path}/ && date -u \
                > ${mirror_path}/project/trace/$(hostname -f)"

        rsync_command = rsync_command.format(
                mirror_url=self.mirror_url,
                mirror_path=self.mirror_path
            )

        self.logger.info("Updating top level directories")
        rsync_status = Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                             shell=True)

        for line in rsync_status.stdout:
            self.logger.debug(line)

    # Check that each index is accurate (Packages.gz and Sources.gz files)
    def check_indices(self):
        self.logger.info("Gathering Indices")
        indices = self._get_indices(self.temp_indices)
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
                self.check_sources_file(index)

            elif "Sources.gz" in dict_indices[key]:
                index = os.path.join(key, "Sources.gz")
                self.check_sources_file(index)

            elif "Sources.bz2" in dict_indices[key]:
                index = os.path.join(key, "Sources.bz2")
                self.check_sources_file(index)

            if "Packages" in dict_indices[key]:
                index = os.path.join(key, "Packages")
                self.check_packages_file(index)

            elif "Packages.gz" in dict_indices[key]:
                index = os.path.join(key, "Packages.gz")
                self.check_packages_file(index)

            elif "Packages.bz2" in dict_indices[key]:
                index = os.path.join(key, "Packages.bz2")
                self.check_packages_file(index)

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
    def check_packages_file(self, file_name):
        rsync_queue = []
        rsync_template = "rsync --times --links --hard-links \
                --contimeout=10 --timeout=10 --no-motd -vzR \
                rsync://{mirror_url}/./{file_path} \
                {mirror_path}/"
        if not re.match(".*(\.gz|\.bz2)$", file_name):
            with open(file_name, 'r') as f_stream:
                f_contents = f_stream.read().decode('utf-8')

        elif re.match(".*\.gz$", file_name):
            with gzip.open(file_name, 'r') as f_stream:
                f_contents = f_stream.read().decode('utf-8')

        elif re.match(".*\.bz2$", file_name):
            with bz2.BZ2File(file_name, 'r') as f_stream:
                f_contents = f_stream.read().decode('utf-8')

        self.logger.debug("Checking index " + file_name)

        for package_block in f_contents.split('\n\n'):
            package_info = self.process_package_data(package_block)

            # If the package has a file , then rsync the file
            if 'relative_path' in package_info.keys():
                self.logger.debug("Downloading: " + package_info['relative_path'])
                rsync_command = rsync_template.format(
                        mirror_url=self.mirror_url,
                        mirror_path=self.mirror_path,
                        file_path=package_info['relative_path']
                    )
                rsync_queue.append((Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                        shell=True), package_info['relative_path'], package_info['full_path']))

                if len(rsync_queue) > self.parallel_downloads:
                    (status, relative_path, full_path) = rsync_queue.pop(0)
                    self.wait_for_rsync(status, relative_path, full_path)

        for (status, relative_path, full_path) in rsync_queue:
            self.wait_for_rsync(status, relative_path, full_path)

    def process_package_data(self, package_data):
        package_info = {}
        for line in package_data.split('\n'):
            if line.startswith("Package:"):
                package_info['package'] = line.split()[1]

            if line.startswith("Filename:"):
                package_info['relative_path'] = line.split(' ')[1]
                package_info['full_path'] = os.path.join(self.mirror_path, package_info['relative_path'])

        return package_info

    # Check that the index is accurate and all the files it says exist in our
    # mirror actually exist (do not check the checksum of the file though as
    # that will take too much time)
    def check_sources_file(self, file_name):
        rsync_queue = []
        rsync_template = "rsync --times --links --hard-links \
                --contimeout=10 --timeout=10 --no-motd -vzR \
                rsync://{mirror_url}/./{file_path} \
                {mirror_path}/"

        if not re.match(".*(\.gz|\.bz2)$", file_name):
            with open(file_name, 'r') as f_stream:
                f_contents = f_stream.read().decode('utf-8')

        elif re.match(".*\.gz$", file_name):
            with gzip.open(file_name, 'r') as f_stream:
                f_contents = f_stream.read().decode('utf-8')

        elif re.match(".*\.bz2$", file_name):
            with bz2.BZ2File(file_name, 'r') as f_stream:
                f_contents = f_stream.read().decode('utf-8')

        lines_to_check = []

        for block in f_contents.split('\n\n'):
            source_info = self.process_source_data(block)
            for i in source_info['files']:
                relative_path = os.path.join(source_info['directory'], i)
                self.logger.debug("Downloading: " + relative_path)
                full_path = os.path.join(self.mirror_path, relative_path)

                rsync_command = rsync_template.format(
                        mirror_url=self.mirror_url,
                        mirror_path=self.mirror_path,
                        file_path=relative_path
                    )
                rsync_queue.append((Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                                     shell=True), relative_path, full_path))

                if len(rsync_queue) >= self.parallel_downloads:
                    (status, relative_path, full_path) = rsync_queue.pop(0)
                    self.wait_for_rsync(status, relative_path, full_path)

        for (status, relative_path, full_path) in rsync_queue:
            self.wait_for_rsync(status, relative_path, full_path)


    def wait_for_rsync(self, status, relative_path, full_path):
        status.wait()
        self.indexed_packages.add(relative_path)
        if not os.path.isfile(full_path):
            self.logger.error("Missing file: " + full_path)
            raise MirrorException("Missing file: " + full_path)


    def process_source_data(self, source_data):
        source_info = {'files': []}
        last_line_files = False
        for line in source_data.split('\n'):
            if line.startswith("Package:"):
                source_info['package'] = line.split()[1]
                last_line_files = False

            elif line.startswith("Directory:"):
                source_info['directory'] = line.split()[1]
                last_line_files = False

            elif line.startswith("Files:"):
                last_line_files = True

            elif line.startswith(" ") and last_line_files == True:
                source_info['files'] = source_info['files'] + [line.split()[2]]
            else:
                last_line_files = False

        return source_info


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

    # List all files in a dir recursively
    def _get_files(self, path):
        if os.path.isdir(path):
            indices = []
            for item in os.listdir(path):
                file_path = os.path.join(path, item)
                indices = indices + self._get_release_files(file_path)
            return indices
        else:
            return [path]

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
                    if self.hash_function == "MD5SUM":
                        self.check_md5(file_path, hash_val)

                    elif self.hash_function == "SHA1":
                        self.check_sha1(file_path, hash_val)

                    elif self.hash_function == "SHA256":
                        self.check_sha256(file_path, hash_val)

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

    def remove_old_packages(self):
        actual_packages = set(self._get_files(os.path.join(self.mirror_path, 'pool')))
        old_files = actual_packages - self.indexed_packages
        yaml_file = os.path.join(self.temp_indices, 'files_to_delete')
        now_num = int(time.time())
        now = str(now_num)

        # Load the file with all the packages that are no longer indexed
        yaml_data = {}
        try:
            with open(yaml_file, 'r') as f_stream:
                yaml_data = yaml.load(f_stream)
                f_stream.close()
        except:
            pass

        # Check each unindexed file, if it's been marked in a previous run and
        # has been unindexed for long enough, delete it, Also clean up empty
        # directories if there are any
        for f in old_files:
            if f in yaml_data.keys():
                time_marked = int(yaml_data[f])
                if now_num - time_marked >= self.package_ttl:
                    full_path = os.path.join(self.mirror_path, f)
                    self.logger.debug("Removing: ")
                    #os.remove(full_path)
            else:
                yaml_data[f] = now

        with open(yaml_file, 'w') as f_stream:
            f_stream.write(yaml.dump(yaml_data))
            f_stream.close()

    def check_md5(self, file_path, hash_val):
        with open(file_path, 'rb') as f_stream:
            contents = f_stream.read()

        self.logger.debug("checking " + file_path + " md5sum")
        actual_md5sum = hashlib.md5(contents).hexdigest()
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

    def check_sha1(self, file_path, hash_val):
        with open(file_path, 'rb') as f_stream:
            contents = f_stream.read()

        self.logger.debug("checking " + file_path + " sha1")
        actual_sha1 = hashlib.sha1(contents).hexdigest()
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

    def check_sha256(self, file_path, hash_val):
        with open(file_path, 'rb') as f_stream:
            contents = f_stream.read()

        self.logger.debug("checking " + file_path + " sha256")
        actual_sha256 = hashlib.sha256(contents).hexdigest()
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


    # List all files in a dir recursively
    def _get_files(self, path):
        if os.path.isdir(path):
            indices = []
            for item in os.listdir(path):
                file_path = os.path.join(path, item)
                indices = indices + self._get_release_files(file_path)
            return indices
        else:
            return [path]
