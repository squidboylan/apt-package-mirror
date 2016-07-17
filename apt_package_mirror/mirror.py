from __future__ import print_function
from ftplib import FTP
import urllib
import gzip
import yaml
import os
import hashlib
from subprocess import Popen, STDOUT, PIPE
import sys

class Mirror:

    def __init__(self, mirror_path, mirror_url, temp_indices='/tmp/dists-indices'):
        self.mirror_path = mirror_path
        self.mirror_url = mirror_url
        self.temp_indices = temp_indices

    def sync(self):
        self.update_mirrors()
        self.get_dists_indices()
        self.check_release_files()
        self.check_indices()
        self.update_indices()
        self.update_project_dir()
        self.gen_lslR()

    def update_mirrors(self):
        rsync_command = "rsync --recursive --times --links --hard-links \
                --exclude 'Packages*' --exclude 'Sources*' --exclude 'Release*' \
                --contimeout=10 --timeout=10 --no-motd \
                -vPz rsync://{mirror_url}/ {mirror_path}"
        rsync_command = rsync_command.format(
                mirror_url=self.mirror_url,
                mirror_path=self.mirror_path
            )

        rsync_status = Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                shell=True)

        for line in rsync_status.stdout:
            print(line)

        if rsync_status.returncode != 0:
            print(rsync_command + " Failed with return code " + str(rsync_status.returncode))


    def get_dists_indices(self):
        rsync_command = "rsync --recursive --times --links --hard-links \
                --exclude 'installer*' --delete --no-motd \
                -vPz rsync://{mirror_url}/dists/ {temp_indices}"
        rsync_command = rsync_command.format(
                mirror_url=self.mirror_url,
                temp_indices=self.temp_indices
            )

        rsync_status = Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                shell=True)

        for line in rsync_status.stdout:
            print(line)

    def update_project_dir(self):
        rsync_command = "rsync --recursive --times --links --hard-links \
                --delete -vPz --no-motd rsync://{mirror_url}/project \
                {mirror_path} && date -u > ${mirror_path}/project/trace/$(hostname -f)"

        rsync_command = rsync_command.format(
                mirror_url=self.mirror_url,
                mirror_path=self.mirror_path
            )

        rsync_status = Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                shell=True)

        for line in rsync_status.stdout:
            print(line)

    def check_indices(self):
        dists_path = self.temp_indices
        indices = self._get_indices(dists_path)
        for index in indices:
            self.check_index(index)
            #print(index)

    def _get_indices(self, dir):
        if not os.path.isfile(dir):
            indices = []
            for item in os.listdir(dir):
                file_path = os.path.join(dir, item)
                indices = indices + self._get_indices(file_path)

            return indices

        else:
            if dir.endswith("Packages.gz") or dir.endswith("Sources.gz"):
                return [dir]
            else:
                return []

    def check_release_files(self):
        release_files = self._get_release_files(self.temp_indices)
        for file in release_files:
            self.check_release_file(file)

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

    def check_release_file(self, file_name):
        with open(file_name) as f_stream:
            f_contents = f_stream.read()

        dir = os.path.split(file_name)[0]

        hash_type = None
        for line in f_contents.split('\n'):
            if line.startswith("MD5Sum"):
                hash_type = "MD5Sum"

            elif line.startswith("SHA1"):
                hash_type = "SHA1"

            elif line.startswith("SHA256"):
                hash_type = "SHA256"

            elif hash_type == "MD5Sum":
                file_to_check = line.split()[2]
                md5sum = line.split()[0]
                file_path = os.path.join(dir, file_to_check)

                if os.path.isfile(file_path):
                    print("Found file: " + file_path)
                    with open(file_path, 'r') as f_stream:
                        file_path_contents = f_stream.read()

                    actual_md5sum = hashlib.md5(file_path_contents).hexdigest()
                    if md5sum != actual_md5sum:
                        print(actual_md5sum + ' does not match ' + md5sum)
                        sys.exit(1)

    def check_index(self, file_name):
        with gzip.open(file_name) as f_stream:
            f_contents = f_stream.read()

        if file_name.endswith("Packages.gz"):
            for line in f_contents.split('\n'):
                if line.startswith("Package:"):
                    package = line.split()[1]

                if line.startswith("Filename:"):
                    file_name = line.split(" ")[1]
                    file_path = os.path.join(self.mirror_path, file_name)
                    if not os.path.isfile(file_path):
                        print("Missing file: " + file_path)
                        sys.exit(1)
                    else:
                        print("Found file: " + file_path)

    def update_indices(self):
        rsync_command = "rsync --recursive --times --links --hard-links \
                -vPz --delete {temp_indices}/ {mirror_path}/dists"
        rsync_command = rsync_command.format(
                mirror_path=self.mirror_path,
                temp_indices=self.temp_indices
            )

        rsync_status = Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                shell=True)

    def gen_lslR(self):
        print("Generating ls -lR file")
        ls_status = Popen("rm {mirror_path}/ls-lR.gz ; ls -lR {mirror_path} > {mirror_path}/ls-lR && gzip {mirror_path}/ls-lR".format(mirror_path=self.mirror_path), stdout=PIPE, stderr=PIPE, shell=True)
