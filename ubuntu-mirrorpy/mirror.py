from __future__ import print_function
from ftplib import FTP
import urllib
import gzip
import yaml
import os
from subprocess import Popen, STDOUT, PIPE
import sys

class Mirror:

    def __init__(self, mirror_path, mirror_url):
        self.mirror_path = mirror_path
        self.mirror_url = mirror_url
        self.temp_indices = '/tmp/dists-indices'

    def sync(self):
        self.get_pool()
        self.get_dists()
        self.get_dists_indices()
        self.get_indices()
        self.check_indices()
        self.update_indices()
        self.gen_lslR()

    def get_indices(self):
        rsync_command = "rsync --recursive --times --links --hard-links -vPz rsync://{mirror_url}:/ubuntu/indices/ {mirror_path}/indices.tmp"
        rsync_command = rsync_command.format(
                mirror_url=self.mirror_url,
                mirror_path=self.mirror_path
            )
        rsync_status = Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                shell=True)

        for line in rsync_status.stdout:
            print(line)

    def get_pool(self):
        rsync_command = "rsync --recursive --times --links --hard-links -vPz rsync://{mirror_url}:/ubuntu/pool {mirror_path}"
        rsync_command = rsync_command.format(
                mirror_url=self.mirror_url,
                mirror_path=self.mirror_path
            )

        rsync_status = Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                shell=True)

        for line in rsync_status.stdout:
            print(line)

    def get_dists(self):
        rsync_command = "rsync --recursive --times --links --hard-links \
                --exclude 'Packages*' --exclude 'Sources*' --exclude 'Release*' \
                -vPz rsync://{mirror_url}:/ubuntu/dists {mirror_path}"
        rsync_command = rsync_command.format(
                mirror_url=self.mirror_url,
                mirror_path=self.mirror_path
            )

        rsync_status = Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                shell=True)

        for line in rsync_status.stdout:
            print(line)

    def get_dists_indices(self):
        rsync_command = "rsync --recursive --times --links --hard-links \
                --exclude 'installer*' \
                -vPz rsync://{mirror_url}:/ubuntu/dists/ {temp_indices}"
        rsync_command = rsync_command.format(
                mirror_url=self.mirror_url,
                temp_indices=self.temp_indices
            )

        rsync_status = Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                shell=True)

        for line in rsync_status.stdout:
            print(line)

    def check_indices(self):
        dists_path = self.temp_indices
        indices = self._check_indices(dists_path)
        for index in indices:
            self.check_index(index)
            #print(index)

    def _check_indices(self, dir):
        #print(dir)
        if not os.path.isfile(dir):
            indices = []
            for item in os.listdir(dir):
                file_path = os.path.join(dir, item)
                indices = indices + self._check_indices(file_path)

            return indices

        else:
            if dir.endswith("Packages.gz") or dir.endswith("Sources.gz"):
                return [dir]
            else:
                return []



    def check_index(self, file_name):
        with gzip.open(file_name) as f_stream:
            f_contents = f_stream.read()

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
                -vPz {temp_indices}/ {mirror_path}/dists"
        rsync_command = rsync_command.format(
                mirror_path=self.mirror_path,
                temp_indices=self.temp_indices
            )

        rsync_status = Popen(rsync_command, stdout=PIPE, stderr=PIPE,
                shell=True)

    def gen_lslR(self):
        print("Generating ls -lR file")
        ls_status = Popen("rm {mirror_path}/ls-lR ; ls -lR {mirror_path} > {mirror_path}/ls-lR && gzip {mirror_path}/ls-lR".format(mirror_path=self.mirror_path), stdout=PIPE, stderr=PIPE, shell=True)
