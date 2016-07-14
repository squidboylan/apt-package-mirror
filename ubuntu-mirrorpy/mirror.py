from __future__ import print_function
from ftplib import FTP
import urllib
import yaml
import os
import sys

class Mirror:

    def __init__(self, mirror_path, mirror_url):
        self.mirror_path = mirror_path
        self.mirror_url = mirror_url

    def sync(self):
        self.get_indices()

    def get_indices(self):
        rsync_status = os.popen("rsync -tarP rsync://{mirror_url}:/ubuntu/indices ./".format(mirror_url=self.mirror_url))
        print(rsync_status.read())
