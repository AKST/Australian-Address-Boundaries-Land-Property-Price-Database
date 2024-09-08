import os
import json

from shutil import rmtree
from urllib.request import urlretrieve, Request, urlopen
from zipfile import ZipFile, BadZipFile

import lib.notebook_constants as nb
from lib.data_types import Target

static_dirs = [
    'web-out', 
    'zip-out', 
]

CHUNKSIZE_16KB = 16384

def _is_dir_empty(directory):
    # Use os.scandir() to iterate over directory entries
    with os.scandir(directory) as it:
        for entry in it:
            return False  # Return False as soon as we find any entry
    return True

def recursively_unzip_child_zips(directory):
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.zip'):
                zip_path = os.path.join(root, file)
                dst_path = os.path.join(root, os.path.splitext(file)[0])
                with ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(dst_path)
                os.remove(zip_path)
    else:
        return
        
    recursively_unzip_child_zips(directory)

class StaticFileInitialiser:
    def __init__(self, targets, dirs):
        self.targets = targets
        self.dirs = dirs

    def add_target(self, target):
        self.targets.append(target)

    @staticmethod
    def create():
        return StaticFileInitialiser([
            Target(
                url=nb.non_abs_structures_shapefiles,
                web_dst='non_abs_shape.zip',
                zip_dst='non_abs_structures_shapefiles'),
            Target(url=nb.abs_structures_shapefiles, web_dst='cities.zip', zip_dst='cities'),
        ], static_dirs)

    def setup_dirs(self):
        for d in self.dirs:
            if not os.path.isdir(d):
                os.mkdir(d) 

    def fetch_remote_resources(self):
        for t in self.targets:
            print(f'Checking {t.web_dst}')
            w_out = 'web-out/%s' % t.web_dst
            z_out = t.zip_dst and 'zip-out/%s' % t.zip_dst
            
            if not os.path.isfile(w_out):
                print(f'  - Downloading file {t.url}')
                request = Request(t.url)
                
                if t.token:
                    request.add_header('Authorization', f'Basic {t.token}')
                    
                with urlopen(request) as response, open(w_out, 'wb') as f:
                    while True:
                        chunk = response.read(CHUNKSIZE_16KB)
                        if not chunk:
                            break
                        f.write(chunk)
                    
            if t.zip_dst and not os.path.isdir(z_out):
                os.mkdir(z_out)
            
            if t.zip_dst and _is_dir_empty(z_out):
                try:
                    with ZipFile(w_out, 'r') as z:
                        z.extractall(z_out)
                    recursively_unzip_child_zips(z_out)
                except BadZipFile as e:
                    print(f'failed to unzip, {t.web_dst} to {t.zip_dst} {e}')
                    os.remove(w_out)