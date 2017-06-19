# setup.py
#
# Copyright 2017 Socos LLC
#

import os

from setuptools import setup

def read(*paths):
    """Build a file path from *paths* and return the contents."""
    with open(os.path.join(*paths), 'r') as f:
        return f.read()

setup(
    name='datastore-to-gcs',
    packages=['datastore_to_gcs'],
    version='0.2',
    license='Apache License 2.0',
    description='Transfer data from Google Cloud Datastore to Google Cloud Storage.',
    long_description=(read('README.rst')),
    author='Brandon Istenes',
    author_email='bistenes@socos.me',
    url='https://github.com/SocosLLC/datastore-to-gcs',
    download_url='https://github.com/SocosLLC/datastore-to-gcs/archive/0.2.tar.gz',
    keywords=['gae', 'gcp', 'gcs', 'gcd', 'datastore', 'cloudstorage', 'transfer', 'dump'],
    classifiers=['Development Status :: 4 - Beta',
                 'License :: OSI Approved :: Apache Software License']
)
