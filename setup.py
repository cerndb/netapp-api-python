# Copyright (C) 2016, CERN
# This software is distributed under the terms of the GNU General Public
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
# In applying this license, CERN does not waive the privileges and immunities
# granted to it by virtue of its status as Intergovernmental Organization
# or submit itself to any jurisdiction.

import os
from setuptools import setup
import netapp

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name="netapp-api",
    version=netapp.__version__,
    author="CERN",
    author_email='albin.stjerna@cern.ch',
    maintainer='Albin Stjerna',
    maintainer_email='albin.stjerna@cern.ch',
    description=("NetApp OCUM API wrapper"),
    url='https://github.com/cerndb/netapp-api-python',
    license="GPLv3",
    packages=['netapp'],
    install_requires=['pytz', 'requests', 'lxml'],
    long_description=read('README.md'),
)
