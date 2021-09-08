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
    name='netapp-api',
    version=netapp.__version__,
    author='CERN',
    author_email='albin.stjerna@cern.ch',
    maintainer='Borja Aparicio Cotarelo',
    maintainer_email='borja.aparicio.cotarelo@cern.ch',
    description='NetApp OCUM API wrapper',
    url='https://github.com/cerndb/netapp-api-python',
    license='GPLv3',
    packages=['netapp'],
    install_requires=['pytz', 'requests', 'lxml', 'six'],
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
    ],
    scripts=['bin/ontap_tool'],
)
