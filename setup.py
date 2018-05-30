#!/usr/bin/env python2.7

from distutils.core import setup

setup(name='backup',
    version='0.1',
    description='A backup utility for ASE database to Azure Blob Storage',
    author='Dr. Christian Geuer-Pollmann',
    author_email='chgeuer@microsoft.com',
    install_requires=[
                        'pid>=2.2.0',
                        'azure-storage-blob>=1.1.0'
                    ],
    )
