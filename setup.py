"""Setup script"""

from setuptools import setup

setup(
    name='asebackupcli',
    version='0.3.13',
    packages=['asebackupcli'],
    description="A backup utility for Sybase ASE databases into Azure blob storage",
    author="Dr. Christian Geuer-Pollmann",
    author_email='chgeuer@microsoft.com',
    entry_points={
        'console_scripts': [
            'asebackupcli = asebackupcli.__main__:main'
        ]
    },
    include_package_data=True,
    install_requires=[
        'pid>=2.2.0',
        'azure-storage-common>=1.2.0rc0,<1.3.0',
        'azure-storage-blob>=1.1.0rc0,<1.3.0',
        'msrestazure>=0.4.14'
    ],
    tests_require=[
        'mock'
    ])
