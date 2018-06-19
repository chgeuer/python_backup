from setuptools import setup

setup(
    name = 'asebackupcli',
    version = '0.1.0',
    packages = ['asebackupcli'],
    entry_points = {
        'console_scripts': [
            'asebackupcli = asebackupcli.__main__:main'
        ]
    })
