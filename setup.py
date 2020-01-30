#!/usr/bin/env python3

from setuptools import setup, find_packages

requirements = [
    'testing', 'fastjsonschema', 'geopandas', 'shapely', 'requests'  #
    # dcfs_geodb requirements are given in file ./environment.yml.
    #
]

packages = find_packages(exclude=["tests", "tests.*"])

# Same effect as "from cate import version", but avoids importing cate:
version = None
with open('xcube_geodb/version.py') as f:
    exec(f.read())

setup(
    name="dcfs_geodb",
    version=version,
    description='Geo DB for DCFS',
    license='MIT',
    author='DCFS Development Team',
    packages=packages,
    entry_points={
        'console_scripts': [
            'geodb = dcfs_geodb.cli.main:main',
        ],
    },
    install_requires=requirements,
)
