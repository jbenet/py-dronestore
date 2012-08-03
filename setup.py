#!/usr/bin/env python

from setuptools import setup, find_packages

__version__ = '0.2.2'
# don't forget to update dronestore/__init__.py

packages = filter(lambda p: p.startswith('dronestore'), find_packages())

setup(
  name="dronestore",
  version=__version__,
  description="DroneStore python implementation <http://dronestore.org/>",
  author="Juan Batiz-Benet",
  author_email="juan@benet.ai",
  url="http://github.com/jbenet/py-dronestore",
  keywords=["dronestore", "data versioning"],
  packages=packages,
  install_requires=[
    "bson>=0.3.3",
    "datastore>=0.2.6",
    "nanotime>=0.5.2",
    "smhasher>=0.136.2",
  ],
  license="MIT License"
)
