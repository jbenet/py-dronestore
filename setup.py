#!/usr/bin/env python

from setuptools import setup

__version__ = '0.1.8'
# don't forget to update dronestore/__init__.py

setup(
  name="dronestore",
  version=__version__,
  description="DroneStore python implementation <http://dronestore.org/>",
  author="Juan Batiz-Benet",
  author_email="jbenet@cs.stanford.com",
  url="http://github.com/jbenet/dronestore",
  keywords=["dronestore", "data versioning"],
  packages=["dronestore"],
  install_requires=["bson", "smhasher", "nanotime"],
  license="MIT License"
)
