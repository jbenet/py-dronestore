#!/usr/bin/env python

from setuptools import setup, find_packages

__version__ = '0.1.10'
# don't forget to update dronestore/__init__.py

packages = filter(lambda p: p.startswith('dronestore'), find_packages())

setup(
  name="dronestore",
  version=__version__,
  description="DroneStore python implementation <http://dronestore.org/>",
  author="Juan Batiz-Benet",
  author_email="jbenet@cs.stanford.com",
  url="http://github.com/jbenet/py-dronestore",
  keywords=["dronestore", "data versioning"],
  packages=packages,
  install_requires=["bson", "smhasher", "nanotime"],
  license="MIT License"
)
