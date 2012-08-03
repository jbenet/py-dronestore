#!/usr/bin/env python

from setuptools import setup, find_packages

import re
main_py = open('dronestore/__init__.py').read()
metadata = dict(re.findall("__([a-z]+)__ = '([^']+)'", main_py))
packages = filter(lambda p: p.startswith('dronestore'), find_packages())

setup(
  name="dronestore",
  version=metadata['version'],
  description="DroneStore python implementation <http://dronestore.org/>",
  author=metadata['author'],
  author_email=metadata['email'],
  url="http://github.com/jbenet/py-dronestore",
  keywords=["dronestore", "data versioning"],
  packages=packages,
  install_requires=[
    "bson>=0.3.3",
    "datastore>=0.2.8",
    "nanotime>=0.5.2",
    "smhasher>=0.136.2",
  ],
  license="MIT License"
)
