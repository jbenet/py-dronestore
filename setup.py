#!/usr/bin/env python

from setuptools import setup

import dronestore
# not customary, but if this doesn't work, then why bother installing?


setup(
  name="dronestore",
  version=dronestore.__version__,
  description="DroneStore python implementation <http://dronestore.org/>",
  author="Juan Batiz-Benet",
  author_email="jbenet@cs.stanford.com",
  url="http://github.com/jbenet/dronestore",
  keywords=["dronestore", "data versioning"],
  packages=["dronestore"],
  install_requires=["bson", "smhasher"],
  license="MIT License"
)
