#!/usr/bin/env python

from setuptools import setup

# Remember to change in dronestore/__init__.py as well!
version = "0.1"

setup(
    name="dronestore",
    version=version,
    description="DroneStore python implementation <http://dronestore.org/>",
    author="Juan Batiz-Benet",
    author_email="jbenet@cs.stanford.com",
    url="http://github.com/jbenet/dronestore",
    keywords=["dronestore", "data versioning"],
    packages=["dronestore"],
    install_requires=["bson"],
    license="MIT License"
)
