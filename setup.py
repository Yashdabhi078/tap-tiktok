#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-tiktok",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_tiktok"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python==5.12.1",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-tiktok=tap_tiktok:main
    """,
    packages=["tap_tiktok"],
    package_data={
        "schemas": ["tap_tiktok/schemas/*.json"]
    },
    include_package_data=True,
)
