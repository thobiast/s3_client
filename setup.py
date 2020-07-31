# -*- coding: utf-8 -*-
"""setup.py."""


import setuptools

# Package meta-data.
NAME = "s3_client"
DESCRIPTION = "Sample python script to work with Amazon S3."
URL = "https://github.com/thobiast/s3_client"
AUTHOR = "Thobias Salazar Trevisan"
VERSION = "0.1.0"


def read_file(fname):
    """Read file and return the its content."""
    with open(fname, "r") as f:
        return f.read()


setuptools.setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    author=AUTHOR,
    url=URL,
    license="MIT",
    long_description=read_file("README.md"),
    long_description_content_type="text/markdown",
    install_requires=read_file("requirements.txt").splitlines(),
    packages=setuptools.find_packages(
        exclude=(["tests", "*.tests", "*.tests.*", "tests.*"])
    ),
    include_package_data=True,
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Environment :: Console",
        "Topic :: Software Development",
        "Topic :: Terminals",
        "Topic :: Utilities",
        "Programming Language :: Python :: 3",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Operating System :: OS Independent",
    ],
    # flake8: noqa: E231
    entry_points={"console_scripts": ["s3_client=s3_client.s3_client:main",],},
)

# vim: ts=4
