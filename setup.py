#!/usr/bin/python

from setuptools import setup, find_packages

setup(
    name="dkc",
    license='Apache License 2.0',
    version="0.0.2",
    description="",
    author="Leonid Rashkovsky",
    author_email="rashkovsky@gmail.com",
    url="https://github.com/gtfx/dkc",
    scripts=["bin/controller"],
    packages=["dkc"],
    install_requires=[
        'boto==2.27.0',
        'pytz==2014.2'
    ],
    tests_require=[
        'unittest2',
        'Nose>=1.0',
    ],
    classifiers=[
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Topic :: Software Development",
        "Topic :: Utilities",
    ])
