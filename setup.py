# -*- coding:utf-8 -*-
import setuptools
import glob
from numpygraph import __version__

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="numpygraph",  # Replace with your own username
    version=__version__,
    author="hp027",
    author_email="hp027@foxmail.com",
    description="基于numpy实现的图存储",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/turinggraph/numpygraph",
    packages=setuptools.find_packages(),
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={"console_scripts": ["npg = numpygraph:cmd"]},
    python_requires=">=3.6",
    install_requires=open("requirements.txt", "r").read().strip().split("\n"),
)
