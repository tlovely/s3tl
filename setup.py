#!/usr/bin/env python

from distutils.core import setup


setup(
    name="s3tl",
    version="0.0.1",
    description="Framework for building ETLs against AWS S3",
    author="Tyler Lovely",
    author_email="tyler.n.lovely@gmail.com",
    packages=["s3tl"],
    python_requires=">=3.6, <4",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3 :: Only",
    ],
    install_requires=[
        "aiofiles>=0.6.0,<1",
        "aiobotocore>=1.2.1,<2",
        "toolz",
    ],
)
