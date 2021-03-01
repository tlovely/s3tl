#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="s3tl",
    version="0.0.1",
    description="Framework for building ETLs against AWS S3",
    long_description=open("README.md").read(),
    license="MIT",
    author="Tyler Lovely",
    author_email="tyler.n.lovely@gmail.com",
    packages=find_packages(),
    python_requires=">=3.6, <4",
    url="https://github.com/tlovely/s3tl",
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
    extras_require={
        "dev": [
            "pytest==6.2.2",
            "mypy==0.812",
            "black==20.8b1",
        ]
    },
)
