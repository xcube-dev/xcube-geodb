#!/usr/bin/env python3

# The MIT License (MIT)
# Copyright (c) 2020 by Brockmann Consult GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from pathlib import Path

from setuptools import find_packages, setup
from xcube_geodb.version import version

requirements = [
    "geopandas",
    "python-dotenv",
    "requests",
    "shapely",
]

packages = find_packages(exclude=["test", "test.*"])
readme = Path(__file__).parent.joinpath("README.md").read_text(encoding="utf-8")


setup(
    name="xcube_geodb",
    version=version,
    description="Python client for the xcube geoDB service",
    long_description=readme,
    long_description_content_type="text/markdown",
    license="MIT",
    author="xcube Development Team",
    url="https://github.com/xcube-dev/xcube-geodb",
    project_urls={
        "Source": "https://github.com/xcube-dev/xcube-geodb",
        "Documentation": "https://xcube-dev.github.io/xcube-geodb",
        "Issues": "https://github.com/xcube-dev/xcube-geodb/issues",
    },
    python_requires=">=3.10",
    packages=packages,
    install_requires=requirements,
    include_package_data=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: GIS",
    ],
)
