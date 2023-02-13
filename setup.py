"""Install packages as defined in this file into the Python environment."""
from typing import Any, Dict
from setuptools import setup, find_namespace_packages

# The version of this tool is based on the following steps:
# https://packaging.python.org/guides/single-sourcing-package-version/
VERSION: Dict[str, Any] = {}

with open("./footballdashboardsdata/version.py") as fp:
    # pylint: disable=W0122
    exec(fp.read(), VERSION)

setup(
    name="footballdashboardsdata",
    author="Dmitry Mogilevsky",
    author_email="dmitry.mogilevsky@gmail.com",
    description="Football Dashboards",
    version=VERSION.get("__version__", "0.0.0"),
    package_dir={".": "."},
    packages=find_namespace_packages(where="."),
    install_requires=[
        "setuptools>=45.0",
        "pandas",
        "dbconnect @ git+http://github.com/dmoggles/dbconnect",
        
    ],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Programming Language :: Python :: 3.0",
        "Topic :: Utilities",
    ],
    package_data={"": ["*.ttf"]},
)
