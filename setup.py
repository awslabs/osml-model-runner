import os

from setuptools import find_packages, setup

# Declare your non-python data files:
# Files underneath configuration/ will be copied into the build preserving the
# subdirectory structure if they exist.

setup(
    name="ModelRunnerContainer",
    version="1.0",
    # declare your packages
    packages=find_packages(where="src", exclude=("test",)),
    package_dir={"": "src"},
    scripts=["bin/mr-entry-point.py"],
    root_script_source_version="python3.8",
    default_python="python3.8"
)
