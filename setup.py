"""Setup code"""

from setuptools import find_packages, setup

setup(
    name="climate-cabinet-campaign-finance",
    version="0.1.0",
    packages=find_packages(
        include=[
            "utils",
            "utils.*",
        ]
    ),
    install_requires=[],
)
