from setuptools import find_packages, setup

setup(
    name="2024-winter-clinic-climate-cabinet",
    version="0.1.0",
    packages=find_packages(
        include=[
            "utils",
            "utils.*",
        ]
    ),
    install_requires=[],
)
