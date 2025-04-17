from setuptools import setup, find_packages

setup(
    name="my_package",
    version="0.1.0",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "my_package": ["scala/*.jar"],
    },
)