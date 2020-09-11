from setuptools import setup
from setuptools import find_packages

setup(name='carefull-alert-common',
      packages=find_packages(),
      install_requires=["structlog"],
      version='0.1')
