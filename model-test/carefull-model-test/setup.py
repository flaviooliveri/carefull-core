from setuptools import find_packages, setup

setup(
    name='carefull-model-test',
    packages=find_packages(),
    version='0.1.0',
    description='Test for models',
    author='Carefull',
    license='MIT',
    install_requires=['carefull-model-common@git+ssh://git@github.com/get-carefull/carefull-core.git@develop#egg=carefull-model-vendor&subdirectory=lib/carefull-model-vendor']
)
