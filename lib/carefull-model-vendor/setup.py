from setuptools import find_packages, setup

setup(
    name='carefull-model-vendor',
    packages=find_packages(),
    version='0.1.0',
    description='Models for vendor extraction',
    author='Carefull',
    license='MIT',
    install_requires=['carefull-model-common@git+ssh://git@github.com/get-carefull/carefull-core.git@develop#egg=carefull-model-common&subdirectory=lib/carefull-model-common',
                      'python-Levenshtein-wheels', 'simhash', 'fuzzywuzzy']
)
