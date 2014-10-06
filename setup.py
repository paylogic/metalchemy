"""Setuptools entry point script."""

import codecs
import os.path

from setuptools import setup, find_packages

import metalchemy

dirname = os.path.dirname(__file__)

long_description = (
    codecs.open(os.path.join(dirname, 'README.rst'), encoding='utf-8').read() + '\n' +
    codecs.open(os.path.join(dirname, 'CHANGES.rst'), encoding='utf-8').read()
)

setup(
    name='metalchemy',
    description='SQLAlchemy hierarchical key/value helper',
    long_description=long_description,
    version=metalchemy.__version__,
    author='Paylogic International',
    author_email='developers@paylogic.com',
    license='MIT',
    url='https://github.com/paylogic/metalchemy',
    install_requires=[
        'SQLAlchemy'
    ],
    packages=find_packages(exclude=['ez_setup', 'tests']),
    dependency_links=[],
    tests_require=['detox'],
    include_package_data=True,
    keywords='sqlalchemy dynamic object fields metadata key/value',
)
