import os

from setuptools import setup, find_packages

base_packages = ["click==7.0", 'pytest']


setup(
    name='raft',
    packages=find_packages(where='src'),
    package_dir={"": "src"},
    install_requires=base_packages,
    entry_points={
        'console_scripts': [
            'raft = raft.cli:main',
        ]
    },
    description='',
    author='Matthijs Brouns',
)
