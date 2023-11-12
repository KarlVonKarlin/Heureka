"""
Setup of heureka packages.
"""

from setuptools import setup, find_packages

setup(
    name='heureka',
    version='1.0.0',
    author='Karel Zeman',
    packages=find_packages(where='src'),
    package_dir={
        '':'src'
        },
    include_package_data=True,
    data_files=['src/producer/mock_offers.json'],
    install_requires=(
        'rabbitmq',
        'pika',
        'psycopg2'
    )
)