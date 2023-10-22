from setuptools import setup, find_packages

setup(
    name='csv_db_loader',
    version='0.1',
    packages=find_packages(),
    license='MIT',
    long_description=open('README.md').read(),
    install_requires=[
        'mysql-connector-python == 8.0.32'
    ]
)
