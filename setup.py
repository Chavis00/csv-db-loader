from setuptools import setup, find_packages
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()


VERSION = '1.0.0'
DESCRIPTION = "Python package that enables customized loading of data from a CSV file into a MySQL database"

setup(
    name='csv_db_loader',
    version=VERSION,
    author='Chavis',
    author_email="chavis.delcourt@gmail.com",
    description=DESCRIPTION,
    packages=find_packages(),
    keywords=['python', 'csv', 'mysql', 'database', 'loader', 'db', 'load', 'csv_db_loader'],
    license='MIT',
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=[
        'mysql-connector-python == 8.0.32'
    ],
    classifiers=[
            "Development Status :: 1 - Planning",
            "Intended Audience :: Developers",
            "Programming Language :: Python :: 3",
            "Operating System :: Unix",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: Microsoft :: Windows",
        ]
)
