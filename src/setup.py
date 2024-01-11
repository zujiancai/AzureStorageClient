from setuptools import setup, find_packages

from batch_job import __version__


setup(
    name='minimal-batch-job',
    version=__version__,

    url='https://github.com/zujiancai/minimal-batch-job',
    author='Zujian Cai',
    author_email='jixproject@gmail.com',

    packages=find_packages(exclude=['tests', 'tests.*']),

    install_requires=[
        'azure-data-tables',
        'azure-storage-blob'
    ],

    classifiers=[
        'Intended Audience :: Developers',

        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ],
)