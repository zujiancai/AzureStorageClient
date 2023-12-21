from setuptools import setup

from batch_job import __version__


setup(
    name='minimal_batch_job',
    version=__version__,

    url='https://github.com/zujiancai/BatchJob',
    author='Zujian Cai',
    author_email='jixproject@gmail.com',

    py_modules=['batch_job'],

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