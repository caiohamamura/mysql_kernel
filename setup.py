import os
import sys
import json

from setuptools import setup, find_packages


def readme():
    with open('README.md', encoding='utf-8') as f:
        return f.read()

setup(name='mysql_kernel',
      version='0.5.1',
      description='A generic kernel for Jupyter forked from JinQing Lees mysql_kernel',
      long_description=readme(),
      long_description_content_type='text/markdown',
      url='https://github.com/Hourout/mysql_kernel',
      author='Caio Hamamura',
      author_email='caiohamamura@gmail.com',
      keywords=['jupyter_kernel', 'mysql_kernel'],
      license='Apache License Version 2.0',
      install_requires=['pymysql', 'sqlalchemy', 'pandas', 'jupyter','pygments>=2.12'],
      classifiers = [
          'Framework :: IPython',
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Intended Audience :: Developers',
          'Intended Audience :: Education',
          'Intended Audience :: Science/Research',
          'Topic :: Scientific/Engineering',
          'Topic :: System :: Shells',
      ],
      packages=['mysql_kernel'],
      include_package_data=True,
      zip_safe=False
)
