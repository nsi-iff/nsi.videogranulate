from setuptools import setup, find_packages
import sys, os

version = '1.0.1'

setup(name='nsi.videogranulate',
      version=version,
      description="A webservice to granulate videos.",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='video granulate webservice python cyclone nsi',
      author='Eduardo Braga Ferreira Junior',
      author_email='ebfjunior@gmail.com',
      url='',
      license='GPL',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      dependency_links=['http://newton.iff.edu.br/pypi/'],
      include_package_data=True,
      zip_safe=True,
      install_requires=[
         'setuptools',
         'simplejson',
         'lxml',
         'httplib2',
         'cyclone',
         'twisted',
         'nsi.granulate',
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
