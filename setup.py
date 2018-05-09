from setuptools import setup

setup(name='h5plexos',
      version='0.2.0',
      packages=['h5plexos'],
      entry_points={
          'console_scripts': [
              'h5plexos = h5plexos.__main__:main'
          ]
      },
      )
