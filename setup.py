from setuptools import setup

setup(name='h5plexos',
      version='0.4.2',
      packages=['h5plexos',
                'h5plexos.process',
                'h5plexos.query'],
      entry_points={
          'console_scripts': [
              'h5plexos = h5plexos.__main__:main'
          ]
      },
      )
