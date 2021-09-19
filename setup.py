from setuptools import setup

version='0.0.2'

setup(
    name='simpletask',
    version=version,
    license='MIT',
    author='Vladislav Dudnikov',
    author_email='dudnikov.vladislav@gmail.com',
    description='Simple scheduling of command line tasks',
    install_requires=['pid'],
    packages=[
        'simpletask'
    ],
    entry_points = {
        'console_scripts': [
            'simpletask=simpletask.cli:cli'
        ]
    },
    platforms='any'
)
