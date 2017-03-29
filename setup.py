import os

try:
    from setuptools import setup
    extra = dict(entry_points={
            'console_scripts': ['cluster-explore=cluster_config.explore:main',
                                'cluster-generate=cluster_config.generate:main',
                                'cluster-push=cluster_config.push:main',
                                'cluster-generate-push=cluster_config.generate_push:main']
            })
except ImportError:
    from distutils.core import setup
    extra = dict(scripts=["cluster-explore","cluster-generate","cluster-push", "cluster-generate-push"])



setup(
    # Application name:
    name="cluster_config",

    # Version number (initial):
    version=u"0.1.0",

    # Application author details:

    # Packages
    packages=["cluster_config","cluster_config/cdh","cluster_config/tests", "cluster_config/utils"],

    # Include additional files into the package
    include_package_data=True,

    # Details
    #url="https://analyticstoolkit.intel.com",

    #
    license="Apache 2.0",
    description="big data cluster configuration tool",

    long_description=open("README.md").read(),

    # Dependent packages (distributions)
    install_requires=[
        'argparse >= 1.3.0',
        'cm-api >= 9.0.0',
        'pyyaml >= 3.11'
    ],
    **extra
)
