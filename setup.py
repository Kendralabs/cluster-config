import os

try:
    from setuptools import setup
    extra = dict(entry_points={
           #'paste.app_factory': ['main=pypiserver:paste_app_factory'],
            'console_scripts': ['cluster-config=cluster_config.config:main',
                                'cluster-generate=cluster_config.auto_config:main',
                               'cluster-explore=cluster_config.explore:main',
                                'cluster-push=cluster_config.push:main']
            })
except ImportError:
    from distutils.core import setup
    extra = dict(scripts=["cluster-config","cluster-generate","cluster-explore", "cluster-push"])



setup(
    # Application name:
    name="cluster_config",

    # Version number (initial):
    version=u"0.1.0-{0}".format(os.environ.get("BUILD_NUMBER")) if os.environ.get("BUILD_NUMBER") else u"0.1.0",

    # Application author details:
    author="Intel",
    author_email="bleh@intel.com",

    # Packages
    packages=["cluster_config","cluster_config/cdh","cluster_config/tests"],

    # Include additional files into the package
    include_package_data=True,

    # Details
    url="https://analyticstoolkit.intel.com",

    #
    license="LICENSE.txt",
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