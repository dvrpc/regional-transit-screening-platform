from setuptools import find_packages, setup

setup(
    name='regional_transit_screening_platform',
    packages=find_packages(),
    version='0.1.0',
    description='Python and SQL code to create data products related to transit service in the DVRPC region',
    author='Aaron Fraint, AICP',
    license='MIT',
    entry_points="""
        [console_scripts]
        rtsp=regional_transit_screening_platform.cli:main
    """,
)