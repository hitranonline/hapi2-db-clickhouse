from setuptools import find_packages, setup

setup(
    name='hapi2_db_clickhouse',
    version='0.1',
    author="Roman Kochanov",
    author_email="",
    description="ClickHouse plugin for HAPI v2",
    url="https://github.com/hitranonline/hapi2-db-clickhouse",
    python_requires=">=3.5",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
)
