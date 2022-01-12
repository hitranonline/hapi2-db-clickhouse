from setuptools import setup
#from hapi2 import __version__

setup(
    name='hapi2_db_clickhouse',
    #version=__version__,
    version='0.1',
    packages=[
        'hapi2_db_clickhouse',
        'hapi2_db_clickhouse.sqlalchemy',
        'hapi2_db_clickhouse.sqlalchemy.clickhouse',
    ],
    #license='BSD-2',
)
