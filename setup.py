from setuptools import setup
#from hapi2 import __version__

setup(
    name='hapi2_db_clickhouse',
    #version=__version__,
    version='0.1',
    packages=[
        'hapi2_db_clickhouse',        
        'hapi2_db_clickhouse.db',
        'hapi2_db_clickhouse.db.sqlalchemy',
        'hapi2_db_clickhouse.db.sqlalchemy.clickhouse',
        'hapi2_db_clickhouse.format',        
    ],
    #license='BSD-2',
)
