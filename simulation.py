import models, utils
import orca
from sqlalchemy import create_engine
from pysandag.database import get_connection_string

orca.run([
    "feasibility",               # compute development feasibility
    "residential_developer"     # build residential buildings
     ], iter_vars=range(2016, 2051))

db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)


buildings = orca.get_table('buildings').to_frame()
buildings = buildings.reset_index(drop=False)
buildings = buildings.loc[(buildings['building_id'] > 2889578)]
buildings['run_id'] = 1
buildings.to_sql(name='buildings_output', con=mssql_engine, schema='dbo', if_exists='append', index=False)