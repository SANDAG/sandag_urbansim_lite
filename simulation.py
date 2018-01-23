import models, utils
import orca
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd


utils.initialize_tables()

orca.run([
    "feasibility",
    "residential_developer"
     ], iter_vars=range(2020, 2051))


db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

buildings = orca.get_table('buildings').to_frame()
buildings = buildings.reset_index(drop=False)
buildings = buildings.loc[(buildings['year_built'] > 2016)]
buildings_out = buildings[['parcel_id','residential_units','year_built']].copy()
buildings_out.reset_index(drop=True,inplace=True)

buildings_out.rename(columns = {'year_built': 'year_simulation'},inplace=True)
buildings_out.rename(columns = {'residential_units': 'units_added'},inplace=True)

run_id_sql = '''
SELECT max(run_id)
  FROM [urbansim].[urbansim].[urbansim_lite_output_units]
'''
run_id_df = pd.read_sql(run_id_sql, mssql_engine)
run_id = run_id_df.iloc[0][0]

index_sql = '''
SELECT max(index)
  FROM [urbansim].[urbansim].[urbansim_lite_output_units]
'''
index_df = pd.read_sql(index_sql, mssql_engine)
index_id = run_id_df.iloc[0][0]



buildings_out['run_id'] = run_id + 1

buildings_out.to_csv('data/new_units.csv')

units_by_jur = orca.get_table('uj').to_frame()

units_by_jur.to_csv('data/units_by_jur.csv')
buildings_out.to_sql(name='urbansim_lite_output_units', con=mssql_engine, schema='urbansim', index=True,if_exists='append')

