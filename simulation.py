import models, utils
import orca
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd

units_per_j = pd.DataFrame()
orca.add_table("uj", units_per_j)

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

run_id_sql = '''
SELECT max(run_id)
  FROM [urbansim].[urbansim].[urbansim_lite_output_units]
'''
run_id_df = pd.read_sql(run_id_sql, mssql_engine)
run_id = run_id_df.iloc[0][0]

buildings_out['run_id'] = run_id + 1

buildings_out.to_csv('data/buildings.csv')

units_by_jur = orca.get_table('uj').to_frame()

units_by_jur.to_csv('data/units_by_jur.csv')
#buildings_out.to_sql(name='urbansim_lite_output_units', con=mssql_engine, schema='urbansim', if_exists='append', index=False)
