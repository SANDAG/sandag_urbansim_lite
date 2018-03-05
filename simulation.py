import models, utils
import orca
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd
import sqlalchemy
import datetime


utils.initialize_tables()

orca.run([
    "scheduled_development_events",
    "feasibility",
    "residential_developer",
    "summary"
     ], iter_vars=range(2017, 2051))


db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

buildings = orca.get_table('buildings').to_frame()
buildings = buildings.reset_index(drop=False)
buildings = buildings.loc[(buildings['year_built'] > 2016)]
buildings_out = buildings[['parcel_id','residential_units','year_built','source']].copy()
buildings_out.reset_index(drop=True,inplace=True)

buildings_out.rename(columns = {'year_built': 'year_simulation'},inplace=True)
buildings_out.rename(columns = {'residential_units': 'units_added'},inplace=True)

run_id_sql = '''
SELECT max(run_id)
  FROM [urbansim].[urbansim].[urbansim_lite_output]
'''
run_id_df = pd.read_sql(run_id_sql, mssql_engine)
try:
    run_id = int(run_id_df.values) + 1
except:
    run_id = int(1)

buildings_out['run_id'] = run_id

buildings_out.to_csv('data/new_units.csv')

units_by_jur = orca.get_table('uj').to_frame()

units_by_jur.to_csv('data/units_by_jur.csv')


# output_records = pd.DataFrame(columns=['run_id', 'run_description', 'run_date'])
# run_description = 'Test sched_dev; jur=14,19 cap not updated; jur=8 max_units 100/year and phase yr for desert'
# run_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
# output_records.loc[run_id] = [run_id, run_description, run_date]
# output_records.to_sql(name='urbansim_lite_output_runs', con=mssql_engine, schema='urbansim', index=False, if_exists='append')
#
# buildings_out.to_sql(name='urbansim_lite_output', con=mssql_engine, schema='urbansim', index=False,if_exists='append',
#                    dtype = {'parcel_id': sqlalchemy.types.INTEGER(),'units_added': sqlalchemy.types.INTEGER(),
#                             'year_simulation': sqlalchemy.types.INTEGER(), 'source': sqlalchemy.types.VARCHAR(length=50),
#                             'run_id': sqlalchemy.types.INTEGER()})

