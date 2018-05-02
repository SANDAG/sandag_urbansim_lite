import models, utils, bulk_insert
import orca
from sqlalchemy import create_engine
from database import get_connection_string
# from pysandag.database import get_connection_string
import pandas as pd
import sqlalchemy
import datetime
import subprocess
from datetime import timedelta
import time


start_time = time.monotonic()

db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

orca.run([
    "scheduled_development_events",
    "negative_parcel_reducer",
    "feasibility",
    "residential_developer",
    "summary"
    #,"write_to_sql"
     ], iter_vars=range(2017, 2051))

hu_forecast = orca.get_table('hu_forecast').to_frame()
hu_forecast = hu_forecast.reset_index(drop=False)
hu_forecast = hu_forecast.loc[(hu_forecast['year_built'] > 2016)]
hu_forecast['source'] = hu_forecast['source'].astype(str)
hu_forecast_out = hu_forecast[['parcel_id','units_added','year_built','source','capacity_type']].copy()
hu_forecast_out.reset_index(drop=True,inplace=True)
hu_forecast_out.rename(columns = {'year_built': 'year_simulation'},inplace=True)
hu_forecast_out.rename(columns = {'units_added': 'unit_change'},inplace=True)

run_id_sql = '''
SELECT max(run_id)
  FROM [urbansim].[urbansim].[urbansim_lite_output]
'''
run_id_df = pd.read_sql(run_id_sql, mssql_engine)

if run_id_df.values:
    run_id = int(run_id_df.values) + 1
else:
    run_id = 1

hu_forecast_out['run_id'] = run_id
hu_forecast_out.to_csv('data/new_units.csv')
scenarios = utils.yaml_to_dict('data/scenario_config.yaml', 'scenario')
subregional_ctrl_id = scenarios['subregional_ctrl_id']
housing_units_version_id = scenarios['demographic_simulation_id']
phase_yr_id = scenarios['parcel_phase_yr']
additional_capacity_version_id = scenarios['additional_capacity_version']

last_commit = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).rstrip()

output_records = pd.DataFrame(columns=['run_id', 'run_description', 'run_date','subregional_ctrl_id',\
                                       'housing_units_version_id','phase_yr_id','additional_capacity_version_id','git'])
run_description = 'sch dev phasing from urbansim.scheduled_development_do_not_use'
run_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
output_records.loc[run_id] = [run_id, run_description, run_date,subregional_ctrl_id,\
                              housing_units_version_id,phase_yr_id,additional_capacity_version_id,last_commit]
# output_records.to_sql(name='urbansim_lite_output_runs', con=mssql_engine, schema='urbansim', index=False, if_exists='append')
#
# hu_forecast_out.to_sql(name='urbansim_lite_output', con=mssql_engine, schema='urbansim', index=False,if_exists='append',
#                    dtype = {'parcel_id': sqlalchemy.types.INTEGER(),'unit_change': sqlalchemy.types.INTEGER(),
#                             'year_simulation': sqlalchemy.types.INTEGER(), 'source': sqlalchemy.types.VARCHAR(length=50),
#                             'capacity_type': sqlalchemy.types.VARCHAR(length=50),
#                             'run_id': sqlalchemy.types.INTEGER()})
end_time = time.monotonic()
print("Total time to run Simulation:", timedelta(seconds=end_time - start_time))

# ], iter_vars = range(2017, 2051), data_out = 'data\\results.h5', out_interval = 1)
    ## data_out writes output to .h5 every year (change out interval for increment)
    ## to view contents of out file
    # import h5py
    # filename = 'data\\results.h5'
    # f = h5py.File(filename, 'r')
    # print("Keys: %s" % f.keys())
    # a_group_key = list(f.keys())[0]
    # data = list(f[a_group_key])