import models, utils, bulk_insert
import orca
from sqlalchemy import create_engine
from database import get_connection_string
import pandas as pd
import sqlalchemy
import datetime
import subprocess
from datetime import timedelta
import time


start_time = time.monotonic()

db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

version_ids = utils.yaml_to_dict('data/scenario_config.yaml', 'scenario')

# add run id first in case two runs going simultaneously
run_description = 'version 106 test'

run_id_sql = '''
SELECT max(run_id)
  FROM [urbansim].[urbansim].[urbansim_lite_output_runs]
'''
run_id_df = pd.read_sql(run_id_sql, mssql_engine)

if run_id_df.values:
    run_id = int(run_id_df.values) + 1
else:
    run_id = 1

subregional_controls = version_ids['subregional_ctrl_id']
target_housing_units = version_ids['demographic_simulation_id']
phase_year = version_ids['parcel_phase_yr']
additional_capacity = version_ids['additional_capacity_version']
scheduled_development = version_ids['sched_dev_version']

last_commit = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).rstrip()

output_records = pd.DataFrame(columns=['run_id','run_date','subregional_controls','target_housing_units','phase_year','additional_capacity','scheduled_development','git','run_description'])

run_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
output_records.loc[run_id] = [run_id,run_date,subregional_controls,target_housing_units,phase_year,additional_capacity,scheduled_development,last_commit,run_description]
output_records.to_sql(name='urbansim_lite_output_runs', con=mssql_engine, schema='urbansim', index=False, if_exists='append')


orca.run([
    "scheduled_development_events",
    "negative_parcel_reducer",
    "feasibility",
    "residential_developer",
    "summary"
     ,"write_to_sql"
     ], iter_vars=range(2017, 2051))


# write to database
hu_forecast = orca.get_table('hu_forecast').to_frame()
hu_forecast_out = hu_forecast[['parcel_id','units_added','year_built','source','capacity_type']].copy()
hu_forecast_out.rename(columns = {'year_built': 'year_simulation'},inplace=True)
hu_forecast_out.rename(columns = {'units_added': 'unit_change'},inplace=True)
hu_forecast_out['run_id'] = run_id
hu_forecast_out.to_csv('data/new_units.csv')


hu_forecast_out.to_sql(name='urbansim_lite_output', con=mssql_engine, schema='urbansim', index=False,if_exists='append',
                   dtype = {'parcel_id': sqlalchemy.types.INTEGER(),'unit_change': sqlalchemy.types.INTEGER(),
                            'year_simulation': sqlalchemy.types.INTEGER(), 'source': sqlalchemy.types.INTEGER(),
                            'capacity_type': sqlalchemy.types.VARCHAR(length=50),
                            'run_id': sqlalchemy.types.INTEGER()})
end_time = time.monotonic()
print("Total time to run Simulation:", timedelta(seconds=end_time - start_time))
