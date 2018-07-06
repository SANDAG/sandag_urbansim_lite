import models
import utils
import bulk_insert
import orca
from sqlalchemy import create_engine
from database import get_connection_string
import sqlalchemy
from datetime import timedelta
import time
import pandas as pd
import numpy as np

# Save the start time to track how long the program takes to run from start to finish
start_time = time.monotonic()

# Link to SQL Server
db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

# Generate run_id and record information about run_id details
run_id = utils.add_run_to_db()

# Run the urbansim model iterations (see subsections for details)
orca.run([
    #"scheduled_development_events",
    #"negative_parcel_reducer",
    #"subregional_share"
     "feasibility",
     "residential_developer",
     "summary",
     "write_to_sql"
      ], iter_vars=range(2017, 2051))

# for adding control percents
#utils.create_control_percents()

# Write the output of the model to SQL
hu_forecast = orca.get_table('hu_forecast').to_frame()
hu_forecast_out = hu_forecast[['parcel_id', 'units_added', 'year_built', 'source', 'capacity_type']].copy()
hu_forecast_out.rename(columns={'year_built': 'year_simulation'}, inplace=True)
hu_forecast_out.rename(columns={'units_added': 'unit_change'}, inplace=True)
hu_forecast_out['run_id'] = run_id
hu_forecast_out.to_csv('data/new_units.csv')

hu_forecast_out.to_sql(name='urbansim_lite_output', con=mssql_engine, schema='urbansim', index=False,
                       if_exists='append', dtype={'parcel_id': sqlalchemy.types.INTEGER(),
                                                  'unit_change': sqlalchemy.types.INTEGER(),
                                                  'year_simulation': sqlalchemy.types.INTEGER(),
                                                  'source': sqlalchemy.types.INTEGER(),
                                                  'capacity_type': sqlalchemy.types.VARCHAR(length=50),
                                                  'run_id': sqlalchemy.types.INTEGER()})

# Save the end time to track how long the program takes to run from start to finish
end_time = time.monotonic()

# Display total model run time
print("Total time to run Simulation:", timedelta(seconds=round(end_time - start_time, 2)))
# Typical times (as of 06/06/2018):
# # With no SQL parcel tables: 1.5-2 minutes
# # With SQL cap_parcel table: 4-5 minutes
# # With SQL all_parcel table: 40-45 minutes
# # With SQL cap_parcel and all_parcel tables: 45-50 minutes
