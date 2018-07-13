import models
import utils
import bulk_insert
import orca
from sqlalchemy import create_engine
from database import get_connection_string
from datetime import timedelta
import time
#import pandas as pd
#import numpy as np

# Save the start time to track how long the program takes to run from start to finish
start_time = time.monotonic()

# Link to SQL Server
db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

# Generate run_id and record information about run_id details
print("\n\nWrite results to db?")
write_results_to_db = input("\nChoose y or n: ")
if write_results_to_db == 'y':
      run_id = utils.add_run_to_db()

print("\n\nCreate a new set of subregional controls?")
write_controls = input("\nChoose y or n: ")

# Run the urbansim model iterations (see subsections for details)
orca.run([
    #"scheduled_development_events",
    #"negative_parcel_reducer",
      "subregional_share",
      "feasibility",
      "residential_developer",
      "summary",
      "write_to_sql"
      ], iter_vars=range(2017, 2051))

# for adding control percents

if write_controls == "y":
    utils.create_control_percents()

if write_results_to_db == 'y':
      utils.write_results(run_id)

# Save the end time to track how long the program takes to run from start to finish
end_time = time.monotonic()

# Display total model run time
print("Total time to run Simulation:", timedelta(seconds=round(end_time - start_time, 2)))
# Typical times (as of 06/06/2018):
# # With no SQL parcel tables: 1.5-2 minutes
# # With SQL cap_parcel table: 4-5 minutes
# # With SQL all_parcel table: 40-45 minutes
# # With SQL cap_parcel and all_parcel tables: 45-50 minutes
