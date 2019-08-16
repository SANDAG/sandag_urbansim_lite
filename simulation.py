import utils
import orca
from sqlalchemy import create_engine
from database import get_connection_string
from datetime import timedelta
import time
import models
# import bulk_insert

# Save the start time to track how long the program takes to run from start to finish
start_time = time.monotonic()

# Link to SQL Server
db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

print("\n\nAttempt to match former run?")
# match_results = input("\nChoose y or n: ")
match_results = "n"
if match_results == 'y':
    orca.run(["match_prior_run"])

# Run the urbansim model iterations (see subsections for details)
orca.run([
    "feasibility",
    "scheduled_development_events",
    "residential_developer",
    "summary"
    ], iter_vars=range(2018, 2051))

# Save the end time to track how long the program takes to run from start to finish
end_time = time.monotonic()

# Display total model run time
print("Total time to run Simulation:", timedelta(seconds=round(end_time - start_time, 2)))

# Generate run_id and record information about run_id details
print("\n\nWrite results to db?")
write_results_to_db = input("\nChoose y or n: ")
# write_results_to_db = "n"
if write_results_to_db == 'y':
    run_id = utils.add_run_to_db()
    utils.write_results(run_id)

hu_forecast = orca.get_table('hu_forecast').to_frame()
hu_forecast_out = hu_forecast[['parcel_id', 'units_added', 'year_built', 'source', 'capacity_type']].copy()
hu_forecast_out.rename(columns={'year_built': 'year_simulation'}, inplace=True)
hu_forecast_out.rename(columns={'units_added': 'unit_change'}, inplace=True)
hu_forecast_out.to_csv('data/new_units.csv')

