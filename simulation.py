import models, utils, bulk_insert
import orca
from sqlalchemy import create_engine
from database import get_connection_string
import pandas as pd
import sqlalchemy
from datetime import timedelta
import time


start_time = time.monotonic()

db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

run_id = utils.add_run_to_db()

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
