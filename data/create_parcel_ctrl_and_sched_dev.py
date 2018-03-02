## Create column of parcels to decide when a parcel can be developed

import pandas as pd
from sqlalchemy import create_engine
from pysandag.database import get_connection_string

db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

parcels_2017_sql = '''
     SELECT	parcelid_2015 as parcel_id, cap_remaining_new AS capacity_base_yr
       FROM urbansim.urbansim.parcel_update_2017 update2017
      WHERE cap_remaining_new > 0 
'''

parcels_2015_sql = '''
     SELECT	parcel_id, capacity AS capacity_base_yr
       FROM urbansim.urbansim.parcel p
      WHERE capacity > 0 
'''

parcels_2015_df = pd.read_sql(parcels_2015_sql, mssql_engine, index_col='parcel_id')
parcels_2017_df = pd.read_sql(parcels_2017_sql, mssql_engine, index_col='parcel_id')

# parcel 2017 update table does not have latest parcel ids for city and county
# combine parcel ids from both tables so code does not break
# when parcel update 2017 table has latest parcel ids w/ capacity.
parcels_df = pd.concat([parcels_2015_df,parcels_2017_df])

parcels_df.reset_index(inplace=True, drop=False)

parcels_df.drop_duplicates('parcel_id',inplace=True)

parcels_df.parcel_id = parcels_df.parcel_id.astype(int)

parcels_df.set_index('parcel_id',inplace=True)

parcels_df['phase'] = 2015
parcels_df['scenario'] = 0
parcels_df = parcels_df[['phase','scenario']]

parcels_df.to_sql(name='urbansim_lite_parcel_control', con=mssql_engine, schema='urbansim', index=True,if_exists='replace')


# create sched dev table
# sched_dev = pd.read_csv('sched_dev.csv')
# sched_dev.to_sql(name='scheduled_development_do_not_use', con=mssql_engine, schema='urbansim', index=False,if_exists='replace')