## Create column of parcels to decide when a parcel can be developed

import pandas as pd
from sqlalchemy import create_engine
from pysandag.database import get_connection_string

db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

parcels_sql = '''
  WITH bldgs_by_parcel AS (SELECT parcel_id, SUM(residential_units) AS residential_units, 
                                  count(building_id) AS num_of_bldgs
                           FROM   urbansim.urbansim.building GROUP BY parcel_id)
  SELECT parcels.parcel_id, parcels.jurisdiction_id, parcels.site_id,
         parcels.capacity AS additional_units, 
         COALESCE(bldgs_by_parcel.residential_units,0) AS residential_units,
         COALESCE(bldgs_by_parcel.num_of_bldgs,0) AS bldgs,
         0 as partial_build
  FROM urbansim.urbansim.parcel parcels
  LEFT JOIN bldgs_by_parcel 
  ON bldgs_by_parcel.parcel_id = parcels.parcel_id
  WHERE parcels.capacity > 0
'''
parcels_df = pd.read_sql(parcels_sql, mssql_engine, index_col='parcel_id')

parcels_df['earliest_dev_year'] = 2015
parcels_df['scenario'] = 0

parcels_df.to_sql(name='urbansim_lite_dev_control', con=mssql_engine, schema='urbansim', index=True,if_exists='replace')