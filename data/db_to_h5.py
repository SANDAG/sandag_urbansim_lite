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
         COALESCE(bldgs_by_parcel.num_of_bldgs,0) AS bldgs
  FROM urbansim.urbansim.parcel parcels
  LEFT JOIN bldgs_by_parcel 
  ON bldgs_by_parcel.parcel_id = parcels.parcel_id
  WHERE parcels.capacity > 0
'''

households_sql = '''
  SELECT yr AS year
        ,sum(households) AS hh
  FROM urbansim.urbansim.household_control
  GROUP BY yr
  ORDER BY yr
'''

buildings_sql = '''
  SELECT building_id
        ,parcel_id
        ,COALESCE(development_type_id,0) AS building_type_id
        ,COALESCE(residential_units,0) AS residential_units
        ,COALESCE(year_built,0) AS year_built
        ,0 as random_prob 
        ,0 as random
  FROM urbansim.urbansim.building
'''

units_control_sql = '''
  SELECT residential_control_id
        ,scenario
        ,yr
        ,geo
        ,geo_id
        ,control
        ,control_type
  FROM urbansim.urbansim.residential_control
  WHERE scenario = 1
'''

jurisdictions_sql = '''
  SELECT jurisdiction_id, name
  FROM urbansim.ref.jurisdiction
'''

households_df = pd.read_sql(households_sql, mssql_engine, index_col='year')
buildings_df = pd.read_sql(buildings_sql, mssql_engine, index_col='building_id')
parcels_df = pd.read_sql(parcels_sql, mssql_engine, index_col='parcel_id')
parcels_df['total_cap'] = parcels_df['additional_units'] + parcels_df['residential_units']
units_control_df = pd.read_sql(units_control_sql, mssql_engine)
units_control_df['control_type'] = units_control_df['control_type'].astype(str)
units_control_df['geo'] = units_control_df['geo'].astype(str)
jurisdictions_df = pd.read_sql(jurisdictions_sql, mssql_engine)
jurisdictions_df['name'] = jurisdictions_df['name'].astype(str)

with pd.HDFStore('urbansim.h5', mode='w') as store:
    store.put('parcels', parcels_df, format='table')
    store.put('households',households_df,format='table')
    store.put('buildings', buildings_df, format='table')
    store.put('unit_controls', units_control_df, format='table')
    store.put('jurisdictions', jurisdictions_df, format='table')