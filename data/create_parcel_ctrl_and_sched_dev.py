## Create column of parcels to decide when a parcel can be developed

import pandas as pd
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import utils

db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

scenarios = utils.yaml_to_dict('scenario_config.yaml', 'scenario')

parcel_sql = '''
      SELECT parcel_id, p.mgra_id, 
             cap_jurisdiction_id,
             jurisdiction_id,
             p.luz_id, p.site_id, capacity_2 AS capacity_base_yr, 
             du_2017 AS residential_units, 
             0 as partial_build
      FROM urbansim.urbansim.parcel p
      WHERE capacity_2 != 0 and capacity_2 is not null
'''
parcels_df = pd.read_sql(parcel_sql, mssql_engine)

xref_geography_sql = '''
    SELECT mgra_13, cocpa_2016, cicpa_13,cocpa_13, jurisdiction_2016, 
           COALESCE(cocpa_2016,cicpa_13,cocpa_13) as CPAs
      FROM data_cafe.ref.vi_xref_geography_mgra_13'''
xref_geography_df = pd.read_sql(xref_geography_sql, mssql_engine)
# simulation output
parcels_df = pd.merge(parcels_df,xref_geography_df,left_on='mgra_id',right_on='mgra_13',how='left')
parcels_df.loc[parcels_df.cap_jurisdiction_id == 19,'jcid'] = parcels_df['cocpa_2016']
parcels_df.loc[parcels_df.cap_jurisdiction_id == 14,'jcid'] = parcels_df['cicpa_13']
parcels_df['jcid'].fillna(parcels_df['cap_jurisdiction_id'],inplace=True)

parcels_df.reset_index(inplace=True, drop=False)
#
parcels_df.drop_duplicates('parcel_id',inplace=True)
#
parcels_df.parcel_id = parcels_df.parcel_id.astype(int)
#
parcels_df.set_index('parcel_id',inplace=True)
#
parcels_df['phase_yr'] = 2017
parcels_df['phase_yr_version_id'] = 2
parcels_df['type'] = 'jur'
parcels_df.loc[parcels_df.cocpa_2016==1904,'phase_yr'] = 2034
parcels_df = parcels_df[['phase_yr','phase_yr_version_id','type']]


# parcels_df.to_sql(name='urbansim_lite_parcel_control', con=mssql_engine, schema='urbansim', index=True,if_exists='append')
assigned_parcel_sql = '''
SELECT [version_id]
      ,[jur_id]
      ,[parcel_id]
      ,[type]
      ,[name]
      ,[du]
  FROM [urbansim].[urbansim].[additional_capacity]
  where version_id = %s
'''
assigned_parcel_sql = assigned_parcel_sql % scenarios['additional_capacity_version']

assigned_df = pd.read_sql(assigned_parcel_sql, mssql_engine)

assigned_df.reset_index(inplace=True, drop=False)

assigned_df.parcel_id = assigned_df.parcel_id.astype(int)
#
assigned_df.set_index('parcel_id',inplace=True)
#
assigned_df['phase_yr'] = 2035
assigned_df['phase_yr_version_id'] = 2
assigned_df = assigned_df[['phase_yr','phase_yr_version_id','type']]

assigned_df.to_sql(name='urbansim_lite_parcel_control', con=mssql_engine, schema='urbansim', index=True,if_exists='append')


# create sched dev table

#select 10 as scenario,p.parcel_id,2017 as yr, p.site_id ,p.capacity as res_units
#   from urbansim.parcel p
#  where site_id IS NOT NULL and capacity > 0



# sched_dev = pd.read_csv('sched_dev_load2.csv')
# sched_dev.to_sql(name='scheduled_development_do_not_use', con=mssql_engine, schema='urbansim', index=False,if_exists='append')