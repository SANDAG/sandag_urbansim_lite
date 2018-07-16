## Create column of parcels to decide when a parcel can be developed

import pandas as pd
from sqlalchemy import create_engine
from database import get_connection_string
import utils

db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

scenarios = utils.yaml_to_dict('scenario_config.yaml', 'scenario')


# run_id_sql = '''
# SELECT max(phase_yr_version_id)
#   FROM [urbansim].[urbansim].[urbansim_lite_output]
# '''
# version_id_df = pd.read_sql(run_id_sql, mssql_engine)
#
# if version_id_df.values:
#     version_id = int(version_id_df.values) + 1

# version_id = 112

# Retrieves maximum existing run_id from the table. If none exists, creates run_id = 1.
version_id_sql = '''
  SELECT max(phase_yr_version_id)
  FROM [urbansim].[urbansim].[urbansim_lite_parcel_control]
'''
version_id_df = pd.read_sql(version_id_sql, mssql_engine)
if version_id_df.values:
    version_id = int(version_id_df.values) + 1
else:
    version_id = 1


parcel_sql = '''
      SELECT parcel_id, p.mgra_id, 
             cap_jurisdiction_id,
             jurisdiction_id,
             p.luz_id, p.site_id, capacity_2 AS capacity_base_yr, 
             du_2017 AS residential_units, 
             0 as partial_build
      FROM urbansim.urbansim.parcel p
      WHERE capacity_2 > 0 and site_id is NULL
'''
parcels_df = pd.read_sql(parcel_sql, mssql_engine)

# xref_geography_sql = '''
#     SELECT mgra_13, cocpa_2016, cicpa_13,cocpa_13, jurisdiction_2016,
#            COALESCE(cocpa_2016,cicpa_13,cocpa_13) as CPAs
#       FROM data_cafe.ref.vi_xref_geography_mgra_13'''
# xref_geography_df = pd.read_sql(xref_geography_sql, mssql_engine)
# # simulation output
# parcels_df = pd.merge(parcels_df,xref_geography_df,left_on='mgra_id',right_on='mgra_13',how='left')
# parcels_df.loc[parcels_df.cap_jurisdiction_id == 19,'jcid'] = parcels_df['cocpa_2016']
# parcels_df.loc[parcels_df.cap_jurisdiction_id == 14,'jcid'] = parcels_df['cicpa_13']
# parcels_df['jcid'].fillna(parcels_df['cap_jurisdiction_id'],inplace=True)

# parcels_df.reset_index(inplace=True, drop=False)
# #
# parcels_df.drop_duplicates('parcel_id',inplace=True)
# #
# parcels_df.parcel_id = parcels_df.parcel_id.astype(int)
#
parcels_df.set_index('parcel_id',inplace=True)
#
parcels_df['phase_yr'] = 2017
parcels_df['phase_yr_version_id'] = version_id
parcels_df['capacity_type'] = 'jur'
# parcels_df.loc[parcels_df.cocpa_2016==1904,'phase_yr'] = 2034
parcels_df = parcels_df[['phase_yr','phase_yr_version_id','capacity_type']]

# WRITE TO DB
parcels_df.to_sql(name='urbansim_lite_parcel_control', con=mssql_engine, schema='urbansim', index=True,if_exists='append')


assigned_parcel_sql = '''
SELECT [version_id]
      ,[jur_id]
      ,[parcel_id]
      ,[type] as capacity_type
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
assigned_df['phase_yr'] = 2019
assigned_df['phase_yr_version_id'] = version_id

# The following jurisdictions have agreed to make their ADUs available for "realization" beginning from 2019
# the City of San Diego
# Chula Vista
# Oceanside
# El Cajon
# see https://sandag.atlassian.net/wiki/spaces/LUM/pages/726302736/Additional+Capacity

assigned_df.loc[((assigned_df.capacity_type=='adu')),'phase_yr'] = 2019

assigned_df.loc[((assigned_df.jur_id==14) & (assigned_df.capacity_type=='adu')),'phase_yr'] = 2019
assigned_df.loc[((assigned_df.jur_id==2) & (assigned_df.capacity_type=='adu')),'phase_yr'] = 2019
assigned_df.loc[((assigned_df.jur_id==12) & (assigned_df.capacity_type=='adu')),'phase_yr'] = 2019
assigned_df.loc[((assigned_df.jur_id==5) & (assigned_df.capacity_type=='adu')),'phase_yr'] = 2019


assigned_df = assigned_df[['phase_yr','phase_yr_version_id','capacity_type']]

# WRITE TO DB
assigned_df.to_sql(name='urbansim_lite_parcel_control', con=mssql_engine, schema='urbansim', index=True,if_exists='append')


sched_dev_parcel_sql = '''SELECT  [site_id]
      ,[parcel_id]
      ,[capacity_3]
      ,[sfu_effective_adj]
      ,[mfu_effective_adj]
      ,[mhu_effective_adj]
      ,[notes]
      ,[editor]
  FROM [urbansim].[urbansim].[scheduled_development_parcel]
  where capacity_3 > 0
'''

sched_dev_df = pd.read_sql(sched_dev_parcel_sql, mssql_engine)


sched_dev_df.reset_index(inplace=True, drop=False)

sched_dev_df.parcel_id = sched_dev_df.parcel_id.astype(int)
#
sched_dev_df.set_index('parcel_id',inplace=True)
#
sched_dev_df['phase_yr'] = 2017

sched_dev_df.loc[(sched_dev_df.site_id==15005),'phase_yr'] = 2025
sched_dev_df.loc[(sched_dev_df.site_id.isin([1730,1731,14088])),'phase_yr'] = 2025
sched_dev_df.loc[(sched_dev_df.site_id.isin([14084,14085,14086])),'phase_yr'] = 2030
sched_dev_df.loc[(sched_dev_df.site_id.isin([2019,3063,2003,2015,2011,2013,2014,2001])),'phase_yr'] = 2033
sched_dev_df.loc[(sched_dev_df.site_id.isin([15035,15019,15028,15029,15004,15007,15015,15016])),'phase_yr'] = 2034


sched_dev_df['phase_yr_version_id'] = version_id
sched_dev_df['capacity_type'] = 'sch'
sched_dev_df = sched_dev_df[['phase_yr','phase_yr_version_id','capacity_type']]


# WRITE TO DB
sched_dev_df.to_sql(name='urbansim_lite_parcel_control', con=mssql_engine, schema='urbansim', index=True,if_exists='append')
