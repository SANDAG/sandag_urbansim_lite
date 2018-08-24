## Create column of parcels to decide when a parcel can be developed

import pandas as pd
import numpy as np
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
print('\nNew version_id: {}'.format(version_id))

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
assigned_df['phase_yr'] = 2036
assigned_df['phase_yr_version_id'] = version_id

# The following jurisdictions have agreed to make their ADUs available for "realization" beginning from 2019
# the City of San Diego
# Chula Vista
# Oceanside
# El Cajon
# see https://sandag.atlassian.net/wiki/spaces/LUM/pages/726302736/Additional+Capacity

# assigned_df.loc[((assigned_df.capacity_type=='adu')),'phase_yr'] = 2019

assigned_df.loc[((assigned_df.jur_id==14) & (assigned_df.capacity_type=='adu')),'phase_yr'] = 2019
assigned_df.loc[((assigned_df.jur_id==2) & (assigned_df.capacity_type=='adu')),'phase_yr'] = 2019
assigned_df.loc[((assigned_df.jur_id==12) & (assigned_df.capacity_type=='adu')),'phase_yr'] = 2019
assigned_df.loc[((assigned_df.jur_id==5) & (assigned_df.capacity_type=='adu')),'phase_yr'] = 2019


assigned_df = assigned_df[['phase_yr','phase_yr_version_id','capacity_type']]

# WRITE TO DB
assigned_df.to_sql(name='urbansim_lite_parcel_control', con=mssql_engine, schema='urbansim', index=True,if_exists='append')


sched_dev_parcel_sql = '''
SELECT [site_id]
      ,[parcel_id]
      ,MAX([capacity_3]) AS capacity_3
      ,YEAR([startdate]) AS startdate
      ,YEAR(MAX(DATEADD(DAY,-1,[compdate]))) AS compdate
  FROM [urbansim].[urbansim].[scheduled_development_parcel]
  INNER JOIN [urbansim].[ref].[scheduled_development_site]
  ON site_id = siteid
  WHERE (sfu > 0 OR mfu > 0 OR mhu > 0) AND status != 'completed' AND capacity_3 > 0
  GROUP BY parcel_id, site_id, startdate
  ORDER BY site_id, parcel_id
'''

sched_dev_df = pd.read_sql(sched_dev_parcel_sql, mssql_engine)

# # Selected sites are given manual overrides based on information from Mike Calandra
# Override startdate
sched_dev_df.loc[(sched_dev_df.site_id.isin([1746, 1921, 3395, 3396, 12075])), 'startdate'] = 2017
sched_dev_df.loc[(sched_dev_df.site_id.isin([9009, 10009])), 'startdate'] = 2018
sched_dev_df.loc[(sched_dev_df.site_id.isin([3404, 4014, 14075])), 'startdate'] = 2019
sched_dev_df.loc[(sched_dev_df.site_id.isin([899, 1820, 3389, 3390, 7022, 9008])),'startdate'] = 2020
sched_dev_df.loc[(sched_dev_df.site_id.isin([1750, 7001, 9007, 14000, 14001, 14002, 15005, 15007, 15014, 15015,
                                             15016])), 'startdate'] = 2025
sched_dev_df.loc[(sched_dev_df.site_id.isin([14084, 14085, 14086])), 'startdate'] = 2029
sched_dev_df.loc[(sched_dev_df.site_id.isin([16001])), 'startdate'] = 2030
sched_dev_df.loc[(sched_dev_df.site_id.isin([250])), 'startdate'] = 2035

# Override compdate
sched_dev_df.loc[(sched_dev_df.site_id.isin([9009, 10009])), 'compdate'] = 2018
sched_dev_df.loc[(sched_dev_df.site_id.isin([1820])), 'compdate'] = 2020
sched_dev_df.loc[(sched_dev_df.site_id.isin([1746, 3389, 3390, 3395, 3396, 3404, 14075])), 'compdate'] = 2022
sched_dev_df.loc[(sched_dev_df.site_id.isin([7022, 9008, 12075])), 'compdate'] = 2025
sched_dev_df.loc[(sched_dev_df.site_id.isin([899, 15005])), 'compdate'] = 2025
sched_dev_df.loc[(sched_dev_df.site_id.isin([7001, 9007])), 'compdate'] = 2030
sched_dev_df.loc[(sched_dev_df.site_id.isin([14084, 14085, 14086])), 'compdate'] = 2031
sched_dev_df.loc[(sched_dev_df.site_id.isin([1750, 14000, 14001, 14002, 15007, 15014, 15015, 15016])),
                 'compdate'] = 2035
sched_dev_df.loc[(sched_dev_df.site_id.isin([250, 16001])), 'compdate'] = 2040

# Set parcel_id as index
sched_dev_df.reset_index(inplace=True, drop=False)
sched_dev_df.parcel_id = sched_dev_df.parcel_id.astype(int)
sched_dev_df.set_index('parcel_id',inplace=True)

# Set version_id and label parcels as scheduled developments
sched_dev_df['phase_yr_version_id'] = version_id
sched_dev_df['capacity_type'] = 'sch'

# # Set Priority Rules:
# If site_id has start and complete date, Priority = 1
# If site_id has start date only, Priority = 2
# If site_id has complete date only, Priority = 3
# If Project has no associated dates, Priority = 4
sched_dev_df['priority'] = 4
sched_dev_df.loc[sched_dev_df['compdate'].notnull() & sched_dev_df['startdate'].isnull(), 'priority'] = 3
sched_dev_df.loc[sched_dev_df['compdate'].isnull() & sched_dev_df['startdate'].notnull(), 'priority'] = 2
sched_dev_df.loc[sched_dev_df['compdate'].notnull() & sched_dev_df['startdate'].notnull(), 'priority'] = 1

# Write out new priority version to SQL
sched_dev_priority = sched_dev_df[['phase_yr_version_id', 'site_id', 'capacity_3', 'priority']].astype('int')
sched_dev_priority.rename(columns={"phase_yr_version_id": "sched_version_id"}, inplace=True)
sched_dev_priority.to_sql(name='scheduled_development_priority', con=mssql_engine, schema='urbansim', index=True,
                          if_exists='append')

# # Set phase year rules
# Phase year minimum is 2017
# If project has a start date only, set that year as the phase year
# If project has a complete date only, phase year will be complete date minus 2 minus (number units on site / 250)
# Note: the 250 is the current per-year cap for units per site, as of 8/8/2018, but could change. 2 is added to allow
# for flexibility in how fast the project builds out.
sched_dev_site_cap = sched_dev_df.groupby(['site_id'])[['capacity_3']].sum()
sched_dev_site_cap.rename(columns={"capacity_3": "site_cap"}, inplace=True)
sched_dev_df = pd.merge(sched_dev_df, sched_dev_site_cap, how='left', left_on='site_id', right_index=True)

sched_dev_df['phase_yr'] = 2017
sched_dev_df['phase_yr'].where((sched_dev_df['compdate'].isnull()),
                               other=(sched_dev_df['compdate'] - (sched_dev_df['site_cap']/250) - 2), inplace=True)
sched_dev_df['phase_yr'].where(sched_dev_df['startdate'].isnull(), other=sched_dev_df['startdate'], inplace=True)
sched_dev_df.loc[sched_dev_df['startdate'] > 2049, 'phase_yr'] = (2048-(sched_dev_df['site_cap']/250))
sched_dev_df['phase_yr'].where((sched_dev_df['phase_yr'] > 2017), other=2017, inplace=True)
sched_dev_df['phase_yr'] = sched_dev_df['phase_yr'].apply(np.floor)


# Chula Vista - notes from [urbansim].[ref].[scheduled_development_site]
sched_dev_df.loc[(sched_dev_df.site_id.isin([2018,2020,2021,3060])),'phase_yr'] = 2018


# These phase years come from Rachel Cortes with direct contact with each jurisdiction, and overwrite other assumptions.
sched_dev_df.loc[(sched_dev_df.site_id.isin([10009])),'phase_yr'] = 2018
sched_dev_df.loc[(sched_dev_df.site_id.isin([4014])),'phase_yr'] = 2019
sched_dev_df.loc[(sched_dev_df.site_id.isin([7022,9008])),'phase_yr'] = 2020
sched_dev_df.loc[(sched_dev_df.site_id.isin([1730,1731,14088,15005])),'phase_yr'] = 2025
sched_dev_df.loc[(sched_dev_df.site_id.isin([14084,14085,14086])),'phase_yr'] = 2030
sched_dev_df.loc[(sched_dev_df.site_id.isin([2019,3063,2003,2015,2011,2013,2014,2001])),'phase_yr'] = 2039
sched_dev_df.loc[(sched_dev_df.site_id.isin([15035,15019,15028,15029,15004,15007,15015,15016])),'phase_yr'] = 2036


############################################################################
# DEL MAR RESORT - no residential units - need to update in db
sched_dev_df.loc[(sched_dev_df.site_id==4014),'phase_yr'] = 2051
###########################################################################


sched_dev_df = sched_dev_df[['phase_yr','phase_yr_version_id','capacity_type']]
sched_dev_df[['phase_yr','phase_yr_version_id']] = sched_dev_df[['phase_yr','phase_yr_version_id']].astype('int')

# WRITE TO DB
sched_dev_df.to_sql(name='urbansim_lite_parcel_control', con=mssql_engine, schema='urbansim', index=True,if_exists='append')
