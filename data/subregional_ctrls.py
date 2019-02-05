import pandas as pd
from sqlalchemy import create_engine
from database import get_connection_string
import utils

db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

versions = utils.yaml_to_dict('../data/scenario_config.yaml', 'scenario')

# This file produces subregional controls based on the SR13 final allocation totals.

# note: including sr13 sched dev in sr13 capacities in order to match development pattern
# regardless of whether development pattern from sched dev or not.

# note: exclude sr14 sched dev bc control percentages are for jur provided capacity
# sr14 sched dev occurs regardless of control percentages. sched dev happens in first increment or
# based on dates provided.

# Units needed
units_needed_sql = '''
SELECT [yr], [version_id], [housing_units_add] as sr14hu
  FROM [urbansim].[urbansim].[urbansim_target_housing_units]
  WHERE version_id = %s'''

units_needed_sql = units_needed_sql % versions['target_housing_units_version']
hu_df = pd.read_sql(units_needed_sql, mssql_engine)
total_units_needed = int(hu_df['sr14hu'].sum())
print(total_units_needed)

sr13_sql = '''	
select x.mgra, sum([hs]) AS hs, increment, city, cpa, x.luz as luz_id,site
from [regional_forecast].[sr13_final].[capacity] x
join [regional_forecast].[sr13_final].[mgra13] y
on x.mgra = y.mgra
where scenario = 0 
group by x.mgra, site, increment, y.city,y.cpa,x.luz
order by x.mgra, increment'''
sr13_df = pd.read_sql(sr13_sql, mssql_engine)

# len(sr13_df.mgra.unique())
# 23,002 mgras

# check results against:
# file:///M:\RES\GIS\Spacecore\sr14GISinput\LandUseInputs\SR13_SR14_CapacityAllocationRatio.xlsx
sr13summary = pd.DataFrame({'hs_sum': sr13_df.groupby(['increment']).hs.sum()}).reset_index()
sr13summary['chg'] = sr13summary.hs_sum.diff().fillna(0).astype(int)
sr13summary['chg%'] = (sr13summary.hs_sum.pct_change().fillna(0) * 100).round(2)
sr13summary.set_index('increment', inplace=True)
print(sr13summary)

# sr13summary.chg.sum()

sr13_df['jcid'] = sr13_df.city
sr13_df.loc[(sr13_df.city == 14) | (sr13_df.city == 19), 'jcid'] = sr13_df['cpa']

sr13_jcid_df = pd.DataFrame({'hs_sum': sr13_df.groupby(['jcid', 'increment']).hs.sum()}).reset_index()

# len(sr13_jcid_df.jcid.unique())
# 103

sr13_pivot = sr13_jcid_df.pivot(index='jcid', columns='increment',
                                values='hs_sum').reset_index().rename_axis(None, axis=1)
# sr13_pivot.set_index('jcid', inplace=True)

sr13_chg = sr13_pivot.diff(axis=1)

sr13_chg.drop([2012], axis=1, inplace=True)
sr13_chgst = sr13_chg.stack().to_frame()
sr13_chgst.reset_index(inplace=True, drop=False)

sr13_chgst.rename(columns={'level_1': 'yr_to', 0: 'hu_change'}, inplace=True)
sr13_chgst['yr_from'] = sr13_chgst['yr_to'] - 5
sr13_chgst.loc[sr13_chgst.yr_from == 2010, 'yr_from'] = 2012
sr13_chgst = sr13_chgst[['jcid', 'yr_from', 'yr_to', 'hu_change']].copy()

parcel_sql = '''
    SELECT p.parcel_id,p.capacity_2 AS capacity
      FROM urbansim.urbansim.parcel p 
      WHERE capacity_2 > 0 and site_id IS NULL'''
hs = pd.read_sql(parcel_sql, mssql_engine)

# assigned_parcel_sql = '''
# SELECT  a.parcel_id, a.du as capacity, a.type
#    FROM [urbansim].[urbansim].[additional_capacity] a
#    JOIN urbansim.parcel p on p.parcel_id = a.parcel_id
#   WHERE version_id = %s'''
# assigned_parcel_sql = assigned_parcel_sql % versions['additional_capacity_version']
# assigned_df = pd.read_sql(assigned_parcel_sql, mssql_engine)
# sgoa_assigned = assigned_df.loc[assigned_df.type.isin(['mc', 'tco', 'uc', 'tc','cc'])].copy()
# sgoa_assigned.drop(columns=['type'],inplace=True)
#
# sgoa_and_jur = pd.concat([sgoa_assigned,hs])

lookup_sql = '''
SELECT parcel_id,jur_id,cpa_id
FROM [isam].[xpef04].[parcel2015_mgra_jur_cpa] 
WHERE  i=1'''
lookup_df = pd.read_sql(lookup_sql, mssql_engine)
lookup_df['jcpa'] = lookup_df['jur_id']
lookup_df.loc[lookup_df.jur_id == 14, 'jcpa'] = lookup_df['cpa_id']
lookup_df.loc[lookup_df.jur_id == 19, 'jcpa'] = lookup_df['cpa_id']

# update to jcpa view
cocpa_names_sql = '''
    SELECT zone as cpa_id, name as cocpa
    FROM data_cafe.ref.geography_zone WHERE geography_type_id = 20'''
cocpa_names = pd.read_sql(cocpa_names_sql, mssql_engine)
cicpa_names_sql = '''
    SELECT zone as cpa_id, name as cicpa
    FROM data_cafe.ref.geography_zone WHERE geography_type_id = 15'''
cicpa_names = pd.read_sql(cicpa_names_sql, mssql_engine)

# jur_name
jur_name_sql = '''SELECT [jurisdiction_id] as jur_id,[name] as jur_name FROM [urbansim].[ref].[jurisdiction]'''
jur_name = pd.read_sql(jur_name_sql, mssql_engine)
lookup_df = pd.merge(lookup_df, cocpa_names, on='cpa_id', how='left')
lookup_df = pd.merge(lookup_df, cicpa_names, on='cpa_id', how='left')
lookup_df = pd.merge(lookup_df, jur_name, on='jur_id', how='left')
lookup_df['jcpa_name'] = lookup_df['jur_name']
lookup_df.loc[lookup_df.jur_id == 14, 'jcpa_name'] = lookup_df['cicpa']
lookup_df.loc[lookup_df.jur_id == 19, 'jcpa_name'] = lookup_df['cocpa']
lookup_df.drop(columns=['cocpa', 'cicpa', 'jur_name', 'jur_id', 'cpa_id'], inplace=True)

sr14units = pd.merge(hs, lookup_df, on='parcel_id')
sr14units.rename(columns={"jcpa": "jcid"}, inplace=True)
sr14units.fillna(0, inplace=True)

# check for missing jcid (where jcid = 0)
# sr14units.loc[sr14units.jcid==0]

sr14units['jcid'] = sr14units['jcid'].astype(int)

sr14capacity = pd.DataFrame({'sr14c': sr14units.groupby(['jcid']).capacity.sum()}).reset_index()
sr14capacity['jcid'] = sr14capacity['jcid'].astype(int)

sr13capacity = pd.DataFrame({'sr13c': sr13_chgst.groupby(['jcid']).hu_change.sum()}).reset_index()

sx = pd.merge(sr13capacity, sr14capacity, left_on='jcid', right_on='jcid', how='outer')
print(sx.sr13c.sum() - sx.sr14c.sum())

sx['adj_jcid_cap'] = sx['sr14c']/sx['sr13c']

# sr13 yearly
sr13a = sr13_chgst.reindex(sr13_chgst.index.repeat(sr13_chgst.yr_to - sr13_chgst.yr_from)).reset_index(drop=True)

sr13a['yrs'] = sr13a.yr_to - sr13a.yr_from
sr13a['units'] = (sr13a['hu_change']/sr13a['yrs'])
sr13a['yr'] = sr13a.yr_from + 1

# modify the year column to increment by one, rather than repeat by period
while any(sr13a.duplicated()):  # checks for duplicate rows
    sr13a['ym'] = sr13a.duplicated()  # create a boolean column = True if row is a repeat
    sr13a.loc[sr13a.ym == True, 'yr'] = sr13a.yr + 1
del sr13a['ym']

sr13a = sr13a.loc[sr13a.yr > 2016].copy()

sr13a.loc[(sr13a.hu_change < 0), 'hu_change'] = 0
sr13a.loc[(sr13a.units < 0), 'units'] = 0

sr13aa = pd.merge(sr13a[['jcid', 'yr', 'units']], sx, left_on='jcid', right_on='jcid', how='outer')

sr13aa['units_adj1'] = sr13aa['units'] * sr13aa['adj_jcid_cap']
# sr13aa.loc[((sr13aa.jcid==j) & (sr13aa.yr<y))].units_adj1.sum()

sr13tosr14cap = pd.DataFrame({'unitsum1': sr13aa.groupby(['yr']).units_adj1.sum()}).reset_index()
sr14hu_sr13hu = pd.merge(sr13tosr14cap, hu_df, left_on='yr', right_on='yr', how='outer')
sr14hu_sr13hu['adj_forecast_hs'] = sr14hu_sr13hu['sr14hu']/sr14hu_sr13hu['unitsum1']
sr13tosr14 = pd.merge(sr13aa, sr14hu_sr13hu[['yr', 'unitsum1', 'adj_forecast_hs']],
                      left_on='yr', right_on='yr', how='outer')
sr13tosr14['units_adj2'] = (sr13tosr14['units_adj1'] * sr13tosr14['adj_forecast_hs'])

# sr13tosr14.loc[((sr13tosr14.jcid==j) & (sr13tosr14.yr<y))].units_adj1.sum()

sr14checkunits = pd.DataFrame({'units_for_sr14': sr13tosr14.groupby(['yr']).units_adj2.sum()}).reset_index()

sr14checkunits = pd.merge(sr14checkunits, hu_df, left_on='yr', right_on='yr', how='outer')

sr14checkunits['diff'] = sr14checkunits.units_for_sr14 - sr14checkunits.sr14hu
# print(sr14checkunits.loc[sr14checkunits['diff'] > 0.1])

ctrls = pd.merge(sr13tosr14, sr14checkunits[['yr', 'units_for_sr14']], left_on='yr', right_on='yr', how='outer')

ctrls['control'] = ctrls['units_adj2']/ctrls['units_for_sr14']
controlsummary = pd.DataFrame({'totunits': ctrls.groupby(['jcid', 'sr14c']).units_adj2.sum()}).reset_index()
# sr13summary = pd.DataFrame({'hs_sum': sr13_df.groupby(['increment']).hs.sum()}).reset_index()
ctrls.drop(['units', 'unitsum1', 'units_adj1', 'adj_forecast_hs'], axis=1, inplace=True)

ctrls.fillna(0, inplace=True)

# Retrieves maximum existing run_id from the table. If none exists, creates run_id = 1.
version_id_sql = '''
  SELECT max(subregional_crtl_id)
  FROM [urbansim].[urbansim].[urbansim_lite_subregional_control]
'''

version_id_df = pd.read_sql(version_id_sql, mssql_engine)
v = int(version_id_df.values) + 1

ctrls['subregional_crtl_id'] = v
print('version_id:')
print(v)
ctrls['geo_id'] = ctrls['jcid']
ctrls['max_units'] = None
ctrls['geo'] = 'jur_and_cpa'
ctrls['scenario_desc'] = 'capacity_2'
ctrls['control_type'] = 'percentage'

ctrls.loc[ctrls.control < 0, 'control'] = 0
ctrls = ctrls.loc[ctrls.yr != 0].copy()

controlto1 = pd.DataFrame({'ctrlsum1': ctrls.groupby(['yr']).control.sum()}).reset_index()

controls = ctrls[['subregional_crtl_id', 'yr', 'geo', 'geo_id', 'control', 'control_type', 'max_units',
                  'scenario_desc']].copy()

# to write to csv
# controls.to_csv('out/subregional_control_175.csv')
controls.to_sql(name='urbansim_lite_subregional_control', con=mssql_engine, schema='urbansim', index=False,
                if_exists='append')
