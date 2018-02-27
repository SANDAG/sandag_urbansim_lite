import pandas as pd
from sqlalchemy import create_engine
from pysandag.database import get_connection_string

db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

##################################
# includes sched dev (only here)
##################################
sr13_sql_match = '''
    SELECT c.jurisdiction, c.jurisdiction_id, c.yr_from, c.yr_to, c.hu_change
    FROM (SELECT a.jurisdiction, a.jurisdiction_id, a.yr_id AS yr_from, b.yr_id AS yr_to
    ,CASE WHEN b.hu - a.hu > 0 THEN b.hu - a.hu ELSE 0 END AS hu_change
    FROM (SELECT y.jurisdiction, y.jurisdiction_id, x.yr_id, sum(x.units) AS hu
    FROM [demographic_warehouse].[fact].[housing] AS x
    inner join [demographic_warehouse].[dim].[mgra_denormalize] AS y ON x.mgra_id = y.mgra_id
    WHERE x.datasource_id = 13 and x.yr_id in (2015, 2020, 2025, 2030, 2035, 2040, 2045, 2050)
    GROUP BY y.jurisdiction, y.jurisdiction_id, x.yr_id) AS a
    inner join 
    (SELECT y.jurisdiction,y.jurisdiction_id,x.yr_id, sum(x.units) AS hu
    FROM [demographic_warehouse].[fact].[housing] AS x
    inner join [demographic_warehouse].[dim].[mgra_denormalize] AS y ON x.mgra_id = y.mgra_id
    WHERE x.datasource_id = 13 and x.yr_id in (2015, 2020, 2025, 2030, 2035, 2040, 2045, 2050)
    GROUP BY y.jurisdiction, y.jurisdiction_id, x.yr_id) AS b
    ON a.jurisdiction_id = b.jurisdiction_id and a.yr_id = b.yr_id-5) AS c
    ORDER BY yr_from, jurisdiction_id
'''

##################################
# without sched dev
##################################
sr13_sql_match_san_sched_dev = '''
    SELECT '' as jurisdiction, c.City AS jurisdiction_id, c.yr_from, c.yr_to, c.hu_change
    FROM (SELECT a.City, a.increment AS yr_from, b.increment AS yr_to
    ,CASE WHEN  b.hu - a.hu > 0 THEN b.hu - a.hu ELSE 0 END AS hu_change
    FROM (SELECT y.City, x.increment, sum([hs]) AS hu
    FROM [regional_forecast].[sr13_final].[capacity] x
    inner join [regional_forecast].[sr13_final].[mgra13] as y on x.mgra = y.mgra
    WHERE x.scenario = 0 and x.increment in (2015, 2020, 2025, 2030, 2035, 2040, 2045, 2050)
    and x.site = 0 
    GROUP BY y.City, x.increment) AS a
    inner join 
    (SELECT y.City, x.increment, sum([hs]) AS hu
    FROM [regional_forecast].[sr13_final].[capacity] x
    inner join [regional_forecast].[sr13_final].[mgra13] AS y ON x.mgra = y.mgra
    WHERE x.scenario = 0 and x.increment in (2015, 2020, 2025, 2030, 2035, 2040, 2045, 2050) 
    and x.site = 0
    GROUP BY y.City, x.increment) as b
    ON a.City = b.City and a.increment = b.increment-5) AS c
    ORDER BY yr_from, City
'''

##############################################
# choose without sched dev as control (currently without)
##############################################
sr13_hu_df = pd.read_sql(sr13_sql_match_san_sched_dev, mssql_engine)
sr13_hu_df['jurisdiction'] = sr13_hu_df['jurisdiction'].astype(str)
##########################################################################

################################
# add cpa
################################
# San Diego, jurisdiction_id = 14
sd_by_cpa_15_50 = '''
    SELECT c.name as cpa_name, c.City AS jurisdiction_id,c.cpa, c.yr_from, c.yr_to, c.hu_change
    FROM (SELECT a.City,a.cpa,a.name, a.increment AS yr_from, b.increment AS yr_to
    ,CASE WHEN  b.hu - a.hu > 0 THEN b.hu - a.hu ELSE 0 END AS hu_change
    FROM (SELECT y.cpa,g.name,y.City, x.increment, sum([hs]) AS hu
    FROM [regional_forecast].[sr13_final].[capacity] x
    inner join [regional_forecast].[sr13_final].[mgra13] as y on x.mgra = y.mgra
    inner join data_cafe.ref.geography_zone	as g on g.zone = y.cpa
    WHERE x.scenario = 0 and x.increment in (2015, 2020, 2025, 2030, 2035, 2040, 2045, 2050)
    and x.site = 0  and g.geography_type_id = 15
    GROUP BY y.cpa,y.City, x.increment,g.name) AS a
    inner join 
    (SELECT y.cpa,y.City, x.increment, sum([hs]) AS hu
    FROM [regional_forecast].[sr13_final].[capacity] x
    inner join [regional_forecast].[sr13_final].[mgra13] AS y ON x.mgra = y.mgra
    WHERE x.scenario = 0 and x.increment in (2015, 2020, 2025, 2030, 2035, 2040, 2045, 2050) 
    and x.site = 0
    GROUP BY y.cpa,y.City, x.increment) as b
    ON a.cpa = b.cpa and a.increment = b.increment-5) AS c
    WHERE City = 14
    ORDER BY yr_from, City
    '''
cpa_sd_df= pd.read_sql(sd_by_cpa_15_50, mssql_engine)

# Unincorporated, jurisdiction_id = 19
unincorp_by_cpa_15_50 = '''
    SELECT c.name as cpa_name, c.City AS jurisdiction_id,c.cpa, c.yr_from, c.yr_to, c.hu_change
    FROM (SELECT a.City,a.cpa,a.name, a.increment AS yr_from, b.increment AS yr_to
    ,CASE WHEN  b.hu - a.hu > 0 THEN b.hu - a.hu ELSE 0 END AS hu_change
    FROM (SELECT y.cpa,g.name,y.City, x.increment, sum([hs]) AS hu
    FROM [regional_forecast].[sr13_final].[capacity] x
    inner join [regional_forecast].[sr13_final].[mgra13] as y on x.mgra = y.mgra
    inner join data_cafe.ref.geography_zone	as g on g.zone = y.cpa
    WHERE x.scenario = 0 and x.increment in (2015, 2020, 2025, 2030, 2035, 2040, 2045, 2050)
    and x.site = 0  and g.geography_type_id = 20
    GROUP BY y.cpa,y.City, x.increment,g.name) AS a
    inner join 
    (SELECT y.cpa,y.City, x.increment, sum([hs]) AS hu
    FROM [regional_forecast].[sr13_final].[capacity] x
    inner join [regional_forecast].[sr13_final].[mgra13] AS y ON x.mgra = y.mgra
    WHERE x.scenario = 0 and x.increment in (2015, 2020, 2025, 2030, 2035, 2040, 2045, 2050) 
    and x.site = 0
    GROUP BY y.cpa,y.City, x.increment) as b
    ON a.cpa = b.cpa and a.increment = b.increment-5) AS c
    WHERE City = 19
    ORDER BY yr_from, City
    '''
cpa_unincorp_df = pd.read_sql(unincorp_by_cpa_15_50, mssql_engine)

households_sql = '''
  SELECT sum(hh) AS hh,yr
  FROM isam.demographic_output.summary
  WHERE sim_id = 1004 and yr > 2019
  GROUP BY yr
'''

hh_df =  pd.read_sql(households_sql, mssql_engine)
hh = hh_df.loc[hh_df.yr == 2050].hh.values[0]

du_sql = '''
  SELECT  SUM(COALESCE(residential_units,0)) AS residential_units
  FROM urbansim.urbansim.building
'''

du_df = pd.read_sql(du_sql, mssql_engine)
du = int(du_df.values)

sched_dev_sql = '''
  SELECT  SUM(COALESCE(capacity,0)) 
  FROM urbansim.urbansim.parcel
  WHERE site_id is NOT NULL and capacity > 0
'''

sh_df = pd.read_sql(sched_dev_sql, mssql_engine)
sched_dev_capacity = int(sh_df.values)

sr14_cap_sql = '''
  SELECT jurisdiction_id as jur_or_cpa_id, sum(capacity)  as sr14_cap
  FROM urbansim.urbansim.parcel
  WHERE capacity > 0 AND site_id IS NULL
  GROUP BY jurisdiction_id
  ORDER BY jurisdiction_id
'''

city_cap_sql = '''
  SELECT cicpa_13 as jur_or_cpa_id, sum(capacity)  as sr14_cap
  FROM urbansim.urbansim.parcel parcels
  JOIN data_cafe.ref.vi_xref_geography_mgra_13   as x on x.mgra_13 = parcels.mgra_id
  JOIN data_cafe.ref.geography_zone				 as g on x.cicpa_13 = g.zone 
  WHERE parcels.capacity > 0  and  site_id IS NULL and jurisdiction_id = 14 and g.geography_type_id = 15
  GROUP BY cicpa_13
  ORDER BY cicpa_13
  '''

county_cap_sql = '''
  SELECT cocpa_13 as jur_or_cpa_id, sum(capacity)  as sr14_cap
  FROM urbansim.urbansim.parcel parcels
  JOIN data_cafe.ref.vi_xref_geography_mgra_13   as x on x.mgra_13 = parcels.mgra_id
  JOIN data_cafe.ref.geography_zone				 as g on x.cocpa_13 = g.zone 
  WHERE parcels.capacity > 0  and  site_id IS NULL and jurisdiction_id = 19 and g.geography_type_id = 20
  GROUP BY cocpa_13
  ORDER BY cocpa_13
  '''

jurs_cap_df = pd.read_sql(sr14_cap_sql, mssql_engine,index_col='jur_or_cpa_id')
city_cap_df = pd.read_sql(city_cap_sql, mssql_engine,index_col='jur_or_cpa_id')
county_cap_df = pd.read_sql(county_cap_sql, mssql_engine,index_col='jur_or_cpa_id')
sr14_cap_df = pd.concat([jurs_cap_df, city_cap_df, county_cap_df])

# sr14 units for region
units_needed = (hh - du - sched_dev_capacity).astype(float)

# add cpa to dataframe
# drops jurisdictions 14 and 19 to re-add them at CPA level
add_cpa_df = sr13_hu_df[sr13_hu_df.jurisdiction_id != 14]
add_cpa_df = add_cpa_df[add_cpa_df.jurisdiction_id != 19]
sr13_hu_df = pd.concat([add_cpa_df, cpa_sd_df, cpa_unincorp_df], ignore_index=True)

#sr13 units from 2020
units_needed_sr13_2020 = sr13_hu_df.hu_change.sum()

# calculate adjustment for targets
adj_to_totals = units_needed/units_needed_sr13_2020

# keep original hu calculation
sr13_hu_df['orig_hu_change'] = sr13_hu_df['hu_change']

# adjustment to meet target for sr14
sr13_hu_df['hu_change'] = adj_to_totals * sr13_hu_df['hu_change']

sr13_hu_df['jurisdiction_id_orig'] = sr13_hu_df['jurisdiction_id']

sr13_hu_df['jur_or_cpa_id'] = sr13_hu_df['jurisdiction_id']

sr13_hu_df.loc[(sr13_hu_df.jurisdiction_id == 14) | (sr13_hu_df.jurisdiction_id == 19), 'jur_or_cpa_id'] = sr13_hu_df['cpa']

# units by jursidiction
geo_share = pd.DataFrame({'hu_change_sum': sr13_hu_df.
                       groupby(["jur_or_cpa_id"]).hu_change.sum()})

# compare sr13 and sr14 share
compare_cap_to_forecast = geo_share.join(sr14_cap_df)

# adjustment needed for capacity differences bet sr13 and sr14
compare_cap_to_forecast['adj_per'] = compare_cap_to_forecast.sr14_cap/compare_cap_to_forecast.hu_change_sum
compare_cap_to_forecast.fillna(0,inplace=True)
compare_cap_to_forecast.reset_index(inplace=True)
compare_cap_to_forecast['jur_or_cpa_id'] = compare_cap_to_forecast['jur_or_cpa_id'].astype('int')
sr13_hu_df['jur_or_cpa_id'] = sr13_hu_df['jur_or_cpa_id'].astype('int')
# join with sr13 data
sr13_hu_df = sr13_hu_df.merge(compare_cap_to_forecast[['jur_or_cpa_id','sr14_cap','adj_per']],on='jur_or_cpa_id')

# adjust hu by capacity differences
sr13_hu_df['hu_change'] = sr13_hu_df['adj_per'] * sr13_hu_df['hu_change']

# create control percentages per period as a share of total housing unit change in that period
sr13_hu_df['control'] = sr13_hu_df.hu_change
number_periods = 0
for period in sr13_hu_df['yr_from'].unique():
    number_periods = number_periods + 1
    period_allocation = sr13_hu_df.loc[sr13_hu_df['yr_from'] == period].hu_change.sum()
    sr13_hu_df.loc[sr13_hu_df.yr_from == period, 'control'] = sr13_hu_df.control / period_allocation
# Check calculation for when period length or number is changed:
print(round(sr13_hu_df.control.sum()/number_periods), "<- Should be equal to 1!")

# split each period into the required number of years (creating duplicated rows - see while loop comments below)
sr14_res_control = sr13_hu_df.reindex(sr13_hu_df.index.repeat(sr13_hu_df.yr_to - sr13_hu_df.yr_from)).reset_index(drop=True)

# rename for clarity with generalized geography and year incrementing
sr14_res_control.rename(columns={'jurisdiction_id':'geo_id','yr_from': 'yr'},inplace=True)

# add one to year to start at 2021 and finish 2050 (sched dev before that)
sr14_res_control['yr'] = sr14_res_control.yr + 1

# modify the year column to increment by one, rather than repeat by period
while any(sr14_res_control.duplicated()): # checks for duplicate rows
    sr14_res_control['year_maker'] = sr14_res_control.duplicated() # create a boolean column = True if row is a repeat
    sr14_res_control.loc[sr14_res_control.year_maker == True, 'yr'] = sr14_res_control.yr + 1 # adds one to the year if the row is a duplicate

# organize by year and geography
sr14_res_control.sort_values(by=['yr','geo_id','cpa'],inplace=True)

# label each row by geography type
sr14_res_control['geo'] = 'jurisdiction'
sr14_res_control.loc[(sr14_res_control.geo_id == 14) | (sr14_res_control.geo_id == 19), 'geo'] = 'cpa'

# set geography_id for sub-jurisdiction regions
sr14_res_control.loc[sr14_res_control.cpa.notnull(), 'geo_id'] = sr14_res_control.cpa
sr14_res_control.geo_id = sr14_res_control.geo_id.astype(int)

# formalize dataframe for exporting
########################################
## Be sure to change scenario!
# scenario 1 has cpa for San Diego and Unincorporated (jurisdiction_id 14 and 19)
########################################
sr14_res_control['scenario'] = 1
sr14_res_control['scenario_desc'] = 'jurisdictions and cpa for city and county'
sr14_res_control['control_type'] = 'percentage'
sr14_res_control = sr14_res_control.reset_index()

# to write to csv
sr14_res_control.to_csv('sr14_res_control.csv')

# keep only columns for db table
sr14_res_control = sr14_res_control[['scenario','yr','geo','geo_id','control','control_type','scenario_desc']]
sr14_res_control.fillna(0,inplace=True)

# to write to database
sr14_res_control.to_sql(name='urbansim_lite_subregional_control', con=mssql_engine, schema='urbansim', index=False,if_exists='append')

## set max units to 100
# UPDATE [urbansim].[urbansim].[urbansim_lite_subregional_control]
# SET max_units = 100
# WHERE geo_id = 8 and scenario = 1