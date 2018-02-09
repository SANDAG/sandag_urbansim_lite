import pandas as pd
from sqlalchemy import create_engine
from pysandag.database import get_connection_string

db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

##################################
# includes sched dev
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
    WHERE x.scenario = 0 and x.increment in (2020, 2025, 2030, 2035, 2040, 2045, 2050)
    and x.site = 0 
    GROUP BY y.City, x.increment) AS a
    inner join 
    (SELECT y.City, x.increment, sum([hs]) AS hu
    FROM [regional_forecast].[sr13_final].[capacity] x
    inner join [regional_forecast].[sr13_final].[mgra13] AS y ON x.mgra = y.mgra
    WHERE x.scenario = 0 and x.increment in (2020, 2025, 2030, 2035, 2040, 2045, 2050) 
    and x.site = 0
    GROUP BY y.City, x.increment) as b
    ON a.City = b.City and a.increment = b.increment-5) AS c
    ORDER BY yr_from, City
'''

##############################################
# choose with or without sched dev as control
##############################################
sr13_hu_df = pd.read_sql(sr13_sql_match_san_sched_dev, mssql_engine)
sr13_hu_df['jurisdiction'] = sr13_hu_df['jurisdiction'].astype(str)
##########################################################################

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
  SELECT jurisdiction_id, sum(capacity)  as sr14_cap
  FROM urbansim.urbansim.parcel
  WHERE capacity > 0 AND site_id IS NULL
  GROUP BY jurisdiction_id
  ORDER BY jurisdiction_id
'''

sr14_cap_df = pd.read_sql(sr14_cap_sql, mssql_engine,index_col='jurisdiction_id')

# sr14 units for region
units_needed = (hh - du - sched_dev_capacity).astype(float)

#sr13 units from 2020
units_needed_sr13_2020 = sr13_hu_df.hu_change.sum()

# calculate adjustment for targets
adj_to_totals = units_needed/units_needed_sr13_2020

# keep original hu calculation
sr13_hu_df['orig_hu_change'] = sr13_hu_df['hu_change']

# adjustment to meet target for sr14
sr13_hu_df['hu_change'] = adj_to_totals * sr13_hu_df['hu_change']

# units by jursidiction
geo_share = pd.DataFrame({'hu_change_sum': sr13_hu_df.
                       groupby(["jurisdiction_id"]).hu_change.sum()})

# compare sr13 and sr14 share
compare_cap_to_forecast = geo_share.join(sr14_cap_df)

# adjustment needed for capacity differences bet sr13 and sr14
compare_cap_to_forecast['adj_per'] = compare_cap_to_forecast.sr14_cap/compare_cap_to_forecast.hu_change_sum

# join with sr13 data
sr13_hu_df = sr13_hu_df.join(compare_cap_to_forecast,on='jurisdiction_id')

# adjust hu by capacity differences
sr13_hu_df['hu_change'] = sr13_hu_df['adj_per'] * sr13_hu_df['hu_change']

sr13_hu_df['control'] = sr13_hu_df.hu_change
number_periods = 0
for period in sr13_hu_df['yr_from'].unique():
    number_periods = number_periods + 1
    period_allocation = sr13_hu_df.loc[sr13_hu_df['yr_from'] == period].hu_change.sum()
    print("%d allocation is %d" % (period,period_allocation))
    sr13_hu_df.loc[sr13_hu_df.yr_from == period, 'control'] = sr13_hu_df.control / period_allocation
print(sr13_hu_df.control.sum()/number_periods, "Should equal 1!")

sr14_res_control = sr13_hu_df.reindex(sr13_hu_df.index.repeat(sr13_hu_df.yr_to - sr13_hu_df.yr_from)).reset_index(drop=True)
sr14_res_control['scenario'] = 6
sr14_res_control.rename(columns={'jurisdiction_id':'geo_id','yr_from': 'yr'},inplace=True)

# add one to year to start at 2021 and finish 2050 (sched dev before that)
sr14_res_control['yr'] = sr14_res_control.yr + 1

while any(sr14_res_control.duplicated()):
    sr14_res_control['year_maker'] = sr14_res_control.duplicated()
    sr14_res_control.loc[sr14_res_control.year_maker == True, 'yr'] = sr14_res_control.yr + 1

sr14_res_control.sort_values(by=['yr','geo_id'],inplace=True)
sr14_res_control['geo'] = 'jurisdiction'
sr14_res_control['control_type'] = 'percentage'

# to write to csv
# sr14_res_control.to_csv('sr14_res_control.csv')

# keep only columns for db table
sr14_res_control = sr14_res_control[['scenario','yr','geo','geo_id','control','control_type']]

# to write to database
# sr14_res_control.to_sql(name='residential_control', con=mssql_engine, schema='urbansim', index=False,if_exists='append')


# add name to cpa...
city_by_cpa = '''
	SELECT '' as jurisdiction, c.name as CPA_name, c.City AS jurisdiction_id,c.CPA, c.yr_from, c.yr_to, c.hu_change
    FROM (SELECT a.City,a.CPA,a.name, a.increment AS yr_from, b.increment AS yr_to
    ,CASE WHEN  b.hu - a.hu > 0 THEN b.hu - a.hu ELSE 0 END AS hu_change
    FROM (SELECT y.CPA,g.name,y.City, x.increment, sum([hs]) AS hu
    FROM [regional_forecast].[sr13_final].[capacity] x
    inner join [regional_forecast].[sr13_final].[mgra13] as y on x.mgra = y.mgra
	inner join data_cafe.ref.geography_zone	as g on g.zone = y.CPA
    WHERE x.scenario = 0 and x.increment in (2020, 2025, 2030, 2035, 2040, 2045, 2050)
    and x.site = 0  and  g.geography_type_id = 15 
    GROUP BY y.CPA,y.City, x.increment,g.name) AS a
    inner join 
    (SELECT y.CPA,y.City, x.increment, sum([hs]) AS hu
    FROM [regional_forecast].[sr13_final].[capacity] x
    inner join [regional_forecast].[sr13_final].[mgra13] AS y ON x.mgra = y.mgra
    WHERE x.scenario = 0 and x.increment in (2020, 2025, 2030, 2035, 2040, 2045, 2050) 
    and x.site = 0
    GROUP BY y.CPA,y.City, x.increment) as b
    ON a.CPA = b.CPA and a.increment = b.increment-5) AS c
	where City = 14
    ORDER BY yr_from, City
    '''
