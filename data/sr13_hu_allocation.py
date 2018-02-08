import pandas as pd
from sqlalchemy import create_engine
from pysandag.database import get_connection_string

db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

# includes sched dev
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

# without sched dev
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

# choose which sql to use - with or without sched dev included
sr13_hu_df = pd.read_sql(sr13_sql_match_san_sched_dev, mssql_engine)

sr13_hu_df['jurisdiction'] = sr13_hu_df['jurisdiction'].astype(str)


sr13_hu_df['control'] = sr13_hu_df.hu_change
number_periods = 0
for period in sr13_hu_df['yr_from'].unique():
    number_periods = number_periods + 1
    period_allocation = sr13_hu_df.loc[sr13_hu_df['yr_from'] == period].hu_change.sum()
    print("%d allocation is %d" % (period,period_allocation))
    sr13_hu_df.loc[sr13_hu_df.yr_from == period, 'control'] = sr13_hu_df.control / period_allocation
print(sr13_hu_df.control.sum()/number_periods, "Should equal 1!")

sr14_res_control = sr13_hu_df.reindex(sr13_hu_df.index.repeat(sr13_hu_df.yr_to - sr13_hu_df.yr_from)).reset_index(drop=True)
sr14_res_control['scenario'] = 4
sr14_res_control.rename(columns={'jurisdiction_id':'geo_id','yr_from': 'yr'},inplace=True)

# add one to year to start at 2021 and finish 2050 (sched dev before that)
sr14_res_control['yr'] = sr14_res_control.yr + 1

while any(sr14_res_control.duplicated()):
    sr14_res_control['year_maker'] = sr14_res_control.duplicated()
    sr14_res_control.loc[sr14_res_control.year_maker == True, 'yr'] = sr14_res_control.yr + 1

sr14_res_control = sr14_res_control.drop(['jurisdiction', 'hu_change', 'yr_to', 'year_maker'], axis=1)
sr14_res_control.sort_values(by=['yr','geo_id'],inplace=True)
sr14_res_control['geo'] = 'jurisdiction'
sr14_res_control['control_type'] = 'percentage'

# to write to csv
# sr14_res_control.to_csv('sr14_res_control.csv')

# to write to database
# sr14_res_control.to_sql(name='residential_control', con=mssql_engine, schema='urbansim', index=False,if_exists='append')