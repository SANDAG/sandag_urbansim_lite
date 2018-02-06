import pandas as pd
from sqlalchemy import create_engine
from pysandag.database import get_connection_string

db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

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
sr13_hu_df = pd.read_sql(sr13_sql_match, mssql_engine)
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
sr14_res_control['scenario'] = 3
sr14_res_control.rename(columns={'jurisdiction_id':'geo_id','yr_from': 'yr'},inplace=True)
while any(sr14_res_control.duplicated()):
    sr14_res_control['year_maker'] = sr14_res_control.duplicated()
    sr14_res_control.loc[sr14_res_control.year_maker == True, 'yr'] = sr14_res_control.yr + 1

sr14_res_control = sr14_res_control.drop(['jurisdiction', 'hu_change', 'yr_to', 'year_maker'], axis=1)
sr14_res_control.sort_values(by=['yr','geo_id'],inplace=True)
#needs to be written to database
#sr14_res_control.to_csv('data/sr14_res_control.csv')