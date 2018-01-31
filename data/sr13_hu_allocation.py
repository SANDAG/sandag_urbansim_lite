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
    WHERE x.datasource_id = 13 and x.yr_id in (2020, 2030, 2040, 2050)
    GROUP BY y.jurisdiction, y.jurisdiction_id, x.yr_id) AS a
    inner join 
    (SELECT y.jurisdiction,y.jurisdiction_id,x.yr_id, sum(x.units) AS hu
    FROM [demographic_warehouse].[fact].[housing] AS x
    inner join [demographic_warehouse].[dim].[mgra_denormalize] AS y ON x.mgra_id = y.mgra_id
    WHERE x.datasource_id = 13 and x.yr_id in (2020, 2030, 2040, 2050)
    GROUP BY y.jurisdiction, y.jurisdiction_id, x.yr_id) AS b
    ON a.jurisdiction_id = b.jurisdiction_id and a.yr_id = b.yr_id-10) AS c
    ORDER BY yr_from, jurisdiction_id
'''
sr13_hu_df = pd.read_sql(sr13_sql_match, mssql_engine)
sr13_hu_df['jurisdiction'] = sr13_hu_df['jurisdiction'].astype(str)

for decade in sr13_hu_df['yr_from'].tolist():
    if


    str(decade)_allocation = sr13_hu_df.loc[sr13_hu_df['yr_from'] == decade].hu_change.sum()
    print(decade,"allocation is",decade_allocation) # the scalar is correct

sr13_hu_df['decade_allocation_by_jur'] = sr13_hu_df.apply(sr13_hu_df.hu_change/sr13_hu_df.loc[sr13_hu_df['yr_from']].hu_change.sum()) #.hu_change / decade_allocation



for jur in jurs['jurisdiction_id'].tolist():
    target_units_for_geo = subregional_targets.loc[subregional_targets['geo_id'] == jur].targets.values[0]
    geo_name = jurs.loc[jurs.jurisdiction_id == jur].name.values[0]
    print("Jurisdiction %d %s target units: %d" % (jur, geo_name, target_units_for_geo))
    parcels_in_geo = feasible_parcels_df.loc[feasible_parcels_df['jurisdiction_id'] == jur].copy()
    chosen = parcel_picker(parcels_in_geo, target_units_for_geo, geo_name, year)
    sr14cap = sr14cap.append(chosen)