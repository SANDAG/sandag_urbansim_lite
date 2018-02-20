from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd
import numpy as np

db_connection_string = get_connection_string('..\data\config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

sr13_sql = '''
SELECT [increment]
      ,[luz]   
      ,sum([hs])  as hs
      ,sum([cap_hs])  as cap_hs
  FROM [regional_forecast].[sr13_final].[capacity]
  where scenario = 0
  GROUP BY increment,luz
  order by increment
  '''

sr14_sql = '''
SELECT p.parcel_id
    ,p.mgra_id
    ,p.luz_id
    ,p.jurisdiction_id
    ,p.du
    ,p.capacity
    ,p.site_id
    ,o.units_added
    ,o.year_simulation
  FROM [urbansim].[urbansim].[urbansim_lite_output] o
  FULL OUTER JOIN [urbansim].[urbansim].[parcel] p
  ON p.parcel_id = o.parcel_id
  WHERE p.capacity > 0
  ORDER BY o.year_simulation, p.jurisdiction_id
'''

sr13_df = pd.read_sql(sr13_sql,mssql_engine)
sr14_df = pd.read_sql(sr14_sql, mssql_engine)

sr14_df['increment'] = np.nan
for period in sr13_df['increment'].unique():
    period = period
    sr14_df.loc[sr14_df.year_simulation >= period, 'increment'] = period
