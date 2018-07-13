import pandas as pd
from sqlalchemy import create_engine
from database import get_connection_string
import utils
import numpy as np

db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

scenarios = utils.yaml_to_dict('scenario_config.yaml', 'scenario')

# Retrieves maximum existing run_id from the table. If none exists, creates run_id = 1.
version_id_sql = '''
  SELECT max(version_id)
  FROM [urbansim].[urbansim].[urbansim_lite_adu_control]
'''
version_id_df = pd.read_sql(version_id_sql, mssql_engine)
if version_id_df.values:
    version_id = int(version_id_df.values) + 1
else:
    version_id = 1


adu_sql = '''
       SELECT jcpa,sum(du) as adu_total
  FROM [urbansim].[urbansim].[additional_capacity] c
  JOIN [ref].[vi_parcel_xref] x
  ON x.parcel_id = c.parcel_id
 where type = 'adu' and version_id = 107
 GROUP BY jcpa
 ORDER BY jcpa
'''
adu_df = pd.read_sql(adu_sql, mssql_engine)

adu_df['num_of_years'] = 21 # 2030 to 2050
adu_df['start_yr'] = 2030

# for jurisdictions: 2,5,12,14 (cpa 1400-1499)
# start year is 2019

adu_df.loc[(adu_df.jcpa==2),'num_of_years'] = 32 #2019 to 2050
adu_df.loc[((adu_df.jcpa==5)),'num_of_years'] = 32 #2019 to 2050
adu_df.loc[((adu_df.jcpa==12)),'num_of_years'] = 32 #2019 to 2050
adu_df.loc[((adu_df.jcpa>19) & (adu_df.jcpa<1900)),'num_of_years'] = 32 #2019 to 2050
adu_df.loc[(adu_df.jcpa==2),'start_yr'] = 2019
adu_df.loc[((adu_df.jcpa==5)),'start_yr'] = 2019
adu_df.loc[((adu_df.jcpa==12)),'start_yr'] = 2019
adu_df.loc[((adu_df.jcpa>19) & (adu_df.jcpa<1900)),'start_yr'] = 2019

adu_df['adu_per_year'] = adu_df['adu_total'] // adu_df['num_of_years']
adu_df['rem'] = adu_df['adu_total'] % adu_df['num_of_years']

adu_df['version_id'] = version_id

print(adu_df.head())

# adu_df_for_db = adu_df[['version_id','yr','allocation','jcpa','adu_total','num_of_years','adu_per_year','rem','start_yr']].copy()

adu_df_long = pd.DataFrame(adu_df.values.repeat(adu_df['num_of_years'], axis=0), columns=adu_df.columns)
adu_df2 = adu_df_long.copy()
# adu_df2 = adu_df2[['jcpa','adu_total','num_of_years','adu_per_year','rem','start_yr']].copy()
adu_df2['increment'] = adu_df2.groupby(['jcpa']).cumcount()
adu_df2['yr'] = adu_df2['start_yr'] + adu_df2['increment']

def add_remaining(grp):
     remaining = int(grp.iloc[0]['rem'])
     dfupdate = grp.sample(remaining)
     dfupdate.adu_per_year = dfupdate.adu_per_year + 1
     grp.update(dfupdate)
     # grp['mkt_return'] = grp['return'].sum()
     return grp

adu_controls = adu_df2.groupby('jcpa').apply(add_remaining)

sum_tots = adu_controls.groupby(['jcpa']).adu_per_year.sum()

adu_controls.jcpa =adu_controls.jcpa.astype(int)
adu_controls.adu_total = adu_controls.adu_total.astype(int)
adu_controls.num_of_years = adu_controls.num_of_years.astype(int)
adu_controls.start_yr = adu_controls.start_yr.astype(int)
adu_controls.adu_per_year = adu_controls.adu_per_year.astype(int)
adu_controls.rem = adu_controls.rem.astype(int)
adu_controls.yr = adu_controls.yr.astype(int)
adu_controls.version_id = adu_controls.version_id.astype(int)

adu_controls['allocation'] = adu_controls['adu_per_year']

adu_controls.drop(['adu_total','increment','rem','start_yr','num_of_years','adu_per_year'], axis=1,inplace=True)

print(adu_controls.allocation.sum())
print(adu_controls.head())
# adu_controls.to_sql(name='urbansim_lite_adu_control', con=mssql_engine, schema='urbansim', index=False,if_exists='append')