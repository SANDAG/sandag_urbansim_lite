import pandas as pd
from sqlalchemy import create_engine
from database import get_connection_string
import utils

db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

versions = utils.yaml_to_dict('../data/scenario_config.yaml', 'scenario')


# Units needed
units_needed_sql = '''
SELECT [yr], [version_id], [housing_units_add] as sr14hu
  FROM [urbansim].[urbansim].[urbansim_target_housing_units]
  WHERE version_id = %s'''

units_needed_sql = units_needed_sql % versions['target_housing_units_version']
hu_df =  pd.read_sql(units_needed_sql, mssql_engine)
total_units_needed = int(hu_df['sr14hu'].sum())
# 468866 total units needed (was 396,354)

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

# print(sr13_df.loc[sr13_df.mgra==10184])
#       hs  increment  city   cpa  luz_id  site
# mgra
# 10184  39       2012    19  1901     198     0
# 10184  39       2015    19  1901     198     0
# 10184  45       2020    19  1901     198     0
# 10184  45       2025    19  1901     198     0
# 10184  45       2030    19  1901     198     0

### check results against:
### file:///M:\RES\GIS\Spacecore\sr14GISinput\LandUseInputs\SR13_SR14_CapacityAllocationRatio.xlsx
sr13summary = pd.DataFrame({'hs_sum': sr13_df.groupby(['increment']).hs.sum()}).reset_index()
sr13summary['chg'] = sr13summary.hs_sum.diff().fillna(0).astype(int)
sr13summary['chg%'] = (sr13summary.hs_sum.pct_change().fillna(0) * 100).round(2)
sr13summary.set_index('increment',inplace=True)
# print(sr13summary)

#             hs_sum    chg  chg%
# increment
# 2012       1165818      0  0.00
# 2015       1194908  29090  2.50
# 2020       1249684  54776  4.58
# 2025       1301870  52186  4.18
# 2030       1348802  46932  3.60
# 2035       1394783  45981  3.41
# 2040       1434653  39870  2.86
# 2045       1466366  31713  2.21
# 2050       1491935  25569  1.74

# sr13summary.chg.sum()
# total change 326,117

sr13_df['jcid'] = sr13_df.city
sr13_df.loc[(sr13_df.city == 14) | (sr13_df.city == 19), 'jcid'] = sr13_df['cpa']

sr13_jcid_df = pd.DataFrame({'hs_sum': sr13_df.groupby(['jcid','increment']).
                               hs.sum()}).reset_index()
# print(sr13_jcid_df.loc[sr13_jcid_df.jcid==1901])
#     jcid  increment  hs_sum
# 1901       2012    6565
# 1901       2015    6419
# 1901       2020    7319
# 1901       2025    7834
# 1901       2030    8255
# 1901       2035    8636
# 1901       2040    8802
# 1901       2045    9158
# 1901       2050    9495

# len(sr13_jcid_df.jcid.unique())
# 103

sr13_pivot = sr13_jcid_df.pivot\
(index='jcid', columns='increment', values='hs_sum').\
reset_index().rename_axis(None, axis=1)
sr13_pivot.set_index('jcid',inplace=True)

# print(sr13_pivot.loc[1901:1905])
#       2012  2015  2020  2025  2030  2035  2040  2045  2050
# jcid
# 1901  6565  6419  7319  7834  8255  8636  8802  9158  9495
# 1902  2199  2201  2328  2343  2466  2635  2635  2640  2646
# 1903  3582  3586  3783  3797  3815  3869  3881  3892  3908
# 1904  3565  3699  3726  3793  3952  4118  4215  4314  5117


sr13_chg = sr13_pivot.diff(axis=1)

# print(sr13_chg.loc[1901:1905])
#       2012   2015   2020   2025   2030   2035   2040   2045   2050
# jcid
# 1901   NaN -146.0  900.0  515.0  421.0  381.0  166.0  356.0  337.0
# 1902   NaN    2.0  127.0   15.0  123.0  169.0    0.0    5.0    6.0
# 1903   NaN    4.0  197.0   14.0   18.0   54.0   12.0   11.0   16.0
# 1904   NaN  134.0   27.0   67.0  159.0  166.0   97.0   99.0  803.0


sr13_chg.drop([2012], axis=1,inplace=True)
sr13_chgst = sr13_chg.stack().to_frame()
sr13_chgst.reset_index(inplace=True,drop=False)
# print(sr13_chgst.loc[sr13_chgst.jcid==1901])
# jcid  level_1      0
# 1901     2015 -146.0
# 1901     2020  900.0
# 1901     2025  515.0
# 1901     2030  421.0
# 1901     2035  381.0
# 1901     2040  166.0
# 1901     2045  356.0
# 1901     2050  337.0

sr13_chgst.rename(columns={'level_1': 'yr_to',0:'hu_change'},inplace=True)
sr13_chgst['yr_from'] = sr13_chgst['yr_to'] - 5
sr13_chgst.loc[sr13_chgst.yr_from == 2010, 'yr_from'] = 2012
sr13_chgst = sr13_chgst[['jcid','yr_from','yr_to','hu_change']].copy()
# sr13_chgst.set_index('jcid',inplace=True)
# print(sr13_chgst.loc[sr13_chgst.index==1901])
#       yr_from  yr_to  hu_change
# jcid
# 1901     2012   2015     -146.0
# 1901     2015   2020      900.0
# 1901     2020   2025      515.0
# 1901     2025   2030      421.0
# 1901     2030   2035      381.0
# 1901     2035   2040      166.0
# 1901     2040   2045      356.0
# 1901     2045   2050      337.0

parcel_sql = '''
    SELECT p.parcel_id, p.site_id, j.name,  p.cap_jurisdiction_id, p.jurisdiction_id, p.mgra_id, p.luz_id,
    p.capacity_2 AS capacity_base_yr,'par' as capt,du_2017
      FROM urbansim.urbansim.parcel p 
      JOIN urbansim.ref.jurisdiction j on p.cap_jurisdiction_id = j.jurisdiction_id
      WHERE capacity_2 > 0 and site_id IS NULL
  ORDER BY j.name,p.jurisdiction_id, site_id'''
hs = pd.read_sql(parcel_sql,mssql_engine)
# print("\n   capacity: {:,}".format(int(hs.capacity_base_yr.sum())))
# capacity: 361,644
# print("\n   parcels with capacity: {:,}".format(len(hs)))
# parcels with capacity: 50,369
# hs.head()
#            site_id      name  cap_jurisdiction_id  jurisdiction_id  mgra_id  \
# parcel_id
# 5117537        NaN  Carlsbad                    1                1    18057
# 5117616        NaN  Carlsbad                    1                1    18090
# 337513         NaN  Carlsbad                    1                1    14337
# 337514         NaN  Carlsbad                    1                1    14337
# 337515         NaN  Carlsbad                    1                1    14337
#            luz_id  capacity_base_yr
# parcel_id
# 5117537        15                 2
# 5117616        15                 1
# 337513         11                 3
# 337514         11                 2
# 337515         11                 2
#

# sched_dev_sql = '''
#     SELECT  p.parcel_id, p.site_id, j.name,  p.cap_jurisdiction_id, p.jurisdiction_id, p.mgra_id, p.luz_id,
#             sum([res_units]) AS capacity_base_yr
#     FROM [urbansim].[urbansim].[scheduled_development_do_not_use] s
#     JOIN urbansim.urbansim.parcel p on p.parcel_id = s.parcel_id
#     JOIN urbansim.ref.jurisdiction j on p.cap_jurisdiction_id = j.jurisdiction_id
#     GROUP BY p.parcel_id, p.site_id, j.name,  p.cap_jurisdiction_id, p.jurisdiction_id, p.mgra_id, p.luz_id'''
# sched_dev_df = pd.read_sql(sched_dev_sql,mssql_engine)
# hs = pd.concat([hs,sched_dev_df])

xref_geography_sql = '''
    SELECT mgra_13, cocpa_2016, cicpa_13
      FROM data_cafe.ref.vi_xref_geography_mgra_13
      where jurisdiction_2016 IN (14,19)'''
xref_geography_df = pd.read_sql(xref_geography_sql, mssql_engine)
# remove mgras without a CPA (mgra_13 = 7259)
xref_geography_df = xref_geography_df.loc[~((xref_geography_df.cocpa_2016.isnull()) & (xref_geography_df.cicpa_13.isnull()))].copy()
#          cocpa_2016  cicpa_13
# mgra_13
# 2917            NaN    1442.0
# 5834            NaN    1412.0
# 11407        1907.0       NaN
# 19897        1909.0       NaN
# 1566            NaN    1444.0

# get geo ids
# use OUTER merge to get every CPA even those with no capacity
units = pd.merge(hs,xref_geography_df,left_on='mgra_id',right_on='mgra_13',how = 'outer')
# units.loc[units.cocpa_2016==1901].head()
#            site_id            name  cap_jurisdiction_id  jurisdiction_id  \
# parcel_id
# 740288.0       NaN  Unincorporated                 19.0             19.0
# 719586.0       NaN  Unincorporated                 19.0             19.0
# 72333.0        NaN  Unincorporated                 19.0             19.0
# 625400.0       NaN  Unincorporated                 19.0             19.0
# 625401.0       NaN  Unincorporated                 19.0             19.0
#            mgra_id  luz_id  capacity_base_yr  mgra_13  cocpa_2016  cicpa_13
# parcel_id
# 740288.0   22236.0   198.0               3.0  22236.0      1901.0       NaN
# 719586.0   10202.0   198.0               2.0  10202.0      1901.0       NaN
# 72333.0    10202.0   198.0               2.0  10202.0      1901.0       NaN
# 625400.0   10202.0   198.0               8.0  10202.0      1901.0       NaN
# 625401.0   10202.0   198.0               7.0  10202.0      1901.0       NaN

units['jcid'] = units['cap_jurisdiction_id']
units.loc[units.cap_jurisdiction_id == 19,'jcid'] = units['cocpa_2016']
units.loc[units.cap_jurisdiction_id == 14,'jcid'] = units['cicpa_13']

# cases where there are parcels with capacity in a CPA
# (or parcel e.g. 5038426 without a CPA)
# units.loc[units.jcid.isnull()].head()
# parcel_id
#  5038426.0      NaN  Unincorporated                 19.0             19.0
# NaN             NaN             NaN                  NaN              NaN
# NaN             NaN             NaN                  NaN              NaN
# NaN             NaN             NaN                  NaN              NaN
# NaN             NaN             NaN                  NaN              NaN
#             mgra_id  luz_id  capacity_base_yr  mgra_13  cocpa_2016  cicpa_13  \
# parcel_id
#  5038426.0  19415.0    18.0               5.0      NaN         NaN       NaN
# NaN             NaN     NaN               NaN  19897.0      1909.0       NaN
# NaN             NaN     NaN               NaN   1566.0         NaN    1444.0
# NaN             NaN     NaN               NaN     46.0         NaN    1442.0
# NaN             NaN     NaN               NaN   5788.0         NaN    1435.0
#             jcid
# parcel_id
#  5038426.0   NaN
# NaN          NaN
# NaN          NaN
# NaN          NaN
# NaN          NaN

units.loc[units.jcid.isnull(),'jcid'] = units['cicpa_13']
units.loc[units.jcid.isnull(),'jcid'] = units['cocpa_2016']
units.fillna(0, inplace=True)
# units.loc[units.parcel_id==0].head()
#        parcel_id  site_id name  cap_jurisdiction_id  jurisdiction_id  mgra_id  \
# 50369        0.0      0.0    0                  0.0              0.0      0.0
# 50370        0.0      0.0    0                  0.0              0.0      0.0
# 50371        0.0      0.0    0                  0.0              0.0      0.0
# 50372        0.0      0.0    0                  0.0              0.0      0.0
# 50373        0.0      0.0    0                  0.0              0.0      0.0
#        luz_id  capacity_base_yr  mgra_13  cocpa_2016  cicpa_13    jcid
# 50369     0.0               0.0  19897.0      1909.0       0.0  1909.0
# 50370     0.0               0.0   1566.0         0.0    1444.0  1444.0
# 50371     0.0               0.0     46.0         0.0    1442.0  1442.0
# 50372     0.0               0.0   5788.0         0.0    1435.0  1435.0
# 50373     0.0               0.0  11453.0         0.0    1431.0  1431.0

######################################################################################
# check for missing jcid (where jcid = 0)
# units.loc[units.jcid==0]
#            site_id            name  cap_jurisdiction_id  jurisdiction_id  \
# parcel_id
# 5038426.0      0.0  Unincorporated                 19.0             19.0
#            mgra_id  luz_id  capacity_base_yr  mgra_13  cocpa_2016  cicpa_13  \
# parcel_id
# 5038426.0  19415.0    18.0               5.0      0.0         0.0       0.0
#            jcid
# parcel_id
# 5038426.0   0.0

# manually add CPA for mgra_id = 19415, missing CPA
units.loc[units.mgra_id==19415,'jcid'] = 1909
units.loc[units.mgra_id==19415,'cocpa_2016'] = 1909
units['jcid'] = units['jcid'].astype(int)
sr14x = pd.DataFrame({'sr14c': units.groupby(['jcid']).
                               capacity_base_yr.sum()}).reset_index()
sr14x['jcid'] = sr14x['jcid'].astype(int)
# sr14x.set_index('jcid',inplace=True)
# print(sr14x.loc[1901:1910])
#        sr14c
# jcid
# 1901  3732.0
# 1902   500.0
# 1903   438.0
# 1904  7454.0
# 1906  3469.0
# 1907  3606.0
# 1908  2792.0
# 1909  9626.0


sr13x = pd.DataFrame({'sr13c': sr13_chgst.groupby(['jcid']).
                               hu_change.sum()}).reset_index()
# sr13x.set_index('jcid',inplace=True)
# print(sr13x.loc[1901:1910])
#         sr13c
# jcid
# 1901   2930.0
# 1902    447.0
# 1903    326.0
# 1904   1552.0
# 1906   1619.0
# 1907  11555.0
# 1908   1291.0
# 1909   7166.0

sx = pd.merge(sr13x,sr14x,left_on='jcid',right_on='jcid',how = 'outer')
# sx.set_index('jcid',inplace=True)
# print(sx.loc[1901:1910])
#         sr13c   sr14c
# jcid
# 1901   2930.0  3732.0
# 1902    447.0   500.0
# 1903    326.0   438.0
# 1904   1552.0  7454.0
# 1906   1619.0  3469.0
# 1907  11555.0  3606.0
# 1908   1291.0  2792.0
# 1909   7166.0  9626.0

sx['adj_jcid_cap'] = sx['sr14c']/sx['sr13c']
# sx.set_index('jcid',inplace=True)
# print(sx.loc[1901:1910])
#         sr13c   sr14c  adj_jcid_cap
# jcid
# 1901   2930.0  3732.0      1.273720
# 1902    447.0   500.0      1.118568
# 1903    326.0   438.0      1.343558
# 1904   1552.0  7454.0      4.802835
# 1906   1619.0  3469.0      2.142681
# 1907  11555.0  3606.0      0.312073
# 1908   1291.0  2792.0      2.162665
# 1909   7166.0  9626.0      1.343288


# sr13 yearly
sr13a = sr13_chgst.reindex(sr13_chgst.index.repeat(sr13_chgst.yr_to - sr13_chgst.yr_from)).reset_index(drop=True)
# print(sr13a.loc[sr13a.index==1901].head())
#       yr_from  yr_to  hu_change
# jcid
# 1901     2012   2015     -146.0
# 1901     2012   2015     -146.0
# 1901     2012   2015     -146.0
# 1901     2015   2020      900.0
# 1901     2015   2020      900.0
# 1901     2015   2020      900.0

sr13a['yrs'] = sr13a.yr_to - sr13a.yr_from
sr13a['units'] = (sr13a['hu_change']/sr13a['yrs'])
sr13a['yr'] = sr13a.yr_from + 1
# print(sr13a.loc[sr13a.index==1901])
#       yr_from  yr_to  hu_change  yrs       units    yr
# jcid
# 1901     2012   2015     -146.0    3  -48.666667  2013
# 1901     2012   2015     -146.0    3  -48.666667  2013
# 1901     2012   2015     -146.0    3  -48.666667  2013
# 1901     2015   2020      900.0    5  180.000000  2016
# 1901     2015   2020      900.0    5  180.000000  2016
# 1901     2015   2020      900.0    5  180.000000  2016
# 1901     2015   2020      900.0    5  180.000000  2016
# 1901     2015   2020      900.0    5  180.000000  2016

# modify the year column to increment by one, rather than repeat by period
while any(sr13a.duplicated()): # checks for duplicate rows
    sr13a['ym'] = sr13a.duplicated() # create a boolean column = True if row is a repeat
    sr13a.loc[sr13a.ym == True, 'yr'] = sr13a.yr + 1
del sr13a['ym']
# print(sr13a.loc[sr13a.jcid==1901])
#       yr_from  yr_to  hu_change  yrs       units    yr
# jcid
# 1901     2012   2015     -146.0    3  -48.666667  2013
# 1901     2012   2015     -146.0    3  -48.666667  2014
# 1901     2012   2015     -146.0    3  -48.666667  2015
# 1901     2015   2020      900.0    5  180.000000  2016
# 1901     2015   2020      900.0    5  180.000000  2017
# 1901     2015   2020      900.0    5  180.000000  2018
# 1901     2015   2020      900.0    5  180.000000  2019
# 1901     2015   2020      900.0    5  180.000000  2020

sr13a = sr13a.loc[sr13a.yr > 2016].copy()


sr13aa = pd.merge(sr13a[['jcid','yr','units']],sx,left_on='jcid',right_on='jcid',how = 'outer')
# sr13aa.set_index('jcid',inplace=True)
# print(sr13aa.loc[1901])
#         yr  units   sr13c   sr14c  adj_jcid_cap
# jcid
# 1901  2017  180.0  2930.0  3732.0       1.27372
# 1901  2018  180.0  2930.0  3732.0       1.27372
# 1901  2019  180.0  2930.0  3732.0       1.27372
# 1901  2020  180.0  2930.0  3732.0       1.27372
# 1901  2021  103.0  2930.0  3732.0       1.27372
# 1901  2022  103.0  2930.0  3732.0       1.27372
# 1901  2023  103.0  2930.0  3732.0       1.27372
# 1901  2024  103.0  2930.0  3732.0       1.27372
# 1901  2025  103.0  2930.0  3732.0       1.27372
# 1901  2026   84.2  2930.0  3732.0       1.27372#

sr13aa['units_adj1'] = sr13aa['units'] * sr13aa['adj_jcid_cap']


sr13b = pd.DataFrame({'unitsum': sr13aa.groupby(['yr']).
                               units_adj1.sum()}).reset_index()
# sr13b.set_index('yr',inplace=True)
# sr13b.head()
#            unitsum
# yr
# 2017  12117.082355
# 2018  12117.082355
# 2019  12117.082355
# 2020  12117.082355
# 2021  11094.686480
# 2022  11094.686480
# 2023  11094.686480
# 2024  11094.686480
# 2025  11094.686480
# 2026  10376.368588#

# note: 10437.2 * 5 = 52186 (matches sr13summary for 2025 = 52186)


sr13b = pd.merge(sr13b,hu_df,left_on='yr',right_on='yr',how = 'outer')
# sr13b.set_index('yr',inplace=True)
# sr13b.head()
#         unitsum   sr14hu
# yr
# 2017  12117.082355  10947.0
# 2018  12117.082355  11642.0
# 2019  12117.082355  12433.0
# 2020  12117.082355  12196.0
# 2021  11094.686480  11910.0
# 2022  11094.686480  13363.0
# 2023  11094.686480  15211.0
# 2024  11094.686480  17067.0
# 2025  11094.686480  19862.0
# 2026  10376.368588  24597.0


sr13b['adj_forecast_hs'] = sr13b['sr14hu']/sr13b['unitsum']
#            unitsum   sr14hu    adj_forecast_hs
# yr
# 2017  12117.082355  10947.0  0.903435
# 2018  12117.082355  11642.0  0.960792
# 2019  12117.082355  12433.0  1.026072
# 2020  12117.082355  12196.0  1.006513
# 2021  11094.686480  11910.0  1.073487
# 2022  11094.686480  13363.0  1.204450
# 2023  11094.686480  15211.0  1.371017
# 2024  11094.686480  17067.0  1.538304
# 2025  11094.686480  19862.0  1.790226
# 2026  10376.368588  24597.0  2.370482


sr13ab = pd.merge(sr13aa,sr13b[['yr','adj_forecast_hs']],left_on='yr',right_on='yr',how = 'outer')
# sr13ab  = sr13ab [['jcid','yr','units','units_adj1','adj_forecast_hs']].copy()
sr13ab['units_adj2'] = (sr13ab ['units_adj1'] * sr13ab ['adj_forecast_hs'])
# sr13ab.set_index('jcid',inplace=True)
# print(sr13ab.loc[1901])
#         yr  units  units_adj1  adj_forecast_hs  units_adj2
# jcid
# 1901  2017  180.0  229.269625         0.903435  207.130273
# 1901  2018  180.0  229.269625         0.960792  220.280501
# 1901  2019  180.0  229.269625         1.026072  235.247163
# 1901  2020  180.0  229.269625         1.006513  230.762840
# 1901  2021  103.0  131.193174         1.073487  140.834147
# 1901  2022  103.0  131.193174         1.204450  158.015676
# 1901  2023  103.0  131.193174         1.371017  179.868027
# 1901  2024  103.0  131.193174         1.538304  201.814977
# 1901  2025  103.0  131.193174         1.790226  234.865476
# 1901  2026   84.2  107.247235         2.370482  254.22769

sr13check = pd.DataFrame({'units_for_sr14': sr13ab.groupby(['yr']).
                     units_adj2.sum()}).reset_index()

sr13check = pd.merge(sr13check,hu_df,left_on='yr',right_on='yr',how = 'outer')
# sr13check.head()
#       units_for_sr14   sr14hu
# yr
# 2017         10947.0  10947.0
# 2018         11642.0  11642.0
# 2019         12433.0  12433.0
# 2020         12196.0  12196.0
# 2021         11910.0  11910.0
# 2022         13363.0  13363.0
# 2023         15211.0  15211.0
# 2024         17067.0  17067.0
# 2025         19862.0  19862.0
# 2026         24597.0  24597.0

sr13check['diff'] = sr13check.units_for_sr14 - sr13check.sr14hu
# print(sr13check.loc[sr13check['diff'] > 0.1])
# Empty DataFrame
# Columns: [yr, units_for_sr14, sr14hu, diff]
# Index: []

sr13 = pd.merge(sr13ab,sr13check[['yr','units_for_sr14']],left_on='yr',right_on='yr',how = 'outer')

sr13['control'] = sr13['units_adj2']/sr13['units_for_sr14']
sr13.drop(['units', 'units_adj1','adj_forecast_hs'], axis=1,inplace=True)
# sr13.set_index('jcid',inplace=True)
# print(sr13.loc[sr13.jcid==1901])
#         yr  units_adj2  units_for_sr14   control
# jcid
# 1901  2017  207.130273         10947.0  0.018921
# 1901  2018  220.280501         11642.0  0.018921
# 1901  2019  235.247163         12433.0  0.018921
# 1901  2020  230.762840         12196.0  0.018921
# 1901  2021  140.834147         11910.0  0.011825
# 1901  2022  158.015676         13363.0  0.011825
# 1901  2023  179.868027         15211.0  0.011825
# 1901  2024  201.814977         17067.0  0.011825
# 1901  2025  234.865476         19862.0  0.011825
# 1901  2026  254.227693         24597.0  0.010336

sr13.fillna(0,inplace=True)

sr13['subregional_crtl_id'] = 6
sr13['geo_id'] = sr13['jcid']
sr13['max_units'] = None
sr13['geo'] = 'jur_and_cpa'
sr13['scenario_desc'] = 'capacity_2'
sr13['control_type'] = 'percentage'

sr13.loc[sr13.control < 0, 'control'] = 0
sr13 = sr13.loc[sr13.yr!=0].copy()
controls  = sr13[['subregional_crtl_id','yr','geo','geo_id','control','control_type','max_units','scenario_desc']].copy()

# print(controls.loc[controls.geo_id==1901].head())
#      scenario    yr          geo  geo_id   control control_type max_units  \
# 79          3  2017  jur_and_cpa    1901  0.016431   percentage      None
# 182         3  2018  jur_and_cpa    1901  0.016431   percentage      None
# 285         3  2019  jur_and_cpa    1901  0.016431   percentage      None
# 388         3  2020  jur_and_cpa    1901  0.016431   percentage      None
# 491         3  2021  jur_and_cpa    1901  0.009869   percentage      None
#
#               scenario_desc
# 79   jurisdictions and CPAs
# 182  jurisdictions and CPAs
# 285  jurisdictions and CPAs
# 388  jurisdictions and CPAs
# 491  jurisdictions and CPAs

# to write to csv
controls.to_csv('out/subregional_control_6b.csv')
# controls.to_sql(name='urbansim_lite_subregional_control', con=mssql_engine, schema='urbansim', index=False,if_exists='append')


