import pandas as pd
from sqlalchemy import create_engine
from pysandag.database import get_connection_string

db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

#########################################################################################
# note: need to CHANGE when parcel_update_2017 has updated capacities for city and county
#########################################################################################

parcel_update_2017_sql = '''
    SELECT	parcelid_2015 as parcel_id, p.mgra_id, p.jurisdiction_id, 
            p.luz_id, p.site_id, cap_remaining_new AS capacity_base_yr, 
            du_2017 AS residential_units, 
            0 as partial_build
       FROM urbansim.urbansim.parcel_update_2017 update2017
       JOIN urbansim.urbansim.parcel p
         ON p.parcel_id = update2017.parcelid_2015
      WHERE jurisdiction_id NOT IN (14,19)
'''

# parcel_update_2017 does not have city and county capacity updates yet
# problematic since sched dev table has city and county update
# was inflating capacity since site ids did not match
# work around is to use city and county capacity prior to update to 2017
# from urbansim.parcel
# delete this code and get all capacities from parcel_update_2017 when avail
parcel_city_and_county_sql = '''
    SELECT	parcel_id, p.mgra_id, p.jurisdiction_id, 
            p.luz_id, p.site_id, capacity AS capacity_base_yr, 
            du AS residential_units, 
            0 as partial_build
       FROM urbansim.urbansim.parcel p
      WHERE jurisdiction_id IN (14,19)
'''

sched_dev_sql = '''
    SELECT parcel_id, site_id, min(yr) as first_year
     FROM urbansim.urbansim.scheduled_development_do_not_use
     WHERE scenario = 1 and yr > 2016
	 group by parcel_id, site_id
	 order by parcel_id
'''

xref_geography_sql = '''
    SELECT mgra_13, luz_13, cocpa_13, cocpa_2016,
           jurisdiction_2016, cicpa_13
      FROM data_cafe.ref.vi_xref_geography_mgra_13
'''

xref_geography_df = pd.read_sql(xref_geography_sql, mssql_engine)
xref_geography_df['jur_or_cpa_id'] = xref_geography_df['cocpa_13']
xref_geography_df['jur_or_cpa_id'].fillna(xref_geography_df['cicpa_13'],inplace=True)
xref_geography_df['jur_or_cpa_id'].fillna(xref_geography_df['jurisdiction_2016'],inplace=True)
xref_geography_df['jur_or_cpa_id'] = xref_geography_df['jur_or_cpa_id'].astype(int)

parcel_update_2017_df = pd.read_sql(parcel_update_2017_sql, mssql_engine)
parcel_city_and_county_df= pd.read_sql(parcel_city_and_county_sql, mssql_engine)
parcels_df = pd.concat([parcel_update_2017_df,parcel_city_and_county_df])
parcels = pd.merge(parcels_df,xref_geography_df[['mgra_13','jur_or_cpa_id']],left_on='mgra_id',right_on='mgra_13')
parcels.parcel_id = parcels.parcel_id.astype(int)
parcels.set_index('parcel_id',inplace=True)
parcels.sort_index(inplace=True)
parcels['buildout'] = parcels['residential_units'] + parcels['capacity_base_yr']
sched_dev_df = pd.read_sql(sched_dev_sql, mssql_engine)
parcels_full = pd.merge(parcels.loc[:, parcels.columns != 'mgra_13'],sched_dev_df[['parcel_id','first_year','site_id']],
                        how='left',right_on='parcel_id',left_index=True)

#Verify site_id merge:
rows = parcels_full.loc[parcels_full['site_id_x'] != parcels_full['site_id_y']]
rows = rows.loc[~rows['site_id_x'].isnull() & ~rows['site_id_y'].isnull()]
print('%d mismatched parcels by site_id' % (len(rows.parcel_id.tolist())))
for parcel in rows.parcel_id.tolist():
    print(parcel)
    parcels_full.loc[parcels_full.parcel_id == parcel, 'mismatch'] = 'mismatch'

parcels_full.to_csv('parcels_full.csv',index=False)