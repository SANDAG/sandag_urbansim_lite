import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from pysandag.database import get_connection_string


db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)


parcel_sql = '''
      SELECT	parcel_id, p.mgra_id, cap_jurisdiction_id as jurisdiction_id, 
        jurisdiction_id as orig_jurisdiction_id,
            p.luz_id, p.site_id, capacity AS capacity_base_yr, 
            du AS residential_units, 
            0 as partial_build
       FROM urbansim.urbansim.parcel p
      WHERE capacity > 0
'''


all_parcel_sql = '''
      SELECT parcel_id, mgra_id as mgra, cap_jurisdiction_id as jur_reported, 
        jurisdiction_id as jur, luz_id as luz, site_id, cap_remaining_new AS base_cap, 
        du_2017 AS base_hu, (du_2017 + cap_remaining_new) as buildout
        FROM urbansim.urbansim.parcel
'''


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
      WHERE cap_remaining_new > 0 and jurisdiction_id NOT IN (14,19)
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
      WHERE capacity > 0 and jurisdiction_id IN (14,19)
'''

sched_dev_sql = '''
    SELECT parcel_id, yr, site_id, 
           res_units, max(yr) over (partition by site_id) as final_year
      FROM urbansim.urbansim.scheduled_development_do_not_use
     WHERE scenario = 1 and yr > 2016
'''

luz_names_sql = '''
    SELECT zone, name
      FROM data_cafe.ref.geography_zone
     WHERE geography_type_id = 64
'''

jurisdictions_names_sql = '''
    SELECT zone, name
      FROM data_cafe.ref.geography_zone
     WHERE geography_type_id = 150
'''

cicpa_names_sql = '''
    SELECT zone, name
      FROM data_cafe.ref.geography_zone
     WHERE geography_type_id = 147
'''

cocpa_names_sql = '''
    SELECT zone, name
      FROM data_cafe.ref.geography_zone
     WHERE geography_type_id = 148
'''

xref_geography_sql = '''
    SELECT mgra_13, luz_13, cocpa_13, cocpa_2016,
           jurisdiction_2016, cicpa_13
      FROM data_cafe.ref.vi_xref_geography_mgra_13
'''

households_sql = '''
    SELECT  yr as year, households, housing_units_add 
      FROM  isam.economic_output.urbansim_housing_units
'''

buildings_sql = '''
    SELECT building_id as hu_forecast_id
        ,parcel_id
        ,COALESCE(development_type_id,0) AS hu_forecast_type_id
        ,COALESCE(residential_units,0) AS residential_units
        ,COALESCE(year_built,0) AS year_built
     FROM urbansim.urbansim.building
     where year_built > 2015
'''

negative_capacity_parcels = '''
    SELECT parcel_id, 
        p.site_id,
        null as yr,
        capacity
    FROM urbansim.urbansim.parcel p
    WHERE capacity < 0 and site_id is null
'''

regional_capacity_controls_sql = '''
    SELECT scenario, yr, geo,
           geo_id, control, control_type, max_units
      FROM urbansim.urbansim.urbansim_lite_subregional_control
     WHERE scenario = 1
'''

parcel_dev_control_sql = '''
    SELECT parcel_id, phase as phase_yr_ctrl, scenario
      FROM urbansim.urbansim.urbansim_lite_parcel_control
     WHERE scenario = 1
'''

xref_geography_df = pd.read_sql(xref_geography_sql, mssql_engine)
xref_geography_df['jur_or_cpa_id'] = xref_geography_df['cocpa_13']
xref_geography_df['jur_or_cpa_id'].fillna(xref_geography_df['cicpa_13'],inplace=True)
xref_geography_df['jur_or_cpa_id'].fillna(xref_geography_df['jurisdiction_2016'],inplace=True)
xref_geography_df['jur_or_cpa_id'] = xref_geography_df['jur_or_cpa_id'].astype(int)

# parcel_update_2017_df = pd.read_sql(parcel_update_2017_sql, mssql_engine)
# parcel_city_and_county_df= pd.read_sql(parcel_city_and_county_sql, mssql_engine)
# parcels_df = pd.concat([parcel_update_2017_df,parcel_city_and_county_df])
parcels_df = pd.read_sql(parcel_sql, mssql_engine)
parcels = pd.merge(parcels_df,xref_geography_df[['mgra_13','jur_or_cpa_id','cocpa_13']],left_on='mgra_id',right_on='mgra_13')
parcels.parcel_id = parcels.parcel_id.astype(int)
parcels.set_index('parcel_id',inplace=True)
parcels.sort_index(inplace=True)
parcels.loc[parcels.jurisdiction_id != parcels.orig_jurisdiction_id,'jur_or_cpa_id'] = parcels['jurisdiction_id']
parcels.loc[parcels.jur_or_cpa_id ==19,'jur_or_cpa_id'] = parcels['cocpa_13']

all_parcels_df = pd.read_sql(all_parcel_sql, mssql_engine)
all_parcels = pd.merge(all_parcels_df,xref_geography_df[['mgra_13','jur_or_cpa_id']],how='left',left_on='mgra',right_on='mgra_13')
all_parcels.parcel_id = all_parcels.parcel_id.astype(int)
all_parcels.set_index('parcel_id',inplace=True)
all_parcels.sort_index(inplace=True)
all_parcels.loc[all_parcels.jur_reported.isnull(),'jur_reported'] = all_parcels['jur']
all_parcels.loc[all_parcels.jur_or_cpa_id < 20, 'jur_or_cpa_id'] = np.nan
all_parcels = all_parcels.drop(['mgra_13'],axis=1)
all_parcels.mgra = all_parcels.mgra.astype(float)
#There are missing MGRAs / LUZs, spacecore has them but they are parcels with multiple MGRAs /other oddities


parcels['buildout'] = parcels['residential_units'] + parcels['capacity_base_yr']
sched_dev_df = pd.read_sql(sched_dev_sql, mssql_engine, index_col='site_id')
households_df = pd.read_sql(households_sql, mssql_engine, index_col='year')
households_df['total_housing_units'] = households_df.housing_units_add.cumsum()
hu_forecast_df = pd.read_sql(buildings_sql, mssql_engine, index_col='hu_forecast_id')
hu_forecast_df['source'] = 'existing'
devyear_df = pd.read_sql(parcel_dev_control_sql, mssql_engine, index_col='parcel_id')
regional_controls_df = pd.read_sql(regional_capacity_controls_sql, mssql_engine)
regional_controls_df['control_type'] = regional_controls_df['control_type'].astype(str)
regional_controls_df['geo'] = regional_controls_df['geo'].astype(str)
jurisdictions_df = pd.read_sql(jurisdictions_names_sql, mssql_engine)
jurisdictions_df['name'] = jurisdictions_df['name'].astype(str)
negative_parcels_df = pd.read_sql(negative_capacity_parcels, mssql_engine)

with pd.HDFStore('urbansim.h5', mode='w') as store:
    store.put('scheduled_development', sched_dev_df, format='table')
    store.put('parcels', parcels, format='table')
    store.put('households',households_df,format='table')
    store.put('hu_forecast', hu_forecast_df, format='table')
    store.put('regional_controls', regional_controls_df, format='table')
    store.put('jurisdictions', jurisdictions_df, format='table')
    store.put('devyear', devyear_df, format='table')
    store.put('negative_parcels', negative_parcels_df, format='table')
    store.put('all_parcels', all_parcels, format='table')
