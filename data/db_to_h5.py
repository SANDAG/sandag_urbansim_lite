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
    SELECT scenario, parcel_id, yr, site_id, 
           res_units, job_spaces, households, jobs
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
    SELECT building_id
        ,parcel_id
        ,COALESCE(development_type_id,0) AS building_type_id
        ,COALESCE(residential_units,0) AS residential_units
        ,COALESCE(year_built,0) AS year_built
     FROM urbansim.urbansim.building
     where year_built > 2015
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

parcel_update_2017_df = pd.read_sql(parcel_update_2017_sql, mssql_engine)
parcel_city_and_county_df= pd.read_sql(parcel_city_and_county_sql, mssql_engine)
parcels_df = pd.concat([parcel_update_2017_df,parcel_city_and_county_df])
parcels = pd.merge(parcels_df,xref_geography_df[['mgra_13','jur_or_cpa_id']],left_on='mgra_id',right_on='mgra_13')
parcels.parcel_id = parcels.parcel_id.astype(int)
parcels.set_index('parcel_id',inplace=True)
parcels.sort_index(inplace=True)
parcels['buildout'] = parcels['residential_units'] + parcels['capacity_base_yr']
sched_dev_df = pd.read_sql(sched_dev_sql, mssql_engine, index_col='site_id')
households_df = pd.read_sql(households_sql, mssql_engine, index_col='year')
households_df['total_housing_units'] = households_df.housing_units_add.cumsum()
buildings_df = pd.read_sql(buildings_sql, mssql_engine, index_col='building_id')
buildings_df['source'] = 'existing'
devyear_df = pd.read_sql(parcel_dev_control_sql, mssql_engine, index_col='parcel_id')
regional_controls_df = pd.read_sql(regional_capacity_controls_sql, mssql_engine)
regional_controls_df['control_type'] = regional_controls_df['control_type'].astype(str)
regional_controls_df['geo'] = regional_controls_df['geo'].astype(str)
jurisdictions_df = pd.read_sql(jurisdictions_names_sql, mssql_engine)
jurisdictions_df['name'] = jurisdictions_df['name'].astype(str)

with pd.HDFStore('urbansim.h5', mode='w') as store:
    store.put('scheduled_development', sched_dev_df, format='table')
    store.put('parcels', parcels, format='table')
    store.put('households',households_df,format='table')
    store.put('buildings', buildings_df, format='table')
    store.put('regional_controls', regional_controls_df, format='table')
    store.put('jurisdictions', jurisdictions_df, format='table')
    store.put('devyear', devyear_df, format='table')
