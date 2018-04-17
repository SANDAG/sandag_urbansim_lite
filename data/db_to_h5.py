import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import utils


db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

scenarios = utils.yaml_to_dict('scenario_config.yaml', 'scenario')

parcel_sql = '''
      SELECT parcel_id, p.mgra_id, 
             cap_jurisdiction_id,
             jurisdiction_id,
             p.luz_id, p.site_id, capacity_2 AS capacity_base_yr, 
             du_2017 AS residential_units, 
             0 as partial_build,
             'jur' as type
      FROM urbansim.urbansim.parcel p
      WHERE capacity_2 != 0 and capacity_2 is not null
'''
parcels_df = pd.read_sql(parcel_sql, mssql_engine)


assigned_parcel_sql = '''
SELECT [version_id]
      ,[jur_id]
      ,[parcel_id]
      ,[type]
      ,[name]
      ,[du]
  FROM [urbansim].[urbansim].[additional_capacity]
  where version_id = %s
'''
assigned_parcel_sql = assigned_parcel_sql % scenarios['additional_capacity_version']
assigned_df = pd.read_sql(assigned_parcel_sql, mssql_engine)


all_parcel_sql = '''
      SELECT parcel_id, mgra_id, cap_jurisdiction_id, 
             jurisdiction_id, luz_id, site_id, capacity_2 AS base_cap, 
             du_2017 AS residential_units, (du_2017 + capacity_2) as buildout
      FROM urbansim.urbansim.parcel
'''
all_parcels_df = pd.read_sql(all_parcel_sql, mssql_engine)

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
xref_geography_df = pd.read_sql(xref_geography_sql, mssql_engine)

households_sql = '''
    SELECT [yr]
          ,[version_id]
          ,[housing_units_add]
    FROM [isam].[economic_output].[urbansim_housing_units]
    WHERE version_id = %s
'''

households_sql = households_sql % scenarios['demographic_simulation_id']


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
        capacity_2
    FROM urbansim.urbansim.parcel p
    WHERE capacity_2 < 0 and site_id is null
'''

regional_capacity_controls_sql = '''
    SELECT subregional_crtl_id, yr, geo,
           geo_id, control, control_type, max_units
      FROM urbansim.urbansim.urbansim_lite_subregional_control
     WHERE subregional_crtl_id = %s
'''
regional_capacity_controls_sql = regional_capacity_controls_sql % scenarios['subregional_ctrl_id']

parcel_dev_control_sql = '''
SELECT [parcel_id]
      ,[phase_yr]
      ,[phase_yr_version_id]
      ,[type]
FROM  [urbansim].[urbansim].[urbansim_lite_parcel_control]
     WHERE phase_yr_version_id = %s
'''
parcel_dev_control_sql  = parcel_dev_control_sql % scenarios['parcel_phase_yr']

parcels = pd.merge(parcels_df,xref_geography_df,left_on='mgra_id',right_on='mgra_13')
parcels.loc[parcels.cap_jurisdiction_id == 19,'jur_or_cpa_id'] = parcels['cocpa_2016']
parcels.loc[((parcels.cap_jurisdiction_id == 19) & (parcels.jur_or_cpa_id.isnull())),'jur_or_cpa_id'] = parcels['cocpa_13']
parcels.loc[parcels.cap_jurisdiction_id == 14,'jur_or_cpa_id'] = parcels['cicpa_13']
parcels['jur_or_cpa_id'].fillna(parcels['cap_jurisdiction_id'],inplace=True)
parcels.parcel_id = parcels.parcel_id.astype(int)
parcels.type = parcels.type.astype(str)
parcels.jur_or_cpa_id = parcels.jur_or_cpa_id.astype(int)
parcels.set_index('parcel_id',inplace=True)
parcels.sort_index(inplace=True)
parcels.loc[parcels.mgra_id==19415,'jur_or_cpa_id'] = 1909
parcels = parcels.drop(['mgra_13','luz_13','cocpa_13','cocpa_2016','jurisdiction_2016','cicpa_13'], axis=1)

all_parcels = pd.merge(all_parcels_df,xref_geography_df,how='left',left_on='mgra_id',right_on='mgra_13')
all_parcels.parcel_id = all_parcels.parcel_id.astype(int)
all_parcels.set_index('parcel_id',inplace=True)
all_parcels.sort_index(inplace=True)
all_parcels.loc[all_parcels.cap_jurisdiction_id == 19,'jur_or_cpa_id'] = all_parcels['cocpa_2016']
all_parcels.loc[((all_parcels.cap_jurisdiction_id == 19) & (all_parcels.jur_or_cpa_id.isnull())),'jur_or_cpa_id'] = all_parcels['cocpa_13']
all_parcels.loc[all_parcels.cap_jurisdiction_id == 14,'jur_or_cpa_id'] = all_parcels['cicpa_13']
all_parcels = all_parcels.drop(['mgra_13','luz_13','cocpa_13','cocpa_2016','jurisdiction_2016','cicpa_13'],axis=1)
all_parcels.mgra_id = all_parcels.mgra_id.astype(float)
#There are missing MGRAs / LUZs, spacecore has them but they are parcels with multiple MGRAs /other oddities

parcels['buildout'] = parcels['residential_units'] + parcels['capacity_base_yr']
sched_dev_df = pd.read_sql(sched_dev_sql, mssql_engine, index_col='site_id')
households_df = pd.read_sql(households_sql, mssql_engine, index_col='yr')
households_df['total_housing_units'] = households_df.housing_units_add.cumsum()
hu_forecast_df = pd.read_sql(buildings_sql, mssql_engine, index_col='hu_forecast_id')
hu_forecast_df['source'] = 'existing'
devyear_df = pd.read_sql(parcel_dev_control_sql, mssql_engine, index_col='parcel_id')
devyear_df['type'] = devyear_df['type'].astype(str)

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
