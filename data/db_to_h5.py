import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from database import get_connection_string
# from pysandag.database import get_connection_string
import utils
import database


db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

scenarios = utils.yaml_to_dict('scenario_config.yaml', 'scenario')

parcel_sql = '''
      SELECT parcel_id, p.mgra_id, 
             cap_jurisdiction_id,
             jurisdiction_id,
             p.luz_id, p.site_id,
             capacity_2 as capacity,
             0 as capacity_used,
             du_2017 AS residential_units, 
             0 as partial_build,
             'jur' as capacity_type,
             development_type_id_2017 as dev_type_2017,
             development_type_id_2015 as dev_type_2015,
             lu_2017 as lu_sim,
             lu_2015,
             lu_2017
      FROM urbansim.urbansim.parcel p
      WHERE capacity_2 > 0 and site_id IS NULL
      ORDER BY parcel_id
'''
parcels_df = pd.read_sql(parcel_sql, mssql_engine)
parcels_df['site_id'] = parcels_df.site_id.astype(float)

assigned_parcel_sql = '''
SELECT  a.parcel_id,
        p.mgra_id, 
        p.cap_jurisdiction_id,
        p.jurisdiction_id,
        p.luz_id,
        p.site_id,
        a.du as capacity,
        0 as capacity_used,
        p.du_2017 as residential_units,
        0 as partial_build,
        type as capacity_type,
        p.development_type_id_2017 as dev_type_2017,
        p.development_type_id_2015 as dev_type_2015,
        p.lu_2017 as lu_sim,
        p.lu_2015,
        p.lu_2017
  FROM [urbansim].[urbansim].[additional_capacity] a
  join urbansim.parcel p on p.parcel_id = a.parcel_id
  where version_id = %s
  ORDER BY a.parcel_id
'''
assigned_parcel_sql = assigned_parcel_sql % scenarios['additional_capacity_version']
assigned_df = pd.read_sql(assigned_parcel_sql, mssql_engine)
assigned_df['site_id'] = assigned_df.site_id.astype(float)

parcels_df = pd.concat([parcels_df, assigned_df])

all_parcel_sql = '''
      SELECT parcel_id, p.mgra_id, 
             cap_jurisdiction_id,
             jurisdiction_id,
             p.luz_id,
             NULL as site_id,
             capacity_2 as capacity,
             0 as capacity_used,
             du_2017 AS residential_units, 
             0 as partial_build,
             'jur' as capacity_type,
             development_type_id_2017 as dev_type_2017,
             development_type_id_2015 as dev_type_2015,
             lu_2017 as lu_sim,
             lu_2015,
             lu_2017
      FROM urbansim.urbansim.parcel p
      where parcel_id not in (select parcel_id from urbansim.urbansim.scheduled_development_priority)
      ORDER BY parcel_id
'''
all_parcels_df = pd.read_sql(all_parcel_sql, mssql_engine)
all_parcels_df['site_id'] = all_parcels_df.site_id.astype(float)
all_parcels_df = pd.concat([all_parcels_df, assigned_df])

sched_dev_sql = '''
    SELECT s.parcel_id, p.mgra_id, 
            p.cap_jurisdiction_id, 
            p.jurisdiction_id, 
            p.luz_id, 
            s.site_id, 
            s.capacity_3 as capacity, 
            p.du_2017 as residential_units, 
            s.priority, 'sch' as capacity_type,
            0 as capacity_used, 
            p.development_type_id_2017 as dev_type_2017,
            p.development_type_id_2015 as dev_type_2015,
            p.lu_2017 as lu_sim,
            p.lu_2015,
            p.lu_2017
    FROM urbansim.urbansim.parcel as p
        inner join [urbansim].[urbansim].[scheduled_development_priority] as s
        on p.parcel_id = s.parcel_id
        WHERE s.sched_version_id = %s
        ORDER BY s.parcel_id
'''
sched_dev_sql = sched_dev_sql % scenarios['sched_dev_version']

luz_names_sql = '''
    SELECT zone, name
      FROM data_cafe.ref.geography_zone
     WHERE geography_type_id = 64
     ORDER BY zone
'''

jurisdictions_names_sql = '''
    SELECT zone, name
      FROM data_cafe.ref.geography_zone
     WHERE geography_type_id = 150
     ORDER BY zone
'''

cicpa_names_sql = '''
    SELECT zone, name
      FROM data_cafe.ref.geography_zone
     WHERE geography_type_id = 147
     ORDER BY zone
'''

cocpa_names_sql = '''
    SELECT zone, name
      FROM data_cafe.ref.geography_zone
     WHERE geography_type_id = 148
     ORDER BY zone
'''

xref_geography_sql = '''
    SELECT mgra_13, luz_13, cocpa_13, cocpa_2016,
           jurisdiction_2016, cicpa_13
      FROM data_cafe.ref.vi_xref_geography_mgra_13
      ORDER BY mgra_13
'''
xref_geography_df = pd.read_sql(xref_geography_sql, mssql_engine)

households_sql = '''
  SELECT  [yr],[housing_units_add]
     FROM [urbansim].[urbansim].[urbansim_target_housing_units]
    WHERE [version_id] = %s
    ORDER BY yr
'''
households_sql = households_sql % scenarios['target_housing_units_version']

hu_forecast_sql = '''
    SELECT parcel_id
        ,0 as units_added
        ,COALESCE(year_built,0) AS year_built
        ,0 as source
        ,'no_cap' as capacity_type
     FROM urbansim.urbansim.building
     where year_built > 2016
     ORDER BY parcel_id
'''

negative_capacity_parcels = '''
    SELECT parcel_id, 
        p.site_id,
        null as yr,
        capacity_2,
        'neg_cap' as capacity_type
    FROM urbansim.urbansim.parcel p
    WHERE capacity_2 < 0 and site_id is null
    ORDER BY parcel_id
'''

regional_capacity_controls_sql = '''
    SELECT subregional_crtl_id, yr, geo,
           geo_id, control, control_type, max_units
      FROM urbansim.urbansim.urbansim_lite_subregional_control
     WHERE subregional_crtl_id = %s
     ORDER BY yr,geo
'''
regional_capacity_controls_sql = regional_capacity_controls_sql % scenarios['subregional_ctrl_id']

parcel_dev_control_sql = '''
SELECT [parcel_id]
      ,[phase_yr]
      ,[capacity_type]
FROM  [urbansim].[urbansim].[urbansim_lite_parcel_control]
     WHERE phase_yr_version_id = %s
     ORDER BY parcel_id
'''
parcel_dev_control_sql = parcel_dev_control_sql % scenarios['parcel_phase_yr']

gplu_sql = '''
SELECT parcel_id, gplu as plu
  FROM [urbansim].[urbansim].[general_plan_parcel]
  ORDER BY parcel_id
'''
gplu_df = pd.read_sql(gplu_sql, mssql_engine)

dev_lu_sql = '''
SELECT development_type_id as dev_type_sim, lu_code as lu_sim
    FROM urbansim.ref.development_type_lu_code
    ORDER BY development_type_id
'''
dev_lu_df = pd.read_sql(dev_lu_sql, mssql_engine)

geography_view_sql = '''
SELECT [parcel_id]
      ,[taz_13] as taz
      ,[jcpa] as jur_or_cpa_id
  FROM [urbansim].[ref].[vi_parcel_xref]
  ORDER BY parcel_id'''
geography_view_df = pd.read_sql(geography_view_sql, mssql_engine)

adu_allocation_sql = '''
SELECT [yr]
      ,[allocation]
  FROM [urbansim].[urbansim].[urbansim_lite_adu_control]
  where version_id = %s
  ORDER BY yr
'''

adu_allocation_sql = adu_allocation_sql % scenarios['adu_control']
adu_allocation_df = pd.read_sql(adu_allocation_sql, mssql_engine)

parcels = pd.merge(parcels_df, geography_view_df, how='left', on='parcel_id')
parcels.parcel_id = parcels.parcel_id.astype(int)
parcels.capacity_type = parcels.capacity_type.astype(str)
parcels.jur_or_cpa_id = parcels.jur_or_cpa_id.astype(int)
parcels = pd.merge(parcels, gplu_df,  how='left', on='parcel_id')
parcels.sort_index(inplace=True)


all_parcels = pd.merge(all_parcels_df, geography_view_df, how='left', on='parcel_id')
all_parcels.parcel_id = all_parcels.parcel_id.astype(int)
all_parcels.capacity_type = all_parcels.capacity_type.astype(str)
all_parcels = all_parcels.loc[~all_parcels.jur_or_cpa_id.isnull()].copy()
all_parcels.jur_or_cpa_id = all_parcels.jur_or_cpa_id.astype(int)
all_parcels = pd.merge(all_parcels, gplu_df, how='left', on='parcel_id')
all_parcels.sort_index(inplace=True)


sched_dev_df = pd.read_sql(sched_dev_sql, mssql_engine)
sched_dev = pd.merge(sched_dev_df, geography_view_df, how='left', on='parcel_id')
sched_dev.jur_or_cpa_id = sched_dev.jur_or_cpa_id.astype(int)
sched_dev.parcel_id = sched_dev.parcel_id.astype(int)
sched_dev.capacity_type = sched_dev.capacity_type.astype(str)


households_df = pd.read_sql(households_sql, mssql_engine, index_col='yr')
households_df['total_housing_units'] = households_df.housing_units_add.cumsum()
hu_forecast_df = pd.read_sql(hu_forecast_sql, mssql_engine)
hu_forecast_df.capacity_type = hu_forecast_df.capacity_type.astype(str)
devyear_df = pd.read_sql(parcel_dev_control_sql, mssql_engine, index_col='parcel_id')
devyear_df['capacity_type'] = devyear_df['capacity_type'].astype(str)


regional_controls_df = pd.read_sql(regional_capacity_controls_sql, mssql_engine)
regional_controls_df['control_type'] = regional_controls_df['control_type'].astype(str)
regional_controls_df['geo'] = regional_controls_df['geo'].astype(str)
jurisdictions_df = pd.read_sql(jurisdictions_names_sql, mssql_engine)
jurisdictions_df['name'] = jurisdictions_df['name'].astype(str)
negative_parcels_df = pd.read_sql(negative_capacity_parcels, mssql_engine)
negative_parcels_df.capacity_type = negative_parcels_df.capacity_type.astype(str)


with pd.HDFStore('urbansim.h5', mode='w') as store:
    store.put('scheduled_development', sched_dev, format='table')
    store.put('parcels', parcels, format='table')
    store.put('households', households_df, format='table')
    store.put('hu_forecast', hu_forecast_df)
    store.put('regional_controls', regional_controls_df, format='table')
    store.put('jurisdictions', jurisdictions_df, format='table')
    store.put('devyear', devyear_df, format='table')
    store.put('negative_parcels', negative_parcels_df, format='table')
    store.put('all_parcels', all_parcels, format='table')
    store.put('dev_lu_table', dev_lu_df, format='table')
    store.put('adu_allocation', adu_allocation_df, format='table')
