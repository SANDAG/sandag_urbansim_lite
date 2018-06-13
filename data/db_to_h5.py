import pandas as pd
from sqlalchemy import create_engine
from database import get_connection_string
import utils

# Link to SQL Server
db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

# Pull the scenario input information, used to select the version_id from various SQL tables below.
scenarios = utils.yaml_to_dict('scenario_config.yaml', 'scenario')

# NOTE: All SQL statements should use an "ORDER BY" statement to ensure consistent queries from different machines.
# Failure to do so can result in inconsistent outputs, even if all other input requirements are the same.

# SQL statement for parcels with jurisdiction-provided capacity (excludes scheduled development).
parcel_sql = '''
SELECT [parcel_id]
    ,[mgra_id]
    ,[cap_jurisdiction_id]
    ,[jurisdiction_id]
    ,[luz_id]
    ,[site_id]
    ,[capacity_2] AS capacity
    ,[du_2017] AS residential_units
    ,[development_type_id_2017] AS dev_type_2017
    ,[development_type_id_2015] AS dev_type_2015
    ,[lu_2015]
    ,[lu_2017]
    ,[lu_2017] AS lu_sim
    ,'jur' AS capacity_type
    ,0 AS capacity_used
    ,0 AS partial_build
FROM [urbansim].[urbansim].[parcel]
WHERE [capacity_2] > 0 and [site_id] IS NULL
ORDER BY parcel_id
'''
parcels_df = pd.read_sql(parcel_sql, mssql_engine)
parcels_df['site_id'] = parcels_df.site_id.astype(float)

# SQL statement for all parcels (excludes scheduled development).
all_parcel_sql = '''
SELECT [parcel_id]
    ,[mgra_id]
    ,[cap_jurisdiction_id]
    ,[jurisdiction_id]
    ,[luz_id]
    ,[site_id]
    ,[capacity_2] AS capacity
    ,[du_2017] AS residential_units
    ,[development_type_id_2017] AS dev_type_2017
    ,[development_type_id_2015] AS dev_type_2015
    ,[lu_2015]
    ,[lu_2017]
    ,[lu_2017] AS lu_sim
    ,'jur' AS capacity_type
    ,0 AS capacity_used
    ,0 AS partial_build
FROM [urbansim].[urbansim].[parcel]
WHERE [parcel_id] NOT IN (
SELECT [parcel_id] FROM [urbansim].[urbansim].[scheduled_development_priority])
ORDER BY [parcel_id]
'''
all_parcels_df = pd.read_sql(all_parcel_sql, mssql_engine)
all_parcels_df['site_id'] = all_parcels_df.site_id.astype(float)

# SQL statement for parcels with additional (SGOA and ADU) capacity.
assigned_parcel_sql = '''
SELECT a.[parcel_id]
    ,p.[mgra_id]
    ,p.[cap_jurisdiction_id]
    ,p.[jurisdiction_id]
    ,p.[luz_id]
    ,p.[site_id]
    ,a.[du] AS capacity
    ,p.[du_2017] AS residential_units
    ,p.[development_type_id_2017] AS dev_type_2017
    ,p.[development_type_id_2015] AS dev_type_2015
    ,p.[lu_2015]
    ,p.[lu_2017]
    ,p.[lu_2017] AS lu_sim
    ,a.[type] AS capacity_type
    ,0 as capacity_used
    ,0 as partial_build
FROM [urbansim].[urbansim].[additional_capacity] AS a
JOIN [urbansim].[parcel] AS p 
ON p.[parcel_id] = a.[parcel_id]
WHERE [version_id] = %s
ORDER BY a.[parcel_id]
'''
assigned_parcel_sql = assigned_parcel_sql % scenarios['additional_capacity_version']
assigned_df = pd.read_sql(assigned_parcel_sql, mssql_engine)
assigned_df['site_id'] = assigned_df.site_id.astype(float)

# Add the additional capacity information to the two parcel tables (excludes scheduled development).
parcels_df = pd.concat([parcels_df, assigned_df])
all_parcels_df = pd.concat([all_parcels_df, assigned_df])

# SQL statement for scheduled development parcels.
# As of 06/06/2018 scheduled_development is being built on a priority system, rather than by scheduled date. Each
# site_id (and all parcels included in it) are assigned a value 1-10. These priorities have been randomly assigned.
sched_dev_sql = '''
SELECT s.[parcel_id]
    ,p.[mgra_id]
    ,p.[cap_jurisdiction_id]
    ,p.[jurisdiction_id]
    ,p.[luz_id]
    ,s.[site_id]
    ,s.[capacity_3] AS capacity
    ,p.[du_2017] AS residential_units
    ,p.[development_type_id_2017] AS dev_type_2017
    ,p.[development_type_id_2015] AS dev_type_2015
    ,p.[lu_2015]
    ,p.[lu_2017]
    ,p.[lu_2017] AS lu_sim
    ,'sch' AS capacity_type
    ,0 as capacity_used
    ,s.[priority]
FROM [urbansim].[urbansim].[parcel] AS p
INNER JOIN [urbansim].[urbansim].[scheduled_development_priority] as s
ON p.[parcel_id] = s.[parcel_id]
WHERE s.[sched_version_id] = %s
ORDER BY s.[parcel_id]
'''
sched_dev_sql = sched_dev_sql % scenarios['sched_dev_version']
sched_dev_df = pd.read_sql(sched_dev_sql, mssql_engine)

# SQL statements for geography area names.
# Can this be condensed? luz = 64, cicpa = 147, cocpa = 148, jurs = 150 (and we only use jurs currently).
luz_names_sql = '''
SELECT [zone]
    ,[name]
FROM [data_cafe].[ref].[geography_zone]
WHERE [geography_type_id] = 64
ORDER BY [zone]
'''

cicpa_names_sql = '''
SELECT [zone]
    ,[name]
FROM [data_cafe].[ref].[geography_zone]
WHERE [geography_type_id] = 147
ORDER BY [zone]
'''

cocpa_names_sql = '''
SELECT [zone]
    ,[name]
FROM [data_cafe].[ref].[geography_zone]
WHERE [geography_type_id] = 148
ORDER BY [zone]
'''

jurisdictions_names_sql = '''
SELECT [zone]
    ,[name]
FROM [data_cafe].[ref].[geography_zone]
WHERE [geography_type_id] = 150
ORDER BY [zone]
'''
jurisdictions_df = pd.read_sql(jurisdictions_names_sql, mssql_engine)
jurisdictions_df['name'] = jurisdictions_df['name'].astype(str)

# SQL statement for the target units per year table.
households_sql = '''
SELECT [yr]
    ,[housing_units_add]
FROM [urbansim].[urbansim].[urbansim_target_housing_units]
WHERE [version_id] = %s
ORDER BY [yr]
'''
households_sql = households_sql % scenarios['target_housing_units_version']
households_df = pd.read_sql(households_sql, mssql_engine, index_col='yr')
households_df['total_housing_units'] = households_df.housing_units_add.cumsum()

# SQL statement for the output table.
# This should be empty when pulled, as no buildings are listed as being built in 2016 or later.
hu_forecast_sql = '''
SELECT [parcel_id]
    ,0 AS units_added
    ,COALESCE(year_built, 0) AS year_built
    ,0 AS source
    ,'no_cap' AS capacity_type
FROM [urbansim].[urbansim].[building]
WHERE [year_built] > 2016
ORDER BY [parcel_id]
'''
hu_forecast_df = pd.read_sql(hu_forecast_sql, mssql_engine)
hu_forecast_df.capacity_type = hu_forecast_df.capacity_type.astype(str)

# SQL statement for parcels with negative capacity (excludes scheduled development).
# As of 06/06/2018, there are no parcels with a negative capacity.
negative_capacity_parcels = '''
SELECT [parcel_id]
    ,[site_id]
    ,[capacity_2]
    ,NULL AS yr
    ,'neg_cap' AS capacity_type
FROM [urbansim].[urbansim].[parcel]
WHERE [capacity_2] < 0 AND [site_id] IS NULL
ORDER BY [parcel_id]
'''
negative_parcels_df = pd.read_sql(negative_capacity_parcels, mssql_engine)
negative_parcels_df.capacity_type = negative_parcels_df.capacity_type.astype(str)

# SQL statement for sub-regional allocations.
regional_capacity_controls_sql = '''
SELECT [subregional_crtl_id]
    ,[yr]
    ,[geo]
    ,[geo_id]
    ,[control]
    ,[control_type]
    ,[max_units]
FROM [urbansim].[urbansim].[urbansim_lite_subregional_control]
WHERE [subregional_crtl_id] = %s
ORDER BY [yr], [geo_id]
'''
regional_capacity_controls_sql = regional_capacity_controls_sql % scenarios['subregional_ctrl_id']
regional_controls_df = pd.read_sql(regional_capacity_controls_sql, mssql_engine)
regional_controls_df['control_type'] = regional_controls_df['control_type'].astype(str)
regional_controls_df['geo'] = regional_controls_df['geo'].astype(str)

# SQL statement for parcel control table. This is used to phase specific parcels.
parcel_dev_control_sql = '''
SELECT [parcel_id]
    ,[phase_yr]
    ,[capacity_type]
FROM [urbansim].[urbansim].[urbansim_lite_parcel_control]
WHERE [phase_yr_version_id] = %s
ORDER BY [parcel_id]
'''
parcel_dev_control_sql = parcel_dev_control_sql % scenarios['parcel_phase_yr']
devyear_df = pd.read_sql(parcel_dev_control_sql, mssql_engine, index_col='parcel_id')
devyear_df['capacity_type'] = devyear_df['capacity_type'].astype(str)

# SQL statement to retrieve General Plan Land Use information for each parcel.
gplu_sql = '''
SELECT [parcel_id]
    ,[gplu] AS plu
FROM [urbansim].[urbansim].[general_plan_parcel]
ORDER BY [parcel_id]
'''
gplu_df = pd.read_sql(gplu_sql, mssql_engine)

# SQL statement to match land use codes to development type.
dev_lu_sql = '''
SELECT [development_type_id] AS dev_type_sim
    ,[lu_code] as lu_sim
FROM [urbansim].[ref].[development_type_lu_code]
ORDER BY [development_type_id]
'''
dev_lu_df = pd.read_sql(dev_lu_sql, mssql_engine)

# SQL statement for cpa and taz information by parcel. This is used to add additional geography information.
geography_view_sql = '''
SELECT [parcel_id]
    ,[taz_13] AS taz
    ,[jcpa] AS jur_or_cpa_id
FROM [urbansim].[ref].[vi_parcel_xref]
ORDER BY [parcel_id]
'''
geography_view_df = pd.read_sql(geography_view_sql, mssql_engine)

# SQL statement for the target ADU units per year table.
adu_allocation_sql = '''
SELECT [yr]
    ,[allocation]
FROM [urbansim].[urbansim].[urbansim_lite_adu_control]
WHERE [version_id] = %s
ORDER BY [yr]
'''
adu_allocation_sql = adu_allocation_sql % scenarios['adu_control']
adu_allocation_df = pd.read_sql(adu_allocation_sql, mssql_engine)

# Combine capacity parcel table with additional geography and plu information.
parcels = pd.merge(parcels_df, geography_view_df, how='left', on='parcel_id')
parcels.parcel_id = parcels.parcel_id.astype(int)
parcels.capacity_type = parcels.capacity_type.astype(str)
parcels.jur_or_cpa_id = parcels.jur_or_cpa_id.astype(int)
parcels = pd.merge(parcels, gplu_df,  how='left', on='parcel_id')
parcels.sort_index(inplace=True)

# Combine all parcel table with additional geography and plu information.
all_parcels = pd.merge(all_parcels_df, geography_view_df, how='left', on='parcel_id')
all_parcels.parcel_id = all_parcels.parcel_id.astype(int)
all_parcels.capacity_type = all_parcels.capacity_type.astype(str)
all_parcels = all_parcels.loc[~all_parcels.jur_or_cpa_id.isnull()].copy()
all_parcels.jur_or_cpa_id = all_parcels.jur_or_cpa_id.astype(int)
all_parcels = pd.merge(all_parcels, gplu_df, how='left', on='parcel_id')
all_parcels.sort_index(inplace=True)

# Combine scheduled development parcel table with additional geography information.
sched_dev = pd.merge(sched_dev_df, geography_view_df, how='left', on='parcel_id')
sched_dev.jur_or_cpa_id = sched_dev.jur_or_cpa_id.astype(int)
sched_dev.parcel_id = sched_dev.parcel_id.astype(int)
sched_dev.capacity_type = sched_dev.capacity_type.astype(str)

# Store all the above tables in a .h5 file for use with orca. These are called via datasources.py, or directly retrieved
# in certain portions of utils.py and bulk_insert.py.
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
