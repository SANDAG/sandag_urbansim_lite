import pandas as pd
from sqlalchemy import create_engine
from pysandag.database import get_connection_string

db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

phase_year_sql = '''
    WITH parcels_with_multiple_lckeys AS (
		SELECT parcels.parcel_id
		FROM urbansim.urbansim.parcel parcels
		JOIN spacecore.input.forecast_landcore lc
		ON lc.parcelid = parcels.parcel_id
		WHERE parcels.capacity > 0 and site_id is NULL
		GROUP BY parcels.parcel_id
		HAVING count(parcels.parcel_id) > 1)
	SELECT parcel_id
	FROM urbansim.urbansim.parcel parcels
	JOIN spacecore.input.forecast_landcore lc
	ON lc.parcelId = parcels.parcel_id
	LEFT JOIN regional_forecast.sr13_final.capacity sr13_cap
	inner join [regional_forecast].[sr13_final].[mgra13] AS y ON sr13_cap.mgra = y.mgra
	ON sr13_cap.LCKey = lc.LCKey
	WHERE (parcels.capacity > 0 or site_id is not NULL) AND
		parcels.parcel_id NOT IN (SELECT parcel_id FROM parcels_with_multiple_lckeys) and 
		sr13_cap.scenario=0  and cpa = 1904 and site_id IS NULL and increment > 2035 and lc.LCKey IN 
			(SELECT lckey from regional_forecast.sr13_final.capacity sr13_cap WHERE cap_hs > 0)
	GROUP BY parcel_id
'''
phase_year_parcels_df = pd.read_sql(phase_year_sql, mssql_engine)
# These are all parcels from 1904 (Unincorporated Desert Region) that had capacity remaining after 2040 in series 13)

parcel_dev_control_sql = '''
    SELECT parcel_id, phase, scenario
      FROM urbansim.urbansim.urbansim_lite_parcel_control
     WHERE scenario = 0
'''
devyear_df = pd.read_sql(parcel_dev_control_sql, mssql_engine, index_col='parcel_id')

for parcel in phase_year_parcels_df['parcel_id']:
    devyear_df.loc[parcel].phase = 2045

devyear_df['scenario'] = 1

devyear_df.to_sql(name='urbansim_lite_parcel_control', con=mssql_engine, schema='urbansim', index=True, if_exists='append')
