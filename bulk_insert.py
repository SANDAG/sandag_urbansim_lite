import pandas as pd
import orca
import numpy as np
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pyodbc

db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)
conn = mssql_engine.connect()


def run_insert(year):
    if year == 2017:
        try:
            scenario_sql = '''
            SELECT max(scenario)
              FROM [urbansim].[urbansim].[sr14_residential_parcel_results]
            '''
            scenario_df = pd.read_sql(scenario_sql, mssql_engine)
            scenario = int(scenario_df.values) + 1
        except:
            cursor = conn.cursor()
            cursor.execute(
                '''
                DROP TABLE IF EXISTS urbansim.urbansim.sr14_residential_parcel_results
                '''
            )
            conn.commit()
            conn.execute(
                '''
                USE [urbansim]
                GO
    
                SET ANSI_NULLS ON
                GO
    
                SET QUOTED_IDENTIFIER ON
                GO
    
                CREATE TABLE [urbansim].[sr14_residential_parcel_results](
                    [scenario] [tinyint] NOT NULL,
                    [increment] [int] NOT NULL,
                    [parcel_id] [int] NOT NULL,
                    [year] [int] NOT NULL,
                    [jur] [smallint] NOT NULL,
                    [jur_reported] [smallint] NOT NULL,
                    [cpa] [int] NULL,
                    [mgra] [int] NULL,
                    [luz] [smallint] NULL,
                    [taz] [int] NULL,
                    [site_id] [smallint] NULL,
                    [lu] [smallint] NULL,
                    [hs] [int] NOT NULL,
                    [chg_hs] [int] NOT NULL,
                    [cap_hs] [int] NOT NULL,
                       [source] [smallint] NOT NULL,
                       [phase] [int] NULL
                    CONSTRAINT [PK_sr14_residential_parcel_yearly] PRIMARY KEY CLUSTERED 
                   (
                       [scenario] ASC,
                       [year] ASC,
                       [parcel_id] ASC,
                       [source] ASC
                   ))WITH (DATA_COMPRESSION = page)
                   GO
                '''
            )
            conn.commit()
            cursor.close()
            scenario = int(1)

    else:
        scenario_sql = '''
                    SELECT max(scenario)
                      FROM [urbansim].[urbansim].[sr14_residential_parcel_results]
                    '''
        scenario_df = pd.read_sql(scenario_sql, mssql_engine)
        scenario = int(scenario_df.values)

    all_parcels = orca.get_table('all_parcels').to_frame()
    phase_year = orca.get_table('devyear').to_frame()
    hu_forecast = orca.get_table('hu_forecast').to_frame()
    hu_forecast = hu_forecast[~hu_forecast['year_built'] != year]

    year_update = pd.merge(all_parcels, hu_forecast[['parcel_id','res_units','source']],how='left',left_index=True,
                           right_on='parcel_id')
    year_update = year_update.join(phase_year,how='left')
    year_update['scenario'] = scenario
    year_update['year'] = year
    year_update['taz'] = np.nan
    year_update['lu'] = np.nan
    #year_update['hs'] =
    year_update.rename(columns={"res_units":"chg_hs", "B": "c"},inplace=True)


    all_parcels.parcel_id = all_parcels.parcel_id.astype(int)
    all_parcels.set_index('parcel_id', inplace=True)
    all_parcels.sort_index(inplace=True)
    all_parcels.loc[all_parcels.jur_reported.isnull(), 'jur_reported'] = all_parcels['jur']
    all_parcels.loc[all_parcels.jur_or_cpa_id < 20, 'jur_or_cpa_id'] = np.nan
    all_parcels = all_parcels.drop(['mgra_13'], axis=1)
    all_parcels.mgra = all_parcels.mgra.astype(float)





    tran = conn.begin()

    # check out batch size
    try:
        conn.execute(
            """
            BULK INSERT urbansim.urbansim.residential_control
            FROM "\\\\sandag.org\\home\\shared\\TEMP\\NOZ\\urbansim_lite_parcels.csv"
            WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a'
            )
            """
        )
        tran.commit()
    except:
        tran.rollback()
        raise

    conn.close()
