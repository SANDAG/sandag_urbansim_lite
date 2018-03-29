import pandas as pd
import orca
import numpy as np
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import time
from datetime import timedelta

db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)


def parcel_table_update(parcel_table, current_builds):
    # This is the new parcel update section
    # Now merges parcels that were updated in the current year with existing parcel table
    updated_parcel_table = pd.merge(parcel_table, current_builds[['parcel_id', 'residential_units']], how='left', left_index=True,
                       right_on='parcel_id')
    updated_parcel_table.set_index('parcel_id',inplace=True)
    updated_parcel_table.rename(columns={"residential_units_x": "residential_units", "residential_units_y": "updated_units"}, inplace=True)
    updated_parcel_table.updated_units = updated_parcel_table.updated_units.fillna(0)
    updated_parcel_table['residential_units'] = updated_parcel_table['residential_units'] + updated_parcel_table['updated_units']
    updated_parcel_table = updated_parcel_table.drop(['updated_units'], 1)
    updated_parcel_table.residential_units = updated_parcel_table.residential_units.astype(int)
    return updated_parcel_table


def run_insert(year):
    if year == 2017:
        try:
            scenario_sql = '''
            SELECT max(scenario)
              FROM [urbansim].[urbansim].[sr14_residential_CAP_parcel_results]
            '''
            scenario_df = pd.read_sql(scenario_sql, mssql_engine)
            scenario = int(scenario_df.values) + 1
        except KeyError:
            conn = mssql_engine.connect()
            with conn.begin() as trans:
                conn.execute('DROP TABLE IF EXISTS urbansim.urbansim.sr14_residential_CAP_parcel_results')
            with conn.begin() as trans:
                create_table_sql = '''
                USE [urbansim]
                SET ANSI_NULLS ON
                SET QUOTED_IDENTIFIER ON
                CREATE TABLE [urbansim].[sr14_residential_CAP_parcel_results](
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
                    CONSTRAINT [PK_sr14_residential_CAP_parcel_yearly] PRIMARY KEY CLUSTERED
                    (
                        [scenario] ASC,
                        [year] ASC,
                        [parcel_id] ASC,
                        [source] ASC
                    ))WITH (DATA_COMPRESSION = page)'''
                conn.execute(create_table_sql)
            conn.close()
            scenario = int(1)
    else:
        scenario_sql = '''
                    SELECT max(scenario)
                      FROM [urbansim].[urbansim].[sr14_residential_CAP_parcel_results]
                    '''
        scenario_df = pd.read_sql(scenario_sql, mssql_engine)
        scenario = int(scenario_df.values)

    all_parcels = orca.get_table('all_parcels').to_frame()
    capacity_parcels = orca.get_table('parcels').to_frame()
    phase_year = orca.get_table('devyear').to_frame()
    hu_forecast = orca.get_table('hu_forecast').to_frame()
    current_builds = hu_forecast.loc[(hu_forecast.year_built == year)].copy()

    # Check if parcels occur multiple times (due to multiple sources). Will skip if false.
    if any(current_builds.parcel_id.duplicated()):
        repeated_parcels = pd.concat(g for _, g in current_builds.groupby("parcel_id") if len(g) > 1)  # df of repeats
        for repeats in repeated_parcels['parcel_id'].unique():
            current_builds.loc[current_builds.parcel_id == repeats, 'source'] = 5  # Change source for groupby
        current_builds = pd.DataFrame({'residential_units': current_builds.
                                      groupby(["parcel_id", "year_built", "hu_forecast_type_id", "source"]).
                                      residential_units.sum()}).reset_index()

    # update capacity parcels table
    capacity_parcels = parcel_table_update(capacity_parcels, current_builds)
    orca.add_table("parcels", capacity_parcels)

    # update all parcels table
    all_parcels = parcel_table_update(all_parcels, current_builds)
    orca.add_table("all_parcels", all_parcels)

    '''
    # year_update = pd.merge(all_parcels, hu_forecast[['parcel_id','res_units','source']],how='left',left_index=True,
    #                       right_on='parcel_id')
    year_update = pd.merge(capacity_parcels, current_builds[['parcel_id', 'residential_units', 'source']], how='left',
                           left_index=True, right_on='parcel_id')
    year_update = pd.merge(year_update,phase_year[['phase_yr_ctrl']],how='left',left_on='parcel_id',right_index=True)
    year_update['scenario'] = scenario
    year_update['year'] = year
    year_update['taz'] = np.nan
    year_update['lu'] = np.nan
    year_update.rename(columns={"orig_jurisdiction_id":"jur","jurisdiction_id":"jur_reported",
                                "jur_or_cpa_id":"cpa","luz_id":"luz","residential_units_x":"hs","residential_units_y":
                                "chg_hs","phase_yr_ctrl":"phase","mgra_id":"mgra"},inplace=True)
    year_update['cap_hs'] = year_update['buildout'] - year_update['hs']
    year_update.loc[year_update.cpa < 20, 'cpa'] = np.nan
    year_update = year_update.drop(['capacity_base_yr','partial_build','buildout'], axis=1)
    increment = year - (year%5)
    if increment == 2015:
        increment = 2017
    year_update['increment'] = increment
    year_update['chg_hs'] = year_update['chg_hs'].fillna(0)
    year_update['source'] = year_update['source'].fillna(0)
    '''
    start_time = time.monotonic()
    year_update.to_sql(name='sr14_residential_CAP_parcel_results', con=mssql_engine, schema='urbansim', index=False,
                       if_exists='append')
    end_time = time.monotonic()
    print(timedelta(seconds=end_time - start_time))
    '''
    # This creates a new file of parcel info for each year
    # parcels['year'] = year
    # yname = '\\\\sandag.org\\home\\shared\\TEMP\\NOZ\\urbansim_lite_parcels_{}.csv'.format(year)
    #year_update.to_csv('C:\\Users\\noz\\Documents\\sandag_urbansim_lite\\outputs\\year_update_{}.csv'.format(year))



    # tran = conn.begin()
    #
    # # check out batch size
    # try:
    #     conn.execute(
    #         """
    #         BULK INSERT urbansim.urbansim.residential_control
    #         FROM "\\\\sandag.org\\home\\shared\\TEMP\\NOZ\\urbansim_lite_parcels.csv"
    #         WITH (
    #         FIRSTROW = 2,
    #         FIELDTERMINATOR = ',',
    #         ROWTERMINATOR = '0x0a'
    #         )
    #         """
    #     )
    #     tran.commit()
    # except:
    #     tran.rollback()
    #     raise
    #
    # conn.close()



'''
#This loop can write the all the parcels for each year as one (very large) .csv file.
if year == 2020:
    parcels.to_csv('M:/TEMP/NOZ/urbansim_lite_parcels.csv')
else:
    parcels.to_csv('M:/TEMP/NOZ/urbansim_lite_parcels.csv', mode='a', header=False)
db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)
parcels.to_sql(name='urbansim_lite_output_parcels', con=mssql_engine, schema='urbansim', if_exists='replace',
                 index=True) #no run ID -> appending to database
'''