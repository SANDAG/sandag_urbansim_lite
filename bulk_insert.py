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


def year_update_formater(parcel_table, current_builds, phase_year, scenario, year):
    parcel_table.rename(columns={"residential_units": "hs"}, inplace=True)
    year_update = pd.merge(parcel_table, current_builds[['parcel_id', 'residential_units', 'source']], how='left',
                           left_index=True, right_on='parcel_id')
    year_update = pd.merge(year_update, phase_year[['phase_yr_ctrl']], how='left', left_on='parcel_id',
                           right_index=True)
    year_update.rename(columns={"residential_units": "chg_hs", "phase_yr_ctrl": "phase",
                                "jur_or_cpa_id": "cpa"}, inplace=True)
    year_update.loc[year_update.cpa < 20, 'cpa'] = np.nan
    year_update['scenario'] = scenario
    year_update['year'] = year
    year_update['taz'] = np.nan
    year_update['lu'] = np.nan
    year_update['cap_hs'] = year_update['buildout'] - year_update['hs']
    year_update = year_update.drop(['buildout'], axis=1)
    increment = year - (year % 5)
    if increment == 2015:
        increment = 2017
    year_update['increment'] = increment
    year_update['chg_hs'] = year_update['chg_hs'].fillna(0)
    year_update['source'] = year_update['source'].fillna(0)
    year_update['phase'] = year_update['phase'].fillna(2015)
    year_update.loc[:, year_update.isnull().any() == False] = year_update.loc[:,
                                                              year_update.isnull().any() == False].astype(int)
    year_update = year_update[['scenario', 'increment', 'parcel_id', 'year', 'jur', 'jur_reported', 'cpa', 'mgra',
                               'luz', 'taz', 'site_id', 'lu', 'hs', 'chg_hs', 'cap_hs', 'source', 'phase']]
    year_update.sort_values(by=['parcel_id'])
    year_update = year_update.reset_index(drop=True)
    return year_update


def run_insert(year):
    # # This section below is for use in the first run / creation of the table. The method it uses to pull the
    # # scenario is only valid if it is writing directly to the database every loop (either with a .to_sql() or by
    # # writing to M: drive and bulk inserting with an append). Neither of these solutions are likely to be optimal.
    # # Probably this will be better either as written to C: drive and uploaded with a batch method. It is unclear
    # # whether we should be using one file per year or one large file appended with all years
    # if year == 2017:
    #     try:
    #         scenario_sql = '''
    #         SELECT max(scenario)
    #           FROM [urbansim].[urbansim].[sr14_residential_CAP_parcel_results]
    #         '''
    #         scenario_df = pd.read_sql(scenario_sql, mssql_engine)
    #         scenario = int(scenario_df.values) + 1
    #     except KeyError:
    #         conn = mssql_engine.connect()
    #         with conn.begin() as trans:
    #             conn.execute('DROP TABLE IF EXISTS urbansim.urbansim.sr14_residential_CAP_parcel_results')
    #         with conn.begin() as trans:
    #             create_table_sql = '''
    #             USE [urbansim]
    #             SET ANSI_NULLS ON
    #             SET QUOTED_IDENTIFIER ON
    #             CREATE TABLE [urbansim].[sr14_residential_CAP_parcel_results](
    #                 [scenario] [tinyint] NOT NULL,
    #                 [increment] [int] NOT NULL,
    #                 [parcel_id] [int] NOT NULL,
    #                 [year] [int] NOT NULL,
    #                 [jur] [smallint] NOT NULL,
    #                 [jur_reported] [smallint] NOT NULL,
    #                 [cpa] [int] NULL,
    #                 [mgra] [int] NULL,
    #                 [luz] [smallint] NULL,
    #                 [taz] [int] NULL,
    #                 [site_id] [smallint] NULL,
    #                 [lu] [smallint] NULL,
    #                 [hs] [int] NOT NULL,
    #                 [chg_hs] [int] NOT NULL,
    #                 [cap_hs] [int] NOT NULL,
    #                 [source] [smallint] NOT NULL,
    #                 [phase] [int] NULL
    #                 CONSTRAINT [PK_sr14_residential_CAP_parcel_yearly] PRIMARY KEY CLUSTERED
    #                 (
    #                     [scenario] ASC,
    #                     [year] ASC,
    #                     [parcel_id] ASC,
    #                     [source] ASC
    #                 ))WITH (DATA_COMPRESSION = page)'''
    #             conn.execute(create_table_sql)
    #         conn.close()
    #         scenario = int(1)
    # else:
    #     scenario_sql = '''
    #                 SELECT max(scenario)
    #                   FROM [urbansim].[urbansim].[sr14_residential_CAP_parcel_results]
    #                 '''
    #     scenario_df = pd.read_sql(scenario_sql, mssql_engine)
    #     scenario = int(scenario_df.values)
    try:
        scenario_sql = '''
            SELECT max(scenario)
              FROM [urbansim].[urbansim].[sr14_residential_CAP_parcel_results]
            '''
        scenario_df = pd.read_sql(scenario_sql, mssql_engine)
        scenario = int(scenario_df.values) + 1
    except KeyError:
        scenario = int(1)

    all_parcels = orca.get_table('all_parcels').to_frame()
    capacity_parcels = orca.get_table('parcels').to_frame()
    phase_year = orca.get_table('devyear').to_frame()
    hu_forecast = orca.get_table('hu_forecast').to_frame()
    current_builds = hu_forecast.loc[(hu_forecast.year_built == year)].copy()

    # # Check if parcels occur multiple times (due to multiple sources). Will skip if false.
    # if any(current_builds.parcel_id.duplicated()):
    #     repeated_parcels = pd.concat(g for _, g in current_builds.groupby("parcel_id") if len(g) > 1)  # df of repeats
    #     for repeats in repeated_parcels['parcel_id'].unique():
    #         current_builds.loc[current_builds.parcel_id == repeats, 'source'] = 5  # Change source for groupby
    #     current_builds = pd.DataFrame({'residential_units': current_builds.
    #                                   groupby(["parcel_id", "year_built", "hu_forecast_type_id", "source"]).
    #                                   residential_units.sum()}).reset_index()

    # # update capacity parcels table
    # capacity_parcels = parcel_table_update(capacity_parcels, current_builds)
    # orca.add_table("parcels", capacity_parcels)

    # # create capacity parcels yearly update table
    # year_update_cap = capacity_parcels.copy()
    # year_update_cap.rename(columns={"orig_jurisdiction_id": "jur", "jurisdiction_id": "jur_reported",
    #                                 "luz_id": "luz", "mgra_id": "mgra"}, inplace=True)
    # year_update_cap = year_update_cap.drop(['capacity_base_yr', 'partial_build'], axis=1)
    # year_update_cap = year_update_formater(year_update_cap, current_builds, phase_year, scenario, year)

    # # update all parcels table
    # all_parcels = parcel_table_update(all_parcels, current_builds)
    # orca.add_table("all_parcels", all_parcels)
    #
    # # create all parcels yearly update table
    # year_update_all = all_parcels.copy()
    # year_update_all = year_update_all.drop(['base_cap'], axis=1)
    # year_update_all = year_update_formater(year_update_all, current_builds, phase_year, scenario, year)

    #########################################################################
    # Everything below is related to uploading to sql in one way or another #
    #########################################################################

    # start_time = time.monotonic()
    # # # This is only for capacity > 0 parcels at the moment, modify names and file uploaded for all parcels

    # # # This section writes files for individual years as .csv files
    # # C: drive method takes ~1 minute total (April 2, 2018)
    # path_name = 'C:\\Users\\noz\\Documents\\sandag_urbansim_lite\\outputs\\year_update_{}.csv'.format(year)

    # # M: drive method takes ~85 seconds per file, total of 55 minutes (April 2, 2018)
    # # Have not added time for bulk insert run yet (should be quick once on M: drive and working)
    # path_name = 'M:\\TEMP\\noz\\outputs\\year_update_{}.csv'.format(year)

    # year_update_cap.to_csv(path_name, index=False)

    # # .to_sql method takes ~70 seconds per file, total of 45 minutes (April 2, 2018)
    # # Needs different method of scenario choosing, pick from sql in 2017 then continue after
    # year_update_cap.to_sql(name='sr14_residential_CAP_parcel_results', con=mssql_engine, schema='urbansim',
    #                        index=False, if_exists='append')

    # # # This section writes one large .csv file with all years appended
    # # C: drive method takes ~1 minute total (April 2, 2018)
    # path_name = 'C:\\Users\\noz\\Documents\\sandag_urbansim_lite\\outputs\\cap_update_scenario_{}.csv'.format(scenario)

    # # M: drive method takes ~60 seconds per iteration, total of 35 minutes (April 2, 2018)
    # # Have not added time for bulk insert run yet (should be quick once on M: drive and working)
    # path_name = 'M:\\TEMP\\noz\\outputs\\cap_update_scenario_{}.csv'.format(scenario)
    #
    # if year == 2017:
    #     year_update_cap.to_csv(path_name, index=False)
    # else:
    #     year_update_cap.to_csv(path_name, mode='a', header=False, index=False)

    # # .to_sql method should be indifferent for this approach, as it appends by year

    # end_time = time.monotonic()
    # print(timedelta(seconds=end_time - start_time))

    ####################################################################################
    # End of upload section, below is the actual bulk insert, which needs modification #
    ####################################################################################
    # # Currently having errors with manual bulk insert using the following code:
    # BULK INSERT urbansim.urbansim.sr14_residential_CAP_parcel_results
    # FROM '\\sandag.org\home\shared\TEMP\noz\outputs\cap_update_scenario_2.csv'
    # WITH (
    # FIRSTROW = 2,
    # FIELDTERMINATOR = ',',
    # ROWTERMINATOR = '0x0a'
    # )
    # GO
    # # Error: (may have something to do with row terminator?)
    # Msg 4864, Level 16, State 1, Line 1
    # Bulk load data conversion error (type mismatch or invalid character for the specified codepage)

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