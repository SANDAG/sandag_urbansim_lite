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
                                "jur_or_cpa_id": "cpa_id", "source": "source_id"}, inplace=True)
    year_update.loc[year_update.cpa_id < 20, 'cpa_id'] = np.nan
    year_update['scenario_id'] = scenario
    year_update['year'] = year
    year_update['taz'] = np.nan
    year_update['lu'] = np.nan
    year_update['plu'] = np.nan
    year_update['cap_hs'] = year_update['buildout'] - year_update['hs']
    year_update = year_update.drop(['buildout'], axis=1)
    increment = year - (year % 5)
    if increment == 2015:
        increment = 2017
    year_update['increment'] = increment
    year_update['chg_hs'] = year_update['chg_hs'].fillna(0)
    year_update['source_id'] = year_update['source_id'].fillna(0)
    year_update['phase'] = year_update['phase'].fillna(2015)
    year_update.loc[:, year_update.isnull().any() == False] = year_update.loc[:,
                                                              year_update.isnull().any() == False].astype(int)
    year_update = year_update[['scenario_id', 'increment', 'parcel_id', 'year', 'jurisdiction_id', 'cap_jurisdiction_id',
                               'cpa_id', 'mgra_id', 'luz_id', 'taz', 'site_id', 'lu', 'plu', 'hs', 'chg_hs', 'cap_hs',
                               'source_id', 'phase']]
    year_update.sort_values(by=['parcel_id'])
    year_update = year_update.reset_index(drop=True)
    return year_update


def table_setup(table_type, conn):
    # Once this is up and running, it would be wise to remove (or at least dead-end) the 'write' and 'replace' options.
    while True:
        print("Write (w), Replace (r) or Append (a) to the SQL {}_parcels table?".format(table_type))
        print("Write will drop and re-create the table, while replace will truncate the existing table.")
        setup = input()
        if setup == "w":
            with conn.begin() as trans:
                conn.execute('DROP TABLE IF EXISTS urbansim.urbansim.sr14_residential_{}_parcel_results'.format(table_type))
            with conn.begin() as trans:
                create_table_sql = '''
                    USE [urbansim]
                    SET ANSI_NULLS ON
                    SET QUOTED_IDENTIFIER ON
                    CREATE TABLE [urbansim].[sr14_residential_{}_parcel_results](
                        [scenario_id] [tinyint] NOT NULL,
                        [increment] [smallint] NOT NULL,
                        [parcel_id] [int] NOT NULL,
                        [year] [smallint] NOT NULL,
                        [jurisdiction_id] [tinyint] NOT NULL,
                        [cap_jurisdiction_id] [tinyint] NOT NULL,
                        [cpa_id] [smallint] NULL,
                        [mgra_id] [smallint] NULL,
                        [luz_id] [tinyint] NULL,
                        [taz] [tinyint] NULL,
                        [site_id] [smallint] NULL,
                        [lu] [tinyint] NULL,
                        [plu] [tinyint] NULL,
                        [hs] [smallint] NOT NULL,
                        [chg_hs] [smallint] NOT NULL,
                        [cap_hs] [smallint] NOT NULL,
                        [source_id] [tinyint] NOT NULL,
                        [phase] [smallint] NULL
                        CONSTRAINT [PK_sr14_residential_{}_parcel_yearly] PRIMARY KEY CLUSTERED
                        (
                            [scenario_id] ASC,
                            [year] ASC,
                            [parcel_id] ASC,
                            [source_id] ASC
                        ))WITH (DATA_COMPRESSION = page)'''.format(table_type, table_type)
                conn.execute(create_table_sql)
            scenario = int(1)
            break
        elif setup == "r":
            with conn.begin() as trans:
                conn.execute('TRUNCATE TABLE urbansim.urbansim.sr14_residential_{}_parcel_results'.format(table_type))
            scenario = int(1)
            break
        elif setup == "a":
            scenario_sql = '''
                        SELECT max(scenario_id)
                            FROM [urbansim].[urbansim].[sr14_residential_{}_parcel_results]
                        '''.format(table_type)
            scenario_df = pd.read_sql(scenario_sql, mssql_engine)
            try:
                scenario = int(scenario_df.values) + 1
            except TypeError:
                print("Table exists, but is empty. Setting scenario_id to 1")
                scenario = int(1)
            break
        else:
            print("Please insert only 'w', 'r' or 'a' as a response")
            continue
    return scenario


def scenario_grab(table_type):
    scenario_sql = '''
    SELECT max(scenario_id)
        FROM [urbansim].[urbansim].[sr14_residential_{}_parcel_results]
    '''.format(table_type)
    scenario_df = pd.read_sql(scenario_sql, mssql_engine)
    scenario = int(scenario_df.values)
    return scenario


def table_insert(parcel_table, year, table_type, conn):
    # Load table to M: drive
    start_time = time.monotonic()
    path_name = 'M:\\TEMP\\noz\\outputs\\year_update_{}_{}.csv'.format(table_type, year)
    parcel_table.to_csv(path_name, index=False)
    end_time = time.monotonic()
    print("Time to write {}_table to drive:".format(table_type), timedelta(seconds=end_time - start_time))

    start_time = time.monotonic()
    # Clear staging table
    with conn.begin() as trans:
        conn.execute('TRUNCATE TABLE urbansim.sr14_residential_parcel_staging')

    # Insert table from M: drive to staging
    with conn.begin() as trans:
        bulk_insert_staging_sql = '''
                BULK INSERT urbansim.sr14_residential_parcel_staging
                FROM '\\\\sandag.org\\home\\shared\\TEMP\\noz\\outputs\\year_update_{}_{}.csv'
                WITH (FIRSTROW = 2, FIELDTERMINATOR = ',')
                '''.format(table_type, year)
        conn.execute(bulk_insert_staging_sql)

    # Move from staging table to target table
    with conn.begin() as trans:
        staging_to_target_sql = '''
                INSERT INTO urbansim.urbansim.sr14_residential_{}_parcel_results
                SELECT * FROM urbansim.sr14_residential_parcel_staging
                '''.format(table_type)
        conn.execute(staging_to_target_sql)

    end_time = time.monotonic()
    print("Time to insert {}_table to target:".format(table_type), timedelta(seconds=end_time - start_time))


def run_insert(year):
    all_parcels = orca.get_table('all_parcels').to_frame()
    capacity_parcels = orca.get_table('parcels').to_frame()
    phase_year = orca.get_table('devyear').to_frame()
    hu_forecast = orca.get_table('hu_forecast').to_frame()
    current_builds = hu_forecast.loc[(hu_forecast.year_built == year)].copy()

    # Setup the staging table
    conn = mssql_engine.connect()
    if year == 2017:
        with conn.begin() as trans:
            conn.execute('DROP TABLE IF EXISTS urbansim.sr14_residential_parcel_staging')
        with conn.begin() as trans:
            staging_table_sql = '''
                        IF OBJECT_ID(N'urbansim.sr14_residential_parcel_staging', N'U') IS NULL
                        CREATE TABLE [urbansim].[sr14_residential_parcel_staging](
                           [scenario_id] [float] NOT NULL,
                           [increment] [float] NOT NULL,
                           [parcel_id] [float] NOT NULL,
                           [year] [float] NOT NULL,
                           [jurisdiction_id] [float] NOT NULL,
                           [cap_jurisdiction_id] [float] NOT NULL,
                           [cpa_id] [float] NULL,
                           [mgra_id] [float] NULL,
                           [luz_id] [float] NULL,
                           [taz] [float] NULL,
                           [site_id] [float] NULL,
                           [lu] [float] NULL,
                           [plu] [float] NULL,
                           [hs] [float] NOT NULL,
                           [chg_hs] [float] NOT NULL,
                           [cap_hs] [float] NOT NULL,
                           [source_id] [float] NOT NULL,
                           [phase] [float] NULL
                           )'''
            conn.execute(staging_table_sql)

    # create capacity parcels yearly update table
    if year == 2017:
        scenario = table_setup("cap", conn)
    else:
        scenario = scenario_grab("cap")
    year_update_cap = capacity_parcels.copy()
    year_update_cap = year_update_cap.drop(['capacity_base_yr', 'partial_build'], axis=1)
    year_update_cap = year_update_formater(year_update_cap, current_builds, phase_year, scenario, year)
    table_insert(year_update_cap, year, "cap", conn)

    # # update all parcels table
    # if any(current_builds.parcel_id.duplicated()):
    #     repeated_parcels = pd.concat(g for _, g in current_builds.groupby("parcel_id") if len(g) > 1)  # df of repeats
    #     for repeats in repeated_parcels['parcel_id'].unique():
    #         current_builds.loc[current_builds.parcel_id == repeats, 'source'] = 5  # Change source for groupby
    #     current_builds = pd.DataFrame({'residential_units': current_builds.
    #                                   groupby(["parcel_id", "year_built", "hu_forecast_type_id", "source"]).
    #                                   residential_units.sum()}).reset_index()
    # all_parcels = parcel_table_update(all_parcels, current_builds)
    # orca.add_table("all_parcels", all_parcels)
    #
    # # create all parcels yearly update table
    # if year == 2017:
    #     scenario = table_setup("all", conn)
    # else:
    #     scenario = scenario_grab("all")
    # year_update_all = all_parcels.copy()
    # year_update_all = year_update_all.drop(['base_cap'], axis=1)
    # year_update_all = year_update_formater(year_update_all, current_builds, phase_year, scenario, year)
    # table_insert(year_update_all, year, "all", conn)
    conn.close()
