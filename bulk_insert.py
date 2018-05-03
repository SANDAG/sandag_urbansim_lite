import pandas as pd
import orca
import numpy as np
from sqlalchemy import create_engine
from database import get_connection_string
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


def year_update_formater(parcel_table, current_builds, phase_year, sched_dev, dev_lu_table, scenario, year):
    phase_year.reset_index(inplace=True)
    parcel_table = pd.merge(parcel_table, phase_year[['parcel_id', 'phase_yr', 'capacity_type']], how='left',
                           on=['parcel_id', 'capacity_type'])
    sched_dev.rename(columns={"yr": "phase_yr"}, inplace=True)
    parcel_table = pd.concat([parcel_table, sched_dev])
    parcel_table.rename(columns={"residential_units": "hs"}, inplace=True)
    # get list of all parcels with source 3
    # change source 2 to source 3 when source 3 exists for parcel
    current_builds_source_3_list = current_builds.loc[current_builds.source==3].parcel_id.tolist()
    # all occurrences of that parcel changed to source 3 (source 2 changed)
    current_builds.loc[current_builds.parcel_id.isin(current_builds_source_3_list), 'source'] = 3
    current_builds_grouped = pd.DataFrame({'chg_hs': current_builds.groupby(['parcel_id', 'capacity_type', 'source']).units_added.sum()})
    current_builds_grouped.reset_index(inplace=True)
    #phase_year = pd.concat([phase_year, sched_dev[['parcel_id', 'phase_yr', 'capacity_type']]])
    year_update = pd.merge(parcel_table, current_builds_grouped[['parcel_id', 'chg_hs', 'source', 'capacity_type']],
                           how='left', on=['parcel_id', 'capacity_type'])
    year_update.rename(columns={"phase_yr": "phase","jur_or_cpa_id": "cpa_id", \
                                "source": "source_id", "capacity_type": "cap_type"}, inplace=True)
    year_update.loc[year_update.cpa_id < 20, 'cpa_id'] = np.nan
    year_update['scenario_id'] = scenario
    year_update['yr'] = year
    year_update['taz'] = np.nan
    year_update['lu_2017'] = np.nan
    year_update['capacity'].fillna(0,inplace=True)
    year_update['capacity_used'].fillna(0,inplace=True)
    year_update['cap_hs'] = year_update['capacity'] - year_update['capacity_used']
    increment = year - (year % 5)
    if increment == 2015:
        increment = 2017
    year_update['increment'] = increment
    year_update['chg_hs'] = year_update['chg_hs'].fillna(0)
    year_update['source_id'] = year_update['source_id'].fillna(0)
    year_update['phase'] = year_update['phase'].fillna(2017)
    year_update['cap_type'] = year_update['cap_type'].fillna("no capacity")
    year_update.sort_values(by=['parcel_id'])
    year_update = year_update.reset_index(drop=True)
    year_update.fillna(-99, inplace=True) # pivot does not handle null

    ###########################################################################
    # regional overflow
    ##########################################################################
    # change all occurrences of parcel used as remaining to regional overflow
    # all occurrences of that parcel changed to regional overflow
    year_update['regional_overflow'] = 0
    parcel_regional_overflow = year_update.loc[year_update.source_id == 3].parcel_id.tolist()
    year_update.loc[year_update.parcel_id.isin(parcel_regional_overflow), 'regional_overflow'] = 1

    year_update_pivot = pd.pivot_table(year_update, index=['scenario_id', 'increment', 'parcel_id', 'yr', 'lu_2017',
                            'jurisdiction_id', 'cap_jurisdiction_id', 'cpa_id', 'mgra_id', 'luz_id', 'taz', 'site_id',
                            'lu', 'plu', 'hs', 'regional_overflow'], columns='cap_type',
                            values=['cap_hs', 'chg_hs']).reset_index()
    year_update_pivot.replace(to_replace=-99, value=np.nan, inplace=True)
    year_update_pivot['tot_cap_hs'] = year_update_pivot['cap_hs'].sum(axis=1)
    year_update_pivot['cap_hs'] = year_update_pivot['cap_hs'].fillna(0)
    year_update_pivot['tot_chg_hs'] = year_update_pivot['chg_hs'].sum(axis=1)
    year_update_pivot['chg_hs'] = year_update_pivot['chg_hs'].fillna(0)
    year_update_pivot.rename(columns={"chg_hs": "chg_hs_", "cap_hs": "cap_hs_"}, inplace=True)
    colnames = year_update_pivot.columns
    ind = pd.Index([e[0] + e[1] for e in colnames.tolist()])
    year_update_pivot.columns = ind
    year_update_pivot = pd.merge(year_update_pivot, dev_lu_table, how='left', on='lu')
    year_update_pivot = year_update_pivot[['scenario_id', 'parcel_id', 'yr', 'increment', 'jurisdiction_id',
                                           'cap_jurisdiction_id', 'cpa_id', 'mgra_id', 'luz_id', 'site_id', 'taz',
                                           'hs', 'tot_cap_hs', 'tot_chg_hs', 'lu', 'plu', 'lu_2017', 'dev_type',
                                           'regional_overflow', 'cap_hs_adu', 'cap_hs_cc', 'cap_hs_jur', 'cap_hs_mc',
                                           'cap_hs_sch', 'cap_hs_tc', 'cap_hs_tco', 'cap_hs_uc', 'chg_hs_adu',
                                           'chg_hs_cc', 'chg_hs_jur', 'chg_hs_mc', 'chg_hs_sch', 'chg_hs_tc',
                                           'chg_hs_tco', 'chg_hs_uc']]
    return year_update_pivot


def table_setup(table_type, conn):
    # Once this is up and running, it would be wise to remove (or at least dead-end) the 'write' and 'replace' options.
    while True:
        print("Write (w), Replace (r) or Append (a) to the SQL {}_parcels table?".format(table_type))
        print("Write will drop and re-create the table, while replace will truncate the existing table.")
        setup = input()
        #setup = 'r'
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
                        [parcel_id] [int] NOT NULL,
                        [yr] [smallint] NOT NULL,
                        [increment] [smallint] NOT NULL,
                        [jurisdiction_id] [tinyint] NOT NULL,
                        [cap_jurisdiction_id] [tinyint] NOT NULL,
                        [cpa_id] [smallint] NULL,
                        [mgra_id] [smallint] NULL,
                        [luz_id] [tinyint] NULL,
                        [site_id] [smallint] NULL,
                        [taz] [tinyint] NULL,
                        [hs] [smallint] NOT NULL,
                        [tot_cap_hs] [smallint] NOT NULL,
                        [tot_chg_hs] [smallint] NOT NULL,
                        [lu] [smallint] NULL,
                        [plu] [smallint] NULL,
                        [lu_2017] [smallint] NULL,
                        [dev_type] [tinyint] NULL,
                        [regional_overflow] [bit] NOT NULL,
                        [cap_hs_adu] [smallint] NULL,
                        [cap_hs_cc] [smallint] NULL,
                        [cap_hs_jur] [smallint] NULL,
                        [cap_hs_mc] [smallint] NULL,
                        [cap_hs_sch] [smallint] NULL,
                        [cap_hs_tc] [smallint] NULL,
                        [cap_hs_tco] [smallint] NULL,
                        [cap_hs_uc] [smallint] NULL,
                        [chg_hs_adu] [smallint] NULL,
                        [chg_hs_cc] [smallint] NULL,
                        [chg_hs_jur] [smallint] NULL,
                        [chg_hs_mc] [smallint] NULL,
                        [chg_hs_sch] [smallint] NULL,
                        [chg_hs_tc] [smallint] NULL,
                        [chg_hs_tco] [smallint] NULL,
                        [chg_hs_uc] [smallint] NULL
                    )WITH (DATA_COMPRESSION = page)'''.format(table_type, table_type)
                conn.execute(create_table_sql)
                #Would prefer to remove jur_id and hs from the key, but there is a discrepancy in additional units atm
            # CONSTRAINT[PK_sr14_residential_
            # {}
            # _parcel_yearly] PRIMARY
            # KEY
            # CLUSTERED(
            #     [scenario_id]
            # ASC,
            # [yr]
            # ASC,
            # [parcel_id]
            # ASC,
            # [jurisdiction_id]
            # ASC,
            # [hs]
            # ASC
            # )
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
            print("Please insert only lowercase 'w', 'r' or 'a' as a response.")
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
    #all_parcels = orca.get_table('all_parcels').to_frame()
    capacity_parcels = orca.get_table('parcels').to_frame()
    phase_year = orca.get_table('devyear').to_frame()
    dev_lu_table = orca.get_table('dev_lu_table').to_frame()
    sched_dev = orca.get_table('scheduled_development').to_frame()
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
                        [parcel_id] [float] NOT NULL,
                        [yr] [float] NOT NULL,
                        [increment] [float] NOT NULL,
                        [jurisdiction_id] [float] NOT NULL,
                        [cap_jurisdiction_id] [float] NOT NULL,
                        [cpa_id] [float] NULL,
                        [mgra_id] [float] NULL,
                        [luz_id] [float] NULL,
                        [site_id] [float] NULL,
                        [taz] [float] NULL,
                        [hs] [float] NOT NULL,
                        [tot_cap_hs] [float] NOT NULL,
                        [tot_chg_hs] [float] NOT NULL,
                        [lu] [float] NULL,
                        [plu] [float] NULL,
                        [lu_2017] [float] NULL,
                        [dev_type] [float] NULL,
                        [regional_overflow] [bit] NOT NULL,
                        [cap_hs_adu] [float] NULL,
                        [cap_hs_cc] [float] NULL,
                        [cap_hs_jur] [float] NULL,
                        [cap_hs_mc] [float] NULL,
                        [cap_hs_sch] [float] NULL,
                        [cap_hs_tc] [float] NULL,
                        [cap_hs_tco] [float] NULL,
                        [cap_hs_uc] [float] NULL,
                        [chg_hs_adu] [float] NULL,
                        [chg_hs_cc] [float] NULL,
                        [chg_hs_jur] [float] NULL,
                        [chg_hs_mc] [float] NULL,
                        [chg_hs_sch] [float] NULL,
                        [chg_hs_tc] [float] NULL,
                        [chg_hs_tco] [float] NULL,
                        [chg_hs_uc] [float] NULL
                        )'''
            conn.execute(staging_table_sql)

    # create capacity parcels yearly update table
    if year == 2017:
        scenario = table_setup("cap", conn)
    else:
        scenario = scenario_grab("cap")
    if year == 2032:
        print(year)
    year_update_cap = capacity_parcels.copy()
    year_update_cap = year_update_cap.drop(['partial_build'], axis=1)
    year_update_cap = year_update_formater(year_update_cap, current_builds, phase_year, sched_dev, dev_lu_table, scenario, year)
    table_insert(year_update_cap, year, "cap", conn)

    # # update all parcels table
    # current_builds = pd.DataFrame({'residential_units': current_builds.
    #                               groupby(["parcel_id", "year_built", "hu_forecast_type_id", "source"]).
    #                               residential_units.sum()}).reset_index()
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
