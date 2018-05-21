import pandas as pd
import orca
import numpy as np
from sqlalchemy import create_engine
from database import get_connection_string
import time
import utils
from datetime import timedelta

db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)


def table_setup():
    while True:
        print("Writing to the capacity_parcels table (c), all_parcels table (a), both (b) or neither (n)?")
        table_type_input = input("Choose c, a, b or n: ")
        if table_type_input == "c":
            table_type_list = ["cap"]
            break
        elif table_type_input == "a":
            table_type_list = ["all"]
            break
        elif table_type_input == "b":
            table_type_list = ["cap", "all"]
            break
        elif table_type_input == "n":
            table_type_list = []
            break
        else:
            print("Please insert only lowercase 'c', 'a', 'b' or 'n' as a response.")
            continue

    if len(table_type_list) == 0:
        pass
    else:
        conn = mssql_engine.connect()
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
                        [lu_2015] [float] NULL,
                        [dev_type_2015] [float] NULL,
                        [lu_2017] [float] NULL,
                        [dev_type_2017] [float] NULL,
                        [plu] [float] NULL,
                        [lu_sim] [float] NULL,
                        [dev_type_sim] [float] NULL,
                        [regional_overflow] [bit] NOT NULL,
                        [cap_hs_adu] [float] NULL,
                        [cap_hs_jur] [float] NULL,
                        [cap_hs_sch] [float] NULL,
                        [cap_hs_sgoa] [float] NULL,
                        [chg_hs_adu] [float] NULL,
                        [chg_hs_jur] [float] NULL,
                        [chg_hs_sch] [float] NULL,
                        [chg_hs_sgoa] [float] NULL
                        )'''
            conn.execute(staging_table_sql)

        for table_type in table_type_list:
            while True:
                print("Write (w), Replace (r) or Append (a) to the SQL {}_parcels table?".format(table_type))
                print("Write will drop and re-create the table, while replace will truncate the existing table.")
                setup = input("Choose w, r or a: ")
                if setup == "w":
                    with conn.begin() as trans:
                        conn.execute('DROP TABLE IF EXISTS urbansim.urbansim.sr14_residential_{}_parcel_results'.format(
                            table_type))
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
                                [taz] [smallint] NULL,
                                [hs] [smallint] NOT NULL,
                                [tot_cap_hs] [smallint] NOT NULL,
                                [tot_chg_hs] [smallint] NOT NULL,
                                [lu_2015] [smallint] NULL,
                                [dev_type_2015] [tinyint] NULL,
                                [lu_2017] [smallint] NULL,
                                [dev_type_2017] [tinyint] NULL,
                                [plu] [smallint] NULL,
                                [lu_sim] [smallint] NULL,
                                [dev_type_sim] [tinyint] NULL,
                                [regional_overflow] [bit] NOT NULL,
                                [cap_hs_adu] [smallint] NULL,
                                [cap_hs_jur] [smallint] NULL,
                                [cap_hs_sch] [smallint] NULL,
                                [cap_hs_sgoa] [smallint] NULL,
                                [chg_hs_adu] [smallint] NULL,
                                [chg_hs_jur] [smallint] NULL,
                                [chg_hs_sch] [smallint] NULL,
                                [chg_hs_sgoa] [smallint] NULL
                                CONSTRAINT[PK_sr14_residential_{}_parcel_yearly] PRIMARY KEY CLUSTERED(
                                    [scenario_id] ASC,
                                    [yr] ASC,
                                    [parcel_id] ASC)
                                    )WITH (DATA_COMPRESSION = page)'''.format(table_type, table_type)
                        conn.execute(create_table_sql)
                    scenario = int(1)
                    break
                elif setup == "r":
                    with conn.begin() as trans:
                        conn.execute(
                            'TRUNCATE TABLE urbansim.urbansim.sr14_residential_{}_parcel_results'.format(table_type))
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

            # create parcels yearly update table
            if table_type == "cap":
                base_year_table = orca.get_table('parcels').to_frame()
            if table_type == "all":
                base_year_table = orca.get_table('all_parcels').to_frame()
            no_builds = pd.DataFrame()
            base_year_table = year_update_formatter(base_year_table, no_builds, scenario, 2016, table_type)
            table_insert(base_year_table, 2016, table_type)
    return table_type_list


def year_update_formatter(parcel_table, current_builds, scenario, year, table_type):
    #phase_year = orca.get_table('devyear').to_frame()
    dev_lu_table = orca.get_table('dev_lu_table').to_frame()
    sched_dev = orca.get_table('scheduled_development').to_frame()
    #phase_year.reset_index(inplace=True)
    #parcel_table = pd.merge(parcel_table, phase_year[['parcel_id', 'phase_yr', 'capacity_type']], how='left',
    #                       on=['parcel_id', 'capacity_type'])
    #sched_dev.rename(columns={"yr": "phase_yr"}, inplace=True)
    parcel_table = pd.concat([parcel_table, sched_dev])
    parcel_table.rename(columns={"residential_units": "hs"}, inplace=True)
    #phase_year = pd.concat([phase_year, sched_dev[['parcel_id', 'phase_yr', 'capacity_type']]])
    increment = year - (year % 5)
    if year == 2016:
        year_update = parcel_table.copy()
        year_update['chg_hs'] = 0
        year_update['source'] = 0
        year_update['increment'] = 2016
        year_update['cap_hs'] = year_update['capacity']
    else:
        # get list of all parcels with source 3
        # change source 2 to source 3 when source 3 exists for parcel
        current_builds_source_3_list = current_builds.loc[current_builds.source == 3].parcel_id.tolist()
        # all occurrences of that parcel changed to source 3 (source 2 changed)
        current_builds.loc[current_builds.parcel_id.isin(current_builds_source_3_list), 'source'] = 3
        current_builds_grouped = pd.DataFrame(
            {'chg_hs': current_builds.groupby(['parcel_id', 'capacity_type', 'source']).units_added.sum()})
        current_builds_grouped.reset_index(inplace=True)
        year_update = pd.merge(parcel_table, current_builds_grouped[['parcel_id', 'chg_hs', 'source', 'capacity_type']],
                               how='left', on=['parcel_id', 'capacity_type'])
        if increment == 2015:
            increment = 2017
        year_update['increment'] = increment
        year_update['capacity_used'].fillna(0, inplace=True)
        year_update['cap_hs'] = year_update['capacity'] - year_update['capacity_used']

    updated_lu_list = year_update.loc[year_update.lu_sim == year_update.plu].parcel_id.tolist()
    year_update['lu_sim'].where(~year_update.parcel_id.isin(updated_lu_list), other=year_update['plu'], inplace=True)

    year_update.rename(columns={"jur_or_cpa_id": "cpa_id", "source": "source_id",
                                "capacity_type": "cap_type"}, inplace=True) #"phase_yr": "phase",
    year_update.loc[year_update.cpa_id < 20, 'cpa_id'] = np.nan
    year_update['yr'] = year
    year_update['scenario_id'] = scenario
    year_update['capacity'].fillna(0,inplace=True)
    year_update['chg_hs'] = year_update['chg_hs'].fillna(0)
    year_update['source_id'] = year_update['source_id'].fillna(0)
    #year_update['phase'] = year_update['phase'].fillna(2017)
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
    year_update.replace('cc', 'sgoa', inplace=True)
    year_update.replace('mc', 'sgoa', inplace=True)
    year_update.replace('tc', 'sgoa', inplace=True)
    year_update.replace('tco', 'sgoa', inplace=True)
    year_update.replace('uc', 'sgoa', inplace=True)
    #year_update = year_update.drop(['source_id', 'priority', 'phase', 'partial_build', 'capacity_used', 'capacity'], axis=1)
    year_update_pivot = pd.pivot_table(year_update, index=['scenario_id', 'increment', 'parcel_id', 'yr', 'lu_2017',
                            'jurisdiction_id', 'cap_jurisdiction_id', 'cpa_id', 'mgra_id', 'luz_id', 'taz', 'site_id',
                            'lu_sim', 'plu', 'hs', 'regional_overflow', 'lu_2015', 'dev_type_2015', 'dev_type_2017'],
                            columns='cap_type', values=['cap_hs', 'chg_hs']).reset_index()
    year_update_pivot.replace(to_replace=-99, value=np.nan, inplace=True)
    year_update_pivot['tot_cap_hs'] = year_update_pivot['cap_hs'].sum(axis=1)
    year_update_pivot['cap_hs'] = year_update_pivot['cap_hs'].fillna(0)
    year_update_pivot['tot_chg_hs'] = year_update_pivot['chg_hs'].sum(axis=1)
    year_update_pivot['chg_hs'] = year_update_pivot['chg_hs'].fillna(0)
    year_update_pivot.rename(columns={"chg_hs": "chg_hs_", "cap_hs": "cap_hs_"}, inplace=True)
    colnames = year_update_pivot.columns
    ind = pd.Index([e[0] + e[1] for e in colnames.tolist()])
    year_update_pivot.columns = ind
    year_update_pivot = pd.merge(year_update_pivot, dev_lu_table, how='left', on='lu_sim')
    # There are parcels with dev_type_2017 = 23 or 29, which are military housing. These do no appear in the table
    # being merged here, so we override those and plug in the dev_type_sim manually (can handle case-by-case if needed):
    year_update_pivot.loc[year_update_pivot.dev_type_2017 == 23, 'dev_type_sim'] = 23
    year_update_pivot.loc[year_update_pivot.dev_type_2017 == 29, 'dev_type_sim'] = 29
    year_update_pivot = year_update_pivot[['scenario_id', 'parcel_id', 'yr', 'increment', 'jurisdiction_id',
                                           'cap_jurisdiction_id', 'cpa_id', 'mgra_id', 'luz_id', 'site_id', 'taz',
                                           'hs', 'tot_cap_hs', 'tot_chg_hs', 'lu_2015', 'dev_type_2015', 'lu_2017',
                                           'dev_type_2017', 'plu', 'lu_sim', 'dev_type_sim', 'regional_overflow',
                                           'cap_hs_adu', 'cap_hs_jur', 'cap_hs_sch', 'cap_hs_sgoa', 'chg_hs_adu',
                                           'chg_hs_jur', 'chg_hs_sch', 'chg_hs_sgoa']]
    return year_update_pivot


def scenario_grab(table_type):
    scenario_sql = '''
    SELECT max(scenario_id)
        FROM [urbansim].[urbansim].[sr14_residential_{}_parcel_results]
    '''.format(table_type)
    scenario_df = pd.read_sql(scenario_sql, mssql_engine)
    scenario = int(scenario_df.values)
    return scenario


def table_insert(parcel_table, year, table_type):
    conn = mssql_engine.connect()
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
    conn.close()


def run_insert(parcel_tables, year):
    all_parcels = orca.get_table('all_parcels').to_frame()
    capacity_parcels = orca.get_table('parcels').to_frame()
    hu_forecast = orca.get_table('hu_forecast').to_frame()
    current_builds = hu_forecast.loc[(hu_forecast.year_built == year)].copy()

    for table_type in parcel_tables:
        if table_type == "all":
            all_parcels = utils.parcel_table_update_units(all_parcels, current_builds)
            orca.add_table("all_parcels", all_parcels)
            year_update = all_parcels.copy()
        if table_type == "cap":
            year_update = capacity_parcels.copy()
        scenario = scenario_grab(table_type)
        year_update = year_update_formatter(year_update, current_builds, scenario, year, table_type)
        table_insert(year_update, year, table_type)
