import pandas as pd
import orca
import numpy as np
from sqlalchemy import create_engine
from database import get_connection_string
import time
import utils
from datetime import timedelta

# Link to SQL Server
db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)


def table_setup():
    """
    Before the first iteration, determines what detailed outputs are desired and prepares the output tables.

    :return:
        list: the desired detailed output table types. This list will contain 0-2 items: 'all' and / or 'cap'.
    """

    # This statement is called before the iterations begin. Based on user input, it will prepare the output tables.
    while True:
        # These two lines describe what options are available and asks the user for input.
        # print("Writing to the capacity_parcels table (c), all_parcels table (a), both (b) or neither (n)?")
        # table_type_input = input("Choose c, a, b or n: ")

        # These two lines disallow detailed outputs (for when the stored versions are preferred to any new versions.)
        print("Writing detailed results to the database is currently disabled. (Preferred run is already stored.)")
        table_type_input = "n"

        # Based on user input, select which list to pass to the rest of the table_setup function:
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
            # If an invalid input is given, the loop repeats and attempts to inform the user of allowed inputs options.
            print("Please insert only lowercase 'c', 'a', 'b' or 'n' as a response.")
            continue

    # If no detailed outputs are requested, table_setup is not needed and can end. Otherwise, the function creates the
    # staging table for uploading the simulation results to SQL and asks user what to do with the target tables.
    if len(table_type_list) == 0:
        pass
    else:
        # Begin the SQL connection.
        conn = mssql_engine.connect()

        # Drop the existing staging table.
        with conn.begin() as trans:
            conn.execute('DROP TABLE IF EXISTS urbansim.sr14_residential_parcel_staging')

        # Create a new staging table. Datatypes should be kept as float in the staging table, because SQL knows how to
        # convert them to the appropriate datatype in the target table. Attempting to force upload datatypes (ints)
        # from pandas to SQL resulted in numerous errors that were cumbersome to work around.
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

        # Prepare the target tables for the table_type(s) requested.
        for table_type in table_type_list:
            while True:
                # The input statement is self-explanatory, however the selection can impact the scenario_id.
                # Write and Replace are in effect the same (unless changes have been made to the table creation script)
                # and will set the scenario_id to 1. Append will add the new entries to the old table, with an updated
                # scenario_id (incremented to be 1 higher than the current maximum).
                print("Write (w), Replace (r) or Append (a) to the SQL {}_parcels table?".format(table_type))
                print("Write will drop and re-create the table, while replace will truncate the existing table.")
                setup = input("Choose w, r or a: ")
                # Note: If the table does not exist, attempting to Replace or Append will result in an error.

                if setup == "w":
                    # This fully drops and rewrites the target table. Not advised unless changes have been made to the
                    # table generation script below, such as the addition/modification of new columns or changes to the
                    # primary key.
                    with conn.begin() as trans:
                        conn.execute('DROP TABLE IF EXISTS urbansim.urbansim.sr14_residential_{}_parcel_results'.
                                     format(table_type))
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

                    # Sets the scenario_id to 1.
                    scenario = int(1)
                    break

                elif setup == "r":
                    # This keeps the existing table but removes all data currently in it. This is the preferred option
                    # if no changes are being made other than the simulation inputs, and the old detailed outputs are
                    # no longer needed.
                    with conn.begin() as trans:
                        conn.execute(
                            'TRUNCATE TABLE urbansim.urbansim.sr14_residential_{}_parcel_results'.format(table_type))
                    # Resets the scenario_id to 1.
                    scenario = int(1)
                    break

                elif setup == "a":
                    # This option adds a new set of detailed outputs to the existing table. This option is likely only
                    # needed when doing final model runs with different input ids.
                    # Note: please record which run_id corresponds to which scenario_id in confluence.

                    # Pulls the maximum scenario_id from the existing table and converts it into a 1x1 dataframe.
                    scenario_sql = '''
                                SELECT max(scenario_id)
                                    FROM [urbansim].[urbansim].[sr14_residential_{}_parcel_results]
                                '''.format(table_type)
                    scenario_df = pd.read_sql(scenario_sql, mssql_engine)
                    try:
                        # Converts the value of the 1x1 dataframe and increments it by 1.
                        scenario = int(scenario_df.values) + 1
                    except TypeError:
                        # If the extracted value is not a number, set scenario_id to 1. (Should only happen if the
                        # table was previously truncated and not repopulated, and append was selected.)
                        print("Table exists, but is empty. Setting scenario_id to 1")
                        scenario = int(1)
                    break

                else:
                    # If an invalid input is given, the loop repeats and attempts to inform the user of allowed inputs
                    # options.
                    print("Please insert only lowercase 'w', 'r' or 'a' as a response.")
                    continue

            # This section plugs in the current (unmodified) parcel table to the target tables as a baseline.
            if table_type == "cap":
                base_year_table = orca.get_table('parcels').to_frame()
            if table_type == "all":
                base_year_table = orca.get_table('all_parcels').to_frame()

            # The year_update_formatter needs a table of modified parcels, and no parcels are yet modified.
            no_builds = pd.DataFrame()

            # Runs year_update_formatter (the detailed output formatting function) for the base-year parcels.
            if len(base_year_table):
                base_year_table_final = year_update_formatter(base_year_table, no_builds, scenario, 2016)
                # Inserts the base-year parcels into the target table in SQL.
                table_insert(base_year_table_final, 2016, table_type)
    return table_type_list


def year_update_formatter(parcel_table, current_builds, scenario, year):
    """
    Formats the given parcel table for detailed output, with a unique [parcel / year / scenario] combination.

    :param parcel_table:
        The parcel table to be formatted for output. This will either be the parcel or all_parcel table.
    :param current_builds:
        A dataframe of parcels chosen and units added in the current year.
    :param scenario:
        The current scenario_id to apply to the output.
    :param year:
        The iteration year of the simulation.
    :return:
        dataframe: the primary-key appropriate parcel table, with changes based on the current year developments.
    """

    dev_lu_table = orca.get_table('dev_lu_table').to_frame()
    sched_dev = orca.get_table('scheduled_development').to_frame()

    # Add the scheduled development parcels to the parcel table.
    parcel_table = pd.concat([parcel_table, sched_dev])  # type: pd.DataFrame
    parcel_table.rename(columns={"residential_units": "hs"}, inplace=True)

    # Increment is the most recent year divisible by 5 (years 2020-2024 are increment 2020).
    increment = year - (year % 5)

    # The next statement prepares the year_update table by adding increment, chg_hs (how much capacity was added on the
    # parcel in the current year) and cap_hs (remaining capacity after the current year).
    if year == 2016:
        # This segment runs for the base-year table only (pre-simulation).

        year_update = parcel_table.copy()  # type: pd.DataFrame
        year_update['chg_hs'] = 0
        year_update['source'] = 0
        year_update['increment'] = 2016
        year_update['cap_hs'] = year_update['capacity']
    else:
        # This segment runs for each iteration year.

        # For the current year (new) builds, get a list of all parcels with source 3 (regional_overflow) and change any
        # other instances of those parcels to also have source 3. This occurs when a parcel is selected during the
        # normal stochastic building process and is reselected during the regional_overflow.
        current_builds_source_3_list = current_builds.loc[current_builds.source == 3].parcel_id.tolist()
        current_builds.loc[current_builds.parcel_id.isin(current_builds_source_3_list), 'source'] = 3

        # Total the unit change by capacity_type and source on each parcel. This merges parcels affected by the source
        # change immediately above into one entry.
        current_builds_grouped = pd.DataFrame({'chg_hs': current_builds.
                                              groupby(['parcel_id', 'capacity_type', 'source']).units_added.sum()})
        current_builds_grouped.reset_index(inplace=True)

        # Combine the parcel table with the current build information on parcel_id and capacity type. This adds the
        # number of units changed and the source of the change to the parcel table, creating year_update.
        year_update = pd.merge(parcel_table, current_builds_grouped[['parcel_id', 'chg_hs', 'source', 'capacity_type']],
                               how='left', on=['parcel_id', 'capacity_type'])

        # Because the simulation beings in 2017, the lowest increment (other than base-year) is 2017. Add the increment
        # to the year_update table.
        if increment == 2015:
            increment = 2017
        year_update['increment'] = increment

        # Replace NaN values in capacity used with 0, then subtract capacity used from capacity to determine cap_hs.
        year_update['capacity_used'].fillna(0, inplace=True)
        year_update['cap_hs'] = year_update['capacity'] - year_update['capacity_used']

    # Create a list of parcels where the lu_sim matches the plu (lu_sim was previously updated for current year in the
    # parcel_table_update_units function in utils.py.
    updated_lu_list = year_update.loc[year_update.lu_sim == year_update.plu].parcel_id.tolist()

    # This takes any parcel_id identified in the above list and applies the plu to the lu_sim. This seems redundant,
    # but will update other versions of the parcel (with different capacity types).
    year_update['lu_sim'].where(~year_update.parcel_id.isin(updated_lu_list), other=year_update['plu'], inplace=True)

    # Rename columns for clarity in the output table.
    year_update.rename(columns={"jur_or_cpa_id": "cpa_id", "source": "source_id", "capacity_type": "cap_type"},
                       inplace=True)

    # For non-cpa regions (Jurisdictions 1-13, 15-18) set cpa to NaN. Fill in other information for year_update.
    year_update.loc[year_update.cpa_id < 20, 'cpa_id'] = np.nan
    year_update['yr'] = year
    year_update['scenario_id'] = scenario

    # Fill the following columns with 0 if they have NaN values. This is intuitive for capacity and chg_hs. Source 0 is
    # no change. If starting capacity was 0 (NaN before), set the capacity_type to state this.
    year_update['capacity'].fillna(0, inplace=True)
    year_update['chg_hs'] = year_update['chg_hs'].fillna(0)
    year_update['source_id'] = year_update['source_id'].fillna(0)
    year_update['cap_type'] = year_update['cap_type'].fillna("no capacity")

    year_update.sort_values(by=['parcel_id'])
    year_update = year_update.reset_index(drop=True)

    # To more easily track parcels chosen in the regional_overflow section, we add a binary column set to 1 if the
    # parcel had any units built as part of the regional overflow.
    year_update['regional_overflow'] = 0
    parcel_regional_overflow = year_update.loc[year_update.source_id == 3].parcel_id.tolist()
    year_update.loc[year_update.parcel_id.isin(parcel_regional_overflow), 'regional_overflow'] = 1

    # Change the different Smart Growth Opportunity Area sub-codes in capacity_type to SGOA to simplify the pivot.
    year_update.replace('cc', 'sgoa', inplace=True)
    year_update.replace('mc', 'sgoa', inplace=True)
    year_update.replace('tc', 'sgoa', inplace=True)
    year_update.replace('tco', 'sgoa', inplace=True)
    year_update.replace('uc', 'sgoa', inplace=True)

    # Fill all other NaN cells with -99. The pivot fails if there are null values, and no column in our table should
    # have a value of -99, which allows for reconversion to NaN easily after the pivot.
    year_update.fillna(-99, inplace=True)

    # This pivots year_update, maintaining the 'index' columns but condensing the various capacity_type columns into
    # a multidex. In essence, year_update could have had multiple rows for each parcel based on capacity_type, and each
    # entry would have its own cap_hs and chg_hs; after the pivot each parcel will have exactly one row, with columns
    # of cap_hs by cap_type and chg_hs by cap_type.
    year_update_pivot = pd.pivot_table(year_update,
                                       index=['scenario_id', 'increment', 'parcel_id', 'yr', 'lu_2017',
                                              'jurisdiction_id', 'cap_jurisdiction_id', 'cpa_id', 'mgra_id', 'luz_id',
                                              'taz', 'site_id', 'lu_sim', 'plu', 'hs', 'regional_overflow', 'lu_2015',
                                              'dev_type_2015', 'dev_type_2017'],
                                       columns='cap_type',
                                       values=['cap_hs', 'chg_hs']).reset_index()

    # Reset the -99s to NaN.
    year_update_pivot.replace(to_replace=-99, value=np.nan, inplace=True)

    # For each parcel, sum the total capacity and fill any NaN with 0.
    year_update_pivot['tot_cap_hs'] = year_update_pivot['cap_hs'].sum(axis=1)
    year_update_pivot['cap_hs'] = year_update_pivot['cap_hs'].fillna(0)

    # For each parcel, sum the total unit change and fill any NaN with 0.
    year_update_pivot['tot_chg_hs'] = year_update_pivot['chg_hs'].sum(axis=1)
    year_update_pivot['chg_hs'] = year_update_pivot['chg_hs'].fillna(0)

    # Rename the first level of each multidex.
    year_update_pivot.rename(columns={"chg_hs": "chg_hs_", "cap_hs": "cap_hs_"}, inplace=True)

    # Unstack the multidex into a regular index. In essence, the middle line is joining the first level of the multidex
    # column names to the second level. If e[0] is 'chg_hs_' and e[1] is 'jur', it will become 'chg_hs_jur'.
    colnames = year_update_pivot.columns
    ind = pd.Index([e[0] + e[1] for e in colnames.tolist()])
    year_update_pivot.columns = ind

    # Development type is based on land use. This merge brings in the dev_type table based on the current lu (lu_sim).
    year_update_pivot = pd.merge(year_update_pivot, dev_lu_table, how='left', on='lu_sim')

    # There are parcels with dev_type_2017 = 23 or 29, which are military housing. These do no appear in the table
    # being merged here, so we override those and plug in the dev_type_sim manually (can handle case-by-case if needed):
    year_update_pivot.loc[year_update_pivot.dev_type_2017 == 23, 'dev_type_sim'] = 23
    year_update_pivot.loc[year_update_pivot.dev_type_2017 == 29, 'dev_type_sim'] = 29

    # This reorders the columns to how they will be displayed in the SQL table. (Not required to run.)
    year_update_pivot = year_update_pivot[['scenario_id', 'parcel_id', 'yr', 'increment', 'jurisdiction_id',
                                           'cap_jurisdiction_id', 'cpa_id', 'mgra_id', 'luz_id', 'site_id', 'taz',
                                           'hs', 'tot_cap_hs', 'tot_chg_hs', 'lu_2015', 'dev_type_2015', 'lu_2017',
                                           'dev_type_2017', 'plu', 'lu_sim', 'dev_type_sim', 'regional_overflow',
                                           'cap_hs_adu', 'cap_hs_jur', 'cap_hs_sch', 'cap_hs_sgoa', 'chg_hs_adu',
                                           'chg_hs_jur', 'chg_hs_sch', 'chg_hs_sgoa']]
    return year_update_pivot


def scenario_grab(table_type):
    """
    Pulls the current maximum scenario_id from the detailed output table.

    :param table_type:
        The desired detailed output table types chosen in table_setup. This is a list that will contain 0-2 items:
    'all' and / or 'cap'.
    :return:
        int: the maximum scenario_id in the current output table.
    """

    # Pulls the maximum scenario_id from the existing table and converts it into a 1x1 dataframe.
    scenario_sql = '''
    SELECT max(scenario_id)
        FROM [urbansim].[urbansim].[sr14_residential_{}_parcel_results]
    '''.format(table_type)
    scenario_df = pd.read_sql(scenario_sql, mssql_engine)

    # Converts the value of the 1x1 dataframe into an integer.
    scenario = int(scenario_df.values)
    return scenario


def table_insert(parcel_table, year, table_type):
    """
    Inserts the formatted year_update_pivot table into the SQL server.

    :param parcel_table:
        The parcel table to be formatted for output. This will either be the parcel or all_parcel table.
    :param year:
        The iteration year of the simulation.
    :param table_type:
        The desired detailed output table types chosen in table_setup. This is a list that will contain 0-2 items:
    'all' and / or 'cap'.
    :return:
        Does not return an object, but uploads the detailed output to the SQL Server.
    """

    # Begin the SQL connection.
    conn = mssql_engine.connect()

    # Write output table to M drive: \\sandag.org\home\shared\.
    # Save the start time to track how long the program takes to write to the M Drive
    start_time = time.monotonic()

    # Set the path and write the detailed update as a .csv
    # the second path is a more permanent location than my temp file. Planning to save that as final once it is
    # confirmed as working.
    path_name = 'M:\\TEMP\\noz\\outputs\\year_update_{}_{}.csv'.format(table_type, year)
    # path_name = 'M:\\RES\\estimates & forecast\\SR14 Forecast\\UrbanSim\\Capacity_Parcel_Updates\\year_update_{}_{}.csv'.format(table_type, year)
    parcel_table.to_csv(path_name, index=False)

    # Save the end time to track how long the program takes to write to the M Drive.
    end_time = time.monotonic()

    # Display write-out time.
    print("Time to write {}_table to drive:".format(table_type), timedelta(seconds=round(end_time - start_time, 2)))

    # Upload output table into SQL Server database.
    # Save the start time to track how long the program takes to upload to SQL database.
    start_time = time.monotonic()

    # Clear staging table.
    with conn.begin() as trans:
        conn.execute('TRUNCATE TABLE urbansim.sr14_residential_parcel_staging')

    # Insert .csv from M drive to staging table.
    with conn.begin() as trans:
        bulk_insert_staging_sql = '''
                BULK INSERT urbansim.sr14_residential_parcel_staging
                FROM '\\\\sandag.org\\home\\shared\\TEMP\\noz\\outputs\\year_update_{}_{}.csv'
                WITH (FIRSTROW = 2, FIELDTERMINATOR = ',')
                '''.format(table_type, year)
        # '\\\\sandag.org\\home\\shared\\RES\\estimates & forecast\\SR14 Forecast\\UrbanSim\\Capacity_Parcel_Updates\\year_update_{}_{}.csv'
        conn.execute(bulk_insert_staging_sql)

    # Move from staging table to target table.
    with conn.begin() as trans:
        staging_to_target_sql = '''
                INSERT INTO urbansim.urbansim.sr14_residential_{}_parcel_results
                SELECT * FROM urbansim.sr14_residential_parcel_staging
                '''.format(table_type)
        conn.execute(staging_to_target_sql)

    # End the SQL connection.
    conn.close()

    # Save the end time to track how long the program takes to upload to SQL database.
    end_time = time.monotonic()

    # Display write-out time.
    print("Time to insert {}_table to target:".format(table_type), timedelta(seconds=round(end_time - start_time, 2)))


def run_insert(parcel_tables, year):
    """
    Inserts the detailed output table(s) to SQL.

    :param parcel_tables:
        The desired detailed output table types chosen in table_setup. This is a list that will contain 0-2 items:
    'all' and / or 'cap'.
    :param year:
        The iteration year of the simulation.
    :return:
        Does not return an object, but runs the formatter and insert functions for the the detailed outputs.
    """

    all_parcels = orca.get_table('all_parcels').to_frame()
    capacity_parcels = orca.get_table('parcels').to_frame()
    hu_forecast = orca.get_table('hu_forecast').to_frame()

    # Select only builds that occured in the current year.
    current_builds = hu_forecast.loc[(hu_forecast.year_built == year)].copy()

    for table_type in parcel_tables:
        if table_type == "all":
            # Only the normal parcel table is updated normally; the all_parcel table will only be updated if it is
            # selected and passed to here.
            all_parcels_updated = utils.parcel_table_update_units(all_parcels, current_builds)

            # Adds the updated all_parcel table to orca.
            orca.add_table("all_parcels", all_parcels_updated)
            year_update = all_parcels_updated.copy()
        if table_type == "cap":
            year_update = capacity_parcels.copy()
        scenario = scenario_grab(table_type)
        if len(year_update):
            # Runs year_update_formatter (the detailed output formatting function) for the base-year parcels.
            year_update_final = year_update_formatter(year_update, current_builds, scenario, year)

            # Inserts the base-year parcels into the target table in SQL.
            table_insert(year_update_final, year, table_type)
