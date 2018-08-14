import datetime
import numpy as np
import orca
import pandas as pd
import subprocess
import yaml
from database import get_connection_string
from sqlalchemy import create_engine
import sqlalchemy


def yaml_to_dict(yaml_file, yaml_section):
    """
    Load YAML from a file; read specific section to dictionary.

    :param yaml_file:
        File name from which to load YAML.
    :param yaml_section:
        Section of YAML file to process.
    :return:
        dict: Conversion from YAML for a specific section.
    """

    with open(yaml_file, 'r') as f:
            d = yaml.load(f)[yaml_section]

    return d


def add_run_to_db():
    """
    Generates new run_id and saves input information to SQL.

    :return:
        int: the numerical run_id.
    """

    # Link to SQL Server.
    db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
    mssql_engine = create_engine(db_connection_string)

    # Retrieve input information from scenario_config.yaml.
    version_ids = yaml_to_dict('data/scenario_config.yaml', 'scenario')
    # Gives the user an opportunity to describe the run_id.
    run_description = input("Please provide a run description: ")

    # Retrieves maximum existing run_id from the table. If none exists, creates run_id = 1.
    run_id_sql = '''
    SELECT max(run_id)
      FROM [urbansim].[urbansim].[urbansim_lite_output_runs]
    '''
    run_id_df = pd.read_sql(run_id_sql, mssql_engine)
    if run_id_df.values:
        run_id = int(run_id_df.values) + 1
    else:
        run_id = 1

    # Retrieves the input version_id values for the run, places them with run_id in the record.
    subregional_controls = version_ids['subregional_ctrl_id']
    target_housing_units = version_ids['target_housing_units_version']
    phase_year = version_ids['parcel_phase_yr']
    additional_capacity = version_ids['additional_capacity_version']
    scheduled_development = version_ids['sched_dev_version']
    adu_control = version_ids['adu_control']

    # Pulls git of last commit.
    last_commit = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).rstrip()

    # Creates dataframe of run_id information.
    output_records = pd.DataFrame(
        columns=['run_id', 'run_date', 'subregional_controls', 'target_housing_units', 'phase_year',
                 'additional_capacity', 'scheduled_development', 'git', 'run_description', 'adu_control'])

    # Records the time of the commit.
    run_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    # Places the collected information into the dataframe and appends to the existing table in SQL.
    output_records.loc[run_id] = [run_id, run_date, subregional_controls, target_housing_units, phase_year,
                                  additional_capacity, scheduled_development, last_commit, run_description, adu_control]
    output_records.to_sql(name='urbansim_lite_output_runs', con=mssql_engine, schema='urbansim', index=False,
                          if_exists='append')
    return run_id


def write_results(run_id):
    # Link to SQL Server.
    db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
    mssql_engine = create_engine(db_connection_string)

    # Write the output of the model to SQL
    hu_forecast = orca.get_table('hu_forecast').to_frame()
    hu_forecast_out = hu_forecast[['parcel_id', 'units_added', 'year_built', 'source', 'capacity_type']].copy()
    hu_forecast_out.rename(columns={'year_built': 'year_simulation'}, inplace=True)
    hu_forecast_out.rename(columns={'units_added': 'unit_change'}, inplace=True)
    hu_forecast_out['run_id'] = run_id
    hu_forecast_out.to_csv('data/new_units.csv')

    hu_forecast_out.to_sql(name='urbansim_lite_output', con=mssql_engine, schema='urbansim', index=False,
                           if_exists='append', dtype={'parcel_id': sqlalchemy.types.INTEGER(),
                                                      'unit_change': sqlalchemy.types.INTEGER(),
                                                      'year_simulation': sqlalchemy.types.INTEGER(),
                                                      'source': sqlalchemy.types.INTEGER(),
                                                      'capacity_type': sqlalchemy.types.VARCHAR(length=50),
                                                      'run_id': sqlalchemy.types.INTEGER()})


def create_control_percents():
    """
    Generates new control percentages and saves input information to SQL.

    :return:
        int: the numerical run_id.
    """

    # Link to SQL Server.
    db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
    mssql_engine = create_engine(db_connection_string)

    # Retrieves maximum existing run_id from the table. If none exists, creates run_id = 1.
    sub_reg_control_id_sql = '''
      SELECT max(subregional_crtl_id)
      FROM [urbansim].[urbansim].[urbansim_lite_subregional_control]
    '''
    sub_reg_control_id_df = pd.read_sql(sub_reg_control_id_sql, mssql_engine)
    if sub_reg_control_id_df.values:
        sub_reg_control_id = int(sub_reg_control_id_df.values) + 1
    else:
        sub_reg_control_id = 1
    print('\n\nNew subregional control id: %d' %  sub_reg_control_id)
    matchregion = orca.get_table('controls').to_frame()
    matchregion['subregional_crtl_id'] = sub_reg_control_id
    matchregion['geo'] = 'jur'
    matchregion['geo_id'] = matchregion['cap_jurisdiction_id']
    matchregion['control'] = matchregion['share']
    matchregion['control_type'] = 'proportion'
    matchregion['max_units'] = None
    matchregion['scenario_desc'] = 'match_to_region'

    controls = matchregion[
        ['subregional_crtl_id', 'yr', 'geo', 'geo_id', 'control', 'control_type', 'max_units', 'scenario_desc']].copy()
    print('controls to db')
    # controls.to_csv('test2.csv')
    # write to db
    controls.to_sql(name='urbansim_lite_subregional_control', con=mssql_engine, schema='urbansim', index=False,
                   if_exists='append')



def largest_remainder_allocation(regional_targets, target_units):
    """
    Ensures that yearly targets are whole numbers, and that the sum of all targets is the correct total.

    :param regional_targets:
        A dataframe containing geo_id and percentage targets that sum to 1, with one entry per geography.
    :param target_units:
        The integer target total units, after scheduled development and additional dwelling units are accounted for.
    :return:
        dataframe: the given dataframe with an additional column of target units (integer), which sums to target_units.
    """

    regional_targets.reset_index(inplace=True, drop=True)

    # Converts percentages into an array.
    ratios = regional_targets.control.values

    # Converts percentages into float unit totals.
    frac, results = np.modf(target_units * ratios)

    # Determines how many additional units need to be assigned to reach the target.
    remainder = int(target_units - results.sum())

    # Sorts by largest fractional unit.
    indices = np.argsort(frac)[::-1]
    if remainder > 0:
        # Add one to the geographies with the largest fractional unit.
        results[indices[0:remainder]] += 1

    if remainder < 0:
        # If there is an over-allocation of units, deducts the extra(s) from region cpa=1920 (Valley Center).
        # This is because the Unincorporated County expects to fill up later than other cities.
        idx = regional_targets.index[regional_targets.geo_id == 1920]
        results[idx] = results[idx] + remainder
        print('\n\nNegative remainder: %d' % remainder)
    regional_targets['targets'] = results.astype(int).tolist()

    return regional_targets


def parcel_table_update_units(parcel_table, current_builds):
    """
    Updates the parcel table with the results of the model run for the year.

    :param parcel_table:
        The parcel dataframe being updated: 'parcels' will be updated yearly, even if no detailed reporting is selected
    (this is necessary for the code to know what has been built in previous years). 'all_parcels' will only be updated
    if the user has requested the detailed reporting for that table.
    :param current_builds:
        The dataframe of parcels modified in that year.
    :return:
        dataframe: the updated parcel table.
    """

    parcel_table.reset_index(inplace=True, drop=True)

    # Merges the units added in this year with the main parcel table, matching parcel_id and capacity_type (as some
    # parcels exist with multiple capacity types (namely provided capacity and SGOA, or multiple SGOA types)
    updated_parcel_table = pd.merge(parcel_table, current_builds[['parcel_id', 'capacity_type', 'units_added']],
                                    how='left', left_on=['parcel_id', 'capacity_type'],
                                    right_on=['parcel_id', 'capacity_type'])

    # Sets units added to 0 if the parcel was unchanged and adds the value to the amount of capacity used already
    updated_parcel_table.units_added = updated_parcel_table.units_added.fillna(0)
    updated_parcel_table['capacity_used'] = updated_parcel_table['capacity_used'] + updated_parcel_table['units_added']

    # If a parcel has units_added, replace lu_sim (land use in simulation) with the plu (planned land use)
    # If units_added == 0, lu_sim is not modified
    # If lu_sim is null or missing, it is reset to the current lu (lu_2017)
    updated_parcel_table['lu_sim'].where(updated_parcel_table.units_added == 0, other=updated_parcel_table['plu'],
                                         inplace=True)
    updated_parcel_table['lu_sim'].where(~updated_parcel_table.lu_sim.isnull(), other=updated_parcel_table['lu_2017'],
                                         inplace=True)

    # Sums the units added on each parcel (across capacity_types), and updates the parcel with the new total units.
    # This allows parcels with multiple capacity_types to stay consistent with the number of units built on the parcel.
    residential_unit_total = pd.DataFrame({'total_units_added': updated_parcel_table.
                                          groupby(["parcel_id", "residential_units"]).units_added.sum()}).reset_index()
    residential_unit_total['residential_units'] = (residential_unit_total['total_units_added'] +
                                                   residential_unit_total['residential_units'])

    # Partial build is assigned to parcels where units were added but not all units of the capacity_type were built,
    # flagging that parcel for priority construction in the following year.
    updated_parcel_table = updated_parcel_table.drop(['partial_build'], 1)
    updated_parcel_table['partial_build'] = updated_parcel_table.units_added
    updated_parcel_table.partial_build = updated_parcel_table.partial_build.fillna(0)

    # Remove the units_added and residential_units columns from the parcel table, then add the new residential_units
    updated_parcel_table = updated_parcel_table.drop(['units_added'], 1)
    updated_parcel_table = updated_parcel_table.drop(['residential_units'], 1)
    updated_parcel_table = pd.merge(updated_parcel_table, residential_unit_total[['parcel_id', 'residential_units']],
                                    how='left', left_on=['parcel_id'], right_on=['parcel_id'])
    updated_parcel_table.residential_units = updated_parcel_table.residential_units.astype(int)
    return updated_parcel_table


def run_scheduled_development(hu_forecast, households, year):
    """
    Builds the scheduled development parcels.

    :param hu_forecast:
        The dataframe of buildable parcels, used to track new builds during the simulation.
    :param households:
        The dataframe of target units by year.
    :param year:
        The iteration year of the simulation.
    :return:
        Does not return an object, but does update the scheduled_development and hu_forecast tables in orca.
    """

    # Sched_dev_rework branch: 06/28/18
    # scheduled development should no longer be built using priority, due to the issues with needed staggering of
    # development across subregions in early years (and some developments with later end dates). This comment is
    # marking the start of this process. It will either involve a large overhaul of this function, or possibly removal
    # and conversion of sched dev into a special type of priority in regular development.

    # As of 06/06/2018 scheduled_development is being built on a priority system, rather than by scheduled date.
    # Each site_id (and all parcels included in it) are assigned a value 1-10. These priorities have been randomly
    # assigned. All parcels listed as priority 1 are built first, followed by 2, etc. With the exception of ADUs
    # beginning in 2019, all scheduled developments are constructed before any other capacity_type is built.

    print('\n Adding scheduled developments in year: %d' % year)

    # Find the target number of units to be built in the current simulation year.
    target_units = int(households.to_frame().at[year, 'housing_units_add'])
    print('\n Number of households in year: %d' % target_units)

    # Determine if ADUs are expected in the given year, and 'under-produce' scheduled developments to allow for the
    # appropriate number of ADUs without exceeding the target number of units for the year.
    adu_share_df = orca.get_table('adu_allocation').to_frame()
    adu_share = int(round(adu_share_df.loc[adu_share_df['yr'] == year].allocation * target_units, 0))
    target_units = target_units - adu_share

    # Retrieve the scheduled_development table and organize by priority and site_id.
    sched_dev = orca.get_table('scheduled_development').to_frame()
    sched_dev.sort_values(by=['priority', 'site_id'], inplace=True)

    # Select parcels that have more capacity than is used, then calculate how much capacity is remaining.
    # Note: 'capacity' is not subtracted from the built parcels, so 'capacity' should always be >= 'capacity_used'.
    sched_dev_yr = sched_dev.loc[sched_dev['capacity'] > sched_dev['capacity_used']].copy()
    sched_dev_yr['remaining'] = sched_dev_yr['capacity'] - sched_dev_yr['capacity_used']
    sched_dev_yr['remaining'] = sched_dev_yr['remaining'].astype(int)

    # If there is no scheduled development capacity remaining at the start of the year, the function ends here.
    if sched_dev_yr.remaining.sum() > 0:
        # Creates dataframe that has one row for each unit of capacity available. If a parcel has 'capacity' = 20,
        # 'capacity_used' = 18 and 'remaining' = 2, the new dataframe will have 2 rows for that parcel.
        one_row_per_unit = sched_dev_yr.reindex(sched_dev_yr.index.repeat(sched_dev_yr.remaining)).\
            reset_index(drop=True)

        # Takes the subset of the dataframe equal to the target_number_of_units.
        # If target_number_of_units > len(one_row_per_unit), one_row_per_unit_picked will contain the entire
        # one_row_per_unit dataframe.
        one_row_per_unit_picked = one_row_per_unit.head(target_units)

        # Recombines the picked units into a dataframe, and determines how much capacity from each parcel was selected.
        # Due to the nature of the selection, at most one parcel could be not fully built.
        sched_dev_picked = pd.DataFrame({'units_added': one_row_per_unit_picked.groupby(["parcel_id"]).size()}).\
            reset_index()

        # Assigns build information to the parcels built. Source 1 is scheduled development.
        sched_dev_picked['year_built'] = year
        sched_dev_picked['source'] = 1
        sched_dev_picked['capacity_type'] = 'sch'

        # Adds the new scheduled_developments to the hu_forecast table.
        sim_builds = hu_forecast.to_frame(hu_forecast.local_columns)
        sim_units = pd.concat([sim_builds, sched_dev_picked[sim_builds.columns]])  # type: pd.DataFrame
        sim_units.reset_index(drop=True, inplace=True)
        sim_units.source = sim_units.source.astype(int)
        orca.add_table("hu_forecast", sim_units)

        # Updates the scheduled_development table with the selected builds for the iteration year.
        sched_dev_updated = pd.merge(sched_dev, sched_dev_picked[['parcel_id', 'units_added']],
                                     how='left', on='parcel_id')
        sched_dev_updated.units_added.fillna(0, inplace=True)
        sched_dev_updated['residential_units'] = (sched_dev_updated['residential_units'] +
                                                  sched_dev_updated['units_added'])
        sched_dev_updated['capacity_used'] = sched_dev_updated['capacity_used'] + sched_dev_updated['units_added']
        sched_dev_updated = sched_dev_updated.drop(['units_added'], axis=1)
        orca.add_table("scheduled_development", sched_dev_updated)


def run_reducer(hu_forecast, year):
    """
    Account for parcels that have a negative capacity.

    :param hu_forecast:
        The dataframe of buildable parcels, used to track new builds during the simulation.
    :param year:
        The iteration year of the simulation.
    :return:
        Does not return an object, but does update the hu_forecast table in orca.
    """

    # As of 06/06/2018, there are no negative capacity parcels. This function may need to be updated if they are
    # reintroduced, as significant changes have been made since they were last included.

    # Try to load the negative parcel dataframe. If it fails (due to the dataframe being empty) it passes.
    try:
        reducible_parcels = orca.get_table('negative_parcels').to_frame()
    except KeyError:
        pass
    else:
        # Double check that only parcels with negative capacity are included.
        neg_cap_parcels = reducible_parcels[reducible_parcels['capacity_2'] < 0].copy()

        # If a parcel has a specific year to be demolished, it will be selected in that year and won't be available to
        # reduce in other years. All other negative capacity parcels (year is null) will be included.
        reduce_first_parcels = neg_cap_parcels[(neg_cap_parcels.yr == year)].copy()
        neg_cap_parcels = neg_cap_parcels[neg_cap_parcels['yr'].isnull()]

        # Execute the below statement only if there are parcels with negative capacity.
        if (len(neg_cap_parcels) + len(reduce_first_parcels)) > 0:
            # This is designed to evenly space out demolition of parcels yearly through 2024.
            # num_to_reduce is an integer number of parcels to demolish in the simulation year.
            try:
                num_to_reduce = (len(neg_cap_parcels) / (2025 - year)) + len(reduce_first_parcels)
            except ValueError:
                print('There are negative null-year parcels in 2025!')
                num_to_reduce = len(neg_cap_parcels) + len(reduce_first_parcels)
            num_to_reduce = min((len(neg_cap_parcels) + len(reduce_first_parcels)), int(np.ceil(num_to_reduce)))

            # Randomly reorder the null-year parcels, then append them below the list of year-specific parcels.
            random_neg_parcels = neg_cap_parcels.sample(frac=1, random_state=50).reset_index(drop=False)
            parcels_to_reduce = pd.concat([reduce_first_parcels, random_neg_parcels])  # type: pd.DataFrame

            # Selects the parcels to reduce.
            parcels_reduced = parcels_to_reduce.head(num_to_reduce)
            parcels_reduced = parcels_reduced.set_index('index')

            # Assigns build information to the parcels chosen. Source 4 is demolition / reduction.
            # This section may need updating if new negative parcels are introduced.
            parcels_reduced['year_built'] = year
            parcels_reduced['hu_forecast_type_id'] = ''
            parcels_reduced['residential_units'] = parcels_reduced['capacity_2']
            parcels_reduced['source'] = 4
            for parcel in parcels_reduced['parcel_id'].tolist():
                reducible_parcels.loc[reducible_parcels.parcel_id == parcel, 'capacity_2'] = 0

            # Updates the negative_parcel and hu_forecast tables.
            orca.add_table("negative_parcels", reducible_parcels)
            all_hu_forecast = pd.concat([hu_forecast.to_frame(hu_forecast.local_columns),
                                         parcels_reduced[hu_forecast.local_columns]])  # type: pd.DataFrame
            all_hu_forecast.reset_index(drop=True, inplace=True)
            orca.add_table("hu_forecast", all_hu_forecast)


def run_subregional_share(year,households):
    """
    Determines feasible parcels for iteration year.

    :param year:
        The iteration year of the simulation.
    :return:
        Does not return an object, but adds a dataframe of feasible parcels to orca.
    """

    controls = orca.get_table('controls').to_frame()
    parcels = orca.get_table('parcels').to_frame()
    jcpa = parcels.cap_jurisdiction_id.unique().tolist() # list of all jcpa for index
    phase_yr = orca.get_table('devyear').to_frame()
    phase_yr.reset_index(inplace=True,drop=False)
    # use phase year only for adus
    # phase_yr = phase_yr.loc[phase_yr.capacity_type=='adu']
    parcels = pd.merge(parcels, phase_yr, how='left', left_on=['parcel_id', 'capacity_type'],
                       right_on=['parcel_id', 'capacity_type'])
    parcels.phase_yr = parcels.phase_yr.fillna(2017)
    parcels = parcels.loc[parcels['phase_yr'] <= year].copy()
    #parcels.loc[(parcels.jur_or_cpa_id == 1432), 'capacity'] = 0
    # parcels.loc[(parcels.jur_or_cpa_id == 1467), 'capacity'] = 0
    # slim_df = parcels[['cap_jurisdiction_id', 'capacity', 'capacity_type', 'jur_or_cpa_id']].copy()
    # capacity = pd.DataFrame({'capacity': parcels.groupby(['jur_or_cpa_id']).capacity.sum()}).reset_index()
    # capacity = pd.DataFrame({'capacity': parcels.groupby(['cap_jurisdiction_id']).capacity.sum()}).reset_index()
    #capacity.loc[(capacity.cap_jurisdiction_id == 6), 'capacity'] = capacity.loc[(capacity.cap_jurisdiction_id == 6)].capacity.values[0] + 85
    #capacity.loc[(capacity.cap_jurisdiction_id == 8), 'capacity'] = capacity.loc[(capacity.cap_jurisdiction_id == 8)].capacity.values[0] + 260 # 275 running out of units
    # capacity.loc[(capacity.cap_jurisdiction_id == 19), 'capacity'] = capacity.loc[(capacity.cap_jurisdiction_id == 19)].capacity.values[0] + 6000

    # set capacity to zero for 1432 and 1467
    #capacity.loc[(capacity.jur_or_cpa_id == 1432), 'capacity'] = 0
    #capacity.loc[(capacity.jur_or_cpa_id == 1467), 'capacity'] = 0
    #cpas with majority sched dev: 1462,1464,1465, 1954
    # Torrey Highlands, Black Mountain Ranch, Pacific Highlands Ranch, Pendleton-De Luz,

    # subtract capacity used based on controls from previous year
    controls_yr = controls.loc[controls.yr == year - 1]
    hh_df = households.to_frame()
    hh_df.reset_index(inplace=True)
    hh_yr = hh_df.loc[hh_df.yr==year].housing_units_add.values[0]
    capacity = pd.DataFrame({'capacity': parcels.groupby(['cap_jurisdiction_id']).capacity.sum()}).reset_index()

    capacity = pd.merge(capacity, controls_yr[['cap_jurisdiction_id', 'capacity_used']], left_on=['cap_jurisdiction_id'],
                         right_on=['cap_jurisdiction_id'], how='left')
    capacity['capacity_used'].fillna(0, inplace=True)
    capacity['rem'] = capacity['capacity'] - capacity['capacity_used']

    # set percent
    capacity['tot'] = capacity.rem.sum()
    capacity.loc[capacity.cap_jurisdiction_id == 19, 'rem'] = .169 * capacity['tot'].values[0] # Unincorporated
    capacity['tot'] = capacity.rem.sum()
    capacity.loc[capacity.cap_jurisdiction_id == 7, 'rem'] = 0.04 * capacity['tot'].values[0] #Escondido
    capacity['tot'] = capacity.rem.sum()
    capacity.loc[capacity.cap_jurisdiction_id == 9, 'rem'] = 0.0286 * capacity['tot'].values[0] #La Mesa
    capacity['tot'] = capacity.rem.sum()
    capacity.loc[capacity.cap_jurisdiction_id == 12, 'rem'] = 0.022 * capacity['tot'].values[0]  # Oceanside
    capacity['tot'] = capacity.rem.sum()
    capacity.loc[capacity.cap_jurisdiction_id == 11, 'rem'] = 0.018 * capacity['tot'].values[0]  # National City
    capacity['tot'] = capacity.rem.sum()
    capacity.loc[capacity.cap_jurisdiction_id == 18, 'rem'] =  0.0203 * capacity['tot'].values[0]  # Vista
    capacity['tot'] = capacity.rem.sum()
    capacity.loc[capacity.cap_jurisdiction_id == 1, 'rem'] = .0175 * capacity['tot'].values[0] #Carlsbad
    capacity['tot'] = capacity.rem.sum()
    #capacity.loc[capacity.cap_jurisdiction_id == 16, 'rem'] = 0.0125 * capacity['tot'].values[0]  # Santee - change in sched dev
    #capacity['tot'] = capacity.rem.sum()
    capacity.loc[capacity.cap_jurisdiction_id == 8, 'rem'] = .00865 * capacity['tot'].values[0] #Imperial Beach
    capacity['tot'] = capacity.rem.sum()
    capacity.loc[capacity.cap_jurisdiction_id == 6, 'rem'] = .0072 * capacity['tot'].values[0] #Encinitas
    capacity['tot'] = capacity.rem.sum()
    capacity.loc[capacity.cap_jurisdiction_id == 13, 'rem'] = 0.0052 * capacity['tot'].values[0]  # Poway  0.0055
    capacity['tot'] = capacity.rem.sum()
    capacity.loc[capacity.cap_jurisdiction_id == 10, 'rem'] = 0.0038 * capacity['tot'].values[0]  # Lemon Grove 0.0040
    capacity['tot'] = capacity.rem.sum()
    if year > 2035:
        capacity.loc[capacity.cap_jurisdiction_id == 2, 'rem'] = 0.155  * capacity['tot'].values[0] # Chula Vista
        capacity['tot'] = capacity.rem.sum()
    else:
        capacity.loc[capacity.cap_jurisdiction_id==2, 'rem'] = 0.039 * capacity['tot'].values[0] # Chula Vista
        capacity['tot'] = capacity.rem.sum()


    #if year > 2035:
    #    capacity.loc[capacity.cap_jurisdiction_id == 19, 'rem'] = 1.5 * capacity.loc[capacity.cap_jurisdiction_id == 19].rem
    #else:
    #    capacity.loc[capacity.cap_jurisdiction_id==19, 'rem'] = .75 * capacity.loc[capacity.cap_jurisdiction_id==19].rem
    capacity['tot'] = capacity.rem.sum()

    # calculate subregional share. remaining capacity divided by total capacity
    capacity['share'] = capacity.rem / capacity.tot
    capacity['yr'] = year
   #     capacity.loc[(capacity.jur_or_cpa_id == 1407), 'share'] = capacity.loc[(capacity.jur_or_cpa_id == 1407)].share * 2
   #     capacity.loc[(capacity.jur_or_cpa_id == 1433), 'share'] = capacity.loc[(capacity.jur_or_cpa_id == 1433)].share.values[0] - \
   #                                                               capacity.loc[(capacity.jur_or_cpa_id == 1407)].share.values[0] * 1
    # calculate capacity used by multiplying w/ households

    capacity = pd.merge(capacity, hh_df[['yr','housing_units_add']], left_on='yr', right_on='yr', how='left')
    capacity['new_capacity_used'] = capacity['share'] * capacity['housing_units_add']
    capacity['capacity_used'] = capacity['new_capacity_used'] + capacity['capacity_used']
    capacity.drop(['new_capacity_used'], axis=1,inplace=True)
    # add to controls table with shares for each year
    # add every cpa - fill with zeros for those w no capacity in given yr
    capacity.set_index('cap_jurisdiction_id',inplace=True)
    capacity = capacity.reindex(jcpa)
    capacity.fillna(0,inplace=True)
    capacity['yr'] = year # fill in year for cpas with zero capacity
    capacity.reset_index(inplace=True, drop=False)
    controls = pd.concat([controls,capacity])
    orca.add_table("controls", controls)


def run_feasibility(year):
    """
    Determines feasible parcels for iteration year.

    :param year:
        The iteration year of the simulation.
    :return:
        Does not return an object, but adds a dataframe of feasible parcels to orca.
    """

    print("Computing feasibility")

    # Retrieve dataframes of parcels and development restrictions, and combines them by parcel_id and capacity_type.
    parcels = orca.get_table('parcels').to_frame()
    devyear = orca.get_table('devyear').to_frame()
    parcels.reset_index(inplace=True, drop=True)
    devyear.reset_index(inplace=True, drop=False)
    parcels = pd.merge(parcels, devyear, how='left', left_on=['parcel_id', 'capacity_type'],
                       right_on=['parcel_id', 'capacity_type'])
    parcels.set_index('parcel_id', inplace=True)

    # Select parcels that have more capacity than is used.
    # Note: 'capacity' is not subtracted from the built parcels, so 'capacity' should always be >= 'capacity_used'.
    feasible_parcels = parcels.loc[parcels['capacity'] > parcels['capacity_used']].copy()
    feasible_parcels.phase_yr = feasible_parcels.phase_yr.fillna(2017)
    # Restrict feasible parcels based on assigned phase years (not feasible before phase year occurs).
    feasible_parcels = feasible_parcels.loc[feasible_parcels['phase_yr'] <= year].copy()
    orca.add_table("feasibility", feasible_parcels)


def adu_picker(year, current_hh, feasible_parcels_df,subregional_targets):
    """
    Selects additional dwelling unit parcels to build each year (1 additional unit on an existing residential parcel).

    :param year:
        The iteration year of the simulation.
    :param current_hh:
        The integer target total units, after scheduled development units are accounted for.
    :param feasible_parcels_df:
        The dataframe generated in feasibility (contains parcels that are available to build on).
    :return:
        dataframe: the selected ADU table.
    """

    # As of 06/06/2018, the external table used for these targets was generated manually. In the future, this would
    # ideally be constructed algorithmically. The goal is to take the total 'planned' number of ADUs from the cities of
    # San Diego, Chula Vista, Oceanside and El Cajon and build ~half of those from 2019-2034. This should be only 1-2%
    # of yearly targets, based on current estimates. After 2035 all ADUs from all regions are feasible, but we want to
    # build a consistent proportion of them per year (~5-10% of yearly targets). (Currently we use 0% in 2017-2018,
    # 2% 2019-2034, and 10% 2035-2050. In 2050 it becomes 13% to absorb the remaining ADUs.)

    # Bring in the ADU allocation table and determine the share of units for the year.
    # Note: Due to the priority system used in scheduled development, both sections should be double checked that they
    # match here or the model can over/under produce.
    adu_share_df = orca.get_table('adu_allocation').to_frame()
    adu_share = int(round(adu_share_df.loc[adu_share_df['yr'] == year].allocation * current_hh, 0))

    # Only choose from feasible parcels with ADU capacity_type.
    adu_parcels = feasible_parcels_df.loc[(feasible_parcels_df.capacity_type == 'adu')].copy()

    # Randomize the parcels to be selected, and select the target number.
    # try:
    #     # shuffled_adu = adu_parcels.sample(frac=1, random_state=50).reset_index(drop=False)
    #     adu_select_list = pd.DataFrame()
    #     while len(adu_select_list) < adu_share:
    #         for geo in adu_parcels.jur_or_cpa_id.unique().tolist():
    #             adu_grab = adu_parcels.loc[adu_parcels['jur_or_cpa_id'] == geo].head(1)
    #             adu_parcels = adu_parcels[~adu_parcels.index.isin(adu_grab.index)]
    #             adu_select_list = pd.concat([adu_select_list, adu_grab])
    #
    # except ValueError:
    #     adu_select_list = adu_parcels
    # picked_adu_parcels = adu_select_list.head(adu_share).copy()


    try:
        shuffled_adu = adu_parcels.sample(frac=1, random_state=50).reset_index(drop=False)
    except ValueError:
        shuffled_adu = adu_parcels
    picked_adu_parcels = shuffled_adu.head(adu_share).copy()

    #    if len(targets_w_adus.loc[targets_w_adus.rem < 0]):
    #        print(len(targets_w_adus.loc[targets_w_adus.rem < 0]))
    adu_jcpa = pd.DataFrame({'adu_sum':  picked_adu_parcels.
                             groupby(["jurisdiction_id"]).capacity.sum()}).reset_index()

    targets_w_adus = pd.merge(subregional_targets,adu_jcpa,how='left',left_on='geo_id',right_on = 'jurisdiction_id')

    targets_w_adus['rem'] = targets_w_adus['targets'] - targets_w_adus['adu_sum']

    jcpas_w_overage = targets_w_adus.loc[targets_w_adus.rem < 0].jurisdiction_id.tolist()

    for jur in jcpas_w_overage:
        # int(grp.iloc[0]['rem'])
        extra_units = int(abs(targets_w_adus.loc[targets_w_adus.geo_id == jur].rem.iloc[0]))
        parcels_to_drop = (
            picked_adu_parcels.loc[picked_adu_parcels['jurisdiction_id'] == jur].head(extra_units)).parcel_id.tolist()
        picked_adu_parcels = picked_adu_parcels[~picked_adu_parcels.parcel_id.isin(parcels_to_drop)].copy()

    # Assigns build information to the parcels built. Source 5 is ADU.
    picked_adu_parcels['source'] = 5
    picked_adu_parcels['units_added'] = 1
    return picked_adu_parcels

    # Assigns build information to the parcels built. Source 5 is ADU.
  #  picked_adu_parcels['source'] = 5
  ##  picked_adu_parcels['units_added'] = 1
  #  return picked_adu_parcels


def parcel_picker(parcels_to_choose, target_number_of_units, name_of_geo, year_simulation):
    """
    Chooses parcels to build by region, and how much to build on them, for the simulation.

    :param parcels_to_choose:
        A subset of the feasibility dataframe, limited to parcels in the geographical region identified by name_of_geo.
    :param target_number_of_units:
        The target number of units to build in this geographical region, determined by the sub-regional target
    percentage derived from the largest_remainder_allocation function, the target total units, and any other
    limitations in place for the region (ie. a region may ask to intentionally limit production in certain years).
    :param name_of_geo:
        An integer geography_id for the sub-region being simulated. Currently this is the jurisdiction_id (1-13, 15-18)
    for most cities in the region. The City of San Diego (jurisdiction_id = 14) and the Unincorporated regions of the
    county (jurisdiction_id = 19) are broken down further into CPA-level areas.
        This could accommodate any geographical distinction, such as MGRA or LUZ.
        This could also be 'all', meaning that after all the regions were fed through the parcel_picker, there was a
    shortage of units below the year's target total units. This occurs if a sub-region has less available units than
    the target_number_of_units. The picker will instead choose from feasible parcels randomly from the entire county.
        Note: There are possible edge cases in the 'all' cycle where a sub-region may be given units beyond what is
    requested by the jurisdiction (as of 06/06/2018 no jurisdiction has requested specific limitations). However, the
    'all' cycle allows for a level of randomized selection that we believe is reasonable to assume in any given year.
    :param year_simulation:
        The iteration year of the simulation.
    :return:
        dataframe: the parcels selected for the region, and the number of units added to those parcels.
    """

    original_target = target_number_of_units
    # Create an empty dataframe to add selected parcels to.
    parcels_picked = pd.DataFrame()

    # This checks that the sub-region has a target number of units to build. If not, the function ends and returns the
    # empty dataframe.
    if target_number_of_units > 0:

        # This checks if there are enough units to meet the sub-region's target_number_of_units. If not, it prints a
        # warning and then builds all available units in the sub-region (if there are any). Otherwise it proceeds to
        # randomized selection of parcels (the 'else' statement).
        if parcels_to_choose.remaining_capacity.sum() < target_number_of_units:
            print("WARNING: NOT ENOUGH UNITS TO MATCH DEMAND FOR", name_of_geo, "IN YEAR", year_simulation)

            # This checks if there are any units available to build. If so, they are all used up. If not, the function
            # ends and returns an empty dataframe.
            if len(parcels_to_choose):
                parcels_picked = parcels_to_choose
                parcels_picked['units_added'] = parcels_picked['remaining_capacity']
                parcels_picked.drop(['site_id', 'remaining_capacity'], axis=1, inplace=True)
        else:
            # Randomize the order of available parcels
            shuffled_parcels = parcels_to_choose.sample(frac=1, random_state=50).reset_index(drop=False)

            capacity_sch = shuffled_parcels.loc[(shuffled_parcels.capacity_type == 'sch')].copy()
            capacity_site = capacity_sch.groupby(['site_id', 'phase_yr', 'priority', 'jur_or_cpa_id']).\
                agg({'remaining_capacity': 'sum', 'partial_build': 'sum'}).reset_index()

            # Subset parcels that are partially completed from the year before
            previously_picked = shuffled_parcels.loc[(shuffled_parcels.partial_build > 0) &
                                                     (shuffled_parcels.capacity_type != 'sch')]

            # Subset parcels with jurisdiction-provided capacity and that aren't partially built
            capacity_jur = shuffled_parcels.loc[(shuffled_parcels.capacity_type == 'jur') &
                                                (shuffled_parcels.partial_build == 0)]

            # Subset ADU parcels
            adu_parcels = shuffled_parcels.loc[shuffled_parcels.capacity_type == 'adu']
            # This places parcels that are partially built before jurisdiction-provided parcels
            # changed order - sched dev first. otherwise previously picked sgoa was getting
            # chosen ahead of sched dev for chula vista

            if len(capacity_site):
                capacity_site.sort_values(by=['partial_build', 'priority', 'site_id'], ascending=[False, True, True],
                                          inplace=True)
                capacity_site['units_for_year'] = capacity_site.remaining_capacity
                if year_simulation < 2048:
                    large_build_checker = capacity_site.remaining_capacity >= 250
                    capacity_site.loc[large_build_checker, 'units_for_year'] = 250
                # if capacity_site.units_for_year.sum() < target_number_of_units:
                #     capacity_site['units_for_year'] = capacity_site.remaining_capacity
                one_row_per_unit = capacity_site.reindex(capacity_site.index.repeat(
                    capacity_site.units_for_year)).reset_index(drop=True)
                one_row_per_unit_picked = one_row_per_unit.head(target_number_of_units)
                sites_picked = pd.DataFrame({'units_added': one_row_per_unit_picked.groupby(['site_id']).
                                            size()}).reset_index()
                new_sch = capacity_sch[['parcel_id', 'site_id', 'remaining_capacity']].sample(frac=1,random_state=50).\
                    sort_values(by='site_id')
                merge_sch = pd.merge(new_sch, sites_picked, how='left', on=['site_id'])
                merge_sch['cum_remaining'] = merge_sch.groupby('site_id')['remaining_capacity'].cumsum()
                merge_sch['remaining_units'] = merge_sch['units_added'].subtract(merge_sch['cum_remaining'],
                                                                                 level=merge_sch['site_id'])
                merge_sch['remaining_units'] = merge_sch.groupby(['site_id'])['remaining_units'].shift(1)
                merge_sch.loc[merge_sch['units_added'].notnull(), 'remaining_units'] = \
                    merge_sch.loc[:, ['units_added', 'remaining_units']].min(axis=1)
                merge_sch.loc[merge_sch['units_added'].notnull(), 'units_added'] = \
                    merge_sch.loc[:, ['remaining_capacity', 'remaining_units']].min(axis=1)
                merge_sch.loc[merge_sch['units_added'] < 0, 'units_added'] = 0
                merge_sch = merge_sch.loc[merge_sch['units_added'] > 0]
                sch_picked = merge_sch[['parcel_id', 'units_added']].copy()
                shuffled_parcels = pd.merge(shuffled_parcels, sch_picked, how='left', on=['parcel_id'])
                shuffled_parcels['units_added'] = shuffled_parcels['units_added'].fillna(0).astype(int)
                shuffled_parcels['remaining_capacity'] = shuffled_parcels['remaining_capacity'] - shuffled_parcels[
                    'units_added']
                shuffled_parcels = shuffled_parcels.drop(['units_added'], axis=1)
                sch_picked['capacity_type'] = 'sch'
                target_number_of_units = int(target_number_of_units - merge_sch['units_added'].sum())
                selected_sites_parcels = shuffled_parcels[shuffled_parcels['site_id'].isin(merge_sch.site_id.values.tolist())]
                shuffled_parcels = shuffled_parcels[
                    ~shuffled_parcels['site_id'].isin(merge_sch.site_id.values.tolist())]

            else:
                sch_picked = pd.DataFrame(columns=['parcel_id', 'capacity_type', 'units_added'])
                selected_sites_parcels = pd.DataFrame()

            # Non-Scheduled Development Parcels
            priority_parcels = pd.concat([previously_picked, capacity_jur])

            # Remove the subset parcels above from the rest of the shuffled parcels. In effect this should leave
            # only parcels with an SGOA capacity_type in the shuffled parcels dataframe.
            shuffled_parcels = shuffled_parcels[
                ~shuffled_parcels['parcel_id'].isin(priority_parcels.parcel_id.values.tolist())]
            shuffled_parcels = shuffled_parcels[
                ~shuffled_parcels['parcel_id'].isin(adu_parcels.parcel_id.values.tolist())]

            # This places the SGOA parcels and ADU parcels after the prioritized parcels.
            if year_simulation < 2048:
                priority_then_random = pd.concat([priority_parcels, selected_sites_parcels, shuffled_parcels,
                                                  adu_parcels])
            else:
                priority_then_random = pd.concat([selected_sites_parcels, priority_parcels, shuffled_parcels,
                                                  adu_parcels])

            # This section prohibits building very large projects in one year. If a parcel has over 250 or 500
            # available capacity, is capped at 250 or 500, respectively, units when selected. This generally assumes
            # that larger projects can build faster than smaller ones, but prevents them from building instantaneously.
            priority_then_random['units_for_year'] = priority_then_random.remaining_capacity
            if year_simulation < 2048:
                large_build_checker = priority_then_random.remaining_capacity >= 250
                priority_then_random.loc[large_build_checker, 'units_for_year'] = 250
            #max_build_checker = priority_then_random.remaining_capacity >= 500
            #priority_then_random.loc[max_build_checker, 'units_for_year'] = 500

            # This statement allows a sub-region to fully complete large projects immediately if there is not enough
            # other capacity to reach the target_number_of_units. This also allows a large parcel picked late in the
            # simulation to be completed by 2050 if it wouldn't otherwise.
            if priority_then_random.units_for_year.sum() < target_number_of_units:
                priority_then_random['units_for_year'] = priority_then_random.remaining_capacity

            # Creates dataframe that has one row for each 'units_for_year' on a parcel in priority_then_random. If a
            # parcel has 'units_for_year' = 2, the new dataframe will have 2 rows for that parcel.
            one_row_per_unit = priority_then_random.reindex(priority_then_random.index.repeat(
                priority_then_random.units_for_year)).reset_index(drop=True)

            # Takes the subset of the dataframe equal to the target_number_of_units.
            # If target_number_of_units > len(one_row_per_unit), one_row_per_unit_picked will contain the entire
            # one_row_per_unit dataframe.
            one_row_per_unit_picked = one_row_per_unit.head(target_number_of_units)

            # Recombines the picked units into a dataframe, and determines how much capacity from each parcel was
            # selected. Due to the nature of the selection, one parcel may be split in addition to the large_builds.
            parcels_picked = pd.DataFrame({'units_added': one_row_per_unit_picked.groupby(
                ["parcel_id", 'capacity_type']).size()}).reset_index()

            parcels_picked = pd.concat([parcels_picked, sch_picked])

            parcels_picked.set_index('parcel_id', inplace=True)
            if original_target != parcels_picked['units_added'].sum():
                print('{0} UNITS PRODUCED INSTEAD OF TARGET {1}!'.format(parcels_picked['units_added'].sum(), original_target))
    return parcels_picked


    #
    #
    #         capacity_sch = parcels_to_choose.loc[(parcels_to_choose.capacity_type == 'sch')].copy()
    #         parcels_to_choose = parcels_to_choose.loc[parcels_to_choose.capacity_type != 'sch'].copy()
    #         previously_picked = parcels_to_choose.loc[parcels_to_choose.partial_build > 0]
    #         parcels_to_choose = parcels_to_choose.loc[parcels_to_choose.partial_build <= 0].copy()
    #
    #         parcels_to_choose['weight'] = 0
    #         jur_weight = parcels_to_choose.loc[parcels_to_choose.capacity_type=='jur'].capacity.sum()
    #         adu_weight = parcels_to_choose.loc[parcels_to_choose.capacity_type == 'adu'].remaining_capacity.sum()
    #         sgoa_weight = parcels_to_choose.loc[parcels_to_choose.capacity_type.isin(['cc','mc','tc','tco','uc'])].capacity.sum()
    #
    #         parcels_to_choose.loc[(parcels_to_choose.capacity_type == 'jur'), 'weight'] = jur_weight * 4
    #         parcels_to_choose.loc[(parcels_to_choose.capacity_type == 'adu'), 'weight'] = adu_weight
    #         parcels_to_choose.loc[(parcels_to_choose.capacity_type == 'cc'), 'weight'] = sgoa_weight/4
    #         parcels_to_choose.loc[(parcels_to_choose.capacity_type == 'mc'), 'weight'] = sgoa_weight/4
    #         parcels_to_choose.loc[(parcels_to_choose.capacity_type == 'tc'), 'weight'] = sgoa_weight/4
    #         parcels_to_choose.loc[(parcels_to_choose.capacity_type == 'tco'), 'weight'] = sgoa_weight/4
    #         parcels_to_choose.loc[(parcels_to_choose.capacity_type == 'uc'), 'weight'] = sgoa_weight/4
    #
    #
    #         parcels_to_choose.loc[(parcels_to_choose.capacity_type == 'jur'), 'weight'] = 1000
    #         parcels_to_choose.loc[(parcels_to_choose.capacity_type == 'adu'), 'weight'] = 10
    #         parcels_to_choose.loc[(parcels_to_choose.capacity_type == 'cc'), 'weight'] = 1
    #         parcels_to_choose.loc[(parcels_to_choose.capacity_type == 'mc'), 'weight'] = 1
    #         parcels_to_choose.loc[(parcels_to_choose.capacity_type == 'tc'), 'weight'] = 1
    #         parcels_to_choose.loc[(parcels_to_choose.capacity_type == 'tco'), 'weight'] = 1
    #         parcels_to_choose.loc[(parcels_to_choose.capacity_type == 'uc'), 'weight'] = 1
    #
    #
    #         #parcels_to_choose[['capacity_type', 'remaining_capacity', 'weight']].head()
    #         if len(parcels_to_choose) > 0:
    #             shuffled_parcels = parcels_to_choose.sample(frac=1,replace=False,weights='weight',random_state=50).reset_index(drop=False)
    #         else:
    #             shuffled_parcels = pd.DataFrame()
    #         #shuffled_parcels[['parcel_id','capacity_type', 'remaining_capacity', 'weight']].head()
    #
    #         capacity_sch.sort_values(by=['priority', 'site_id'], inplace=True)
    #
    #         # This places parcels that are partially built before jurisdiction-provided parcels
    #
    #         priority_parcels = pd.concat([previously_picked,capacity_sch])
    #         priority_parcels.reset_index(inplace=True,drop=False)
    #         priority_then_random = pd.concat([priority_parcels, shuffled_parcels])
    #
    #         # This section prohibits building very large projects in one year. If a parcel has over 250 or 500
    #         # available capacity, is capped at 250 or 500, respectively, units when selected. This generally assumes
    #         # that larger projects can build faster than smaller ones, but prevents them from building instantaneously.
    #         priority_then_random['units_for_year'] = priority_then_random.remaining_capacity
    #         large_build_checker = priority_then_random.remaining_capacity >= 250
    #         priority_then_random.loc[large_build_checker, 'units_for_year'] = 250
    #         max_build_checker = priority_then_random.remaining_capacity >= 500
    #         priority_then_random.loc[max_build_checker, 'units_for_year'] = 500
    #
    #         # This statement allows a sub-region to fully complete large projects immediately if there is not enough
    #         # other capacity to reach the target_number_of_units. This also allows a large parcel picked late in the
    #         # simulation to be completed by 2050 if it wouldn't otherwise.
    #         if priority_then_random.units_for_year.sum() < target_number_of_units:
    #             priority_then_random['units_for_year'] = priority_then_random.remaining_capacity
    #
    #         # Creates dataframe that has one row for each 'units_for_year' on a parcel in priority_then_random. If a
    #         # parcel has 'units_for_year' = 2, the new dataframe will have 2 rows for that parcel.
    #         priority_then_random.reset_index(inplace=True,drop=True)
    #         one_row_per_unit = priority_then_random.reindex(priority_then_random.index.repeat(
    #             priority_then_random.units_for_year)).reset_index(drop=True)
    #
    #         # Takes the subset of the dataframe equal to the target_number_of_units.
    #         # If target_number_of_units > len(one_row_per_unit), one_row_per_unit_picked will contain the entire
    #         # one_row_per_unit dataframe.
    #         one_row_per_unit_picked = one_row_per_unit.head(target_number_of_units)
    #
    #         # Recombines the picked units into a dataframe, and determines how much capacity from each parcel was
    #         # selected. Due to the nature of the selection, one parcel may be split in addition to the large_builds.
    #         parcels_picked = pd.DataFrame({'units_added': one_row_per_unit_picked.groupby(
    #             ["parcel_id", 'capacity_type']).size()}).reset_index()
    #         parcels_picked.set_index('parcel_id', inplace=True)
    # return parcels_picked


def run_developer(households, hu_forecast, reg_controls, supply_fname, feasibility, year):
    """
    Run the developer model to pick and build for the hu_forecast.

    :param households:
        A dataframe wrapper with the target number of units by year.
    :param hu_forecast:
        The dataframe wrapper of constructed units. This starts as an empty dataframe with columns for parcel_id,
    units_added, year_built, source, and capacity_type. For each iteration year, this dataframe is populated with the
    relevant information for each parcel chosen and built. The final hu_forecast table is the output used for other
    modeling.
    :param reg_controls:
        A dataframe wrapper containing the percentage build targets for each of sub-regions for each year. In every
    year the sum of the percentages should be equal to 1 (100%), which will be multiplied by the total target units for
    the year. This will determine the target number of housing units for each sub-region. The values for each year are
    passed to the largest_remainder_allocation function to ensure the appropriate number of units is targeted after
    rounding the percentages. Additionally, this dataframe contains a max_units column, in the event that a sub-region
    wanted to intentionally set a unit limit in certain years.
    :param supply_fname:
        A string set to be "units_added". This is a column in the hu_forecast output with the units added per parcel
    per year. This allows the function to track the total amount of units already added during the simulation.
    :param feasibility:
        The dataframe wrapper of feasible parcels generated by the run_feasibility function.
    :param year:
        The iteration year of the simulation.
    :return:
        Does not return an object, but does update the hu_forecast table in orca.
    """

    # Unwraps the dataframes
    control_totals = reg_controls.to_frame()
    hh_df = households.to_frame()
    feasible_parcels_df = feasibility.to_frame()
    hu_forecast_df = hu_forecast.to_frame()

    # Creates empty dataframe to track added parcels
    sr14cap = pd.DataFrame()
    if year==2019:
        print(year)

    # Pull out the control totals for only the current iteration year
    control_totals_by_year = control_totals.loc[control_totals.yr == year].copy()
    #
    # for jur in [14, 19]:
    #     control_adjustments = control_totals_by_year.loc[control_totals_by_year.jurisdiction_id == jur].copy()
    #     jur_percent = control_adjustments.control.sum()
    #     slim_df = feasible_parcels_df[['cap_jurisdiction_id', 'capacity', 'capacity_type', 'jur_or_cpa_id']].copy()
    #     slim_df.rename(columns={"cap_jurisdiction_id": "jur_id", "capacity": "cap", "jur_or_cpa_id": "jcpa"},
    #                    inplace=True)
    #     slim_df.replace(['cc', 'mc', 'tc', 'tco', 'uc'], 'sgoa', inplace=True)
    #     units_available = slim_df.loc[slim_df.jur_id == jur].copy()
    #     if len(units_available.loc[units_available.capacity_type == 'sch']) > 0:
    #       units_available = units_available.groupby(['jcpa', 'capacity_type'], as_index=False)['cap'].sum()
    #       sch_cpas = units_available.loc[units_available.capacity_type == 'sch'].jcpa.tolist()
    #       no_sch_cpas = [x for x in units_available.jcpa.tolist() if x not in sch_cpas]
    #       units_available = units_available.groupby('capacity_type', as_index=False)['cap'].sum()
    #       sch_target = units_available.loc[units_available.capacity_type == 'sch'].cap.values[0]
    #       if len(sch_cpas) and (sch_target > 0):
    #           adjust_sch = ((.7*jur_percent) / control_adjustments.loc[control_adjustments.geo_id.isin(sch_cpas)].control.sum())
    #           control_totals_by_year.loc[control_totals_by_year.geo_id.isin(sch_cpas), 'control'] = \
    #               control_totals_by_year.loc[control_totals_by_year.geo_id.isin(sch_cpas), 'control'] * adjust_sch
    #           adjust_jur = ((.3*jur_percent) / control_adjustments.loc[~control_adjustments.geo_id.isin(sch_cpas)].control.sum())
    #           control_totals_by_year.loc[control_totals_by_year.geo_id.isin(no_sch_cpas), 'control'] = \
    #               control_totals_by_year.loc[control_totals_by_year.geo_id.isin(no_sch_cpas), 'control'] * adjust_jur

    # Check that the target percentages sum to 1 (100%). If not, print statement and cancel the run.
    # This would need to be modified if sub-regional targets were changed away from percentage-based values.
    if round(control_totals_by_year.control.sum()) != 1:
        print("Control percentages for %d do not total 100: Cancelling model run." % year)
        exit()

    # Pull the current year housing targets (current_hh) and the cumulative target for the end of the year (net_hh).
    net_hh = int(hh_df.at[year, 'total_housing_units'])
    current_hh = int(hh_df.at[year, 'housing_units_add'])
    # num_units will have the sum of all units built since the start of the simulation, including the ADU units for the
    # current iteration year. target_units will be the target taking into account the cumulative totals. In essence,
    # this is a check that the year-by-year values match the cumulative totals. target_units should be equal to
    # current_hh less ADU and scheduled development units for the current year.
    num_units = int(hu_forecast_df.loc[hu_forecast_df.year_built > 2016][supply_fname].sum())  # + adu_build_count)
    target_units = int(max(net_hh - num_units, 0))

    # recalculate control percentages based on what is feasible
    #units_available = feasible_parcels_df.groupby(['jur_or_cpa_id'], as_index=False)['remaining_capacity'].sum()
    #units_available['control'] = units_available.remaining_capacity / units_available.remaining_capacity.sum()
    #control_totals_by_year = control_totals_by_year.drop(['control'], 1)
    #control_totals_by_year = pd.merge(control_totals_by_year, units_available[['jur_or_cpa_id', 'control']],
    #                                  how='left', left_on='geo_id', right_on='jur_or_cpa_id')
    # control_totals_by_year.control = control_totals_by_year.control.fillna(0)
    #

    subregional_targets = largest_remainder_allocation(control_totals_by_year, target_units)

    # Run the adu_picker before determining other builds for the year. Doing this first allows for better control of
    # how many ADUs are chosen in each year rather than allowing them to be randomly selected from the pool of
    # feasible parcels.
    adu_builds = adu_picker(year, current_hh, feasible_parcels_df,subregional_targets) # commented out 7/15/19

    # If ADU parcels were chosen, add them to the dataframe of parcel changes. If there are no parcels selected (such
    # as in 2017-2018) simply return the original empty dataframe.
    adu_build_count = len(adu_builds) # commented out 7/15/19
    if adu_build_count > 0: # commented out 7/15/19
        sr14cap = sr14cap.append(adu_builds[['parcel_id', 'capacity_type', 'units_added', 'source']])
        sr14cap.set_index('parcel_id', inplace=True)
        feasible_parcels_df = feasible_parcels_df.loc[~feasible_parcels_df.index.isin(adu_builds.parcel_id.tolist())]



        # Print statements to see the current values of the above numbers.
    print("Number of households: {:,}".format(net_hh))
    print("Number of units: {:,}".format(num_units))
    print("Target of new units = {:,} total".format(current_hh))
    # print("Target of new units = {:,} after scheduled developments and ADUs are built".format(target_units))
    print("{:,} feasible parcels before running developer (excludes sched dev)".format(len(feasible_parcels_df)))
    #if num_units != current_hh:
    #   print(num_units)
    #   print(current_hh)

    # Use the sub-regional percentages and target units to determine integer sub-regional targets by running the
    # largest_remainder_allocation function.

    # If there are no feasible parcels, no building can occur. This is primarily a debugging tool, but it can occur if
    # development is too rapid in early years and the region runs out of capacity. The code will continue to run, but
    # will be unable to build new units.
    if len(feasible_parcels_df) == 0:
        print('WARNING: 0 feasible parcels!')
        return

    # Determine the available capacity for each parcel.
    feasible_parcels_df['remaining_capacity'] = feasible_parcels_df.capacity - feasible_parcels_df.capacity_used
    feasible_parcels_df.remaining_capacity = feasible_parcels_df.remaining_capacity.astype(int)

    # The below section runs the parcel selection and unit building function for each sub-region. For each unique
    # geo_id (1-13, 15-18, and the CPAs for City of San Diego (14##) and the Unincorporated regions (19##)), it
    # performs the following loop. Could be modified to work with other geographies, such as LUZ.
    for jur in control_totals.geo_id.unique().tolist():
        # Pull the appropriate sub-regional target unit value, and the max_units for the sub-region. These values are
        # already iteration year specific (see above).
        if jur==14:
            print(jur)
        subregion_targets = subregional_targets.loc[subregional_targets['geo_id'] == jur].targets.values[0]
        subregion_max = subregional_targets.loc[subregional_targets['geo_id'] == jur].max_units.values[0]
        if jur==1404:
            print(jur)
        # Selects the lower value of subregion_targets and subregion_max, but does not count 'NaN' as the lower value,
        # because the minimum of a number and NaN would be NaN. (Usually subregion_max will be a null value).
        if pd.isnull(subregional_targets.loc[subregional_targets['geo_id'] == jur].target_units.values[0]):
            target_units_for_geo = np.nanmin(np.array([subregion_targets, subregion_max]))
            target_units_for_geo = int(target_units_for_geo)
        else:
            target_units_for_geo = int(subregional_targets.loc[subregional_targets['geo_id'] == jur].target_units.values[0])
        geo_name = str(jur)
        print("Jurisdiction %s target units: %d" % (geo_name, target_units_for_geo))

        # Only use feasible parcels in the current sub-region when selecting parcels for the sub-region.
        parcels_in_geo = feasible_parcels_df.loc[feasible_parcels_df['jurisdiction_id'] == jur].copy()

        # Run the parcel_picker function to select parcels and build units for the sub-region.
        target_units_for_geo = target_units_for_geo - len(adu_builds.loc[adu_builds.jurisdiction_id == jur]) # commented out 7/15/19
        chosen = parcel_picker(parcels_in_geo, target_units_for_geo, geo_name, year)

        # Activates if subregion_max has a numeric value (non-Null). If the subregion_max was built, remove parcels in
        # the subregion from feasibility so they won't be picked at the end during 'regional overflow' (when additional
        # units are selected randomly from the entire region to meet the target_units for the year).
        if not np.isnan(subregion_max):
            # Should find a way to track this in 'regional overflow' if non-0
            subregion_max = subregion_max - len(chosen)
            if subregion_targets >= subregion_max:
                # This removes the jurisdiction if it had a subregional max from feasibility so it won't be picked in
                # the remaining capacity run later.
                feasible_parcels_df = feasible_parcels_df.loc[feasible_parcels_df.jur_or_cpa_id != jur].copy()

        # If parcels were chosen for the sub-region, assign build information to the parcels built. Source 2 is
        # expected stochastic development for a sub-region.
        if len(chosen):
            chosen['source'] = np.where(chosen['capacity_type'] == 'sch', 1, 2)

        # Add the selected parcels and units to the sr14cap dataframe.
        sr14cap = sr14cap.append(chosen)

    # After all sub-regions have been run through the parcel picker, determine if enough units have been built for the
    # year to reach the target. If a region had less capacity than it's sub-regional target, or if a sub-region was
    # limited by a sub-region max_units, then there will be remaining units needed to reach the target.
    if len(sr14cap):
        remaining_units = target_units - int(sr14cap.units_added.sum()) #+ adu_build_count
    else:
        # If no units were built above, the remaining target will be equal to the original target. This should be 0,
        # but might not be if the entire region has run out of capacity.
        remaining_units = target_units

    # If there are additional units needed to fill the target, this loop will select them from the entire region.
    # Every parcel chosen in this loop will be classified as 'regional_overflow'.
    if remaining_units > 0:
        feasible_parcels_df.reset_index(inplace=True)
        sr14cap.reset_index(inplace=True)

        # This updates the feasible_parcel_df with units already built for the year and tracks how many units were added
        # to prevent over-building if a parcel is reselected in this loop. If the parcel had not been selected already,
        # units_added is set to 0.
        feasible_parcels_df = pd.merge(feasible_parcels_df, sr14cap[['parcel_id', 'units_added', 'capacity_type']],
                                       how='left', on=['parcel_id', 'capacity_type'])
        feasible_parcels_df.set_index('parcel_id', inplace=True)
        sr14cap.set_index('parcel_id', inplace=True)
        feasible_parcels_df.units_added = feasible_parcels_df.units_added.fillna(0)

        # Remaining capacity is determined for each parcel before running the regional_overflow portion. 'capacity_used'
        # has not been updated yet, hence subtracting both values from capacity.
        feasible_parcels_df['remaining_capacity'] = (feasible_parcels_df.capacity - feasible_parcels_df.capacity_used
                                                     - feasible_parcels_df.units_added)
        feasible_parcels_df['remaining_capacity'] = feasible_parcels_df['remaining_capacity'].astype(int)

        # Limit available parcels to those with a positive remaining_capacity.
        feasible_parcels_df = feasible_parcels_df.loc[feasible_parcels_df.remaining_capacity > 0].copy()
        feasible_parcels_df['partial_build'] = feasible_parcels_df.units_added
        feasible_parcels_df = feasible_parcels_df.drop(['units_added'], 1)

        # Run the parcel_picker function to select parcels and build units for the regional_overflow. After this runs,
        # the iteration year target_units should be completely built.
        chosen = parcel_picker(feasible_parcels_df, remaining_units, "all", year)

        # If parcels were chosen for the regional_overflow, assign build information to the parcels built. Source 3 is
        # regional_overflow.
        if len(chosen):
            chosen['source'] = 3
        sr14cap = sr14cap.append(chosen)

    # If non-scheduled development units were built, update the hu_forecast.
    if len(sr14cap) > 0:
        # If units were built apply the current iteration year as the year_built for those units.
        if year is not None:
            sr14cap["year_built"] = year

        # Display how many parcels were selected and how many units were added on them (not including sched dev)
        print("Adding {:,} parcels with {:,} {}".format(len(sr14cap), int(sr14cap[supply_fname].sum()), supply_fname))

        # Merge the existing hu_forecast with current-year units built (sr14cap).
        sr14cap.reset_index(inplace=True, drop=False)
        all_hu_forecast = pd.concat([hu_forecast.to_frame(hu_forecast.local_columns),
                                     sr14cap[hu_forecast.local_columns]])  # type: pd.DataFrame
        all_hu_forecast.reset_index(drop=True, inplace=True)
        orca.add_table("hu_forecast", all_hu_forecast)


def summary(year):
    """
    Updates orca tables and tracks other changes, and reports unit totals by type.

    :param year:
        The iteration year of the simulation.
    :return:
        Does not return an object, but updates the parcels dataframe in orca for the next iteration year.
    """

    parcels = orca.get_table('parcels').to_frame()
    hu_forecast = orca.get_table('hu_forecast').to_frame()

    # Selects only parcels that were selected in the current year.
    hu_forecast_year = hu_forecast.loc[(hu_forecast.year_built == year)].copy()

    # Subset scheduled development parcels.
    sched_dev_built = (hu_forecast_year.loc[(hu_forecast_year.source == 1)]).units_added.sum()

    # Subset parcels selected for stochastic development.
    subregional_control_built = (hu_forecast_year.loc[(hu_forecast_year.source == 2)]).units_added.sum()

    # Subset parcels selected in the regional_overflow.
    entire_region_built = (hu_forecast_year.loc[(hu_forecast_year.source == 3)]).units_added.sum()

    # Subset ADU parcels.
    adus_built = (hu_forecast_year.loc[(hu_forecast_year.source == 5)]).units_added.sum()

    # Calculate total units built in the current year.
    all_built = hu_forecast_year.units_added.sum()

    # Print statements to see the current values of the above numbers.
    print(' %d units built as Scheduled Development in %d' % (sched_dev_built, year))
    print(' %d units built as ADU in %d' % (adus_built, year))
    print(' %d units built as Stochastic Units in %d' % (subregional_control_built, year))
    print(' %d units built as Total Remaining in %d' % (entire_region_built, year))
    print(' %d total housing units in %d' % (all_built, year))

    # Combine parcels by capacity type. This is relevant primarily if a parcel was selected both for stochastic
    # development and for regional_overflow needs.
    current_builds = pd.DataFrame({'units_added': hu_forecast_year.
                                  groupby(["parcel_id", "year_built", "capacity_type"]).
                                  units_added.sum()}).reset_index()

    # Update parcel information using the parcel_table_update_units function. While much of the function is accessory
    # and used to track information for the detailed outputs, the returned table needs to be stored in orca so that the
    # next iteration year will pull accurate values for capacity_used on the parcels.
    parcels = parcel_table_update_units(parcels, current_builds)
    orca.add_table("parcels", parcels)
