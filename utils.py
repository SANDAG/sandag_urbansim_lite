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
            d = yaml.load(f, Loader=yaml.FullLoader)[yaml_section]

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
        print('fix this')
        exit()
        # If there is an over-allocation of units, deducts the extra(s) from region cpa=1920 (Valley Center).
        # This is because the Unincorporated County expects to fill up later than other cities.
        # idx = regional_targets.index[regional_targets.geo_id == 1920]
        # results[idx] = results[idx] + remainder
        # print(f'\n\nNegative remainder: {remainder}')
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


def run_scheduled_development(hu_forecast, households, feasibility, reg_controls, parcels, year, compyear):
    """
    Builds the scheduled development parcels.

    :param hu_forecast:
        The dataframe of buildable parcels, used to track new builds during the simulation.
    :param households:
        The dataframe of target units by year.
    :param feasibility:
    :param reg_controls:
    :param parcels:
    :param year:
        The iteration year of the simulation.
    :return:
        Does not return an object, but does update the scheduled_development and hu_forecast tables in orca.
    """

    print(f'\n Adding scheduled developments in year: {year}')

    # Find the target number of units to be built in the current simulation year.
    target_units = int(households.to_frame().at[year, 'housing_units_add'])
    print(f'\n Number of households in year: {target_units}')
    # adu_share_df = orca.get_table('adu_allocation').to_frame()
    # adu_share = int(round(adu_share_df.loc[adu_share_df['yr'] == year].allocation * target_units, 0))
    # target_units = target_units - adu_share

    parcels = parcels.to_frame()
    hu_forecast_df = hu_forecast.to_frame()
    compyear_df = compyear.to_frame()
    control_totals = reg_controls.to_frame()
    control_totals_by_year = control_totals.loc[control_totals.yr == year].copy()
    subregional_targets = largest_remainder_allocation(control_totals_by_year, target_units)

    # Need to fix target units for RHNA
    subregional_targets_scs2 = subregional_targets.copy()

    built_this_yr = hu_forecast_df.loc[hu_forecast.year_built == year].copy()
    built_this_yr['parcel_id'] = built_this_yr.parcel_id.astype(int)
    built_this_yr = pd.merge(built_this_yr, parcels[['parcel_id', 'capacity_type', 'jur_or_cpa_id']], how='left',
                             left_on=['parcel_id', 'capacity_type'], right_on=['parcel_id', 'capacity_type'])
    units_built_matching = built_this_yr.groupby(['jur_or_cpa_id'], as_index=False)['units_added'].sum()

    # Determine if ADUs are expected in the given year, and 'under-produce' scheduled developments to allow for the
    # appropriate number of ADUs without exceeding the target number of units for the year.
    feasible_parcels_df = feasibility.to_frame().copy()

    # Determine the available capacity for each parcel.
    feasible_parcels_df['remaining_capacity'] = feasible_parcels_df.capacity - feasible_parcels_df.capacity_used
    feasible_parcels_df.remaining_capacity = feasible_parcels_df.remaining_capacity.astype(int)
    parcels_sch = feasible_parcels_df.loc[feasible_parcels_df.capacity_type == 'sch'].copy()
    sr14cap = pd.DataFrame()

    #########
    site_parcels = parcels.loc[parcels.site_id.notnull()][['parcel_id', 'site_id']].copy()
    unit_tracking = hu_forecast_df.groupby(['parcel_id']).agg({'units_added': 'sum'}).reset_index()
    site_started = pd.merge(site_parcels, unit_tracking, how='left', on='parcel_id')
    site_status = site_started.groupby(['site_id']).agg({'units_added': 'sum'}).reset_index()
    #########
    req_site_targets = parcels_sch.groupby(['site_id', 'phase_yr', 'jur_or_cpa_id']). \
        agg({'remaining_capacity': 'sum', 'partial_build': 'sum'}).reset_index()
    req_site_targets = pd.merge(req_site_targets, site_status, how='left', on='site_id')
    req_site_targets['partial_build'].where(req_site_targets['units_added'] == 0,
                                            other=req_site_targets['units_added'], inplace=True)
    req_site_targets = req_site_targets.drop(['units_added'], 1)

    req_site_targets = pd.merge(req_site_targets, compyear_df, how='left', on=['site_id'])
    req_site_targets.compyear = req_site_targets.compyear.fillna(2051)
    req_site_targets.startyear = req_site_targets.startyear.fillna(2017)
    req_site_targets['years_left'] = req_site_targets.compyear - year
    #req_site_targets.loc[req_site_targets['years_left'] <= 0, 'years_left'] = 3

    req_site_targets['units_for_year'] = np.ceil(req_site_targets.remaining_capacity / req_site_targets.years_left)
    req_site_targets.loc[req_site_targets['units_for_year'] < 250, 'units_for_year'] = 250
    if 'cap_priority' not in parcels.columns:
        # This if statement removes the 500 unit yearly cap for the SCS forecast
        # This works because the 'cap_priority' column only appears in the scs forecast parcel table
        req_site_targets.loc[req_site_targets['units_for_year'] > 500, 'units_for_year'] = 500
    req_site_targets.loc[req_site_targets['startyear'] > year, 'units_for_year'] = 0

    # We were given specific requests that these sites build out at the following rates
    req_site_targets.loc[req_site_targets['site_id'] == 19002, 'units_for_year'] = 200
    req_site_targets.loc[req_site_targets['site_id'] == 19017, 'units_for_year'] = 65
    req_site_targets.loc[req_site_targets['site_id'] == 19018, 'units_for_year'] = 90

    req_site_targets.loc[req_site_targets['units_for_year'].notnull(), 'units_for_year'] = \
        req_site_targets.loc[:, ['units_for_year', 'remaining_capacity']].min(axis=1)

    # Subset started sites
    previously_picked = req_site_targets.loc[(req_site_targets.partial_build > 0)]
    capacity_site = req_site_targets[~req_site_targets['site_id'].isin(previously_picked.site_id.values.tolist())]

    # Subset sites with upcoming compdates
    upcoming_comp = capacity_site.loc[capacity_site.years_left <= 10]
    capacity_site = capacity_site[~capacity_site['site_id'].isin(upcoming_comp.site_id.values.tolist())]

    manual_sites = capacity_site.loc[capacity_site['site_id'].isin([19002, 19017, 19018])]
    capacity_site = capacity_site[~capacity_site['site_id'].isin(manual_sites.site_id.values.tolist())]

    req_sites = pd.concat([previously_picked, upcoming_comp, manual_sites], sort=True)
    req_sites_cols = req_sites.columns.tolist()

    upcoming_comp['control'] = upcoming_comp.units_for_year / (upcoming_comp.units_for_year.sum())
    new_target = np.nanmin(np.array([target_units, req_sites.units_for_year.sum()]))
    new_target -= (previously_picked.units_for_year.sum() + manual_sites.units_for_year.sum())
    req_sites = largest_remainder_allocation(upcoming_comp, new_target)
    req_sites.units_for_year = req_sites.targets
    req_sites = req_sites[req_sites_cols]
    req_sites = pd.concat([previously_picked, manual_sites, req_sites], sort=True)
    req_sites.rename(columns={"units_for_year": "final_target"}, inplace=True)
    req_sites['unit_gap'] = req_sites.remaining_capacity - req_sites.final_target
    unit_fill = 0
    while True:
        unit_fill += 1
        redistribute_remaining = target_units - req_sites.final_target.sum()
        if req_sites.loc[req_sites.unit_gap == unit_fill].unit_gap.sum() > redistribute_remaining:
            break
        else:
            req_sites.loc[req_sites.unit_gap == unit_fill, 'final_target'] += unit_fill

        if unit_fill > 20:
            break

    if req_sites.final_target.sum() > target_units:
        print(f'Error: Required sched_dev ({req_sites.final_target.sum()}) > target ({target_units})!')

    new_geo_targets = req_sites.groupby(['jur_or_cpa_id']).agg({'final_target': 'sum'}).reset_index()
    subregional_targets_cols = subregional_targets.columns.tolist()
    subregional_targets = pd.merge(subregional_targets, new_geo_targets, how='left', left_on='geo_id', right_on='jur_or_cpa_id')
    subregional_targets.target_units = subregional_targets.final_target

    subregional_targets_upd = largest_remainder_allocation(control_totals_by_year, redistribute_remaining)
    subregional_targets_upd.rename(columns={"targets": "redistribute"}, inplace=True)
    subregional_targets = pd.merge(subregional_targets, subregional_targets_upd[['geo_id', 'redistribute']], how='left', on='geo_id')
    subregional_targets.target_units = subregional_targets.target_units + subregional_targets.redistribute
    subregional_targets.target_units = (subregional_targets.target_units.fillna(0)).astype(int)
    subregional_targets = subregional_targets[subregional_targets_cols]

    # Need to fix target units for RHNA
    subregional_targets = subregional_targets_scs2.copy()

    for jur in control_totals.geo_id.unique().tolist():
        jur_parcels_sch = parcels_sch.loc[parcels_sch.jur_or_cpa_id == jur].copy()
        # Pull the appropriate sub-regional target unit value, and the max_units for the sub-region. These values are
        # already iteration year specific (see above).
        subregion_targets = subregional_targets.loc[subregional_targets['geo_id'] == jur].targets.values[0]
        subregion_max = subregional_targets.loc[subregional_targets['geo_id'] == jur].max_units.values[0]

        if len(units_built_matching) > 0:
            if jur in units_built_matching.jur_or_cpa_id.tolist():
                units_built_already = units_built_matching.loc[units_built_matching.jur_or_cpa_id ==
                                                               jur].units_added.values[0]
            else:
                units_built_already = 0
        else:
            units_built_already = 0
        # Selects the lower value of subregion_targets and subregion_max, but does not count 'NaN' as the lower value,
        # because the minimum of a number and NaN would be NaN. (Usually subregion_max will be a null value).
        if pd.isnull(subregional_targets.loc[subregional_targets['geo_id'] == jur].target_units.values[0]):
            target_units_for_geo = np.nanmin(np.array([subregion_targets, subregion_max]))
            target_units_for_geo = target_units_for_geo - units_built_already
            target_units_for_geo = int(target_units_for_geo)
        else:
            target_units_for_geo = int(subregional_targets.loc[subregional_targets['geo_id'] == jur].
                                       target_units.values[0])
            target_units_for_geo = target_units_for_geo - units_built_already
            target_units_for_geo = int(target_units_for_geo)

        if (len(jur_parcels_sch) > 0) & (year != 2017):
            shuffled_parcels = jur_parcels_sch.sample(frac=1, random_state=1).reset_index(drop=False)
            capacity_sch = shuffled_parcels.copy()  # keep for consistency# type: pd.DataFrame
            capacity_site = capacity_sch.groupby(['site_id', 'phase_yr', 'jur_or_cpa_id']).\
                agg({'remaining_capacity': 'sum', 'partial_build': 'sum'}).reset_index()

            capacity_site = pd.merge(capacity_site, compyear_df, how='left', on=['site_id'])
            capacity_site.compyear = capacity_site.compyear.fillna(2051)
            capacity_site['years_left'] = capacity_site.compyear - year
            capacity_site.loc[capacity_site['years_left'] <= 0, 'years_left'] = 3

            capacity_site['units_for_year'] = np.ceil(capacity_site.remaining_capacity / capacity_site.years_left).astype(int)
            capacity_site.loc[capacity_site['units_for_year'] < 250, 'units_for_year'] = 250

            # We were given specific requests that these sites build out at the following rates
            capacity_site.loc[capacity_site['site_id'] == 19002, 'units_for_year'] = 200
            capacity_site.loc[capacity_site['site_id'] == 19017, 'units_for_year'] = 65
            capacity_site.loc[capacity_site['site_id'] == 19018, 'units_for_year'] = 90

            capacity_site.loc[capacity_site['units_for_year'].notnull(), 'units_for_year'] = \
                capacity_site.loc[:, ['units_for_year', 'remaining_capacity']].min(axis=1)

            capacity_site = capacity_site.sample(frac=1, random_state=1).reset_index(drop=False)
            capacity_site.sort_values(by=['partial_build'], ascending=[False],
                                      inplace=True)

            # Subset started sites
            previously_picked = capacity_site.loc[(capacity_site.partial_build > 0)]
            capacity_site = capacity_site[~capacity_site['site_id'].isin(previously_picked.site_id.values.tolist())]

            # Subset sites with upcoming compdates
            upcoming_comp = capacity_site.loc[capacity_site.years_left <= 10]
            capacity_site = capacity_site[~capacity_site['site_id'].isin(upcoming_comp.site_id.values.tolist())]

            manual_sites = capacity_site.loc[capacity_site['site_id'].isin([19002, 19017, 19018])]
            capacity_site = capacity_site[~capacity_site['site_id'].isin([19002, 19017, 19018])]

            # req_units = (previously_picked.units_for_year.sum() + upcoming_comp.units_for_year.sum() +
            #              manual_sites.units_for_year.sum())
            # target_units_for_geo = np.nanmax(np.array([req_units, target_units_for_geo]))

            priority_then_random = pd.concat([previously_picked, manual_sites, upcoming_comp, capacity_site], sort=True)
            priority_then_random = pd.merge(priority_then_random, req_sites[['site_id', 'final_target']],
                                            how='left', on='site_id')
            priority_then_random['units_for_year'].where(priority_then_random.final_target.isnull(),
                                                         other=priority_then_random['final_target'], inplace=True)

            one_row_per_unit = priority_then_random.reindex(priority_then_random.index.repeat(
                priority_then_random.units_for_year)).reset_index(drop=True)
            one_row_per_unit_picked = one_row_per_unit.head(target_units_for_geo)
            sites_picked = pd.DataFrame({'units_added': one_row_per_unit_picked.groupby(['site_id']).
                                        size()}).reset_index()
            new_sch = capacity_sch[['parcel_id', 'site_id', 'remaining_capacity']].sample(frac=1, random_state=1). \
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
            # shuffled_parcels = shuffled_parcels.drop(['units_added'], axis=1)
            sch_picked['capacity_type'] = 'sch'
        else:
            sch_picked = pd.DataFrame(columns=['parcel_id', 'capacity_type', 'units_added'])

        sr14cap = sr14cap.append(sch_picked[['parcel_id', 'capacity_type', 'units_added']], sort=True)

    print(f'Sched Dev units: {sr14cap.units_added.sum()}')

    if len(sr14cap) > 0:
        sr14cap['year_built'] = year
        sr14cap['source'] = 1
        # Adds the new scheduled_developments to the hu_forecast table.
        sim_builds = hu_forecast.to_frame(hu_forecast.local_columns)
        sim_units = pd.concat([sim_builds, sr14cap[sim_builds.columns]], sort=True)  # type: pd.DataFrame
        sim_units.reset_index(drop=True, inplace=True)
        sim_units.source = sim_units.source.astype(int)
        orca.add_table("hu_forecast", sim_units)


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
            parcels_to_reduce = pd.concat([reduce_first_parcels, random_neg_parcels], sort=True)  # type: pd.DataFrame

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
                                         parcels_reduced[hu_forecast.local_columns]], sort=True)  # type: pd.DataFrame
            all_hu_forecast.reset_index(drop=True, inplace=True)
            orca.add_table("hu_forecast", all_hu_forecast)


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


    if 'cap_priority' in parcels.columns:
        # This if statement only applies during the SCS Forecast
        # This works because the 'cap_priority' column only appears in the scs forecast parcel table
        compyear = orca.get_table('compyear').to_frame()
        compyear.reset_index(inplace=True, drop=True)
        parcels = pd.merge(parcels, compyear, how='left', on=['site_id'])
        parcels['phase_yr'].where(parcels.site_id.isnull(), other=parcels['startyear'], inplace=True)

    parcels.set_index('parcel_id', inplace=True)
    # Select parcels that have more capacity than is used.
    # Note: 'capacity' is not subtracted from the built parcels, so 'capacity' should always be >= 'capacity_used'.
    feasible_parcels = parcels.loc[parcels['capacity'] > parcels['capacity_used']].copy()
    # feasible_parcels.loc[feasible_parcels.capacity_type.isin(['cc','mc','tc','tco','uc']), 'phase_yr'] = 2051
    feasible_parcels.phase_yr = feasible_parcels.phase_yr.fillna(2017)
    # Restrict feasible parcels based on assigned phase years (not feasible before phase year occurs).
    feasible_parcels = feasible_parcels.loc[feasible_parcels['phase_yr'] <= year].copy()
    orca.add_table("feasibility", feasible_parcels)

    print(f'Feasible Units: {feasible_parcels.capacity.sum() - feasible_parcels.capacity_used.sum()}')


def adu_picker(feasible_parcels_df, adu_share):
    """
    Selects additional dwelling unit parcels to build each year (1 additional unit on an existing residential parcel).

    :param feasible_parcels_df:
        The dataframe generated in feasibility (contains parcels that are available to build on).
    :param adu_share:
        The number of ADU units to be built
    :return:
        dataframe: the selected ADU table.
    """

    # This function has been changed dramatically. Where it originally did much of the ADU calculations and some other
    # processing, we now simply pass the feasible parcels (and draw out those that are ADUs) and find the target
    # number needed. This function assumes we want a specific ADU buildout like we had in datasource_id = 17.

    # The newest version as of July 2019 will simply add ADUs to feasible parcels and remove them before remaining
    # units are allocated in the second pass.

    # Only choose from feasible parcels with ADU capacity_type.
    adu_parcels = feasible_parcels_df.loc[(feasible_parcels_df.capacity_type == 'adu')].copy()

    try:
        shuffled_adu = adu_parcels.sample(frac=1, random_state=50).reset_index(drop=False)
    except ValueError:
        shuffled_adu = adu_parcels
    picked_adu_parcels = shuffled_adu.head(adu_share).copy()

    # Assigns build information to the parcels built. Source 5 is ADU.
    picked_adu_parcels['source'] = 5
    picked_adu_parcels['units_added'] = 1
    return picked_adu_parcels


def parcel_picker(parcels_to_choose, target_number_of_units, name_of_geo, year_simulation, rem):
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
    :param rem:
        0 for first pass of parcel picking, 1 during remainder loop(s); used for adu allocation.
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
            print(f"WARNING: NOT ENOUGH UNITS TO MATCH DEMAND FOR {name_of_geo} IN YEAR {year_simulation}")

            # This checks if there are any units available to build. If so, they are all used up. If not, the function
            # ends and returns an empty dataframe.
            if len(parcels_to_choose):
                parcels_picked = parcels_to_choose
                parcels_picked['units_added'] = parcels_picked['remaining_capacity']
                parcels_picked.drop(['site_id', 'remaining_capacity'], axis=1, inplace=True)
        else:
            years_left = 2051 - year_simulation
            # Randomize the order of available parcels
            shuffled_parcels = parcels_to_choose.sample(frac=1, random_state=50).reset_index(drop=False)

            if 'cap_priority' in shuffled_parcels.columns:
                scs_p1_parcels = shuffled_parcels.loc[(shuffled_parcels.cap_priority == 1)]
                scs_p2_parcels = shuffled_parcels.loc[(shuffled_parcels.cap_priority == 2)]
                scs_p3_parcels = shuffled_parcels.loc[(shuffled_parcels.cap_priority == 3)]
                shuffled_parcels = pd.concat([scs_p1_parcels, scs_p2_parcels, scs_p3_parcels], sort=True)

            # Subset parcels that are partially completed from the year before
            previously_picked = shuffled_parcels.loc[(shuffled_parcels.partial_build > 0) &
                                                     (shuffled_parcels.capacity_type != 'sch')]
            shuffled_parcels = shuffled_parcels[
                ~shuffled_parcels['parcel_id'].isin(previously_picked.parcel_id.values.tolist())]

            # # Subset SCH parcels
            sch_parcels = shuffled_parcels.loc[shuffled_parcels.capacity_type == 'sch']
            shuffled_parcels = shuffled_parcels[
                ~shuffled_parcels['parcel_id'].isin(sch_parcels.parcel_id.values.tolist())]

            if rem > 0:
                # # Subset ADU parcels
                adu_parcels = shuffled_parcels.loc[shuffled_parcels.capacity_type == 'adu']
                shuffled_parcels = shuffled_parcels[
                    ~shuffled_parcels['parcel_id'].isin(adu_parcels.parcel_id.values.tolist())]

                if rem == 1:
                    # This places the ADU parcels after the prioritized parcels.
                    priority_then_random = pd.concat([previously_picked, sch_parcels, shuffled_parcels, adu_parcels]
                                                     , sort=True)

                else:
                    priority_then_random = pd.concat([sch_parcels, previously_picked, shuffled_parcels], sort=True)

            else:
                if year_simulation < 2036:
                    priority_then_random = pd.concat([previously_picked, shuffled_parcels, sch_parcels], sort=True)
                else:
                    priority_then_random = pd.concat([previously_picked, sch_parcels, shuffled_parcels], sort=True)

            # This section prohibits building very large projects in one year. If a parcel has over 250 or 500
            # available capacity, is capped at 250 or 500, respectively, units when selected. This generally assumes
            # that larger projects can build faster than smaller ones, but prevents them from building instantaneously.
            priority_then_random['units_for_year'] = np.ceil(priority_then_random.remaining_capacity / years_left).\
                astype(int)
            # priority_then_random.loc[priority_then_random['units_for_year'] < 250, 'units_for_year'] = 250
            # priority_then_random.loc[priority_then_random['units_for_year'].notnull(), 'units_for_year'] = \
            #    priority_then_random.loc[:, ['units_for_year', 'remaining_capacity']].min(axis=1)

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

            # parcels_picked = pd.concat([parcels_picked, sch_picked], sort=True)

            parcels_picked.set_index('parcel_id', inplace=True)
            if original_target != parcels_picked['units_added'].sum():
                print(f"{parcels_picked['units_added'].sum()} UNITS PRODUCED INSTEAD OF TARGET {original_target}!")
    return parcels_picked


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
    feasible_parcels_df = feasibility.to_frame().copy()
    hu_forecast_df = hu_forecast.to_frame()
    parcels = orca.get_table('parcels').to_frame()

    # Creates empty dataframe to track added parcels
    sr14cap = pd.DataFrame(columns=['source', 'units_added', 'capacity_type', 'year_built'])

    # Pull out the control totals for only the current iteration year
    control_totals_by_year = control_totals.loc[control_totals.yr == year].copy()

    # Check that the target percentages sum to 1 (100%). If not, print statement and cancel the run.
    # This would need to be modified if sub-regional targets were changed away from percentage-based values.
    if round(control_totals_by_year.control.sum(), 1) != 1.0:
        print(f"Control percentages for {year} do not total 100: Cancelling model run.")
        exit()

    # Pull the current year housing targets (current_hh) and the cumulative target for the end of the year (net_hh).
    net_hh = int(hh_df.at[year, 'total_housing_units'])
    current_hh = int(hh_df.at[year, 'housing_units_add'])

    units_built_sum = hu_forecast_df.loc[hu_forecast_df.year_built == year].units_added.sum()

    # subregional_targets = largest_remainder_allocation(control_totals_by_year, target_without_adu)
    rem_current_hh = current_hh - units_built_sum
    subregional_targets1 = largest_remainder_allocation(control_totals_by_year, rem_current_hh)

    built_this_yr = hu_forecast_df.loc[hu_forecast.year_built == year].copy()
    built_this_yr['parcel_id'] = built_this_yr.parcel_id.astype(int)

    if year == 2017:
        # Recalibrate 1st year targets
        control_totals_by_year['jur_id'] = control_totals_by_year.geo_id
        control_totals_by_year['jur_id'].where(control_totals_by_year.geo_id < 20, other=14, inplace=True)
        control_totals_by_year['jur_id'].where(control_totals_by_year.geo_id < 1900, other=19, inplace=True)
        control_sum_df = control_totals_by_year.groupby(['jur_id'], as_index=False)['target_units'].sum()

        built_this_yr2 = pd.merge(built_this_yr,
                                  parcels[['parcel_id', 'capacity_type', 'jur_or_cpa_id', 'cap_jurisdiction_id']],
                                  how='left',
                                  on=['parcel_id', 'capacity_type'])
        jur_units_sch = built_this_yr2.groupby(['cap_jurisdiction_id'], as_index=False)['units_added'].sum()

        control_sum_df = pd.merge(control_sum_df, jur_units_sch, how='left',
                                  left_on='jur_id', right_on='cap_jurisdiction_id')
        control_sum_df.units_added.fillna(0, inplace=True)
        control_sum_df['target_units'] = control_sum_df.target_units - control_sum_df.units_added
        control_totals_by_year = control_totals_by_year.drop(['target_units', 'targets'], 1)
        new_controls_by_year = pd.merge(control_totals_by_year, control_sum_df[['jur_id', 'target_units']], how='left',
                                        on='jur_id')

        for jur in [14, 19]:
            control_adjustments = new_controls_by_year.loc[new_controls_by_year.jur_id == jur].copy()
            adjust = (1 / control_adjustments.control.sum())
            control_adjustments['control'] = control_adjustments.control * adjust
            try:
                target_units = int(control_adjustments.target_units.values[0])
            except ValueError:
                break
            cpa_targets = largest_remainder_allocation(control_adjustments, target_units)
            cpa_targets = cpa_targets[['yr', 'geo_id', 'targets']]
            new_controls_by_year = pd.merge(new_controls_by_year, cpa_targets, how='left', on=['yr', 'geo_id'])
            new_controls_by_year['target_units'].where(new_controls_by_year.targets.isnull(),
                                                       other=new_controls_by_year['targets'], inplace=True)
            new_controls_by_year = new_controls_by_year.drop('targets', axis=1)

        subregional_targets1 = subregional_targets1.drop(['target_units', 'jur_id'], 1)
        subregional_targets1 = pd.merge(subregional_targets1, new_controls_by_year[['geo_id', 'target_units']],
                                       how='left', on='geo_id')

    # num_units will have the sum of all units built since the start of the simulation, including the ADU units for the
    # current iteration year. target_units will be the target taking into account the cumulative totals. In essence,
    # this is a check that the year-by-year values match the cumulative totals. target_units should be equal to
    # current_hh less ADU and scheduled development units for the current year.

    num_units = int(hu_forecast_df.loc[(hu_forecast_df.year_built > 2016) &
                                       (hu_forecast_df.year_built <= year)][supply_fname].sum())
    target_units = int(max(net_hh - num_units, 0))
    # # target_without_adu = int(max(net_hh - num_units - adu_unit_count, 0))
    built_this_yr.set_index('parcel_id', inplace=True)
    built_this_yr2 = pd.concat([built_this_yr, sr14cap], sort=True)
    built_this_yr2.reset_index(inplace=True)
    built_this_yr2.rename(columns={"index": "parcel_id"}, inplace=True)
    built_this_yr2 = pd.merge(built_this_yr2, parcels[['parcel_id', 'capacity_type', 'jur_or_cpa_id']], how='left',
                              left_on=['parcel_id', 'capacity_type'], right_on=['parcel_id', 'capacity_type'])
    # units_built_sch_adu = built_this_yr2.groupby(['jur_or_cpa_id'], as_index=False)['units_added'].sum()

    # This updates the feasible_parcel_df with units already built for the year and tracks how many units were added
    # to prevent over-building if a parcel is reselected in this loop. If the parcel had not been selected already,
    # units_added is set to 0.
    feasible_parcels_df.reset_index(inplace=True)
    feasible_parcels_df = pd.merge(feasible_parcels_df, built_this_yr2[['parcel_id', 'units_added', 'capacity_type']],
                                   how='left', on=['parcel_id', 'capacity_type'])
    feasible_parcels_df.set_index('parcel_id', inplace=True)
    feasible_parcels_df.units_added = feasible_parcels_df.units_added.fillna(0)

    subregional_targets = largest_remainder_allocation(control_totals_by_year, current_hh)
    subregional_added = built_this_yr2.groupby(['jur_or_cpa_id'], as_index=False)['units_added'].sum()
    subregional_targets = pd.merge(subregional_targets, subregional_added, how='left', left_on='geo_id',
                                   right_on='jur_or_cpa_id')
    subregional_targets.units_added.fillna(0, inplace=True)
    subregional_targets.targets = (subregional_targets.targets - subregional_targets.units_added)
    subregional_targets = subregional_targets.drop(['jur_or_cpa_id'], 1)

    print(f"Number of households: {net_hh:,}")
    print(f"Number of units: {num_units:,}")
    print(f"Target of new units = {current_hh:,} total")
    print(f"{len(feasible_parcels_df):,} feasible parcels before running developer (excludes sched dev)")

    # Use the sub-regional percentages and target units to determine integer sub-regional targets by running the
    # largest_remainder_allocation function.

    # If there are no feasible parcels, no building can occur. This is primarily a debugging tool, but it can occur if
    # development is too rapid in early years and the region runs out of capacity. The code will continue to run, but
    # will be unable to build new units.
    if len(feasible_parcels_df) == 0:
        print('WARNING: 0 feasible parcels!')
        return

    # Determine the available capacity for each parcel.
    feasible_parcels_df['remaining_capacity'] = (feasible_parcels_df.capacity - feasible_parcels_df.capacity_used -
                                                 feasible_parcels_df.units_added)
    feasible_parcels_df.remaining_capacity = feasible_parcels_df.remaining_capacity.astype(int)
    feasible_parcels_df = feasible_parcels_df.drop(['units_added'], 1)

    # The below section runs the parcel selection and unit building function for each sub-region. For each unique
    # geo_id (1-13, 15-18, and the CPAs for City of San Diego (14##) and the Unincorporated regions (19##)), it
    # performs the following loop. Could be modified to work with other geographies, such as LUZ.
    for jur in control_totals.geo_id.unique().tolist():
        # Pull the appropriate sub-regional target unit value, and the max_units for the sub-region. These values are
        # already iteration year specific (see above).
        subregion_targets = subregional_targets.loc[subregional_targets['geo_id'] == jur].targets.values[0]
        subregion_max = subregional_targets.loc[subregional_targets['geo_id'] == jur].max_units.values[0]
        subregion_sch = subregional_targets.loc[subregional_targets['geo_id'] == jur].units_added.values[0]
        subregion_sch = int(subregion_sch)
        # if len(units_built_sch_adu) > 0:
        #     if jur in units_built_sch_adu.jur_or_cpa_id.tolist():
        #         units_built_already = units_built_sch_adu.loc[units_built_sch_adu.jur_or_cpa_id ==
        #                                                       jur].units_added.values[0]
        #     else: units_built_already = 0
        # else: units_built_already = 0
        # Selects the lower value of subregion_targets and subregion_max, but does not count 'NaN' as the lower value,
        # because the minimum of a number and NaN would be NaN. (Usually subregion_max will be a null value).
        if pd.isnull(subregional_targets.loc[subregional_targets['geo_id'] == jur].target_units.values[0]):
            target_units_for_geo = np.nanmin(np.array([subregion_targets, subregion_max]))
            # target_units_for_geo = target_units_for_geo - units_built_already
            target_units_for_geo = int(target_units_for_geo)
        else:
            target_units_for_geo = int(subregional_targets.loc[subregional_targets['geo_id'] == jur].
                                       target_units.values[0])
            # target_units_for_geo = target_units_for_geo - units_built_already
            target_units_for_geo = int(target_units_for_geo)

        geo_total = target_units_for_geo + subregion_sch
        geo_name = str(jur)
        print(f"Jurisdiction {geo_name} target units: {target_units_for_geo} jur + {subregion_sch} sch = {geo_total}")
        # num_units_already = int(hu_forecast_df.loc[((hu_forecast_df.year_built ==year) & ())][supply_fname].sum())
        # Only use feasible parcels in the current sub-region when selecting parcels for the sub-region.
        parcels_in_geo = feasible_parcels_df.loc[feasible_parcels_df['jur_or_cpa_id'] == jur].copy()

        # Run the parcel_picker function to select parcels and build units for the sub-region.
        # target_units_for_geo = int(target_units_for_geo - len(adu_builds.loc[adu_builds.jur_or_cpa_id == jur]))

        if 'cap_priority' not in parcels.columns:
            parcels_in_geo = parcels_in_geo.loc[parcels_in_geo['capacity_type'].isin(['jur', 'adu'])].copy()
        chosen = parcel_picker(parcels_in_geo, target_units_for_geo, geo_name, year, 0)

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
            chosen['source'] = np.where(chosen['capacity_type'] == 'adu', 5, chosen['source'])
            # Add the selected parcels and units to the sr14cap dataframe.
            sr14cap = sr14cap.append(chosen[['capacity_type', 'units_added', 'source']], sort=True)
            if 'cap_priority' not in parcels.columns:
                # This allows the parcel removal to only activate in non-scs scenarios
                if year < 2040:
                    feasible_parcels_df = feasible_parcels_df.loc[~feasible_parcels_df.index.isin(
                        chosen.index.tolist())]

    # After all sub-regions have been run through the parcel picker, determine if enough units have been built for the
    # year to reach the target. If a region had less capacity than it's sub-regional target, or if a sub-region was
    # limited by a sub-region max_units, then there will be remaining units needed to reach the target.
    if len(sr14cap):
        remaining_units = target_units - int(sr14cap.units_added.sum())
    else:
        # If no units were built above, the remaining target will be equal to the original target. This should be 0,
        # but might not be if the entire region has run out of capacity.
        remaining_units = target_units

    # If there are additional units needed to fill the target, this loop will select them from the entire region.
    # Every parcel chosen in this loop will be classified as 'regional_overflow'.
    if remaining_units > 0:
        print(f'Remaining units: {remaining_units}')
        feasible_parcels_df.reset_index(inplace=True)
        sr14cap.reset_index(inplace=True)
        sr14cap.rename(columns={"index": "parcel_id"}, inplace=True)

        # This updates the feasible_parcel_df with units already built for the year and tracks how many units were added
        # to prevent over-building if a parcel is reselected in this loop. If the parcel had not been selected already,
        # units_added is set to 0.
        feasible_parcels_df = pd.merge(feasible_parcels_df, sr14cap[['parcel_id', 'units_added', 'capacity_type']],
                                       how='left', on=['parcel_id', 'capacity_type'])
        feasible_parcels_df.set_index('parcel_id', inplace=True)
        sr14cap.set_index('parcel_id', inplace=True)
        feasible_parcels_df.units_added = feasible_parcels_df.units_added.fillna(0)
        # feasible_parcels_df['units_added'] = 0

        # Remaining capacity is determined for each parcel before running the regional_overflow portion. 'capacity_used'
        # has not been updated yet, hence subtracting both values from capacity.
        feasible_parcels_df['remaining_capacity'] = (feasible_parcels_df.remaining_capacity -
                                                     feasible_parcels_df.units_added)
        feasible_parcels_df['remaining_capacity'] = feasible_parcels_df['remaining_capacity'].astype(int)

        # Limit available parcels to those with a positive remaining_capacity.
        feasible_parcels_df = feasible_parcels_df.loc[feasible_parcels_df.remaining_capacity > 0].copy()
        feasible_parcels_df['partial_build'] = feasible_parcels_df.units_added
        feasible_parcels_df = feasible_parcels_df.drop(['units_added'], 1)
        # if year < 2036:
        #     feasible_parcels_df = feasible_parcels_df.loc[feasible_parcels_df.capacity_type != 'sch'].copy()

        # Run the parcel_picker function to select parcels and build units for the regional_overflow. After this runs,
        # the iteration year target_units should be completely built.
        slim_df = feasible_parcels_df[['cap_jurisdiction_id', 'remaining_capacity', 'capacity_type',
                                       'jur_or_cpa_id']].copy()
        slim_df.rename(columns={"cap_jurisdiction_id": "jur_id", "remaining_capacity": "cap", "jur_or_cpa_id": "jcpa"},
                       inplace=True)
        adjust_df = slim_df.copy()
        # slim_df.replace(['cc', 'mc', 'tc', 'tco', 'uc'], 'sgoa', inplace=True)
        # if (year > 2040) & (slim_df.loc[slim_df.capacity_type == 'sch'].cap.sum() > remaining_units):
        #     adjust_df = slim_df.loc[slim_df.capacity_type == 'sch'].copy()
        # else:
        #     adjust_df = slim_df.loc[slim_df.capacity_type.isin(['jur', 'sch'])].copy()
        # if adjust_df.cap.sum() < remaining_units:
        #     chosen = feasible_parcels_df.loc[feasible_parcels_df.capacity_type.isin(['jur'])][['capacity_type',
        #                                                                                        'remaining_capacity']]
        #     if len(chosen):
        #         chosen.rename(columns={"remaining_capacity": "units_added"}, inplace=True)
        #         remaining_units = int(remaining_units - chosen.units_added.sum())
        #         chosen['source'] = 3
        #
        #         # Add the selected parcels and units to the sr14cap dataframe.
        #         sr14cap = sr14cap.append(chosen[['capacity_type', 'units_added', 'source']], sort=True)
        #         feasible_parcels_df = feasible_parcels_df.loc[~feasible_parcels_df.index.isin(chosen.index.tolist())]
        #
        #     adjust_df = slim_df.copy()
        #
        # if year > 2035:
        #     if year > 2045:
        #         # if year == 2050:
        #         #     adjust_df['cap'].where(adjust_df.capacity_type == 'sch', other=0, inplace=True)
        #         # else:
        #         adjust_df['cap'].where(adjust_df.capacity_type == 'jur', other=adjust_df['cap'] * 3, inplace=True)
        #         adjust_df['cap'].where(adjust_df.capacity_type == 'sch', other=adjust_df['cap'] * 0.1, inplace=True)
        #     else:
        #         adjust_df['cap'].where(adjust_df.capacity_type == 'jur', other=adjust_df['cap'] * 1.5, inplace=True)
        #         adjust_df['cap'].where(adjust_df.capacity_type == 'sch', other=adjust_df['cap'] * 0.5, inplace=True)
        # else:
        #     adjust_df['cap'].where(adjust_df.capacity_type == 'jur', other=adjust_df['cap'] * 1.1, inplace=True)

        units_available = adjust_df.groupby(['jcpa'], as_index=False)['cap'].sum()
        remaining_cap = units_available.cap.sum()
        units_available['remaining_control'] = units_available.cap / remaining_cap
        control_adjustments = pd.merge(control_totals_by_year, units_available, how='left', left_on=['geo_id'],
                                       right_on=['jcpa'])
        control_adjustments['control'].where(control_adjustments.jcpa.notnull(), other=0, inplace=True)
        control_adjustments['control'].where(control_adjustments.jcpa.isnull(),
                                             other=control_adjustments['remaining_control'], inplace=True)
        control_adjustments.drop(['targets', 'jcpa', 'cap', 'remaining_control'], axis=1, inplace=True)
        remaining_targets = largest_remainder_allocation(control_adjustments, remaining_units)

        for jur in control_totals.geo_id.unique().tolist():
            # Pull the appropriate sub-regional target unit value, and the max_units for the sub-region. These values
            # are already iteration year specific (see above).
            subregion_targets = remaining_targets.loc[remaining_targets['geo_id'] == jur].targets.values[0]
            subregion_max = remaining_targets.loc[remaining_targets['geo_id'] == jur].max_units.values[0]

            # Selects the lower value of subregion_targets and subregion_max, but does not count 'NaN' as the lower
            # value, because the minimum of a number and NaN would be NaN. (Usually subregion_max will be a null value).
            if pd.isnull(remaining_targets.loc[remaining_targets['geo_id'] == jur].target_units.values[0]):
                target_units_for_geo = np.nanmin(np.array([subregion_targets, subregion_max]))
                target_units_for_geo = int(target_units_for_geo)
            else:
                target_units_for_geo = int(
                    remaining_targets.loc[remaining_targets['geo_id'] == jur].targets.values[0])
            geo_name = str(jur)
            print(f"Jurisdiction {geo_name} target units: {target_units_for_geo}")

            # Only use feasible parcels in the current sub-region when selecting parcels for the sub-region.
            parcels_in_geo = feasible_parcels_df.loc[feasible_parcels_df['jur_or_cpa_id'] == jur].copy()

            # if year == 2050:
            #     parcels_in_geo = parcels_in_geo.loc[parcels_in_geo['capacity_type'] == 'sch']

            # Run the parcel_picker function to select parcels and build units for the sub-region.
            chosen = parcel_picker(parcels_in_geo, target_units_for_geo, geo_name, year, 1)

            # Activates if subregion_max has a numeric value (non-Null). If the subregion_max was built, remove parcels
            # in the subregion from feasibility so they won't be picked at the end during 'regional overflow' (when
            # additional units are selected randomly from the entire region to meet the target_units for the year).
            if not np.isnan(subregion_max):
                # Should find a way to track this in 'regional overflow' if non-0
                subregion_max = subregion_max - len(chosen)
                if subregion_targets >= subregion_max:
                    # This removes the jurisdiction if it had a subregional max from feasibility so it won't be picked
                    # in the remaining capacity run later.
                    feasible_parcels_df = feasible_parcels_df.loc[feasible_parcels_df.jur_or_cpa_id != jur].copy()

            # If parcels were chosen for the sub-region, assign build information to the parcels built. Source 2 is
            # expected stochastic development for a sub-region.
            if len(chosen):
                remaining_units = int(remaining_units - chosen.units_added.sum())
                chosen['source'] = 3
                # print(chosen)
                # Add the selected parcels and units to the sr14cap dataframe.
                sr14cap = sr14cap.append(chosen[['capacity_type', 'units_added', 'source']], sort=True)
                feasible_parcels_df = feasible_parcels_df.loc[~feasible_parcels_df.index.isin(chosen.index.tolist())]

        if remaining_units > 0:
            print(f'Remaining units: {remaining_units}')
            feasible_parcels_df.reset_index(inplace=True)
            sr14cap.reset_index(inplace=True)
            feasible_parcels_df = pd.merge(feasible_parcels_df, sr14cap[['parcel_id', 'units_added', 'capacity_type']],
                                           how='left', on=['parcel_id', 'capacity_type'])
            feasible_parcels_df.set_index('parcel_id', inplace=True)
            sr14cap.set_index('parcel_id', inplace=True)
            feasible_parcels_df.units_added = feasible_parcels_df.units_added.fillna(0)
            feasible_parcels_df['remaining_capacity'] = (feasible_parcels_df.remaining_capacity -
                                                         feasible_parcels_df.units_added)
            feasible_parcels_df['remaining_capacity'] = feasible_parcels_df['remaining_capacity'].astype(int)
            feasible_parcels_df = feasible_parcels_df.loc[feasible_parcels_df.remaining_capacity > 0].copy()
            feasible_parcels_df['partial_build'].where(feasible_parcels_df['units_added'] == 0,
                                                       other=feasible_parcels_df['units_added'], inplace=True)
            feasible_parcels_df = feasible_parcels_df.drop(['units_added'], 1)
            feasible_parcels_df = feasible_parcels_df.loc[~feasible_parcels_df.index.isin(sr14cap.index.tolist())]
            chosen = parcel_picker(feasible_parcels_df, remaining_units, "all", year, 2)
            print(f'!{chosen.units_added.sum()} were picked in the second round of cleanup!')
            if len(chosen):
                chosen['source'] = 3
                # Add the selected parcels and units to the sr14cap dataframe.
                sr14cap = sr14cap.append(chosen[['capacity_type', 'units_added', 'source']], sort=True)

    # If non-scheduled development units were built, update the hu_forecast.
    if len(sr14cap) > 0:
        # If units were built apply the current iteration year as the year_built for those units.
        if year is not None:
            sr14cap["year_built"] = year

        # Display how many parcels were selected and how many units were added on them (not including sched dev)
        print(f"Adding {len(sr14cap):,} parcels with {int(sr14cap[supply_fname].sum()):,} {supply_fname}")

        # Merge the existing hu_forecast with current-year units built (sr14cap).
        sr14cap.reset_index(inplace=True, drop=False)
        sr14cap.rename(columns={"index": "parcel_id"}, inplace=True)
        all_hu_forecast = pd.concat([hu_forecast.to_frame(hu_forecast.local_columns),
                                     sr14cap[hu_forecast.local_columns]], sort=True)  # type: pd.DataFrame
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
    target_units_df = orca.get_table('households').to_frame()
    target_for_year = int(target_units_df.at[year, 'housing_units_add'])

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
    print(f' {sched_dev_built} units built as Scheduled Development in {year}')
    print(f' {adus_built} units built as ADU in {year}')
    print(f' {subregional_control_built} units built as Stochastic Units in {year}')
    print(f' {entire_region_built} units built as Total Remaining in {year}')
    print(f' {all_built} total housing units in {year}')
    print(f' {target_for_year} was the target number of units for {year}.')
    if all_built != target_for_year:
        print(f'WARNING! TARGET {target_for_year} =/= ACTUAL {all_built} IN {year}!.')
        exit()

    # Subset scheduled development parcels.
    sch_units = (hu_forecast_year.loc[(hu_forecast_year.capacity_type == 'sch')]).units_added.sum()
    print(f'  {sch_units} sch units.')

    # Subset parcels from jur / upd.
    jur_units = (hu_forecast_year.loc[(hu_forecast_year.capacity_type.isin(['jur', 'upd']))]).units_added.sum()
    print(f'  {jur_units} jur/upd units.')

    # # Subset parcels from sgoa.
    # sgoa_units = (hu_forecast_year.loc[(hu_forecast_year.capacity_type.isin(['cc', 'mc', 'tc', 'tco', 'uc']
    #                                                                         ))]).units_added.sum()
    # print(f'  {sgoa_units} sgoa units.')

    # Subset ADU parcels.
    adu_units = (hu_forecast_year.loc[(hu_forecast_year.capacity_type == 'adu')]).units_added.sum()
    print(f'  {adu_units} adu units.')

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


def run_matching(run_match_output, hu_forecast):
    # Unwraps the dataframes
    run_match_df = run_match_output.to_frame()
    parcels = orca.get_table('parcels').to_frame()

    # target_match_df = run_match_df.groupby(['jcpa', 'year_simulation'])['unit_change'].sum().reset_index()

    # Creates empty dataframe to track added parcels
    # sr14cap = pd.DataFrame(columns=['parcel_id', 'capacity_type', 'units_added'])

    # target_comp_df = pd.DataFrame()
    # for year in control_totals_df.yr.unique().tolist():
    #     control_totals_by_year = control_totals_df.loc[control_totals_df.yr == year].copy()
    #     current_hh = int(hh_df.at[year, 'housing_units_add'])
    #     subregional_targets = largest_remainder_allocation(control_totals_by_year, current_hh)
    #     target_comp_df = target_comp_df.append(subregional_targets[['geo_id', 'yr', 'targets']], sort=True)
    #
    # matched_geos = pd.merge(target_to_match_df, target_comp_df, how='left', left_on=['jcpa', 'year_simulation'],
    #                         right_on=['geo_id', 'yr'])
    # matched_geos['diffs'] = matched_geos.targets - matched_geos.unit_change
    #
    # num_match = 0
    # num_low = 0
    # num_high = 0
    # num_total = 0
    # for yr, geo_id in zip(matched_geos.yr, matched_geos.geo_id):
    #     num_total = num_total + 1
    #     compatible_value = matched_geos.loc[(matched_geos.yr == yr) & (matched_geos.geo_id == geo_id)].diffs.values[0]
    #     if (num_total % 600) == 0:
    #         print(f'{round(num_total/24.45, 3)}% done...')
    #     if compatible_value >= 0:
    #         full_match_geo = run_match_df.loc[(run_match_df.year_simulation == yr) &
    #                                           (run_match_df.jcpa == geo_id)].copy()
    #         sr14cap = sr14cap.append(full_match_geo[['parcel_id', 'unit_change', 'year_simulation', 'capacity_type']],
    #                                  sort=True)
    #     if compatible_value == 0:
    #         num_match = num_match + 1
    #     elif compatible_value > 0:
    #         num_high = num_high + 1
    #     else:
    #         num_low = num_low + 1

    sr14cap = run_match_df.loc[run_match_df.capacity_type == 'sch'].copy()

    if len(sr14cap) > 0:
        # Adds the new scheduled_developments to the hu_forecast table.
        sr14cap.reset_index(inplace=True, drop=True)
        sr14cap.rename(columns={'year_simulation': 'year_built'}, inplace=True)
        sr14cap.rename(columns={'unit_change': 'units_added'}, inplace=True)
        condense_builds = pd.DataFrame({'units_added': sr14cap.groupby(["parcel_id", "year_built", "capacity_type"]).
                                       units_added.sum()}).reset_index()
        condense_builds['source'] = 6
        all_hu_forecast = pd.concat([hu_forecast.to_frame(hu_forecast.local_columns),
                                     condense_builds[hu_forecast.local_columns]], sort=True)  # type: pd.DataFrame
        all_hu_forecast.reset_index(drop=True, inplace=True)
        orca.add_table("hu_forecast", all_hu_forecast)

        matched_builds = pd.DataFrame({'units_added': all_hu_forecast.groupby(["parcel_id", "capacity_type"]).
                                      units_added.sum()}).reset_index()
        parcels = parcel_table_update_units(parcels, matched_builds)
        orca.add_table("parcels", parcels)

    # print(f'The current scenario will match {num_match} geo/year combinations (of {num_total}) exactly.\n'
    #       f'{num_high} geo/years match but will need additional units.\n'
    #       f'{num_low} geo/years now have reduced targets below the run to match.')
    # exit()

    # unmatched_geos = matched_geos.loc[~(matched_geos.diffs == 0)]
