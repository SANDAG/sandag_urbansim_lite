import math
import datetime
import numpy as np
import orca
import pandas as pd
import subprocess
import yaml
from database import get_connection_string
from sqlalchemy import create_engine


def yaml_to_dict(yaml_file, yaml_section):
    """
    Load YAML from a file
    Read specific section to dictionary

    Parameters
    ----------
    yaml_file : File name from which to load YAML
    yaml_section : Section of YAML file to process

    Returns
    -------
    dict
        Conversion from YAML for a specific section.

    """

    with open(yaml_file,'r') as f:
            d = yaml.load(f)[yaml_section]

    return d


def add_run_to_db():
    db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
    mssql_engine = create_engine(db_connection_string)
    version_ids = yaml_to_dict('data/scenario_config.yaml', 'scenario')
    run_description = input("Please provide a run description: ")
    #run_description = get_run_desc()

    run_id_sql = '''
    SELECT max(run_id)
      FROM [urbansim].[urbansim].[urbansim_lite_output_runs]
    '''
    run_id_df = pd.read_sql(run_id_sql, mssql_engine)

    if run_id_df.values:
        run_id = int(run_id_df.values) + 1
    else:
        run_id = 1

    subregional_controls = version_ids['subregional_ctrl_id']
    target_housing_units = version_ids['target_housing_units_version']
    phase_year = version_ids['parcel_phase_yr']
    additional_capacity = version_ids['additional_capacity_version']
    scheduled_development = version_ids['sched_dev_version']

    last_commit = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).rstrip()

    output_records = pd.DataFrame(
        columns=['run_id', 'run_date', 'subregional_controls', 'target_housing_units', 'phase_year',
                 'additional_capacity', 'scheduled_development', 'git', 'run_description'])

    run_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    output_records.loc[run_id] = [run_id, run_date, subregional_controls, target_housing_units, phase_year,
                                  additional_capacity, scheduled_development, last_commit, run_description]
    output_records.to_sql(name='urbansim_lite_output_runs', con=mssql_engine, schema='urbansim', index=False,
                          if_exists='append')
    return run_id


def largest_remainder_allocation(df, k):
    df.reset_index(inplace=True,drop=True)
    ratios = df.control.values
    frac, results = np.modf(k * ratios)
    remainder = int(k - results.sum()) # how many left
    indices = np.argsort(frac)[::-1]
    if remainder > 0:
        results[indices[0:remainder]] += 1 # add one to the ones with the highest decimal.
    if remainder < 0:
            idx = df.index[df.geo_id == 1920]
            results[idx] = results[idx] + remainder
            print('\n\nNegative remainder: %d' % (remainder))
    df['targets'] = results.astype(int).tolist()

    return df




def parcel_table_update_units(parcel_table, current_builds):
    # This is the new parcel update section
    # Now merges parcels that were updated in the current year with existing parcel table
    parcel_table.reset_index(inplace=True, drop=True)
    updated_parcel_table = pd.merge(parcel_table, current_builds[['parcel_id', 'capacity_type', 'units_added']],\
                                    how='left', left_on=['parcel_id', 'capacity_type'],\
                                    right_on=['parcel_id', 'capacity_type'])
    updated_parcel_table.units_added = updated_parcel_table.units_added.fillna(0)
    updated_parcel_table['capacity_used'] = updated_parcel_table['capacity_used'] + updated_parcel_table['units_added']
    updated_parcel_table['lu_sim'].where(updated_parcel_table.units_added == 0, other=updated_parcel_table['plu'], inplace=True)
    updated_parcel_table['lu_sim'].where(~updated_parcel_table.lu_sim.isnull(), other=updated_parcel_table['lu_2017'],
                                         inplace=True)
    residential_unit_total = pd.DataFrame({'total_units_added': updated_parcel_table.\
                                          groupby(["parcel_id", "residential_units"]).units_added.sum()}).reset_index()
    residential_unit_total['residential_units'] = residential_unit_total['total_units_added'] + residential_unit_total['residential_units']
    updated_parcel_table = updated_parcel_table.drop(['partial_build'], 1)
    updated_parcel_table['partial_build'] = updated_parcel_table.units_added
    updated_parcel_table.partial_build = updated_parcel_table.partial_build.fillna(0)
    updated_parcel_table = updated_parcel_table.drop(['units_added'], 1)
    updated_parcel_table = updated_parcel_table.drop(['residential_units'], 1)
    updated_parcel_table = pd.merge(updated_parcel_table, residential_unit_total[['parcel_id','residential_units']], \
                                    how='left', left_on=['parcel_id'], right_on=['parcel_id'])
    updated_parcel_table.residential_units = updated_parcel_table.residential_units.astype(int)
    return updated_parcel_table


# def run_scheduled_development(hu_forecast, year):
#     print('\n Adding scheduled developments in year: %d' % (year))
#     sched_dev = orca.get_table('scheduled_development').to_frame()
#     sched_dev_yr = sched_dev[(sched_dev.yr==year) & (sched_dev.capacity > 0)].copy()
#     if len(sched_dev_yr) > 0:
#         sched_dev_yr['year_built'] = year
#         sched_dev_yr['units_added'] = sched_dev_yr['capacity']
#         sched_dev_yr['source'] = 1
#         b = hu_forecast.to_frame(hu_forecast.local_columns)
#         units = pd.concat([b,sched_dev_yr[b.columns]])
#         units.reset_index(drop=True,inplace=True)
#         units['source'] = units['source'].astype(int)
#         orca.add_table("hu_forecast",units)
#         sched_dev = pd.merge(sched_dev,sched_dev_yr[['parcel_id', 'units_added']],\
#                              how='left', left_on=['parcel_id'], right_on=['parcel_id'])
#         sched_dev.units_added.fillna(0,inplace=True)
#         sched_dev['residential_units'] = sched_dev['residential_units'] + sched_dev['units_added']
#         sched_dev['capacity_used'] = sched_dev['capacity_used'] + sched_dev['units_added']
#         sched_dev = sched_dev.drop(['units_added'], axis=1)
#         orca.add_table("scheduled_development", sched_dev)


# add all sched dev first - before using jur provided units
def run_scheduled_development(hu_forecast,households,year):
    print('\n Adding scheduled developments in year: %d' % (year))
    hh = int(households.to_frame().at[year, 'housing_units_add'])
    # prior to 2019 use 0 percent adu
    # 2019 to 2034 use 1% adu of target housing units
    # 2035 to 2050 use 5% adu of target housing units
    # note: anything higher than 1% from 2019 to 2034 with use up all adu capacity in
    # city of san diego, chula vista, oceanside, el cajon prior to 2035

    adu_share_df = orca.get_table('adu_allocation').to_frame()
    adu_share = int(round(adu_share_df.loc[adu_share_df['yr'] == year].allocation * hh, 0))
    hh = hh - adu_share

    print('\n Number of households in year: %d' % (hh + adu_share))
    sched_dev = orca.get_table('scheduled_development').to_frame()
    sched_dev.sort_values(by=['priority', 'site_id'],inplace=True)
    # sched_dev_yr = sched_dev[(sched_dev.yr==year) & (sched_dev.capacity > 0)].copy()
    sched_dev_yr = sched_dev.loc[sched_dev['capacity'] > sched_dev['capacity_used']].copy()
    sched_dev_yr['remaining'] = sched_dev_yr['capacity'] - sched_dev_yr['capacity_used']
    sched_dev_yr['remaining'] =  sched_dev_yr['remaining'].astype(int)
    if sched_dev_yr.remaining.sum() > 0:
        one_row_per_unit = sched_dev_yr.reindex(sched_dev_yr.index.repeat(sched_dev_yr.remaining)).\
            reset_index(drop=True)
        one_row_per_unit_picked = one_row_per_unit.head(hh)
        # for debugging purposes
        sched_dev_picked = pd.DataFrame({'units_added': one_row_per_unit_picked.
                                      groupby(["parcel_id"]).size()}).reset_index()
        sched_dev_picked['year_built'] = year
        sched_dev_picked['source'] = 1
        sched_dev_picked['capacity_type'] = 'sch'
        b = hu_forecast.to_frame(hu_forecast.local_columns)
        units = pd.concat([b, sched_dev_picked[b.columns]])
        units.reset_index(drop=True, inplace=True)
        units['source'] = units['source'].astype(int)
        orca.add_table("hu_forecast", units)
        sched_dev_updated = pd.merge(sched_dev,sched_dev_picked[['parcel_id','units_added']],how='left',on='parcel_id')
        sched_dev_updated.units_added.fillna(0,inplace=True)
        sched_dev_updated['residential_units'] = sched_dev_updated['residential_units'] + sched_dev_updated['units_added']
        sched_dev_updated['capacity_used'] = sched_dev_updated['capacity_used'] + sched_dev_updated['units_added']
        sched_dev_updated = sched_dev_updated.drop(['units_added'], axis=1)
        orca.add_table("scheduled_development", sched_dev_updated)


def run_reducer(hu_forecast, year):
    try:
        reducible_parcels = orca.get_table('negative_parcels').to_frame()
    except KeyError:
        pass
    else:
        neg_cap_parcels = reducible_parcels[reducible_parcels['capacity_2'] < 0]
        reduce_first_parcels = neg_cap_parcels[(neg_cap_parcels.yr == year)]
        neg_cap_parcels = neg_cap_parcels[neg_cap_parcels['yr'].isnull()]
        if len(neg_cap_parcels) > 0:
            parcels_to_reduce = (len(neg_cap_parcels) / (2025 - year)) + len(reduce_first_parcels)
            parcels_to_reduce = min((len(neg_cap_parcels) + len(reduce_first_parcels)), int(np.ceil(parcels_to_reduce)))
            random_neg_parcels = neg_cap_parcels.sample(frac=1, random_state=50).reset_index(drop=False)
            reducer = pd.concat([reduce_first_parcels, random_neg_parcels])
            parcels_reduced = reducer.head(parcels_to_reduce)
            parcels_reduced = parcels_reduced.set_index('index')
            parcels_reduced['year_built'] = year
            parcels_reduced['hu_forecast_type_id'] = ''
            parcels_reduced['residential_units'] = parcels_reduced['capacity_2']
            parcels_reduced['source'] = 4
            for parcel in parcels_reduced['parcel_id'].tolist():
                reducible_parcels.loc[reducible_parcels.parcel_id == parcel, 'capacity_2'] = 0
            orca.add_table("negative_parcels", reducible_parcels)
            all_hu_forecast = pd.concat([hu_forecast.to_frame(hu_forecast.local_columns),\
                                         parcels_reduced[hu_forecast.local_columns]])
            all_hu_forecast .reset_index(drop=True,inplace=True)
            orca.add_table("hu_forecast", all_hu_forecast)
    

def run_feasibility(parcels, year=None):
    """
    Execute development feasibility on all parcels

    Parameters
    ----------
    parcels : DataFrame Wrapper
        The data frame wrapper for the parcel data

    Returns
    -------
    Adds a table called feasibility to the orca object (returns nothing)
    """

    print("Computing feasibility")
    parcels = orca.get_table('parcels').to_frame()
    devyear = orca.get_table('devyear').to_frame()
    parcels.reset_index(inplace=True,drop=True)
    devyear.reset_index(inplace=True, drop=False)
    parcels = pd.merge(parcels, devyear, how='left', left_on=['parcel_id', 'capacity_type'], right_on=['parcel_id', 'capacity_type'])
    parcels.set_index('parcel_id',inplace=True)
    feasible_parcels = parcels.loc[parcels['capacity'] > parcels['capacity_used']].copy()
    feasible_parcels.phase_yr = feasible_parcels.phase_yr.fillna(2017)
    # Restrict feasibility to specific years, based on scenario (TBD)
    feasible_parcels = feasible_parcels.loc[feasible_parcels['phase_yr'] <= year].copy()
    # remove scheduled developments from feasibility table
    feasible_parcels = feasible_parcels.loc[feasible_parcels['site_id'].isnull()].copy()
    # no SGOAs before 2035
    if year < 2035:
        feasible_parcels = feasible_parcels.loc[feasible_parcels['capacity_type'].isin(['jur', 'adu'])].copy()
    orca.add_table("feasibility", feasible_parcels)


def adu_picker(year, current_hh, feasible_parcels_df):
    # prior to 2019 use 0 percent adu
    # 2019 to 2034 use 1% adu of target housing units
    # 2035 to 2050 use 5% adu of target housing units
    # note: anything higher than 1% from 2019 to 2034 with use up all adu capacity in
    # city of san diego, chula vista, oceanside, el cajon prior to 2035

    # Design to calculate HALF of available ADU in these cities: allocate that amount for 2019-2034, then make all
    # of it available for 2035-2050. Use dynamic percentages!
    adu_share_df = orca.get_table('adu_allocation').to_frame()
    adu_share = int(round(adu_share_df.loc[adu_share_df['yr'] == year].allocation * current_hh,0))

    adu_parcels = feasible_parcels_df.loc[(feasible_parcels_df.capacity_type == 'adu')].copy()
    try:
        shuffled_adu = adu_parcels.sample(frac=1, random_state=50).reset_index(drop=False)
    except ValueError:
        shuffled_adu = adu_parcels
    picked_adu_parcels = shuffled_adu.head(adu_share).copy()
    picked_adu_parcels['source'] = 5
    picked_adu_parcels['units_added'] = 1
    return picked_adu_parcels


def parcel_picker(parcels_to_choose, target_number_of_units, name_of_geo, year_simulation):
    parcels_picked = pd.DataFrame()
    if name_of_geo != 'all':
        parcels_to_choose = parcels_to_choose.loc[parcels_to_choose.capacity_type != 'adu'].copy()
    if target_number_of_units > 0:
        if parcels_to_choose.remaining_capacity.sum() < target_number_of_units:
            print("WARNING THERE WERE NOT ENOUGH UNITS TO MATCH DEMAND FOR", name_of_geo, "IN YEAR", year_simulation)
            if len(parcels_to_choose):
                parcels_picked= parcels_to_choose
                parcels_picked['units_added'] = parcels_picked['remaining_capacity']
                parcels_picked.drop(['site_id', 'remaining_capacity'], axis=1, inplace=True)
        else:
            shuffled_parcels = parcels_to_choose.sample(frac=1, random_state=50).reset_index(drop=False)
            previously_picked = shuffled_parcels.loc[shuffled_parcels.partial_build > 0]
            capacity_jur = shuffled_parcels.loc[(shuffled_parcels.capacity_type=='jur') & (shuffled_parcels.partial_build==0)]
            adu_parcels = shuffled_parcels.loc[shuffled_parcels.capacity_type == 'adu']
            # number_of_adu = math.ceil(.10* target_number_of_units)
            # if len(adu_parcels) > 0:
            #     adu_parcels_to_add = adu_parcels.head(number_of_adu)
            # else:
            #     adu_parcels_to_add = adu_parcels
            priority_parcels = pd.concat([previously_picked, capacity_jur])
            shuffled_parcels = shuffled_parcels[
                ~shuffled_parcels['parcel_id'].isin(priority_parcels.parcel_id.values.tolist())]
            shuffled_parcels = shuffled_parcels[
                ~shuffled_parcels['parcel_id'].isin(adu_parcels.parcel_id.values.tolist())]
            priority_then_random = pd.concat([priority_parcels, shuffled_parcels, adu_parcels])
            # if name_of_geo == 'all':
            #     adu_parcels = parcels_to_choose.loc[parcels_to_choose.capacity_type == 'adu'].copy()
            #     priority_then_random = pd.concat([priority_then_random, adu_parcels])

            # shuffled_parcels['project_urgency'] = (shuffled_parcels.remaining_capacity - 250)/(2051 - year_simulation + 1)
            #if shuffled_parcels.project_urgency.max() > 500:
            #    large_projects = shuffled_parcels.loc[shuffled_parcels.project_urgency > 500]
            #    priority_parcels = pd.concat([previously_picked, large_projects])
            #else:
            # priority_parcels = pd.concat([previously_picked])
            # if name_of_geo == "all":
             #   priority_then_random = pd.concat([shuffled_parcels, priority_parcels])
                #to remove edge-case double picking, could make this: priority_the_random = shuffled_parcels
            # else:
            #    priority_then_random = pd.concat([priority_parcels, shuffled_parcels])
            priority_then_random['units_for_year'] = priority_then_random.remaining_capacity
            large_build_checker = priority_then_random.remaining_capacity >= 250
            priority_then_random.loc[large_build_checker, 'units_for_year'] = 250
            max_build_checker = priority_then_random.remaining_capacity >= 500
            priority_then_random.loc[max_build_checker, 'units_for_year'] = 500
            if priority_then_random.units_for_year.sum() < target_number_of_units:
                priority_then_random['units_for_year'] = priority_then_random.remaining_capacity
            one_row_per_unit = priority_then_random.reindex(priority_then_random.index.repeat(priority_then_random.units_for_year)).reset_index(drop=True)
            one_row_per_unit_picked = one_row_per_unit.head(target_number_of_units)
            parcels_picked = pd.DataFrame({'units_added': one_row_per_unit_picked.
                                          groupby(["parcel_id",'capacity_type'])
                                          .size()}).reset_index()
            parcels_picked.set_index('parcel_id', inplace=True)
    return parcels_picked


def run_developer(forms, parcels, households, hu_forecast, reg_controls, jurisdictions, supply_fname,
                  total_units, feasibility, year=None,
                  target_vacancy=.03, form_to_btype_callback=None,
                  add_more_columns_callback=None, max_parcel_size=200000,
                  residential=True, bldg_sqft_per_job=400.0):
    """
    Run the developer model to pick and build hu_forecast

    Parameters
    ----------

    parcels : DataFrame Wrapper
        Used to update residential units at the parcel level
    agents : DataFrame Wrapper
        Used to compute the current demand for units/floorspace in the area
         (households)
    hu_forecast : DataFrame Wrapper
        Used to compute the current supply of units/floorspace in the area
    supply_fname : string
        Identifies the column in hu_forecast which indicates the supply of
        units/floorspace ("residential units")
    total_units : Series
        Passed directly to dev.pick - total current residential_units /
        job_spaces
    feasibility : DataFrame Wrapper
        The output from feasibility above (the table called 'feasibility')
    year : int
        The year of the simulation - will be assigned to 'year_built' on the
        new hu_forecast
    target_vacancy : float
        The target vacancy rate - used to determine how much to build

    Returns
    -------
    Writes the result back to the hu_forecast table (returns nothing)
    """

    parcels = parcels.to_frame()
    control_totals = reg_controls.to_frame()
    jurs = jurisdictions.to_frame()

    control_totals_by_year = control_totals.loc[control_totals.yr == year].copy()
    if round(control_totals_by_year.control.sum()) != 1:
        print("Control percentages for %d do not total 100: Cancelling model run." % year)
        exit()
    hh = households.to_frame().at[year, 'total_housing_units']

    # initialize dataframes for i/o tracking
    sr14cap = pd.DataFrame()
    feasible_parcels_df = feasibility.to_frame()


    current_hh = int(households.to_frame().at[year, 'housing_units_add'])
    adu_builds = adu_picker(year, current_hh, feasible_parcels_df)
    try:
        sr14cap = sr14cap.append(adu_builds[['parcel_id', 'capacity_type', 'units_added', 'source']])
        sr14cap.set_index('parcel_id', inplace=True)
    except KeyError:
        sr14cap
    adu_build_count = len(adu_builds)

    num_units = hu_forecast.to_frame().loc[hu_forecast.year_built > 2016][supply_fname].sum() + adu_build_count
    print("Number of households: {:,}".format(int(hh)))
    print("Number of units: {:,}".format(int(num_units)))
    target_vacancy = 0
    target_units = int(max(hh / (1 - target_vacancy) - num_units, 0))
    print("Target of new units = {:,}".format(current_hh))


    print("Target of new units = {:,} after scheduled developments and adu's are built".format(target_units))

    print("{:,} feasible parcels before running developer (excludes sched dev)"
          .format(len(feasible_parcels_df)))

    # allocate target to each jurisdiction based on database table
    subregional_targets = largest_remainder_allocation(control_totals_by_year, target_units)

    '''
        Do not pick or develop if there are no feasible parcels
    '''
    if len(feasible_parcels_df) == 0:
        print('0 feasible parcels')
        return

    '''
        Pick parcels to for new units
    '''

    feasible_parcels_df['remaining_capacity'] = feasible_parcels_df.capacity - \
                                                feasible_parcels_df.capacity_used
    feasible_parcels_df.remaining_capacity = feasible_parcels_df.remaining_capacity.astype(int)
    for jur in control_totals.geo_id.unique().tolist():
        subregion_targets = subregional_targets.loc[subregional_targets['geo_id']==jur].targets.values[0]
        subregion_max = subregional_targets.loc[subregional_targets['geo_id']==jur].max_units.values[0]
        target_units_for_geo = np.nanmin(np.array([subregion_targets, subregion_max]))
        # Selects the lower value of subregion_targets and subregion_max, but does not count 'NaN' as the lower value,
        # because normally the minimum of a number and NaN would be the NaN-value
        # geo_name = jurs.loc[jurs.cap_jurisdiction_id == jur].name.values[0]
        target_units_for_geo = int(target_units_for_geo)
        geo_name = str(jur)
        print("Jurisdiction %s target units: %d" % (geo_name,target_units_for_geo))
        # parcels_in_geo = feasible_parcels_df.loc[feasible_parcels_df['jurisdiction_id'] == jur].copy()
        parcels_in_geo = feasible_parcels_df.loc[feasible_parcels_df['jur_or_cpa_id'] == jur].copy()
        chosen = parcel_picker(parcels_in_geo, target_units_for_geo, geo_name, year)
        if not np.isnan(subregion_max): # Activates if subregion_max has a numeric value (not Null)
            # May need to add a tracker to update the subregional max for the remaining loop here
            # subregion_max = subregion_max - subregion_targets
            # Then would need to carry the new subregion max forward into the remaining loop
            if subregion_targets > subregion_max:
                # This removes the jurisdiction if it had a subregional max from feasibility so it won't be picked in
                # the remaining capacity run later.
                feasible_parcels_df = feasible_parcels_df.loc[feasible_parcels_df.jur_or_cpa_id!=jur].copy()
        if len(chosen):
            chosen['source'] = 2
        sr14cap = sr14cap.append(chosen)

    if len(sr14cap):
        remaining_units = target_units - int(sr14cap.units_added.sum()) + adu_build_count
    else: remaining_units = target_units

    if remaining_units > 0:
        # feasible_parcels_df = feasible_parcels_df.join(sr14cap[['units_added']])
        feasible_parcels_df.reset_index(inplace=True)
        sr14cap.reset_index(inplace=True)
        feasible_parcels_df = pd.merge(feasible_parcels_df,sr14cap[['parcel_id','units_added','capacity_type']],how='left',on=['parcel_id','capacity_type'])
        feasible_parcels_df.set_index('parcel_id', inplace=True)
        sr14cap.set_index('parcel_id',inplace=True)
        feasible_parcels_df.units_added = feasible_parcels_df.units_added.fillna(0)

        feasible_parcels_df['remaining_capacity'] = feasible_parcels_df.capacity - feasible_parcels_df.capacity_used\
                                                    - feasible_parcels_df.units_added
        feasible_parcels_df['remaining_capacity'] = feasible_parcels_df['remaining_capacity'].astype(int)
        feasible_parcels_df= feasible_parcels_df.loc[feasible_parcels_df.remaining_capacity > 0].copy()
        feasible_parcels_df['partial_build'] = feasible_parcels_df.units_added
        chosen = parcel_picker(feasible_parcels_df, remaining_units, "all", year)
        if len(chosen):
            chosen['source'] = 3
        sr14cap = sr14cap.append(chosen)

    if len(sr14cap) > 0:
        # temporarily assign hu_forecast type id
        if year is not None:
            sr14cap["year_built"] = year

        print("Adding {:,} parcels with {:,} {}"
                .format(len(sr14cap),
                        int(sr14cap[supply_fname].sum()),
                        supply_fname))
        '''
            Merge old hu_forecast with the new hu_forecast
        '''
        sr14cap.reset_index(inplace=True,drop=False)
        all_hu_forecast = pd.concat([hu_forecast.to_frame(hu_forecast.local_columns), \
                                     sr14cap[hu_forecast.local_columns]])
        all_hu_forecast.reset_index(drop=True, inplace=True)
        orca.add_table("hu_forecast", all_hu_forecast)


def summary(year):
    parcels = orca.get_table('parcels').to_frame()
    hu_forecast = orca.get_table('hu_forecast').to_frame()
    hu_forecast_year = hu_forecast.loc[(hu_forecast.year_built == year)].copy()
    sched_dev_built = (hu_forecast_year.loc[(hu_forecast_year.source == 1)]).units_added.sum()
    subregional_control_built = (hu_forecast_year.loc[(hu_forecast_year.source == 2)]).units_added.sum()
    entire_region_built = (hu_forecast_year.loc[(hu_forecast_year.source == 3)]).units_added.sum()
    adus_built = (hu_forecast_year.loc[(hu_forecast_year.source == 5)]).units_added.sum()
    all_built = hu_forecast_year.units_added.sum()
    print(' %d units built as Scheduled Development in %d' % (sched_dev_built, year))
    print(' %d units built as ADU in %d' % (adus_built, year))
    print(' %d units built as Stochastic Units in %d' % (subregional_control_built, year))
    print(' %d units built as Total Remaining in %d' % (entire_region_built, year))
    print(' %d total housing units in %d' % (all_built, year))
    # The below section is also run in bulk_insert. Will comment out the section in bulk_insert
    # Check if parcels occur multiple times (due to multiple sources). Will skip if false.
    current_builds = pd.DataFrame({'units_added': hu_forecast_year.
                                       groupby(["parcel_id", "year_built", "capacity_type"]).
                                       units_added.sum()}).reset_index()
    parcels = parcel_table_update_units(parcels, current_builds)
    orca.add_table("parcels", parcels)

