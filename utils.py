from __future__ import print_function

import numpy as np
import orca
import pandas as pd
import yaml


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
    parcel_table.reset_index(inplace=True, drop=False)
    updated_parcel_table = pd.merge(parcel_table, current_builds[['parcel_id', 'capacity_type', 'units_in_yr']],\
                                    how='left', left_on=['parcel_id', 'capacity_type'],\
                                    right_on=['parcel_id', 'capacity_type'])
    updated_parcel_table.units_in_yr = updated_parcel_table.units_in_yr.fillna(0)
    updated_parcel_table['capacity_used'] = updated_parcel_table['capacity_used'] + updated_parcel_table['units_in_yr']
    # updated_parcel_table['lu'].where(updated_parcel_table.units_added == 0, other=updated_parcel_table['plu'], inplace=True)
    residential_unit_total = pd.DataFrame({'total_units_added': updated_parcel_table.\
                                          groupby(["parcel_id", "residential_units"]).units_in_yr.sum()}).reset_index()
    residential_unit_total['residential_units'] = residential_unit_total['total_units_added'] + residential_unit_total['residential_units']
    updated_parcel_table = updated_parcel_table.drop(['partial_build'], 1)
    updated_parcel_table['partial_build'] = updated_parcel_table.capacity - updated_parcel_table['capacity_used']
    updated_parcel_table.partial_build = updated_parcel_table.partial_build.fillna(0)
    updated_parcel_table = updated_parcel_table.drop(['units_in_yr'], 1)
    updated_parcel_table = updated_parcel_table.drop(['residential_units'], 1)
    updated_parcel_table = pd.merge(updated_parcel_table, residential_unit_total[['parcel_id','residential_units']], \
                                    how='left', left_on=['parcel_id'], right_on=['parcel_id'])
    updated_parcel_table.residential_units = updated_parcel_table.residential_units.astype(int)
    updated_parcel_table.set_index('parcel_id', inplace=True)
    return updated_parcel_table


def run_scheduled_development(hu_forecast, year):
    print('\n Adding scheduled developments in year: %d' % (year))
    sched_dev = orca.get_table('scheduled_development').to_frame()
    sched_dev_yr = sched_dev[(sched_dev.yr==year) & (sched_dev.capacity > 0)].copy()
    if len(sched_dev_yr) > 0:
        sched_dev_yr['year_built'] = year
        sched_dev_yr['units_added'] = sched_dev_yr['capacity']
        sched_dev_yr['source'] = 1
        b = hu_forecast.to_frame(hu_forecast.local_columns)
        units = pd.concat([b,sched_dev_yr[b.columns]])
        units.reset_index(drop=True,inplace=True)
        units['source'] = units['source'].astype(int)
        orca.add_table("hu_forecast",units)
        sched_dev = pd.merge(sched_dev,sched_dev_yr[['parcel_id', 'units_added']],\
                             how='left', left_on=['parcel_id'], right_on=['parcel_id'])
        sched_dev.units_added.fillna(0,inplace=True)
        sched_dev['residential_units'] = sched_dev['residential_units'] + sched_dev['units_added']
        sched_dev['capacity_used'] = sched_dev['capacity_used'] + sched_dev['units_added']
        sched_dev = sched_dev.drop(['units_added'], axis=1)
        orca.add_table("scheduled_development", sched_dev)

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
    # for debugging purposes
    # if year==2049:
        # print(year)
    parcels = orca.get_table('parcels').to_frame()
    devyear = orca.get_table('devyear').to_frame()
    parcels.reset_index(inplace=True,drop=False)
    devyear.reset_index(inplace=True, drop=False)
    parcels = pd.merge(parcels, devyear, how='left', left_on=['parcel_id', 'capacity_type'], right_on=['parcel_id', 'capacity_type'])
    parcels.set_index('parcel_id',inplace=True)
    feasible_parcels = parcels.loc[parcels['capacity'] > parcels['capacity_used']].copy()
    feasible_parcels.phase_yr = feasible_parcels.phase_yr.fillna(2017)
    # Restrict feasibility to specific years, based on scenario (TBD)
    feasible_parcels = feasible_parcels.loc[feasible_parcels['phase_yr'] <= year]
    # remove scheduled developments from feasibility table
    feasible_parcels = feasible_parcels.loc[feasible_parcels['site_id'].isnull()].copy()
    orca.add_table("feasibility", feasible_parcels)


def parcel_picker(parcels_to_choose, target_number_of_units, name_of_geo, year_simulation):
    parcels_picked = pd.DataFrame()
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
            # shuffled_parcels['project_urgency'] = (shuffled_parcels.remaining_capacity - 250)/(2051 - year_simulation + 1)
            #if shuffled_parcels.project_urgency.max() > 500:
            #    large_projects = shuffled_parcels.loc[shuffled_parcels.project_urgency > 500]
            #    priority_parcels = pd.concat([previously_picked, large_projects])
            #else:

            priority_parcels = pd.concat([previously_picked])
            shuffled_parcels = shuffled_parcels[
                ~shuffled_parcels['parcel_id'].isin(priority_parcels.parcel_id.values.tolist())]
            if name_of_geo == "all":
                priority_then_random = pd.concat([shuffled_parcels, priority_parcels])
                #to remove edge-case double picking, could make this: priority_the_random = shuffled_parcels
            else:
                priority_then_random = pd.concat([priority_parcels, shuffled_parcels])
            priority_then_random['units_for_year'] = priority_then_random.remaining_capacity
            large_build_checker = priority_then_random.remaining_capacity >= 250
            priority_then_random.loc[large_build_checker, 'units_for_year'] = 250
            max_build_checker = priority_then_random.partial_build >= 500
            priority_then_random.loc[max_build_checker, 'units_for_year'] = 500
            if priority_then_random.units_for_year.sum() < target_number_of_units:
                priority_then_random['units_for_year'] = priority_then_random.remaining_capacity
            one_row_per_unit = priority_then_random.reindex(priority_then_random.index.repeat(priority_then_random.units_for_year)).reset_index(drop=True)
            one_row_per_unit_picked = one_row_per_unit.head(target_number_of_units)
            # for debugging purposes
            if len(one_row_per_unit_picked.loc[one_row_per_unit_picked.parcel_id==641960]) > 0:
                print(one_row_per_unit_picked)
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

    control_totals_by_year =  control_totals.loc[control_totals.yr == year].copy()
    hh = households.to_frame().at[year, 'total_housing_units']
    num_units = hu_forecast.to_frame().loc[hu_forecast.year_built > 2016][supply_fname].sum()
    print("Number of households: {:,}".format(int(hh)))
    print("Number of units: {:,}".format(int(num_units)))
    target_vacancy = 0
    target_units = int(max(hh / (1 - target_vacancy) - num_units, 0))
    print("Target of new units = {:,}"
          .format(target_units))

    feasible_parcels_df = feasibility.to_frame()

    print("Target of new units = {:,} after scheduled developments are built".format(target_units))

    print("{:,} feasible parcels before running developer (excludes sched dev)"
          .format(len(feasible_parcels_df)))

    # allocate target to each jurisdiction based on database table
    print(year)
    subregional_targets = largest_remainder_allocation(control_totals_by_year, target_units)

    '''
        Do not pick or develop if there are no feasible parcels
    '''
    if len(feasible_parcels_df) == 0:
        print ('0 feasible parcels')
        return

    '''
        Pick parcels to for new units
    '''
    # initialize dataframes for i/o tracking
    sr14cap = pd.DataFrame()
    feasible_parcels_df['remaining_capacity'] = feasible_parcels_df.capacity - \
                                                feasible_parcels_df.capacity_used
    feasible_parcels_df.remaining_capacity = feasible_parcels_df.remaining_capacity.astype(int)
    for jur in control_totals.geo_id.unique().tolist():
    # for jur in jurs['cap_jurisdiction_id'].tolist():
    # for debugging purposes
        if (year==2018):
            print(jur)
        subregion_targets = subregional_targets.loc[subregional_targets['geo_id']==jur].targets.values[0]
        subregion_max = subregional_targets.loc[subregional_targets['geo_id']==jur].max_units.values[0]
        # use nanmin to handle null values for max units
        target_units_for_geo = np.nanmin(np.array([subregion_targets, subregion_max]))
        # target_units_for_geo = min(subregion_targets, subregion_max)
        # geo_name = jurs.loc[jurs.cap_jurisdiction_id == jur].name.values[0]
        target_units_for_geo = int(target_units_for_geo)
        geo_name = str(jur)
        print("Jurisdiction %s target units: %d" % (geo_name,target_units_for_geo))
        # parcels_in_geo = feasible_parcels_df.loc[feasible_parcels_df['jurisdiction_id'] == jur].copy()
        parcels_in_geo = feasible_parcels_df.loc[feasible_parcels_df['jur_or_cpa_id'] == jur].copy()
        chosen = parcel_picker(parcels_in_geo, target_units_for_geo, geo_name, year)
        if not np.isnan(subregion_max): # check not null before comparing to subregion targets
            if subregion_targets > subregion_max: #if subregion_max is NaN, this gets skipped (which is fine)
                # feasible_parcels_df = feasible_parcels_df.drop(feasible_parcels_df[feasible_parcels_df.jur_or_cpa_id == jur].index)
                feasible_parcels_df = feasible_parcels_df.loc[feasible_parcels_df.jur_or_cpa_id!=jur].copy()
        if len(chosen):
            # for debugging purposes
            if len(chosen.loc[chosen.index==641960]) > 0:
                print(chosen.loc[641960])
            chosen['source'] = 2
        sr14cap = sr14cap.append(chosen)

    if len(sr14cap):
        remaining_units = target_units - int(sr14cap.units_added.sum())
    else: remaining_units = target_units

    if remaining_units > 0:
        feasible_parcels_df = feasible_parcels_df.join(sr14cap[['units_added']])
        feasible_parcels_df.units_added = feasible_parcels_df.units_added.fillna(0)
        feasible_parcels_df['remaining_capacity'] = feasible_parcels_df.capacity - feasible_parcels_df.capacity_used\
                                                    - feasible_parcels_df.units_added
        feasible_parcels_df['remaining_capacity'] = feasible_parcels_df['remaining_capacity'].astype(int)
        feasible_parcels_df= feasible_parcels_df.loc[feasible_parcels_df.remaining_capacity > 0].copy()
        if year==2024:
            print(year)
        feasible_parcels_df['partial_build'] = feasible_parcels_df.units_added
        chosen = parcel_picker(feasible_parcels_df, remaining_units, "all", year)
        if len(chosen):
            chosen['source'] = 3
        sr14cap = sr14cap.append(chosen)

    if len(sr14cap) > 0:
        # temporarily assign hu_forecast type id
        if year is not None:
            sr14cap["year_built"] = year

        print("Adding {:,} hu_forecast with {:,} {}"
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
    print(' %d units built as Scheduled Development in %d' % (sched_dev_built, year))
    print(' %d units built as Stochastic Units in %d' % (subregional_control_built, year))
    print(' %d units built as Total Remaining in %d' % (entire_region_built, year))
    # The below section is also run in bulk_insert. Will comment out the section in bulk_insert
    # Check if parcels occur multiple times (due to multiple sources). Will skip if false.
    current_builds = pd.DataFrame({'units_in_yr': hu_forecast_year.
                                       groupby(["parcel_id", "year_built", "capacity_type"]).
                                       units_added.sum()}).reset_index()
    parcels = parcel_table_update_units(parcels, current_builds)
    orca.add_table("parcels", parcels)
