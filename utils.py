from __future__ import print_function

import numpy as np
import orca
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
from urbansim.developer import developer
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
    ratios = df.control.values
    frac, results = np.modf(k * ratios)
    remainder = int(k - results.sum()) # how many left
    indices = np.argsort(frac)[::-1]
    if remainder > 0:
        results[indices[0:remainder]] += 1 # add one to the ones with the highest decimal.
    df['targets'] = results.astype(int).tolist()

    return df


def initialize_tables():
    units_per_j = pd.DataFrame()
    orca.add_table("uj", units_per_j)
    cap_results= pd.DataFrame()
    orca.add_table('sr14cap_out',cap_results)


def run_scheduled_development(hu_forecast, year):
    print('\n Now in year: %d' % (year))
    sched_dev = orca.get_table('scheduled_development').to_frame()
    completed_devs = sched_dev[sched_dev.final_year == year]
    final_sched_dev = pd.DataFrame({'residential_units': completed_devs.groupby(["parcel_id"]).res_units.sum()}).reset_index()
    orca.add_table("final_sched_dev", final_sched_dev)
    sched_dev = sched_dev[(sched_dev.yr==year) & (sched_dev.res_units > 0)]
    if len(sched_dev) > 0:
        max_bid = hu_forecast.index.values.max()
        idx = np.arange(max_bid + 1,max_bid+len(sched_dev)+1)
        sched_dev['hu_forecast_id'] = idx
        sched_dev = sched_dev.set_index('hu_forecast_id')
        sched_dev['year_built'] = year
        sched_dev['residential_units'] = sched_dev['res_units']
        sched_dev['hu_forecast_type_id'] = ''
        sched_dev['source'] = '1'
        from urbansim.developer.developer import Developer
        merge = Developer(pd.DataFrame({})).merge
        b = hu_forecast.to_frame(hu_forecast.local_columns)
        all_hu_forecast = merge(b,sched_dev[b.columns])
        orca.add_table("hu_forecast", all_hu_forecast)


def run_reducer(hu_forecast, year):
    reducible_parcels = orca.get_table('negative_parcels').to_frame()
    dev = developer.Developer(hu_forecast.to_frame())
    neg_cap_parcels = reducible_parcels[reducible_parcels['capacity'] < 0]
    reduce_first_parcels = neg_cap_parcels[(neg_cap_parcels.yr==year)]
    neg_cap_parcels = neg_cap_parcels[neg_cap_parcels['yr'].isnull()]
    if len(neg_cap_parcels) > 0:
        parcels_to_reduce = (len(neg_cap_parcels) / (2025-year)) + len(reduce_first_parcels)
        parcels_to_reduce = min((len(neg_cap_parcels)+len(reduce_first_parcels)), int(np.ceil(parcels_to_reduce)))
        random_neg_parcels = neg_cap_parcels.sample(frac=1, random_state=50).reset_index(drop=False)
        reducer = pd.concat([reduce_first_parcels, random_neg_parcels])
        parcels_reduced = reducer.head(parcels_to_reduce)
        parcels_reduced= parcels_reduced.set_index('index')
        parcels_reduced['year_built'] = year
        parcels_reduced['hu_forecast_type_id'] = ''
        parcels_reduced['residential_units'] = parcels_reduced['capacity']
        parcels_reduced['source'] = '4'
        for parcel in parcels_reduced['parcel_id'].tolist():
            reducible_parcels.loc[reducible_parcels.parcel_id == parcel, 'capacity'] = 0
        orca.add_table("negative_parcels", reducible_parcels)
        all_hu_forecast = dev.merge(hu_forecast.to_frame(hu_forecast.local_columns),
                          parcels_reduced[hu_forecast.local_columns])
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
    parcels = parcels.join(devyear)
    finished_dev = orca.get_table('final_sched_dev').to_frame()
    for parcel in finished_dev['parcel_id'].tolist():
        parcels.loc[parcels.index == parcel, 'residential_units'] = finished_dev.loc[finished_dev.parcel_id== parcel]['residential_units']
        parcels.loc[parcels.index == parcel, 'site_id'] = np.nan
    feasible_parcels = parcels.loc[parcels['buildout'] > parcels['residential_units']]
    # Restrict feasibility to specific years, based on scenario (TBD)
    feasible_parcels = feasible_parcels.loc[feasible_parcels['phase_yr_ctrl'] <= year]
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
                parcels_picked['residential_units_sim_yr'] = parcels_picked['remaining_capacity']
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
            one_row_per_unit = priority_then_random.reindex(priority_then_random.index.repeat(priority_then_random.units_for_year)).reset_index(drop=True)
            one_row_per_unit_picked = one_row_per_unit.head(target_number_of_units)
            parcels_picked = pd.DataFrame({'residential_units_sim_yr': one_row_per_unit_picked.
                                          groupby(["parcel_id", "jurisdiction_id", "capacity_base_yr",
                                                   "residential_units", "buildout"])
                                          .size()}).reset_index()
            parcels_picked.set_index('parcel_id', inplace=True)
    return parcels_picked


def run_developer(forms, parcels, agents, hu_forecast, reg_controls, jurisdictions, supply_fname,
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
    dev = developer.Developer(feasibility.to_frame())
    control_totals = reg_controls.to_frame()
    jurs = jurisdictions.to_frame()

    control_totals_by_year =  control_totals.loc[control_totals.yr == year].copy()

    # target units is num of households minus existing residential units
    # note: num of households is first adjusted by vacancy rate using:  num of households/(1-vacancy rate)
    # target vacancy from call to run_developer in models

    print("Agents are households. Agent spaces are dwelling units")
    # current vacancy = 1 - num_agents / float(num_units)
    # target_units = dev.\
    #     compute_units_to_build(agents.to_frame().housing_units_add.get_value(year),
    #                            hu_forecast[supply_fname].sum(),
    #                            target_vacancy)
    target_units = dev.\
        compute_units_to_build(agents.to_frame().total_housing_units.get_value(year),
                               hu_forecast.to_frame().loc[hu_forecast.year_built > 2016][supply_fname].sum(),
                               target_vacancy)

    feasible_parcels_df = feasibility.to_frame()

    print("Target of new units = {:,} after scheduled developments are built".format(target_units))

    print("{:,} feasible parcels before running developer (excludes sched dev)"
          .format(len(feasible_parcels_df)))

    # allocate target to each jurisdiction based on database table
    subregional_targets = largest_remainder_allocation(control_totals_by_year, target_units)

    '''
        Do not pick or develop if there are no feasible parcels
    '''
    if len(dev.feasibility) == 0:
        print ('0 feasible parcels')
        return

    '''
        Pick parcels to for new units
    '''
    # initialize dataframes for i/o tracking
    sr14cap = pd.DataFrame()
    feasible_parcels_df['remaining_capacity'] = (feasible_parcels_df.buildout - feasible_parcels_df.residential_units)
    feasible_parcels_df.remaining_capacity = feasible_parcels_df.remaining_capacity.astype(int)
    for jur in control_totals.geo_id.unique().tolist():
    # for jur in jurs['jurisdiction_id'].tolist():
        subregion_targets = subregional_targets.loc[subregional_targets['geo_id']==jur].targets.values[0]
        subregion_max = subregional_targets.loc[subregional_targets['geo_id']==jur].max_units.values[0]
        # use nanmin to handle null values for max units
        target_units_for_geo = np.nanmin(np.array([subregion_targets, subregion_max]))
        # target_units_for_geo = min(subregion_targets, subregion_max)
        # geo_name = jurs.loc[jurs.jurisdiction_id == jur].name.values[0]
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
            chosen['source'] = '2'
        sr14cap = sr14cap.append(chosen)

    if len(sr14cap):
        remaining_units = target_units - sr14cap.residential_units_sim_yr.sum()
    else: remaining_units = target_units

    if remaining_units > 0:
        feasible_parcels_df = feasible_parcels_df.join(sr14cap[['residential_units_sim_yr']])
        feasible_parcels_df.residential_units_sim_yr = feasible_parcels_df.residential_units_sim_yr.fillna(0)
        feasible_parcels_df['remaining_capacity'] = feasible_parcels_df.buildout - feasible_parcels_df.residential_units\
                                                    - feasible_parcels_df.residential_units_sim_yr
        feasible_parcels_df['remaining_capacity'] = feasible_parcels_df['remaining_capacity'].astype(int)
        feasible_parcels_df= feasible_parcels_df.loc[feasible_parcels_df.remaining_capacity > 0]
        feasible_parcels_df['partial_build'] = feasible_parcels_df.residential_units_sim_yr
        chosen = parcel_picker(feasible_parcels_df, remaining_units, "all", year)
        if len(chosen):
            chosen['source'] = '3'
        sr14cap = sr14cap.append(chosen)


    if len(sr14cap) > 0:
        # group by parcel id again if same parcel was picked
        parcel_sr14_units = pd.DataFrame({'residential_units_sim_yr': sr14cap.
                                            groupby(["parcel_id", "jurisdiction_id",
                                                     "capacity_base_yr", "residential_units",
                                                     "buildout"]).residential_units_sim_yr.sum()}).reset_index()
        parcel_sr14_units.set_index('parcel_id', inplace=True)
        parcel_sr14_units['partial_build'] = parcel_sr14_units.buildout - parcel_sr14_units.residential_units_sim_yr - parcel_sr14_units.residential_units
        parcels = parcels.drop(['partial_build'], 1)
        parcels = parcels.join(parcel_sr14_units[['residential_units_sim_yr','partial_build']])
        parcels.residential_units_sim_yr = parcels.residential_units_sim_yr.fillna(0)
        parcels.partial_build = parcels.partial_build.fillna(0)
        parcels['residential_units'] = parcels['residential_units'] + parcels['residential_units_sim_yr']
        parcels = parcels.drop(['residential_units_sim_yr'], 1)
        orca.add_table("parcels", parcels)


        sr14cap = sr14cap.reset_index()
        sr14cap['residential_units'] = sr14cap['residential_units_sim_yr']
        # temporarily assign hu_forecast type id
        sr14cap['hu_forecast_type_id'] = ''
        if year is not None:
            sr14cap["year_built"] = year

        print("Adding {:,} hu_forecast with {:,} {}"
              .format(len(sr14cap),
                      int(sr14cap[supply_fname].sum()),
                      supply_fname))
        '''
            Merge old hu_forecast with the new hu_forecast
        '''

        all_hu_forecast = dev.merge(hu_forecast.to_frame(hu_forecast.local_columns),
                                  sr14cap[hu_forecast.local_columns])

        orca.add_table("hu_forecast", all_hu_forecast)

def summary(year):
    current_builds = orca.get_table('hu_forecast').to_frame()
    current_builds = current_builds.loc[(current_builds.year_built == year)]
    sched_dev_built = (current_builds.loc[(current_builds.source == '1')]).residential_units.sum()
    subregional_control_built = (current_builds.loc[(current_builds.source == '2')]).residential_units.sum()
    entire_region_built = (current_builds.loc[(current_builds.source == '3')]).residential_units.sum()
    print(' %d units built as Scheduled Development in %d' % (sched_dev_built, year))
    print(' %d units built as Stochastic Units in %d' % (subregional_control_built, year))
    print(' %d units built as Total Remaining in %d' % (entire_region_built, year))