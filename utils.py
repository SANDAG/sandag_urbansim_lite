from __future__ import print_function

import numpy as np
import orca
from urbansim.developer import developer
import pandas as pd


def largest_remainder_allocation(df, k):
    ratios = df.control.values
    frac, results = np.modf(k * ratios)
    remainder = int(k - results.sum()) # how many left

    indices = np.argsort(frac)[::-1]
    results[indices[0:remainder]] += 1 # add one to the ones with the highest decimal.

    df['targets'] = results.astype(int).tolist()

    return df


def run_feasibility(parcels):
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
    parcels = parcels.to_frame()
    # feasible parcels have capacity for additional units
    feasible_parcels = parcels.loc[parcels['total_cap'] > parcels['residential_units']]
    orca.add_table("feasibility", feasible_parcels)


def run_developer(forms, parcels, agents, buildings, reg_controls, jurisdictions, supply_fname,
                  total_units, feasibility, year=None,
                  target_vacancy=.1, form_to_btype_callback=None,
                  add_more_columns_callback=None, max_parcel_size=200000,
                  residential=True, bldg_sqft_per_job=400.0):
    """
    Run the developer model to pick and build buildings

    Parameters
    ----------

    parcels : DataFrame Wrapper
        Used to update residential units at the parcel level
    agents : DataFrame Wrapper
        Used to compute the current demand for units/floorspace in the area
    buildings : DataFrame Wrapper
        Used to compute the current supply of units/floorspace in the area
    supply_fname : string
        Identifies the column in buildings which indicates the supply of
        units/floorspace
    total_units : Series
        Passed directly to dev.pick - total current residential_units /
        job_spaces
    feasibility : DataFrame Wrapper
        The output from feasibility above (the table called 'feasibility')
    year : int
        The year of the simulation - will be assigned to 'year_built' on the
        new buildings
    target_vacancy : float
        The target vacancy rate - used to determine how much to build

    Returns
    -------
    Writes the result back to the buildings table (returns nothing)
    """

    dev = developer.Developer(feasibility.to_frame())

    control_totals = reg_controls.to_frame()
    jurs = jurisdictions.to_frame()

    control_totals_by_year =  control_totals.loc[control_totals.yr == year].copy()

    target_units = dev.\
        compute_units_to_build(agents.to_frame().hh.get_value(year),
                               buildings[supply_fname].sum(),
                               target_vacancy)

    print("{:,} feasible buildings before running developer"
          .format(len(dev.feasibility)))
    df = feasibility.to_frame()

    # subtract scheduled dev capacity
    target_units = target_units - df.loc[df['site_id'].notnull()].additional_units.sum()

    df = df.loc[df['site_id'].isnull()].copy()

    print('year is: ', year)
    print('target_units for region: ', target_units)

    subregional_targets = largest_remainder_allocation(control_totals_by_year, target_units)

    '''
        Do not pick or develop if there are no feasible parcels
    '''
    if len(dev.feasibility) == 0:
        print ('0 feasible buildings')
        return

    '''
        Pick parcels to for new buildings
    '''

    new_buildings= pd.DataFrame()
    units_per_jur = pd.DataFrame()
    units_per_j = pd.DataFrame()

    jur_list = jurs['jurisdiction_id'].tolist()
    df['net_units'] = (df.total_cap - df.residential_units)
    df.net_units = df.net_units.astype(int)

    for jur in jur_list:
        units = subregional_targets.loc[subregional_targets['geo_id']==jur].targets.values[0]
        jur_name = jurs.loc[jurs.jurisdiction_id == jur].name.values[0]
        print("Jurisdiction %d %s target units: %d" % (jur,jur_name,units))

        df_jur = df.loc[df['jurisdiction_id'] == jur].copy()

        one_row_per_unit = df_jur.loc[np.repeat(df_jur.index.values, df_jur.net_units)].copy()
        one_row_per_unit.reset_index(drop=False,inplace=True)
        one_row_per_unit['net_units'] = 1

        if len(one_row_per_unit) < units:
            print("WARNING THERE WERE NOT ENOUGH PROFITABLE UNITS TO",
                  "MATCH DEMAND FOR ", jur_name,"IN YEAR ",year)
            choices = one_row_per_unit.index.values
        elif target_units <= 0:
            choices = []
        else:
            choices = np.random.choice(one_row_per_unit.index.values, size=min(len(one_row_per_unit.index), units),
                                       replace=False, p=None)

        random_choice_parcels =  one_row_per_unit.loc[choices]
        # group by parcel id  - one bldg per parcel with multiple units
        new_bldgs = pd.DataFrame({'count': random_choice_parcels.groupby(["parcel_id","jurisdiction_id", "additional_units","residential_units", "bldgs", "total_cap"]).size()}).reset_index()
        new_bldgs.rename(columns = {'count': 'net_units'},inplace=True)
        new_bldgs.set_index('parcel_id',inplace=True)

        new_units = new_bldgs.net_units.sum()

        new_buildings = new_buildings.append(new_bldgs)
        new_buildings.index = new_buildings.index.astype(int)

        dj = {'year': [year], 'jurisdiction': [jur_name], 'target_units_for_jur': [units],
              'target_units_for_region': [target_units], 'units_picked': [new_units]}

        jur_df = pd.DataFrame(data=dj)
        units_per_jur = units_per_jur.append(jur_df)

    parcels = parcels.to_frame()
    parcels = parcels.join(new_buildings[['net_units']])
    parcels.net_units  = parcels.net_units.fillna(0)
    parcels['residential_units'] = parcels['residential_units'] + parcels['net_units']
    parcels = parcels.drop(['net_units'], 1)

    remaining_units = target_units - units_per_jur.units_picked.sum()

    if remaining_units:

        df = df.drop(['net_units'], 1)
        df_remaining = df.join(new_buildings[['net_units']])
        df_remaining.net_units = df_remaining.net_units.fillna(0)
        df_remaining['residential_units'] = df_remaining['residential_units'] + df_remaining['net_units']
        df_remaining['net_units'] = df_remaining.total_cap - df_remaining.residential_units
        df_remaining.net_units = df_remaining.net_units.astype(int)
        df_remaining = df_remaining.loc[df_remaining.net_units != 0]
        one_row_per_unit = df_remaining.loc[np.repeat(df_remaining.index.values, df_remaining.net_units)].copy()
        one_row_per_unit.reset_index(drop=False,inplace=True)
        one_row_per_unit['net_units'] = 1
        if len(one_row_per_unit) < remaining_units:
            print("WARNING THERE WERE NOT ENOUGH PROFITABLE UNITS TO",
                  "MATCH DEMAND FOR IN YEAR ",year)
            choices = one_row_per_unit.index.values
        else:
            choices = np.random.choice(one_row_per_unit.index.values, size=min(len(one_row_per_unit.index), remaining_units),
                                       replace=False, p=None)
        random_choice_parcels =  one_row_per_unit.loc[choices]
        # group by parcel id  - one bldg per parcel with multiple units
        new_bldgs = pd.DataFrame({'count': random_choice_parcels.groupby(["parcel_id","jurisdiction_id", "additional_units","residential_units", "bldgs", "total_cap"]).size()}).reset_index()
        new_bldgs.rename(columns = {'count': 'net_units'},inplace=True)
        new_bldgs.set_index('parcel_id',inplace=True)
        parcels = parcels.join(new_bldgs[['net_units']])
        parcels.net_units = parcels.net_units.fillna(0)
        parcels['residential_units'] = parcels['residential_units'] + parcels['net_units']
        parcels = parcels.drop(['net_units'], 1)
        new_units = new_bldgs.net_units.sum()

        #new_buildings = new_buildings.append(new_bldgs)
        #new_buildings.index = new_buildings.index.astype(int)

        dj = {'year': [year], 'jurisdiction': ['all'], 'target_units_for_jur': [0],
              'target_units_for_region': [target_units], 'units_picked': [new_units]}

        jur_df = pd.DataFrame(data=dj)
        units_per_jur = units_per_jur.append(jur_df)


    dj = {'year': [year], 'jurisdiction': ['total'],
          'target_units_for_jur': [units_per_jur.target_units_for_jur.sum()],
          'target_units_for_region': [target_units], 'units_picked': [units_per_jur.units_picked.sum()]}


    jur_df = pd.DataFrame(data=dj)
    units_per_jur = units_per_jur.append(jur_df)
    units_per_j = units_per_j.append(units_per_jur)

    uj = orca.get_table('uj').to_frame()
    uj = uj.append(units_per_j)
    orca.add_table("uj", uj)

    '''
        Join parcels with parcels that have new units on parcel_id (add net units column)
    '''
    # parcels = parcels.to_frame()
    # parcels = parcels.join(new_buildings[['net_units']])
    #
    # parcels.net_units  = parcels.net_units.fillna(0)
    # parcels['residential_units'] = parcels['residential_units'] + parcels['net_units']
    # parcels = parcels.drop(['net_units'], 1)
    orca.add_table("parcels", parcels)

    new_buildings = new_buildings.reset_index()
    new_buildings['residential_units'] = new_buildings['net_units']
    # temporarily assign building type id
    new_buildings['building_type_id'] = 21
    if year is not None:
        new_buildings["year_built"] = year

    print("Adding {:,} buildings with {:,} {}"
          .format(len(new_buildings),
                  int(new_buildings[supply_fname].sum()),
                  supply_fname))
    '''
        Merge old building with the new buildings
    '''

    all_buildings = dev.merge(buildings.to_frame(buildings.local_columns),
                              new_buildings[buildings.local_columns])

    orca.add_table("buildings", all_buildings)
