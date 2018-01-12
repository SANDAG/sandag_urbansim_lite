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
    parcels = orca.get_table('parcels').to_frame()
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

    if len(df.loc[df.index.get_duplicates()]):
        print('error: duplicate parcel ids:')
        print(df.loc[df.index.get_duplicates()])

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
    # initialize dataframes for i/o tracking
    new_buildings= pd.DataFrame()
    units_per_jur = pd.DataFrame()
    units_per_j = pd.DataFrame()

    df['available_units_to_build'] = (df.total_cap - df.residential_units)
    df.available_units_to_build = df.available_units_to_build.astype(int)

    for jur in jurs['jurisdiction_id'].tolist():
        target_units_for_jur = subregional_targets.loc[subregional_targets['geo_id']==jur].targets.values[0]
        jur_name = jurs.loc[jurs.jurisdiction_id == jur].name.values[0]
        print("Jurisdiction %d %s target units: %d" % (jur,jur_name,target_units_for_jur))

        df_jur = df.loc[df['jurisdiction_id'] == jur].copy()

        one_row_per_unit = df_jur.loc[np.repeat(df_jur.index.values, df_jur.available_units_to_build)].copy()
        one_row_per_unit.reset_index(drop=False,inplace=True)
        del one_row_per_unit['available_units_to_build']

        if len(one_row_per_unit) < target_units_for_jur:
            print("WARNING THERE WERE NOT ENOUGH PROFITABLE UNITS TO",
                  "MATCH DEMAND FOR ", jur_name,"IN YEAR ",year)
            choices = one_row_per_unit.index.values
        elif target_units <= 0:
            choices = []
        else:
            choices = np.random.choice(one_row_per_unit.index.values,
                                       size=min(len(one_row_per_unit.index),
                                                target_units_for_jur),
                                       replace=False, p=None)

        parcels_picked = one_row_per_unit.loc[choices]

        # group by parcel id since more than one units may be picked on a parcel
        new_bldgs = pd.DataFrame({'units_built': parcels_picked.
                                 groupby(["parcel_id","jurisdiction_id",
                                          "additional_units","residential_units",
                                          "bldgs", "total_cap"]).size()}).reset_index()
        # new_bldgs.rename(columns = {'count_units_on_parcel': 'net_units'},inplace=True)

        new_bldgs.set_index('parcel_id',inplace=True)

        new_units = new_bldgs.units_built.sum()

        new_buildings = new_buildings.append(new_bldgs)
        new_buildings.index = new_buildings.index.astype(int)

        # count units for debugging
        dj = {'year': [year], 'jurisdiction': [jur_name], 'target_units_for_jur': [target_units_for_jur],
              'target_units_for_region': [target_units], 'units_picked': [new_units]}

        jur_df = pd.DataFrame(data=dj)
        units_per_jur = units_per_jur.append(jur_df)

    remaining_units = target_units - units_per_jur.units_picked.sum()

    if remaining_units:

        df = df.drop(['available_units_to_build'], 1)
        df_remaining = df.join(new_buildings[['units_built']])
        df_remaining.units_built = df_remaining.units_built.fillna(0)
        df_remaining['available_units_to_build'] = df_remaining.total_cap - df_remaining.residential_units - df_remaining.units_built
        df_remaining.available_units_to_build = df_remaining.available_units_to_build.astype(int)
        df_remaining = df_remaining.loc[df_remaining.available_units_to_build != 0]
        one_row_per_unit = df_remaining.loc[np.repeat(df_remaining.index.values, df_remaining.available_units_to_build)].copy()
        one_row_per_unit.reset_index(drop=False,inplace=True)
        del one_row_per_unit['available_units_to_build']

        if len(one_row_per_unit) < remaining_units:
            print("WARNING THERE WERE NOT ENOUGH PROFITABLE UNITS TO",
                  "MATCH DEMAND FOR IN YEAR ",year)
            choices = one_row_per_unit.index.values
        else:
            choices = np.random.choice(one_row_per_unit.index.values, size=min(len(one_row_per_unit.index), remaining_units),
                                       replace=False, p=None)
        parcels_picked =  one_row_per_unit.loc[choices]
        # group by parcel id  - one bldg per parcel with multiple units
        new_bldgs = pd.DataFrame({'units_built': parcels_picked.
                                 groupby(["parcel_id","jurisdiction_id",
                                          "additional_units","residential_units",
                                          "bldgs", "total_cap"]).size()}).reset_index()

        new_bldgs.set_index('parcel_id',inplace=True)
        new_units = new_bldgs.units_built.sum()
        units_by_jur = pd.DataFrame({'units_picked_remaining': new_bldgs.
                                 groupby(["jurisdiction_id"]).units_built.sum()}).reset_index()

        units_remaining_by_jur = units_by_jur.merge(jurs, on='jurisdiction_id')

        # count units for debugging
        dj = {'year': [year], 'jurisdiction': ['all'], 'target_units_for_jur': [0],
              'target_units_for_region': [target_units], 'units_picked': [new_units]}

        jur_df = pd.DataFrame(data=dj)
        units_per_jur = units_per_jur.append(jur_df)

        units_per_jur = units_per_jur.merge(units_remaining_by_jur, left_on='jurisdiction', right_on='name', how='left')

        units_per_jur.units_picked_remaining = units_per_jur.units_picked_remaining.fillna(0)
        del units_per_jur['name']
        del units_per_jur['jurisdiction_id']
        bldgs_append = new_buildings.append(new_bldgs)

        # bldgs_append.reset_index(inplace=True)

        # sum units built over parcel id
        new_buildings = pd.DataFrame({'total_units_built': bldgs_append.
                                            groupby(["parcel_id", "jurisdiction_id",
                                                     "additional_units", "residential_units",
                                                     "bldgs", "total_cap"]).units_built.sum()}).reset_index()
        new_buildings.set_index('parcel_id', inplace=True)
        new_buildings.index = new_buildings.index.astype(int)
        new_buildings.rename(columns={'total_units_built': 'units_built'}, inplace=True)

    '''
        Join parcels with parcels that have new units on parcel_id (add net units column)
    '''

    parcels = parcels.to_frame()
    parcels = parcels.join(new_buildings[['units_built']])
    parcels.units_built = parcels.units_built.fillna(0)
    parcels['residential_units'] = parcels['residential_units'] + parcels['units_built']
    parcels = parcels.drop(['units_built'], 1)
    orca.add_table("parcels", parcels)

    target_unit_sum = units_per_jur.loc[units_per_jur.jurisdiction!='all'].target_units_for_jur.sum()

    # count units for debugging
    dj = {'year': [year], 'jurisdiction': ['total'],
          'target_units_for_jur': [target_unit_sum],
          'target_units_for_region': [target_units], 'units_picked': [units_per_jur.units_picked.sum()]}

    jur_df = pd.DataFrame(data=dj)
    units_per_jur = units_per_jur.append(jur_df)
    units_per_j = units_per_j.append(units_per_jur)

    uj = orca.get_table('uj').to_frame()
    uj = uj.append(units_per_j)
    orca.add_table("uj", uj)

    new_buildings = new_buildings.reset_index()
    new_buildings['residential_units'] = new_buildings['units_built']
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
