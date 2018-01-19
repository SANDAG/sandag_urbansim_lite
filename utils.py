from __future__ import print_function

import numpy as np
import orca
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
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
                  target_vacancy=.03, form_to_btype_callback=None,
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

    # target units is num of households minus existing residential units
    # note: num of households is first adjusted by vacancy rate using:  num of households/(1-vacancy rate)
    # target vacancy from call to run_developer in models
    print('year is: ', year)

    print("Agents are households. Agent spaces are dwelling units")
    target_units = dev.\
        compute_units_to_build(agents.to_frame().hh.get_value(year),
                               buildings[supply_fname].sum(),
                               target_vacancy)

    df = feasibility.to_frame()
    target_units = target_units - df.loc[df['site_id'].notnull()].additional_units.sum()

    print("Target of new units = {:,} after scheduled developments are built".format(target_units))

    # remove scheduled developments from feasibility table
    df = df.loc[df['site_id'].isnull()].copy()

    print("{:,} feasible parcels before running developer (excludes sched dev)"
          .format(len(df)))

    # allocate target to each jurisdiction based on database table
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


        if df_jur.available_units_to_build.sum() < target_units_for_jur:
            print("WARNING THERE WERE NOT ENOUGH PROFITABLE UNITS TO",
                  "MATCH DEMAND FOR ", jur_name,"IN YEAR ",year)
            if len(df_jur):
                new_bldgs = df_jur.copy()
                new_bldgs['units_built'] = new_bldgs['available_units_to_build']
                new_bldgs.drop(['site_id', 'year', 'available_units_to_build'], axis=1, inplace=True)
                new_units = new_bldgs.units_built.sum()
                new_buildings = new_buildings.append(new_bldgs)
                new_buildings.index = new_buildings.index.astype(int)
            else: new_units = 0
            # parcels_picked.reset_index(drop=False, inplace=True)
        elif target_units <= 0:
            new_units = 0
        else:
            # shuffle order of parcels
            df_jur_random_order = df_jur.sample(frac=1, random_state=50).reset_index(drop=False)

            # get partial built parcels from previous year of simulation - not all capacity used
            partial_built_parcel = df_jur_random_order.loc[df_jur_random_order.partial_build > 0]

            # drop parcels that are partially developed
            df_jur_random_order  = df_jur_random_order[~df_jur_random_order['parcel_id'].isin( partial_built_parcel.parcel_id.values.tolist() )]

            #add partially built parcels to the top of the list to be developed first.
            partial_then_random = pd.concat([partial_built_parcel,df_jur_random_order])

            one_row_per_unit = partial_then_random.reindex(partial_then_random.index.repeat(partial_then_random.available_units_to_build)).reset_index(drop=True)


            del one_row_per_unit['available_units_to_build']
            parcels_picked = one_row_per_unit.head(target_units_for_jur)

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
        df_updated = df.join(new_buildings[['units_built']])
        df_updated.units_built = df_updated.units_built.fillna(0)
        df_updated['available_units_to_build'] = df_updated.total_cap - df_updated.residential_units - df_updated.units_built

        df_updated.available_units_to_build = df_updated.available_units_to_build.astype(int)
        df_updated = df_updated.loc[df_updated.available_units_to_build != 0]

        if df_updated.available_units_to_build.sum() < remaining_units:
            print("WARNING THERE WERE NOT ENOUGH PROFITABLE UNITS TO",
                  "MATCH DEMAND FOR IN YEAR ",year)
            if len(df_updated):
                new_bldgs = df_updated.copy()
                new_bldgs['units_built'] = new_bldgs['available_units_to_build']
                new_bldgs.drop(['site_id', 'year', 'available_units_to_build'], axis=1, inplace=True)
                new_units = new_bldgs.units_built.sum()
                new_buildings = new_buildings.append(new_bldgs)
                new_buildings.index = new_buildings.index.astype(int)
            else:
                new_units = 0
        else:
            df_updated['partial_build'] = df_updated.units_built
            df_random_order = df_updated.sample(frac=1, random_state=50).reset_index(drop=False)

            # get partial built parcels from current year of simulation - not all capacity used
            partial_built_parcel = df_updated.loc[df_updated['partial_build'] > 0].reset_index()

            # drop parcels that are partially developed
            df_random_order = df_random_order[
                ~df_random_order['parcel_id'].isin(partial_built_parcel.parcel_id.values.tolist())]

            # add partially built parcels to the top of the list to be developed first.
            partial_then_random = pd.concat([partial_built_parcel, df_random_order])
            partial_then_random.set_index('parcel_id', inplace=True)
            one_row_per_unit = partial_then_random.loc[
                np.repeat(partial_then_random.index.values, partial_then_random.available_units_to_build)].copy()
            one_row_per_unit.reset_index(drop=False, inplace=True)
            del one_row_per_unit['available_units_to_build']
            parcels_picked = one_row_per_unit.head(target_units_for_jur)

            # group by parcel id since more than one units may be picked on a parcel
            new_bldgs = pd.DataFrame({'units_built': parcels_picked.
                                     groupby(["parcel_id", "jurisdiction_id",
                                              "additional_units", "residential_units",
                                              "bldgs", "total_cap"]).size()}).reset_index()
            # new_bldgs.rename(columns = {'count_units_on_parcel': 'net_units'},inplace=True)

            new_bldgs.set_index('parcel_id', inplace=True)

            new_units = new_bldgs.units_built.sum()

            new_buildings = new_buildings.append(new_bldgs)
            new_buildings.index = new_buildings.index.astype(int)


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

    db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
    mssql_engine = create_engine(db_connection_string)

    parcels = parcels.to_frame()
    new_buildings['units_not_built'] = new_buildings.total_cap - new_buildings.units_built - new_buildings.residential_units
    parcels = parcels.join(new_buildings[['units_built','units_not_built']])
    parcels.units_built = parcels.units_built.fillna(0)
    parcels.units_not_built = parcels.units_not_built.fillna(0)
    parcels.partial_build = parcels['units_not_built']
    parcels['residential_units'] = parcels['residential_units'] + parcels['units_built']
    parcels = parcels.drop(['units_built','units_not_built'], 1)


    orca.add_table("parcels", parcels)
    parcels['year'] = year

    #parcels.to_sql(name='urbansim_lite_output_parcels', con=mssql_engine, schema='urbansim', if_exists='append',
    #                 index=False) #no run ID -> appending to database

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
