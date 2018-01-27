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
    if remainder > 0:
        results[indices[0:remainder]] += 1 # add one to the ones with the highest decimal.
    df['targets'] = results.astype(int).tolist()

    return df


def initialize_tables():
    units_per_j = pd.DataFrame()
    orca.add_table("uj", units_per_j)

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
    feasible_parcels = parcels.loc[parcels['max_res_units'] > parcels['residential_units']]
    # Restrict feasibility to specific years, based on scenario (TBD)
    feasible_parcels = feasible_parcels.loc[feasible_parcels['earliest_dev_year'] < year]
    # remove scheduled developments from feasibility table
    feasible_parcels = feasible_parcels.loc[feasible_parcels['site_id'].isnull()].copy()
    orca.add_table("feasibility", feasible_parcels)

def parcel_picker(df_area, area_units, jur_name, year):
    parcels_picked = pd.DataFrame()
    new_bldgs = pd.DataFrame()
    if df_area.remaining_capacity.sum() < area_units:
        print("WARNING THERE WERE NOT ENOUGH PROFITABLE UNITS TO MATCH DEMAND FOR ", jur_name, "IN YEAR ", year)
        if len(df_area):
            new_bldgs = df_area.copy()
            new_bldgs['residential_units_sim_yr'] = new_bldgs['remaining_capacity']
            new_bldgs.drop(['site_id', 'remaining_capacity'], axis=1, inplace=True)
            new_units_count = new_bldgs.residential_units_sim_yr.sum()
        else:
            new_units_count = 0
    elif area_units <= 0:
        new_units_count = 0
    else:
        # shuffle order of parcels
        df_random_order = df_area.sample(frac=1, random_state=50).reset_index(drop=False)

        # get partial built parcels from <previous/current> year of simulation - not all capacity used
        partial_built_parcel = df_random_order.loc[df_random_order.partial_build > 0]

        # drop parcels that are partially developed
        df_random_order = df_random_order[~df_random_order['parcel_id'].isin(partial_built_parcel.parcel_id.values.tolist())]

        # add partially built parcels to the top of the list to be developed first.
        partial_then_random = pd.concat([partial_built_parcel, df_random_order])

        one_row_per_unit = partial_then_random.reindex(partial_then_random.index.repeat(partial_then_random.remaining_capacity)).reset_index(drop=True)

        del one_row_per_unit['remaining_capacity']
        parcels_picked = one_row_per_unit.head(area_units)

        # group by parcel id since more than one units may be picked on a parcel
        new_bldgs = pd.DataFrame({'residential_units_sim_yr': parcels_picked.groupby(["parcel_id", "jurisdiction_id","capacity_base_yr", "residential_units","bldgs", "max_res_units"]).size()}).reset_index()
        # new_bldgs_df.rename(columns = {'count_units_on_parcel': 'net_units'},inplace=True)

        new_bldgs.set_index('parcel_id', inplace=True)

        new_units_count = new_bldgs.residential_units_sim_yr.sum()
    return new_units_count, parcels_picked, new_bldgs


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
    target_units = dev.\
        compute_units_to_build(agents.to_frame().hh.get_value(year),
                               buildings[supply_fname].sum(),
                               target_vacancy)

    feasible_parcels_df = feasibility.to_frame()

    num_of_sched_dev = parcels.loc[~parcels['site_id'].isnull()].capacity_base_yr.sum()
    target_units = target_units - num_of_sched_dev

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
    new_units_df= pd.DataFrame()
    units_per_jurisdiction = pd.DataFrame()
    units_summary = pd.DataFrame()

    feasible_parcels_df['remaining_capacity'] = (feasible_parcels_df.max_res_units - feasible_parcels_df.residential_units)
    feasible_parcels_df.remaining_capacity = feasible_parcels_df.remaining_capacity.astype(int)

    for jur in jurs['jurisdiction_id'].tolist():
        target_units_for_jur = subregional_targets.loc[subregional_targets['geo_id']==jur].targets.values[0]
        jur_name = jurs.loc[jurs.jurisdiction_id == jur].name.values[0]
        print("Jurisdiction %d %s target units: %d" % (jur,jur_name,target_units_for_jur))

        df_jur = feasible_parcels_df.loc[feasible_parcels_df['jurisdiction_id'] == jur].copy()

        new_units_count, parcels_picked, new_bldgs = parcel_picker(df_jur, target_units_for_jur, jur_name, year)
        new_units_df = new_units_df.append(new_bldgs)
        # orca.add_table('new_units', new_units_df)
        new_units_df.index = new_units_df.index.astype(int)
        # count units for debugging
        dj = {'year': [year], 'jurisdiction': [jur_name], 'target_units_for_jur': [target_units_for_jur],
              'target_units_for_region': [target_units], 'units_picked': [new_units_count]}

        jur_df = pd.DataFrame(data=dj)
        units_per_jurisdiction = units_per_jurisdiction.append(jur_df)

    count_units_picked_remaining = 0
    remaining_units = target_units - units_per_jurisdiction.units_picked.sum()

    if remaining_units > 0:

        feasible_parcels_df = feasible_parcels_df.drop(['remaining_capacity'], 1)
        df_updated = feasible_parcels_df.join(new_units_df[['residential_units_sim_yr']])
        df_updated.residential_units_sim_yr = df_updated.residential_units_sim_yr.fillna(0)
        df_updated['remaining_capacity'] = df_updated.max_res_units - df_updated.residential_units - df_updated.residential_units_sim_yr
        # feasible_parcels = parcels.loc[parcels['max_res_units'] > parcels['residential_units']]

        df_updated.remaining_capacity = df_updated.remaining_capacity.astype(int)
        df_updated = df_updated.loc[df_updated.remaining_capacity != 0]
        df_updated['partial_build'] = df_updated.residential_units_sim_yr

        new_units_count, parcels_picked, new_bldgs = parcel_picker(df_updated, remaining_units, "", year)
        new_units_df = new_units_df.append(new_bldgs)
        # orca.add_table('new_units', new_units_df)
        new_units_df.index = new_units_df.index.astype(int)
        units_by_jur = pd.DataFrame({'units_picked_remaining': new_bldgs.
                                 groupby(["jurisdiction_id"]).residential_units_sim_yr.sum()}).reset_index()

        units_remaining_by_jur = units_by_jur.merge(jurs, on='jurisdiction_id')

        # # count units for debugging
        # dj = {'year': [year], 'jurisdiction': ['all'], 'target_units_for_jur': [0],
        #       'target_units_for_region': [target_units], 'units_picked': [new_units_count]}
        #
        # jur_df = pd.DataFrame(data=dj)
        # units_per_jurisdiction = units_per_jurisdiction.append(jur_df)

        units_per_jurisdiction = units_per_jurisdiction.merge(units_remaining_by_jur, left_on='jurisdiction', right_on='name', how='left')

        units_per_jurisdiction.units_picked_remaining = units_per_jurisdiction.units_picked_remaining.fillna(0)
        count_units_picked_remaining = units_per_jurisdiction.units_picked_remaining.sum()
        del units_per_jurisdiction['name']
        del units_per_jurisdiction['jurisdiction_id']
        # bldgs_append = new_units_df.append(new_bldgs)

        # bldgs_append.reset_index(inplace=True)

    # sum units built over parcel id
    # need grouping here because if remaining units picked from parcels that had partial build
    # so some parcel ids in the dataframe twice


    # in case region wide targets less than zero
    if len(new_units_df) > 0:
        new_units_grouped = pd.DataFrame({'total_units_built': new_units_df.
                                            groupby(["parcel_id", "jurisdiction_id",
                                                     "capacity_base_yr", "residential_units",
                                                     "bldgs", "max_res_units"]).residential_units_sim_yr.sum()}).reset_index()
        new_units_grouped.set_index('parcel_id', inplace=True)
        new_units_grouped.index = new_units_grouped.index.astype(int)
        new_units_grouped.rename(columns={'total_units_built': 'residential_units_sim_yr'}, inplace=True)

        '''
            Join parcels with parcels that have new units on parcel_id (add net units column)
        '''

        db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
        mssql_engine = create_engine(db_connection_string)

        parcels = parcels.drop(['partial_build'], 1)
        new_units_grouped['partial_build'] = new_units_grouped.max_res_units - new_units_grouped.residential_units_sim_yr - new_units_grouped.residential_units
        parcels = parcels.join(new_units_grouped[['residential_units_sim_yr','partial_build']])
        parcels.residential_units_sim_yr = parcels.residential_units_sim_yr.fillna(0)
        parcels.partial_build = parcels.partial_build.fillna(0)
        parcels['residential_units'] = parcels['residential_units'] + parcels['residential_units_sim_yr']
        parcels = parcels.drop(['residential_units_sim_yr'], 1)
        orca.add_table("parcels", parcels)

        #This creates a new file of parcel info for each year
        # parcels['year'] = year
        #yname = '\\\\sandag.org\\home\\shared\\TEMP\\NOZ\\urbansim_lite_parcels_{}.csv'.format(year)
        #parcels.to_csv(yname)
        '''
        #This loop can write the all the parcels for each year as one (very large) .csv file.
        if year == 2020:
            parcels.to_csv('M:/TEMP/NOZ/urbansim_lite_parcels.csv')
        else:
            parcels.to_csv('M:/TEMP/NOZ/urbansim_lite_parcels.csv', mode='a', header=False)
    
        parcels.to_sql(name='urbansim_lite_output_parcels', con=mssql_engine, schema='urbansim', if_exists='replace',
                         index=True) #no run ID -> appending to database
        '''

        target_unit_sum = units_per_jurisdiction.loc[units_per_jurisdiction.jurisdiction!='all'].target_units_for_jur.sum()

        # count units for debugging
        dj = {'year': [year], 'jurisdiction': ['total'],
              'target_units_for_jur': [target_unit_sum],
              'target_units_for_region': [target_units],
              'units_picked': [units_per_jurisdiction.units_picked.sum()],
              'units_picked_remaining': [count_units_picked_remaining]}

        jur_df = pd.DataFrame(data=dj)
        units_per_jurisdiction = units_per_jurisdiction.append(jur_df)
        units_summary = units_summary.append(units_per_jurisdiction)

        uj = orca.get_table('uj').to_frame()
        uj = uj.append(units_summary)
        orca.add_table("uj", uj)

        new_units_grouped = new_units_grouped.reset_index()
        new_units_grouped['residential_units'] = new_units_grouped['residential_units_sim_yr']
        # temporarily assign building type id
        new_units_grouped['building_type_id'] = ''
        if year is not None:
            new_units_grouped["year_built"] = year

        print("Adding {:,} buildings with {:,} {}"
              .format(len(new_units_grouped),
                      int(new_units_grouped[supply_fname].sum()),
                      supply_fname))
        '''
            Merge old building with the new buildings
        '''

        all_buildings = dev.merge(buildings.to_frame(buildings.local_columns),
                                  new_units_grouped[buildings.local_columns])

        orca.add_table("buildings", all_buildings)
