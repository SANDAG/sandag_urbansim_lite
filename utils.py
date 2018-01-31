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
    cap_results= pd.DataFrame()
    orca.add_table('sr14cap_out',cap_results)


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
    feasible_parcels = feasible_parcels.loc[feasible_parcels['phase_yr_ctrl'] < year]
    # remove scheduled developments from feasibility table
    feasible_parcels = feasible_parcels.loc[feasible_parcels['site_id'].isnull()].copy()
    orca.add_table("feasibility", feasible_parcels)


def parcel_picker(parcels_to_choose, target_number_of_units, name_of_geo, year_simulation):
    parcels_picked = pd.DataFrame()
    if target_number_of_units > 0:
        if parcels_to_choose.remaining_capacity.sum() < target_number_of_units:
            print("WARNING THERE WERE NOT ENOUGH UNITS TO MATCH DEMAND FOR ", name_of_geo, "IN YEAR ", year_simulation)
            if len(parcels_to_choose):
                parcels_picked= parcels_to_choose
                parcels_picked['residential_units_sim_yr'] = parcels_picked['remaining_capacity']
                parcels_picked.drop(['site_id', 'remaining_capacity'], axis=1, inplace=True)
        else:
            shuffled_parcels = parcels_to_choose.sample(frac=1, random_state=50).reset_index(drop=False)
            previously_picked = shuffled_parcels.loc[shuffled_parcels.partial_build > 0]
            shuffled_parcels = shuffled_parcels[~shuffled_parcels['parcel_id'].isin(previously_picked.parcel_id.values.tolist())]
            partial_then_random = pd.concat([previously_picked, shuffled_parcels])
            one_row_per_unit = partial_then_random.reindex(partial_then_random.index.repeat(partial_then_random.remaining_capacity)).reset_index(drop=True)
            one_row_per_unit_picked = one_row_per_unit.head(target_number_of_units)
            parcels_picked= pd.DataFrame({'residential_units_sim_yr': one_row_per_unit_picked.
                                         groupby(["parcel_id", "jurisdiction_id","capacity_base_yr",
                                                  "residential_units","bldgs", "max_res_units"])
                                         .size()}).reset_index()
            parcels_picked.set_index('parcel_id', inplace=True)
    return parcels_picked


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
         (households)
    buildings : DataFrame Wrapper
        Used to compute the current supply of units/floorspace in the area
    supply_fname : string
        Identifies the column in buildings which indicates the supply of
        units/floorspace ("residential units")
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

    print("\n Agents are households. Agent spaces are dwelling units")
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
    sr14cap = pd.DataFrame()
    feasible_parcels_df['remaining_capacity'] = (feasible_parcels_df.max_res_units - feasible_parcels_df.residential_units)
    feasible_parcels_df.remaining_capacity = feasible_parcels_df.remaining_capacity.astype(int)

    for luz in list(range(1,230)):
        target_units_for_geo = subregional_targets.loc[subregional_targets['geo_id']==luz].targets.values[0]
        geo_name = jurs.loc[jurs.jurisdiction_id == jur].name.values[0]
        print("Jurisdiction %d %s target units: %d" % (jur,geo_name,target_units_for_geo))
        parcels_in_geo = feasible_parcels_df.loc[feasible_parcels_df['jurisdiction_id'] == jur].copy()
        chosen = parcel_picker(parcels_in_geo, target_units_for_geo, geo_name, year)
        sr14cap = sr14cap.append(chosen)

    if len(sr14cap):
        remaining_units = target_units - sr14cap.residential_units_sim_yr.sum()
    else: remaining_units = target_units

    if remaining_units > 0:
        feasible_parcels_df = feasible_parcels_df.join(sr14cap[['residential_units_sim_yr']])
        feasible_parcels_df.residential_units_sim_yr = feasible_parcels_df.residential_units_sim_yr.fillna(0)
        feasible_parcels_df['remaining_capacity'] = feasible_parcels_df.max_res_units - feasible_parcels_df.residential_units\
                                                    - feasible_parcels_df.residential_units_sim_yr
        feasible_parcels_df['remaining_capacity'] = feasible_parcels_df['remaining_capacity'].astype(int)
        feasible_parcels_df= feasible_parcels_df.loc[feasible_parcels_df.remaining_capacity > 0]
        feasible_parcels_df['partial_build'] = feasible_parcels_df.residential_units_sim_yr
        chosen = parcel_picker(feasible_parcels_df, remaining_units, "all", year)
        sr14cap = sr14cap.append(chosen)


    if len(sr14cap) > 0:
        # group by parcel id again if same parcel was picked
        sr14cap = pd.DataFrame({'residential_units_sim_yr': sr14cap.
                                            groupby(["parcel_id", "jurisdiction_id",
                                                     "capacity_base_yr", "residential_units",
                                                     "bldgs", "max_res_units"]).residential_units_sim_yr.sum()}).reset_index()
        sr14cap.set_index('parcel_id', inplace=True)
        sr14cap['partial_build'] = sr14cap.max_res_units - sr14cap.residential_units_sim_yr - sr14cap.residential_units
        parcels = parcels.drop(['partial_build'], 1)
        parcels = parcels.join(sr14cap[['residential_units_sim_yr','partial_build']])
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
        db_connection_string = get_connection_string('data\config.yml', 'mssql_db')
        mssql_engine = create_engine(db_connection_string)
        parcels.to_sql(name='urbansim_lite_output_parcels', con=mssql_engine, schema='urbansim', if_exists='replace',
                         index=True) #no run ID -> appending to database
        '''


        sr14cap = sr14cap.reset_index()
        sr14cap['residential_units'] = sr14cap['residential_units_sim_yr']
        # temporarily assign building type id
        sr14cap['building_type_id'] = ''
        if year is not None:
            sr14cap["year_built"] = year

        print("Adding {:,} buildings with {:,} {}"
              .format(len(sr14cap),
                      int(sr14cap[supply_fname].sum()),
                      supply_fname))
        '''
            Merge old building with the new buildings
        '''

        all_buildings = dev.merge(buildings.to_frame(buildings.local_columns),
                                  sr14cap[buildings.local_columns])

        orca.add_table("buildings", all_buildings)
