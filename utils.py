from __future__ import print_function

import numpy as np
import orca
from urbansim.developer import developer


def profit_to_prob_function(df1, var_list=['distance_to_coast']):

    """
    Execute development feasibility on all parcels

    Parameters
    ----------
    df : DataFrame Wrapper
        The data frame wrapper for the parcel data
    var_list : array
        List of variables to be used to determine the probability of being picked
    Returns
    -------
    Adds a series with profit function
    """
    df1['random'] = np.random.normal(.5, .05, df1.shape[0])

    for x in var_list:
            df1[x + '_normal'] = ((df1[x].max() - df1[x]) / (df1[x].max() - df1[x].min()))

            df1['random'] = (df1['random'] * df1[x + '_normal'])

            df1.drop(x + '_normal', axis=1, inplace=True)

    df1['random_prob'] = (df1.random / df1.random.sum())
#    df1.drop('random', axis=1, inplace=True)
 #   return df1['random_prob']
    print ('the length is: ',len(df1))
    return df1

def run_feasibility(parcels):
    """
    Execute development feasibility on all parcels

    Parameters
    ----------
    parcels : DataFrame Wrapper
        The data frame wrapper for the parcel data

    Returns
    -------
    Adds a table called feasibility to the sim object (returns nothing)
    """

    print("Computing feasibility")
    parcels = parcels.to_frame()
    feasible_parcels = parcels.loc[parcels['total_cap'] > parcels['residential_units']]
    print (feasible_parcels)
    orca.add_table("feasibility", feasible_parcels)


def run_developer(forms, parcels, agents, buildings, supply_fname,
                  total_units, feasibility, year=None,
                  target_vacancy=.1, form_to_btype_callback=None,
                  add_more_columns_callback=None, max_parcel_size=200000,
                  residential=True, bldg_sqft_per_job=400.0):
    """
    Run the developer model to pick and build buildings

    Parameters
    ----------
    forms : string or list of strings
        Passed directly dev.pick
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
    form_to_btype_callback : function
        Will be used to convert the 'forms' in the pro forma to
        'building_type_id' in the larger model
    add_more_columns_callback : function
        Takes a dataframe and returns a dataframe - is used to make custom
        modifications to the new buildings that get added
    max_parcel_size : float
        Passed directly to dev.pick - max parcel size to consider
    residential : boolean
        Passed directly to dev.pick - switches between adding/computing
        residential_units and job_spaces
    bldg_sqft_per_job : float
        Passed directly to dev.pick - specified the multiplier between
        floor spaces and job spaces for this form (does not vary by parcel
        as ave_unit_size does)

    Returns
    -------
    Writes the result back to the buildings table (returns nothing)
    """

    dev = developer.Developer(feasibility.to_frame())

    target_units = dev.\
        compute_units_to_build(agents.to_frame().hh.get_value(year),
                               buildings[supply_fname].sum(),
                               target_vacancy)

    print("{:,} feasible buildings before running developer"
          .format(len(dev.feasibility)))
    df = feasibility.to_frame()
    p = profit_to_prob_function(df, var_list=[]) #random
#    p = profit_to_prob_function(df) #distance to coast
    p1 = p['random_prob']

    '''
        Do not pick or develop if there are no feasible parcels
    '''
    if len(dev.feasibility) == 0:
        print ('0 feasible buildings')
        return

    '''
        Pick parcels to for new buildings
    '''

#    choices = np.random.choice(df.index.values, size=min(len(df.index), target_units),
 #                              replace=False, p=p.tolist())
    choices = np.random.choice(p.index.values, size=min(len(p.index), target_units),
                               replace=False, p=p1.tolist())
    print (choices)
    df['net_units'] = (df.total_cap - df.residential_units)
    tot_units = df.net_units.loc[choices].values.cumsum()
    ind = int(np.searchsorted(tot_units, target_units, side="left")) + 1
    build_idx = choices[:ind]
    new_buildings = df.loc[build_idx]
    new_buildings.index.name = "parcel_id"

    '''
       Add new buildings residential units to the parcels by
       1. Joining parcels and buildings by parcel id
       2. Adding net_units (new units) to residential parcels
    '''

    new_buildings["residential_units"] = (new_buildings["net_units"])
#   new_buildings = new_buildings.drop(['random_prob'], 1)

    parcels = parcels.to_frame()
    parcels = parcels.join(new_buildings[['net_units']])

    parcels = parcels.fillna(0)
    parcels['residential_units'] = parcels['residential_units'] + parcels['net_units']
    parcels = parcels.drop(['net_units'], 1)
    orca.add_table("parcels", parcels)

    new_buildings = new_buildings.reset_index()

    if year is not None:
        new_buildings["year_built"] = year

    print("Adding {:,} buildings with {:,} {}"
          .format(len(new_buildings),
                  int(new_buildings[supply_fname].sum()),
                  supply_fname))

    '''
        Displays number of feasible buildings after dropping parcels where new buildings were built
    '''

    print("{:,} feasible buildings after running developer"
          .format(len(dev.feasibility.drop(build_idx))))
    print (new_buildings.head())

    '''
        Merge old building with the new buildings
    '''
    all_buildings = dev.merge(buildings.to_frame(buildings.local_columns),
                              new_buildings[buildings.local_columns])

    orca.add_table("buildings", all_buildings)
