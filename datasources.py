import orca
import pandas as pd
import bulk_insert


# The .h5 file to match run_id=109 is saved at M:\RES\estimates & forecast\SR14 Forecast\UrbanSim\urbansim_v109.h5
# The .h5 file to match run_id=390 is saved at M:\RES\estimates & forecast\SR14 Forecast\UrbanSim\urbansim_v390.h5
# 109 was used to create datasource_id 14, 390 was used to create datasource_id 17

# When using the M: drive saved files, the input versions recorded in [urbansim].[urbansim].[urbansim_lite_output_runs]
# may not actually correspond to the versions used to create the .h5 file. While we try to make them match for record
# keeping purposes, those saved inputs are not actually used in the active run. We may add a column to this table to
# reflect this, or somehow indicate this for record keeping purposes. However, the original version (ie. 390) will
# have accurate version ids recorded, since it was created with the regular .h5 methodology.
orca.add_injectable("store", pd.HDFStore('M:\\RES\\estimates & forecast\\SR14 Forecast\\UrbanSim\\urbansim_v390.h5', mode="r"))


@orca.table('parcels', cache=True)
def parcels(store):
    df = store['parcels']
    return df

# for subregional percentage controls
@orca.table('controls', cache=True)
def controls(store):
    df = store['controls']
    return df


@orca.table('households', cache=True)
def households(store):
    df = store['households']
    return df


@orca.table('hu_forecast', cache=True)
def hu_forecast(store):
    df = store['hu_forecast']
    return df


@orca.table('regional_controls', cache=True)
def regional_controls(store):
    df = store['regional_controls']
    return df


@orca.table('jurisdictions', cache=True)
def jurisdictions(store):
    df = store['jurisdictions']
    return df


@orca.table('devyear', cache=True)
def devyear(store):
    df = store['devyear']
    return df


@orca.table('scheduled_development', cache=True)
def sched_dev(store):
    df = store['scheduled_development']
    return df


@orca.table('negative_parcels', cache=True)
def negative_parcels(store):
    df = store['negative_parcels']
    return df


@orca.table('all_parcels', cache=True)
def all_parcels(store):
    df = store['all_parcels']
    return df


@orca.table('dev_lu_table', cache=True)
def dev_lu_table(store):
    df = store['dev_lu_table']
    return df


@orca.table('adu_allocation', cache=True)
def adu_allocation(store):
    df = store['adu_allocation']
    return df

@orca.table('adu_allocation2', cache=True)
def adu_allocation(store):
    df = store['adu_allocation2']
    return df
