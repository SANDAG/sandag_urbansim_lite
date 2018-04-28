import orca
import os
import pandas as pd


orca.add_injectable("store", pd.HDFStore('.\\data\\urbansim.h5', mode="r"))


@orca.table('parcels', cache=True)
def parcels(store):
    df = store['parcels']
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