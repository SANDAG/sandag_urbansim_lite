import orca
import os
import pandas as pd

from urbansim.utils import misc


orca.add_injectable("store", pd.HDFStore(os.path.join(misc.data_dir(), "urbansim.h5"), mode="r"))


@orca.table('parcels', cache=True)
def parcels(store):
    df = store['parcels']
    return df


@orca.table('households', cache=True)
def households(store):
    df = store['households']
    return df


@orca.table('buildings', cache=True)
def buildings(store):
    df = store['buildings']
    return df


@orca.table('regional_controls', cache=True)
def regional_controls(store):
    df = store['regional_controls']
    return df


@orca.table('jurisdictions', cache=True)
def jurisdictions(store):
    df = store['jurisdictions']
    return df
