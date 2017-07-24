import warnings

import orca
import pandas as pd
import os
from urbansim.utils import misc

warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)

orca.add_injectable("store", pd.HDFStore(os.path.join(misc.data_dir(), "urbansim.h5"), mode="r"))


@orca.table('buildings', cache=True)
def buildings(store):
    df = store['buildings']
    return df


@orca.table('households', cache=True)
def households(store):
    df = store['households']
    return df


@orca.table('parcels', cache=True)
def parcels(store):
    df = store['parcels']
    return df


