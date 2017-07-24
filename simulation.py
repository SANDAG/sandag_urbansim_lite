import models, utils
import orca
from sqlalchemy import create_engine
from pysandag.database import get_connection_string

orca.run([
    "feasibility",               # compute development feasibility
    "residential_developer"     # build residential buildings
     ], iter_vars=range(2016, 2051))

engine = create_engine(get_connection_string("data\config.yml", 'postgres_database'))
orca.get_table('buildings').to_frame().to_sql(name='buildings', con=engine, schema='urbansim_lite', if_exists='replace', index=True)