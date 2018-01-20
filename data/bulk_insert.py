from sqlalchemy import create_engine
from pysandag.database import get_connection_string

db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

conn = mssql_engine.connect()
tran = conn.begin()
try:
    conn.execute(
        """
        BULK INSERT urbansim.urbansim.urbansim_lite_output_parcels
        FROM "\\\\sandag.org\\home\\shared\\TEMP\\NOZ\\urbansim_lite_parcels_2020.csv"
        WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a'
        )
        """
    )
    tran.commit()
except:
        tran.rollback()
        raise

conn.close()
