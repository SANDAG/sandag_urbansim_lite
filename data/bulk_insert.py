import pandas as pd
from sqlalchemy import create_engine
from pysandag.database import get_connection_string


db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

create_table_sql = '''
USE [urbansim]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [urbansim].[sr14_residential_parcel_results](
	[scenario] [tinyint] NOT NULL,
	[increment] [int] NOT NULL,
	[parcel_id] [int] NOT NULL,
	[year] [int] NOT NULL,
	[jur] [smallint] NOT NULL,
	[jur_reported] [smallint] NOT NULL,
	[cpa] [int] NULL,
	[mgra] [int] NOT NULL,
	[luz] [smallint] NOT NULL,
	[taz] [int] NULL,
	[site_id] [smallint] NULL,
	[lu] [smallint] NULL,
	[hs] [int] NOT NULL,
	[chg_hs] [int] NOT NULL,
	[cap_hs] [int] NOT NULL,
	[source] [smallint] NOT NULL,
	[phase] [int] NULL
 CONSTRAINT [PK_sr14_residential_parcel_yearly] PRIMARY KEY CLUSTERED 
(
	[scenario] ASC,
	[year] ASC,
	[parcel_id] ASC,
	[source] ASC
))WITH (DATA_COMPRESSION = page)
GO
'''


conn = mssql_engine.connect()
tran = conn.begin()
#check out batch size
try:
    conn.execute(
        """
        BULK INSERT urbansim.urbansim.residential_control
        FROM "\\\\sandag.org\\home\\shared\\TEMP\\NOZ\\urbansim_lite_parcels.csv"
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
