from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import COLLECTIONS as cln
import html5lib
import datetime as dt
import pickle
from collections import Counter

cnx = create_engine('postgresql://%s:%s@localhost:%s/%s' % (cln.username, 
                                                            cln.password, 
                                                            cln.port,
                                                            cln.db_name))

output_fn = 'Flight_Delays_Paths.csv'

query = """SELECT "FlightDate", "Carrier", "Origin", "Dest",
				   COUNT(*) AS "Num_Flights", 
				   SUM("ArrDel15") AS "Num_ArrDelays", 
				   SUM("DepDel15") AS "Num_DepDelays"
           FROM ontimeperformance
           WHERE "FlightDate" >= '2016-01-01'
           GROUP BY "FlightDate", "Carrier", "Origin", "Dest";"""

print("Running Query for Delays by Flight Path")

df = pd.read_sql_query(query, cnx)

print("Number of rows: %d" % len(df))

print("Writing to CSV File: %s" % output_fn)

df.to_csv(output_fn)
