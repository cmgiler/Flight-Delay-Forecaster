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
output_fn = 'Flight_Delays_DestinationWeather.csv'

query = """SELECT "FlightDate", "Carrier", "Origin", "Dest", "Obsv_Type",
				   AVG("Obsv_Value") AS "Obsv_Val_A",
				   MIN("Obsv_Value") AS "Obsv_Val_B",
				   AVG("Latitude") AS "Lat",
				   AVG("Longitude") AS "Long",
				   AVG("Altitude") AS "Alt",
				   COUNT(*) AS "Num_Flights", 
				   SUM("ArrDel15") AS "Num_ArrDelays", 
				   SUM("DepDel15") AS "Num_DepDelays"
			FROM (SELECT * FROM ontimeperformance otp
                  JOIN primary_airports pa ON pa."IATA" = otp."Origin"
                  WHERE "Enplanements" > 1000000) AS subq
			JOIN airports ON airports."IATA" = subq."Dest"
			JOIN weatherairportlinks ON weatherairportlinks."IATA" = subq."Dest"
			JOIN weather ON weather."StationID" = weatherairportlinks."StationID" AND
							weather."Date" = subq."FlightDate"
			GROUP BY "FlightDate", "Carrier", "Origin", "Dest", "Obsv_Type";"""

print("Running Query for Destination Weather")

df = pd.read_sql_query(query, cnx)

print("Number of rows: %d" % len(df))

print("Writing to CSV File: %s" % output_fn)

df.to_csv(output_fn)