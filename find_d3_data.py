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

def read_and_save(file_name, query):
    print("Running Query for %s" % file_name)
    df = pd.read_sql_query(query, cnx)
    print("Number of rows: %d" % len(df))
    print("Writing to CSV File: %s" % file_name)
    df.to_csv("Query_Data/" + file_name)
    print("DONE")

if False:
    # Get Airports of Interest w/ geo info
    fn = 'US_Airport_geo.csv'
    query = """SELECT "Name", airports."IATA", 
                       "Longitude", "Latitude", "Altitude" 
                FROM airports
                JOIN primary_airports pa ON pa."IATA" = airports."IATA"
                WHERE "Country" = 'United States' AND "Enplanements" > 500000;"""
    read_and_save(fn, query)

if False:
    # Get US Weather Stations
    fn = 'US_Weather_Stations_geo.csv'
    query = """SELECT wa."StationID", "Latitude", "Longitude",
                      "Altitude_ft", "Name", "IATA"
               FROM weatherstations wa
               LEFT JOIN weatherairportlinks wal 
                    ON wal."StationID" = wa."StationID"
               WHERE "Latitude" < 49.3457868 AND
                     "Latitude" > 24.7433195 AND
                     "Longitude" < -66.9513812 AND
                     "Longitude" > -124.7844079;"""
    read_and_save(fn, query)

if True:
    # Get Weather Data
    fn = "US_Weather_Station_data.csv"
    query = """SELECT * FROM weather_all_2016
               JOIN
               (SELECT wa."StationID", "Name", "Latitude", "Longitude", "Altitude_ft", "IATA"
                FROM weatherstations wa
                LEFT JOIN weatherairportlinks wal 
                     ON wal."StationID" = wa."StationID"
                WHERE "Latitude" < 49.3457868 AND
                      "Latitude" > 24.7433195 AND
                      "Longitude" < -66.9513812 AND
                      "Longitude" > -124.7844079) AS subq
               ON subq."StationID" = weather_all_2016."StationID"
               ORDER BY "Name";"""
    read_and_save(fn, query)

if False:
    # Get Flight Performance (Daily Accumulated)
    fn = "US_Accumulated_Daily_Delay_Count.csv"
    query = """SELECT "FlightDate", "Carrier", "Origin",
                      COUNT(*) AS "Num_Flights", 
                      SUM("ArrDel15") AS "Num_ArrDelays", 
                      SUM("DepDel15") AS "Num_DepDelays"
               FROM ontimeperformance
               GROUP BY "FlightDate", "Carrier", "Origin";"""
    read_and_save(fn, query)

if False:
    # Get Flight Performance (Case by case)
    fn = "US_Flight_On_Time_Performance.csv"
    query = """SELECT "FlightDate", "Carrier", "Origin", "Dest",
                      "ArrDel15", "DepDel15", "WeatherDelay"
               FROM ontimeperformance otp
               LEFT JOIN primary_airports pa ON pa."IATA" = otp."Origin"
               WHERE pa."Enplanements" > 500000;"""
    read_and_save(fn, query)