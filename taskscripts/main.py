import pandas as pd
import numpy as np
import urllib.request
import time
from config import Database_user, Database_password, Host, Database_name , Path

from sqlalchemy import create_engine

# Process start time
process_start = time.time()

# connect to SQLite Database
# nyc_database = create_engine(
#     'sqlite:///C:\\Users\\rahvakul\\Downloads\\SQLite\\sqlite-tools-win32-x86-3360000\\nyc_database.db')

# Establishing connection to mysql database
# engine = create_engine('mysql://sql11422063:xYuWgTvXrN@sql11.freesqldatabase.com/sql11422063')

db_string = "mysql://{}:{}@{}/{}".format(Database_user, Database_password, Host, Database_name)
engine = create_engine(db_string)
print(engine)
engine.connect()
print("Connected to MySQL DB")

engine.execute("USE nyc_taxi;")
# creates the required table if not exits in database
engine.execute("CREATE TABLE IF NOT EXISTS nyc_taxi ( VendorID int NULL);")
engine.execute("CREATE TABLE IF NOT EXISTS nyc_taxi_history ( year_mon varchar(30) NULL,pickup varchar(30) NULL,"
               "drop_off varchar(30) NULL,ranks int NULL,CONSTRAINT nyc_taxi_hist_unique UNIQUE (year_mon, pickup, "
               "drop_off, ranks));")

# Download the Trip Record Data
for month in range(6,12):
    urllib.request.urlretrieve("https://s3.amazonaws.com/nyc-tlc/trip+data/"+ \
                               "yellow_tripdata_2019-{0:0=2d}.csv".format(month),
                               "nyc-2019-{0:0=2d}.csv".format(month))
#
# Download the location Data
# urllib.request.urlretrieve("https://s3.amazonaws.com/nyc-tlc/misc/taxi_zones.zip", "taxi_zones.zip")
# with zipfile.ZipFile("taxi_zones.zip","r") as zip_ref:
#     zip_ref.extractall("./shape")

#  Download the lookup file Data
urllib.request.urlretrieve("https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv", "taxi_zone_lookup.csv")

# Ingest the data to mysql tables for the files downloaded
# df = pd.read_csv("C:\\Users\\shyam sunder\\PycharmProjects\\Crosslend\\nyc-2019-01.csv")
# print(df)
# df.to_sql('nyc_taxi', con=engine, schema='nyc_taxi', index=False, if_exists='replace' )

df = pd.read_csv("C:\\Users\\shyam sunder\\PycharmProjects\\Crosslend\\taxi_zone_lookup.csv")
print(df)
df.to_sql('nyc_taxi_lookup', con=engine, schema='nyc_taxi', if_exists='replace')

process_time = time.time() - process_start
mins = int(process_time / 60)
secs = int(process_time % 60)

print(
    'Total time after file downlaoded: {mins} minutes {secs} seconds'
        .format(mins=mins, secs=secs)
)

j, chunksize = 1, 100000

for month in range(6,12):
    fp = "C:\\Users\\shyam sunder\\PycharmProjects\\Crosslend\\nyc-2019-{0:0=2d}.csv".format(month)
    for df in pd.read_csv(fp, chunksize=chunksize, iterator=True):
        df = df.rename(columns={c: c.replace(' ', '_') for c in df.columns})
        df['pickup_hour'] = [x[11:13] for x in df['tpep_pickup_datetime']]
        df['dropoff_hour'] = [x[11:13] for x in df['tpep_dropoff_datetime']]
        df.index += j
        df.to_sql('nyc_taxi', con=engine, schema='nyc_taxi', index=False, if_exists='replace' )
        j = df.index[-1] + 1
# del df
print("data got ingeted to mysql tables")

# Task 2-1
engine.execute("CREATE OR REPLACE VIEW nyc_taxi_pop_dest_pickups AS Select rank() over (order by "
               "ttlpassengerscount desc) as rnk,x.dt, x.PULocationName, x.DOLocationName from ( select date_format("
               "tpep_pickup_datetime, '%Y-%m') as dt,count(a.passenger_count) as ttlpassengerscount,b.zone as "
               "PULocationName,c.zone as DOLocationName from nyc_taxi.nyc_taxi a left JOIN nyc_taxi.nyc_taxi_lookup b "
               "ON a.PULocationID = b.LocationID left JOIN nyc_taxi.nyc_taxi_lookup c ON a.DOLocationID = "
               "c.LocationID group by date_format(tpep_pickup_datetime, '%Y-%m'),b.zone,c.zone) as x ;")

# Task 2-2
engine.execute("CREATE OR REPLACE VIEW nyc_taxi_pop_boroughs AS Select rank() over (order by "
               "ttlpassengerscount desc) as rnk,x.dt, x.PUBorough  , x.DOBorough from ( select date_format("
               "tpep_pickup_datetime, '%Y-%m') as dt, count(a.passenger_count) as ttlpassengerscount, b.Borough as "
               "PUBorough  , c.Borough as DOBorough from nyc_taxi.nyc_taxi a left JOIN nyc_taxi.nyc_taxi_lookup b ON "
               "a.PULocationID = b.LocationID left JOIN nyc_taxi.nyc_taxi_lookup c ON a.DOLocationID = c.LocationID "
               "WHERE b.Borough is not null and c.Borough is not null group by b.Borough,c.Borough) as x;")

# Task 3
engine.execute("INSERT IGNORE INTO nyc_taxi_history(year_mon,pickup,drop_off,ranks) select * from ( Select "
               "x.dt, x.PULocationName as pickup, x.DOLocationName as drop_off, rank() over (order by "
               "ttlpassengerscount desc) as rnk from ( select date_format(tpep_pickup_datetime, '%Y-%m') as dt,"
               "count(a.passenger_count) as ttlpassengerscount,b.zone as PULocationName,c.zone as DOLocationName from "
               "nyc_taxi.nyc_taxi a left JOIN nyc_taxi.nyc_taxi_lookup b ON a.PULocationID = b.LocationID left JOIN "
               "nyc_taxi.nyc_taxi_lookup c ON a.DOLocationID = c.LocationID group by date_format("
               "tpep_pickup_datetime, '%Y-%m'),b.zone,c.zone) as x ) as y;")

# Task 4
engine.execute("DROP TABLE IF EXISTS nyc_taxi_curr;")
engine.execute("CREATE TABLE nyc_taxi_curr AS select pickup,drop_off,rnk from ( Select x.dt, "
               "x.PULocationName as pickup, x.DOLocationName as drop_off, rank() over (order by ttlpassengerscount "
               "desc) as rnk from ( select date_format(tpep_pickup_datetime, '%Y-%m') as dt,count(a.passenger_count) "
               "as ttlpassengerscount,b.zone as PULocationName,c.zone as DOLocationName from nyc_taxi.nyc_taxi a left "
               "JOIN nyc_taxi.nyc_taxi_lookup b ON a.PULocationID = b.LocationID left JOIN nyc_taxi.nyc_taxi_lookup c "
               "ON a.DOLocationID = c.LocationID WHERE YEAR(tpep_pickup_datetime) = YEAR(curdate())-2 group by "
               "date_format(tpep_pickup_datetime, '%Y-%m'),b.zone,c.zone) as x ) as y;")

print(
    'Total time consumed: {mins} minutes {secs} seconds'
        .format(mins=mins, secs=secs)
)