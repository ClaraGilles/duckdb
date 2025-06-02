import duckdb as dd

con = dd.connect('taxi.db')

con.execute('''
CREATE OR REPLACE TABLE taxi_trips (
    VendorID INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance DOUBLE,
    RatecodeID INTEGER,
    store_and_fwd_flag VARCHAR,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type INTEGER,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE
);
''')

con.sql('''
    INSERT INTO taxi_trips (
        VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
        trip_distance, RatecodeID, store_and_fwd_flag, PULocationID,
        DOLocationID, payment_type, fare_amount, extra, mta_tax,
        tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge
    )
    (SELECT 
        VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
        trip_distance, RatecodeID, store_and_fwd_flag, PULocationID,
        DOLocationID, payment_type, fare_amount, extra, mta_tax,
        tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge
    FROM "data/yellow_tripdata_2019-01.csv")
''')

print(con.sql('SHOW ALL TABLES'))
print(con.sql('SELECT * FROM taxi_trips LIMIT 5;'))