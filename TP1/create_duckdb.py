import duckdb as dd

# Connexion à la base
con = dd.connect('../outputs/taxi.db')

def create_insert_table(file_name, table_name):
    
    if file_name == "data/taxi_data_csv.csv":
        read_type = "read_csv_auto"
    else:
        read_type = "read_parquet"
    
    con.execute("PRAGMA profiling_mode='detailed'")
    con.execute(f"PRAGMA profiling_output='../outputs/{table_name}.txt'")
    con.execute("PRAGMA enable_profiling")
    
    con.execute(f'''
        CREATE OR REPLACE TABLE {table_name} (
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

    con.sql(f'''
        INSERT INTO {table_name} (
            VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
            trip_distance, RatecodeID, store_and_fwd_flag, PULocationID,
            DOLocationID, payment_type, fare_amount, extra, mta_tax,
            tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge
        )
        SELECT 
            VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
            trip_distance, RatecodeID, store_and_fwd_flag, PULocationID,
            DOLocationID, payment_type, fare_amount, extra, mta_tax,
            tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge
        FROM {read_type}('{file_name}')
    ''')

    print(con.sql(f'''
        SELECT * FROM {table_name} LIMIT 5
    '''))
    
# Exemple d’utilisation CSV
create_insert_table('data/taxi_data_csv.csv', 'table_taxi_trips_csv')

# Export d'une des tables
con.sql('''
    COPY table_taxi_trips_csv TO 'data/taxi_data_parquet.parquet' (FORMAT 'parquet'); 
''')

con.sql('''
    COPY table_taxi_trips_csv TO '../outputs/taxi_data_parquet.parquet' (FORMAT 'parquet'); 
''')

# Exemple d’utilisation Parquet
create_insert_table('data/taxi_data_parquet.parquet', 'table_taxi_trips_parquet')

