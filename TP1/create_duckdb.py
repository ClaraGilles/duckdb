import duckdb as dd

# Connexion à la base
con = dd.connect('../outputs/covid.db')

def create_insert_table(file_name, table_name):
    
    con.sql(f'''
        CREATE TABLE {table_name} AS
            SELECT * FROM '{file_name}';
    ''')
    
    # print(con.sql(f'''
    #     SELECT * FROM {table_name} LIMIT 5
    # '''))
    
# Exemple d’utilisation CSV
create_insert_table('data/country_wise_latest.csv', 'covid_table')

con.sql('''
    COPY covid_table TO '../outputs/country_wise_latest_parquet.parquet' (FORMAT 'parquet'); 
''')

# # Exemple d’utilisation Parquet
# create_insert_table('../outputs/country_wise_latest_parquet.parquet', 'table_covid_parquet')

