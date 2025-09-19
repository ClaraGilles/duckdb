import duckdb as dd

# Connexion à la base
con = dd.connect('output/covid.db')

def create_insert_table(file_name, table_name):
    
    con.sql(f'''
        CREATE TABLE IF NOT EXISTS {table_name} AS
            SELECT * FROM '{file_name}';
    ''')
    
# Exemple d’utilisation CSV
create_insert_table('data/country_wise_latest.csv', 'covid_table')

con.sql('''
    COPY covid_table TO 'output/country_wise_latest_parquet.parquet' (FORMAT 'parquet'); 
''')

# # Exemple d’utilisation Parquet
create_insert_table('output/country_wise_latest_parquet.parquet', 'table_covid_parquet')

