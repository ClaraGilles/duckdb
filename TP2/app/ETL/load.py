import duckdb as dd
import pandas as pd
# Dossier où tu veux sauvegarder les données brutes

def load_into_duckdb(df):
    print(df)
    """Charge les données nettoyées dans DuckDB"""
    if df.empty:
        print("Aucune donnée à charger dans DuckDB.")
        return
   
    # Connexion à DuckDB, crée une base persistante si nécessaire
    con = dd.connect('../outputs/weather_data.db')  # Ou un chemin valide pour stocker la DB persistante
  
    # Créer une table si elle n'existe pas
    con.execute('''
    CREATE TABLE IF NOT EXISTS weather_data (
        city VARCHAR,
        temperature DOUBLE,
        humidity DOUBLE,
        pressure DOUBLE,
        weather VARCHAR,
        wind_speed DOUBLE,
        timestamp TIMESTAMP
    );
    ''')

    # Insérer les données nettoyées dans la table
    con.executemany('''
    INSERT INTO weather_data (city, temperature, humidity, pressure, weather, wind_speed, timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?);
    ''', df.values.tolist())

    print(f"{len(df)} enregistrements chargés dans DuckDB.")

    # Optionnel: Sauvegarde en Parquet
    df.to_parquet('../data/clean/weather_data.parquet')

    con.close()
