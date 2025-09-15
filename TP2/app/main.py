from ETL.extract import fetch_data
from ETL.transform import clean_data
from ETL.load import load_into_duckdb
import logging

def run_pipeline():
    try:
        logging.info('Ca marche')
        print('ok')
        df = fetch_data()
        df_clean = clean_data(df)
        load_into_duckdb(df_clean)
    except Exception as e:
        logging.error(f"Erreur lors du pipeline : {e}")

if __name__ == "__main__":
    run_pipeline()
