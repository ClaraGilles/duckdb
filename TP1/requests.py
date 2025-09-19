import duckdb as dd

# Connexion à la base
con = dd.connect('output/covid.db')

def run_queries(table_name, file_name):
    """Exécute les requêtes avec profiling activé uniquement pendant les requêtes."""

    # Activer le profiling
    con.execute("PRAGMA profiling_mode='detailed'")
    con.execute(f"PRAGMA profiling_output='output/{file_name}_profiling.txt'")
    con.execute("PRAGMA enable_profiling")

    print("\n🔟 Top 10 des pays avec le plus de cas :")
    print(con.sql(f"""
        SELECT "Country/Region", Confirmed
        FROM {table_name}
        ORDER BY Confirmed DESC
        LIMIT 10;
    """))

    print("\n📊 Moyenne globale de l'augmentation hebdomadaire (%):")
    try:
        print(con.sql(f"""
            SELECT AVG("1 week % increase") AS avg_weekly_growth
            FROM {table_name};
        """))
    except Exception:
        print("⚠️ Colonne '1 week % increase' manquante — requête ignorée.")


    print("\n🌍 Agrégation par région :")
    print(con.sql(f"""
        SELECT "WHO Region",
               SUM(Confirmed) AS total_confirmed,
               SUM(Deaths) AS total_deaths,
               SUM(Recovered) AS total_recovered
        FROM {table_name}
        GROUP BY "WHO Region"
        ORDER BY total_confirmed DESC;
    """))

    # Désactiver le profiling
    con.execute("PRAGMA disable_profiling;")


run_queries('covid_table', 'covid_table')
run_queries("read_csv_auto('data/country_wise_latest.csv')", 'csv_data')
run_queries("read_parquet('output/country_wise_latest_parquet.parquet')", 'parquet_data')