# app.py
import streamlit as st
import duckdb
import pandas as pd
import os

PARQUET_DIR = "output"
EXPORT_DIR = "exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

@st.cache_resource
def get_conn():
    return duckdb.connect(database=":memory:")

def refresh_view(con):
    con.execute(f"CREATE OR REPLACE VIEW parquet_view AS SELECT * FROM read_parquet('{PARQUET_DIR}/*.parquet')")

def list_tables(con):
    res = con.execute("SHOW TABLES").fetchdf()
    if res.empty:
        return []
    return res['name'].tolist()

def table_head(con, table, n=5):
    try:
        return con.execute(f"SELECT * FROM {table} LIMIT {n}").fetchdf()
    except Exception as e:
        return pd.DataFrame({"error":[str(e)]})

def drop_table(con, table):
    try:
        con.execute(f"DROP TABLE IF EXISTS {table}")
        return True, f"Table {table} supprimée."
    except Exception as e:
        return False, str(e)

def create_table_from_df(con, table_name, df):
    con.register("df_tmp_for_create", df)
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_tmp_for_create")
    try:
        con.unregister("df_tmp_for_create")
    except Exception:
        pass
    return True

def export_table_to_parquet(con, table_name, target_path):
    con.execute(f"COPY (SELECT * FROM {table_name}) TO '{target_path}' (FORMAT PARQUET)")
    return target_path

st.title("TP4 — Mini Data Lake local (DuckDB + Streamlit)")

con = get_conn()

# Sidebar
st.sidebar.header("Actions")
if st.sidebar.button("Refresh parquet view"):
    refresh_view(con)
    st.sidebar.success("Vue parquet_view rafraîchie.")

st.sidebar.subheader("Tables DuckDB")
tables = list_tables(con)
if "parquet_view" not in tables:
    try:
        refresh_view(con)
    except Exception as e:
        st.sidebar.error(f"Erreur création vue: {e}")
tables = list_tables(con)
st.sidebar.write(tables if tables else "Aucune table")

# Supprimer une table
st.sidebar.subheader("Supprimer une table")
table_to_drop = st.sidebar.selectbox("Choisir la table", options=(tables or ["-- Aucune --"]))
if st.sidebar.button("Supprimer la table"):
    if table_to_drop and table_to_drop != "-- Aucune --":
        ok, msg = drop_table(con, table_to_drop)
        if ok:
            st.sidebar.success(msg)
        else:
            st.sidebar.error(msg)
        tables = list_tables(con)

# Upload CSV -> créer table
st.sidebar.subheader("Uploader un CSV")
uploaded = st.sidebar.file_uploader("Fichier CSV", type=["csv"])
table_name_input = st.sidebar.text_input("Nom de la nouvelle table", value="uploaded_table")
if uploaded is not None:
    df_up = pd.read_csv(uploaded)
    st.sidebar.write(f"Fichier reçu: {uploaded.name} — {df_up.shape[0]} lignes")
    if st.sidebar.button("Créer la table depuis CSV"):
        try:
            create_table_from_df(con, table_name_input, df_up)
            st.sidebar.success(f"Table '{table_name_input}' créée.")
            tables = list_tables(con)
        except Exception as e:
            st.sidebar.error(str(e))

# Main
st.header("Explorer les tables")
selected = st.selectbox("Choisir une table à afficher", options=(tables or ["parquet_view"]))
if selected:
    df_preview = table_head(con, selected, n=5)
    st.subheader(f"Aperçu — {selected} (5 premières lignes)")
    st.dataframe(df_preview)

    # Filtres dynamiques
    st.subheader("Filtres dynamiques")
    try:
        cols = con.execute(f"PRAGMA table_info('{selected}')").fetchdf()['name'].tolist()
    except Exception:
        cols = df_preview.columns.tolist() if not df_preview.empty else []
    if cols:
        col_filter = st.selectbox("Choisir une colonne", options=cols)
        try:
            unique_vals = con.execute(f"SELECT DISTINCT {col_filter} FROM {selected} LIMIT 1000").fetchdf()[col_filter].tolist()
        except Exception:
            unique_vals = []
        val = st.selectbox("Filter value (facultatif)", options=(["-- aucun --"] + [str(x) for x in unique_vals]))
        if val and val != "-- aucun --":
            try:
                q = f"SELECT * FROM {selected} WHERE {col_filter} = '{val}' LIMIT 100"
                df_filtered = con.execute(q).fetchdf()
                st.write(f"Résultat filtre ({len(df_filtered)} lignes)")
                st.dataframe(df_filtered)
            except Exception as e:
                st.error(f"Erreur filtre: {e}")

    # Export parquet
    st.subheader("Exporter la table en Parquet")
    if st.button("Exporter en Parquet"):
        target = os.path.join(EXPORT_DIR, f"{selected}.parquet")
        try:
            export_table_to_parquet(con, selected, target)
            st.success(f"Exporté vers {target}")
            with open(target, "rb") as f:
                st.download_button("Télécharger le parquet", data=f, file_name=os.path.basename(target))
        except Exception as e:
            st.error(f"Erreur export: {e}")

    # Graphique simple (colonne 'value' si présente)
    st.subheader("Graphique simple")
    try:
        sample = con.execute(f"SELECT * FROM {selected} LIMIT 1000").fetchdf()
        if 'value' in sample.columns:
            st.line_chart(sample[['value']])
        else:
            st.write("Aucune colonne 'value' pour tracer un graphique simple.")
    except Exception as e:
        st.write(f"Impossible de tracer: {e}")

st.markdown("""
---
**Notes**
- Le dossier `parquet_data/` est lu dynamiquement via DuckDB.
- Pour tester localement : `streamlit run app.py`.
""")
