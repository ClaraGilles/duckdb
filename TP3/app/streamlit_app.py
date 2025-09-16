# import streamlit as st
# import duckdb
# import pandas as pd

# Connexion à DuckDB
# con = duckdb.connect('/data/duckdb/weather_data.db', read_only=True)

# st.title("Dashboard Météo")

# # Filtres interactifs
# cities = con.execute("SELECT DISTINCT city FROM weather_data").fetchall()
# cities = [c for c in cities]
# selected_city = st.selectbox("Choisir une ville", cities)

# # Filtrage par ville
# df = con.execute(
#     "SELECT * FROM weather_data WHERE city = ?", [selected_city]
# ).df()

# # KPIs
# st.metric("Nombre d'enregistrements", len(df))
# st.metric("Température moyenne", round(df["temperature"].mean(), 2))

# # Graphiques
# st.line_chart(df.set_index("timestamp")["temperature"])
# st.bar_chart(df.set_index("timestamp")["humidity"])

# # Export CSV
# csv = df.to_csv(index=False)
# st.download_button("Télécharger CSV", data=csv, file_name="filtered_data.csv")



# con.close()

import streamlit as st
import altair as alt
import duckdb
import pandas as pd

# -----------------------------
# CONFIG
# -----------------------------
CITIES = ['London', 'Paris', 'New York', 'Tokyo', 'Berlin']
DB_PATH = "/data/duckdb/weather_data.db"

st.set_page_config(
    page_title="Weather Dashboard",
    page_icon="🌦️",
    layout="wide",
)

# -----------------------------
# LOAD DATA
# -----------------------------
con = duckdb.connect(DB_PATH, read_only=True)

query = """
SELECT *
FROM weather_data
WHERE city IN ({cities})
""".format(cities=",".join(["'" + c + "'" for c in CITIES]))

df = con.execute(query).fetchdf()
con.close()

# Assurer que timestamp est en datetime
df["timestamp"] = pd.to_datetime(df["timestamp"])

# -----------------------------
# HEADER
# -----------------------------
st.title("🌦️ Weather Dashboard")

st.markdown("Données météo stockées dans DuckDB, pour les villes : **" + ", ".join(CITIES) + "**")

# -----------------------------
# FILTRES
# -----------------------------
col1, col2 = st.columns([2, 1])
with col1:
    selected_cities = st.multiselect("Choisir des villes :", CITIES, default=["Paris"])
with col2:
    years = df["timestamp"].dt.year.unique().tolist()
    selected_years = st.multiselect("Années :", years, default=years)

if not selected_cities or not selected_years:
    st.warning("Veuillez sélectionner au moins une ville et une année.")
    st.stop()

df = df[df["city"].isin(selected_cities)]
df = df[df["timestamp"].dt.year.isin(selected_years)]

# -----------------------------
# KPIs (Metrics)
# -----------------------------
st.subheader("Résumé météo")

latest = df.sort_values("timestamp").groupby("city").tail(1)

cols = st.columns(len(latest))
for i, (city, row) in enumerate(latest.set_index("city").iterrows()):
    with cols[i]:
        st.metric(
            f"{city} - Température",
            f"{row['temperature']:.1f} °C",
            help=f"Humidité: {row['humidity']}%, Pression: {row['pressure']} hPa, Vent: {row['wind_speed']} m/s"
        )

# -----------------------------
# GRAPHIQUES
# -----------------------------
st.subheader("Évolution de la température")

chart_temp = (
    alt.Chart(df)
    .mark_line(point=True)
    .encode(
        x=alt.X("timestamp:T", title="Date"),
        y=alt.Y("temperature:Q", title="Température (°C)"),
        color="city:N"
    )
    .properties(height=300)
)
st.altair_chart(chart_temp, use_container_width=True)

col1, col2 = st.columns(2)

with col1:
    st.markdown("### Répartition météo")
    chart_weather = (
        alt.Chart(df)
        .mark_arc()
        .encode(
            theta=alt.Theta("count()", stack=True),
            color=alt.Color("weather:N", legend=alt.Legend(title="Conditions météo")),
            tooltip=["weather", "count()"]
        )
    )
    st.altair_chart(chart_weather, use_container_width=True)

with col2:
    st.markdown("### Vent moyen")
    chart_wind = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("timestamp:T", title="Date"),
            y=alt.Y("mean(wind_speed):Q", title="Vent moyen (m/s)"),
            color="city:N"
        )
    )
    st.altair_chart(chart_wind, use_container_width=True)

st.subheader("Données brutes")
st.dataframe(df.sort_values("timestamp", ascending=False).head(200))
