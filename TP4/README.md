## TP4 – Mini Data Lake Local avec DuckDB + Streamlit

### Objectifs

- Simuler un data lake local
- Utiliser DuckDB avec des fichiers CSV/Parquet partitionnés
- Créer une interface Streamlit pour gérer les tables

### Travaux

1. Générer un dataset CSV et y ajouter une colonne de partition (année, mois, etc.)
2. Sauvegarder les données en plusieurs fichiers Parquet (ex : 2024_data.parquet, 2025_data.parquet)
3. Lire dynamiquement ces fichiers avec DuckDB (read_parquet('parquet_data/\*.parquet'))
4. Créer une app Streamlit avec les fonctionnalités suivantes :
   - Visualiser les tables DuckDB
   - Supprimer une table
   - Uploader un CSV et créer une nouvelle table
   - Afficher les 5 premières lignes des tables
5. Dockeriser l'application

- Exporter une table en Parquet
- Ajouter des filtres dynamiques dans l’interface
- Afficher un graphique à partir d’une table