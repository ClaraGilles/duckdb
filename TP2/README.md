## TP2 – Pipeline d’Ingestion de Données avec Docker + DuckDB

### Objectifs

- Construire un mini-pipeline ETL dans Docker
- Automatiser ingestion -> transformation -> stockage

### Énoncé

1. Utiliser un script Python dans Docker qui :
   - Télécharge un fichier JSON/CSV d'une API (ex : OpenWeatherMap, OpenFoodFacts)
   - Nettoie les données (colonnes inutiles, valeurs nulles)
   - Charge dans DuckDB
2. Créer une base DuckDB persistante via volume Docker.
3. Mettre en place un cron à l'intérieur du container (ou via docker-compose).

- Sauvegarder les données propres en Parquet
- Journaliser les erreurs dans un fichier log
