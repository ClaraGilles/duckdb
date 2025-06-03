FROM python:3.11-slim

# Installer DuckDB + dépendances utiles
RUN pip install duckdb pandas pyarrow


# # Copier ton script Python dans ce dossier
COPY create_duckdb.py app/.
COPY data/. app/data/.

# Créer un dossier pour ton code
WORKDIR /app

COPY . .
# Lancer le script (on est déjà dans /app grâce à WORKDIR)
CMD ["python", "create_duckdb.py"]

# ENTRYPOINT ["tail", "-f", "/dev/null"]