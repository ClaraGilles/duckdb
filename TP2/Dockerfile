FROM python:3.11-slim

# Installer DuckDB + dépendances utiles
RUN pip install duckdb pandas pyarrow requests

# # Créer un dossier pour ton code
# WORKDIR /app

# Copier ton script Python dans ce dossier
COPY . .

# Lancer le script (on est déjà dans /app grâce à WORKDIR)
CMD ["sh", "-c", "python app/main.py && sleep infinity"]