# FROM python:3.11-slim

# # Installer DuckDB + dépendances utiles
# RUN pip install duckdb pandas pyarrow requests

# # # Créer un dossier pour ton code
# # WORKDIR /app

# # Copier ton script Python dans ce dossier
# COPY . .

# # Lancer le script (on est déjà dans /app grâce à WORKDIR)
# CMD ["sh", "-c", "python app/main.py && sleep infinity"]

FROM python:3.11-slim

# Installer DuckDB + dépendances utiles + cron
RUN apt-get update && apt-get install -y cron \
    && pip install duckdb pandas pyarrow requests

# Créer le fichier de log et donner les droits
RUN mkdir -p /outputs \
    && touch /outputs/etl.log \
    && chmod 664 /outputs/etl.log

# # Créer un dossier pour ton code
# WORKDIR /app

# Copier ton script Python et les fichiers nécessaires
COPY . .

# Copier le fichier cron dans le container
COPY etl-cron /etc/cron.d/etl-cron

# Permissions du fichier cron
RUN chmod 0644 /etc/cron.d/etl-cron

# Appliquer le cron
RUN crontab /etc/cron.d/etl-cron

# Lancer cron au premier plan (Docker-friendly)
CMD ["cron", "-f"]
