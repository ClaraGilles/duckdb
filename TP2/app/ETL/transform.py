import pandas as pd

def clean_data(df):
    """Nettoie les données : suppression des colonnes inutiles et gestion des valeurs manquantes"""
    if df.empty:
        return df

    # Suppression des colonnes inutiles (ex: si tu veux garder uniquement certaines colonnes)
    df_clean = df[['city', 'temperature', 'humidity', 'pressure', 'weather', 'wind_speed', 'timestamp']]

    # Remplacer les valeurs manquantes (NaN) par une valeur par défaut ou suppression
    df_clean.fillna({
        'temperature': df_clean['temperature'].mean(),  # Remplacer NaN par la moyenne de température
        'humidity': df_clean['humidity'].mean(),  # Remplacer NaN par la moyenne d'humidité
        'pressure': df_clean['pressure'].mean(),  # Remplacer NaN par la moyenne de pression
        'weather': 'Unknown',  # Remplacer NaN par une valeur par défaut
        'wind_speed': df_clean['wind_speed'].mean(),
        'timestamp': pd.to_datetime('now')  # Utiliser l'heure actuelle si la date est manquante
    }, inplace=True)

    # Supprimer les lignes contenant des NaN si nécessaire
    df_clean.dropna(inplace=True)

    return df_clean