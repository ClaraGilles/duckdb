import requests
import pandas as pd
import json
import os

API_KEY = '9565b8d5bdb14847a2722101ca3eecdf' 
CITY = 'London'  
URL = f'http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric'

# Dossier où tu veux sauvegarder les données brutes
RAW_DATA_FOLDER = '../data/raw/'

def fetch_data():
    """Télécharge les données météo depuis l'API OpenWeatherMap et les retourne sous forme de DataFrame"""
    try:
        # Appel à l'API
        response = requests.get(URL)
        response.raise_for_status()  # Vérifie si la requête a réussi
        data = response.json()

        # Enregistrer le fichier JSON dans le dossier raw
        json_filename = os.path.join(RAW_DATA_FOLDER, f'{CITY}_weather_data.json')
        with open(json_filename, 'w') as json_file:
            json.dump(data, json_file, indent=4)  # Enregistre les données JSON dans le fichier

        print(f"Données JSON enregistrées dans {json_filename}")

        # Extraction des données nécessaires et transformation en DataFrame
        weather_data = {
            'city': CITY,
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'weather': data['weather'][0]['description'],
            'wind_speed': data['wind']['speed'],
            'timestamp': pd.to_datetime(data['dt'], unit='s')
        }

        # Conversion en DataFrame pandas
        df = pd.DataFrame([weather_data])
        return df
    
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors du téléchargement des données: {e}")
        return pd.DataFrame()  # Retourne un DataFrame vide en cas d'erreur