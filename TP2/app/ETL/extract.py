import requests
import pandas as pd
import json
import os

API_KEY = '9565b8d5bdb14847a2722101ca3eecdf' 
CITIES = ['London', 'Paris', 'New York', 'Tokyo', 'Berlin']  # Liste des villes
BASE_URL = 'http://api.openweathermap.org/data/2.5/weather'

# Dossier où sauvegarder le JSON brut
RAW_DATA_FOLDER = '../data/raw/'
os.makedirs(RAW_DATA_FOLDER, exist_ok=True)

def fetch_data():
    """Télécharge les données météo pour plusieurs villes et sauvegarde dans un JSON unique"""
    all_data = []

    for city in CITIES:
        try:
            url = f'{BASE_URL}?q={city}&appid={API_KEY}&units=metric'
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            weather_data = {
                'city': city,
                'temperature': data['main']['temp'],
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'weather': data['weather'][0]['description'],
                'wind_speed': data['wind']['speed'],
                'timestamp': pd.to_datetime(data['dt'], unit='s').isoformat()
            }
            all_data.append(weather_data)

        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur lors du téléchargement pour {city}: {e}")

    # Sauvegarder dans un fichier JSON unique
    json_filename = os.path.join(RAW_DATA_FOLDER, 'all_cities_weather.json')
    with open(json_filename, 'w') as json_file:
        json.dump(all_data, json_file, indent=4)

    print(f"✅ Données JSON enregistrées dans {json_filename}")

    # Retourner un DataFrame pandas
    return pd.DataFrame(all_data)