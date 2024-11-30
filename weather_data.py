import requests
import pandas as pd

# API Configuration
API_KEY = 'c872d3baeb70b4b22323919b67783c4f'
BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'
CITY = 'London'

def fetch_weather_data():
    params = {
        'q': CITY,
        'appid': API_KEY,
        'units': 'metric'
    }
    response = requests.get(BASE_URL, params=params)
    return response.json()

def extract_weather_data():
    data = fetch_weather_data()
    # Extract relevant data fields
    weather_data = {
        'city': CITY,
        'temperature': data['main']['temp'],
        'humidity': data['main']['humidity'],
        'pressure': data['main']['pressure'],
        'weather': data['weather'][0]['description'],
        'timestamp': pd.to_datetime('now')
    }
    return weather_data

# Fetch data
weather = extract_weather_data()
weather_df = pd.DataFrame([weather])

# Save the data to a CSV (this can be part of the ETL pipeline)
weather_df.to_csv('weather_data.csv', index=False)
