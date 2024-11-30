import requests
import pandas as pd
import psycopg2
import logging
from datetime import datetime

# API Configuration
API_KEY = 'c872d3baeb70b4b22323919b67783c4f'
BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'
CITY = 'London'

# Set up logging
logging.basicConfig(filename='weather_data.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# PostgreSQL Database Configuration
DB_NAME = 'ml_model_data'
DB_USER = 'postgres'
DB_PASSWORD = 'password'
DB_HOST = 'localhost'
DB_PORT = '5432'

def fetch_weather_data():
    """Fetch weather data from OpenWeather API."""
    params = {
        'q': CITY,
        'appid': API_KEY,
        'units': 'metric'
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        logging.info(f"API Response: {response.json()}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        print(f"Error fetching data from API: {e}")
        return None

def extract_weather_data():
    """Extract relevant weather data from the API response."""
    data = fetch_weather_data()
    if data is None:
        return None

    try:
        weather_data = {
            'city': CITY,
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'wind_speed': data['wind']['speed'],
            'weather': data['weather'][0]['description'],
            'timestamp': pd.to_datetime('now')
        }
        return weather_data
    except KeyError as e:
        logging.error(f"Error extracting data: Missing key {e}")
        print(f"Error extracting data: Missing key {e}")
        return None

def insert_weather_data_to_db(weather_data):
    """Insert weather data into PostgreSQL database."""
    if weather_data is None:
        return

    try:
        # Establish database connection
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        cursor = conn.cursor()

        # Insert data into weather_data table
        cursor.execute("""
            INSERT INTO weather_data (date, temperature, humidity, wind_speed, location, recorded_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            weather_data['timestamp'].date(),
            weather_data['temperature'],
            weather_data['humidity'],
            weather_data['wind_speed'],
            weather_data['city'],
            weather_data['timestamp']
        ))

        # Commit the transaction
        conn.commit()
        logging.info("Weather data inserted successfully into the database.")
        print("Weather data inserted successfully.")
    except Exception as e:
        logging.error(f"Error inserting data into database: {e}")
        print(f"Error inserting data into database: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    """Main function to fetch and insert weather data."""
    weather_data = extract_weather_data()
    if weather_data:
        insert_weather_data_to_db(weather_data)

# Run the main function
if __name__ == "__main__":
    main()
