import requests
import pandas as pd
import psycopg2
from psycopg2 import sql
import logging

# API Configuration
API_KEY = 'c872d3baeb70b4b22323919b67783c4f'
BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'
CITY = 'London'

# Logging setup
logging.basicConfig(filename='weather_data.log', level=logging.INFO)

def fetch_weather_data():
    """Fetch weather data from OpenWeatherMap API."""
    try:
        params = {
            'q': CITY,
            'appid': API_KEY,
            'units': 'metric'
        }
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None

def extract_weather_data():
    """Extract relevant weather data fields."""
    data = fetch_weather_data()
    if data:
        weather_data = {
            'city': CITY,
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'weather': data['weather'][0]['description'],
            'timestamp': pd.to_datetime('now')
        }
        return weather_data
    else:
        return None

def insert_weather_data_to_db(weather_data):
    """Insert weather data into PostgreSQL database."""
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="your_db_name", 
            user="your_db_user", 
            password="your_db_password", 
            host="localhost", 
            port="5432"
        )
        cursor = conn.cursor()

        # Insert the weather data into the table (assuming the table has columns city, temperature, humidity, etc.)
        cursor.execute("""
            INSERT INTO weather_data (city, temperature, humidity, pressure, weather, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (weather_data['city'], weather_data['temperature'], weather_data['humidity'], 
              weather_data['pressure'], weather_data['weather'], weather_data['timestamp']))

        # Commit the transaction
        conn.commit()

        # Close the connection
        cursor.close()
        conn.close()

        logging.info("Weather data inserted successfully into the database.")

    except Exception as e:
        logging.error(f"Error inserting data into database: {e}")

def main():
    """Main function to fetch weather data and insert into database."""
    logging.info("Starting weather data extraction and database insertion.")
    weather_data = extract_weather_data()

    if weather_data:
        logging.info(f"Weather data fetched: {weather_data}")
        # Insert into database
        insert_weather_data_to_db(weather_data)
    else:
        logging.error("Failed to extract weather data.")

if __name__ == "__main__":
    main()
