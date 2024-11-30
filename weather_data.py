import requests
import pandas as pd
import psycopg2
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(filename='weather_data.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# API Configuration
API_KEY = 'c872d3baeb70b4b22323919b67783c4f'
BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'
CITY = 'London'

# Database Configuration
DB_NAME = 'ml_model_data'
DB_USER = 'postgres'
DB_PASSWORD = 'your_db_password'  # Replace with your password
DB_HOST = 'localhost'
DB_PORT = '5432'

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
        logging.info("Successfully fetched data from the API.")
        print("API Response:", response.json())  # Print the raw response for debugging
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None

def extract_weather_data():
    """Extract relevant weather data."""
    data = fetch_weather_data()
    if data is None:
        logging.error("No data returned from the API. Exiting.")
        return None
    
    # Extract relevant data fields
    weather_data = {
        'city': CITY,
        'temperature': data['main']['temp'],
        'humidity': data['main']['humidity'],
        'wind_speed': data['wind']['speed'],
        'location': CITY,
        'recorded_at': pd.to_datetime('now')
    }
    logging.info(f"Extracted weather data: {weather_data}")
    return weather_data

def insert_weather_data_to_db(weather_data):
    """Insert weather data into PostgreSQL database."""
    if weather_data is None:
        logging.error("No weather data to insert into the database.")
        return
    
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname=DB_NAME, 
            user=DB_USER, 
            password=DB_PASSWORD, 
            host=DB_HOST, 
            port=DB_PORT
        )
        cursor = conn.cursor()

        # Insert the weather data into the table
        cursor.execute("""
            INSERT INTO weather_data (date, temperature, humidity, wind_speed, location, recorded_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (weather_data['recorded_at'].date(), weather_data['temperature'], weather_data['humidity'], 
              weather_data['wind_speed'], weather_data['location'], weather_data['recorded_at']))

        # Commit the transaction
        conn.commit()
        logging.info("Weather data inserted successfully into the database.")

        # Close the connection
        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Error inserting data into database: {e}")
        print(f"Error inserting data into database: {e}")  # Print error for debugging

def save_weather_data_locally(weather_data):
    """Save weather data to CSV locally."""
    weather_df = pd.DataFrame([weather_data])
    weather_df.to_csv('weather_data.csv', index=False)
    logging.info("Weather data saved to CSV file.")

def main():
    """Main function to orchestrate the data extraction, transformation, and loading (ETL)."""
    logging.info("Starting ETL process...")
    weather_data = extract_weather_data()

    if weather_data:
        # Insert into the database
        insert_weather_data_to_db(weather_data)
        
        # Optionally, save to CSV
        save_weather_data_locally(weather_data)
        
    else:
        logging.error("ETL process failed. No weather data to process.")

if __name__ == "__main__":
    main()
