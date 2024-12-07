import requests
import psycopg2
import pandas as pd
from datetime import datetime

# Extracting Data from OpenWeatherMap API
def extract_weather_data(api_url, api_key):
    """Extract data from the weather API."""
    try:
        response = requests.get(f"{api_url}&appid={api_key}")
        response.raise_for_status()
        data = response.json()
        print("Data successfully extracted from the API.")
        return data
    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

# Transforming Data
def transform_weather_data(data):
    """Transform raw weather data into a structured format."""
    try:
        transformed_data = {
            'date': datetime.utcfromtimestamp(data['dt']).strftime('%Y-%m-%d'),
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'wind_speed': data['wind']['speed'],
            'location': data['name'],
            'recorded_at': datetime.utcfromtimestamp(data['dt']).strftime('%Y-%m-%d %H:%M:%S')
        }
        print("Data successfully transformed.")
        return transformed_data
    except KeyError as e:
        print(f"Error transforming data: Missing key {e}")
        return None
    except Exception as e:
        print(f"Unexpected error during transformation: {e}")
        return None

# Loading Data into PostgreSQL
def load_data_to_postgres(transformed_data, db_config):
    """Load transformed data into PostgreSQL."""
    connection = None
    cursor = None
    try:
        print("Connecting to PostgreSQL...")
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        # Insert data into the table
        query = """
        INSERT INTO test_weather_data (date, temperature, humidity, wind_speed, location, recorded_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, list(transformed_data.values()))
        
        connection.commit()
        print("Data successfully loaded into PostgreSQL!")
    except psycopg2.OperationalError as e:
        print(f"Operational error: {e}")
    except psycopg2.Error as e:
        print(f"Error interacting with PostgreSQL: {e}")
    except Exception as e:
        print(f"Unexpected error during data loading: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("PostgreSQL connection closed.")

# Error Handling & Monitoring
def main():
    # API Configuration
    API_KEY = 'c872d3baeb70b4b22323919b67783c4f'
    API_URL = 'http://api.openweathermap.org/data/2.5/weather?q=London'

    # Database Configuration
    db_config = {
        'dbname': 'ml_model_data',
        'user': 'postgres',
        'password': 'EngLubna',  
        'host': 'localhost',
        'port': '5432'
    }

    print("Starting ETL pipeline...")

    # Step 1: Extract
    raw_data = extract_weather_data(API_URL, API_KEY)
    if not raw_data:
        print("ETL pipeline terminated at the extraction step.")
        return

    # Step 2: Transform
    structured_data = transform_weather_data(raw_data)
    if not structured_data:
        print("ETL pipeline terminated at the transformation step.")
        return

    # Step 3: Load
    load_data_to_postgres(structured_data, db_config)

    print("ETL pipeline completed successfully!")

# Execute the ETL process
if __name__ == "__main__":
    main()
