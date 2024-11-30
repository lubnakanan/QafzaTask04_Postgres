import requests
import psycopg2
from datetime import datetime

# Database connection details
db_config = {
    'dbname': 'ml_model_data',   # database name
    'user': 'postgres',          # PostgreSQL username
    'password': 'EngLubna',      # PostgreSQL password
    'host': 'localhost',         # database host
    'port': '5432'               # database port (default is 5432)
}

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
    weather_data = {
        'date': datetime.now().date(),  # current date
        'temperature': data['main']['temp'],
        'humidity': data['main']['humidity'],
        'wind_speed': data['wind']['speed'],
        'location': CITY,
        'recorded_at': datetime.now()
    }
    print(f"Extracted Data: {weather_data}")  # Debugging line
    return weather_data

def store_weather_data(weather_data):
    try:
        # Connect to the database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # SQL Insert Query for new table test_weather_data
        insert_query = """
        INSERT INTO test_weather_data (date, temperature, humidity, wind_speed, location, recorded_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            weather_data['date'],
            weather_data['temperature'],
            weather_data['humidity'],
            weather_data['wind_speed'],
            weather_data['location'],
            weather_data['recorded_at']
        ))

        # Commit the transaction
        conn.commit()
        
        # Print to verify the commit is successful
        print("Weather data inserted successfully into test_weather_data!")

        # Check if data is actually inserted (for debugging)
        cursor.execute("SELECT * FROM test_weather_data;")
        rows = cursor.fetchall()
        print(f"Inserted Data: {rows}")

        # Close the connection
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error while inserting data: {e}")

# Fetch weather data
weather_data = extract_weather_data()

# Store the data in the new table
store_weather_data(weather_data)
