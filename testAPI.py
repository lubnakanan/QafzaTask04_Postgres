import os
import csv
import requests
from datetime import datetime

# Set the API URL and your API key (replace 'YOUR_API_KEY' with your actual key)
API_KEY = 'c872d3baeb70b4b22323919b67783c4f'
API_URL = 'http://api.openweathermap.org/data/2.5/weather?q=London&appid=' + API_KEY

# Make the API request
response = requests.get(API_URL)

# Check if the request was successful
if response.status_code == 200:
    print("API Call Successful")
    # Extract the data from the response
    data = response.json()

    # Example: Extracting relevant weather information
    weather_data = {
        'date': datetime.utcfromtimestamp(data['dt']).strftime('%Y-%m-%d'),  # Convert timestamp to date
        'temperature': data['main']['temp'],
        'humidity': data['main']['humidity'],
        'wind_speed': data['wind']['speed'],
        'location': data['name'],  # Location name
        'recorded_at': datetime.utcfromtimestamp(data['dt']).strftime('%Y-%m-%d %H:%M:%S')
    }

    # Path to your local GitHub repository (adjust the path as per your local system)
    repo_path = '/mnt/c/Users/User/QafzaTask04_Postgres'  # Update this if your repo is located elsewhere

    # Define the path for the CSV file inside your repo
    csv_file_path = os.path.join(repo_path, 'weather_data.csv')

    # Check if the CSV file already exists
    file_exists = os.path.isfile(csv_file_path)

    # Write to CSV file in the GitHub repo
    with open(csv_file_path, 'a', newline='') as file:
        writer = csv.writer(file)

        # If the file doesn't exist, write the header
        if not file_exists:
            writer.writerow(['date', 'temperature', 'humidity', 'wind_speed', 'location', 'recorded_at'])

        # Write the weather data to the CSV file
        writer.writerow([weather_data['date'], weather_data['temperature'], weather_data['humidity'],
                         weather_data['wind_speed'], weather_data['location'], weather_data['recorded_at']])

    print(f"CSV file saved at: {csv_file_path}")
else:
    print("Failed to retrieve data from the API.")
