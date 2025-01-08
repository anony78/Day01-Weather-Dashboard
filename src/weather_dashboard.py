import os
import json
import boto3
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Debugging: Print environment variables to check if they are loaded
print("Loaded Environment Variables:")
print(f"OPEN_WEATHER_API_KEY: {os.getenv('OPEN_WEATHER_API_KEY')}")
print(f"AWS_BUCKET_NAME: {os.getenv('AWS_BUCKET_NAME')}")
print(f"AWS_REGION: {os.getenv('AWS_REGION')}")

class WeatherDashboard:
    def __init__(self):
        # Load API key and AWS information from environment variables
        self.api_key = os.getenv('OPEN_WEATHER_API_KEY')
        self.bucket_name = os.getenv('AWS_BUCKET_NAME')
        self.aws_region = os.getenv('AWS_REGION', 'eu-west-2')  # Default to eu-west-2 if not set
        
        # Initialize S3 client with the correct region
        self.s3_client = boto3.client('s3', region_name=self.aws_region)

    def create_bucket_if_not_exists(self):
        """Create S3 bucket if it doesn't exist"""
        if not self.bucket_name:
            print("Error: Bucket name is missing.")
            return

        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"Bucket {self.bucket_name} exists")
        except:
            print(f"Bucket {self.bucket_name} does not exist. Creating...")

            try:
                # Check if the region is not us-east-1, as it requires different handling
                if self.aws_region != 'us-east-1':
                    self.s3_client.create_bucket(
                        Bucket=self.bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': self.aws_region}
                    )
                else:
                    # Simpler creation for us-east-1 (no LocationConstraint needed)
                    self.s3_client.create_bucket(Bucket=self.bucket_name)

                print(f"Successfully created bucket {self.bucket_name} in region {self.aws_region}")
            except Exception as e:
                print(f"Error creating bucket: {e}")

    def fetch_weather(self, city):
        """Fetch weather data from OpenWeather API"""
        if not self.api_key:
            print("Error: OpenWeather API key is missing.")
            return None

        base_url = "http://api.openweathermap.org/data/2.5/weather"
        params = {
            "q": city,
            "appid": self.api_key,
            "units": "imperial"
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather data for {city}: {e}")
            return None

    def save_to_s3(self, weather_data, city):
        """Save weather data to S3 bucket"""
        if not weather_data:
            return False
            
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        file_name = f"weather-data/{city}-{timestamp}.json"
        
        try:
            weather_data['timestamp'] = timestamp
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_name,
                Body=json.dumps(weather_data),
                ContentType='application/json'
            )
            print(f"Successfully saved data for {city} to S3")
            return True
        except Exception as e:
            print(f"Error saving to S3: {e}")
            return False

def main():
    dashboard = WeatherDashboard()
    
    # Create bucket if needed
    dashboard.create_bucket_if_not_exists()
    
    cities = ["Philadelphia", "Seattle", "New York"]
    
    for city in cities:
        print(f"\nFetching weather for {city}...")
        weather_data = dashboard.fetch_weather(city)
        if weather_data:
            temp = weather_data['main']['temp']
            feels_like = weather_data['main']['feels_like']
            humidity = weather_data['main']['humidity']
            description = weather_data['weather'][0]['description']
            
            print(f"Temperature: {temp}°F")
            print(f"Feels like: {feels_like}°F")
            print(f"Humidity: {humidity}%")
            print(f"Conditions: {description}")
            
            # Save to S3
            success = dashboard.save_to_s3(weather_data, city)
            if success:
                print(f"Weather data for {city} saved to S3!")
        else:
            print(f"Failed to fetch weather data for {city}")

if __name__ == "__main__":
    main()
