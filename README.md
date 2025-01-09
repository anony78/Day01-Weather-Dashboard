# Day01-Weather-Dashboard
30 Days DevOps Challenge - Weather Dashboard
Day 1: Building a weather data collection system using AWS S3 and OpenWeather API

Weather Data Collection System - DevOps Day 1 Challenge
Project Overview
This project is a Weather Data Collection System that demonstrates core DevOps principles by combining:

External API Integration (OpenWeather API)
Cloud Storage (AWS S3)
Infrastructure as Code
Version Control (Git)
Python Development
Error Handling
Environment Management
Features
Fetches real-time weather data for multiple cities
Displays temperature (°F), humidity, and weather conditions
Automatically stores weather data in AWS S3
Supports multiple cities tracking
Timestamps all data for historical tracking
Technical Architecture
Language: Python 3.x
Cloud Provider: AWS (S3)
External API: OpenWeather API
Dependencies:
boto3 (AWS SDK)
python-dotenv
requests
## Project Structure
weather-dashboard/
  src/
    __init__.py
    weather_dashboard.py
  tests/
  data/
  .env
  .gitignore
  requirements.txt
  
What I Learned

AWS S3 bucket creation and management
Environment variable management for secure API keys
Python best practices for API integration
Git workflow for project development
Error handling in distributed systems
Cloud resource management

Future Enhancements

Add weather forecasting
Implement data visualization
Add more cities
Create automated testing
Set up CI/CD pipeline
