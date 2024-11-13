"""
Test databricks fucntionaility
"""
#!pip install python-dotenv
#%restart_python
import requests
from dotenv import load_dotenv
import os
import pytest

# Load environment variables (make sure you have the .env file configured)
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
url = f"https://{server_h}/api/2.0/clusters/list"  # Simple API call to check authentication

# Function to test if credentials are correct
def check_credentials(headers):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Will raise an exception if the status code is not 200
        return True  # If we get a valid response, credentials are correct
    except requests.exceptions.HTTPError as err:
        print(f"HTTP Error: {err}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False

# Pytest test function to verify credentials
def test_databricks_credentials():
    headers = {'Authorization': f'Bearer {access_token}'}
    assert check_credentials(headers) is True, "The credentials are invalid or the API call failed."

