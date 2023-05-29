import requests
from datetime import date

url = "http://www.psudataeng.com:8000/getStopEvents"  
date = date.today().strftime("%Y-%m-%d")
destination_path = f"/Users/vysalikallepalli/stopevents_{date}.html"  
response = requests.get(url)
response.raise_for_status()  # Check for any errors

with open(destination_path, "wb") as file:
    file.write(response.content)

print("Download complete!")
