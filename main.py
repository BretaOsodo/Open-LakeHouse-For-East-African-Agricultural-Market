import requests
import json

def get_data():
    request=requests.get("https://archive-api.open-meteo.com/v1/archive?latitude=51.5&longitude=-0.12&start_date=2026-05-01&end_date=2026-05-07&hourly=temperature_2m")

    #get data as a python dict
    data= request.json()

    #Convert to pretty JSON string for display
    pretty_json = json.dumps(data, indent=4)

    return pretty_json

called_data = get_data()
print(called_data)

