import requests
import csv
import json
import os
import functions_framework

#1: API URL
url = "https://sheets.googleapis.com/v4/spreadsheets/"

#2: Authentication
sheets = { "sheet1": { "id": "11Uhjl9xQEQk6l7ZFfhFzsvPs6sFgG65JovPM4DZ5nCg", "range": "TestNamedRange" } }
api_key = "AIzaSyDfH7QqqvOUkUZq9rAiDKqblDNgNIaWazI"
# Read more about Google Sheet ranges here: https://developers.google.com/sheets/api/samples/reading

#3: HTTP call
@functions_framework.http
def handler(req):
    #3.1 Make sure request is coming from Weld
    if req.headers["authorization"].split(" ")[1] != os.environ.get('WELD_API_KEY'):
        return { "message": "Not authorized" }, 403

    #3.2 If path is schema, return the schema to Weld. If schema is not defined, Weld will auto-infer types for you.
    path = req.full_path[1:-1]
    if path == "schema":
      return {
        "schema": {
          "sheet1" : {
            "primary_key" : "ID"
          }
        }
      }

    #3.3 Set potential URL parameters


    #3.4 Call the API
    request_json = req.get_json(silent=True)
    sheet = sheets[request_json['name']]
    result = requests.get(url + sheet["id"] + "/values/" + sheet["range"] + "?key=" + os.environ.get('GOOGLE_SHEETS_API_KEY'))
    data = json.loads(result.text)

    #3.5 Format data to Weld format
    keys = data["values"][0]
    rest = data["values"][1:]
    rows = []
    for row in rest:
      obj = {}
      for index, value in enumerate(keys):
        obj[value] = row[index]
      rows.append(obj)

    #3.6 Handle pagination - find next URL, different how APIs handles this
    # Not needed for Google Sheets

    return {
        "state": {},
        "insert": rows,
        "hasMore" : False
    }