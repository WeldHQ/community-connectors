import functions_framework
import os
import requests
import json
import time
from datetime import timedelta


def getSchema():
  return {
    "schema": {
      "beers" : {
        "id" : "string"
      }
    }
  }

  
# Define helper function to flatten nested dictionaries and lists

def flatten_data(y):
    nested_dict = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name)
                i += 1
        else:
            nested_dict[name[:-1]] = x

    flatten(y)
    return nested_dict

#1: API URL
url =  "https://api.punkapi.com/v2/"
#2: No Authentication required
headers = {}
#3: HTTP call
@functions_framework.http
def handler(req):
    #3.1 Make sure request is coming from Weld
    print(req.headers["authorization"])
    if req.headers["authorization"].split(" ")[1] != os.environ.get('WELD_API_KEY'):
     return { "message": "Not authorized" }, 403
    #3.2 If path is schema, return the schema to Weld

    path = req.full_path[1:-1]
    if path == "schema":
      return getSchema()

    body = json.loads(req.data)

    if(body["state"] is None):
      body["state"] = {}

    output = ""
    if body["name"] == "beers":
      output = 'beers'
   
    state = body["state"]

     
    #3.3 Check ratelimit
    if(state.get("ratelimit")):
      ratelimit_remaining = int(state.get("ratelimit").get("remaining"))
      if(ratelimit_remaining<5):
        current_time = time.time()

        n = 1
    #Add 60 minutes to datetime object containing current time
        wait_time = current_time + timedelta(minutes=n)

        time.sleep(3600)
      else:
        time.sleep(0.5)


    current_page = 1

    if(state.get("next_page")):
      current_page = state.get("next_page")
      

     # URL 
    
    url_with_page = url + output + "/?page=" + format(current_page) + "&per_page=80"
    print(url_with_page)



    #3.4 Call the API
    response = requests.get(url_with_page, headers=headers)
    if(response.status_code>299):
      response.raise_for_status()
    data = json.loads(response.text)

    new_ratelimit_remaining = response.headers["x-ratelimit-remaining"]
    new_ratelimit = {
      "remaining": new_ratelimit_remaining,
    }
    

    #3.5 Format data to Weld format
    rows = []
    for row in data:
      dataRow = dict(row)
      
        # Flatten all remaining dictionary objects
      d_objects = [k for k in dataRow.keys() if type(dataRow[k]) is dict]
      for d in d_objects:
          dataRow = flatten_data(dataRow)
 
      rows.append(dataRow)

   
    #3.5 Handle pagination
    next_page = current_page + 1
    has_more = len(rows) >= 79

    return {
        "insert": rows,
        "state": {
          "next_page":next_page if has_more else None,
          "ratelimit": new_ratelimit    
        } ,
        "hasMore" : has_more
    }

 
    
