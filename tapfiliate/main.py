import functions_framework
import os
import requests
import json
import datetime
import pytz
import urllib
import urllib.parse
import time

def getSchema():
 return {
  "schema": {
            "affiliates" : {
                "primary_key": "id",
                "fields":[
                    {"name": "id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "firstname", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "lastname", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "email", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "created_at", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
                    {"name": "company_name", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "company_description", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "address_address", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "address_address_two", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "address_city", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "address_country_code", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "address_country_name", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "address_state", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "address_postal_code", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "affiliate_group_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "parent_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "promoted_at", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "promotion_method", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "meta_data_Domain", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None}
                  ]
          },
            "conversions" : {
                "primary_key": "id",
                "fields":[
                    {"name": "id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "affiliate_firstname", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "affiliate_lastname", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "affiliate_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "created_at", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
                    {"name": "customer_customer_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "customer_status", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "external_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "click_created_at", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
                    {"name": "affiliate_meta_data", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "click_landing_page", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "click_referrer", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "amount", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "double", "logicalType": "string-double"}], "default": None},
                    {"name": "commissions_affiliate_firstname", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "commissions_affiliate_lastname", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "commissions_affiliate_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "commissions_amount", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "double", "logicalType": "string-double"}], "default": None},
                    {"name": "commissions_approved", "type": ["null", {"type": "boolean", "logicalType": "boolean"}], "default": None},
                    {"name": "commissions_comment", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "commissions_commission_type", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "commissions_conversion_sub_amount", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "double", "logicalType": "string-double"}], "default": None},
                    {"name": "commissions_created_at", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
                    {"name": "commissions_currency", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "commissions_final", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "commissions_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "commissions_kind", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "program_currency", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "program_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "program_title", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "warnings", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None}


                  ]
          },
    "customers" : {
      "primary_key": "id",
                "fields":[
                    {"name": "id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "affiliate_firstname", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "affiliate_lastname", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "affiliate_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "created_at", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
                    {"name": "customer_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "click_created_at", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
                    {"name": "affiliate_meta_data", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "click_landing_page", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "click_referrer", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "program_currency", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "program_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "program_title", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "warnings", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None}

                
                ]

   },
      "commissions" : {
      "primary_key": "id",
                "fields":[
                    {"name": "id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "affiliate_firstname", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "affiliate_lastname", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "affiliate_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "amount", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "double", "logicalType": "string-double"}], "default": None},
                    {"name": "approved", "type": ["null", {"type": "boolean", "logicalType": "boolean"}], "default": None},
                    {"name": "created_at", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
                    {"name": "comment", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "commission_type", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "conversion_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "conversion_sub_amount", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "double", "logicalType": "string-double"}], "default": None},
                    {"name": "currency", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "final", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "kind", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "payout", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None}

                 ]

   },
    "affiliate-groups" : {
          "primary_key": "id",
                "fields":[
                    {"name": "id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "title", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "affiliate_count", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "double", "logicalType": "string-double"}], "default": None}
                ]
   },
   "programs" : {
      "primary_key": "id",
                "fields":[
                    {"name": "id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "title", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "program_category_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "program_category_identifier", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "program_category_is_admitad_suitable", "type": ["null", {"type": "boolean", "logicalType": "boolean"}], "default": None},
                    {"name": "cookie_time", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "double", "logicalType": "string-double"}], "default": None},
                    {"name": "program_category_title", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "recurring", "type": ["null", {"type": "boolean", "logicalType": "boolean"}], "default": None},
                    {"name": "recurring_cap", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "recurring_period_days", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "double", "logicalType": "string-double"}], "default": None}
                ]
   },
    "payments" : {
      "primary_key": "id",
                "fields":[
                    {"name": "id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "affiliate_firstname", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "affiliate_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "affiliate_lastname", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "amount", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "double", "logicalType": "string-double"}], "default": None},
                    {"name": "created_at", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
                    {"name": "currency", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None}
                ]
   }
  }
 }
  

# Define helper function to run incremental
def should_run_incremental(table_name):
  match table_name:
    case "customers":
      return True
    case "conversions":
      return True
    case other:
      return False
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
url = "https://api.tapfiliate.com/1.6/"
#2: Authentication
headers = {"X-Api-Key": os.environ.get('API_BEARER_TOKEN')}
#3: HTTP call
@functions_framework.http
def handler(req):
    #3.1 Make sure request is coming from Weld
    #print(req.headers["authorization"])
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
    if body["name"] == "affiliates":
      output = 'affiliates'
    if body["name"] == "conversions":
      output = 'conversions'
    if body["name"] == "customers":
      output = 'customers'
    if body["name"] == "commissions":
      output = 'commissions'
    if body["name"] == "affiliate-groups":
      output = 'affiliate-groups'
    if body["name"] == "programs":
      output = 'programs'
    if body["name"] == "payments":
      output = 'payments'

    state = body["state"]

    # If running incremental

    if(state.get('started')):
      started = state.get('started')
    else: 
      started = datetime.date.today().strftime("%Y-%m-%d")

    pointer = None
    if should_run_incremental(output):
      if(state.get("pointer")):
        pointer = state.get("pointer")
      else:
        pointer = "1970-01-01"

    print(pointer)

     
    #3.3 Check ratelimit
    if(state.get("ratelimit")):
      ratelimit_remaining = int(state.get("ratelimit").get("remaining"))
      ratelimit_reset = int(state.get("ratelimit").get("reset"))
      if(ratelimit_remaining<5):
        current_time = time.time()
        wait_time = ratelimit_reset-current_time


        time.sleep(wait_time)
      else:
        time.sleep(0.5)

     # URL 

    current_page = 1

    if(state.get("next_page")):
      current_page = state.get("next_page")

    url_with_page = url + output + "/?page=" + format(current_page) + ("&date_from=" + pointer if pointer else "")
    print(url_with_page)

    #3.4 Call the API
    response = requests.get(url_with_page, headers=headers)
    if(response.status_code>299):
      response.raise_for_status()
    data = json.loads(response.text)
    new_ratelimit_remaining = response.headers["X-Ratelimit-Remaining"]
    new_ratelimit_reset = response.headers["X-Ratelimit-Reset"]
    new_ratelimit = {
      "remaining": new_ratelimit_remaining,
      "reset": new_ratelimit_reset
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
    has_more = len(rows)>0
    
    save_state = has_more or (should_run_incremental(output) and not has_more)

    return {
        "insert": rows,
        "state": {
          "next_page":next_page if has_more else None,
          "ratelimit": new_ratelimit,
          "pointer": pointer if has_more else started,
          "sync_started": started if has_more else None
        } if save_state else None,
        "hasMore" : has_more
    }

  