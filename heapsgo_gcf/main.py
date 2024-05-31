import functions_framework
import os
import datetime
import pytz
import urllib.parse
import requests
import json


# Define schema
def getSchema():
    return {
        "schema": {
            "order" : {
                "primary_key": "id",
                "fields":[
                    {"name": "id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "created_at", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
                    {"name": "paid_at", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
                    {"name": "accepted_at", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
                    {"name": "ready_at", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
                    {"name": "in_delivery_at", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
                    {"name": "delivered_at", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
                    {"name": "pre_order_for", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
                    {"name": "rejected_at", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
                    {"name": "state", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "delivery_type", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "subtotal_currency", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "subtotal_value", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "double", "logicalType": "string-double"}], "default": None},
                    {"name": "total_currency", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "total_value", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "double", "logicalType": "string-double"}], "default": None},
                    {"name": "total_discount_currency", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "total_discount_value", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "double", "logicalType": "string-double"}], "default": None},
                    {"name": "source", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "comment", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "customer_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "customer_name", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "customer_gender", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "customer_birthday", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
                    {"name": "shop_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "shop_name", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "shop_title", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "shop_online_pos_venue_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                ]
            },
            "order_line" : {
                "primary_key" : "id",
                "fields":[
                    {"name": "id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "order_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "product_reference", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "online_pos_product_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "description", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "amount", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "int", "logicalType": "string-int"}], "default": None},
                    {"name": "price_currency", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "price_value", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "double", "logicalType": "string-double"}], "default": None},
                    {"name": "subtotal_currency", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "subtotal_value", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "double", "logicalType": "string-double"}], "default": None},
                    {"name": "total_currency", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "total_value", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "double", "logicalType": "string-double"}], "default": None},
                    {"name": "category_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "category_name", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "category_title", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                ]
            },
            "order_line_child" : {
                "primary_key" : "id",
                "fields":[
                    {"name": "id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "order_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "order_line_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "product_reference", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "online_pos_product_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "description", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "amount", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "int", "logicalType": "string-int"}], "default": None},
                    {"name": "price_currency", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "price_value", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "double", "logicalType": "string-double"}], "default": None},
                    {"name": "subtotal_currency", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "subtotal_value", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "double", "logicalType": "string-double"}], "default": None},
                    {"name": "total_currency", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "total_value", "type": [{"type": "null", "logicalType": "null-empty-string"}, {"type": "double", "logicalType": "string-double"}], "default": None},
                    {"name": "category_id", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "category_name", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                    {"name": "category_title", "type": ["null", {"type": "string", "logicalType": "stringify-string"}], "default": None},
                ]
            }
        }
    }


# Define helper function to flatten nested dictionaries
def flatten_nested_dict(dict, key):
    nested_dict = {}
    if key in dict.keys():
        for k, v in dict[key].items():
            nested_dict[key + '_' + k] = v
        del dict[key]
    return {**dict, **nested_dict}


# Make the HTTP call
@functions_framework.http
def handler(req):
    # Ensure request is coming from Weld
    # Our cloud function is public, so we want to ensure that the request is coming from Weld
    # We need to generate and store an API key in Weld when setting up the custom connector
    # This API key also needs to be stored as a cloud function envorionment variable with the name WELD_API_KEY
    if req.headers["authorization"].split(" ")[1] != os.environ.get('WELD_API_KEY'):
        return { "message": "Not authorized" }, 403


    # If path is schema, return the schema to Weld # When would path be schema?
    path = req.full_path[1:-1]
    if path == "schema":
        return getSchema()


    # Define API endpoint
    url = "https://drinks.heapsapp.com/api/partner/export/orders"

    # Define API authentication
    headers = {"Authorization": "Bearer " + os.environ.get('API_BEARER_TOKEN')}

    # Log
    body = req.get_json()
    print('(log) Body name:', body["name"], '; Last state:', body["state"].get("last_state", "No last state saved"))


    # Define API parameters using incremental sync cursor where applicable, set end_time as n = 40 minutes from start_time
    start_time = body["state"].get("last_state", datetime.datetime(2022, 1, 1, 0, 0, 0, tzinfo=pytz.UTC).isoformat('T'))
    n = 40
    max_time = datetime.datetime.now(pytz.UTC).replace(microsecond=0).isoformat('T')
    end_time = min([
        datetime.datetime.fromisoformat(start_time) + datetime.timedelta(minutes=n),
        datetime.datetime.fromisoformat(max_time)
    ]).isoformat('T')

    # Create API parameters JSON object
    params = {
        "filterFrom": start_time,
        "filterTo": end_time
        }

    # Log
    print('(log) filterFrom:', start_time, '; filterTo:', end_time)


    # Make a request to the API
    response = requests.get(url, params=params, headers=headers)
    data = json.loads(response.text)

    # Initialise schema objects
    output_order = []
    output_order_line = []
    output_order_line_child = []

    # We normalise the data into three tables: order_line_child, order_line and order
    for row in data:

        # Normalising order
        order = dict(row)

        # Normalising order_line
        if "lines" in order.keys():
            lines = order["lines"]
            del order["lines"]
            for line in lines:
                # Capture order_id
                line['order_id'] = order["id"]

                # Normalising order_line_child
                if "children" in line.keys():
                    children = line["children"]
                    del line["children"]
                    for child in children:
                        # Capture order_id and order_line_id
                        child['order_id'] = order["id"]
                        child['order_line_id'] = line["id"]
                        # Flatten all dictionary objects
                        d_objects = [k for k in child.keys() if type(child[k]) is dict]
                        for d in d_objects:
                            child = flatten_nested_dict(child, d)
                        # Append output
                        output_order_line_child.append(child)

                # Flatten all remaining dictionary objects in line
                d_objects = [k for k in line.keys() if type(line[k]) is dict]
                for d in d_objects:
                    line = flatten_nested_dict(line, d)
                # Append output
                output_order_line.append(line)

            # Flatten all remaining dictionary objects
            d_objects = [k for k in order.keys() if type(order[k]) is dict]
            for d in d_objects:
                order = flatten_nested_dict(order, d)
            # Append output
            output_order.append(order)


    # Assign data to return
    if body["name"] == "order":
        output = output_order
    elif body["name"] == "order_line":
        output = output_order_line
    elif body["name"] == "order_line_child":
        output = output_order_line_child


    # Assign last state time and hast more flag
    if end_time < max_time:
        last_state_value = end_time
        has_more_flag = True
    elif end_time == max_time:
        last_state_raw = max([start_time] + [r.get("delivered_at", start_time) for r in data] + [r.get("rejected_at", start_time) for r in data])
        last_state_value = datetime.datetime.fromisoformat(last_state_raw).isoformat('T')
        has_more_flag = False
    else:
        return { "message": "filterTo parameter beyond max time" }


    # Assign function return
    return {
        "insert": output, # Output to insert
        "state": {"last_state": last_state_value}, # Values to save for next call
        "hasMore" : has_more_flag # Boolean for for pagination
    }
