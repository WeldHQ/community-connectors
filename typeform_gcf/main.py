# Note: Requires at least Python 3.10 (match-case)
# You'll need to add TYPEFORM_API_KEY and WELD_AUTH_KEY environment variables
import functions_framework
import flask
import os
import requests
import json
import pandas as pd
from time import sleep

def get_schema():
    return {
        "schema":{
            "form":{
                "primary_key": "id",
                "fields":[
                    {"name":"id", "type":"string"}
                ]
            },
            "form_response":{
                "primary_key": "response_id",
                "fields":[
                    {"name":"response_id", "type":"string"}
                ]
            }
        }
    }

def call_api(endpoint, params, retries=0):
    base_url = "https://api.typeform.com"
    url = f"{base_url}/{endpoint}"
    
    params = {**params}
    headers = {"Authorization": f"Bearer {os.environ.get('TYPEFORM_API_KEY')}"}
    
    response = requests.get(url, params=params, headers=headers)
    
    if(response.status_code != 200):
        if(response.status_code == 429 and retries<3):
            sleep(1.2)
            return call_api(endpoint, params, retries+1)
        response.raise_for_status()
    else:
        return response.json()
    
def get_form_ids() -> list[str]:
    endpoint = 'forms'
    page_num = 0
    form_ids = []
    has_more = True
    
    while(has_more):
        page_num += 1
        data = call_api(endpoint, {"page": page_num, 'page_size': 200})
        form_ids.extend(list(map(lambda item: item["id"], data["items"])))
        has_more =  data["page_count"] > page_num

    return form_ids

def get_endpoint_config(table_name, state):
    endpoint = None
    new_state = {}
    additional_data = {}
    params = {}
    
    match(table_name):
        case "form":
            endpoint = "forms"
            current_page = state.get('page', 1)
            params = {"page": current_page}
            new_state["page"] = current_page+1
            
        case "form_response":
            form_ids = state.get('form_ids', get_form_ids())
            form_id = form_ids[0]
            endpoint = f'forms/{form_id}/responses'
            # INFO:
            # This implementation supports forms with no more than 1000 responses
            # in this case setting the page size to the maximum ensures we get all responses in 1 request
            # If more responses are needed, implement the pagination using the before-after or since-until params.
            # Docs: https://www.typeform.com/developers/responses/reference/retrieve-responses/
            params = {"page_size": 1000}
            new_state["form_ids"] = form_ids[1:]
            additional_data["form_id"] = form_id
            
        case _:
            raise Exception('Invalid or missing table name')
    
    return endpoint,new_state,additional_data,params
    
    
def get_data(request_data):
    state = request_data.get('state', {})
    table_name = request_data.get('name', None)
    
    endpoint, new_state, additional_data, params = get_endpoint_config(table_name, state)
        
    data = call_api(endpoint, params)
    has_more = True
        
    match(table_name):
        case 'form':
            has_more = data.get('page_count') > state.get('page', 1)
        case 'form_response':
            remaining_form_ids = new_state.get('form_ids')
            has_more = len(remaining_form_ids) > 0
        case _:
            raise Exception('Invalid or missing table name')
            
    return {
        "insert": clean_data(data.get('items', []), additional_data),
        "hasMore": has_more,
        "state": new_state if has_more else {}
    }
    
def clean_data(items, additional_data):
    cleaned_items = []
    for item in items:
        item.pop('theme',None)
        item.pop('self', None)
        item.pop('_links', None)
        item.update(additional_data)
        p = pd.json_normalize(item, sep='_')
        
        cleaned_items.append(p.to_dict(orient="records")[0])
    return cleaned_items

@functions_framework.http
def handler(request: flask.Request):
    if(request.headers.get('Authorization', None) != f"Bearer {os.environ.get('WELD_AUTH_KEY')}"):
        return "Unauthorized", 401
    
    if(request.path == '/schema'):
        if(request.method == 'GET'):
            return get_schema()
        else: 
            return 'Method not allowed', 405
    
    if(request.path == '/'):
        if(request.method == 'POST'):
            request_data = json.loads(request.data)
            try:
                return get_data(request_data)
            except requests.HTTPError as e:
                return e.response.json(), e.response.status_code
            except Exception as e:
                return str(e), 500
        else: 
            return 'Method not allowed', 405

    else:
        return 'Endpoint not found', 404
            
        