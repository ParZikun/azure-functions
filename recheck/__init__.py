import logging
import azure.functions as func
import os
import requests
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a recheck request.')

    api_url = os.getenv("CARDS_CARTEL_API_URL", "http://localhost:8000")
    target_url = f"{api_url}/api/trigger/recheck"
    
    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
             "Invalid JSON body",
             status_code=400
        )

    try:
        # Forward the JSON body (e.g. {"duration": "1H", "category": "SKIP"})
        # Timeout increased to 300s (5m) to allow for synchronous processing of rechecks
        response = requests.post(target_url, json=req_body, timeout=300)
        
        if response.status_code == 200:
            return func.HttpResponse(
                json.dumps(response.json()),
                mimetype="application/json",
                status_code=200
            )
        else:
            return func.HttpResponse(
                f"Backend API Error: {response.text}",
                status_code=response.status_code
            )
            
    except Exception as e:
        logging.error(f"Failed to trigger recheck: {e}")
        return func.HttpResponse(
            f"Internal Error: {str(e)}",
            status_code=500
        )
