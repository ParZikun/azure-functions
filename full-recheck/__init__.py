import logging
import azure.functions as func
import os
import requests
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a full-recheck request.')

    api_url = os.getenv("CARDS_CARTEL_API_URL", "http://localhost:8000")
    target_url = f"{api_url}/api/trigger/full-recheck"
    
    try:
        # We need to pass auth or ensure the API is protected if deployed publicly.
        # For now, assuming internal network or localhost.
        response = requests.post(target_url, timeout=5) # fast timeout as it's just a trigger
        
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
        logging.error(f"Failed to trigger full recheck: {e}")
        return func.HttpResponse(
            f"Internal Error: {str(e)}",
            status_code=500
        )
