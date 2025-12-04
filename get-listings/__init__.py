import azure.functions as func
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps({"message": "This endpoint has been moved to the FastAPI container."}),
        status_code=410,
        mimetype="application/json"
    )
