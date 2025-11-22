import logging
import os
import json
import math
import psycopg2
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request for get-all-deals.')

    try:
        # Database connection details from environment variables
        host = os.environ.get('POSTGRES_HOST')
        port = os.environ.get('POSTGRES_PORT')
        dbname = os.environ.get('POSTGRES_DB')
        user = os.environ.get('POSTGRES_USER')
        password = os.environ.get('POSTGRES_PASSWORD')

        # Check if all environment variables are set
        if not all([host, port, dbname, user, password]):
            logging.error("Database connection details are not fully configured.")
            return func.HttpResponse(
                "Server error: Database configuration is incomplete.",
                status_code=500
            )

        # Get pagination parameters
        page = int(req.params.get('page', 1))
        limit = int(req.params.get('limit', 10))
        
        if page < 1:
            page = 1
        if limit < 1:
            limit = 10
            
        offset = (page - 1) * limit

        # Connect to the database
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        cursor = conn.cursor()

        # Get total count
        cursor.execute("SELECT COUNT(*) FROM listings")
        total_count = cursor.fetchone()[0]
        
        # Calculate total pages
        total_pages = math.ceil(total_count / limit)

        # Execute query with pagination
        query = "SELECT * FROM listings ORDER BY listed_at DESC LIMIT %s OFFSET %s"
        cursor.execute(query, (limit, offset))
        
        # Fetch all rows and column names
        rows = cursor.fetchall()
        
        result = []
        # Only process if rows were returned and column descriptions are available
        if rows and cursor.description:
            colnames = [desc[0] for desc in cursor.description]
            result = [dict(zip(colnames, row)) for row in rows]

        # Close connection
        cursor.close()
        conn.close()

        # Construct response
        response_data = {
            "data": result,
            "pagination": {
                "page": page,
                "limit": limit,
                "total": total_count,
                "total_pages": total_pages
            }
        }

        # Return JSON response
        return func.HttpResponse(
            json.dumps(response_data, default=str),
            status_code=200,
            mimetype="application/json",
            headers={
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization'
            }
        )

    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
        return func.HttpResponse(
            "Server error: Could not connect to or query the database.",
            status_code=500
        )
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return func.HttpResponse(
            "An unexpected error occurred.",
            status_code=500
        )
