import logging
import os
import json
import psycopg2
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

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

        # Connect to the database
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        cursor = conn.cursor()

        # Execute query
        cursor.execute("SELECT * FROM listings WHERE is_listed = 1")
        
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

        # Format results as a list of dictionaries
        # Return JSON response
        return func.HttpResponse(
            json.dumps(result, default=str),
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
