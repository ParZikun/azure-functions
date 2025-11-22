import logging
import os
import json
import requests
import psycopg2
import azure.functions as func
import httpx
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict

# --- Alt Data Logic (Adapted from alt_data.py) ---
GRAPHQL_URL = "https://alt-platform-server.production.internal.onlyalt.com/graphql/"
# NOTE: These should be in environment variables
AUTH_TOKEN = os.getenv("ALT_AUTH_TOKEN")
COOKIE = os.getenv("ALT_COOKIE")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0) Gecko/20100101 Firefox/142.0',
    'Accept': '*/*',
    'Content-Type': 'application/json',
    'Referer': 'https://app.alt.xyz/',
    'Origin': 'https://app.alt.xyz'
}
if AUTH_TOKEN:
    HEADERS['authorization'] = f'Bearer {AUTH_TOKEN}'
if COOKIE:
    HEADERS['Cookie'] = COOKIE

async def get_asset_id_async(client, cert_id):
    payload = {
        "operationName": "Cert",
        "variables": {"certNumber": cert_id},
        "query": "query Cert($certNumber: String!) { cert(certNumber: $certNumber) { asset { id name __typename } __typename } }"
    }
    try:
        response = await client.post(url=GRAPHQL_URL, json=payload)
        if response.status_code != 200: return None
        data = response.json()
        asset = data.get('data', {}).get('cert', {}).get('asset')
        if asset and 'id' in asset:
            return asset['id']
    except Exception as e:
        logging.error(f"Error fetching asset ID for {cert_id}: {e}")
    return None

async def get_alt_data_async(client, cert_id, grade, company):
    if not AUTH_TOKEN or not COOKIE:
        return None

    asset_id = await get_asset_id_async(client, cert_id)
    if not asset_id: return None

    details_query = """
    query AssetDetails($id: ID!, $tsFilter: TimeSeriesFilter!) {
      asset(id: $id) {
        altValueInfo(tsFilter: $tsFilter) {
          currentAltValue
          confidenceData {
            currentConfidenceMetric
            currentErrorLowerBound
            currentErrorUpperBound
          }
        }
        cardPops {
          gradingCompany
          gradeNumber
          count
        }
      }
    }
    """
    transactions_query  = """
    query AssetMarketTransactions($id: ID!, $marketTransactionFilter: MarketTransactionFilter!) {
      asset(id: $id) {
        marketTransactions(marketTransactionFilter: $marketTransactionFilter) {
          date
          price
        }
      }
    }
    """
    
    try:
        details_payload = {
            "operationName": "AssetDetails",
            "variables": {
                "id": asset_id,
                "tsFilter": {"gradeNumber": f"{float(grade):.1f}", "gradingCompany": company}
            },
            "query": details_query
        }
        trans_payload = {
            "operationName": "AssetMarketTransactions",
            "variables": {
                "id": asset_id,
                "marketTransactionFilter": {"gradingCompany": company, "gradeNumber": f"{float(grade):.1f}", "showSkipped": True}
            },
            "query": transactions_query
        }

        details_response, trans_response = await asyncio.gather(
            client.post(url=GRAPHQL_URL, json=details_payload),
            client.post(url=GRAPHQL_URL, json=trans_payload)
        )

        if details_response.status_code != 200 or trans_response.status_code != 200:
            return None

        details_data = details_response.json().get('data', {}).get('asset', {}) or {}
        transactions = trans_response.json().get('data', {}).get('asset', {}).get('marketTransactions', [])

        alt_value_info = details_data.get('altValueInfo', {}) or {}
        confidence_data = alt_value_info.get('confidenceData', {}) or {}
        
        supply = 0
        for pop in details_data.get('cardPops', []):
            if pop.get('gradingCompany') == company and str(pop.get('gradeNumber')) == f"{float(grade):.1f}":
                supply = pop.get('count', 0)
                break

        avg_price = 0.0
        if supply > 3000:
            daily_prices, fifteen_days_ago = defaultdict(list), datetime.now() - timedelta(days=15)
            for tx in transactions:
                tx_date = datetime.fromisoformat(tx['date'].split('T')[0])
                if tx_date >= fifteen_days_ago:
                    daily_prices[tx_date.strftime('%Y-%m-%d')].append(float(tx['price']))
            if daily_prices:
                daily_averages = [sum(prices) / len(prices) for prices in daily_prices.values()]
                if daily_averages: avg_price = sum(daily_averages) / len(daily_averages)
        else:
            recent_sales = [float(tx['price']) for tx in transactions[:4]]
            if recent_sales: avg_price = sum(recent_sales) / len(recent_sales)

        return {
            "alt_asset_id": asset_id,
            "alt_value": alt_value_info.get('currentAltValue') or 0.0,
            "avg_price": avg_price,
            "supply": supply,
            "lower_bound": confidence_data.get('currentErrorLowerBound') or 0.0,
            "upper_bound": confidence_data.get('currentErrorUpperBound') or 0.0,
            "confidence": confidence_data.get('currentConfidenceMetric') or 0.0
        }
    except Exception as e:
        logging.error(f"Error fetching Alt data: {e}")
        return None

async def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request for get-wallet-holdings.')

    wallet_address = req.params.get('wallet')
    offset = int(req.params.get('offset', 0))
    limit = int(req.params.get('limit', 20))

    if not wallet_address:
        return func.HttpResponse(
            "Please pass a wallet address on the query string",
            status_code=400
        )

    # Database connection details
    host = os.environ.get('POSTGRES_HOST')
    port = os.environ.get('POSTGRES_PORT')
    dbname = os.environ.get('POSTGRES_DB')
    user = os.environ.get('POSTGRES_USER')
    password = os.environ.get('POSTGRES_PASSWORD')

    try:
        # 1. Fetch Tokens from Magic Eden (Paginated)
        me_url = f"https://api-mainnet.magiceden.dev/v2/wallets/{wallet_address}/tokens"
        
        # Correct structure: Array of Arrays of Objects
        # [[{Category=Pokemon}], [{GC=PSA}, {GC=Beckett}, {GC=BGS}]]
        attributes_filter = [
            [{"traitType": "Category", "value": "Pokemon"}],
            [
                {"traitType": "Grading Company", "value": "PSA"},
                {"traitType": "Grading Company", "value": "Beckett"},
                {"traitType": "Grading Company", "value": "BGS"}
            ]
        ]
        
        params = {
            "collection_symbol": "collector_crypt",
            "limit": limit,
            "offset": offset,
            "attributes": json.dumps(attributes_filter)
        }

        headers = {
            "accept": "application/json"
        }

        # Use requests for synchronous ME call
        response = requests.get(me_url, params=params, headers=headers)
        response.raise_for_status()
        tokens = response.json()

        
        # 2. Extract Mints and Prepare for DB Query
        token_mints = [t.get('mintAddress') for t in tokens if t.get('mintAddress')]
        
        cartel_data_map = {}
        
        # 2a. Check DB first
        if token_mints and all([host, port, dbname, user, password]):
            try:
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    dbname=dbname,
                    user=user,
                    password=password
                )
                cursor = conn.cursor()
                query = "SELECT token_mint, alt_value, avg_price, supply, alt_asset_id, alt_value_lower_bound, alt_value_upper_bound, name, grade, grade_num, grading_id, img_url FROM listings WHERE token_mint = ANY(%s)"
                cursor.execute(query, (token_mints,))
                rows = cursor.fetchall()
                for row in rows:
                    cartel_data_map[row[0]] = {
                        "alt_value": row[1],
                        "cartel_avg": row[2],
                        "supply": row[3],
                        "alt_asset_id": row[4],
                        "alt_range": f"{row[5]} - {row[6]}" if row[5] is not None and row[6] is not None else None,
                        "db_name": row[7],
                        "db_grade": row[8],
                        "db_grade_num": row[9],
                        "db_grading_id": row[10],
                        "db_img_url": row[11]
                    }
                cursor.close()
                conn.close()
            except Exception as db_e:
                logging.error(f"Database error: {db_e}")

        # 3. Fetch Missing Alt Data (Async)
        async with httpx.AsyncClient(headers=HEADERS, timeout=10) as client:
            tasks = []
            tokens_to_fetch = []
            
            for token in tokens:
                mint = token.get('mintAddress')
                if mint in cartel_data_map:
                    continue # Already have data from DB
                
                attributes = token.get('attributes', [])
                def get_attr(attrs, trait):
                    for a in attrs:
                        if a.get('trait_type') == trait:
                            return a.get('value')
                    return None
                
                cert_id = get_attr(attributes, "Grading ID")
                grade = get_attr(attributes, "The Grade")
                company = get_attr(attributes, "Grading Company")
                
                if cert_id and grade and company:
                    tokens_to_fetch.append(mint)
                    tasks.append(get_alt_data_async(client, cert_id, grade, company))
                else:
                    tokens_to_fetch.append(mint)
                    tasks.append(asyncio.sleep(0)) # Placeholder for invalid data
            
            if tasks:
                results = await asyncio.gather(*tasks)
                for i, res in enumerate(results):
                    if res:
                        mint = tokens_to_fetch[i]
                        cartel_data_map[mint] = {
                            "alt_value": res['alt_value'],
                            "cartel_avg": res['avg_price'],
                            "supply": res['supply'],
                            "alt_asset_id": res['alt_asset_id'],
                            "alt_range": f"{res['lower_bound']} - {res['upper_bound']}"
                        }

        # 4. Format Response
        formatted_tokens = []
        for token in tokens:
            mint = token.get('mintAddress')
            attributes = token.get('attributes', [])
            
            def get_attr(attrs, trait):
                for a in attrs:
                    if a.get('trait_type') == trait:
                        return a.get('value')
                return None

            name = token.get('name')
            img = token.get('image')
            grade = get_attr(attributes, "The Grade")
            grade_num = get_attr(attributes, "GradeNum")
            grading_id = get_attr(attributes, "Grading ID")
            
            c_data = cartel_data_map.get(mint, {})
            
            final_name = c_data.get('db_name') or name
            final_grade = c_data.get('db_grade') or grade
            final_grade_num = c_data.get('db_grade_num') or grade_num
            final_img = c_data.get('db_img_url') or img
            
            alt_asset_id = c_data.get('alt_asset_id')
            alt_link = f"https://alt.xyz/assets/{alt_asset_id}" if alt_asset_id else None
            formatted_tokens.append({
                "mint": mint,
                "name": final_name,
                "grade": final_grade,
                "grading_number": final_grade_num,
                "supply": c_data.get('supply'),
                "img": final_img,
                "alt_value": c_data.get('alt_value') or "N/A",
                "alt_range": c_data.get('alt_range') or "N/A",
                "cartel_avg": c_data.get('cartel_avg') or "N/A",
                "alt_link": alt_link or "N/A"
            })

        result = {
            "wallet": wallet_address,
            "tokens": formatted_tokens,
            "count": len(formatted_tokens),
            "offset": offset,
            "limit": limit
        }

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

    except Exception as e:
        logging.error(f"Error fetching wallet holdings: {e}")
        return func.HttpResponse(
            f"Error fetching wallet holdings: {str(e)}",
            status_code=500
        )
