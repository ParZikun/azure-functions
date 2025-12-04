import logging
import azure.functions as func
import json
import httpx

async def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a buy request.')

    # Parse parameters
    try:
        req_body = req.get_json()
    except ValueError:
        req_body = {}

    buyer = req.params.get('buyer') or req_body.get('buyer')
    token_mint = req.params.get('tokenMint') or req_body.get('tokenMint')
    price = req.params.get('price') or req_body.get('price')
    seller = req.params.get('seller') or req_body.get('seller')
    token_ata = req.params.get('tokenATA') or req_body.get('tokenATA')
    auction_house = req.params.get('auctionHouseAddress') or req_body.get('auctionHouseAddress')
    
    # Optional params
    buyer_referral = req.params.get('buyerReferral') or req_body.get('buyerReferral')
    buyer_expiry = req.params.get('buyerExpiry') or req_body.get('buyerExpiry')
    seller_expiry = req.params.get('sellerExpiry') or req_body.get('sellerExpiry')
    
    if not all([buyer, token_mint, price, seller, token_ata]):
        return func.HttpResponse(
            "Missing required parameters: buyer, tokenMint, price, seller, tokenATA",
            status_code=400
        )

    # Magic Eden API URL for Buy Now
    url = "https://api-mainnet.magiceden.dev/v2/instructions/buy_now"
    
    params = {
        "buyer": buyer,
        "seller": seller,
        "tokenMint": token_mint,
        "tokenATA": token_ata,
        "price": price,
        "sellerExpiry": seller_expiry if seller_expiry else "0", # Default to 0 (no expiry) if not provided? Or required? Docs say required.
        "buyerExpiry": buyer_expiry if buyer_expiry else "0"
    }
    
    if auction_house:
        params["auctionHouseAddress"] = auction_house
    if buyer_referral:
        params["buyerReferral"] = buyer_referral

    # Add headers to mimic browser if needed, though API usually accepts standard requests
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            # The response should contain the transaction instruction(s) or the tx itself
            # ME v2/instructions/buy_now usually returns a serialized transaction or instruction data
            
            return func.HttpResponse(
                json.dumps(data),
                mimetype="application/json"
            )
            
        except httpx.HTTPStatusError as e:
            logging.error(f"ME API error: {e.response.text}")
            return func.HttpResponse(
                f"Magic Eden API Error: {e.response.text}",
                status_code=e.response.status_code
            )
        except Exception as e:
            logging.error(f"Internal error: {e}")
            return func.HttpResponse(
                f"Internal Error: {str(e)}",
                status_code=500
            )
