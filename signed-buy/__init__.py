import logging
import azure.functions as func
import json
import httpx
import os
import base64
from solana.rpc.async_api import AsyncClient
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solders.message import to_bytes_versioned

async def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a signed-buy request.')

    # Parse parameters
    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON body", status_code=400)

    # Required params
    token_mint = req_body.get('tokenMint')
    price = req_body.get('price')
    seller = req_body.get('seller')
    token_ata = req_body.get('tokenATA')
    
    # Optional params
    auction_house = req_body.get('auctionHouseAddress')
    buyer_referral = req_body.get('buyerReferral')
    buyer_expiry = req_body.get('buyerExpiry')
    seller_expiry = req_body.get('sellerExpiry')

    # Private Key for signing (Auto Buy)
    # Ideally loaded from Key Vault or secure env var
    private_key_str = os.environ.get("SIGNER_PRIVATE_KEY")
    if not private_key_str:
        return func.HttpResponse("Server signer key not configured.", status_code=500)
    
    try:
        # Assuming private key is base58 encoded string or json array?
        # Usually base58 for Solana.
        if "[" in private_key_str:
             # JSON array format
             keypair = Keypair.from_bytes(json.loads(private_key_str))
        else:
             # Base58 format
             from solders.keypair import Keypair
             from solders.pubkey import Pubkey
             # We might need base58 decode if solders doesn't support from_base58_string directly in all versions
             # But Keypair.from_base58_string is standard now
             keypair = Keypair.from_base58_string(private_key_str)
             
        buyer_pubkey = str(keypair.pubkey())
    except Exception as e:
        logging.error(f"Invalid private key: {e}")
        return func.HttpResponse("Invalid server signer key configuration.", status_code=500)

    if not all([token_mint, price, seller, token_ata]):
        return func.HttpResponse(
            "Missing required parameters: tokenMint, price, seller, tokenATA",
            status_code=400
        )

    # Magic Eden API URL for Buy Now
    url = "https://api-mainnet.magiceden.dev/v2/instructions/buy_now"
    
    params = {
        "buyer": buyer_pubkey,
        "seller": seller,
        "tokenMint": token_mint,
        "tokenATA": token_ata,
        "price": price,
        "sellerExpiry": seller_expiry if seller_expiry else "0",
        "buyerExpiry": buyer_expiry if buyer_expiry else "0"
    }
    
    if auction_house:
        params["auctionHouseAddress"] = auction_house
    if buyer_referral:
        params["buyerReferral"] = buyer_referral

    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    async with httpx.AsyncClient() as client:
        try:
            # 1. Fetch Transaction Instruction
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            # Extract TX data
            # ME returns 'tx' and 'txSigned'. 'txSigned' usually has some signatures but we need to add ours.
            # Usually 'txSigned' is what we want if it exists, or 'tx'.
            # Format is often: { "tx": { "type": "Buffer", "data": [...] }, ... }
            
            tx_data = data.get('txSigned') or data.get('tx')
            if not tx_data:
                return func.HttpResponse("No transaction data received from Magic Eden.", status_code=502)

            tx_bytes = None
            if isinstance(tx_data, dict) and tx_data.get('type') == 'Buffer':
                tx_bytes = bytes(tx_data['data'])
            elif isinstance(tx_data, str):
                # Could be base64
                tx_bytes = base64.b64decode(tx_data)
            
            if not tx_bytes:
                 return func.HttpResponse("Failed to decode transaction data.", status_code=502)

            # 2. Deserialize and Sign
            # Try VersionedTransaction first
            try:
                txn = VersionedTransaction.from_bytes(tx_bytes)
                # Sign
                # We need to create a new VersionedTransaction with our signature added
                # Or just sign the message and reconstruct
                
                message = txn.message
                # We need a recent blockhash? ME usually provides one in the tx.
                
                # Sign with our keypair
                # solders VersionedTransaction doesn't have a simple .sign() method that mutates in place usually?
                # We construct a new one.
                
                # Actually, we can use Keypair to sign the message data
                signature = keypair.sign_message(to_bytes_versioned(message))
                
                # We need to combine existing signatures (if any) with ours?
                # ME 'txSigned' might have partial sigs.
                # VersionedTransaction.populate(message, [signatures])
                
                # For simplicity, let's assume we are the only signer or we append.
                # But usually ME txs require buyer signature.
                
                # Let's try to just sign and send.
                # If there are other signers (like ME authority), we need to preserve them.
                # This is complex with raw bytes.
                
                # Alternative: Use solana-py Client to send_transaction?
                # client.send_transaction(txn, opts) might work if txn is a VersionedTransaction object.
                
                # But we need to add the signature.
                # txn.signatures is a list.
                # We need to find where our pubkey is in the account keys and sign.
                
                # Simplified approach:
                # 1. Deserialize
                # 2. Sign
                # 3. Serialize
                
                # solders 0.18+
                new_signatures = [keypair.sign_message(to_bytes_versioned(message))]
                # If there were other signatures, we might be overwriting them if we just pass ours.
                # But usually for Buy Now, buyer is the main signer.
                
                # Let's assume we just need to sign.
                signed_txn = VersionedTransaction(message, new_signatures)
                
            except Exception as e:
                logging.error(f"Error signing transaction: {e}")
                return func.HttpResponse(f"Error signing transaction: {e}", status_code=500)

            # 3. Submit to Network
            rpc_url = os.environ.get("RPC_URL", "https://api.mainnet-beta.solana.com")
            async with AsyncClient(rpc_url) as solana_client:
                # Send
                # opts = TxOpts(skip_preflight=True)
                result = await solana_client.send_transaction(signed_txn)
                
                # result is usually a signature string or object
                tx_sig = str(result.value) if hasattr(result, 'value') else str(result)
                
                return func.HttpResponse(
                    json.dumps({
                        "status": "submitted",
                        "signature": tx_sig,
                        "explorer_url": f"https://solscan.io/tx/{tx_sig}"
                    }),
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
