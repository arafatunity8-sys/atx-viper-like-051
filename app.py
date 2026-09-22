from flask import Flask, request, jsonify
import os
import asyncio
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
import binascii
import aiohttp
import requests
import json
import like_pb2
import like_count_pb2
import uid_generator_pb2
import threading
import urllib3
import random

# Configuration
TOKEN_BATCH_SIZE = 1000
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Global State for Batch Management
current_batch_indices = {}
batch_indices_lock = threading.Lock()

def get_next_batch_tokens(server_name, all_tokens):
    if not all_tokens:
        return []
    
    total_tokens = len(all_tokens)
    
    # If we have fewer tokens than batch size, use all available tokens
    if total_tokens <= TOKEN_BATCH_SIZE:
        return all_tokens
    
    with batch_indices_lock:
        if server_name not in current_batch_indices:
            current_batch_indices[server_name] = 0
        
        current_index = current_batch_indices[server_name]
        
        # Calculate the batch
        start_index = current_index
        end_index = start_index + TOKEN_BATCH_SIZE
        
        # If we reach or exceed the end, wrap around
        if end_index > total_tokens:
            remaining = end_index - total_tokens
            batch_tokens = all_tokens[start_index:total_tokens] + all_tokens[0:remaining]
        else:
            batch_tokens = all_tokens[start_index:end_index]
        
        # Update the index for next time
        next_index = (current_index + TOKEN_BATCH_SIZE) % total_tokens
        current_batch_indices[server_name] = next_index
        
        return batch_tokens

def get_random_batch_tokens(server_name, all_tokens):
    """Alternative method: use random sampling for better distribution"""
    if not all_tokens:
        return []
    
    total_tokens = len(all_tokens)
    
    # If we have fewer tokens than batch size, use all available tokens
    if total_tokens <= TOKEN_BATCH_SIZE:
        return all_tokens.copy()
    
    # Randomly select tokens without replacement
    return random.sample(all_tokens, TOKEN_BATCH_SIZE)

_token_cache = {}

def load_tokens(server_name, for_visit=False):
    cache_key = (server_name, for_visit)
    if cache_key in _token_cache:
        return _token_cache[cache_key]

    if for_visit:
        if server_name == "IND":
            path = "token_ind_visit.json"
        elif server_name in {"BR", "US", "SAC", "NA"}:
            path = "token_br_visit.json"
        else:
            path = "token_bd_visit.json"
    else:
        if server_name == "IND":
            path = "token_ind.json"
        elif server_name in {"BR", "US", "SAC", "NA"}:
            path = "token_br.json"
        else:
            path = "token_bd.json"

    try:
        with open(path, "r") as f:
            tokens = json.load(f)
            if isinstance(tokens, list) and all(isinstance(t, dict) and "token" in t for t in tokens):
                print(f"Loaded {len(tokens)} tokens from {path} for server {server_name}")
                _token_cache[cache_key] = tokens
                return tokens
            else:
                return []
    except FileNotFoundError:
        return []
    except json.JSONDecodeError:
        return []

def encrypt_message(plaintext):
    key = b'Yg&tc%DEuh6%Zc^8'
    iv = b'6oyZDr22E3ychjM%'
    cipher = AES.new(key, AES.MODE_CBC, iv)
    padded_message = pad(plaintext, AES.block_size)
    encrypted_message = cipher.encrypt(padded_message)
    return binascii.hexlify(encrypted_message).decode('utf-8')

def create_protobuf_message(user_id, region):
    message = like_pb2.like()
    message.uid = int(user_id)
    message.region = region
    return message.SerializeToString()

def create_protobuf_for_profile_check(uid):
    message = uid_generator_pb2.uid_generator()
    message.krishna_ = int(uid)
    message.teamXdarks = 1
    return message.SerializeToString()

def enc_profile_check_payload(uid):
    protobuf_data = create_protobuf_for_profile_check(uid)
    encrypted_uid = encrypt_message(protobuf_data)
    return encrypted_uid

async def send_single_like_request(session, edata, token_dict, url):
    token_value = token_dict.get("token", "")
    if not token_value:
        return 999

    headers = {
        'User-Agent': "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)",
        'Connection': "Keep-Alive",
        'Accept-Encoding': "gzip",
        'Authorization': f"Bearer {token_value}",
        'Content-Type': "application/x-www-form-urlencoded",
        'Expect': "100-continue",
        'X-Unity-Version': "2018.4.11f1",
        'X-GA': "v1 1",
        'ReleaseVersion': "OB55"
    }
    try:
        async with session.post(url, data=edata, headers=headers) as response:
            return response.status
    except asyncio.TimeoutError:
        return 998
    except Exception:
        return 997

async def send_likes_with_token_batch(uid, server_region_for_like_proto, like_api_url, token_batch_to_use):
    if not token_batch_to_use:
        return []

    like_protobuf_payload = create_protobuf_message(uid, server_region_for_like_proto)
    encrypted_like_payload = encrypt_message(like_protobuf_payload)
    edata = bytes.fromhex(encrypted_like_payload)

    connector = aiohttp.TCPConnector(
        limit=150,
        ssl=False,
        ttl_dns_cache=300,
        enable_cleanup_closed=True
    )
    timeout = aiohttp.ClientTimeout(total=4, connect=2)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [
            send_single_like_request(session, edata, token_dict, like_api_url)
            for token_dict in token_batch_to_use
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    successful_sends = sum(1 for r in results if isinstance(r, int) and r == 200)
    failed_sends = len(token_batch_to_use) - successful_sends
    print(f"Attempted {len(token_batch_to_use)} like sends. Successful: {successful_sends}, Failed/Error: {failed_sends}")
    return results

profile_session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=20)
profile_session.mount("https://", adapter)
profile_session.mount("http://", adapter)

def make_profile_check_request(encrypted_profile_payload, server_name, token_dict):
    if not token_dict:
        return None
    token_value = token_dict.get("token", "")
    if not token_value:
        return None

    if server_name == "IND":
        url = "https://client.ind.freefiremobile.com/GetPlayerPersonalShow"
    elif server_name in {"BR", "US", "SAC", "NA"}:
        url = "https://client.us.freefiremobile.com/GetPlayerPersonalShow"
    else:
        url = "https://clientbp.ppmainecoonghj.com/GetPlayerPersonalShow"

    edata = bytes.fromhex(encrypted_profile_payload)
    headers = {
        'User-Agent': "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)",
        'Connection': "Keep-Alive",
        'Accept-Encoding': "gzip",
        'Authorization': f"Bearer {token_value}",
        'Content-Type': "application/x-www-form-urlencoded",
        'Expect': "100-continue",
        'X-Unity-Version': "2018.4.11f1",
        'X-GA': "v1 1",
        'ReleaseVersion': "OB55"
    }
    try:
        response = profile_session.post(url, data=edata, headers=headers, verify=False, timeout=3.5)
        response.raise_for_status()
        return decode_protobuf_profile_info(response.content)
    except Exception:
        return None

def decode_protobuf_profile_info(binary_data):
    try:
        items = like_count_pb2.Info()
        items.ParseFromString(binary_data)
        return items
    except Exception as e:
        print(f"Error decoding Protobuf profile data: {e}")
        return None

app = Flask(__name__)

# API Key Configuration
# Default key is 'ag', you can add more keys to this set or use the API_KEYS environment variable (comma-separated)
VALID_API_KEYS = {
    "ag",
}
env_keys = os.environ.get("API_KEYS", "")
if env_keys:
    VALID_API_KEYS.update(k.strip() for k in env_keys.split(",") if k.strip())
if os.environ.get("API_KEY"):
    VALID_API_KEYS.add(os.environ.get("API_KEY").strip())

REGION_MAP = {
    "BD": "BD",
    "BANGLADESH": "BD",
    "IND": "IND",
    "IN": "IND",
    "INDIA": "IND",
    "BR": "BR",
    "BRAZIL": "BR",
    "US": "US",
    "USA": "US",
    "SAC": "SAC",
    "NA": "NA",
}

@app.route('/', methods=['GET'])
def index():
    return jsonify({
        "status": "online",
        "message": "Free Fire Like API is running",
        "usage": "/{uid}/{region}/{api_key}",
        "example": "/1312746262/bd/ag"
    })

@app.route('/<uid>/<region>/<api_key>', methods=['GET'])
@app.route('/like/<uid>/<region>/<api_key>', methods=['GET'])
@app.route('/like', methods=['GET'])
def handle_requests(uid=None, region=None, api_key=None):
    uid_param = uid or request.args.get("uid")
    region_param = region or request.args.get("region") or request.args.get("server_name")
    api_key_param = api_key or request.args.get("api_key") or request.args.get("key")
    use_random = request.args.get("random", "false").lower() == "true"

    if not api_key_param:
        return jsonify({"status": 0, "error": "API key is required"}), 401

    clean_key = str(api_key_param).strip()
    valid_keys_lower = {k.lower() for k in VALID_API_KEYS}
    if clean_key not in VALID_API_KEYS and clean_key.lower() not in valid_keys_lower:
        return jsonify({"status": 0, "error": "Invalid API key"}), 403

    if not uid_param or not region_param:
        return jsonify({"status": 0, "error": "UID and region are required"}), 400

    if not str(uid_param).isdigit():
        return jsonify({"status": 0, "error": "UID must be numeric"}), 400

    server_name_param = REGION_MAP.get(str(region_param).strip().upper(), str(region_param).strip().upper())

    # Load visit token for profile checking
    visit_tokens = load_tokens(server_name_param, for_visit=True)
    if not visit_tokens:
        return jsonify({"error": f"No visit tokens loaded for server {server_name_param}."}), 500
    
    # Use the first visit token for profile check
    visit_token = visit_tokens[0] if visit_tokens else None
    
    # Load regular tokens for like sending
    all_available_tokens = load_tokens(server_name_param, for_visit=False)
    if not all_available_tokens:
        return jsonify({"error": f"No tokens loaded or token file invalid for server {server_name_param}."}), 500

    print(f"Total tokens available for {server_name_param}: {len(all_available_tokens)}")

    # Get the batch of tokens for like sending
    if use_random:
        tokens_for_like_sending = get_random_batch_tokens(server_name_param, all_available_tokens)
        print(f"Using RANDOM batch selection for {server_name_param}")
    else:
        tokens_for_like_sending = get_next_batch_tokens(server_name_param, all_available_tokens)
        print(f"Using ROTATING batch selection for {server_name_param}")
    
    encrypted_player_uid_for_profile = enc_profile_check_payload(uid_param)
    
    # Get likes BEFORE using visit token
    before_info = make_profile_check_request(encrypted_player_uid_for_profile, server_name_param, visit_token)
    before_like_count = 0
    
    if before_info and hasattr(before_info, 'AccountInfo'):
        before_like_count = int(before_info.AccountInfo.Likes)
    else:
        print(f"Could not reliably fetch 'before' profile info for UID {uid_param} on {server_name_param}.")

    print(f"UID {uid_param} ({server_name_param}): Likes before = {before_like_count}")

    # Determine the URL for sending likes
    if server_name_param == "IND":
        like_api_url = "https://client.ind.freefiremobile.com/LikeProfile"
    elif server_name_param in {"BR", "US", "SAC", "NA"}:
        like_api_url = "https://client.us.freefiremobile.com/LikeProfile"
    else:
        like_api_url = "https://clientbp.ggpolarbear.com/LikeProfile"

    if tokens_for_like_sending:
        print(f"Using token batch for {server_name_param} (size {len(tokens_for_like_sending)}) to send likes.")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(send_likes_with_token_batch(uid_param, server_name_param, like_api_url, tokens_for_like_sending))
        finally:
            loop.close()
    else:
        print(f"Skipping like sending for UID {uid_param} as no tokens available for like sending.")
        
    # Get likes AFTER using visit token
    after_info = make_profile_check_request(encrypted_player_uid_for_profile, server_name_param, visit_token)
    after_like_count = before_like_count
    actual_player_uid_from_profile = int(uid_param)
    player_nickname_from_profile = "N/A"

    if after_info and hasattr(after_info, 'AccountInfo'):
        after_like_count = int(after_info.AccountInfo.Likes)
        actual_player_uid_from_profile = int(after_info.AccountInfo.UID)
        if after_info.AccountInfo.PlayerNickname:
            player_nickname_from_profile = str(after_info.AccountInfo.PlayerNickname)
        else:
            player_nickname_from_profile = "N/A"
    else:
        print(f"Could not reliably fetch 'after' profile info for UID {uid_param} on {server_name_param}.")

    print(f"UID {uid_param} ({server_name_param}): Likes after = {after_like_count}")

    likes_increment = after_like_count - before_like_count
    request_status = 1 if likes_increment > 0 else (2 if likes_increment == 0 else 3)

    response_data = {
        "LikesGivenByAPI": likes_increment,
        "LikesafterCommand": after_like_count,
        "LikesbeforeCommand": before_like_count,
        "PlayerNickname": player_nickname_from_profile,
        "UID": actual_player_uid_from_profile,
        "status": request_status,
        "Note": f"Used visit token for profile check and {'random' if use_random else 'rotating'} batch of {len(tokens_for_like_sending)} tokens for like sending."
    }
    return jsonify(response_data)

@app.route('/token_info', methods=['GET'])
def token_info():
    """Endpoint to check token counts for each server"""
    servers = ["IND", "BD", "BR", "US", "SAC", "NA"]
    info = {}
    
    for server in servers:
        regular_tokens = load_tokens(server, for_visit=False)
        visit_tokens = load_tokens(server, for_visit=True)
        info[server] = {
            "regular_tokens": len(regular_tokens),
            "visit_tokens": len(visit_tokens)
        }
    
    return jsonify(info)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5001))
    app.run(host='0.0.0.0', port=port, debug=False)

