from flask import Flask, request, jsonify, render_template
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
import time

# Configuration
TOKEN_BATCH_SIZE = 100
MAX_CONCURRENT_LIKE_REQUESTS = 40
MIN_CONCURRENT_LIKE_REQUESTS = 10
LIKE_REQUEST_RETRIES = 2
LIKE_REQUEST_TIMEOUT_SECONDS = 6
PROFILE_RECHECK_ATTEMPTS = 4
PROFILE_RECHECK_DELAY_SECONDS = 1.0
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

def load_tokens(server_name, for_visit=False):
    if for_visit:
        if server_name == "CIS":
            path = "token_cis_visit.json"
        elif server_name in {"BR", "US", "SAC", "NA"}:
            path = "token_br_visit.json"
        else:
            path = "token_bd_visit.json"
    else:
        if server_name == "CIS":
            path = "token_cis.json"
        elif server_name in {"BR", "US", "SAC", "NA"}:
            path = "token_br.json"
        else:
            path = "token_bd.json"

    try:
        with open(path, "r") as f:
            tokens = json.load(f)
            if isinstance(tokens, list) and all(isinstance(t, dict) and "token" in t for t in tokens):
                print(f"Loaded {len(tokens)} tokens from {path} for server {server_name}")
                return tokens
            else:
                print(f"Warning: Token file {path} is not in the expected format. Returning empty list.")
                return []
    except FileNotFoundError:
        print(f"Warning: Token file {path} not found. Returning empty list for server {server_name}.")
        return []
    except json.JSONDecodeError:
        print(f"Warning: Token file {path} contains invalid JSON. Returning empty list.")
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

def build_like_diagnostic(likes_increment, like_send_summary, token_batch_size):
    """Explain why delivered likes can be lower than token count."""
    successful = like_send_summary.get("successful_sends", 0)
    failed = like_send_summary.get("failed_sends", 0)
    status_counts = like_send_summary.get("status_counts", {})
    retried = like_send_summary.get("retried_requests", 0)

    possible_reasons = []
    if failed > 0:
        possible_reasons.append(
            "Some requests failed (timeouts/network/HTTP errors). Check status_counts."
        )

    if successful > likes_increment:
        possible_reasons.append(
            "Some successful requests did not increase likes (possible per-profile cap, duplicate liker accounts, or server-side anti-spam filtering)."
        )

    if likes_increment == 20 and token_batch_size > 20:
        possible_reasons.append(
            "Like increase stopped at 20 while more than 20 tokens were used. This often indicates a server/game cap for the current period."
        )

    if not possible_reasons:
        possible_reasons.append("No obvious issue detected from transport layer.")

    return {
        "token_batch_size": token_batch_size,
        "successful_requests": successful,
        "failed_requests": failed,
        "likes_increment": likes_increment,
        "retried_requests": retried,
        "status_counts": status_counts,
        "possible_reasons": possible_reasons
    }

def get_dynamic_concurrency(token_batch_size):
    if token_batch_size <= 0:
        return MIN_CONCURRENT_LIKE_REQUESTS
    return max(MIN_CONCURRENT_LIKE_REQUESTS, min(MAX_CONCURRENT_LIKE_REQUESTS, token_batch_size))

def fetch_like_count_with_retry(encrypted_player_uid_for_profile, server_name_param, visit_token, base_like_count):
    """Re-check profile a few times because like counters can be eventually consistent."""
    latest_like_count = base_like_count
    latest_profile = None

    for attempt in range(1, PROFILE_RECHECK_ATTEMPTS + 1):
        profile_info = make_profile_check_request(encrypted_player_uid_for_profile, server_name_param, visit_token)
        if profile_info and hasattr(profile_info, 'AccountInfo'):
            latest_profile = profile_info
            try:
                fresh_count = int(profile_info.AccountInfo.Likes)
                if fresh_count >= latest_like_count:
                    latest_like_count = fresh_count
            except Exception:
                pass

        if attempt < PROFILE_RECHECK_ATTEMPTS:
            time.sleep(PROFILE_RECHECK_DELAY_SECONDS)

    return latest_like_count, latest_profile

async def send_single_like_request(encrypted_like_payload, token_dict, url, session, semaphore):
    edata = bytes.fromhex(encrypted_like_payload)
    token_value = token_dict.get("token", "")
    if not token_value:
        print("Warning: send_single_like_request received an empty or invalid token_dict.")
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
        'ReleaseVersion': "OB52"
    }
    retried = 0
    for attempt in range(1, LIKE_REQUEST_RETRIES + 1):
        if attempt > 1:
            retried = 1
        try:
            async with semaphore:
                async with session.post(url, data=edata, headers=headers, timeout=aiohttp.ClientTimeout(total=LIKE_REQUEST_TIMEOUT_SECONDS)) as response:
                    if response.status == 200:
                        return {"status": 200, "retried": retried}
                    print(
                        f"Like request failed for token {token_value[:10]}... "
                        f"with status: {response.status} (attempt {attempt}/{LIKE_REQUEST_RETRIES})"
                    )
                    if attempt == LIKE_REQUEST_RETRIES:
                        return {"status": response.status, "retried": retried}
        except asyncio.TimeoutError:
            print(f"Like request timed out for token {token_value[:10]}... (attempt {attempt}/{LIKE_REQUEST_RETRIES})")
            if attempt == LIKE_REQUEST_RETRIES:
                return {"status": 998, "retried": retried}
        except Exception as e:
            print(f"Exception in send_single_like_request for token {token_value[:10]}...: {e} (attempt {attempt}/{LIKE_REQUEST_RETRIES})")
            if attempt == LIKE_REQUEST_RETRIES:
                return {"status": 997, "retried": retried}

        if attempt < LIKE_REQUEST_RETRIES:
            await asyncio.sleep(0.3 * attempt)

    return {"status": 997, "retried": retried}

async def send_likes_with_token_batch(uid, server_region_for_like_proto, like_api_url, token_batch_to_use):
    if not token_batch_to_use:
        print("No tokens provided in the batch to send_likes_with_token_batch.")
        return []

    like_protobuf_payload = create_protobuf_message(uid, server_region_for_like_proto)
    encrypted_like_payload = encrypt_message(like_protobuf_payload)
    
    dynamic_concurrency = get_dynamic_concurrency(len(token_batch_to_use))
    semaphore = asyncio.Semaphore(dynamic_concurrency)
    timeout = aiohttp.ClientTimeout(total=LIKE_REQUEST_TIMEOUT_SECONDS + 2)
    connector = aiohttp.TCPConnector(limit=dynamic_concurrency * 2, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = []
        for token_dict_for_request in token_batch_to_use:
            tasks.append(
                send_single_like_request(
                    encrypted_like_payload,
                    token_dict_for_request,
                    like_api_url,
                    session,
                    semaphore
                )
            )
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    normalized_results = []
    status_counts = {}
    retried_requests = 0
    for result in results:
        if isinstance(result, dict):
            status = int(result.get("status", 997))
            retried_requests += int(result.get("retried", 0))
        elif isinstance(result, int):
            status = result
        else:
            status = 997

        normalized_results.append(status)
        status_key = str(status)
        status_counts[status_key] = status_counts.get(status_key, 0) + 1

    successful_sends = sum(1 for status in normalized_results if status == 200)
    failed_sends = len(token_batch_to_use) - successful_sends
    print(
        f"Attempted {len(token_batch_to_use)} like sends from batch with concurrency={dynamic_concurrency}. "
        f"Successful: {successful_sends}, Failed/Error: {failed_sends}"
    )
    return {
        "results": normalized_results,
        "successful_sends": successful_sends,
        "failed_sends": failed_sends,
        "status_counts": status_counts,
        "retried_requests": retried_requests,
        "used_concurrency": dynamic_concurrency
    }

def make_profile_check_request(encrypted_profile_payload, server_name, token_dict):
    token_value = token_dict.get("token", "")
    if not token_value:
        print("Warning: make_profile_check_request received an empty token_dict.")
        return None

    if server_name == "CIS":
        url = "https://clientbp.ggpolarbear.com/GetPlayerPersonalShow"
    elif server_name in {"BR", "US", "SAC", "NA"}:
        url = "https://client.us.freefiremobile.com/GetPlayerPersonalShow"
    else:
        url = "https://clientbp.ggblueshark.com/GetPlayerPersonalShow"

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
        'ReleaseVersion': "OB52"
    }
    try:
        response = requests.post(url, data=edata, headers=headers, verify=False, timeout=10)
        response.raise_for_status()
        binary_data = response.content
        decoded_info = decode_protobuf_profile_info(binary_data)
        return decoded_info
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error in make_profile_check_request for token {token_value[:10]}...: {e.response.status_code} - {e.response.text[:100]}")
    except requests.exceptions.RequestException as e:
        print(f"Request error in make_profile_check_request for token {token_value[:10]}...: {e}")
    except Exception as e:
        print(f"Unexpected error in make_profile_check_request for token {token_value[:10]}... processing response: {e}")
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


@app.route('/', methods=['GET'])
def web_interface():
    return render_template('index.html')

@app.route('/like', methods=['GET'])
def handle_requests():
    uid_param = request.args.get("uid")
    server_name_param = request.args.get("server_name", "").upper()
    use_random = request.args.get("random", "false").lower() == "true"

    if not uid_param or not server_name_param:
        return jsonify({"error": "UID and server_name are required"}), 400

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
    if server_name_param == "CIS":
        like_api_url = "https://clientbp.ggpolarbear.com/LikeProfile"
    elif server_name_param in {"BR", "US", "SAC", "NA"}:
        like_api_url = "https://client.us.freefiremobile.com/LikeProfile"
    else:
        like_api_url = "https://clientbp.ggblueshark.com/LikeProfile"

    if tokens_for_like_sending:
        print(f"Using token batch for {server_name_param} (size {len(tokens_for_like_sending)}) to send likes.")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            like_send_summary = loop.run_until_complete(
                send_likes_with_token_batch(uid_param, server_name_param, like_api_url, tokens_for_like_sending)
            )
        finally:
            loop.close()
    else:
        print(f"Skipping like sending for UID {uid_param} as no tokens available for like sending.")
        like_send_summary = {
            "results": [],
            "successful_sends": 0,
            "failed_sends": 0,
            "status_counts": {},
            "retried_requests": 0
        }
        
    # Get likes AFTER using visit token (with re-checks for eventual consistency)
    after_like_count, after_info = fetch_like_count_with_retry(
        encrypted_player_uid_for_profile,
        server_name_param,
        visit_token,
        before_like_count
    )
    actual_player_uid_from_profile = int(uid_param)
    player_nickname_from_profile = "N/A"

    # Fix: .get() hata kar direct attributes use kiye hain
    if after_info and hasattr(after_info, 'AccountInfo'):
        try:
            # Agar AccountInfo object hai toh aise chalega
            after_like_count = int(after_info.AccountInfo.Likes)
            actual_player_uid_from_profile = int(after_info.AccountInfo.UID)
            
            if hasattr(after_info.AccountInfo, 'PlayerNickname'):
                player_nickname_from_profile = str(after_info.AccountInfo.PlayerNickname)
            else:
                player_nickname_from_profile = "N/A"
        except AttributeError:
            # Agar kabhi dictionary nikla toh ye fallback hai
            after_like_count = int(after_info.AccountInfo.get('Likes', 0))
            actual_player_uid_from_profile = int(after_info.AccountInfo.get('UID', 0))
            player_nickname_from_profile = str(after_info.AccountInfo.get('PlayerNickname', 'N/A'))
    else:
        print(f"Could not reliably fetch 'after' profile info for UID {uid_param} on {server_name_param}.")

    print(f"UID {uid_param} ({server_name_param}): Likes after = {after_like_count}")

    likes_increment = after_like_count - before_like_count
    request_status = 1 if likes_increment > 0 else (2 if likes_increment == 0 else 3)
    like_diagnostic = build_like_diagnostic(likes_increment, like_send_summary, len(tokens_for_like_sending))

    response_data = {
        "LikesGivenByAPI": likes_increment,
        "LikesafterCommand": after_like_count,
        "LikesbeforeCommand": before_like_count,
        "PlayerNickname": player_nickname_from_profile,
        "UID": actual_player_uid_from_profile,
        "status": request_status,
        "LikeRequestsSuccessful": like_send_summary["successful_sends"],
        "LikeRequestsFailed": like_send_summary["failed_sends"],
        "LikeRequestStatusCounts": like_send_summary["status_counts"],
        "LikeRequestRetried": like_send_summary["retried_requests"],
        "LikeRequestConcurrency": like_send_summary.get("used_concurrency", 0),
        "LikeDiagnostic": like_diagnostic,
        "Note": (
            f"Used visit token for profile check and {'random' if use_random else 'rotating'} "
            f"batch of {len(tokens_for_like_sending)} tokens for like sending."
        )
    }
    return jsonify(response_data)

@app.route('/token_info', methods=['GET'])
def token_info():
    """Endpoint to check token counts for each server"""
    servers = ["CIS", "BD", "BR", "US", "SAC", "NA"]
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
    app.run(host='0.0.0.0', port=5001, debug=True, use_reloader=False)
