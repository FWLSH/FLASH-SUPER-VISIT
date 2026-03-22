from flask import Flask, jsonify, request
import aiohttp
import asyncio
import time
import os
import json
import threading
from datetime import datetime
from byte import encrypt_api, Encrypt_ID
from visit_count_pb2 import Info

app = Flask(__name__)

TOKEN_CACHE_FILE = "tokens_cache.json"
REFRESH_INTERVAL = 2 * 60 * 60  # 2 hours

# Global cache
token_cache = {}
cache_lock = threading.Lock()

def load_accounts(server_name):
    """Load accounts from text file"""
    try:
        if server_name == "IND":
            filename = "account_ind.txt"
        elif server_name in {"BR", "US", "SAC", "NA"}:
            filename = "account_br.txt"
        else:
            filename = "account_bd.txt"
        
        accounts = []
        if os.path.exists(filename):
            with open(filename, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and ":" in line:
                        uid, password = line.split(":", 1)
                        accounts.append({
                            "uid": uid.strip(),
                            "password": password.strip()
                        })
            print(f"✅ Loaded {len(accounts)} accounts from {filename}")
            return accounts
        else:
            print(f"⚠️ File {filename} not found")
            return []
    except Exception as e:
        print(f"❌ Account load error: {e}")
        return []

async def fetch_token_single_api(session, uid, password, api_url):
    """Fetch token from a single API"""
    try:
        async with session.get(api_url, timeout=5) as response:
            if response.status == 200:
                data = await response.json()
                
                # Check different response formats
                if 'jwt_token' in data:
                    return data['jwt_token']
                elif 'token' in data:
                    return data['token']
                elif 'access_token' in data:
                    return data['access_token']
                else:
                    return None
            else:
                return None
    except Exception as e:
        print(f"⚠️ API error {api_url[:50]}: {e}")
        return None

async def fetch_token_parallel(uid, password):
    """Fetch token from multiple APIs in parallel - super fast!"""
    
    # APIs to try (add more if needed)
    apis = [
        f"https://flash-jwt.vercel.app/token?uid={uid}&password={password}",
        f"https://fast-jwt-token-api.vercel.app/token?uid={uid}&password={password}",
    ]
    
    async with aiohttp.ClientSession() as session:
        # Create tasks for all APIs (parallel execution)
        tasks = []
        for api_url in apis:
            tasks.append(fetch_token_single_api(session, uid, password, api_url))
        
        # Run all API calls simultaneously
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Return first successful token
        for token in results:
            if token and isinstance(token, str):
                return token
        
        return None

async def get_or_refresh_token(uid, password):
    """Get token from cache or fetch new one"""
    global token_cache
    
    with cache_lock:
        # Check cache
        if uid in token_cache:
            cached = token_cache[uid]
            current_time = int(time.time())
            
            # If token still valid (not expired)
            if cached.get('expires_at', 0) > current_time:
                return cached['token']
    
    # Fetch new token
    print(f"🔄 Fetching token for UID {uid}...")
    token = await fetch_token_parallel(uid, password)
    
    if token:
        with cache_lock:
            token_cache[uid] = {
                'token': token,
                'expires_at': int(time.time()) + (24 * 60 * 60),  # 24 hours
                'updated_at': int(time.time())
            }
            save_cached_tokens()
        print(f"✅ Token fetched for UID {uid}")
        return token
    else:
        print(f"❌ Failed to get token for UID {uid}")
        return None

def get_url(server_name):
    if server_name == "IND":
        return "https://client.ind.freefiremobile.com/GetPlayerPersonalShow"
    elif server_name in {"BR", "US", "SAC", "NA"}:
        return "https://client.us.freefiremobile.com/GetPlayerPersonalShow"
    else:
        return "https://clientbp.ggblueshark.com/GetPlayerPersonalShow"

def parse_protobuf_response(response_data):
    try:
        info = Info()
        info.ParseFromString(response_data)
        
        player_data = {
            "uid": info.AccountInfo.UID if info.AccountInfo.UID else 0,
            "nickname": info.AccountInfo.PlayerNickname if info.AccountInfo.PlayerNickname else "",
            "likes": info.AccountInfo.Likes if info.AccountInfo.Likes else 0,
            "region": info.AccountInfo.PlayerRegion if info.AccountInfo.PlayerRegion else "",
            "level": info.AccountInfo.Levels if info.AccountInfo.Levels else 0
        }
        return player_data
    except Exception as e:
        print(f"❌ Protobuf parsing error: {e}")
        return None

async def visit(session, url, token, uid, data):
    headers = {
        "ReleaseVersion": "OB52",
        "X-GA": "v1 1",
        "Authorization": f"Bearer {token}",
        "Host": url.replace("https://", "").split("/")[0],
        "Content-Type": "application/octet-stream"
    }
    try:
        async with session.post(url, headers=headers, data=data, ssl=False) as resp:
            if resp.status == 200:
                response_data = await resp.read()
                return True, response_data
            else:
                return False, None
    except Exception as e:
        return False, None

async def send_100_visits(accounts, target_uid, server_name):
    """Send 1000 visits using tokens from parallel API"""
    url = get_url(server_name)
    connector = aiohttp.TCPConnector(limit=0)
    total_success = 0
    total_sent = 0
    first_success_response = None
    player_info = None
    
    # Get tokens for all accounts (parallel fetch)
    print(f"🔄 Fetching tokens for {len(accounts)} accounts...")
    
    # Fetch tokens in parallel for all accounts
    token_tasks = []
    for account in accounts:
        token_tasks.append(get_or_refresh_token(account["uid"], account["password"]))
    
    tokens = await asyncio.gather(*token_tasks)
    tokens = [t for t in tokens if t]  # Filter out None
    
    if not tokens:
        print("❌ No valid tokens")
        return 0, 0, None
    
    print(f"✅ Got {len(tokens)} valid tokens")
    print(f"🎯 Target: 1000 visits")
    
    async with aiohttp.ClientSession(connector=connector) as session:
        encrypted = encrypt_api("08" + Encrypt_ID(str(target_uid)) + "1801")
        data = bytes.fromhex(encrypted)
        
        while total_success < 1000:
            batch_size = min(1000 - total_success, 1000)
            tasks = []
            
            for i in range(batch_size):
                token_index = (total_sent + i) % len(tokens)
                tasks.append(asyncio.create_task(
                    visit(session, url, tokens[token_index], target_uid, data)
                ))
            
            results = await asyncio.gather(*tasks)
            
            if first_success_response is None:
                for success, response in results:
                    if success and response:
                        first_success_response = response
                        player_info = parse_protobuf_response(response)
                        if player_info:
                            print(f"🎉 Player: {player_info.get('nickname')} (Lvl {player_info.get('level')})")
                        break
            
            batch_success = sum(1 for r, _ in results if r)
            total_success += batch_success
            total_sent += batch_size
            
            print(f"📊 Sent: {batch_size} | Success: {batch_success} | Total: {total_success}/1000")
            await asyncio.sleep(0.2)
    
    return total_success, total_sent, player_info

def save_cached_tokens():
    """Save tokens to cache file"""
    try:
        with open(TOKEN_CACHE_FILE, 'w') as f:
            json.dump(token_cache, f, indent=2)
    except Exception as e:
        print(f"❌ Cache save error: {e}")

def load_cached_tokens():
    """Load tokens from cache"""
    global token_cache
    try:
        if os.path.exists(TOKEN_CACHE_FILE):
            with open(TOKEN_CACHE_FILE, 'r') as f:
                token_cache = json.load(f)
                print(f"✅ Loaded {len(token_cache)} cached tokens")
    except Exception as e:
        print(f"⚠️ Cache load error: {e}")

def auto_refresh_worker():
    """Background thread to refresh tokens every 2 hours"""
    while True:
        time.sleep(REFRESH_INTERVAL)
        print("\n🔄 Auto-refreshing all tokens...")
        
        # Get all unique accounts
        all_accounts = []
        for server in ['IND', 'BR', 'BD']:
            accounts = load_accounts(server)
            for acc in accounts:
                if acc['uid'] not in [a['uid'] for a in all_accounts]:
                    all_accounts.append(acc)
        
        # Refresh tokens asynchronously
        async def refresh_all():
            for account in all_accounts:
                token = await fetch_token_parallel(account['uid'], account['password'])
                if token:
                    with cache_lock:
                        token_cache[account['uid']] = {
                            'token': token,
                            'expires_at': int(time.time()) + (24 * 60 * 60),
                            'updated_at': int(time.time())
                        }
            save_cached_tokens()
            print(f"✅ Auto-refresh complete! {len(all_accounts)} tokens updated")
        
        asyncio.run(refresh_all())

@app.route('/<string:server>/<int:uid>', methods=['GET'])
def send_visits(server, uid):
    """Send 1000 visits to UID"""
    server = server.upper()
    accounts = load_accounts(server)
    
    if not accounts:
        return jsonify({"error": f"No accounts for {server}"}), 400
    
    print(f"\n🚀 Sending 1000 visits to UID: {uid}")
    print(f"📁 Using {len(accounts)} accounts")
    
    total_success, total_sent, player_info = asyncio.run(
        send_100_visits(accounts, uid, server)
    )
    
    response = {
        "success": total_success,
        "fail": 1000 - total_success,
        "total_sent": total_sent,
        "accounts_used": len(accounts),
        "target": 1000
    }
    
    if player_info:
        response["player"] = {
            "uid": player_info.get("uid"),
            "name": player_info.get("nickname"),
            "level": player_info.get("level"),
            "likes": player_info.get("likes"),
            "region": player_info.get("region")
        }
    
    return jsonify(response), 200

@app.route('/refresh-tokens', methods=['POST'])
def manual_refresh():
    """Manually refresh all tokens"""
    async def refresh():
        all_accounts = []
        for server in ['IND', 'BR', 'BD']:
            accounts = load_accounts(server)
            for acc in accounts:
                if acc['uid'] not in [a['uid'] for a in all_accounts]:
                    all_accounts.append(acc)
        
        for account in all_accounts:
            token = await fetch_token_parallel(account['uid'], account['password'])
            if token:
                with cache_lock:
                    token_cache[account['uid']] = {
                        'token': token,
                        'expires_at': int(time.time()) + (24 * 60 * 60),
                        'updated_at': int(time.time())
                    }
        save_cached_tokens()
        return len(all_accounts)
    
    count = asyncio.run(refresh())
    return jsonify({"message": f"Refreshed {count} tokens"}), 200

if __name__ == "__main__":
    load_cached_tokens()
    
    # Start auto-refresh thread
    refresh_thread = threading.Thread(target=auto_refresh_worker, daemon=True)
    refresh_thread.start()
    
    print("=" * 50)
    print("🔥 SUPER FAST FREE FIRE VISIT BOT 🔥")
    print("=" * 50)
    print("⚡ Parallel API calls for instant tokens")
    print("🔄 Auto-refresh every 2 hours")
    print("💾 Token cache enabled")
    print("🎯 1000 visits per request")
    print("🌐 http://0.0.0.0:5100")
    print("=" * 50)
    
    app.run(host="0.0.0.0", port=5100, debug=False, threaded=True)