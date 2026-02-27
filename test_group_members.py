"""
Test script to verify WPPConnect group-members-ids endpoint.
Run: python test_group_members.py
Requires: WPP_SERVER_URL, WPP_SECRET_KEY env vars (or edit below)
"""
import asyncio
import httpx
import os

WPP_SERVER_URL = os.getenv("WPP_SERVER_URL", "http://server-cli.railway.internal:8080")
SESSION_NAME   = os.getenv("SESSION_NAME", "idsr_bot2")
WPP_SECRET_KEY = os.getenv("WPP_SECRET_KEY", "THISISMYSECURETOKEN")

# Test groups (subset)
TEST_GROUPS = {
    "DEZAPEGÃO DO ZAPÃO #C2": "120363405097636033@g.us",
    "DEZAPEGÃO DO ZAPÃO #C1": "120363408281153077@g.us",
    "DEZAPEGÃO CENTRAL":      "120363406870144681@g.us",
}

ENDPOINTS = [
    "/api/{session}/group-members-ids/{jid}",   # ← novo correto
    "/api/{session}/group-members/{jid}",        # ← antigo (pode não existir)
]

async def get_token() -> str:
    url = f"{WPP_SERVER_URL}/api/{SESSION_NAME}/{WPP_SECRET_KEY}/generate-token"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(url)
        data = resp.json()
        return data.get("token", data.get("Token", ""))

async def main():
    print(f"Server: {WPP_SERVER_URL}")
    token = await get_token()
    if not token:
        print("❌ Could not get token"); return
    print(f"✅ Token OK: {token[:20]}...\n")
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    for name, jid in TEST_GROUPS.items():
        print(f"\n{'='*60}")
        print(f"Group: {name}  ({jid})")
        for ep_template in ENDPOINTS:
            url = WPP_SERVER_URL + ep_template.format(session=SESSION_NAME, jid=jid)
            try:
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.get(url, headers=headers)
                print(f"  Endpoint: {ep_template.split('/')[-2]}")
                print(f"  Status: {resp.status_code}")
                raw = resp.text[:300]
                print(f"  Raw: {raw}")
                if resp.status_code == 200:
                    data = resp.json()
                    members = data.get("response", data) if isinstance(data, dict) else data
                    if isinstance(members, list):
                        print(f"  ✅ Count: {len(members)}")
                    else:
                        print(f"  ⚠️  Not a list: {type(members)}")
            except Exception as e:
                print(f"  ❌ Error: {e}")

asyncio.run(main())
