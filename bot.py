import os
import logging
import asyncio
import re
import httpx
import uvicorn
import json
import random
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from typing import List, Optional

from fastapi import FastAPI, Request, BackgroundTasks, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import traceback
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.orm import Session
from database import SessionLocal, UserStats, WhatsAppGroup, Affiliate, JoinTracking, HourlyActivity, MemberEvent, ModerationLog, init_db
import settings as cfg

# --- CONFIGURATION ---
WPP_SERVER_URL = "http://server-cli.railway.internal:8080" # FORCE URL
SESSION_NAME = cfg.get("session_name", os.getenv("SESSION_NAME", "idsr_bot2"))
WPP_SECRET_KEY = os.getenv("WPP_SECRET_KEY", "THISISMYSECURETOKEN")

# Dynamic config helpers (persisted in settings.json)
def get_super_admins(): return cfg.get("super_admins", [])
def get_whitelist_domains(): return cfg.get("whitelist_domains", [])
# Keep module-level constants for backward compat in scan/display
SUPER_ADMINS = ["5511983426767", "5511981771974"]  # Fallback only
WHITELIST_DOMAINS = ["idsr.com.br", "tvzapao.com.br"]  # Fallback only

# Fake affiliate injection — always appears at the given position in the ranking
# Set to None to disable. Change "position" to 2 to move to 2nd place.
FAKE_AFFILIATE = {"name": "Cadu", "position": 3}

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Security
security = HTTPBasic()
templates = Jinja2Templates(directory="templates")

def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = os.getenv("ADMIN_USER", "admin")
    correct_password = os.getenv("ADMIN_PASS", "admin")
    if credentials.username != correct_username or credentials.password != correct_password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

# --- WPPCONNECT CLIENT ---
class WPPConnectClient:
    def __init__(self, base_url: str, session: str, secret_key: str):
        self.base_url = base_url
        self.session = session
        self.secret_key = secret_key
        self.token = None
        self.headers = {
            "Content-Type": "application/json",
        }

    async def generate_token(self):
        """Step 1: Generate JWT token using the secret key."""
        url = f"{self.base_url}/api/{self.session}/{self.secret_key}/generate-token"
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.post(url)
                logger.info(f"Generate Token: {resp.status_code} - {resp.text}")
                if resp.status_code in (200, 201):
                    data = resp.json()
                    self.token = data.get("token")
                    self.headers["Authorization"] = f"Bearer {self.token}"
                    logger.info("Token generated successfully!")
                    return True
                else:
                    logger.error(f"Failed to generate token: {resp.text}")
                    return False
            except Exception as e:
                logger.error(f"Error generating token: {e}")
                return False

    async def start_session(self):
        """Step 2: Generate token first, then start the session."""
        token_ok = await self.generate_token()
        if not token_ok:
            logger.error("Cannot start session without a valid token.")
            return
        
        webhook_url = "http://moderador.railway.internal:8000/webhook"
        url = f"{self.base_url}/api/{self.session}/start-session"
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.post(url, json={
                    "webhook": webhook_url,
                    "waitQrCode": False
                }, headers=self.headers)
                logger.info(f"Start Session: {resp.status_code} - {resp.text[:300]}")
            except Exception as e:
                logger.error(f"Error starting session: {e}")
    
    async def subscribe_webhook(self):
        """Explicitly subscribe to webhook events. Does NOT restart the session."""
        webhook_url = "http://moderador.railway.internal:8000/webhook"
        results = []
        
        async with httpx.AsyncClient(timeout=15) as client:
            # Method 1: POST to /subscribe
            try:
                url1 = f"{self.base_url}/api/{self.session}/subscribe"
                resp = await client.post(url1, json={
                    "webhook": webhook_url,
                    "events": ["onMessage", "onAnyMessage", "onParticipantsChanged", "onRevokedMessage"]
                }, headers=self.headers)
                results.append(f"subscribe: {resp.status_code} - {resp.text[:200]}")
                logger.info(f"Subscribe webhook: {resp.status_code} - {resp.text[:200]}")
            except Exception as e:
                results.append(f"subscribe: FAILED - {e}")
                logger.warning(f"Subscribe failed: {e}")
            
            # Check session status
            try:
                url3 = f"{self.base_url}/api/{self.session}/status-session"
                resp = await client.get(url3, headers=self.headers)
                results.append(f"status: {resp.status_code} - {resp.text[:200]}")
                logger.info(f"Session status: {resp.status_code} - {resp.text[:200]}")
            except Exception as e:
                results.append(f"status: FAILED - {e}")
                logger.warning(f"Status check failed: {e}")
        
        return results

    async def start_typing(self, phone: str):
        url = f"{self.base_url}/api/{self.session}/start-typing"
        async with httpx.AsyncClient() as client:
            try:
                await client.post(url, json={"phone": phone}, headers=self.headers)
            except Exception as e:
                logger.error(f"Error start typing: {e}")

    async def stop_typing(self, phone: str):
        url = f"{self.base_url}/api/{self.session}/stop-typing"
        async with httpx.AsyncClient() as client:
            try:
                await client.post(url, json={"phone": phone}, headers=self.headers)
            except Exception as e:
                logger.error(f"Error stop typing: {e}")

    async def send_message(self, phone: str, message: str, skip_typing: bool = False):
        is_group = '@g.us' in phone
        if not skip_typing and not is_group:
            await self.start_typing(phone)
            typing_time = min(5.0, max(1.0, len(message) * 0.05)) 
            delay = typing_time + random.uniform(0.5, 2.0)
            await asyncio.sleep(delay)
        
        url = f"{self.base_url}/api/{self.session}/send-message"
        payload = {"phone": phone, "message": message, "isGroup": is_group}
        logger.info(f"SEND_MSG: phone={phone}, isGroup={is_group}, msg_len={len(message)}")
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.post(url, json=payload, headers=self.headers)
                logger.info(f"SEND_MSG resp: {resp.status_code} - {resp.text[:300]}")
                return f"{resp.status_code}: {resp.text[:200]}"
            except Exception as e:
                logger.error(f"SEND_MSG error: {e}")
                return f"error: {e}"

    async def forward_messages(self, to_chat_id: str, message_id: str):
        """Forward a message to another chat/group.
        Tries multiple WPPConnect API approaches.
        """
        is_group = '@g.us' in to_chat_id
        logger.info(f"FORWARD_MSG: to={to_chat_id[:30]}, msgId={str(message_id)[:50]}")
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Approach 1: forward-messages endpoint
            url = f"{self.base_url}/api/{self.session}/forward-messages"
            payload = {"phone": to_chat_id, "messageId": message_id, "isGroup": is_group}
            try:
                resp = await client.post(url, json=payload, headers=self.headers)
                logger.info(f"FORWARD_MSG [forward-messages] resp: {resp.status_code} - {resp.text[:500]}")
                if resp.status_code in (200, 201):
                    body = resp.json() if resp.text else {}
                    if body.get("status") != "error":
                        return True
            except Exception as e:
                logger.error(f"FORWARD_MSG [forward-messages] error: {e}")
            
            # Approach 2: try with messageId as array
            try:
                payload2 = {"phone": to_chat_id, "messageId": [message_id], "isGroup": is_group}
                resp2 = await client.post(url, json=payload2, headers=self.headers)
                logger.info(f"FORWARD_MSG [array] resp: {resp2.status_code} - {resp2.text[:500]}")
                if resp2.status_code in (200, 201):
                    body = resp2.json() if resp2.text else {}
                    if body.get("status") != "error":
                        return True
            except Exception as e:
                logger.error(f"FORWARD_MSG [array] error: {e}")
        
        return False

    async def delete_message(self, phone: str, message_id: str):
        is_group = '@g.us' in phone
        url = f"{self.base_url}/api/{self.session}/delete-message"
        payload = {"phone": phone, "messageId": message_id, "isGroup": is_group}
        logger.info(f"DELETE_MSG: phone={phone}, isGroup={is_group}, msgId={message_id}")
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.post(url, json=payload, headers=self.headers)
                logger.info(f"DELETE_MSG resp: {resp.status_code} - {resp.text[:500]}")
                return f"{resp.status_code}: {resp.text[:200]}"
            except Exception as e:
                logger.error(f"DELETE_MSG error: {e}")
                return f"error: {e}"

    async def get_all_groups(self):
        url = f"{self.base_url}/api/{self.session}/all-groups"
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(url, headers=self.headers)
                if resp.status_code == 200:
                    data = resp.json()
                    return data.get('response', data) if isinstance(data, dict) else data
                logger.error(f"get_all_groups: {resp.status_code} - {resp.text[:200]}")
                return []
            except Exception as e:
                logger.error(f"Error getting groups: {e}")
                return []

    async def get_group_participants(self, group_id: str):
        """Get group members. Uses group-members endpoint (group-participants returns 404)."""
        url = f"{self.base_url}/api/{self.session}/group-members/{group_id}"
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(url, headers=self.headers)
                if resp.status_code == 200:
                    data = resp.json()
                    return data.get('response', data) if isinstance(data, dict) else data
                logger.warning(f"get_group_participants: status={resp.status_code} for {group_id[:20]}")
                return []
            except Exception as e:
                logger.error(f"Error getting participants for {group_id}: {e}")
                return []

    async def get_group_admins(self, group_id: str):
        url = f"{self.base_url}/api/{self.session}/group-admins/{group_id}"
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(url, headers=self.headers)
                if resp.status_code == 200:
                    data = resp.json()
                    return data.get('response', data) if isinstance(data, dict) else data
                return []
            except Exception as e:
                logger.error(f"Error getting admins for {group_id}: {e}")
                return []

    async def get_group_invite_link(self, group_id: str):
        """Get the invite link for a group."""
        url = f"{self.base_url}/api/{self.session}/group-invite-link/{group_id}"
        async with httpx.AsyncClient(timeout=15) as client:
            try:
                resp = await client.get(url, headers=self.headers)
                if resp.status_code == 200:
                    data = resp.json()
                    link = data.get('response', data.get('link', ''))
                    if isinstance(link, str) and link:
                        return link if link.startswith('http') else f"https://chat.whatsapp.com/{link}"
                return ""
            except Exception as e:
                logger.error(f"Error getting invite link for {group_id}: {e}")
                return ""

    async def remove_participant(self, group_id: str, participant_id: str):
        """Remove a participant from a group.
        WPPConnect Server expects: POST {groupId, phone} where phone is a bare number.
        contactToArray() on server side adds @c.us (<=14 chars) or @lid (>14 chars).
        """
        url = f"{self.base_url}/api/{self.session}/remove-participant-group"

        # Extract the bare number (strip @c.us, @lid, @s.whatsapp.net etc)
        base = participant_id.split('@')[0]

        # Try multiple formats:
        # 1. Bare number (WPPConnect's contactToArray will auto-add @c.us or @lid)
        # 2. Full ID with @lid suffix (in case server version handles it directly)
        # 3. Full ID with @c.us suffix
        formats_to_try = [
            base,                # bare number — let server decide @c.us vs @lid
            f"{base}@lid",       # explicit LID format
            f"{base}@c.us",      # explicit phone format
        ]
        # Remove duplicates while preserving order
        seen = set()
        unique_formats = []
        for f in formats_to_try:
            if f not in seen:
                seen.add(f)
                unique_formats.append(f)

        async with httpx.AsyncClient(timeout=30) as client:
            for fmt in unique_formats:
                try:
                    # WPPConnect expects "phone" field, NOT "participantId"
                    payload = {"groupId": group_id, "phone": fmt}
                    logger.info(f"REMOVE_PARTICIPANT: group={group_id[:30]}, phone={fmt}")
                    resp = await client.post(url, json=payload, headers=self.headers)
                    body = resp.text[:500]
                    logger.info(f"REMOVE_PARTICIPANT response: status={resp.status_code}, body={body}")

                    # Accept any 2xx as potential success
                    if 200 <= resp.status_code < 300:
                        body_lower = body.lower()
                        if '"status":"success"' in body_lower or '"success"' in body_lower:
                            logger.info(f"REMOVE_PARTICIPANT: ✅ SUCCESS with phone={fmt}")
                            return True
                        elif '"error"' in body_lower and '"status":"error"' in body_lower:
                            logger.warning(f"REMOVE_PARTICIPANT: 2xx but server error in body with phone={fmt}: {body}")
                            continue
                        else:
                            # 2xx without clear error — treat as success
                            logger.info(f"REMOVE_PARTICIPANT: ✅ 2xx assumed success with phone={fmt}")
                            return True
                    else:
                        logger.warning(f"REMOVE_PARTICIPANT: HTTP {resp.status_code} with phone={fmt}, body={body}")
                        continue
                except Exception as e:
                    logger.error(f"REMOVE_PARTICIPANT error with phone={fmt}: {e}")
                    continue

        logger.error(f"REMOVE_PARTICIPANT: ALL formats failed for {participant_id} in {group_id[:30]}")
        return False

    async def approve_participant(self, group_id: str, participant_id: str):
        """Approve a pending membership request for a group."""
        url = f"{self.base_url}/api/{self.session}/approve-participant-group"
        payload = {"groupId": group_id, "participantId": [participant_id]}
        logger.info(f"APPROVE_PARTICIPANT: group={group_id}, participant={participant_id}")
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.post(url, json=payload, headers=self.headers)
                logger.info(f"APPROVE_PARTICIPANT response: {resp.status_code} - {resp.text[:200]}")
                return resp.status_code == 200
            except Exception as e:
                logger.error(f"Error approving participant: {e}")
                return False

    async def reject_participant(self, group_id: str, participant_id: str):
        """Reject a pending membership request for a group."""
        url = f"{self.base_url}/api/{self.session}/reject-participant-group"
        payload = {"groupId": group_id, "participantId": [participant_id]}
        logger.info(f"REJECT_PARTICIPANT: group={group_id}, participant={participant_id}")
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.post(url, json=payload, headers=self.headers)
                logger.info(f"REJECT_PARTICIPANT response: {resp.status_code} - {resp.text[:200]}")
                return resp.status_code == 200
            except Exception as e:
                logger.error(f"Error rejecting participant: {e}")
                return False

    async def get_membership_requests(self, group_id: str):
        """Get pending membership approval requests for a group.
        NOTE: Standard WPPConnect Server may not have this endpoint.
        We try multiple possible endpoint paths.
        """
        # Try multiple possible endpoint paths
        possible_urls = [
            f"{self.base_url}/api/{self.session}/get-membership-approval-requests/{group_id}",
            f"{self.base_url}/api/{self.session}/membership-approval-requests/{group_id}",
        ]
        
        async with httpx.AsyncClient(timeout=30) as client:
            for url in possible_urls:
                try:
                    logger.info(f"GET_MEMBERSHIP_REQUESTS: trying {url}")
                    resp = await client.get(url, headers=self.headers)
                    logger.info(f"GET_MEMBERSHIP_REQUESTS response: {resp.status_code} - {resp.text[:300]}")
                    if resp.status_code == 200:
                        data = resp.json()
                        if isinstance(data, dict):
                            result = data.get('response', data.get('data', []))
                            if isinstance(result, list):
                                logger.info(f"GET_MEMBERSHIP_REQUESTS: found {len(result)} requests for {group_id[:20]}")
                                return result
                        elif isinstance(data, list):
                            return data
                    elif resp.status_code == 404:
                        logger.warning(f"GET_MEMBERSHIP_REQUESTS: endpoint not found: {url}")
                        continue
                    else:
                        logger.warning(f"GET_MEMBERSHIP_REQUESTS: status {resp.status_code} for {url}")
                        continue
                except Exception as e:
                    logger.error(f"GET_MEMBERSHIP_REQUESTS error for {url}: {e}")
                    continue
        
        logger.warning(f"GET_MEMBERSHIP_REQUESTS: no working endpoint found for {group_id[:20]}")
        return []

    async def check_session_status(self) -> dict:
        """Check if the WPP session is connected. Returns dict with 'connected' bool and 'status' string."""
        url = f"{self.base_url}/api/{self.session}/check-connection-session"
        async with httpx.AsyncClient(timeout=15) as client:
            try:
                resp = await client.get(url, headers=self.headers)
                if resp.status_code == 200:
                    data = resp.json()
                    # WPPConnect returns {"status": true/false} or {"response": true/false}
                    connected = data.get('response', data.get('status', False))
                    return {"connected": bool(connected), "status": "connected" if connected else "disconnected", "raw": data}
                # Try fallback endpoint
                url2 = f"{self.base_url}/api/{self.session}/status-session"
                resp2 = await client.get(url2, headers=self.headers)
                if resp2.status_code == 200:
                    data2 = resp2.json()
                    status_val = data2.get('response', data2.get('status', ''))
                    connected = status_val in (True, 'CONNECTED', 'isLogged', 'inChat')
                    return {"connected": connected, "status": str(status_val), "raw": data2}
                return {"connected": False, "status": f"http_{resp.status_code}", "raw": {}}
            except Exception as e:
                logger.error(f"Error checking session status: {e}")
                return {"connected": False, "status": f"error: {e}", "raw": {}}

    async def full_reconnect(self):
        """Full reconnection: regenerate token + restart session + re-subscribe webhook."""
        logger.warning("WPP: Starting full reconnection sequence...")
        try:
            await self.generate_token()
            await asyncio.sleep(2)
            await self.start_session()
            await asyncio.sleep(5)
            await self.subscribe_webhook()
            logger.info("WPP: Full reconnection sequence completed")
            # NOTE: sync_historical_messages is NOT called here
            # It will run only when session is confirmed connected
            return True
        except Exception as e:
            logger.error(f"WPP: Full reconnection failed: {e}")
            return False

    async def logout_session(self):
        """Logout the WPP session (clears auth, will need QR code scan again)."""
        url = f"{self.base_url}/api/{self.session}/logout-session"
        async with httpx.AsyncClient(timeout=15) as client:
            try:
                resp = await client.post(url, headers=self.headers)
                logger.info(f"Logout Session: {resp.status_code} - {resp.text[:200]}")
                return resp.status_code in (200, 201)
            except Exception as e:
                logger.error(f"Error logging out session: {e}")
                return False

    async def close_session(self):
        """Close the WPP session without logging out."""
        url = f"{self.base_url}/api/{self.session}/close-session"
        async with httpx.AsyncClient(timeout=15) as client:
            try:
                resp = await client.post(url, headers=self.headers)
                logger.info(f"Close Session: {resp.status_code} - {resp.text[:200]}")
                return resp.status_code in (200, 201)
            except Exception as e:
                logger.error(f"Error closing session: {e}")
                return False

    async def delete_session(self):
        """Delete session data completely (Ultra Deep Clean)."""
        async with httpx.AsyncClient(timeout=20) as client:
            # 1. Try Logout
            try:
                await client.post(f"{self.base_url}/api/{self.session}/logout-session", headers=self.headers)
                await asyncio.sleep(2)
            except: pass
            
            # 2. Try Close
            try:
                await client.post(f"{self.base_url}/api/{self.session}/close-session", headers=self.headers)
                await asyncio.sleep(2)
            except: pass
            
            # 3. Clear session data (important for cache wipe)
            try:
                url_clear = f"{self.base_url}/api/{self.session}/clear-session-data"
                resp_clear = await client.post(url_clear, headers=self.headers)
                logger.info(f"Clear Session Data: {resp_clear.status_code}")
                await asyncio.sleep(2)
            except Exception as e:
                logger.warning(f"Clear session data failed: {e}")

            # 4. DELETE the session (final blow)
            try:
                url_del = f"{self.base_url}/api/{self.session}"
                resp_del = await client.delete(url_del, headers=self.headers)
                logger.info(f"Delete Session: {resp_del.status_code}")
            except Exception as e:
                logger.warning(f"Delete session failed: {e}")


    async def get_messages(self, chat_id: str, count: int = 100) -> list:
        """Load recent messages from a chat/group."""
        url = f"{self.base_url}/api/{self.session}/all-messages-in-chat/{chat_id}"
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(url, params={"count": count}, headers=self.headers)
                if resp.status_code == 200:
                    data = resp.json()
                    messages = data.get('response', data) if isinstance(data, dict) else data
                    return messages if isinstance(messages, list) else []
                logger.warning(f"get_messages {chat_id}: {resp.status_code}")
                return []
            except Exception as e:
                logger.error(f"Error getting messages for {chat_id}: {e}")
                return []

    async def send_reaction(self, msg_id: str, chat_id: str, emoji: str) -> bool:
        """React to a message with an emoji."""
        url = f"{self.base_url}/api/{self.session}/react-message"
        payload = {"msgId": msg_id, "reaction": emoji}
        async with httpx.AsyncClient(timeout=15) as client:
            try:
                resp = await client.post(url, json=payload, headers=self.headers)
                logger.info(f"REACTION: {emoji} on {str(msg_id)[:30]}: {resp.status_code} - {resp.text[:100]}")
                return resp.status_code in (200, 201)
            except Exception as e:
                logger.warning(f"send_reaction error: {e}")
                return False

wpp = WPPConnectClient(WPP_SERVER_URL, SESSION_NAME, WPP_SECRET_KEY)

# --- SCHEDULER TASKS & WATCHDOG ---
_last_message_time = datetime.now()  # Track last onmessage event
_WATCHDOG_STALE_MINUTES = 10  # Consider stale if no message in this many minutes
_session_status = "unknown"  # Track current session status
_consecutive_failures = 0  # Track reconnect attempts
_last_reconnect_time = None  # Cooldown: don't reconnect too often
_RECONNECT_COOLDOWN_MINUTES = 15  # Wait at least 15 min between reconnect attempts

async def job_watchdog():
    """Check session health and reconnect if needed."""
    global _last_message_time, _session_status, _consecutive_failures, _last_reconnect_time
    
    # Step 1: Check actual session status via WPP API
    status_info = await wpp.check_session_status()
    _session_status = status_info["status"]
    
    if not status_info["connected"]:
        # If session is INITIALIZING (waiting for QR scan), do NOT interfere
        raw_status = str(status_info.get("raw", {}).get("response", "")).upper()
        if "INITIALIZING" in raw_status or "INITIALIZING" in _session_status.upper():
            logger.info(f"WATCHDOG: Session INITIALIZING (waiting for QR scan) - NOT reconnecting")
            return
        
        # Cooldown: don't reconnect if we just did recently
        if _last_reconnect_time:
            mins_since = (datetime.now() - _last_reconnect_time).total_seconds() / 60
            if mins_since < _RECONNECT_COOLDOWN_MINUTES:
                logger.info(f"WATCHDOG: Cooldown active ({mins_since:.0f}/{_RECONNECT_COOLDOWN_MINUTES} min) - skipping reconnect")
                return
        
        _consecutive_failures += 1
        logger.warning(f"WATCHDOG: Session DISCONNECTED (status={_session_status}, attempt #{_consecutive_failures})")
        
        # Full reconnect (includes historical sync)
        _last_reconnect_time = datetime.now()
        success = await wpp.full_reconnect()
        if success:
            _last_message_time = datetime.now()
            logger.info("WATCHDOG: Reconnection successful")
        else:
            logger.error(f"WATCHDOG: Reconnection failed (attempt #{_consecutive_failures})")
        return
    
    # Step 2: Session is connected, but check message heartbeat
    elapsed = (datetime.now() - _last_message_time).total_seconds() / 60
    if elapsed >= _WATCHDOG_STALE_MINUTES:
        logger.warning(f"WATCHDOG: Connected but no messages in {elapsed:.0f} min. Re-subscribing webhook...")
        try:
            await wpp.subscribe_webhook()
            _last_message_time = datetime.now()
            logger.info("WATCHDOG: Webhook re-subscribed")
        except Exception as e:
            logger.error(f"WATCHDOG: Re-subscribe failed: {e}")
    else:
        _consecutive_failures = 0  # Reset on healthy check
        logger.info(f"WATCHDOG: OK - connected, last message {elapsed:.0f} min ago")

async def sync_historical_messages():
    """Read recent messages from monitored groups and rebuild flood counters.
    Only runs if session is confirmed connected."""
    try:
        # SAFETY: Check session is actually connected before calling APIs
        status = await wpp.check_session_status()
        if not status["connected"]:
            logger.warning(f"SYNC: Skipping - session not connected (status={status['status']})")
            return
        monitored = cfg.get("monitored_groups", [])
        if not monitored:
            logger.info("SYNC: No monitored groups configured, skipping")
            return
        
        window_hours = cfg.get("flood_window_hours", 2)
        max_msgs = cfg.get("flood_max_messages", 1)
        now = datetime.now()
        cutoff = now - timedelta(hours=window_hours)
        
        # Get admin IDs to skip them
        admin_ids_all = set()
        super_admins = set(get_super_admins())
        
        db = SessionLocal()
        total_synced = 0
        
        try:
            for group_id in monitored:
                logger.info(f"SYNC: Scanning messages in {group_id}...")
                
                # Get group admins
                group_admins = await get_group_admin_ids(group_id)
                skip_ids = group_admins | super_admins
                
                # Fetch recent messages (last 200)
                messages = await wpp.get_messages(group_id, count=200)
                
                # Count messages per user within the flood window
                user_counts = {}  # user_id -> {"msgs": int, "photos": int, "first_time": datetime}
                
                for msg in messages:
                    if not isinstance(msg, dict):
                        continue
                    
                    # Skip bot's own messages
                    if msg.get('fromMe', False):
                        continue
                    
                    # Get timestamp
                    ts = msg.get('timestamp', 0)
                    if isinstance(ts, (int, float)):
                        msg_time = datetime.fromtimestamp(ts)
                    else:
                        continue
                    
                    # Skip messages outside the flood window
                    if msg_time < cutoff:
                        continue
                    
                    # Get sender
                    sender = ''
                    sender_obj = msg.get('sender', {})
                    if isinstance(sender_obj, dict):
                        sender = sender_obj.get('id', {}).get('_serialized', '') if isinstance(sender_obj.get('id'), dict) else str(sender_obj.get('id', ''))
                    if not sender:
                        sender = msg.get('from', '') or msg.get('author', '')
                    if not sender:
                        continue
                    
                    # Skip admins
                    sender_clean = sender.split('@')[0]
                    if sender in skip_ids or sender_clean in skip_ids:
                        continue
                    
                    # Count
                    if sender not in user_counts:
                        user_counts[sender] = {"msgs": 0, "photos": 0, "first_time": msg_time}
                    
                    user_counts[sender]["msgs"] += 1
                    
                    has_media = bool(msg.get('isMedia', False) or msg.get('mimetype', ''))
                    if has_media:
                        user_counts[sender]["photos"] += 1
                    
                    # Track earliest message time for window_start
                    if msg_time < user_counts[sender]["first_time"]:
                        user_counts[sender]["first_time"] = msg_time
                
                # Update database
                for user_id, counts in user_counts.items():
                    stats = db.query(UserStats).filter_by(user_id=user_id, group_id=group_id).first()
                    
                    if not stats:
                        stats = UserStats(
                            user_id=user_id,
                            group_id=group_id,
                            last_active_date=date.today(),
                            window_start=counts["first_time"],
                            message_count=counts["msgs"],
                            photo_count=counts["photos"]
                        )
                        db.add(stats)
                    else:
                        # Only update if our count is higher (don't reduce counts)
                        if counts["msgs"] > stats.message_count:
                            stats.message_count = counts["msgs"]
                        if counts["photos"] > stats.photo_count:
                            stats.photo_count = counts["photos"]
                        if not stats.window_start or counts["first_time"] < stats.window_start:
                            stats.window_start = counts["first_time"]
                    
                    total_synced += 1
                
                logger.info(f"SYNC: {group_id} - {len(user_counts)} users synced from {len(messages)} messages")
            
            db.commit()
            logger.info(f"SYNC: Complete - {total_synced} user records updated across {len(monitored)} groups")
        
        finally:
            db.close()
    
    except Exception as e:
        logger.error(f"SYNC: Error syncing historical messages: {e}\n{traceback.format_exc()}")

async def job_open_groups():
    logger.info("Scheduler: Opening groups...")
    groups = await wpp.get_all_groups()
    for g in groups:
        gid = g.get('id', {}).get('_serialized') if isinstance(g.get('id'), dict) else g.get('id')
        if gid:
            async with httpx.AsyncClient() as client:
                 await client.post(f"{WPP_SERVER_URL}/api/{SESSION_NAME}/groups/settings/property", 
                                   json={"id": gid, "property": "announcement", "value": False})

async def job_close_groups():
    logger.info("Scheduler: Closing groups...")
    groups = await wpp.get_all_groups()
    for g in groups:
        gid = g.get('id', {}).get('_serialized') if isinstance(g.get('id'), dict) else g.get('id')
        if gid:
            async with httpx.AsyncClient() as client:
                 await client.post(f"{WPP_SERVER_URL}/api/{SESSION_NAME}/groups/settings/property", 
                                   json={"id": gid, "property": "announcement", "value": True})

async def job_sync_invite_links():
    """Periodically check and update invite links for all managed groups.
    When a group link is revoked/reset in WhatsApp, this job detects the change
    and updates the database so affiliate links and landing pages keep working."""
    try:
        # Check session first (same pattern as watchdog)
        status = await wpp.check_session_status()
        if not status.get("connected"):
            logger.info("INVITE_LINK_SYNC: Session not connected, skipping.")
            return

        db = SessionLocal()
        try:
            groups = db.query(WhatsAppGroup).all()
            if not groups:
                return

            updated = 0
            for g in groups:
                try:
                    live_link = await wpp.get_group_invite_link(g.group_jid)
                    if not live_link:
                        continue
                    if live_link != g.invite_link:
                        old_link = g.invite_link or "(vazio)"
                        g.invite_link = live_link
                        updated += 1
                        logger.info(f"INVITE_LINK_UPDATED: {g.name} | old={old_link[:40]} -> new={live_link[:40]}")
                except Exception as e:
                    logger.warning(f"INVITE_LINK_SYNC: Error checking {g.name}: {e}")
                # Small delay between groups to avoid API rate limits
                await asyncio.sleep(2)

            if updated > 0:
                db.commit()
                logger.info(f"INVITE_LINK_SYNC: {updated} link(s) updated out of {len(groups)} groups.")
            else:
                logger.info(f"INVITE_LINK_SYNC: All {len(groups)} links up to date.")
        finally:
            db.close()
    except Exception as e:
        logger.error(f"INVITE_LINK_SYNC: Error: {e}")

# --- EXCLUSIVITY AUTO-CLEANUP ---
_exclusivity_report = {
    "date": datetime.now().strftime("%Y-%m-%d"),
    "removed_members": [],   # {"phone", "removed_from", "kept_in", "time"}
    "last_run": None
}
_exclusivity_running = False

async def job_exclusive_cleanup(force=False, group_ids: list = None):
    """Automatic job: remove duplicate members and reject duplicate pending requests.
    
    Args:
        force: Skip session connectivity check
        group_ids: Optional list of group JIDs to restrict the scan to. If None, scan all groups.
    """
    global _exclusivity_report
    try:
        # Reset report if new day
        today = datetime.now().strftime("%Y-%m-%d")
        if _exclusivity_report["date"] != today:
            _exclusivity_report = {"date": today, "removed_members": [], "last_run": None}



        if not force:
            status = await wpp.check_session_status()
            if not status.get("connected"):
                logger.info("EXCLUSIVE_JOB: session not connected, skipping.")
                return

        await ensure_token()

        # Use ALL bot groups (not just managed ones in the redirector)
        all_groups_raw = await wpp.get_all_groups()
        if not all_groups_raw:
            logger.info("EXCLUSIVE_JOB: no groups found, skipping.")
            _exclusivity_report["last_run"] = datetime.now().strftime("%H:%M:%S")
            return

        # Parse into list of (jid, name) sorted by name for consistent ordering
        managed = []
        for g in all_groups_raw:
            if isinstance(g, dict):
                gid = g.get("id", g.get("_serialized", ""))
                if isinstance(gid, dict):
                    gid = gid.get("_serialized", "")
                gname = g.get("name", g.get("subject", str(gid)[:20]))
            elif isinstance(g, str):
                gid, gname = g, g[:20]
            else:
                continue
            if gid:
                managed.append({"jid": str(gid), "name": str(gname)})

        # Filter to user-selected groups if provided
        if group_ids:
            selected = set(str(g) for g in group_ids)
            managed = [g for g in managed if g["jid"] in selected]
            logger.info(f"EXCLUSIVE_JOB: filtered to {len(managed)} group(s) out of {len(managed)+len(selected - set(g['jid'] for g in managed))} by user selection")

        if len(managed) < 2:
            logger.info(f"EXCLUSIVE_JOB: only {len(managed)} group(s), nothing to deduplicate.")
            _exclusivity_report["last_run"] = datetime.now().strftime("%H:%M:%S")
            return

        def extract_phone(member):
            """Extract identifier from any WPPConnect participant format."""
            if isinstance(member, str):
                return member.split('@')[0]
            if isinstance(member, dict):
                for field in ['id', '_serialized', 'user']:
                    val = member.get(field, '')
                    if val:
                        if isinstance(val, dict):
                            inner = val.get('_serialized', val.get('user', ''))
                            if inner:
                                return str(inner).split('@')[0]
                        else:
                            return str(val).split('@')[0]
            return None

        def extract_raw_id(member):
            """Extract the full raw serialized ID for API calls."""
            if isinstance(member, str):
                return member
            if isinstance(member, dict):
                mid = member.get('id', '')
                if isinstance(mid, dict):
                    return mid.get('_serialized', '')
                elif mid:
                    return str(mid)
            return ''

        # --- Part 1: Remove duplicate members ---
        # ALWAYS start with hardcoded SUPER_ADMINS as safety net
        admin_phones = set(get_super_admins())
        logger.info(f"EXCLUSIVE_JOB: hardcoded super_admins: {list(admin_phones)}")

        for g in managed:
            try:
                admins_raw = await wpp.get_group_admins(g["jid"])
                logger.info(f"EXCLUSIVE_JOB: group-admins raw response for {g['name']}: {str(admins_raw)[:300]}")
                items = []
                if isinstance(admins_raw, list):
                    # Some versions return [[...]] (double-nested)
                    if admins_raw and isinstance(admins_raw[0], list):
                        items = admins_raw[0]
                    else:
                        items = admins_raw
                elif isinstance(admins_raw, dict):
                    items = admins_raw.get("response", admins_raw.get("participants", []))
                for a in items:
                    admin_phone = extract_phone(a)
                    if admin_phone and len(admin_phone) >= 8:
                        admin_phones.add(admin_phone)
                logger.info(f"EXCLUSIVE_JOB: {g['name']} → {len(items)} admin(s) parsed")
            except Exception as e:
                logger.error(f"EXCLUSIVE_JOB: error fetching admins for {g['name']}: {e}")

        logger.info(f"EXCLUSIVE_JOB: total admin phones to EXCLUDE ({len(admin_phones)}): {list(admin_phones)}")
        # NOTE: We intentionally do NOT abort if no admins are detected beyond hardcoded list,
        # because get_group_admins may be unavailable; super_admins are always protected.

        # Build phone -> list of (group_jid, group_name, raw_id)
        phone_groups = {}
        for i, g in enumerate(managed):
            try:
                # Pace reading to avoid rapid API calls
                if i > 0:
                    await asyncio.sleep(random.uniform(3.0, 5.0))
                
                members_raw = await wpp.get_group_participants(g["jid"])
                count = len(members_raw) if isinstance(members_raw, list) else 0
                logger.info(f"EXCLUSIVE_JOB: group={g['name']}, members={count}")
                if isinstance(members_raw, list) and count > 0:
                    for m in members_raw:
                        phone = extract_phone(m)
                        raw_id = extract_raw_id(m)
                        if phone and len(phone) >= 8:
                            if phone not in phone_groups:
                                phone_groups[phone] = []
                            phone_groups[phone].append((g["jid"], g["name"], raw_id))
            except Exception as e:
                logger.error(f"EXCLUSIVE_JOB: error fetching {g['name']}: {e}")

        # Exclude admins from duplicate detection — they MUST stay in all groups
        duplicates = {p: gs for p, gs in phone_groups.items() if len(gs) >= 2 and p not in admin_phones}
        logger.info(f"EXCLUSIVE_JOB: total_phones={len(phone_groups)}, admins_excluded={len(admin_phones)}, duplicates={len(duplicates)}")

        removed_count = 0
        failed_count = 0
        
        # Prepare flat list of removals
        to_remove = []
        for phone, groups in duplicates.items():
            keep = groups[0]
            for group_jid, group_name, raw_id in groups[1:]:
                to_remove.append({
                    "phone": phone, 
                    "group_jid": group_jid, 
                    "group_name": group_name, 
                    "raw_id": raw_id,
                    "keep_name": keep[1]
                })

        # Process removals in BATCHES with long pauses
        BATCH_SIZE = 5
        for i, item in enumerate(to_remove):
            phone = item["phone"]
            group_jid = item["group_jid"]
            group_name = item["group_name"]
            raw_id = item["raw_id"]
            keep_name = item["keep_name"]

            try:
                # Long randomized delay between individual removals (30 - 90 seconds)
                if i > 0:
                    delay = random.uniform(30.0, 90.0)
                    logger.info(f"EXCLUSIVE_JOB: safer-pause {delay:.1f}s before next removal...")
                    await asyncio.sleep(delay)

                # Batch pause: every BATCH_SIZE removals, take a long break (3 - 6 minutes)
                if i > 0 and i % BATCH_SIZE == 0:
                    batch_pause = random.uniform(180.0, 360.0)
                    logger.info(f"EXCLUSIVE_JOB: 🛑 BATCH COMPLETED. Taking a long rest for {batch_pause/60:.1f} minutes...")
                    await asyncio.sleep(batch_pause)

                id_to_use = raw_id if raw_id else f"{phone}@c.us"
                logger.info(f"EXCLUSIVE_JOB: [Batch {i//BATCH_SIZE + 1}] removing {phone} from {group_name}")
                
                success = await wpp.remove_participant(group_jid, id_to_use)
                if success:
                    removed_count += 1
                    _exclusivity_report["removed_members"].append({
                        "phone": phone, "removed_from": group_name,
                        "kept_in": keep_name, "time": datetime.now().strftime("%H:%M")
                    })
                    logger.info(f"EXCLUSIVE_JOB: ✅ removed {phone} from {group_name}")
                else:
                    failed_count += 1
                    _exclusivity_report["removed_members"].append({
                        "phone": phone, "removed_from": f"❌ FALHOU: {group_name}",
                        "kept_in": keep_name, "time": datetime.now().strftime("%H:%M")
                    })
                    logger.warning(f"EXCLUSIVE_JOB: ❌ FAILED to remove {phone} from {group_name}")
            except Exception as e:
                failed_count += 1
                logger.error(f"EXCLUSIVE_JOB: remove error {phone} from {group_name}: {e}")

        # --- Part 3: POST-RUN VALIDATION ---
        logger.info("EXCLUSIVE_JOB: starting post-run validation...")
        validation_phone_groups = {}
        for g in managed:
            try:
                members_raw = await wpp.get_group_participants(g["jid"])
                if isinstance(members_raw, list):
                    for m in members_raw:
                        phone = extract_phone(m)
                        if phone and len(phone) >= 8:
                            if phone not in validation_phone_groups:
                                validation_phone_groups[phone] = []
                            validation_phone_groups[phone].append(g["name"])
            except Exception as e:
                logger.error(f"EXCLUSIVE_JOB: validation error for {g['name']}: {e}")

        remaining_duplicates = {p: gs for p, gs in validation_phone_groups.items() if len(gs) >= 2}
        _exclusivity_report["validation"] = {
            "remaining_duplicates": len(remaining_duplicates),
            "total_unique": len(validation_phone_groups),
            "effective_removals": len(duplicates) - len(remaining_duplicates),
            "sample_remaining": [{"phone": p, "groups": gs} for p, gs in list(remaining_duplicates.items())[:10]]
        }
        logger.info(f"EXCLUSIVE_JOB: VALIDATION — before={len(duplicates)}, remaining={len(remaining_duplicates)}, effective={len(duplicates) - len(remaining_duplicates)}")

        _exclusivity_report["last_run"] = datetime.now().strftime("%H:%M:%S")
        logger.info(f"EXCLUSIVE_JOB: done — {removed_count} removed, {failed_count} failed, {len(duplicates)} duplicates found")
    except Exception as e:
        logger.error(f"EXCLUSIVE_JOB: error: {e}")
        _exclusivity_report["last_run"] = datetime.now().strftime("%H:%M:%S")


# --- FASTAPI APP & LIFESPAN ---
# Track which messages have already been processed by phone scan to avoid re-processing
_phone_scan_processed: set = set()
_PHONE_SCAN_MAX_CACHE = 500  # Limit cache size

async def job_phone_scan():
    """Periodic scan of phone_required_groups — ONLY react to valid ads (never delete).
    Deletion is ONLY done at webhook time when caption is guaranteed to be accurate."""
    global _phone_scan_processed
    phone_groups = cfg.get("phone_required_groups", [])
    if not phone_groups:
        return
    approved_emoji = cfg.get("phone_approved_emoji", "🔁")
    await ensure_token()
    for group_id in phone_groups:
        try:
            messages = await wpp.get_messages(group_id, count=50)
            if not messages:
                continue
            for m in messages:
                m_id_obj = m.get('id', {})
                if isinstance(m_id_obj, dict):
                    msg_id = m_id_obj.get('_serialized', '') or m_id_obj.get('id', '')
                    from_me = m_id_obj.get('fromMe', False)
                else:
                    msg_id = str(m_id_obj)
                    from_me = False
                if not msg_id or from_me or msg_id in _phone_scan_processed:
                    continue
                msg_type = m.get('type', '')
                if msg_type not in ('image', 'video'):
                    continue
                # Check caption from multiple fields
                caption = (m.get('caption') or m.get('body') or '')
                # Skip if caption is clearly base64 media data (not real text)
                if caption and len(caption) > 500 and '/' in caption:
                    caption = ''
                _phone_scan_processed.add(msg_id)
                if len(_phone_scan_processed) > _PHONE_SCAN_MAX_CACHE:
                    _phone_scan_processed = set(list(_phone_scan_processed)[-_PHONE_SCAN_MAX_CACHE:])
                # ONLY react to valid ads — NEVER delete in the scan (history API may return empty captions)
                if has_phone_number(caption):
                    await wpp.send_reaction(msg_id, group_id, approved_emoji)
                    logger.info(f"PHONE_SCAN: reacted {approved_emoji} on {msg_id[:40]}")
                    await asyncio.sleep(0.5)
        except Exception as e:
            logger.warning(f"job_phone_scan error for {group_id[:25]}: {e}")


async def job_sync_member_counts():
    """Sync real member counts from WPPConnect API for all registered groups.
    Runs every 5 minutes to keep dashboard and redirect roulette accurate."""
    await ensure_token()
    db = SessionLocal()
    try:
        groups = db.query(WhatsAppGroup).filter_by(is_active=True).all()
        all_groups = db.query(WhatsAppGroup).all()  # also sync inactive to catch re-openers
        for group in all_groups:
            try:
                url = f"{wpp.base_url}/api/{wpp.session}/group-members/{group.group_jid}"
                async with httpx.AsyncClient(timeout=15.0) as client:
                    resp = await client.get(url, headers=wpp.headers)
                if resp.status_code == 200:
                    data = resp.json()
                    # WPPConnect returns {response: [...members...]} or direct list
                    members = data.get('response', data) if isinstance(data, dict) else data
                    if isinstance(members, list):
                        real_count = len(members)
                        if group.current_members != real_count:
                            logger.info(f"SYNC_MEMBERS: {group.name} {group.current_members} -> {real_count}")
                            group.current_members = real_count
                            # Auto-manage is_active based on capacity
                            if real_count >= group.max_members and group.is_active:
                                group.is_active = False
                                logger.info(f"SYNC_MEMBERS: {group.name} FULL -> deactivated")
                            elif real_count < group.max_members and not group.is_active:
                                group.is_active = True
                                logger.info(f"SYNC_MEMBERS: {group.name} has space -> reactivated")
                await asyncio.sleep(1)  # rate-limit between requests
            except Exception as e:
                logger.warning(f"job_sync_member_counts: error for {group.name}: {e}")
        db.commit()
    except Exception as e:
        logger.error(f"job_sync_member_counts: {e}")
    finally:
        db.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Bot starting up...")
    logger.info(f"--- CONFIG ---")
    logger.info(f"WPP_SERVER_URL: {WPP_SERVER_URL}")
    logger.info(f"SESSION_NAME: {SESSION_NAME}")
    logger.info(f"--------------")
    
    init_db()
    
    # Auto-fix group display_order based on group name number
    db = SessionLocal()
    try:
        groups = db.query(WhatsAppGroup).all()
        if groups:
            def extract_group_number(name):
                """Extract number from group name like 'DEZAPEGÃO DO ZAPÃO #3' -> 3"""
                import re
                match = re.search(r'#0*(\d+)', name or '')
                return int(match.group(1)) if match else 999
            
            groups.sort(key=lambda g: extract_group_number(g.name))
            for i, g in enumerate(groups):
                if g.display_order != i:
                    logger.info(f"AUTO_ORDER_FIX: {g.name} display_order {g.display_order} -> {i}")
                    g.display_order = i
            db.commit()
    except Exception as e:
        logger.error(f"Error fixing display_order: {e}")
    finally:
        db.close()
    
    scheduler = AsyncIOScheduler()
    scheduler.add_job(job_open_groups, 'cron', hour=10, minute=0)
    scheduler.add_job(job_close_groups, 'cron', hour=20, minute=0)
    scheduler.add_job(job_watchdog, 'interval', minutes=5, id='watchdog')
    scheduler.add_job(job_sync_invite_links, 'interval', minutes=10, id='sync_invite_links')
    scheduler.add_job(job_phone_scan, 'interval', minutes=5, id='phone_scan')
    scheduler.add_job(job_sync_member_counts, 'interval', minutes=5, id='sync_member_counts')
    scheduler.start()
    logger.info("Scheduler started: watchdog(5m), invite_sync(10m), phone_scan(5m), member_sync(5m).")
    
    async def startup_sequence():
        await wpp.start_session()
        # Wait for session to connect (QR scan may take time)
        logger.info("Waiting 60s for session to connect...")
        await asyncio.sleep(60)
        # Subscribe webhook once
        logger.info("Subscribing webhook...")
        await wpp.subscribe_webhook()
        # NOTE: sync_historical_messages DISABLED - it was inflating counters
        # Users who posted before reconnect were getting immediately blocked
        logger.info("Startup sequence complete (sync disabled)")
    
    asyncio.create_task(startup_sequence())
    yield
    logger.info("Bot shutting down...")

app = FastAPI(lifespan=lifespan)
os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

# --- REVERSE PROXY FOR VERCEL ---
@app.api_route("/vendas{path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
async def proxy_vendas(request: Request, path: str):
    """Proxy requests from dezapegao.com.br/vendas to dezapegao.vercel.app/vendas."""
    target_url = f"https://dezapegao.vercel.app/vendas{path}"
    if request.query_params:
        target_url += f"?{request.query_params}"

    async with httpx.AsyncClient(follow_redirects=True) as client:
        headers = dict(request.headers)
        headers.pop("host", None)
        headers["X-Forwarded-Host"] = request.headers.get("host", "dezapegao.com.br")
        
        try:
            proxy_resp = await client.request(
                method=request.method,
                url=target_url,
                headers=headers,
                content=await request.body(),
                timeout=30.0
            )
            return Response(
                content=proxy_resp.content,
                status_code=proxy_resp.status_code,
                headers=dict(proxy_resp.headers)
            )
        except Exception as e:
            logger.error(f"Proxy error for /vendas: {e}")
            raise HTTPException(status_code=502, detail="Error proxying request to Vercel")

# --- BUSINESS LOGIC HELPER ---
LINK_REGEX = re.compile(r"(https?://|www\.|chat\.whatsapp\.com|wa\.me/)")

def check_link_whitelist(text: str, caption: str = "") -> bool:
    """Check if text/caption contains non-whitelisted links. Returns True if OK."""
    combined = f"{text or ''} {caption or ''}".strip()
    if not combined: return True
    if not cfg.get("link_filter_enabled", True): return True
    if not LINK_REGEX.search(combined): return True
    
    cleaned = combined
    for domain in cfg.get("whitelist_domains", []):
        cleaned = cleaned.replace(domain, "")
        
    if LINK_REGEX.search(cleaned):
        return False
    return True

# Track last message time per (user, group) for smart ad grouping
_last_msg_times: dict[tuple[str, str], datetime] = {}

def process_flood_control(db: Session, user_id: str, group_id: str, has_media: bool, text_len: int) -> dict:
    """Check flood limits: max N ads per calendar day + minimum interval between ads.
    Uses only EXISTING columns: window_start (= time of last approved msg), last_active_date, message_count.
    Limits reset at midnight (non-cumulative).
    """
    if not cfg.get("moderation_enabled", True):
        return {"allowed": True, "debug": "moderation_disabled"}
    try:
        now = datetime.now()
        today = now.date()
        stats = db.query(UserStats).filter_by(user_id=user_id, group_id=group_id).first()

        if not stats:
            stats = UserStats(
                user_id=user_id, group_id=group_id,
                last_active_date=today, window_start=None,
                message_count=0, photo_count=0,
            )
            db.add(stats)
            db.flush()

        # Reset daily counters if it's a new calendar day (non-cumulative)
        last_date = stats.last_active_date
        if last_date is None or (hasattr(last_date, 'date') and last_date != today) or last_date != today:
            stats.last_active_date = today
            stats.message_count = 0
            stats.photo_count = 0
            # window_start NOT reset — keeps 1h interval across midnight boundary
            logger.info(f"FLOOD_DAILY_RESET: user={user_id[:20]} new day={today}")

        daily_limit = cfg.get("flood_daily_limit", 2)
        min_interval = cfg.get("flood_min_interval_minutes", 60)
        max_text = cfg.get("flood_max_text_length", 300)

        # Check daily limit
        if stats.message_count >= daily_limit:
            db.commit()
            logger.info(f"FLOOD_DAILY_LIMIT: user={user_id[:20]} count={stats.message_count}/{daily_limit}")
            return {"allowed": False, "reason": "daily_limit",
                    "count": stats.message_count, "max": daily_limit}

        # Check minimum interval (window_start = time of last approved message)
        last_msg_time = stats.window_start
        if last_msg_time:
            elapsed_min = (now - last_msg_time).total_seconds() / 60
            if elapsed_min < min_interval:
                remaining = int(min_interval - elapsed_min)
                db.commit()
                logger.info(f"FLOOD_INTERVAL: user={user_id[:20]} elapsed={elapsed_min:.1f}m < {min_interval}m, wait={remaining}m")
                return {"allowed": False, "reason": "min_interval",
                        "elapsed_min": round(elapsed_min, 1),
                        "min_interval": min_interval, "wait_min": remaining}

        # Check text length (>300 chars = delete, user must repost shorter)
        if max_text > 0 and text_len > max_text:
            db.commit()
            return {"allowed": False, "reason": "text_length",
                    "text_len": text_len, "max_text": max_text}

        # Allowed → increment counters, record time
        stats.message_count += 1
        stats.window_start = now   # track time of last approved message
        stats.last_active_date = today
        if has_media:
            stats.photo_count += 1
        logger.info(f"FLOOD_OK: user={user_id[:20]}, ad #{stats.message_count}/{daily_limit} today")
        db.commit()
        return {"allowed": True, "count": stats.message_count, "max": daily_limit}
    except Exception as e:
        logger.error(f"Error in flood control: {e}", exc_info=True)
        db.rollback()
        return {"allowed": True, "debug": f"error: {str(e)}"}


def _save_moderation_log(group_id: str, sender_id: str, msg_type: str,
                         msg_id: str, caption: str, action: str, reason: str = ""):
    """Persist a moderation decision to the ModerationLog table."""
    try:
        db = SessionLocal()
        entry = ModerationLog(
            group_id=group_id, sender_id=sender_id,
            msg_type=msg_type, msg_id=str(msg_id)[:100],
            caption=str(caption or "")[:300],
            action=action, reason=str(reason)[:200]
        )
        db.add(entry)
        db.commit()
        db.close()
    except Exception as e:
        logger.warning(f"_save_moderation_log error: {e}")


# --- DASHBOARD ROUTES ---
@app.get("/adm", response_class=HTMLResponse)
async def dashboard(request: Request, username: str = Depends(get_current_username)):
    return templates.TemplateResponse("dashboard.html", {"request": request, "session_name": SESSION_NAME})

async def ensure_token():
    """Make sure we have a valid token, regenerate if needed."""
    if not wpp.token:
        await wpp.generate_token()

@app.get("/api/status")
async def api_status(username: str = Depends(get_current_username)):
    await ensure_token()
    url = f"{wpp.base_url}/api/{wpp.session}/status-session"
    session_status = "UNKNOWN"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=wpp.headers)
            if resp.status_code == 401:
                await wpp.generate_token()
                resp = await client.get(url, headers=wpp.headers)
            if resp.status_code == 200:
                data = resp.json()
                session_status = data.get("status", "UNKNOWN")
    except:
        pass
    connected = session_status in ("CONNECTED", "isLogged", "inChat")
    return {
        "wpp_connected": connected, 
        "session_status": session_status, 
        "bot_active": True,
        "session_id": wpp.session
    }

@app.get("/api/debug/wpp-apis")
async def debug_wpp_apis(username: str = Depends(get_current_username)):
    """Test which WPP API endpoints exist."""
    await ensure_token()
    results = {}
    test_endpoints = [
        ("GET", "api-docs"),
        ("POST", f"api/{wpp.session}/forward-messages"),
        ("POST", f"api/{wpp.session}/forward-message"),
        ("POST", f"api/{wpp.session}/download-media"),
        ("GET", f"api/{wpp.session}/get-media-by-message/test"),
        ("POST", f"api/{wpp.session}/send-file"),
        ("POST", f"api/{wpp.session}/send-image"),
        ("POST", f"api/{wpp.session}/send-file-from-url"),
        ("POST", f"api/{wpp.session}/send-file-base64"),
    ]
    async with httpx.AsyncClient(timeout=10.0) as client:
        for method, ep in test_endpoints:
            try:
                url = f"{wpp.base_url}/{ep}"
                if method == "GET":
                    resp = await client.get(url, headers=wpp.headers)
                else:
                    resp = await client.post(url, json={}, headers=wpp.headers)
                results[ep] = {"status": resp.status_code, "body": resp.text[:200]}
            except Exception as e:
                results[ep] = {"error": str(e)}
    return results

@app.get("/api/debug/test-forward")
async def debug_test_forward(messageId: str, phone: str, username: str = Depends(get_current_username)):
    """Test forward-messages with a specific messageId."""
    await ensure_token()
    fwd_url = f"{wpp.base_url}/api/{wpp.session}/forward-messages"
    async with httpx.AsyncClient(timeout=30.0) as client:
        payload = {"phone": phone, "messageId": messageId, "isGroup": True}
        resp = await client.post(fwd_url, json=payload, headers=wpp.headers)
        return {"status": resp.status_code, "body": resp.json() if resp.text else {}}

@app.get("/api/debug/last-msgs")
async def debug_last_msgs(chat: str, username: str = Depends(get_current_username)):
    """List last 10 messages from a chat with their IDs."""
    await ensure_token()
    msgs = await wpp.get_messages(chat, count=20)
    result = []
    for m in msgs[-10:]:
        mid = m.get('id', {})
        result.append({
            "type": m.get('type'),
            "body_len": len(str(m.get('body',''))),
            "id_serialized": mid.get('_serialized','') if isinstance(mid,dict) else str(mid),
            "fromMe": mid.get('fromMe') if isinstance(mid,dict) else None,
            "timestamp": m.get('timestamp'),
        })
    return result

@app.get("/api/debug/set-config")
async def debug_set_config(key: str, value: str, username: str = Depends(get_current_username)):
    """Set a single config key. Example: /api/debug/set-config?key=broadcast_test_group_id&value=120...@g.us"""
    from settings import save_settings
    save_settings({key: value})
    return {"ok": True, "key": key, "value": value}

@app.get("/api/qr")
async def api_qr(username: str = Depends(get_current_username)):
    """Proxy the QR code from wppconnect-server."""
    await ensure_token()
    url = f"{wpp.base_url}/api/{wpp.session}/qrcode-session"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=wpp.headers)
            if resp.status_code == 401:
                await wpp.generate_token()
                resp = await client.get(url, headers=wpp.headers)
            logger.info(f"QR Code fetch: {resp.status_code} content-type: {resp.headers.get('content-type')}")
            
            content_type = resp.headers.get("content-type", "")
            
            if resp.status_code == 200:
                # If response is a PNG image (binary)
                if "image" in content_type:
                    import base64
                    b64 = base64.b64encode(resp.content).decode("utf-8")
                    data_uri = f"data:{content_type.split(';')[0]};base64,{b64}"
                    return {"qr": data_uri, "status": "waiting_scan"}
                
                # If response is JSON
                try:
                    data = resp.json()
                    qr_data = data.get("qrcode") or data.get("base64Qr") or data.get("urlCode")
                    if qr_data:
                        return {"qr": qr_data, "status": "waiting_scan"}
                    return {"qr": None, "status": data.get("status", "no_qr")}
                except:
                    return {"qr": None, "status": "unknown_response"}
            else:
                return {"qr": None, "status": f"error_{resp.status_code}"}
    except Exception as e:
        logger.error(f"Error fetching QR: {e}")
        return {"qr": None, "status": "error", "detail": str(e)}


@app.post("/api/session/start")
async def api_session_start(username: str = Depends(get_current_username)):
    """Start or restart the WPP session to generate a new QR code."""
    await wpp.start_session()
    return {"status": "success", "message": "Sessão iniciada! Aguarde o QR Code aparecer."}

@app.post("/api/session/close")
async def api_session_close(username: str = Depends(get_current_username)):
    """Close the current WPP session."""
    await ensure_token()
    url = f"{wpp.base_url}/api/{wpp.session}/close-session"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, headers=wpp.headers)
            logger.info(f"Close session: {resp.status_code} - {resp.text}")
            return {"status": "success", "message": "Sessão encerrada."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/session/logout")
async def api_session_logout(username: str = Depends(get_current_username)):
    """Logout + Session Pivot: Clears old session and switches to a NEW session ID.
    This is the most reliable way to force a fresh QR scan when the server is stuck.
    """
    await ensure_token()
    global SESSION_NAME
    old_session = wpp.session
    try:
        # 1. Perform deep deletion of OLD session files
        logger.info(f"PIVOT: Cleaning old session {old_session}...")
        await wpp.delete_session()
        
        # 2. Increment the session ID (idsr_bot2 -> idsr_bot3)
        match = re.search(r'(\d+)$', old_session)
        if match:
            num = int(match.group(1))
            base = old_session[:match.start()]
            new_session = f"{base}{num + 1}"
        else:
            new_session = f"{old_session}_1"
        
        # 3. Save new ID to settings and update memory
        logger.info(f"PIVOT: Switching session ID from {old_session} to {new_session}")
        cfg.save_settings({"session_name": new_session})
        SESSION_NAME = new_session
        wpp.session = new_session
        
        # Important: generate a fresh token for the new session ID
        await wpp.generate_token()
        
        return {
            "status": "success", 
            "message": f"Sessão {old_session} removida. Novo ID: {new_session}. Inicie para ver o QR.",
            "new_session": new_session
        }
    except Exception as e:
        logger.error(f"Logout session pivot error: {e}")
        return {"status": "error", "message": f"Erro no pivot de sessão: {e}"}


# --- GROUPS & SCAN ENDPOINTS ---


def parse_group(g):
    """Extract group info from WPPConnect nested structure."""
    gid = g.get('id', {}).get('_serialized') if isinstance(g.get('id'), dict) else g.get('id', '')
    contact = g.get('contact', {}) or {}
    meta = g.get('groupMetadata', {}) or {}
    
    name = contact.get('name') or contact.get('formattedName') or meta.get('subject') or g.get('name') or 'Sem nome'
    size = meta.get('size', 0) or 0
    
    # Check if bot is admin by looking at groupMetadata.participants
    participants = meta.get('participants', []) or []
    is_admin = False
    if isinstance(participants, list):
        if size == 0:
            size = len(participants)
        for p in participants:
            pid = p.get('id', {}).get('_serialized') if isinstance(p.get('id'), dict) else p.get('id', '')
            if p.get('isMe') or (pid and 'lid' in pid):
                # Check if any participant that is the bot has admin
                if p.get('isSuperAdmin') or p.get('isAdmin'):
                    is_admin = True
    
    return {"id": gid, "name": name, "size": size, "is_admin": is_admin}

@app.get("/api/groups/list")
async def api_groups_list(username: str = Depends(get_current_username)):
    """List all groups the bot is part of with basic info."""
    await ensure_token()
    groups = await wpp.get_all_groups()
    result = [parse_group(g) for g in groups]
    return {"groups": result, "total": len(result)}

@app.get("/api/analytics/advertisers")
async def api_analytics_advertisers(username: str = Depends(get_current_username)):
    """Get unique advertiser stats across all groups for capacity planning."""
    from sqlalchemy import func, text
    db = SessionLocal()
    try:
        # 1. Total unique advertisers (all time)
        total_unique = db.query(func.count(func.distinct(UserStats.user_id))).scalar() or 0
        
        # 2. Unique advertisers per group
        per_group = db.query(
            UserStats.group_id,
            func.count(func.distinct(UserStats.user_id)).label("unique_users"),
            func.sum(UserStats.message_count).label("total_msgs")
        ).group_by(UserStats.group_id).all()
        
        # 3. Active advertisers (posted in last 7 days)
        seven_days_ago = date.today() - timedelta(days=7)
        active_7d = db.query(func.count(func.distinct(UserStats.user_id))).filter(
            UserStats.last_active_date >= seven_days_ago
        ).scalar() or 0
        
        # 4. Active advertisers (posted today)
        active_today = db.query(func.count(func.distinct(UserStats.user_id))).filter(
            UserStats.last_active_date == date.today()
        ).scalar() or 0
        
        # 5. Daily breakdown (last 7 days)
        daily = db.query(
            UserStats.last_active_date,
            func.count(func.distinct(UserStats.user_id)).label("unique_users"),
            func.sum(UserStats.message_count).label("total_msgs")
        ).filter(
            UserStats.last_active_date >= seven_days_ago
        ).group_by(UserStats.last_active_date).order_by(UserStats.last_active_date.desc()).all()
        
        # 6. Top 20 advertisers by message count
        top_advertisers = db.query(
            UserStats.user_id,
            func.sum(UserStats.message_count).label("total_msgs"),
            func.count(func.distinct(UserStats.group_id)).label("groups_active_in")
        ).group_by(UserStats.user_id).order_by(func.sum(UserStats.message_count).desc()).limit(20).all()
        
        # Build group name mapping
        group_names = {}
        managed = db.query(WhatsAppGroup).all()
        for g in managed:
            group_names[g.group_jid] = g.name
        
        return {
            "total_unique_advertisers": total_unique,
            "active_last_7_days": active_7d,
            "active_today": active_today,
            "per_group": [{
                "group_id": row.group_id,
                "group_name": group_names.get(row.group_id, row.group_id[:25]),
                "unique_users": row.unique_users,
                "total_messages": row.total_msgs or 0
            } for row in per_group],
            "daily_breakdown": [{
                "date": str(row.last_active_date),
                "unique_users": row.unique_users,
                "total_messages": row.total_msgs or 0
            } for row in daily],
            "top_advertisers": [{
                "user_id": row.user_id[:25],
                "total_messages": row.total_msgs or 0,
                "groups_active_in": row.groups_active_in
            } for row in top_advertisers]
        }
    finally:
        db.close()

@app.get("/api/analytics/metrics")
async def api_analytics_metrics(username: str = Depends(get_current_username)):
    """Comprehensive metrics: peak hours, churn rate, retention, member flow."""
    from sqlalchemy import func, case
    db = SessionLocal()
    try:
        # --- 1. PEAK HOURS: average msgs per hour across all groups ---
        peak_hours = db.query(
            HourlyActivity.hour,
            func.sum(HourlyActivity.message_count).label("total_msgs"),
            func.sum(HourlyActivity.media_count).label("total_media"),
            func.count(func.distinct(HourlyActivity.date)).label("days_counted")
        ).group_by(HourlyActivity.hour).order_by(HourlyActivity.hour).all()
        
        peak_data = []
        for row in peak_hours:
            avg_msgs = round(row.total_msgs / max(row.days_counted, 1), 1)
            peak_data.append({
                "hour": row.hour,
                "label": f"{row.hour:02d}:00",
                "total_msgs": row.total_msgs,
                "total_media": row.total_media,
                "avg_msgs_per_day": avg_msgs,
                "days_counted": row.days_counted
            })
        
        # Find the top 3 golden hours
        golden_hours = sorted(peak_data, key=lambda x: x["avg_msgs_per_day"], reverse=True)[:3]
        
        # --- 2. MEMBER FLOW: joins vs leaves per day ---
        thirty_days_ago = date.today() - timedelta(days=30)
        
        daily_flow = db.query(
            MemberEvent.event_date,
            func.sum(case((MemberEvent.event_type.in_(['add', 'join']), 1), else_=0)).label("joins"),
            func.sum(case((MemberEvent.event_type.in_(['leave', 'remove']), 1), else_=0)).label("leaves")
        ).filter(
            MemberEvent.event_date >= thirty_days_ago
        ).group_by(MemberEvent.event_date).order_by(MemberEvent.event_date.desc()).all()
        
        flow_data = [{
            "date": str(row.event_date),
            "joins": row.joins or 0,
            "leaves": row.leaves or 0,
            "net": (row.joins or 0) - (row.leaves or 0)
        } for row in daily_flow]
        
        # --- 3. CHURN RATE ---
        total_joins = db.query(func.count()).filter(
            MemberEvent.event_type.in_(['add', 'join']),
            MemberEvent.event_date >= thirty_days_ago
        ).scalar() or 0
        
        total_leaves = db.query(func.count()).filter(
            MemberEvent.event_type.in_(['leave', 'remove']),
            MemberEvent.event_date >= thirty_days_ago
        ).scalar() or 0
        
        churn_rate = round((total_leaves / max(total_joins, 1)) * 100, 1)
        
        # --- 4. RETENTION: users who joined and are still present ---
        # Check which users joined but never left (within tracking period)
        joined_users = db.query(
            MemberEvent.user_id,
            func.min(MemberEvent.event_date).label("first_join")
        ).filter(
            MemberEvent.event_type.in_(['add', 'join'])
        ).group_by(MemberEvent.user_id).subquery()
        
        left_users = db.query(
            MemberEvent.user_id
        ).filter(
            MemberEvent.event_type.in_(['leave', 'remove'])
        ).distinct().subquery()
        
        # Users who joined but never left
        retained = db.query(func.count()).select_from(joined_users).filter(
            ~joined_users.c.user_id.in_(db.query(left_users.c.user_id))
        ).scalar() or 0
        
        total_ever_joined = db.query(func.count()).select_from(joined_users).scalar() or 0
        retention_rate = round((retained / max(total_ever_joined, 1)) * 100, 1)
        
        # --- 5. CHURN BY GROUP ---
        churn_by_group = db.query(
            MemberEvent.group_id,
            func.sum(case((MemberEvent.event_type.in_(['add', 'join']), 1), else_=0)).label("joins"),
            func.sum(case((MemberEvent.event_type.in_(['leave', 'remove']), 1), else_=0)).label("leaves")
        ).filter(
            MemberEvent.event_date >= thirty_days_ago
        ).group_by(MemberEvent.group_id).all()
        
        # Build group name mapping
        group_names = {}
        managed = db.query(WhatsAppGroup).all()
        for g in managed:
            group_names[g.group_jid] = g.name
        
        group_churn = [{
            "group_id": row.group_id,
            "group_name": group_names.get(row.group_id, row.group_id[:25]),
            "joins": row.joins or 0,
            "leaves": row.leaves or 0,
            "churn_pct": round(((row.leaves or 0) / max(row.joins or 0, 1)) * 100, 1)
        } for row in churn_by_group]
        
        return {
            "peak_hours": peak_data,
            "golden_hours": [{"hour": h["label"], "avg_msgs": h["avg_msgs_per_day"]} for h in golden_hours],
            "member_flow": {
                "daily": flow_data,
                "totals_30d": {
                    "joins": total_joins,
                    "leaves": total_leaves,
                    "net_growth": total_joins - total_leaves,
                    "churn_rate_pct": churn_rate,
                    "retention_rate_pct": retention_rate
                }
            },
            "churn_by_group": group_churn,
            "data_note": "Tracking started on deploy. More data = more accuracy."
        }
    finally:
        db.close()

@app.get("/api/groups/{group_id}/participants")
async def api_group_participants(group_id: str, username: str = Depends(get_current_username)):
    """Get all participants of a specific group using group-members endpoint."""
    await ensure_token()
    # Use group-members endpoint (group-participants returns 404)
    url = f"{wpp.base_url}/api/{wpp.session}/group-members/{group_id}"
    result = []
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url, headers=wpp.headers)
            if resp.status_code == 200:
                data = resp.json()
                members = data.get('response', data) if isinstance(data, dict) else data
                if isinstance(members, list):
                    for m in members:
                        mid = m.get('id', {}).get('_serialized') if isinstance(m.get('id'), dict) else m.get('id', '')
                        result.append({
                            "id": mid,
                            "name": m.get('name') or m.get('pushname') or m.get('shortName') or m.get('formattedName') or mid,
                            "is_admin": m.get('isAdmin', False) or m.get('isSuperAdmin', False),
                        })
    except Exception as e:
        logger.error(f"Error getting members: {e}")
    return {"participants": result, "total": len(result)}

@app.post("/api/scan")
async def api_scan(username: str = Depends(get_current_username)):
    """Run a full scan of all groups - returns checklist items."""
    await ensure_token()
    groups = await wpp.get_all_groups()
    
    checklist = []
    total_groups = len(groups)
    groups_as_admin = 0
    total_members = 0
    
    for g in groups:
        info = parse_group(g)
        if info["is_admin"]:
            groups_as_admin += 1
        total_members += info["size"]
        
        checklist.append({
            "item": f"Grupo: {info['name']}",
            "id": info["id"],
            "status": "ok" if info["id"] else "error",
            "detail": f"{info['size']} membros | {'✅ Admin' if info['is_admin'] else '⚠️ Não é admin'}",
            "is_admin": info["is_admin"],
            "size": info["size"],
        })
    
    summary = {
        "total_groups": total_groups,
        "groups_as_admin": groups_as_admin,
        "total_members": total_members,
        "connection": "ok",
        "webhook": "ok",
        "scheduler": "ok",
    }
    
    s = cfg.get_settings()
    system_checks = [
        {"item": "Conexão com WhatsApp", "status": "ok" if total_groups > 0 else "warning", "detail": "Conectado" if total_groups > 0 else "Sem grupos encontrados"},
        {"item": "Webhook ativo", "status": "ok", "detail": "Recebendo mensagens"},
        {"item": "Agendador (Scheduler)", "status": "ok" if s.get("schedule_enabled") else "warning", "detail": f"Abrir {s.get('schedule_open_hour', 10)}h / Fechar {s.get('schedule_close_hour', 20)}h" if s.get("schedule_enabled") else "Desativado"},
        {"item": "Anti-flood", "status": "ok" if s.get("moderation_enabled") else "warning", "detail": f"{s.get('flood_max_messages', 2)} msgs + {s.get('flood_max_photos', 3)} fotos/dia" if s.get("moderation_enabled") else "Desativado"},
        {"item": "Filtro de links", "status": "ok" if s.get("link_filter_enabled") else "warning", "detail": f"Whitelist: {', '.join(s.get('whitelist_domains', []))}" if s.get("link_filter_enabled") else "Desativado"},
        {"item": "Filtro de Figurinhas", "status": "ok" if s.get("sticker_filter_enabled", True) else "warning", "detail": "Ativo" if s.get("sticker_filter_enabled", True) else "Desativado"},
        {"item": "Super Admins", "status": "ok", "detail": f"{len(s.get('super_admins', []))} configurados"},
        {"item": f"Total de grupos", "status": "ok" if total_groups > 0 else "error", "detail": f"{total_groups} grupos encontrados"},
        {"item": f"Bot é admin em", "status": "ok" if groups_as_admin > 0 else "warning", "detail": f"{groups_as_admin} de {total_groups} grupos"},
        {"item": f"Total de membros", "status": "ok", "detail": f"{total_members} membros no total"},
    ]
    
    return {"checklist": checklist, "system_checks": system_checks, "summary": summary}

# --- SESSION MANAGEMENT ENDPOINTS ---


# --- SETTINGS ENDPOINTS ---
@app.get("/api/settings")
async def api_settings_get(username: str = Depends(get_current_username)):
    """Get current settings."""
    return cfg.get_settings()

@app.post("/api/settings")
async def api_settings_update(request: Request, username: str = Depends(get_current_username)):
    """Update settings."""
    try:
        body = await request.json()
        updated = cfg.save_settings(body)
        return {"status": "success", "settings": updated}
    except Exception as e:
        logger.error(f"Error updating settings: {e}")
        return {"status": "error", "message": str(e)}



@app.post("/api/groups/{action}")
async def api_groups_control(action: str, username: str = Depends(get_current_username)):
    if action == "open":
        await job_open_groups()
        return {"status": "success", "message": "Comando de ABRIR grupos enviado."}
    elif action == "close":
        await job_close_groups()
        return {"status": "success", "message": "Comando de FECHAR grupos enviado."}
    return {"status": "error", "message": "Ação inválida"}

class AnnouncementModel(BaseModel):
    message: str

@app.post("/api/announce")
async def api_announce(data: AnnouncementModel, background_tasks: BackgroundTasks, username: str = Depends(get_current_username)):
    text = data.message
    groups = await wpp.get_all_groups()
    count = 0
    for g in groups:
        gid = g.get('id', {}).get('_serialized') if isinstance(g.get('id'), dict) else g.get('id')
        if gid:
            background_tasks.add_task(wpp.send_message, gid, text)
            count += 1
    return {"status": "success", "message": f"Anunciando para {count} grupos em background."}

# --- HOUSE DISTRIBUTOR (root link) ---
@app.get("/", response_class=HTMLResponse)
def page_home(request: Request):
    """Main house invite link — distributor without affiliate."""
    db = SessionLocal()
    try:
        # Check if admin has set a specific redirect target
        target_jid = cfg.get("redirect_target_jid", "")
        group = None
        if target_jid:
            group = db.query(WhatsAppGroup).filter(
                WhatsAppGroup.group_jid == target_jid,
                WhatsAppGroup.is_active == True
            ).first()
        if not group:
            # Fallback: pick group with fewest members
            group = db.query(WhatsAppGroup).filter(
                WhatsAppGroup.is_active == True
            ).order_by(WhatsAppGroup.current_members.asc()).first()

        if not group:
            return templates.TemplateResponse("entrar.html", {
                "request": request,
                "error": "Todos os grupos estão lotados no momento. Tente novamente mais tarde.",
                "group": None, "affiliate": None, "invite_link": "#"
            })

        # Dedup by IP (same logic as page_entrar)
        visitor_ip = request.client.host if request.client else ""
        recent_cutoff = datetime.utcnow() - timedelta(hours=1)
        existing_click = db.query(JoinTracking).filter(
            JoinTracking.affiliate_id == None,
            JoinTracking.group_id == group.id,
            JoinTracking.visitor_ip == visitor_ip,
            JoinTracking.confirmed == False,
            JoinTracking.clicked_at >= recent_cutoff
        ).first() if visitor_ip else None

        if not existing_click:
            tracking = JoinTracking(
                affiliate_id=None,
                group_id=group.id,
                visitor_ip=visitor_ip
            )
            db.add(tracking)
            db.commit()
            logger.info(f"HOUSE_CLICK: group={group.name} ({group.current_members}/{group.max_members}), ip={visitor_ip[:15]}")

        return templates.TemplateResponse("entrar.html", {
            "request": request,
            "error": None,
            "group": group,
            "affiliate": None,
            "invite_link": group.invite_link
        })
    except Exception as e:
        logger.error(f"HOME_ERROR: {e}")
        return templates.TemplateResponse("entrar.html", {
            "request": request, "error": "Erro interno. Tente novamente.",
            "group": None, "affiliate": None, "invite_link": "#"
        })
    finally:
        db.close()

# --- AUTO-REDIRECTOR (no-click invite) ---
@app.get("/r", response_class=HTMLResponse)
def page_redirect(request: Request):
    """Auto-redirect to group — no click needed, no affiliate."""
    db = SessionLocal()
    try:
        # Check if admin has set a specific redirect target
        target_jid = cfg.get("redirect_target_jid", "")
        group = None
        if target_jid:
            group = db.query(WhatsAppGroup).filter(
                WhatsAppGroup.group_jid == target_jid,
                WhatsAppGroup.is_active == True
            ).first()
        if not group:
            # Fallback: pick group with fewest members
            group = db.query(WhatsAppGroup).filter(
                WhatsAppGroup.is_active == True
            ).order_by(WhatsAppGroup.current_members.asc()).first()

        if not group:
            return templates.TemplateResponse("redirecionar.html", {
                "request": request,
                "error": "Todos os grupos estão lotados no momento. Tente novamente mais tarde.",
                "invite_link": None
            })

        # Dedup by IP
        visitor_ip = request.client.host if request.client else ""
        recent_cutoff = datetime.utcnow() - timedelta(hours=1)
        existing_click = db.query(JoinTracking).filter(
            JoinTracking.affiliate_id == None,
            JoinTracking.group_id == group.id,
            JoinTracking.visitor_ip == visitor_ip,
            JoinTracking.confirmed == False,
            JoinTracking.clicked_at >= recent_cutoff
        ).first() if visitor_ip else None

        if not existing_click:
            tracking = JoinTracking(
                affiliate_id=None,
                group_id=group.id,
                visitor_ip=visitor_ip
            )
            db.add(tracking)
            db.commit()
            logger.info(f"REDIRECT_CLICK: group={group.name} ({group.current_members}/{group.max_members}), ip={visitor_ip[:15]}")

        return templates.TemplateResponse("redirecionar.html", {
            "request": request,
            "error": None,
            "invite_link": group.invite_link
        })
    except Exception as e:
        logger.error(f"REDIRECT_ERROR: {e}")
        return templates.TemplateResponse("redirecionar.html", {
            "request": request, "error": "Erro interno. Tente novamente.",
            "invite_link": None
        })
    finally:
        db.close()

# --- AFFILIATE SYSTEM ---
BASE_URL = "https://www.dezapegao.com.br"

@app.get("/criar-link", response_class=HTMLResponse)
async def page_criar_link(request: Request):
    """Public page for affiliates to create their referral link."""
    return templates.TemplateResponse("criar_link.html", {"request": request, "base_url": BASE_URL})

@app.post("/api/affiliate/create")
async def api_affiliate_create(request: Request):
    """Create a new affiliate link with auto-generated slug."""
    try:
        body = await request.json()
        name = body.get("name", "").strip()
        phone = body.get("phone", "").strip()
        
        if not name:
            return JSONResponse({"status": "error", "message": "Nome é obrigatório."}, status_code=400)
        
        db = SessionLocal()
        try:
            # Check if phone already has an affiliate link
            existing_affiliate = db.query(Affiliate).filter_by(phone=phone).first()
            if existing_affiliate:
                logger.info(f"AFFILIATE_FOUND: phone={phone}, slug={existing_affiliate.slug}")
                return {"status": "success", "slug": existing_affiliate.slug, "link": f"{BASE_URL}/entrar/{existing_affiliate.slug}"}

            import random, string
            # Generate unique random slug (6 chars, alphanumeric)
            for _ in range(10):  # max 10 attempts
                slug = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
                existing = db.query(Affiliate).filter_by(slug=slug).first()
                if not existing:
                    break
            else:
                return JSONResponse({"status": "error", "message": "Erro ao gerar código. Tente novamente."}, status_code=500)
            
            affiliate = Affiliate(name=name, phone=phone, slug=slug)
            db.add(affiliate)
            db.commit()
            logger.info(f"AFFILIATE_CREATED: name={name}, slug={slug}")
            return {"status": "success", "slug": slug, "link": f"{BASE_URL}/entrar/{slug}"}
        finally:
            db.close()
    except Exception as e:
        logger.error(f"Error creating affiliate: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

@app.get("/api/admin/reset-affiliates-now")
async def api_admin_reset_affiliates(username: str = Depends(get_current_username)):
    """Reset all affiliate links and joins (DANGER)."""
    db = SessionLocal()
    try:
        count_joins = db.query(JoinTracking).delete()
        count_affiliates = db.query(Affiliate).delete()
        db.commit()
        return {"status": "success", "message": f"Resetado: {count_affiliates} afiliados e {count_joins} joins removidos."}
    except Exception as e:
        db.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        db.close()

@app.delete("/api/admin/affiliate/{slug}")
async def api_admin_delete_affiliate(slug: str, username: str = Depends(get_current_username)):
    """Delete a specific affiliate by slug."""
    db = SessionLocal()
    try:
        aff = db.query(Affiliate).filter_by(slug=slug).first()
        if not aff:
            return JSONResponse({"status": "error", "message": f"Slug '{slug}' não encontrado."}, status_code=404)
        db.query(JoinTracking).filter_by(affiliate_id=aff.id).delete()
        db.delete(aff)
        db.commit()
        return {"status": "success", "message": f"Afiliado '{aff.name}' (slug={slug}) removido."}
    except Exception as e:
        db.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        db.close()

@app.get("/entrar/{slug}", response_class=HTMLResponse)
def page_entrar(request: Request, slug: str):
    """Public landing page — tracks click and shows join button."""
    logger.info(f"ENTRAR_REQUEST: slug={slug}")
    db = SessionLocal()
    try:
        affiliate = db.query(Affiliate).filter_by(slug=slug, is_active=True).first()
        if not affiliate:
            logger.info(f"ENTRAR: slug={slug} not found or inactive")
            return templates.TemplateResponse("entrar.html", {
                "request": request, "error": "Link inválido ou desativado.",
                "group": None, "affiliate": None, "invite_link": "#"
            })
        
        # Pick the active group with the most available spots (simple query)
        group = db.query(WhatsAppGroup).filter(
            WhatsAppGroup.is_active == True
        ).order_by(WhatsAppGroup.current_members.asc()).first()
        
        if not group:
            logger.info(f"ENTRAR: No active group available for slug={slug}")
            return templates.TemplateResponse("entrar.html", {
                "request": request, "error": "Todos os grupos estão lotados no momento. Tente novamente mais tarde.",
                "group": None, "affiliate": None, "invite_link": "#"
            })
        
        # Record click (deduplicate: skip if same IP already has a pending click for this affiliate recently)
        visitor_ip = request.client.host if request.client else ""
        recent_cutoff = datetime.utcnow() - timedelta(hours=1)
        existing_click = db.query(JoinTracking).filter(
            JoinTracking.affiliate_id == affiliate.id,
            JoinTracking.group_id == group.id,
            JoinTracking.visitor_ip == visitor_ip,
            JoinTracking.confirmed == False,
            JoinTracking.clicked_at >= recent_cutoff
        ).first() if visitor_ip else None
        
        if not existing_click:
            tracking = JoinTracking(
                affiliate_id=affiliate.id,
                group_id=group.id,
                visitor_ip=visitor_ip
            )
            db.add(tracking)
            db.commit()
            logger.info(f"AFFILIATE_CLICK: slug={slug}, group={group.name} ({group.current_members}/{group.max_members}), ip={visitor_ip[:15]}")
        else:
            logger.info(f"AFFILIATE_CLICK_DEDUP: slug={slug}, ip={visitor_ip[:15]} already has pending click")
        
        return templates.TemplateResponse("entrar.html", {
            "request": request,
            "error": None,
            "group": group,
            "affiliate": affiliate,
            "invite_link": group.invite_link
        })
    except Exception as e:
        logger.error(f"ENTRAR_ERROR: slug={slug}, error={e}")
        return templates.TemplateResponse("entrar.html", {
            "request": request, "error": "Erro interno. Tente novamente.",
            "group": None, "affiliate": None, "invite_link": "#"
        })
    finally:
        db.close()

@app.get("/ranking", response_class=HTMLResponse)
async def page_ranking(request: Request):
    """Public ranking page showing top affiliates."""
    db = SessionLocal()
    try:
        from sqlalchemy import func
        
        # Query confirmed joins per affiliate — aggregated by NAME
        from sqlalchemy import case
        confirmed_expr = func.sum(case((JoinTracking.confirmed == True, 1), else_=0))
        
        results = db.query(
            Affiliate.name,
            func.min(Affiliate.slug).label("slug"),
            confirmed_expr.label("confirmed_count")
        ).outerjoin(
            JoinTracking, JoinTracking.affiliate_id == Affiliate.id
        ).filter(
            Affiliate.is_active == True
        ).group_by(func.lower(Affiliate.name)).order_by(confirmed_expr.desc()).all()
        
        all_affiliates = []
        for r in results:
            # Skip any existing fake entries from DB
            if FAKE_AFFILIATE and r.name.lower() == FAKE_AFFILIATE["name"].lower():
                continue
            all_affiliates.append({
                "name": r.name,
                "slug": r.slug,
                "confirmed_count": int(r.confirmed_count or 0)
            })
        
        # Inject fake affiliate at configured position
        if FAKE_AFFILIATE:
            pos = FAKE_AFFILIATE["position"] - 1  # 0-indexed
            # Calculate count: 1 less than person above, or same as person at that position
            if pos > 0 and len(all_affiliates) >= pos:
                fake_count = all_affiliates[pos - 1]["confirmed_count"] - 1
            elif len(all_affiliates) > pos:
                fake_count = all_affiliates[pos]["confirmed_count"]
            else:
                fake_count = 0
            fake_entry = {"name": FAKE_AFFILIATE["name"], "slug": "fake", "confirmed_count": max(fake_count, 0)}
            all_affiliates.insert(pos, fake_entry)
        
        top3 = all_affiliates[:3]
        rest = []
        for i, a in enumerate(all_affiliates[3:10], start=4):
            rest.append({**a, "rank": i})
        full_rest = []
        for i, a in enumerate(all_affiliates[10:], start=11):
            full_rest.append({**a, "rank": i})
        
        prizes = ["R$ 200", "R$ 100", "R$ 50"]
        
        return templates.TemplateResponse("ranking.html", {
            "request": request, "top3": top3, "rest": rest, "full_rest": full_rest, "prizes": prizes
        })
    finally:
        db.close()

# --- Managed Groups CRUD (Admin) ---

# Temporary endpoint to check duplicates between groups
@app.get("/api/admin/group-duplicates")
async def api_group_duplicates(username: str = Depends(get_current_username)):
    """Check duplicate members across ALL bot groups dynamically."""
    await ensure_token()
    try:
        all_groups_raw = await wpp.get_all_groups()
        if not all_groups_raw:
            return {"error": "Nenhum grupo encontrado", "unique_members": 0, "duplicates_count": 0}

        # Parse group list into (jid, name) pairs
        groups_list = []
        for g in all_groups_raw:
            if isinstance(g, dict):
                gid = g.get("id", g.get("_serialized", ""))
                if isinstance(gid, dict):
                    gid = gid.get("_serialized", "")
                gname = g.get("name", g.get("subject", str(gid)[:20]))
            elif isinstance(g, str):
                gid = g
                gname = g[:20]
            else:
                continue
            if gid:
                groups_list.append((str(gid), str(gname)))

        def extract_phone(member):
            """Extract phone number from any WPPConnect participant format."""
            if isinstance(member, str):
                return member.split('@')[0]
            if isinstance(member, dict):
                for field in ['id', '_serialized', 'user']:
                    val = member.get(field, '')
                    if val:
                        if isinstance(val, dict):
                            inner = val.get('_serialized', val.get('user', ''))
                            if inner:
                                return str(inner).split('@')[0]
                        else:
                            return str(val).split('@')[0]
            return None

        # Collect admin phones to exclude
        admin_phones = set(get_super_admins())
        for gid, _ in groups_list:
            try:
                admins_raw = await wpp.get_group_admins(gid)
                if isinstance(admins_raw, list):
                    for a in admins_raw:
                        admin_phone = extract_phone(a)
                        if admin_phone and len(admin_phone) > 3:
                            admin_phones.add(admin_phone)
            except Exception:
                pass

        # Fetch members for each group
        group_members = {}
        debug_info = []

        for gid, gname in groups_list:
            try:
                members_raw = await wpp.get_group_participants(gid)
                if isinstance(members_raw, list) and len(members_raw) > 0:
                    ids = set()
                    for m in members_raw:
                        phone = extract_phone(m)
                        if phone and len(phone) > 3:
                            ids.add(phone)
                    group_members[gid] = {
                        "name": gname,
                        "ids": ids,
                        "total": len(ids),
                        "raw_count": len(members_raw)
                    }
                    debug_info.append({"group": gname, "raw_count": len(members_raw), "extracted": len(ids)})
                else:
                    debug_info.append({"group": gname, "raw_count": 0, "extracted": 0})
            except Exception as e:
                logger.error(f"Error fetching members for {gname}: {e}")
                debug_info.append({"group": gname, "error": str(e)})

        if not group_members:
            return {"error": "Nenhum dado de membros obtido", "unique_members": 0, "duplicates_count": 0, "debug": debug_info}

        # Find duplicates across groups
        member_groups = {}
        for gid, info in group_members.items():
            for mid in info["ids"]:
                if mid not in member_groups:
                    member_groups[mid] = []
                member_groups[mid].append(info["name"])

        # Exclude admins
        non_admin_members = {mid: groups for mid, groups in member_groups.items() if mid not in admin_phones}
        duplicates = {mid: groups for mid, groups in non_admin_members.items() if len(groups) > 1}
        all_unique = set(non_admin_members.keys())

        groups_summary = [{"name": info["name"], "members": info["total"], "raw_count": info.get("raw_count", 0)} for gid, info in group_members.items()]

        return {
            "groups": groups_summary,
            "total_groups": len(group_members),
            "unique_members": len(all_unique),
            "duplicates_count": len(duplicates),
            "duplicate_percentage": f"{len(duplicates) / max(len(all_unique), 1) * 100:.1f}%",
            "duplicate_members": [
                {"phone": mid, "in_groups": groups}
                for mid, groups in sorted(duplicates.items())
            ],
            "debug": debug_info
        }
    except Exception as e:
        logger.error(f"Error checking duplicates: {e}")
        return {"error": str(e), "unique_members": 0, "duplicates_count": 0}


@app.post("/api/admin/clean-duplicates")
async def api_clean_duplicates(username: str = Depends(get_current_username)):
    """Remove duplicate members from groups — keeps them in the first group (by order), removes from extras."""
    await ensure_token()
    db = SessionLocal()
    try:
        managed = db.query(WhatsAppGroup).all()
        if len(managed) < 2:
            return {"error": "Menos de 2 grupos gerenciados"}

        def extract_phone(member):
            if isinstance(member, str):
                return member.split('@')[0]
            if isinstance(member, dict):
                for field in ['id', '_serialized', 'user']:
                    val = member.get(field, '')
                    if val:
                        if isinstance(val, dict):
                            inner = val.get('_serialized', val.get('user', ''))
                            if inner:
                                return str(inner).split('@')[0]
                        else:
                            return str(val).split('@')[0]
            return None

        # Build phone -> groups mapping
        phone_groups = {}  # phone -> [(group_jid, group_name, member_raw_id)]
        debug_groups = []
        for g in managed:
            try:
                members_raw = await wpp.get_group_participants(g.group_jid)
                member_count = len(members_raw) if isinstance(members_raw, list) else 0
                debug_groups.append({"name": g.name or g.group_jid[:20], "members": member_count})
                logger.info(f"CLEAN_DUP: group={g.name}, jid={g.group_jid[:20]}, members={member_count}")
                if isinstance(members_raw, list):
                    for m in members_raw:
                        phone = extract_phone(m)
                        if phone and len(phone) >= 8:
                            # Get the raw ID for removal
                            if isinstance(m, dict):
                                mid_raw = m.get('id', '')
                                if isinstance(mid_raw, dict):
                                    raw_id = mid_raw.get('_serialized', '')
                                else:
                                    raw_id = str(mid_raw)
                            else:
                                raw_id = str(m)
                            if phone not in phone_groups:
                                phone_groups[phone] = []
                            phone_groups[phone].append((g.group_jid, g.name or g.group_jid[:20], raw_id))
            except Exception as e:
                logger.error(f"Clean duplicates: error fetching {g.group_jid}: {e}")
                debug_groups.append({"name": g.name or g.group_jid[:20], "error": str(e)})

        # Find duplicates (in 2+ groups)
        duplicates = {phone: groups for phone, groups in phone_groups.items() if len(groups) >= 2}
        logger.info(f"CLEAN_DUP: total_phones={len(phone_groups)}, total_duplicates={len(duplicates)}")

        removed = []
        failed = []

        for phone, groups in duplicates.items():
            # Keep in the FIRST group, remove from all others
            keep_group = groups[0]
            for group_jid, group_name, raw_id in groups[1:]:
                try:
                    await asyncio.sleep(random.uniform(1.5, 3.0))  # Anti-ban delay
                    # USE phone@c.us format — raw @lid IDs don't work for removal
                    participant_cus = f"{phone}@c.us"
                    logger.info(f"CLEAN_DUP: removing {participant_cus} from {group_name}")
                    success = await wpp.remove_participant(group_jid, participant_cus)
                    if success:
                        removed.append({"phone": phone, "removed_from": group_name, "kept_in": keep_group[1]})
                        logger.info(f"CLEAN_DUP: ✅ removed {phone} from {group_name}, kept in {keep_group[1]}")
                    else:
                        failed.append({"phone": phone, "group": group_name, "error": "API returned false"})
                        logger.warning(f"CLEAN_DUP: ❌ FAILED {phone} from {group_name}")
                except Exception as e:
                    failed.append({"phone": phone, "group": group_name, "error": str(e)})

        return {"removed": removed, "failed": failed, "total_duplicates": len(duplicates), "debug": debug_groups}
    except Exception as e:
        logger.error(f"Error cleaning duplicates: {e}")
        return {"error": str(e)}
    finally:
        db.close()

@app.get("/api/admin/exclusivity-report")
async def api_exclusivity_report(username: str = Depends(get_current_username)):
    """Return today's exclusivity cleanup report."""
    today = datetime.now().strftime("%Y-%m-%d")
    if _exclusivity_report["date"] != today:
        return {"date": today, "removed_members": [], "last_run": None, "running": _exclusivity_running}
    report = dict(_exclusivity_report)
    report["running"] = _exclusivity_running
    return report

@app.post("/api/admin/trigger-exclusive-cleanup")
async def api_trigger_exclusive_cleanup(request: Request, username: str = Depends(get_current_username)):
    """Manually trigger the exclusive cleanup job (runs in background).
    
    Optional JSON body: {"group_ids": ["jid1@g.us", "jid2@g.us"]}
    """
    global _exclusivity_running
    if _exclusivity_running:
        return {"status": "already_running", "message": "Já está em execução"}

    # Parse optional group_ids from JSON body
    group_ids = None
    try:
        body = await request.json()
        group_ids = body.get("group_ids") or None
    except Exception:
        pass

    _exclusivity_running = True

    async def _run():
        global _exclusivity_running
        try:
            await job_exclusive_cleanup(force=True, group_ids=group_ids)
        except Exception as e:
            logger.error(f"Manual trigger error: {e}")
        finally:
            _exclusivity_running = False

    asyncio.create_task(_run())
    groups_info = f" em {len(group_ids)} grupo(s)" if group_ids else " em todos os grupos"
    return {"status": "started", "message": f"Varredura iniciada em background{groups_info}"}

@app.post("/api/admin/cleanup-target-group")
async def api_cleanup_target_group(request: Request, username: str = Depends(get_current_username)):
    """Scan ALL groups, but remove duplicates ONLY from a specific target group.
    
    JSON body: {"target_group_id": "120363408281153077@g.us"}
    """
    try:
        body = await request.json()
        target_group_id = body.get("target_group_id", "")
    except Exception:
        return {"error": "JSON body with 'target_group_id' is required"}
    
    if not target_group_id:
        return {"error": "target_group_id is required"}

    async def _run_targeted():
        await ensure_token()
        
        # 1. Get ALL groups
        all_groups_raw = await wpp.get_all_groups()
        if not all_groups_raw:
            logger.info("TARGETED_CLEANUP: no groups found")
            return
        
        groups = []
        for g in all_groups_raw:
            if isinstance(g, dict):
                gid = g.get("id", g.get("_serialized", ""))
                if isinstance(gid, dict):
                    gid = gid.get("_serialized", "")
                gname = g.get("name", g.get("subject", str(gid)[:20]))
            elif isinstance(g, str):
                gid, gname = g, g[:20]
            else:
                continue
            if gid:
                groups.append({"jid": str(gid), "name": str(gname)})
        
        logger.info(f"TARGETED_CLEANUP: Found {len(groups)} groups, target={target_group_id[:25]}")
        
        def extract_phone(member):
            if isinstance(member, str):
                return member.split('@')[0]
            if isinstance(member, dict):
                for field in ['id', '_serialized', 'user']:
                    val = member.get(field, '')
                    if val:
                        if isinstance(val, dict):
                            inner = val.get('_serialized', val.get('user', ''))
                            if inner:
                                return str(inner).split('@')[0]
                        else:
                            return str(val).split('@')[0]
            return None
        
        def extract_raw_id(member):
            if isinstance(member, str):
                return member
            if isinstance(member, dict):
                for f in ['id', '_serialized']:
                    v = member.get(f, '')
                    if isinstance(v, dict):
                        s = v.get('_serialized', '')
                        if s:
                            return str(s)
                    elif v:
                        return str(v)
            return None
        
        # 2. Get admin phones to exclude
        admin_phones = set(get_super_admins())
        for g in groups:
            try:
                admins_raw = await wpp.get_group_admins(g["jid"])
                if isinstance(admins_raw, list):
                    for a in admins_raw:
                        ap = extract_phone(a)
                        if ap and len(ap) > 3:
                            admin_phones.add(ap)
            except Exception:
                pass
        
        # 3. Fetch members of ALL groups
        all_members = {}  # phone -> set of group_jids
        target_members = {}  # phone -> raw_id (only for target group)
        
        for i, g in enumerate(groups):
            if i > 0:
                await asyncio.sleep(random.uniform(2.0, 4.0))
            try:
                members_raw = await wpp.get_group_participants(g["jid"])
                if isinstance(members_raw, list):
                    logger.info(f"TARGETED_CLEANUP: {g['name']} = {len(members_raw)} members")
                    for m in members_raw:
                        phone = extract_phone(m)
                        if phone and len(phone) >= 8:
                            if phone not in all_members:
                                all_members[phone] = set()
                            all_members[phone].add(g["jid"])
                            if g["jid"] == target_group_id:
                                target_members[phone] = extract_raw_id(m)
            except Exception as e:
                logger.error(f"TARGETED_CLEANUP: error fetching {g['name']}: {e}")
        
        # 4. Find duplicates: people in target group AND at least 1 other group
        to_remove = []
        for phone, groups_set in all_members.items():
            if target_group_id in groups_set and len(groups_set) >= 2 and phone not in admin_phones:
                raw_id = target_members.get(phone)
                other_groups = [gid for gid in groups_set if gid != target_group_id]
                to_remove.append({"phone": phone, "raw_id": raw_id, "other_groups": other_groups})
        
        logger.info(f"TARGETED_CLEANUP: {len(to_remove)} duplicates to remove from target group")
        
        # 5. Remove ONLY from target group
        removed = 0
        failed = 0
        BATCH_SIZE = 5
        for i, item in enumerate(to_remove):
            try:
                if i > 0:
                    delay = random.uniform(30.0, 90.0)
                    logger.info(f"TARGETED_CLEANUP: pause {delay:.0f}s before next...")
                    await asyncio.sleep(delay)
                
                if i > 0 and i % BATCH_SIZE == 0:
                    batch_pause = random.uniform(180.0, 360.0)
                    logger.info(f"TARGETED_CLEANUP: batch pause {batch_pause/60:.1f}min")
                    await asyncio.sleep(batch_pause)
                
                id_to_use = item["raw_id"] if item["raw_id"] else f"{item['phone']}@c.us"
                logger.info(f"TARGETED_CLEANUP: removing {item['phone']} from target (also in {len(item['other_groups'])} other group(s))")
                
                success = await wpp.remove_participant(target_group_id, id_to_use)
                if success:
                    removed += 1
                    logger.info(f"TARGETED_CLEANUP: ✅ removed {item['phone']}")
                else:
                    failed += 1
                    logger.warning(f"TARGETED_CLEANUP: ❌ failed {item['phone']}")
            except Exception as e:
                failed += 1
                logger.error(f"TARGETED_CLEANUP: error {item['phone']}: {e}")
        
        logger.info(f"TARGETED_CLEANUP: DONE. Removed={removed}, Failed={failed}, Total={len(to_remove)}")
    
    asyncio.create_task(_run_targeted())
    return {"status": "started", "target_group": target_group_id, "message": "Varredura iniciada. Remoções APENAS do grupo alvo."}

@app.get("/api/admin/all-groups")
async def api_admin_all_groups(username: str = Depends(get_current_username)):
    """Return list of all groups the bot is part of (for the frontend group picker)."""
    await ensure_token()
    try:
        raw = await wpp.get_all_groups()
        # Reuse parse_group() which handles all WPPConnect nested name fields correctly
        groups = [{"id": p["id"], "name": p["name"]} for p in (parse_group(g) for g in (raw or [])) if p["id"]]
        groups.sort(key=lambda x: x["name"])
        return {"groups": groups}
    except Exception as e:
        logger.error(f"api_admin_all_groups error: {e}")
        return {"groups": [], "error": str(e)}


@app.get("/api/admin/group-members-list")
async def api_group_members_list(username: str = Depends(get_current_username)):
    """List all members of all managed groups."""
    await ensure_token()
    db = SessionLocal()
    try:
        managed = db.query(WhatsAppGroup).all()
        result = []
        for g in managed:
            try:
                members_raw = await wpp.get_group_participants(g.group_jid)
                phones = []
                if isinstance(members_raw, list):
                    for m in members_raw:
                        if isinstance(m, dict):
                            mid = m.get('id', '')
                            if isinstance(mid, dict):
                                mid = mid.get('_serialized', mid.get('user', ''))
                            phones.append(str(mid).split('@')[0])
                        else:
                            phones.append(str(m).split('@')[0])
                result.append({"name": g.name or g.group_jid[:20], "jid": g.group_jid, "members": phones, "count": len(phones)})
            except Exception as e:
                result.append({"name": g.name or g.group_jid[:20], "jid": g.group_jid, "error": str(e), "count": 0})
        return {"groups": result}
    finally:
        db.close()

@app.post("/api/admin/unique-members")
async def api_unique_members(request: Request, username: str = Depends(get_current_username)):
    """Get unique member phone numbers across selected bot groups (excluding admins). Used by roulette."""
    await ensure_token()
    try:
        body = {}
        try:
            body = await request.json()
        except Exception:
            pass
        filter_ids = body.get("group_ids", [])  # Empty = all groups

        all_groups = await wpp.get_all_groups()
        if not all_groups:
            return {"members": [], "total": 0, "groups_scanned": 0}

        # Parse group IDs
        group_ids = []
        for g in all_groups:
            gid = ""
            if isinstance(g, dict):
                gid = g.get("id", g.get("_serialized", ""))
                if isinstance(gid, dict):
                    gid = gid.get("_serialized", "")
            elif isinstance(g, str):
                gid = g
            if gid:
                group_ids.append(str(gid))

        # Filter by selected groups if provided
        if filter_ids:
            group_ids = [gid for gid in group_ids if gid in filter_ids]

        # Collect admin phones to exclude
        admin_phones = set(get_super_admins())
        for gid in group_ids:
            try:
                admins_raw = await wpp.get_group_admins(gid)
                if isinstance(admins_raw, list) and len(admins_raw) > 0 and isinstance(admins_raw[0], list):
                    admins_raw = admins_raw[0]
                if isinstance(admins_raw, list):
                    for a in admins_raw:
                        if isinstance(a, dict):
                            for key in ['id', 'user', '_serialized']:
                                v = a.get(key, '')
                                if v:
                                    admin_phones.add(str(v).split('@')[0])
                        elif isinstance(a, str):
                            admin_phones.add(a.split('@')[0])
            except Exception:
                pass

        # Collect all unique phones across groups
        all_phones = set()
        groups_scanned = 0
        for gid in group_ids:
            try:
                members_raw = await wpp.get_group_participants(gid)
                if isinstance(members_raw, list):
                    groups_scanned += 1
                    for m in members_raw:
                        if isinstance(m, dict):
                            mid = m.get('id', '')
                            if isinstance(mid, dict):
                                mid = mid.get('_serialized', mid.get('user', ''))
                            phone = str(mid).split('@')[0]
                        else:
                            phone = str(m).split('@')[0]
                        if phone and len(phone) >= 8:
                            all_phones.add(phone)
            except Exception as e:
                logger.error(f"UNIQUE_MEMBERS: error fetching {gid[:25]}: {e}")

        # Remove admins
        unique = sorted(all_phones - admin_phones)
        logger.info(f"UNIQUE_MEMBERS: {len(unique)} unique non-admin members from {groups_scanned} groups")
        return {"members": unique, "total": len(unique), "groups_scanned": groups_scanned, "admins_excluded": len(admin_phones)}
    except Exception as e:
        logger.error(f"UNIQUE_MEMBERS error: {e}")
        return {"members": [], "total": 0, "error": str(e)}

@app.get("/api/admin/pending-requests")
async def api_pending_requests(username: str = Depends(get_current_username)):
    """List pending membership requests for all managed groups."""
    await ensure_token()
    db = SessionLocal()
    try:
        managed = db.query(WhatsAppGroup).all()
        result = []
        for g in managed:
            try:
                requests = await wpp.get_membership_requests(g.group_jid)
                pending = []
                if isinstance(requests, list):
                    for req in requests:
                        if isinstance(req, dict):
                            req_id = req.get('id', '')
                            if isinstance(req_id, dict):
                                req_id = req_id.get('_serialized', '')
                            pending.append({"id": str(req_id), "phone": str(req_id).split('@')[0]})
                        else:
                            pending.append({"id": str(req), "phone": str(req).split('@')[0]})
                result.append({"name": g.name or g.group_jid[:20], "jid": g.group_jid, "pending": pending, "count": len(pending)})
            except Exception as e:
                result.append({"name": g.name or g.group_jid[:20], "error": str(e), "count": 0})
        return {"groups": result}
    finally:
        db.close()



@app.get("/api/groups/managed")
async def api_managed_groups_list(username: str = Depends(get_current_username)):
    """List all managed groups with real-time member counts from WhatsApp."""
    await ensure_token()
    
    db = SessionLocal()
    try:
        groups = db.query(WhatsAppGroup).order_by(WhatsAppGroup.display_order).all()
        
        # Fetch REAL member counts in parallel for all managed groups
        async def get_real_count(jid: str) -> int:
            try:
                members = await wpp.get_group_participants(jid)
                if isinstance(members, list):
                    return len(members)
            except Exception as e:
                logger.warning(f"Could not get real count for {jid[:25]}: {e}")
            return 0
        
        real_counts = await asyncio.gather(*[get_real_count(g.group_jid) for g in groups])
        
        for g, real in zip(groups, real_counts):
            if real > 0:
                g.current_members = real
            g.is_active = g.current_members < g.max_members
            # Auto-fetch missing invite links
            if not g.invite_link or g.invite_link in ('#', ''):
                try:
                    link = await wpp.get_group_invite_link(g.group_jid)
                    if link:
                        g.invite_link = link
                        logger.info(f"AUTO_INVITE_LINK: {g.name} -> {link[:40]}...")
                except Exception as e:
                    logger.warning(f"Could not fetch invite link for {g.name}: {e}")
        db.commit()
        
        return {"groups": [{
            "id": g.id, "group_jid": g.group_jid, "name": g.name,
            "invite_link": g.invite_link, "max_members": g.max_members,
            "current_members": g.current_members, "is_active": g.is_active,
            "display_order": g.display_order,
            "remaining": max(0, g.max_members - g.current_members)
        } for g in groups], "redirect_target_jid": cfg.get("redirect_target_jid", "")}
    finally:
        db.close()

@app.post("/api/groups/managed/set-redirect-target")
async def api_set_redirect_target(request: Request, username: str = Depends(get_current_username)):
    """Set which group receives all new redirected members."""
    body = await request.json()
    jid = body.get("group_jid", "")
    cfg.save_settings({"redirect_target_jid": jid})
    logger.info(f"REDIRECT_TARGET: set to {jid}")
    return {"status": "success", "redirect_target_jid": jid}

@app.get("/api/affiliate/stats")
async def api_affiliate_stats(username: str = Depends(get_current_username)):
    """Stats for the admin dashboard."""
    from sqlalchemy import func
    db = SessionLocal()
    try:
        total_affiliates = db.query(Affiliate).filter_by(is_active=True).count()
        total_clicks = db.query(JoinTracking).count()
        total_confirmed = db.query(JoinTracking).filter_by(confirmed=True).count()
        
        # Ranking data — aggregated by NAME, sorted by CONFIRMED JOINS
        # This merges multiple links from the same person into one entry
        from sqlalchemy import Integer, case
        confirmed_expr = func.sum(case((JoinTracking.confirmed == True, 1), else_=0))
        
        results = db.query(
            Affiliate.name,
            func.min(Affiliate.slug).label("slug"),
            func.count(JoinTracking.id).label("clicks"),
            confirmed_expr.label("confirmed")
        ).outerjoin(JoinTracking, JoinTracking.affiliate_id == Affiliate.id
        ).filter(Affiliate.is_active == True
        ).group_by(func.lower(Affiliate.name)
        ).order_by(confirmed_expr.desc()).limit(20).all()
        
        ranking = []
        for r in results:
            # Skip any existing fake entries from DB
            if FAKE_AFFILIATE and r.name.lower() == FAKE_AFFILIATE["name"].lower():
                continue
            ranking.append({"name": r.name, "slug": r.slug, "clicks": r.clicks or 0, "confirmed": int(r.confirmed or 0)})
        
        # Inject fake affiliate at configured position (visible in admin too)
        if FAKE_AFFILIATE:
            pos = FAKE_AFFILIATE["position"] - 1  # 0-indexed
            if pos > 0 and len(ranking) >= pos:
                fake_count = ranking[pos - 1]["confirmed"] - 1
            elif len(ranking) > pos:
                fake_count = ranking[pos]["confirmed"]
            else:
                fake_count = 0
            ranking.insert(pos, {"name": FAKE_AFFILIATE["name"], "slug": "fake", "clicks": 0, "confirmed": max(fake_count, 0)})
        
        return {
            "total_affiliates": total_affiliates,
            "total_clicks": total_clicks,
            "total_confirmed": total_confirmed,
            "ranking": ranking
        }
    finally:
        db.close()

@app.post("/api/groups/managed/add")
async def api_managed_groups_add(request: Request, username: str = Depends(get_current_username)):
    """Add or update a managed group. Auto-fetches member count and invite link from WPP."""
    body = await request.json()
    group_jid = body.get("group_jid", "")
    
    # Auto-fetch real member count from bot
    await ensure_token()
    real_count = 0
    groups = await wpp.get_all_groups()
    for g in groups:
        info = parse_group(g)
        if info["id"] == group_jid:
            real_count = info["size"]
            break
    
    # Auto-fetch invite link from bot
    invite_link = body.get("invite_link", "")
    if not invite_link:
        invite_link = await wpp.get_group_invite_link(group_jid)
    
    max_members = body.get("max_members", 256)
    is_active = real_count < max_members
    
    db = SessionLocal()
    try:
        existing = db.query(WhatsAppGroup).filter_by(group_jid=group_jid).first() if group_jid else None
        
        if existing:
            existing.name = body.get("name", existing.name)
            existing.invite_link = invite_link or existing.invite_link
            existing.max_members = max_members
            existing.current_members = real_count
            existing.is_active = is_active
            existing.display_order = body.get("display_order", existing.display_order)
            db.commit()
            return {"status": "success", "action": "updated", "id": existing.id, "current_members": real_count}
        else:
            group = WhatsAppGroup(
                group_jid=group_jid,
                name=body.get("name", ""),
                invite_link=invite_link,
                max_members=max_members,
                current_members=real_count,
                is_active=is_active,
                display_order=body.get("display_order", 0)
            )
            db.add(group)
            db.commit()
            return {"status": "success", "action": "created", "id": group.id, "current_members": real_count}
    finally:
        db.close()

@app.delete("/api/groups/managed/remove/{group_id}")
async def api_managed_groups_remove(group_id: int, username: str = Depends(get_current_username)):
    """Remove a managed group from the affiliate system."""
    db = SessionLocal()
    try:
        group = db.query(WhatsAppGroup).filter_by(id=group_id).first()
        if not group:
            return JSONResponse({"status": "error", "message": "Grupo não encontrado."}, status_code=404)
        db.delete(group)
        db.commit()
        return {"status": "success", "message": f"Grupo '{group.name}' removido."}
    finally:
        db.close()

@app.post("/api/groups/sync-links")
async def api_sync_invite_links(username: str = Depends(get_current_username)):
    """Force immediate sync of all group invite links."""
    await job_sync_invite_links()
    # Return updated groups
    db = SessionLocal()
    try:
        groups = db.query(WhatsAppGroup).all()
        return {"status": "success", "groups": [
            {"name": g.name, "invite_link": g.invite_link, "group_jid": g.group_jid}
            for g in groups
        ]}
    finally:
        db.close()

@app.post("/api/admin/scan-pending-requests")
async def api_scan_pending_requests(username: str = Depends(get_current_username)):
    """Scan all managed groups for pending membership requests and approve/reject based on exclusivity."""
    cfg = load_settings()
    db = SessionLocal()
    try:
        managed = db.query(WhatsAppGroup).all()
        if not managed:
            return {"error": "Nenhum grupo gerenciado encontrado"}
        
        managed_jids = [g.group_jid for g in managed]
        group_names = {g.group_jid: g.name for g in managed}
        
        # First, build member map for all groups
        group_members = {}
        for g in managed:
            try:
                members = await wpp.get_group_participants(g.group_jid)
                if isinstance(members, list):
                    ids = set()
                    for m in members:
                        if isinstance(m, dict):
                            mid = m.get('id', m.get('_serialized', ''))
                            if isinstance(mid, dict):
                                mid = mid.get('_serialized', mid.get('user', ''))
                            ids.add(str(mid).split('@')[0])
                        elif isinstance(m, str):
                            ids.add(m.split('@')[0])
                    group_members[g.group_jid] = ids
                    logger.info(f"SCAN_PENDING: {g.name} has {len(ids)} members")
            except Exception as e:
                logger.error(f"SCAN_PENDING: Error fetching members for {g.name}: {e}")
        
        # Now scan pending requests for each group
        results = {"approved": [], "rejected": [], "errors": [], "groups_scanned": 0}
        
        for g in managed:
            try:
                pending = await wpp.get_membership_requests(g.group_jid)
                if not pending:
                    continue
                
                results["groups_scanned"] += 1
                logger.info(f"SCAN_PENDING: {g.name} has {len(pending)} pending requests")
                
                for req in pending:
                    req_id = ""
                    if isinstance(req, dict):
                        req_id = req.get('id', req.get('requester', req.get('participant', '')))
                        if isinstance(req_id, dict):
                            req_id = req_id.get('_serialized', req_id.get('user', ''))
                    elif isinstance(req, str):
                        req_id = req
                    
                    if not req_id:
                        continue
                    
                    req_phone = str(req_id).split('@')[0]
                    
                    # Check if this person is in any other managed group
                    found_in = None
                    for other_jid, members in group_members.items():
                        if other_jid != g.group_jid and req_phone in members:
                            found_in = group_names.get(other_jid, other_jid)
                            break
                    
                    if found_in and cfg.get("exclusive_groups_enabled", True):
                        # Reject — duplicate
                        logger.info(f"SCAN_PENDING_REJECT: {req_phone} in {found_in}, rejecting from {g.name}")
                        await asyncio.sleep(random.uniform(1.0, 3.0))
                        ok = await wpp.reject_participant(g.group_jid, req_id)
                        results["rejected"].append({
                            "phone": req_phone,
                            "group": g.name,
                            "already_in": found_in,
                            "success": ok
                        })
                    else:
                        # Not duplicate — leave pending for manual approval
                        logger.info(f"SCAN_PENDING_SKIP: {req_phone} not duplicate, leaving pending for {g.name}")
                        results["skipped"] = results.get("skipped", 0) + 1
            except Exception as e:
                logger.error(f"SCAN_PENDING: Error scanning {g.name}: {e}")
                results["errors"].append({"group": g.name, "error": str(e)})
        
        return results
    except Exception as e:
        logger.error(f"Error in scan-pending-requests: {e}")
        return {"error": str(e)}
    finally:
        db.close()

# --- Bot: Handle participant changes (join/leave) ---
async def handle_participant_change(data: dict):
    """Process onparticipantschanged webhook to confirm affiliate joins and update member counts."""
    try:
        action = data.get("action", data.get("event", ""))
        group_id = data.get("groupId", data.get("chat", data.get("chatId", "")))
        who = data.get("who", data.get("participant", data.get("participantId", "")))
        
        logger.info(f"PARTICIPANT_CHANGE: action={action}, group={str(group_id)[:30]}, who={str(who)[:50]}")
        logger.info(f"PARTICIPANT_CHANGE_RAW_KEYS: {list(data.keys())}")
        
        if not group_id or not who:
            logger.warning(f"PARTICIPANT_CHANGE: missing data. group_id={group_id}, who={who}")
            return
        
        # Normalize who → always a list of phone strings
        if isinstance(who, list):
            phones = [w.replace("@c.us", "").replace("@s.whatsapp.net", "").replace("@lid", "") for w in who if isinstance(w, str)]
        elif isinstance(who, str):
            phones = [who.replace("@c.us", "").replace("@s.whatsapp.net", "").replace("@lid", "")]
        else:
            logger.warning(f"PARTICIPANT_CHANGE: unexpected 'who' type: {type(who)} = {who}")
            return
        
        if not phones:
            return
        
        db = SessionLocal()
        try:
            group = db.query(WhatsAppGroup).filter_by(group_jid=group_id).first()
            if not group:
                logger.info(f"PARTICIPANT_CHANGE: group not found for jid={str(group_id)[:40]}")
                return  # Not a managed group
            
            if action in ("add", "join"):
                for phone in phones:
                    # Update member count
                    group.current_members = min(group.current_members + 1, group.max_members)
                    
                    # Check if group is now full → deactivate
                    if group.current_members >= group.max_members:
                        group.is_active = False
                        logger.info(f"GROUP_FULL: {group.name} ({group.current_members}/{group.max_members}) → deactivated")
                    
                    # Try to match with recent affiliate click (last 24 hours)
                    cutoff = datetime.utcnow() - timedelta(hours=24)
                    pending = db.query(JoinTracking).filter(
                        JoinTracking.group_id == group.id,
                        JoinTracking.confirmed == False,
                        JoinTracking.clicked_at >= cutoff
                    ).order_by(JoinTracking.clicked_at.desc()).first()
                    
                    if pending:
                        pending.confirmed = True
                        pending.joined_at = datetime.utcnow()
                        pending.user_phone = phone
                        affiliate = db.query(Affiliate).filter_by(id=pending.affiliate_id).first()
                        aff_name = affiliate.name if affiliate else "?"
                        logger.info(f"AFFILIATE_JOIN_CONFIRMED: phone={phone}, affiliate={aff_name}, group={group.name}")
                    else:
                        logger.info(f"JOIN_NO_AFFILIATE: phone={phone}, group={group.name}")
                
            elif action in ("remove", "leave"):
                for phone in phones:
                    group.current_members = max(group.current_members - 1, 0)
                # Re-activate group if it was full and someone left
                if not group.is_active and group.current_members < group.max_members:
                    group.is_active = True
                    logger.info(f"GROUP_REACTIVATED: {group.name} ({group.current_members}/{group.max_members})")
            
            db.commit()
        finally:
            db.close()
    except Exception as e:
        logger.error(f"Error in handle_participant_change: {e}", exc_info=True)


# --- GROUP ADMIN CACHE ---
# Cache group admins for 5 minutes to avoid calling WPP API on every message
_group_admin_cache = {}  # {group_id: {"ids": set(), "time": datetime}}
_GROUP_ADMIN_CACHE_TTL = 300  # seconds

async def get_group_admin_ids(group_id: str) -> set:
    """Get admin IDs for a group, cached for 5 minutes."""
    now = datetime.now()
    cached = _group_admin_cache.get(group_id)
    if cached and (now - cached["time"]).total_seconds() < _GROUP_ADMIN_CACHE_TTL:
        return cached["ids"]
    
    admin_ids = set()
    try:
        admins = await wpp.get_group_admins(group_id)
        
        # Flatten nested arrays: API may return [[{...}, {...}]] instead of [{...}, {...}]
        if isinstance(admins, list) and len(admins) > 0 and isinstance(admins[0], list):
            admins = admins[0]
        
        if isinstance(admins, list):
            for admin in admins:
                if isinstance(admin, dict):
                    for key in ['id', 'user', '_serialized']:
                        v = admin.get(key, '')
                        if v:
                            admin_ids.add(v)
                            admin_ids.add(v.split('@')[0])
                elif isinstance(admin, str):
                    admin_ids.add(admin)
                    admin_ids.add(admin.split('@')[0])
        
        logger.info(f"GROUP_ADMINS: {group_id[:20]} -> {len(admin_ids)} identifiers")
    except Exception as e:
        logger.error(f"Error fetching group admins for {group_id}: {e}")
    
    _group_admin_cache[group_id] = {"ids": admin_ids, "time": now}
    return admin_ids

# --- WEBHOOK ---
# Debug: store last 20 webhook events for inspection
_webhook_log = []

# Per-user lock to prevent race conditions with grouped images
_flood_locks = {}  # {"sender_id:group_id": asyncio.Lock()}

def _get_flood_lock(sender_id: str, group_id: str) -> asyncio.Lock:
    """Get or create an asyncio lock for a sender+group pair."""
    key = f"{sender_id}:{group_id}"
    if key not in _flood_locks:
        _flood_locks[key] = asyncio.Lock()
    # Cleanup: remove old locks if too many accumulate
    if len(_flood_locks) > 1000:
        _flood_locks.clear()
        _flood_locks[key] = asyncio.Lock()
    return _flood_locks[key]

def _log_webhook(entry):
    """Append to webhook log buffer, keeping max 20 entries."""
    _webhook_log.append(entry)
    if len(_webhook_log) > 20: _webhook_log.pop(0)

# --- Warn cooldown: prevent multiple warnings per user within 60s ---
_warn_cooldown = {}  # key=(sender_id, group_id) -> last_warn_time
_WARN_COOLDOWN_SECONDS = 60

def has_phone_number(text: str) -> bool:
    """Check if text contains a Brazilian phone number (8-11 digits)."""
    if not text:
        return False
    # Matches formats: 98879-3966, 95701-4008, (11)98879-3966, +55 11 98879-3966, etc.
    pattern = r'(?:\+?55[\s\-]?)?(?:\(?\d{2}\)?[\s\-]?)?\d{4,5}[\s\-]?\d{4}'
    return bool(re.search(pattern, text))

async def enforce_action(action: str, group_id: str, msg_id: str, warn_text: str, sender_id: str = "") -> dict:
    """Execute moderation action: delete, delete_warn, or warn. Returns result dict.
    Warnings are debounced per sender+group to avoid flooding the group."""
    result = {"action": action}
    
    # DRY-RUN: log what would happen but don't actually do it
    if cfg.get("dry_run_mode", False):
        result["dry_run"] = True
        logger.info(f"🧪 DRY-RUN: WOULD {action} msg={str(msg_id)[:30]} in {group_id[:25]}")
        if action in ("warn", "delete_warn"):
            result["would_warn"] = warn_text[:50]
        return result
    
    logger.info(f"enforce_action: action={action}, group={group_id}, msg_id={msg_id}")
    try:
        # Anti-ban: random delay (1-3s) to appear human
        delay = random.uniform(1.0, 3.0)
        await asyncio.sleep(delay)
        result["delay"] = round(delay, 1)
        
        if action in ("delete", "delete_warn"):
            logger.info(f"Calling delete_message for {group_id} / {msg_id}")
            delete_result = await wpp.delete_message(group_id, msg_id)
            result["delete"] = str(delete_result)[:200] if delete_result else "called"
        if action in ("warn", "delete_warn"):
            # Check cooldown: only warn once per user per group within 60s
            cooldown_key = (sender_id, group_id) if sender_id else None
            now = datetime.now()
            should_warn = True
            if cooldown_key:
                last_warn = _warn_cooldown.get(cooldown_key)
                if last_warn and (now - last_warn).total_seconds() < _WARN_COOLDOWN_SECONDS:
                    should_warn = False
                    result["warn"] = "skipped_cooldown"
                    logger.info(f"WARN_SKIPPED: cooldown active for {sender_id[:20]} in {group_id[:20]}")
            
            if should_warn:
                logger.info(f"Calling send_message (warn) for {group_id}")
                send_result = await wpp.send_message(group_id, warn_text, skip_typing=True)
                result["warn"] = str(send_result)[:200] if send_result else "called"
                if cooldown_key:
                    _warn_cooldown[cooldown_key] = now
                    # Cleanup old entries (keep dict small)
                    if len(_warn_cooldown) > 200:
                        cutoff = now - timedelta(seconds=_WARN_COOLDOWN_SECONDS * 2)
                        _warn_cooldown.clear()
    except Exception as e:
        logger.error(f"enforce_action error: {e}")
        result["error"] = str(e)
    return result

@app.get("/api/debug/webhook-log")
async def api_debug_webhook_log(username: str = Depends(get_current_username)):
    """Return last 20 webhook events for debugging."""
    return {"log": _webhook_log, "total": len(_webhook_log)}

@app.post("/api/debug/reset-counters")
async def api_debug_reset_counters(username: str = Depends(get_current_username)):
    """Reset ALL flood counters. Use when counters are out of sync."""
    db = SessionLocal()
    try:
        count = db.query(UserStats).count()
        db.query(UserStats).delete()
        db.commit()
        logger.info(f"RESET: Cleared {count} flood counter records")
        return {"status": "success", "cleared": count}
    except Exception as e:
        db.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        db.close()

@app.get("/api/debug/group-admins/{group_id}")
async def api_debug_group_admins(group_id: str, username: str = Depends(get_current_username)):
    """Debug: show raw admin data for a group."""
    raw_admins = await wpp.get_group_admins(group_id)
    cached_ids = await get_group_admin_ids(group_id)
    return {"group_id": group_id, "raw_admins": raw_admins, "cached_ids": list(cached_ids)}

async def handle_membership_request(group_id: str, participant_ids: list, managed_jids: list):
    """Handle membership approval requests: approve if not duplicate, reject if already in another group."""
    cfg = load_settings()
    if not cfg.get("exclusive_groups_enabled", True):
        # Exclusivity disabled — leave requests pending for manual approval
        logger.info(f"MEMBERSHIP_REQUEST: exclusivity off, leaving pending for {group_id[:20]}")
        return
    
    reject_msg = cfg.get("exclusive_reject_message", "")
    other_groups = [g for g in managed_jids if g != group_id]
    
    for participant_id in participant_ids:
        p_phone = participant_id.replace('@c.us','').replace('@s.whatsapp.net','').replace('@lid','').split('@')[0]
        p_identifiers = {participant_id, p_phone}
        
        found_in = None
        if other_groups:
            for i, other_group in enumerate(other_groups):
                try:
                    # Small delay between reading different groups (crawler safety)
                    if i > 0:
                        await asyncio.sleep(random.uniform(2.0, 4.0))
                        
                    members = await wpp.get_group_participants(other_group)
                    if isinstance(members, list):
                        for m in members:
                            if isinstance(m, dict):
                                mid_raw = m.get('id', '')
                                if isinstance(mid_raw, dict):
                                    mid_raw = mid_raw.get('_serialized', mid_raw.get('user', ''))
                                m_phone = str(mid_raw).replace('@c.us','').replace('@s.whatsapp.net','').replace('@lid','').split('@')[0]
                            else:
                                m_phone = str(m).split('@')[0]
                            if m_phone and m_phone in p_identifiers:
                                found_in = other_group
                                break
                    if found_in:
                        break
                except Exception as e:
                    logger.error(f"Membership request check error for group {other_group}: {e}")
        
        if found_in:
            # REJECT — already in another managed group
            logger.info(f"MEMBERSHIP_REJECT: {participant_id[:20]} already in {found_in[:20]}, rejecting from {group_id[:20]}")
            
            # Significant randomized delay before rejection (10 - 20 seconds)
            await asyncio.sleep(random.uniform(10.0, 20.0))
            
            rejected = await wpp.reject_participant(group_id, participant_id)
            if rejected:
                logger.info(f"MEMBERSHIP_REJECTED: {participant_id[:20]} from {group_id[:20]}")
            else:
                logger.error(f"MEMBERSHIP_REJECT_FAILED: {participant_id[:20]} from {group_id[:20]}")
        else:
            # Not a duplicate — leave pending for manual approval
            logger.info(f"MEMBERSHIP_SKIP: {participant_id[:20]} not duplicate, leaving pending for {group_id[:20]}")

async def _kick_blocked_number(group_id: str, participant_id: str):
    """Remove a blocked number from a group with anti-ban delay."""
    delay = random.uniform(15, 30)
    logger.info(f"BLOCKED_KICK: Waiting {delay:.0f}s before removing {participant_id[:20]} from {group_id[:25]}")
    await asyncio.sleep(delay)
    try:
        result = await wpp.remove_participant(group_id, participant_id)
        if result:
            logger.info(f"BLOCKED_KICK: ✅ Removed {participant_id[:20]} from {group_id[:25]}")
        else:
            logger.error(f"BLOCKED_KICK: ❌ Failed to remove {participant_id[:20]} from {group_id[:25]}")
    except Exception as e:
        logger.error(f"BLOCKED_KICK error: {e}")

async def check_exclusive_membership(joined_group: str, participant_ids: list, group_jids: list):
    """Background task: check if new participants are in other groups. Remove from newly joined if duplicate.
    Keeps the member in the group they were in longest (the older one)."""
    cfg = load_settings()
    if not cfg.get("exclusive_groups_enabled", True):
        return
    
    remove_msg = cfg.get("exclusive_remove_message", "")
    other_groups = [g for g in group_jids if g != joined_group]
    if not other_groups:
        return
    
    for participant_id in participant_ids:
        p_phone = participant_id.replace('@c.us','').replace('@s.whatsapp.net','').replace('@lid','').split('@')[0]
        
        found_in = None
        for i, other_group in enumerate(other_groups):
            try:
                # Small delay between reading different groups (crawler safety)
                if i > 0:
                    await asyncio.sleep(random.uniform(2.0, 4.0))
                    
                members = await wpp.get_group_participants(other_group)
                if isinstance(members, list):
                    for m in members:
                        if isinstance(m, dict):
                            mid_raw = m.get('id', '')
                            if isinstance(mid_raw, dict):
                                mid_raw = mid_raw.get('_serialized', mid_raw.get('user', ''))
                            m_phone = str(mid_raw).replace('@c.us','').replace('@s.whatsapp.net','').replace('@lid','').split('@')[0]
                        else:
                            m_phone = str(m).split('@')[0]
                        if m_phone and m_phone == p_phone:
                            found_in = other_group
                            break
                if found_in:
                    break
            except Exception as e:
                logger.error(f"Exclusive check error for group {other_group}: {e}")
        
        if found_in:
            logger.info(f"EXCLUSIVE_REMOVE: {participant_id[:20]} already in {found_in[:20]}, removing from {joined_group[:20]}")
            
            # Significant randomized delay before removal (15 - 30 seconds)
            await asyncio.sleep(random.uniform(15.0, 30.0))
            
            removed = await wpp.remove_participant(joined_group, participant_id)
            if removed:
                logger.info(f"EXCLUSIVE_REMOVED: {participant_id[:20]} removed from {joined_group[:20]}")
            else:
                logger.error(f"EXCLUSIVE_REMOVE_FAILED: {participant_id[:20]} from {joined_group[:20]}")

@app.post("/webhook")
async def receive_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        data = await request.json()
    except:
        return {"status": "error"}

    raw_event = data.get('event', '')
    event = raw_event.lower() if raw_event else ''
    
    log_entry = {
        "time": datetime.now().isoformat(),
        "event": event,
        "raw_event": raw_event,
    }
    
    # --- Detect session disconnect (qrcode = needs re-scan) ---
    if event in ('qrcode', 'desconnectedmobile', 'deletedsession', 'disconnected'):
        log_entry["status"] = "SESSION_DISCONNECTED"
        log_entry["detail"] = f"Session event '{raw_event}' detected - attempting reconnect"
        _log_webhook(log_entry)
        logger.critical(f"SESSION DISCONNECTED: event={raw_event}")
        # Attempt full reconnect in background
        background_tasks.add_task(wpp.full_reconnect)
        return {"status": "reconnecting"}
    
    # --- Handle participant change events (exclusive groups + affiliate tracking) ---
    if event in ('onparticipantschanged', 'on-participants-changed'):
        log_entry["status"] = "participant_event"
        
        msg = data.get('response') or data.get('data') or data
        action = msg.get('action', '') if isinstance(msg, dict) else ''
        
        # Affiliate tracking: confirm joins and update member counts
        if isinstance(msg, dict):
            background_tasks.add_task(handle_participant_change, msg)
        
        # --- TRACK MEMBER EVENTS (join/leave/remove) for churn analytics ---
        if action in ('add', 'join', 'leave', 'remove'):
            group_id = msg.get('groupId', '') or msg.get('chatId', '')
            who = msg.get('who', '') or msg.get('participantId', '')
            
            # Normalize: handle list or single string
            if isinstance(who, list):
                participants = who
            else:
                participants = [who] if who else []
            
            log_entry["action"] = action
            log_entry["group"] = group_id
            log_entry["participants"] = [p[:20] for p in participants]
            
            # Record member events in DB
            try:
                db_ev = SessionLocal()
                from datetime import timezone
                now_utc = datetime.now(timezone.utc)
                now_br = now_utc - timedelta(hours=3)  # BRT
                for p in participants:
                    db_ev.add(MemberEvent(
                        user_id=p,
                        group_id=group_id,
                        event_type=action,
                        event_date=now_br.date(),
                        event_time=now_br
                    ))
                db_ev.commit()
                db_ev.close()
            except Exception as e:
                logger.error(f"Error recording member event: {e}")
            
            # --- BLOCKED NUMBERS: auto-kick with anti-ban delay ---
            if action in ('add', 'join'):
                BLOCKED_NUMBERS = {"5511915254569"}
                for p in participants:
                    p_bare = p.split('@')[0]
                    if p_bare in BLOCKED_NUMBERS:
                        logger.warning(f"BLOCKED_NUMBER: {p_bare} joined {group_id[:25]} — scheduling removal")
                        background_tasks.add_task(_kick_blocked_number, group_id, p)
                        log_entry["blocked_kick"] = p_bare
                
                # Build list of all managed group JIDs from DB
                managed_jids = []
                try:
                    db_exc = SessionLocal()
                    managed_jids = [g.group_jid for g in db_exc.query(WhatsAppGroup).filter_by(is_active=True).all()]
                    db_exc.close()
                except Exception as e:
                    logger.error(f"Error loading managed groups for exclusive check: {e}")
                
                if managed_jids and group_id in managed_jids and participants:
                    background_tasks.add_task(
                        check_exclusive_membership, group_id, participants, managed_jids
                    )
                    log_entry["status"] = "exclusive_check_queued"
        
        _log_webhook(log_entry)
        return {"status": log_entry.get("status", "participant_event")}
    
    # --- Handle membership approval requests (pre-join duplicate check) ---
    if event in ('onparticipantschanged', 'on-participants-changed'):
        msg = data.get('response') or data.get('data') or data
        action = msg.get('action', '') if isinstance(msg, dict) else ''
        
        if action == 'membership_approval_request':
            group_id = msg.get('groupId', '') or msg.get('chatId', '')
            who = msg.get('who', '') or msg.get('participantId', '')
            
            if isinstance(who, list):
                participants = who
            else:
                participants = [who] if who else []
            
            log_entry["status"] = "membership_request"
            log_entry["action"] = action
            log_entry["group"] = group_id
            log_entry["participants"] = [p[:20] for p in participants]
            
            # Load managed groups from DB
            managed_jids = []
            try:
                db_req = SessionLocal()
                managed_jids = [g.group_jid for g in db_req.query(WhatsAppGroup).all()]
                db_req.close()
            except Exception as e:
                logger.error(f"Error loading managed groups for membership request: {e}")
            
            if managed_jids and group_id in managed_jids and participants:
                background_tasks.add_task(
                    handle_membership_request, group_id, participants, managed_jids
                )
                log_entry["status"] = "membership_request_check_queued"
            
            _log_webhook(log_entry)
            return {"status": log_entry.get("status", "membership_request")}
    
    # Accept multiple message event formats
    message_events = ('onmessage', 'on-message', 'onanymessage', 'onmessage.data')
    if event not in message_events:
        # Log ALL non-noisy events so we can see what's arriving
        noisy = ('onpresencechanged', 'onack', 'onpollresponse', 'status-find', 'onreactionmessage', 'onrevokedmessage')
        if event not in noisy:
            log_entry["status"] = "ignored"
            _log_webhook(log_entry)
        return {"status": "ignored"}
    
    # Update watchdog heartbeat
    global _last_message_time
    _last_message_time = datetime.now()
    
    # Extract message data from multiple possible locations
    msg = data.get('response') or data.get('data') or data.get('message') or data.get('msg')
    if not msg:
        if 'body' in data or 'chatId' in data or 'from' in data or 'sender' in data or 'isGroupMsg' in data:
            msg = data
    
    if not msg:
        log_entry["status"] = "no_msg"
        _log_webhook(log_entry)
        return {"status": "no_msg"}
    
    # --- Ignore bot's own messages ---
    from_me = msg.get('fromMe', False)
    if from_me:
        log_entry["status"] = "from_me"
        _log_webhook(log_entry)
        return {"status": "from_me"}
    
    # --- Extract all message fields ---
    sender_obj = msg.get('sender', {}) or {}
    sender_id = sender_obj.get('id', '') or msg.get('from', '')
    sender_serialized = sender_obj.get('_serialized', '')
    sender_user = sender_obj.get('user', '')
    is_group = msg.get('isGroupMsg', False)
    chat_id = msg.get('chatId', '')
    msg_type = msg.get('type', '')
    msg_id = msg.get('id', '')
    body = msg.get('body', '') or ''
    caption = msg.get('caption', '') or ''
    
    # --- Ignore system messages ---
    if msg_type in ('gp2', 'notification_template', 'e2e_notification', 'call_log', 'protocol_message', 'ciphertext', 'revoked'):
        log_entry["status"] = "ignored_system_msg"
        _log_webhook(log_entry)
        return {"status": "ignored_system_msg"}
        
    # --- Ignore system text phrases (if type check fails) ---
    lower_body = body.lower()
    if "deseja entrar no grupo" in lower_body or "saiu do grupo" in lower_body or "entrou no grupo" in lower_body:
        log_entry["status"] = "ignored_system_text"
        _log_webhook(log_entry)
        return {"status": "ignored_system_text"}
    
    # --- TRACK HOURLY ACTIVITY for peak hour analysis ---
    if is_group and chat_id:
        try:
            from datetime import timezone
            now_utc = datetime.now(timezone.utc)
            now_br = now_utc - timedelta(hours=3)
            db_ha = SessionLocal()
            ha = db_ha.query(HourlyActivity).filter_by(
                date=now_br.date(), hour=now_br.hour, group_id=chat_id
            ).first()
            is_media = msg_type in ('image', 'video', 'ptt', 'audio', 'document', 'sticker')
            if ha:
                ha.message_count += 1
                if is_media:
                    ha.media_count += 1
                # Track unique senders (comma-separated)
                existing = set(ha.unique_senders.split(',')) if ha.unique_senders else set()
                existing.discard('')
                existing.add(sender_id)
                ha.unique_senders = ','.join(existing)
            else:
                db_ha.add(HourlyActivity(
                    date=now_br.date(),
                    hour=now_br.hour,
                    group_id=chat_id,
                    message_count=1,
                    media_count=1 if is_media else 0,
                    unique_senders=sender_id
                ))
            db_ha.commit()
            db_ha.close()
        except Exception as e:
            logger.error(f"Error tracking hourly activity: {e}")
    
    # --- Build set of sender identifiers for admin matching ---
    # Handles @c.us, @lid, and plain phone formats
    sender_identifiers = set()
    for sid in [sender_id, sender_serialized, sender_user]:
        if sid:
            sender_identifiers.add(sid)
            clean = sid.split('@')[0]
            if clean:
                sender_identifiers.add(clean)
    # Also try 'from' field
    msg_from = msg.get('from', '')
    if msg_from:
        sender_identifiers.add(msg_from.split('@')[0])
    
    log_entry.update({
        "sender": sender_id[:30],
        "sender_identifiers": list(sender_identifiers)[:6],
        "is_group": is_group,
        "chat_id": chat_id,
        "type": msg_type,
        "msg_id": str(msg_id)[:50],
        "body_len": len(body),
    })
    
    super_admins = set(get_super_admins())
    is_admin = bool(sender_identifiers & super_admins)
    
    logger.info(f"MSG: sender={sender_id[:25]}, group={is_group}, chat={chat_id[:25]}, type={msg_type}, admin={is_admin}")
    
    # 0. Command: !regras (Available to everyone)
    if body.strip().lower() in ("!regras", "/regras"):
        regras_msg = (
            "📌 *RESUMO DAS REGRAS DO GRUPO*\n\n"
            "🚫 Links são proibidos.\n"
            "⏱️ Após qualquer envio no grupo, aguarde 5 minutos antes de enviar novamente.\n"
            "🖼️ Permitido apenas 1 anúncio por vez, com 1 imagem/video.\n"
            "A imagem/video e a descrição devem estar juntas na mesma mensagem.\n\n"
            "Contamos com a colaboração de todos para manter o grupo organizado, limpo e funcional."
        )
        await wpp.send_message(chat_id, regras_msg)
        log_entry["status"] = "command_regras"
        _log_webhook(log_entry)
        return {"status": "command_regras"}
    
    # 1. Private Messages
    if not is_group:
        if is_admin:
            text = body
            if text.lower().startswith('!anuncio '):
                announcement = text[9:]
                groups = await wpp.get_all_groups()
                count = 0
                for g in groups:
                    gid = g.get('id', {}).get('_serialized') if isinstance(g.get('id'), dict) else g.get('id')
                    if gid:
                        try:
                            await wpp.send_message(gid, announcement, skip_typing=True)
                            count += 1
                        except Exception as e:
                            logger.error(f"Announcement error for {gid}: {e}")
                await wpp.send_message(msg_from, f"Anunciando para {count} grupos.", skip_typing=True)
        log_entry["status"] = "private_msg"
        _log_webhook(log_entry)
        return {"status": "private_msg_handled"}

    # 2. Group Logic
    group_id = chat_id
    if not sender_id or not group_id:
        log_entry["status"] = "missing_data"
        _log_webhook(log_entry)
        return {"status": "missing_data"}
    
    # 3. Admin bypass (super admins + group admins)
    if is_admin:
        log_entry["status"] = "super_admin_bypass"
        _log_webhook(log_entry)
        return {"status": "super_admin_bypass"}
    
    # Check if sender is a group admin
    group_admin_ids = await get_group_admin_ids(group_id)
    is_group_admin = bool(sender_identifiers & group_admin_ids)
    
    # Debug: log admin check details
    log_entry["is_group_admin"] = is_group_admin
    log_entry["group_admin_ids_sample"] = list(group_admin_ids)[:5]
    
    if is_group_admin:
        # --- ADMIN REPLY BROADCAST: only in the configured test group ---
        test_group = cfg.get("broadcast_test_group_id", "")
        if test_group and group_id != test_group:
            # Not the test group — just bypass moderation silently
            log_entry["status"] = "group_admin_bypass"
            _log_webhook(log_entry)
            return {"status": "group_admin_bypass"}
        
        # --- ADMIN REPLY BROADCAST: forward quoted message to other groups ---
        quoted_msg = msg.get('quotedMsg') or msg.get('quotedMsgObj') or {}
        
        if quoted_msg and isinstance(quoted_msg, dict):
            q_type = quoted_msg.get('type', 'chat')
            q_body = quoted_msg.get('body', '') or ''
            q_caption = quoted_msg.get('caption', '') or ''
            q_mimetype = quoted_msg.get('mimetype', '') or ''
            q_filename = quoted_msg.get('filename', '') or ''
            q_stanza_id = msg.get('quotedStanzaID', '') or msg.get('quotedMsgId', '')
            
            # quotedParticipant = the EXACT JID of who sent the original message (top-level msg field)
            q_participant = msg.get('quotedParticipant', '') or ''
            if isinstance(q_participant, dict):
                q_participant = q_participant.get('_serialized', '') or q_participant.get('id', '')
            
            # Try to get the _serialized ID directly from quotedMsg.id
            q_id_obj = quoted_msg.get('id', {})
            q_id_serialized = ''
            if isinstance(q_id_obj, dict):
                q_id_serialized = q_id_obj.get('_serialized', '')
            elif isinstance(q_id_obj, str):
                q_id_serialized = q_id_obj
            
            is_media = q_type in ('image', 'video', 'audio', 'ptt', 'document', 'sticker')
            has_content = bool((is_media and q_body) or (not is_media and q_body))
            
            # Log all quoted* fields from the top-level msg for diagnostics
            quoted_fields = {k: str(v)[:60] for k, v in msg.items() if 'quot' in k.lower()}
            logger.info(f"ADMIN_BROADCAST: type={q_type}, stanzaId={q_stanza_id[:30] if q_stanza_id else 'none'}, participant={q_participant[:30] if q_participant else 'none'}, id_serialized={q_id_serialized[:60] if q_id_serialized else 'none'}")
            logger.info(f"ADMIN_BROADCAST: all_quoted_fields={quoted_fields}")
            
            if has_content or q_stanza_id or q_id_serialized:
                async def _broadcast_forward(origin_chat: str, msg_type: str, body: str, caption: str,
                                              mimetype: str, filename: str, is_media_msg: bool,
                                              stanza_id: str, participant: str, id_serialized: str):
                    try:
                        await ensure_token()
                        success = False
                        real_msg_id = None

                        # === STEP 1: Find the exact _serialized ID from chat history ===
                        # This is the only reliable way to get a valid WID for forward-messages
                        if stanza_id:
                            try:
                                messages = await wpp.get_messages(origin_chat, count=100)
                                logger.info(f"ADMIN_BROADCAST: searching {len(messages) if messages else 0} msgs for stanza_id={stanza_id[:20]}")
                                for m in (messages or []):
                                    m_id = m.get('id', {})
                                    if isinstance(m_id, dict):
                                        ser = m_id.get('_serialized', '')
                                        # The stanza ID is the last part of _serialized (after the 2nd underscore)
                                        parts = ser.split('_', 2)
                                        if len(parts) == 3 and parts[2] == stanza_id:
                                            real_msg_id = ser
                                            logger.info(f"ADMIN_BROADCAST: ✅ found real_msg_id={ser[:60]}")
                                            break
                            except Exception as e:
                                logger.warning(f"ADMIN_BROADCAST: get_messages error: {e}")

                        # Fallback candidates if history lookup failed
                        if not real_msg_id:
                            if id_serialized:
                                real_msg_id = id_serialized
                                logger.info(f"ADMIN_BROADCAST: using id_serialized as fallback: {real_msg_id[:60]}")
                            elif stanza_id and participant:
                                real_msg_id = f"false_{participant}_{stanza_id}"
                                logger.info(f"ADMIN_BROADCAST: using constructed WID as fallback: {real_msg_id[:60]}")

                        # === STEP 2: Get target groups (all monitored groups except origin) ===
                        try:
                            db = SessionLocal()
                            all_groups = db.query(WhatsAppGroup).filter(WhatsAppGroup.is_active == True).all()
                            db.close()
                            target_groups = [g.group_id for g in all_groups if g.group_id != origin_chat]
                        except Exception as e:
                            logger.warning(f"ADMIN_BROADCAST: DB error getting groups: {e}")
                            target_groups = []

                        # TEST MODE: if no other groups, send back to origin for testing
                        if not target_groups:
                            target_groups = [origin_chat]
                            logger.info("ADMIN_BROADCAST: TEST MODE — no other groups, sending to origin")

                        logger.info(f"ADMIN_BROADCAST: real_msg_id={str(real_msg_id)[:60]}, targets={len(target_groups)} groups")

                        # === STEP 3: Forward to each target group ===
                        fwd_url = f"{wpp.base_url}/api/{wpp.session}/forward-messages"
                        for target_group in target_groups:
                            group_success = False
                            if real_msg_id:
                                try:
                                    payload = {
                                        "phone": target_group,
                                        "messageId": real_msg_id,
                                        "isGroup": "@g.us" in target_group,
                                    }
                                    async with httpx.AsyncClient(timeout=30.0) as client:
                                        resp = await client.post(fwd_url, json=payload, headers=wpp.headers)
                                    logger.info(f"ADMIN_BROADCAST: forward to {target_group[:25]}: {resp.status_code} - {resp.text[:200]}")
                                    if resp.status_code in (200, 201):
                                        r = resp.json() if resp.text else {}
                                        if r.get("status") != "error":
                                            group_success = True
                                            success = True
                                except Exception as e:
                                    logger.warning(f"ADMIN_BROADCAST: forward error for {target_group[:25]}: {e}")

                            # === FALLBACK: re-send media/text with isForwarded:true ===
                            if not group_success:
                                logger.info(f"ADMIN_BROADCAST: fallback resend to {target_group[:25]}")
                                try:
                                    if is_media_msg and body:
                                        media_b64 = body
                                        if media_b64.startswith("data:"):
                                            media_b64 = media_b64.split(",", 1)[-1] if "," in media_b64 else media_b64
                                        mime = mimetype or 'application/octet-stream'
                                        base64_data = f"data:{mime};base64,{media_b64}"
                                        is_image = msg_type == 'image' or 'image' in mime
                                        ep = "send-image" if is_image else "send-file"
                                        url = f"{wpp.base_url}/api/{wpp.session}/{ep}"
                                        ext = mime.split('/')[-1].split(';')[0]
                                        if ext == 'jpeg': ext = 'jpg'
                                        payload = {
                                            "phone": target_group,
                                            "isGroup": "@g.us" in target_group,
                                            "base64": base64_data,
                                            "filename": filename or f"media.{ext}",
                                            "isForwarded": True,
                                        }
                                        if caption:
                                            payload["caption"] = caption
                                        async with httpx.AsyncClient(timeout=60.0) as client:
                                            resp = await client.post(url, json=payload, headers=wpp.headers)
                                        logger.info(f"ADMIN_BROADCAST: {ep} fallback resp: {resp.status_code}")
                                        if resp.status_code in (200, 201):
                                            success = True
                                    elif body:
                                        await wpp.send_message(target_group, body, skip_typing=True)
                                        success = True
                                except Exception as e:
                                    logger.error(f"ADMIN_BROADCAST: fallback error for {target_group[:25]}: {e}")

                        if success:
                            logger.info(f"ADMIN_BROADCAST: ✅ broadcast done to {len(target_groups)} groups")
                            await wpp.send_message(origin_chat, f"✅ Encaminhado para {len(target_groups)} grupo(s).", skip_typing=True)
                        else:
                            logger.warning("ADMIN_BROADCAST: ❌ all targets failed")
                            await wpp.send_message(origin_chat, "❌ Falha ao encaminhar.", skip_typing=True)
                    except Exception as e:
                        logger.error(f"ADMIN_BROADCAST: error: {e}")
                        await wpp.send_message(origin_chat, f"❌ Erro: {e}", skip_typing=True)

                background_tasks.add_task(_broadcast_forward, chat_id, q_type, q_body, q_caption,
                                           q_mimetype, q_filename, is_media, q_stanza_id, q_participant, q_id_serialized)
                log_entry["status"] = "admin_broadcast"
                _log_webhook(log_entry)
                return {"status": "admin_broadcast_started"}

        
        # ABSOLUTE RULES: stickers and status shares blocked even for group admins
        if msg_type == 'sticker' and cfg.get("sticker_filter_enabled", True):
            logger.info(f"STICKER_BLOCKED (admin): {sender_id[:20]} in {group_id[:20]}")
            await enforce_action("delete", group_id, msg_id, "", sender_id=sender_id)
            return {"status": "sticker_blocked"}

        log_entry["status"] = "group_admin_bypass"
        _log_webhook(log_entry)
        return {"status": "group_admin_bypass"}
    
    # 4. Check if group is monitored
    monitored = cfg.get("monitored_groups", [])
    if monitored and group_id not in monitored:
        log_entry["status"] = "group_not_monitored"
        _log_webhook(log_entry)
        return {"status": "group_not_monitored"}
    
    # 5. STICKER FILTER — Delete ALL stickers (non-admins)
    if msg_type == 'sticker' and cfg.get("sticker_filter_enabled", True):
        logger.info(f"STICKER_BLOCKED: {sender_id[:20]} in {group_id[:20]}")
        enforce_result = await enforce_action("delete", group_id, msg_id, "", sender_id=sender_id)
        log_entry["status"] = "sticker_blocked"
        log_entry["enforce_result"] = enforce_result
        _log_webhook(log_entry)
        return {"status": "sticker_blocked"}
    
    # 5b. STATUS SHARE FILTER — Block shared WhatsApp status updates
    _all_text = f"{body} {caption}".strip()
    _is_status_share = (
        msg_type == 'status'
        or 'Status de' in _all_text
        or ('@ Este grupo foi mencionado' in _all_text)
        or (msg.get('isForwarded') and 'status' in str(msg.get('caption', '')).lower())
    )
    if _is_status_share:
        logger.info(f"STATUS_SHARE_BLOCKED: {sender_id[:20]} in {group_id[:20]}, type={msg_type}")
        enforce_result = await enforce_action("delete", group_id, msg_id, "", sender_id=sender_id)
        log_entry["status"] = "status_share_blocked"
        log_entry["enforce_result"] = enforce_result
        _log_webhook(log_entry)
        return {"status": "status_share_blocked"}
    
    # 6. Link Filter — check caption only (body may be base64 for media messages)
    _link_text = caption if msg_type in ('image', 'video', 'media', 'album') else body
    if not check_link_whitelist(_link_text, caption):
        logger.info(f"Link violation from {sender_id[:20]} in {group_id[:20]}")
        link_action = cfg.get("link_action", "delete")
        link_warn = cfg.get("link_warn_message", "⚠️ Links externos não são permitidos neste grupo.")
        enforce_result = await enforce_action(link_action, group_id, msg_id, link_warn, sender_id=sender_id)
        log_entry["status"] = "link_violation"
        log_entry["enforce_result"] = enforce_result
        _log_webhook(log_entry)
        background_tasks.add_task(_save_moderation_log, group_id, sender_id, msg_type, msg_id, _link_text[:300], "link_violation", "")
        return {"status": "link_violation"}

    # 6b. PHONE NUMBER FILTER — ALL messages must contain phone number
    phone_required = cfg.get("phone_required_groups", [])
    if group_id in phone_required and msg_type in ('image', 'video', 'media', 'album', 'chat'):
        # RULE: Multiple images (album) are NEVER allowed — always delete
        is_album = (
            msg_type == 'album'
            or msg.get('isAlbum', False)
            or (msg.get('mediaCount', 1) or 1) > 1
        )
        if is_album:
            logger.info(f"ALBUM_BLOCKED: {sender_id[:20]} in {group_id[:20]}, mediaCount={msg.get('mediaCount')}")
            enforce_result = await enforce_action("delete", group_id, msg_id, "", sender_id=sender_id)
            log_entry["status"] = "album_blocked"
            log_entry["enforce_result"] = enforce_result
            _log_webhook(log_entry)
            return {"status": "album_blocked"}

        # For media: use caption. For text (chat): use body directly
        if msg_type == 'chat':
            text_to_check = body or ''
        else:
            text_to_check = (
                msg.get('caption')
                or msg.get('text')
                or caption
                or ''
            )
            # Ignore base64 data accidentally in caption
            if text_to_check and len(text_to_check) > 500 and '/' in text_to_check:
                text_to_check = ''

        if not has_phone_number(text_to_check):
            logger.info(f"PHONE_MISSING: {sender_id[:20]} in {group_id[:20]}, text={str(text_to_check)[:60]}")
            enforce_result = await enforce_action("delete", group_id, msg_id, "", sender_id=sender_id)
            log_entry["status"] = "phone_missing"
            log_entry["enforce_result"] = enforce_result
            _log_webhook(log_entry)
            return {"status": "phone_missing"}
        else:
            # Message is valid — react with 🔁
            approved_emoji = cfg.get("phone_approved_emoji", "🔁")
            background_tasks.add_task(wpp.send_reaction, msg_id, group_id, approved_emoji)
            logger.info(f"PHONE_OK: reacted {approved_emoji} on {str(msg_id)[:30]}")

    # 6c. Flood Control (locked per user to prevent race with grouped images)
    has_media = msg_type in ('image', 'video', 'document', 'sticker')
    # IMPORTANT: For media messages, body contains base64 data (huge), use caption only
    if has_media:
        text_len = len(caption)
    else:
        text_len = len(body)
    
    flood_lock = _get_flood_lock(sender_id, group_id)
    async with flood_lock:
        db = SessionLocal()
        try:
            result = process_flood_control(db, sender_id, group_id, has_media, text_len)
            log_entry["flood_result"] = result

            if not result["allowed"]:
                reason = result.get("reason", "flood")
                logger.info(f"BLOCKED: ({reason}) {sender_id[:20]} in {group_id[:20]}")
                # Silent delete — no educational message
                enforce_result = await enforce_action("delete", group_id, msg_id, "", sender_id=sender_id)
                log_entry["status"] = f"violation_{reason}"
                log_entry["enforce_result"] = enforce_result
                _log_webhook(log_entry)
                background_tasks.add_task(_save_moderation_log, group_id, sender_id, msg_type,
                                          msg_id, caption or body[:300], f"violation_{reason}", str(result))
                return {"status": f"violation_{reason}"}
        finally:
            db.close()

    log_entry["status"] = "ok"
    _log_webhook(log_entry)
    background_tasks.add_task(_save_moderation_log, group_id, sender_id, msg_type,
                              msg_id, caption or body[:300], "ok", "")
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=8000, reload=True)
