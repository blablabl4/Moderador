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
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import traceback
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.orm import Session
from database import SessionLocal, UserStats, WhatsAppGroup, Affiliate, JoinTracking, init_db
import settings as cfg

# --- CONFIGURATION ---
WPP_SERVER_URL = "http://server-cli.railway.internal:8080" # FORCE URL
SESSION_NAME = os.getenv("SESSION_NAME", "idsr_bot2")
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
        url = f"{self.base_url}/api/{self.session}/group-participants/{group_id}"
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(url, headers=self.headers)
                if resp.status_code == 200:
                    data = resp.json()
                    return data.get('response', data) if isinstance(data, dict) else data
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
        """Remove a participant from a group."""
        url = f"{self.base_url}/api/{self.session}/remove-participant-group"
        payload = {"groupId": group_id, "participantId": [participant_id]}
        logger.info(f"REMOVE_PARTICIPANT: group={group_id}, participant={participant_id}")
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.post(url, json=payload, headers=self.headers)
                logger.info(f"REMOVE_PARTICIPANT response: {resp.status_code} - {resp.text[:200]}")
                return resp.status_code == 200
            except Exception as e:
                logger.error(f"Error removing participant: {e}")
                return False

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
        """Delete session data completely (clears browser profile, forces new QR)."""
        # Try multiple approaches to clear the session
        async with httpx.AsyncClient(timeout=15) as client:
            # Method 1: DELETE the session
            try:
                url = f"{self.base_url}/api/{self.session}"
                resp = await client.delete(url, headers=self.headers)
                logger.info(f"Delete Session: {resp.status_code} - {resp.text[:200]}")
            except Exception as e:
                logger.warning(f"Delete session failed: {e}")
            # Method 2: Clear session data
            try:
                url = f"{self.base_url}/api/{self.session}/clear-session-data"
                resp = await client.post(url, headers=self.headers)
                logger.info(f"Clear Session Data: {resp.status_code} - {resp.text[:200]}")
            except Exception as e:
                logger.warning(f"Clear session data failed: {e}")

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

# --- FASTAPI APP & LIFESPAN ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Bot starting up...")
    logger.info(f"--- CONFIG ---")
    logger.info(f"WPP_SERVER_URL: {WPP_SERVER_URL}")
    logger.info(f"SESSION_NAME: {SESSION_NAME}")
    logger.info(f"--------------")
    
    init_db()
    
    scheduler = AsyncIOScheduler()
    scheduler.add_job(job_open_groups, 'cron', hour=10, minute=0)
    scheduler.add_job(job_close_groups, 'cron', hour=20, minute=0)
    scheduler.add_job(job_watchdog, 'interval', minutes=5, id='watchdog')
    scheduler.add_job(job_sync_invite_links, 'interval', minutes=10, id='sync_invite_links')
    scheduler.start()
    logger.info("Scheduler started with watchdog (every 5 min) + invite link sync (every 10 min)")
    
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

# --- BUSINESS LOGIC HELPER ---
LINK_REGEX = re.compile(r"(https?://|www\.|chat\.whatsapp\.com)")

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
    """Check flood limits using a time-based window. Returns dict with 'allowed' bool.
    
    Each message counts individually to encourage users to post complete ads in one message.
    """
    if not cfg.get("moderation_enabled", True):
        return {"allowed": True, "debug": "moderation_disabled"}
    try:
        now = datetime.now()
        stats = db.query(UserStats).filter_by(user_id=user_id, group_id=group_id).first()
        
        is_new = False
        if not stats:
            is_new = True
            stats = UserStats(
                user_id=user_id,
                group_id=group_id,
                last_active_date=date.today(),
                window_start=now,
                message_count=0,
                photo_count=0
            )
            db.add(stats)
            db.flush()
        
        # Check if window has expired -> reset counters
        # Support both new (minutes) and legacy (hours) settings
        window_minutes = cfg.get("flood_window_minutes", None)
        if window_minutes is None:
            # Fallback: convert legacy hours setting to minutes
            window_minutes = cfg.get("flood_window_hours", 2) * 60
        window_minutes = max(5, int(window_minutes))  # Minimum 5 minutes
        
        window_start = stats.window_start or now
        elapsed_minutes = (now - window_start).total_seconds() / 60
        
        if elapsed_minutes >= window_minutes:
            stats.window_start = now
            stats.message_count = 0
            stats.photo_count = 0
            stats.last_active_date = date.today()
            logger.info(f"FLOOD_RESET: user={user_id[:20]}, elapsed={elapsed_minutes:.0f}min >= {window_minutes}min")
        
        max_msgs = cfg.get("flood_max_messages", 1)
        max_photos = cfg.get("flood_max_photos", 3)
        max_text = cfg.get("flood_max_text_length", 300)
        
        remaining_mins = max(0, int(window_minutes - elapsed_minutes))
        remaining_hours = remaining_mins // 60
        remaining_mins_only = remaining_mins % 60
        
        # Check message limit
        if stats.message_count >= max_msgs:
            db.commit()
            return {"allowed": False, "reason": "messages", "count": stats.message_count, "max": max_msgs, "reset_in_min": remaining_mins, "remaining_hours": remaining_hours, "remaining_mins_only": remaining_mins_only, "window_minutes": window_minutes}
        
        # Check photo limit
        if has_media and stats.photo_count >= max_photos:
            db.commit()
            return {"allowed": False, "reason": "photos", "count": stats.photo_count, "max": max_photos, "reset_in_min": remaining_mins, "remaining_hours": remaining_hours, "remaining_mins_only": remaining_mins_only}
        
        # Check text length (does NOT count against limit — user can retry with shorter text)
        if max_text > 0 and text_len > max_text:
            db.commit()
            return {"allowed": False, "reason": "text_length", "text_len": text_len, "max_text": max_text}
        
        # Allowed -> increment counters
        stats.message_count += 1
        logger.info(f"FLOOD_COUNT: user={user_id[:20]}, msg #{stats.message_count}/{max_msgs}")
        
        if has_media:
            stats.photo_count += 1
        
        # Set window_start on first message if not set
        if is_new or not stats.window_start:
            stats.window_start = now
            
        db.commit()
        return {"allowed": True, "count": stats.message_count, "max": max_msgs, "is_new": is_new}
    except Exception as e:
        logger.error(f"Error in flood control: {e}")
        db.rollback()
        return {"allowed": True, "debug": f"error: {str(e)}"}

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
    return {"wpp_connected": connected, "session_status": session_status, "bot_active": True}

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
    """Logout from WhatsApp (clears QR, requires new scan)."""
    await ensure_token()
    url = f"{wpp.base_url}/api/{wpp.session}/logout-session"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, headers=wpp.headers)
            logger.info(f"Logout session: {resp.status_code} - {resp.text}")
            return {"status": "success", "message": "Deslogado do WhatsApp. Escaneie o QR novamente."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# --- GROUPS & SCAN ENDPOINTS ---
@app.get("/api/debug/raw-groups")
async def api_debug_raw_groups(username: str = Depends(get_current_username)):
    """Debug: return raw API response from WPPConnect for groups."""
    await ensure_token()
    results = {}
    async with httpx.AsyncClient(timeout=30) as client:
        # Try all-groups
        try:
            resp = await client.get(f"{wpp.base_url}/api/{wpp.session}/all-groups", headers=wpp.headers)
            raw = resp.json()
            results["all_groups_status"] = resp.status_code
            # Get first group sample (truncated for readability)
            if isinstance(raw, dict) and raw.get("response"):
                groups_data = raw["response"]
            elif isinstance(raw, list):
                groups_data = raw
            else:
                groups_data = [raw]
            
            if groups_data and len(groups_data) > 0:
                # Return first group with ALL fields to inspect structure
                first = groups_data[0]
                results["first_group_keys"] = list(first.keys()) if isinstance(first, dict) else str(type(first))
                results["first_group_sample"] = first
                results["total_groups"] = len(groups_data)
                
                # If there's a group ID, try to get its members
                gid = first.get('id', {}).get('_serialized') if isinstance(first.get('id'), dict) else first.get('id', '')
                if gid:
                    results["sample_group_id"] = gid
                    # Try group-members endpoint
                    try:
                        r2 = await client.get(f"{wpp.base_url}/api/{wpp.session}/group-members/{gid}", headers=wpp.headers)
                        results["group_members_status"] = r2.status_code
                        r2_data = r2.json()
                        results["group_members_keys"] = list(r2_data.keys()) if isinstance(r2_data, dict) else str(type(r2_data))
                        if isinstance(r2_data, dict) and r2_data.get("response"):
                            members = r2_data["response"]
                            results["members_count"] = len(members) if isinstance(members, list) else "not_list"
                            if isinstance(members, list) and len(members) > 0:
                                results["first_member_keys"] = list(members[0].keys()) if isinstance(members[0], dict) else str(type(members[0]))
                                results["first_member_sample"] = members[0]
                        else:
                            results["group_members_raw"] = str(r2_data)[:500]
                    except Exception as e:
                        results["group_members_error"] = str(e)
                    
                    # Also try group-participants 
                    try:
                        r3 = await client.get(f"{wpp.base_url}/api/{wpp.session}/group-participants/{gid}", headers=wpp.headers)
                        results["group_participants_status"] = r3.status_code
                        results["group_participants_raw"] = str(r3.json())[:500]
                    except Exception as e:
                        results["group_participants_error"] = str(e)
            else:
                results["groups_data"] = str(groups_data)[:500]
        except Exception as e:
            results["error"] = str(e)
    
    return results

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
        {"item": "Super Admins", "status": "ok", "detail": f"{len(s.get('super_admins', []))} configurados"},
        {"item": f"Total de grupos", "status": "ok" if total_groups > 0 else "error", "detail": f"{total_groups} grupos encontrados"},
        {"item": f"Bot é admin em", "status": "ok" if groups_as_admin > 0 else "warning", "detail": f"{groups_as_admin} de {total_groups} grupos"},
        {"item": f"Total de membros", "status": "ok", "detail": f"{total_members} membros no total"},
    ]
    
    return {"checklist": checklist, "system_checks": system_checks, "summary": summary}

# --- SESSION MANAGEMENT ENDPOINTS ---
@app.post("/api/session/{action}")
async def api_session_action(action: str, username: str = Depends(get_current_username)):
    """Manage WPP session: start, close, logout."""
    await ensure_token()
    if action == "start":
        await wpp.start_session()
        return {"message": "Sessão iniciada! Aguarde o QR code."}
    elif action == "close":
        await wpp.close_session()
        return {"message": "Sessão fechada."}
    elif action == "logout":
        # Step 1: Close browser
        await wpp.close_session()
        await asyncio.sleep(1)
        # Step 2: Logout (clear auth)
        await wpp.logout_session()
        await asyncio.sleep(1)
        # Step 3: Delete session data (clear browser profile)
        await wpp.delete_session()
        await asyncio.sleep(2)
        # Step 4: Start fresh session (should show QR code)
        await wpp.start_session()
        return {"message": "Sessão limpa! Clique 'Atualizar QR Code' em 5 segundos."}
    else:
        return JSONResponse({"message": f"Ação desconhecida: {action}"}, status_code=400)

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

@app.get("/api/groups/list")
async def api_groups_list(username: str = Depends(get_current_username)):
    """List all groups with id and name for configuration."""
    try:
        groups = await wpp.get_all_groups()
        result = []
        for g in groups:
            gid = g.get('id', {}).get('_serialized') if isinstance(g.get('id'), dict) else g.get('id')
            name = g.get('name', g.get('subject', 'Sem nome'))
            if gid:
                result.append({"id": gid, "name": name})
        return {"groups": result}
    except Exception as e:
        logger.error(f"Error listing groups: {e}")
        return {"groups": [], "error": str(e)}

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
    """Check duplicate members between the two managed groups."""
    group1_jid = "120363404846725137@g.us"  # DEZAPEGÃO #01
    group2_jid = "120363408119561849@g.us"  # DEZAPEGÃO #02
    
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            # Get members of group 1
            url1 = f"{wpp.base_url}/api/{wpp.session}/group-members/{group1_jid}"
            resp1 = await client.get(url1, headers=wpp.headers)
            members1_raw = resp1.json() if resp1.status_code == 200 else []
            if isinstance(members1_raw, dict):
                members1_raw = members1_raw.get('response', members1_raw.get('data', []))
            
            # Get members of group 2
            url2 = f"{wpp.base_url}/api/{wpp.session}/group-members/{group2_jid}"
            resp2 = await client.get(url2, headers=wpp.headers)
            members2_raw = resp2.json() if resp2.status_code == 200 else []
            if isinstance(members2_raw, dict):
                members2_raw = members2_raw.get('response', members2_raw.get('data', []))
            
            # Extract phone IDs
            def extract_ids(members):
                ids = set()
                for m in members:
                    if isinstance(m, dict):
                        mid = m.get('id', m.get('_serialized', ''))
                        if isinstance(mid, dict):
                            mid = mid.get('_serialized', mid.get('user', ''))
                        ids.add(str(mid).split('@')[0])
                    elif isinstance(m, str):
                        ids.add(m.split('@')[0])
                return ids
            
            ids1 = extract_ids(members1_raw)
            ids2 = extract_ids(members2_raw)
            duplicates = ids1 & ids2
            
            return {
                "group1_name": "DEZAPEGÃO #01",
                "group1_members": len(ids1),
                "group2_name": "DEZAPEGÃO #02", 
                "group2_members": len(ids2),
                "duplicates_count": len(duplicates),
                "unique_total": len(ids1 | ids2),
                "duplicate_percentage": f"{len(duplicates) / max(len(ids1 | ids2), 1) * 100:.1f}%"
            }
        except Exception as e:
            logger.error(f"Error checking duplicates: {e}")
            return {"error": str(e)}

@app.get("/api/groups/managed")
async def api_managed_groups_list(username: str = Depends(get_current_username)):
    """List all managed groups with real-time member counts from WhatsApp."""
    # Sync real member counts from bot
    await ensure_token()
    all_wpp_groups = await wpp.get_all_groups()
    wpp_sizes = {}
    for g in all_wpp_groups:
        info = parse_group(g)
        wpp_sizes[info["id"]] = info["size"]
    
    db = SessionLocal()
    try:
        groups = db.query(WhatsAppGroup).order_by(WhatsAppGroup.display_order).all()
        for g in groups:
            real = wpp_sizes.get(g.group_jid, g.current_members)
            g.current_members = real
            g.is_active = real < g.max_members
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
        } for g in groups]}
    finally:
        db.close()

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

async def check_exclusive_membership(joined_group: str, participant_ids: list, monitored_groups: list):
    """Background task: check if new participants are in other monitored groups. Remove if so."""
    other_groups = [g for g in monitored_groups if g != joined_group]
    if not other_groups:
        return
    
    for participant_id in participant_ids:
        # Build identifier set for matching
        p_identifiers = {participant_id, participant_id.split('@')[0]}
        
        found_in = None
        for other_group in other_groups:
            try:
                members = await wpp.get_group_participants(other_group)
                if isinstance(members, list):
                    for m in members:
                        mid = m.get('id', '') if isinstance(m, dict) else str(m)
                        muser = m.get('user', '') if isinstance(m, dict) else ''
                        m_ids = {mid, mid.split('@')[0]}
                        if muser:
                            m_ids.add(muser)
                        if p_identifiers & m_ids:
                            found_in = other_group
                            break
                if found_in:
                    break
            except Exception as e:
                logger.error(f"Exclusive check error for group {other_group}: {e}")
        
        if found_in:
            logger.info(f"EXCLUSIVE: {participant_id[:20]} already in {found_in[:20]}, removing from {joined_group[:20]}")
            # Remove from the group they just joined (no DM to reduce ban risk)
            await asyncio.sleep(random.uniform(2.0, 5.0))  # Anti-ban delay
            removed = await wpp.remove_participant(joined_group, participant_id)

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
        
        if cfg.get("exclusive_groups_enabled", False):
            if action in ('add', 'join'):
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
                
                monitored = cfg.get("monitored_groups", [])
                if monitored and group_id in monitored and participants:
                    background_tasks.add_task(
                        check_exclusive_membership, group_id, participants, monitored
                    )
                    log_entry["status"] = "exclusive_check_queued"
        
        _log_webhook(log_entry)
        return {"status": log_entry.get("status", "participant_event")}
    
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
        max_msgs = cfg.get("flood_max_messages", 2)
        win_hours = cfg.get("flood_window_hours", 2)
        max_photos = cfg.get("flood_max_photos", 3)
        max_char = cfg.get("flood_max_text_length", 300)
        
        regras_msg = (
            f"⚠️ *REGRAS DO GRUPO*\n\n"
            f"1️⃣ *Anúncios:* Máx *{max_msgs}* a cada *{win_hours}h*\n"
            f"   _(Envie foto + texto na mesma mensagem!)_\n\n"
            f"2️⃣ *Fotos:* Máx *{max_photos}* por anúncio\n\n"
            f"3️⃣ *Texto:* Máx *{max_char}* caracteres\n\n"
            f"4️⃣ *Links:* Proibidos 🚫\n\n"
            f"🤖 _Mensagens fora do padrão são apagadas._"
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
        log_entry["status"] = "group_admin_bypass"
        _log_webhook(log_entry)
        return {"status": "group_admin_bypass"}
    
    # 4. Check if group is monitored
    monitored = cfg.get("monitored_groups", [])
    if monitored and group_id not in monitored:
        log_entry["status"] = "group_not_monitored"
        _log_webhook(log_entry)
        return {"status": "group_not_monitored"}
    
    # 5. STICKER FILTER — Delete ALL stickers automatically
    if msg_type == 'sticker':
        logger.info(f"STICKER_BLOCKED: {sender_id[:20]} in {group_id[:20]}")
        warn_msg = "🚨 Figurinhas não são permitidas neste grupo! 🚨"
        enforce_result = await enforce_action("delete_warn", group_id, msg_id, warn_msg, sender_id=sender_id)
        log_entry["status"] = "sticker_blocked"
        log_entry["enforce_result"] = enforce_result
        _log_webhook(log_entry)
        return {"status": "sticker_blocked"}
    
    # 5b. STATUS SHARE FILTER — Block shared WhatsApp status updates
    if msg_type == 'status' or (msg_type in ('image', 'video', 'chat') and 'Status de' in body):
        logger.info(f"STATUS_SHARE_BLOCKED: {sender_id[:20]} in {group_id[:20]}, type={msg_type}")
        warn_msg = "🚨 Compartilhamento de status não é permitido neste grupo! 🚨"
        enforce_result = await enforce_action("delete_warn", group_id, msg_id, warn_msg, sender_id=sender_id)
        log_entry["status"] = "status_share_blocked"
        log_entry["enforce_result"] = enforce_result
        _log_webhook(log_entry)
        return {"status": "status_share_blocked"}
    
    # 6. Link Filter (checks body AND caption)
    if not check_link_whitelist(body, caption):
        logger.info(f"Link violation from {sender_id[:20]} in {group_id[:20]}")
        link_action = cfg.get("link_action", "delete")
        link_warn = cfg.get("link_warn_message", "⚠️ Links externos não são permitidos neste grupo.")
        enforce_result = await enforce_action(link_action, group_id, msg_id, link_warn, sender_id=sender_id)
        log_entry["status"] = "link_violation"
        log_entry["enforce_result"] = enforce_result
        _log_webhook(log_entry)
        return {"status": "link_violation"}

    # 6. Flood Control (locked per user to prevent race with grouped images)
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
                
                flood_action = cfg.get("flood_action", "delete")
                
                # Mensagem padrão para todas as infrações
                warn_msg = "🤖 Regras na descrição."
                
                # Se for violação de texto longo, usar delete_warn para garantir que o aviso chegue
                if reason == "text_length":
                    flood_action = "delete_warn"
                
                enforce_result = await enforce_action(flood_action, group_id, msg_id, warn_msg, sender_id=sender_id)
                log_entry["status"] = f"violation_{reason}"
                log_entry["enforce_result"] = enforce_result
                _log_webhook(log_entry)
                return {"status": f"violation_{reason}"}
        finally:
            db.close()

    log_entry["status"] = "ok"
    _log_webhook(log_entry)
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=8000, reload=True)
