import os
import logging
import asyncio
import re
import httpx
import uvicorn
import random
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import List, Optional
from fastapi import FastAPI, BackgroundTasks, Request
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.orm import Session
from database import SessionLocal, UserStats, init_db

# --- CONFIGURATION ---
WPP_SERVER_URL = os.getenv("WPP_SERVER_URL", "http://wppserver:21465")
SESSION_NAME = os.getenv("SESSION_NAME", "idsr_bot")
SUPER_ADMINS = ["5511983426767", "5511981771974"]
WHITELIST_DOMAINS = ["idsr.com.br", "tvzapao.com.br"]

# Function to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- WPPCONNECT CLIENT ---
class WPPConnectClient:
    def __init__(self, base_url: str, session: str):
        self.base_url = base_url
        self.session = session
        self.headers = {"Content-Type": "application/json"}

    async def start_session(self):
        url = f"{self.base_url}/api/{self.session}/start-session"
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.post(url, json={"webhook": f"http://bot:8000/webhook"})
                logger.info(f"Start Session: {resp.status_code} - {resp.text}")
            except Exception as e:
                logger.error(f"Error starting session: {e}")

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

    async def send_message(self, phone: str, message: str):
        # Simulate human behavior
        # 1. Start typing
        await self.start_typing(phone)
        
        # 2. Random delay based on message length (simulating typing speed)
        # 0.1s per char, max 5s, min 1s
        typing_time = min(5.0, max(1.0, len(message) * 0.05)) 
        delay = typing_time + random.uniform(0.5, 2.0)
        await asyncio.sleep(delay)
        
        # 3. Stop typing? (usually send message stops it, but good practice)
        # await self.stop_typing(phone) 

        url = f"{self.base_url}/api/{self.session}/send-message"
        payload = {"phone": phone, "message": message}
        async with httpx.AsyncClient() as client:
            try:
                await client.post(url, json=payload, headers=self.headers)
            except Exception as e:
                logger.error(f"Error sending message: {e}")

    async def delete_message(self, phone: str, message_id: str):
        url = f"{self.base_url}/api/{self.session}/delete-message"
        payload = {"phone": phone, "messageId": message_id}
        async with httpx.AsyncClient() as client:
            try:
                await client.post(url, json=payload, headers=self.headers)
                logger.info(f"Deleted message {message_id} from {phone}")
            except Exception as e:
                logger.error(f"Error deleting message: {e}")

    async def get_all_groups(self):
        url = f"{self.base_url}/api/{self.session}/all-groups"
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    return resp.json().get('response', [])
                return []
            except Exception as e:
                logger.error(f"Error getting groups: {e}")
                return []

    async def set_group_description(self, group_id: str, description: str):
        # Using description change as a proxy for settings if needed, or specific endpoint
        # WPPConnect has specific endpoints for settings.
        # For open/close, usually it's restricting who can send messages.
        # Endpoint: /api/:session/groups/settings/announce
        # announce: true (only admins), false (all participants)
        url = f"{self.base_url}/api/{self.session}/groups/settings/attribute"
        # Correct endpoint for 'announcement' (only admins can send)
        # Check WPPConnect docs. It varies. Assuming logic:
        # We will try to update the 'announcement' property.
        pass

    async def open_group(self, group_id: str):
        # Allow everyone to send specific endpoint
        # /api/:session/groups/chat-admins-only
        # value: false (everyone)
        # Check specific implementation.
        # We'll assume a generic helper or try specific commonly used path
        # Let's use a robust approach:
        # POST /api/:session/groups/settings/property
        # { "id": group_id, "property": "announcement", "value": false }
        pass

wpp = WPPConnectClient(WPP_SERVER_URL, SESSION_NAME)

# --- BUSINESS LOGIC ---

# Regex for links
LINK_REGEX = re.compile(r"(https?://|www\.|chat\.whatsapp\.com)")

def check_link_whitelist(text: str) -> bool:
    if not text:
        return True
    
    found_links = LINK_REGEX.findall(text)
    if not found_links:
        return True
    
    # If links found, check if they are ALL in whitelist
    # This logic is strict: if ANY link is bad, block.
    # We need to extract the actual domain.
    # Simple check: does the text contain allowed domains?
    # If text has "google.com" and "idsr.com.br", it should fail.
    
    # Improved check:
    # If a link is detected, we verify if it matches *only* whitelisted domains.
    # It's a bit complex with regex.
    # Simplification: If text matches LINK_REGEX, verify if the match is whitelisted.
    
    # Let's iterate over matches?
    # No, simple approach:
    # 1. Check if text contains any banned link pattern.
    # 2. If it does, check if it's purely a whitelisted one.
    
    # Implementation:
    # If "idsr.com.br" in text, we might be good. But if "idsr.com.br/evil" and "evil.com"?
    # Let's just block if any "http/www" is present UNLESS the string also contains a whitelist domain?
    # No, that allows "idsr.com.br evil.com".
    
    # Strict implementation: allow message IF (no links found) OR (all found urls are whitelisted).
    # Since extracting URLs reliably is hard, we can try: 
    # If valid link pattern exists AND it's NOT in whitelist -> Delete.
    
    if LINK_REGEX.search(text):
        # A link exists.
        # Check if the text contains ONLY whitelisted domains? No.
        # Check if the detected link is NOT in whitelist.
        # We just iterate whitelist. If the text has a link, is it one of the whitelist?
        # A user could type "idsr.com.br check this out", we want to allow.
        # A user could type "porn.com", we want to block.
        
        # We will iterate through whitelist. If the text contains a whitelisted string, we consider it SAFE?
        # Risk: "idsr.com.br porn.com".
        # We need to be careful.
        # Let's flag as BAD if it matches regex, and then UNFLAG if it matches Whitelist?
        # No.
        # Flag as BAD if (LINK detected) AND (Text does NOT contain whitelisted domain).
        # Better: We remove whitelisted domains from text, then check for links again.
        
        cleaned_text = text
        for domain in WHITELIST_DOMAINS:
            cleaned_text = cleaned_text.replace(domain, "") # Remove allowed domains
            
        if LINK_REGEX.search(cleaned_text):
            return False # Still has links
            
    return True

def process_flood_control(db: Session, user_id: str, group_id: str, has_media: bool, text_len: int) -> bool:
    # Returns True if ALLOWED, False if SHOULD DELETE
    
    today = date.today()
    stats = db.query(UserStats).filter_by(user_id=user_id, group_id=group_id).first()
    
    if not stats:
        stats = UserStats(user_id=user_id, group_id=group_id, last_active_date=today)
        db.add(stats)
    
    # Update logic
    if stats.last_active_date != today:
        stats.last_active_date = today
        stats.message_count = 0
        stats.photo_count = 0
    
    # Check limits
    if stats.message_count >= 2:
        db.commit()
        return False # > 2 messages
    
    if has_media:
        if stats.photo_count >= 3:
            db.commit()
            return False # > 3 photos
    
    if text_len > 300:
        db.commit()
        return False # > 300 chars
        
    # Increment
    stats.message_count += 1
    if has_media:
        stats.photo_count += 1
        
    db.commit()
    return True

# --- SCHEDULER TASKS ---
# We need to access shared state or DB to know which groups to open/close?
# User didn't specify dynamic group list. We can fetch all groups bot knows.

async def job_open_groups():
    logger.info("Scheduler: Opening groups...")
    groups = await wpp.get_all_groups()
    for g in groups:
        # Check if bot is admin? Usually API handles error if not.
        gid = g.get('id', {}).get('_serialized') if isinstance(g.get('id'), dict) else g.get('id')
        if gid:
            # Send command to set group settings 'announcement' = false (all can send)
            # WPPConnect specific call:
            async with httpx.AsyncClient() as client:
                 await client.post(f"{WPP_SERVER_URL}/api/{SESSION_NAME}/groups/settings/property", 
                                   json={"id": gid, "property": "announcement", "value": False})

async def job_close_groups():
    logger.info("Scheduler: Closing groups...")
    groups = await wpp.get_all_groups()
    for g in groups:
        gid = g.get('id', {}).get('_serialized') if isinstance(g.get('id'), dict) else g.get('id')
        if gid:
            # Send command to set group settings 'announcement' = true (admins only)
            async with httpx.AsyncClient() as client:
                 await client.post(f"{WPP_SERVER_URL}/api/{SESSION_NAME}/groups/settings/property", 
                                   json={"id": gid, "property": "announcement", "value": True})

# --- FASTAPI APP ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Bot starting up...")
    init_db()
    
    # Scheduler
    scheduler = AsyncIOScheduler()
    scheduler.add_job(job_open_groups, 'cron', hour=10, minute=0)
    scheduler.add_job(job_close_groups, 'cron', hour=20, minute=0)
    scheduler.start()
    
    # Attempt to start WPP session
    asyncio.create_task(wpp.start_session())
    
    yield
    # Shutdown
    logger.info("Bot shutting down...")

app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def receive_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        data = await request.json()
    except:
        return {"status": "error"}

    # Process event
    # WPPConnect event 'onMessage'
    event = data.get('event')
    if event != 'onMessage':
        return {"status": "ignored"}
    
    msg = data.get('response', {})
    if not msg:
        return {"status": "no_msg"}
    
    # 1. Check if Group Message
    if not msg.get('isGroupMsg'):
        # Handle Private Message (Admin Announcement)
        sender = msg.get('sender', {}).get('id', '').split('@')[0] # 551199999999
        # sender usually comes as '55119999@c.us'
        
        # Normalize sender
        sender_clean = sender.replace('@c.us', '')
        
        if sender_clean in SUPER_ADMINS:
            text = msg.get('body', '')  # or 'content'
            if text.lower().startswith('!anuncio '):
                announcement = text[9:] # strip '!anuncio '
                # Broadcast
                groups = await wpp.get_all_groups()
                count = 0
                for g in groups:
                    gid = g.get('id', {}).get('_serialized') if isinstance(g.get('id'), dict) else g.get('id')
                    if gid:
                        background_tasks.add_task(wpp.send_message, gid, announcement)
                        count += 1
                await wpp.send_message(msg.get('from'), f"Anunciando para {count} grupos.")
        return {"status": "private_msg_handled"}

    # 2. Group Logic
    sender = msg.get('sender', {}).get('id')
    group_id = msg.get('chatId') # or from
    if not sender or not group_id:
        return {"status": "missing_data"}
        
    # Check if sender is admin? Admins bypass rules.
    # WPPConnect sends 'sender.isMyContact', 'sender.formattedName'.
    # Does it send 'isAdmin'? Usually in 'groupInfo' or needs fetch.
    # For now, we apply rules to everyone except SUPER_ADMINS (hardcoded)
    # Refinement: Admins of the group should ideally bypass.
    # Assuming 'sender' object has 'isGroupAdmins' property? Unreliable in webhook sometimes.
    # We will stick to SUPER_ADMINS bypass for now to be safe, or explicit check.
    
    sender_clean = sender.split('@')[0]
    if sender_clean in SUPER_ADMINS:
        return {"status": "admin_bypass"}
    
    # Link Whitelist
    text = msg.get('body', '')
    if not check_link_whitelist(text):
        logger.info(f"Deleting message from {sender} in {group_id}: Link violation")
        background_tasks.add_task(wpp.delete_message, group_id, msg.get('id'))
        return {"status": "deleted_link"}

    # Flood Control
    msg_type = msg.get('type')
    has_media = msg_type in ['image', 'video', 'document']
    text_len = len(text) if text else 0
    
    db = SessionLocal()
    try:
        allowed = process_flood_control(db, sender, group_id, has_media, text_len)
        if not allowed:
            logger.info(f"Deleting message from {sender} in {group_id}: Flood/Content violation")
            background_tasks.add_task(wpp.delete_message, group_id, msg.get('id'))
            return {"status": "deleted_flood"}
    finally:
        db.close()

    return {"status": "ok"}

# Run with: uvicorn bot:app --host 0.0.0.0 --port 8000
if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=8000, reload=True)

