"""
Operator Bot — Multi-Bot WhatsApp Ad Distributor (V2)
=====================================================
Standalone FastAPI app that sits in the SAME advertiser groups as the
moderator bot.  It watches for the moderator's approved-ad reaction (🔁),
then uses `forward-messages` to redistribute the ad to its assigned
destination groups — preserving original image/video quality.

Architecture:
    Advertiser Group → Moderator reacts 🔁 → Operator detects reaction
    → Operator uses forward-messages to its target groups

Usage:
    python operator_bot.py
    # or: uvicorn operator_bot:app --host 0.0.0.0 --port 8001

Configuration:
    Edit operator_config.json (created on first run with safe defaults).
"""

import os
import json
import logging
import asyncio
import base64
import httpx
import uvicorn
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [OPERATOR] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("operator")

# In-memory log ring buffer for live dashboard
from collections import deque

class MemoryLogHandler(logging.Handler):
    """Stores last N log entries in memory for API access."""
    def __init__(self, max_entries=200):
        super().__init__()
        self.buffer = deque(maxlen=max_entries)

    def emit(self, record):
        try:
            entry = {
                "ts": self.format(record),
                "level": record.levelname,
                "msg": record.getMessage(),
                "time": datetime.now().isoformat()
            }
            self.buffer.append(entry)
        except Exception:
            pass

    def get_logs(self, since=None):
        if since:
            return [e for e in self.buffer if e["time"] > since]
        return list(self.buffer)

_mem_handler = MemoryLogHandler(max_entries=200)
_mem_handler.setFormatter(logging.Formatter("%(asctime)s [OPERATOR] %(levelname)s %(message)s", datefmt="%H:%M:%S"))
logger.addHandler(_mem_handler)
# Mute noisy HTTP request logs from httpx
logging.getLogger("httpx").setLevel(logging.WARNING)

# ── Configuration ────────────────────────────────────────────────────────────
# Use /data volume if available (Railway persistent volume), otherwise local
if os.path.exists("/data") and os.access("/data", os.W_OK):
    CONFIG_FILE = "/data/operator_config.json"
else:
    CONFIG_FILE = os.environ.get("OPERATOR_CONFIG", "./operator_config.json")

DEFAULTS = {
    # WPPConnect Server — dedicated operator server on Railway
    "wpp_server_url": os.environ.get("WPP_SERVER_URL", "http://server-cli-operator.railway.internal:8080"),
    "session_name": os.environ.get("OPERATOR_SESSION", "operator_1"),
    "wpp_secret_key": os.environ.get("WPP_SECRET_KEY", "THISISMYSECURETOKEN"),

    # Which advertiser groups this operator monitors (same ones the moderator is in)
    "monitored_groups": ["120363424437184928@g.us"],

    # Moderator bot identification (phone numbers without @c.us)
    "moderator_bot_ids": ["5511983426767", "5511939482383"],

    # The emoji the moderator uses to approve ads
    "approved_emoji": "🔁",

    # Target groups this operator forwards approved ads to
    "target_groups": ["120363424437184928@g.us"],

    # Rate control — control volume by limiting target groups, not by delays
    "forward_delay_seconds": 0,           # 0 = no delay (testing mode)
    "max_forwards_per_hour": 0,           # 0 = unlimited (testing mode)

    # Server — Railway sets PORT automatically
    "port": int(os.environ.get("PORT", 8001)),
}

_config = None


def load_config() -> dict:
    """Load operator config, merging with defaults."""
    global _config
    saved = {}
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
        except Exception as e:
            logger.error(f"Error loading config: {e}")
    _config = {**DEFAULTS, **saved}

    if not os.path.exists(CONFIG_FILE):
        save_config(_config)

    return _config


def save_config(new_cfg: dict):
    """Persist config to JSON."""
    global _config
    _config = new_cfg
    try:
        os.makedirs(os.path.dirname(CONFIG_FILE) if os.path.dirname(CONFIG_FILE) else ".", exist_ok=True)
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(new_cfg, f, ensure_ascii=False, indent=2)
        logger.info(f"Config saved to {CONFIG_FILE}")
    except Exception as e:
        logger.error(f"Error saving config: {e}")


def get_cfg() -> dict:
    global _config
    if _config is None:
        return load_config()
    return _config


# ── WPPConnect Client (EXACT CLONE of moderator's WPPConnectClient) ──────────
class OperatorWPPClient:

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
        """Start the WPP session. Sends webhook URL in the body so WPP server
        knows where to deliver events. Uses internal Railway URL for reliability."""
        if not self.token:
            logger.warning("start_session called without token, generating...")
            await self.generate_token()

        # Build webhook URL: prefer internal Railway networking (same project)
        webhook_url = self._get_webhook_url()
        url = f"{self.base_url}/api/{self.session}/start-session"
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.post(url, json={
                    "webhook": webhook_url,
                    "waitQrCode": False
                }, headers=self.headers)
                logger.info(f"Start Session: {resp.status_code} - {resp.text[:300]}")
                logger.info(f"📨 WEBHOOK URL: {webhook_url}")
            except Exception as e:
                logger.error(f"Error starting session: {e}")

    async def subscribe_webhook(self):
        """Check session status after starting."""
        webhook_url = self._get_webhook_url()
        logger.info(f"📨 WEBHOOK CONFIG: events should arrive at → {webhook_url}")
        async with httpx.AsyncClient(timeout=15) as client:
            # Check session status
            try:
                url = f"{self.base_url}/api/{self.session}/status-session"
                resp = await client.get(url, headers=self.headers)
                logger.info(f"Session status: {resp.status_code} - {resp.text[:200]}")
            except Exception as e:
                logger.warning(f"Status check failed: {e}")

    @staticmethod
    def _get_webhook_url():
        """Build the webhook URL. Prefers internal Railway domain for reliability."""
        # Allow explicit override
        explicit = os.environ.get("OPERATOR_WEBHOOK_URL")
        if explicit:
            return explicit
        # Use Railway internal networking (same project, no public DNS needed)
        private_domain = os.environ.get("RAILWAY_PRIVATE_DOMAIN")
        port = os.environ.get("PORT", "8001")
        if private_domain:
            return f"http://{private_domain}:{port}/webhook"
        # Fallback to public URL
        return "https://victorious-transformation-moderador.up.railway.app/webhook"

    async def get_all_groups(self):
        """Get all groups this session is part of."""
        url = f"{self.base_url}/api/{self.session}/all-groups"
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(url, headers=self.headers)
                if resp.status_code == 200:
                    data = resp.json()
                    raw = data.get('response', data) if isinstance(data, dict) else data
                    # Normalize to [{id, name, participants}]
                    groups = []
                    if isinstance(raw, list):
                        for g in raw:
                            gid = g.get("id", {})
                            if isinstance(gid, dict):
                                gid = gid.get("_serialized", "")
                            groups.append({
                                "id": gid,
                                "name": g.get("name") or g.get("subject") or gid,
                                "participants": g.get("groupMetadata", {}).get("participants", []) if isinstance(g.get("groupMetadata"), dict) else [],
                            })
                    logger.info(f"GET ALL GROUPS: found {len(groups)} groups")
                    return groups
                logger.error(f"get_all_groups: {resp.status_code} - {resp.text[:200]}")
                return []
            except Exception as e:
                logger.error(f"Error getting groups: {e}")
                return []

    async def get_group_members_phones(self, group_id: str) -> dict:
        """Fetch group participants and build a LID→phone mapping.
        Returns dict like {'211854211215482': '5511983426767', ...}"""
        url = f"{self.base_url}/api/{self.session}/group-members/{group_id}"
        async with httpx.AsyncClient(timeout=15) as client:
            try:
                resp = await client.get(url, headers=self.headers)
                if resp.status_code == 200:
                    members = resp.json()
                    lid_to_phone = {}
                    if isinstance(members, list):
                        for m in members:
                            mid = m.get("id", {})
                            if isinstance(mid, dict):
                                serialized = mid.get("_serialized", "")
                                user = mid.get("user", "")
                            else:
                                serialized = str(mid)
                                user = str(mid).split("@")[0]
                            # If this is a LID, try to find the phone via verifiedName or other fields
                            if "@lid" in serialized:
                                lid_num = serialized.split("@")[0]
                                # Check if there's a linked phone in the contact
                                phone = m.get("verifiedName", "") or m.get("pushname", "")
                                lid_to_phone[lid_num] = user  # user field sometimes has phone
                            elif "@c.us" in serialized:
                                # Regular phone number — map it to itself
                                phone_num = serialized.split("@")[0]
                                lid_to_phone[phone_num] = phone_num
                    logger.info(f"LID_MAP [{group_id[:20]}]: {len(lid_to_phone)} members mapped")
                    return lid_to_phone
                else:
                    logger.warning(f"get_group_members_phones: {resp.status_code}")
                    return {}
            except Exception as e:
                logger.warning(f"get_group_members_phones error: {e}")
                return {}

    async def close_session(self):
        """Close/kill existing session to release the browser lock."""
        url = f"{self.base_url}/api/{self.session}/close-session"
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(url, headers=self.headers)
                logger.info(f"Close session: {resp.status_code} - {resp.text[:100]}")
        except Exception as e:
            logger.warning(f"Close session error (non-fatal): {e}")

    async def delete_session(self):
        """Delete session data completely. Uses correct WPPConnect API URLs."""
        async with httpx.AsyncClient(timeout=20.0) as client:
            # 1. Logout (clears auth, forces new QR)
            try:
                resp = await client.post(
                    f"{self.base_url}/api/{self.session}/logout-session",
                    headers=self.headers
                )
                logger.info(f"Logout session: {resp.status_code}")
                await asyncio.sleep(2)
            except Exception as e:
                logger.warning(f"Logout failed: {e}")
            # 2. Close session (stops the browser)
            try:
                resp = await client.post(
                    f"{self.base_url}/api/{self.session}/close-session",
                    headers=self.headers
                )
                logger.info(f"Close session: {resp.status_code}")
                await asyncio.sleep(2)
            except Exception as e:
                logger.warning(f"Close failed: {e}")
            # 3. Clear session data (requires secretkey in URL!)
            try:
                resp = await client.post(
                    f"{self.base_url}/api/{self.session}/{self.secret_key}/clear-session-data"
                )
                logger.info(f"Clear Session Data: {resp.status_code} - {resp.text[:100]}")
            except Exception as e:
                logger.warning(f"Clear session data failed: {e}")

    async def logout_session(self):
        """Logout to force a fresh QR code on next start."""
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(
                    f"{self.base_url}/api/{self.session}/logout-session",
                    headers=self.headers
                )
                logger.info(f"Logout session: {resp.status_code} - {resp.text[:100]}")
                return resp.status_code in (200, 201)
        except Exception as e:
            logger.error(f"Logout error: {e}")
            return False



    async def check_session_status(self) -> dict:
        """Check if session is connected. Uses two methods for reliability."""
        async with httpx.AsyncClient(timeout=5.0) as client:
            # Method 1: check-connection-session (most reliable when it works)
            try:
                resp = await client.get(
                    f"{self.base_url}/api/{self.session}/check-connection-session",
                    headers=self.headers
                )
                if resp.status_code == 200:
                    data = resp.json()
                    connected = data.get("response", False)
                    if connected:
                        return {"connected": True, "status": "CONNECTED"}
            except Exception:
                pass  # Fallback below

            # Method 2: status-session (fallback)
            try:
                resp = await client.get(
                    f"{self.base_url}/api/{self.session}/status-session",
                    headers=self.headers
                )
                if resp.status_code == 200:
                    data = resp.json()
                    status = data.get("status", "CLOSED")
                    connected = status in ("CONNECTED", "QRCODE", "INITIALIZING")
                    return {"connected": status == "CONNECTED", "status": status}
            except Exception as e:
                logger.warning(f"Status check failed: {e}")

        return {"connected": False, "status": "CLOSED"}

    async def get_messages(self, chat_id: str, count: int = 50) -> list:
        """Load recent messages from a chat/group."""
        url = f"{self.base_url}/api/{self.session}/get-messages/{chat_id}"
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(url, params={"count": count}, headers=self.headers)
                if resp.status_code == 200:
                    data = resp.json()
                    return data.get("response", []) if isinstance(data, dict) else data
        except Exception as e:
            logger.error(f"get_messages error: {e}")
        return []

    async def get_message_by_id(self, chat_id: str, msg_id: str) -> dict | None:
        """Try to get a specific message by ID from chat history."""
        url = f"{self.base_url}/api/{self.session}/get-message-by-id/{msg_id}"
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(url, headers=self.headers)
                if resp.status_code == 200:
                    data = resp.json()
                    return data.get("response", data) if isinstance(data, dict) else None
        except Exception:
            pass
        # Fallback: search in recent messages
        messages = await self.get_messages(chat_id, count=50)
        for m in (messages or []):
            m_id = m.get("id", {})
            if isinstance(m_id, dict):
                if m_id.get("_serialized", "") == msg_id:
                    return m
            elif str(m_id) == msg_id:
                return m
        return None

    async def forward_message(self, to_chat_id: str, message_id: str) -> bool:
        """Forward a message using native forward-messages (preserves quality).
        Two approaches: string messageId, then array [messageId] as fallback."""
        url = f"{self.base_url}/api/{self.session}/forward-messages"
        is_group = "@g.us" in to_chat_id

        # Approach 1: messageId as string
        payload = {
            "phone": to_chat_id,
            "messageId": message_id,
            "isGroup": is_group,
        }
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                logger.info(f"FORWARD: POST {url} | phone={to_chat_id[:30]} | isGroup={is_group} | msgId={message_id[:50]}")
                resp = await client.post(url, json=payload, headers=self.headers)
                logger.info(f"FORWARD [string]: {resp.status_code} - {resp.text[:300]}")
                if resp.status_code in (200, 201):
                    body = resp.json() if resp.text else {}
                    if body.get("status") != "error":
                        return True

                # Approach 2: messageId as array (some WPP versions need this)
                payload2 = {
                    "phone": to_chat_id,
                    "messageId": [message_id],
                    "isGroup": is_group,
                }
                resp2 = await client.post(url, json=payload2, headers=self.headers)
                logger.info(f"FORWARD [array]: {resp2.status_code} - {resp2.text[:300]}")
                if resp2.status_code in (200, 201):
                    body = resp2.json() if resp2.text else {}
                    if body.get("status") != "error":
                        return True

                logger.warning(f"⚠️ forward failed both approaches")
        except Exception as e:
            logger.error(f"forward error: {e}")
        return False

    async def get_messages(self, group_id: str, count: int = 20) -> list:
        """Fetch recent messages from a group via WPP API."""
        url = f"{self.base_url}/api/{self.session}/get-messages/{group_id}"
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                resp = await client.get(url, headers=self.headers)
                if resp.status_code == 200:
                    data = resp.json()
                    msgs = data.get("response", data) if isinstance(data, dict) else data
                    if isinstance(msgs, list):
                        return msgs[-count:]  # Last N messages
                    return []
                return []
        except Exception as e:
            logger.error(f"get_messages error: {e}")
            return []

    async def get_reactions(self, message_id: str) -> list:
        """Get reactions for a specific message."""
        url = f"{self.base_url}/api/{self.session}/reactions/{message_id}"
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(url, headers=self.headers)
                if resp.status_code == 200:
                    data = resp.json()
                    return data.get("response", data) if isinstance(data, dict) else (data if isinstance(data, list) else [])
                return []
        except Exception as e:
            logger.error(f"get_reactions error: {e}")
            return []

    async def full_reconnect(self):
        logger.info("Full reconnect triggered...")
        await self.generate_token()
        await self.start_session()


# ── Forwarding Engine ────────────────────────────────────────────────────────
class ForwardEngine:
    """Processes approved ads and forwards them to target groups.
    No delays by default (testing mode). Volume controlled by group count."""

    def __init__(self):
        self._forward_log: list[datetime] = []
        self._stats = {
            "total_forwarded": 0,
            "total_failed": 0,
            "total_ads_detected": 0,
            "total_skipped_not_media": 0,
            "last_forward_time": None,
            "last_ad_detected_time": None,
        }
        # Dedup: avoid forwarding the same message twice
        self._processed_ids: set[str] = set()
        self._MAX_PROCESSED_CACHE = 500

    @property
    def stats(self) -> dict:
        return {
            **self._stats,
            "forwards_this_hour": self._count_recent_forwards(),
        }

    def _count_recent_forwards(self) -> int:
        cutoff = datetime.now() - timedelta(hours=1)
        self._forward_log = [t for t in self._forward_log if t > cutoff]
        return len(self._forward_log)

    def _can_forward(self) -> bool:
        max_per_hour = get_cfg().get("max_forwards_per_hour", 0)
        if max_per_hour <= 0:
            return True  # Unlimited
        return self._count_recent_forwards() < max_per_hour

    async def process_approved_ad(self, wpp: OperatorWPPClient, msg_id: str, chat_id: str):
        """Forward an approved ad to all target groups.
        Only forwards image/video messages with caption (description)."""

        # Dedup check
        if msg_id in self._processed_ids:
            logger.info(f"ENGINE: skip duplicate msg_id={msg_id[:40]}")
            return
        self._processed_ids.add(msg_id)
        if len(self._processed_ids) > self._MAX_PROCESSED_CACHE:
            # Evict oldest entries (set doesn't track order, just clear half)
            self._processed_ids = set(list(self._processed_ids)[self._MAX_PROCESSED_CACHE // 2:])

        self._stats["total_ads_detected"] += 1
        self._stats["last_ad_detected_time"] = datetime.now().isoformat()

        # Fetch message details for logging (but don't filter — moderator already approved)
        msg_data = await wpp.get_message_by_id(chat_id, msg_id)
        if msg_data:
            msg_type = msg_data.get("type", "")
            caption = msg_data.get("caption", "") or msg_data.get("body", "") or ""
            logger.info(f"ENGINE: ✅ moderator-approved — type={msg_type}, caption={caption[:50]}")
        else:
            logger.warning(f"ENGINE: could not fetch msg details, forwarding anyway (trust moderator)")

        # Get target groups
        config = get_cfg()
        target_groups = config.get("target_groups", [])
        if not target_groups:
            logger.warning("ENGINE: no target_groups configured — skipping")
            return

        delay = config.get("forward_delay_seconds", 0)

        logger.info(f"ENGINE: forwarding msg_id={msg_id[:40]} → {len(target_groups)} groups (delay={delay}s)")

        success_count = 0
        fail_count = 0

        for i, group_jid in enumerate(target_groups):
            # Rate limit check (if enabled)
            if not self._can_forward():
                logger.warning(f"ENGINE: hourly limit reached, stopping at group {i+1}/{len(target_groups)}")
                break

            ok = await wpp.forward_message(group_jid, msg_id)
            if ok:
                success_count += 1
                self._forward_log.append(datetime.now())
                self._stats["total_forwarded"] += 1
                logger.info(f"ENGINE: ✅ [{i+1}/{len(target_groups)}] → {group_jid[:25]}")
            else:
                fail_count += 1
                self._stats["total_failed"] += 1
                logger.warning(f"ENGINE: ❌ [{i+1}/{len(target_groups)}] → {group_jid[:25]}")

            # Delay between forwards (0 = no delay for testing)
            if delay > 0 and i < len(target_groups) - 1:
                await asyncio.sleep(delay)

        self._stats["last_forward_time"] = datetime.now().isoformat()
        logger.info(f"ENGINE: done — ✅ {success_count} / ❌ {fail_count} of {len(target_groups)} groups")


# ── Multi-Session Management ─────────────────────────────────────────────────
class OperatorSession:
    """A complete operator: WPP client + forwarding engine + QR cache."""
    def __init__(self, name: str, wpp_client: OperatorWPPClient):
        self.name = name
        self.wpp = wpp_client
        self.engine = ForwardEngine()
        self.qr_cache: dict = {"qr": None, "updated_at": None}
        self._polling_task: asyncio.Task | None = None

    def start_polling(self):
        """Start the active polling loop for this session."""
        if self._polling_task and not self._polling_task.done():
            return  # Already running
        self._polling_task = asyncio.create_task(self._polling_loop())
        logger.info(f"🔄 POLLING: started for session '{self.name}'")

    def stop_polling(self):
        """Stop the polling loop."""
        if self._polling_task and not self._polling_task.done():
            self._polling_task.cancel()
            logger.info(f"🔄 POLLING: stopped for session '{self.name}'")

    async def _polling_loop(self):
        """Active polling loop: check monitored groups for emoji reactions from moderators."""
        POLL_INTERVAL = 60  # seconds (safety fallback — webhooks are primary)
        # Track which messages we've already checked/forwarded
        checked_ids: set = set()
        MAX_CHECKED = 1000

        logger.info(f"🔄 POLLING [{self.name}]: loop starting (every {POLL_INTERVAL}s)")

        while True:
            try:
                await asyncio.sleep(POLL_INTERVAL)

                config = get_cfg()
                monitored_groups = config.get("monitored_groups", [])
                target_groups = config.get("target_groups", [])
                approved_emoji = config.get("approved_emoji", "🔁")
                moderator_ids = config.get("moderator_bot_ids", [])

                if not monitored_groups or not target_groups:
                    continue  # Nothing configured

                # Check connection
                status = await self.wpp.check_session_status()
                if not status.get("connected"):
                    continue  # Not connected, skip

                for group_id in monitored_groups:
                    try:
                        messages = await self.wpp.get_messages(group_id, count=30)
                        if not messages:
                            continue

                        for msg in messages:
                            msg_id = msg.get("id")
                            if isinstance(msg_id, dict):
                                msg_id = msg_id.get("_serialized", str(msg_id))
                            msg_id = str(msg_id) if msg_id else None
                            if not msg_id or msg_id in checked_ids:
                                continue

                            # Only check media messages (image/video)
                            msg_type = msg.get("type", "")
                            if msg_type not in ("image", "video"):
                                checked_ids.add(msg_id)
                                continue

                            # Check reactions on this message
                            reactions = await self.wpp.get_reactions(msg_id)
                            if not reactions:
                                continue  # No reactions yet, don't mark as checked

                            # Look for the approved emoji from a moderator
                            found_approval = False
                            for reaction_group in reactions:
                                # reactions can be [{emoji, users: [{id}]}] or [{id, emoji}]
                                if isinstance(reaction_group, dict):
                                    r_emoji = reaction_group.get("reactionText") or reaction_group.get("emoji") or reaction_group.get("msgKey", {}).get("id", "")
                                    # Format 1: aggregated {id, reactionText, read, ...}
                                    if r_emoji == approved_emoji:
                                        # Check if sender is a moderator
                                        sender = reaction_group.get("senderUserJid") or reaction_group.get("sender") or ""
                                        if isinstance(sender, dict):
                                            sender = sender.get("_serialized", sender.get("user", ""))
                                        sender_num = str(sender).split("@")[0]
                                        if sender_num in moderator_ids:
                                            found_approval = True
                                            break
                                    # Format 2: {aggregateEmoji, senders: [{...}]}
                                    agg_emoji = reaction_group.get("aggregateEmoji")
                                    if agg_emoji == approved_emoji:
                                        senders = reaction_group.get("senders", [])
                                        for s in senders:
                                            s_id = s.get("id") or s.get("_serialized") or ""
                                            if isinstance(s_id, dict):
                                                s_id = s_id.get("_serialized", s_id.get("user", ""))
                                            if str(s_id).split("@")[0] in moderator_ids:
                                                found_approval = True
                                                break
                                        if found_approval:
                                            break

                            checked_ids.add(msg_id)

                            if found_approval:
                                caption = msg.get("caption", "") or msg.get("body", "") or ""
                                logger.info(f"🔄 POLLING [{self.name}]: ✅ APPROVED msg_id={msg_id[:40]} type={msg_type} caption={caption[:50]}")
                                await self.engine.process_approved_ad(self.wpp, msg_id, group_id)

                    except Exception as e:
                        logger.error(f"🔄 POLLING [{self.name}]: error processing group {group_id[:30]}: {e}")

                # Evict old checked IDs to prevent memory leak
                if len(checked_ids) > MAX_CHECKED:
                    checked_ids = set(list(checked_ids)[MAX_CHECKED // 2:])

            except asyncio.CancelledError:
                logger.info(f"🔄 POLLING [{self.name}]: loop cancelled")
                break
            except Exception as e:
                logger.error(f"🔄 POLLING [{self.name}]: loop error: {e}")
                await asyncio.sleep(30)  # Back off on error


class SessionManager:
    """Manages multiple operator sessions simultaneously."""
    def __init__(self):
        self.sessions: dict[str, OperatorSession] = {}

    def add(self, name: str, wpp_url: str, secret_key: str) -> OperatorSession:
        """Create and register a new operator session."""
        client = OperatorWPPClient(wpp_url, name, secret_key)
        session = OperatorSession(name, client)
        self.sessions[name] = session
        logger.info(f"SessionManager: added session '{name}'")
        return session

    def remove(self, name: str):
        """Remove a session from the manager."""
        if name in self.sessions:
            del self.sessions[name]
            logger.info(f"SessionManager: removed session '{name}'")

    def get(self, name: str) -> OperatorSession | None:
        return self.sessions.get(name)

    def get_first(self) -> OperatorSession | None:
        """Get first registered session (backwards compat)."""
        if self.sessions:
            return next(iter(self.sessions.values()))
        return None

    def all(self) -> list[OperatorSession]:
        return list(self.sessions.values())

    def find_by_session(self, session_name: str) -> OperatorSession | None:
        """Find session by WPP session name (may differ from key after pivot)."""
        for s in self.sessions.values():
            if s.wpp.session == session_name:
                return s
        return None

    @property
    def aggregated_stats(self) -> dict:
        """Aggregate stats across all sessions."""
        totals = {
            "total_forwarded": 0,
            "total_failed": 0,
            "total_ads_detected": 0,
            "total_skipped_not_media": 0,
            "last_forward_time": None,
            "last_ad_detected_time": None,
            "forwards_this_hour": 0,
        }
        for s in self.sessions.values():
            st = s.engine.stats
            totals["total_forwarded"] += st.get("total_forwarded", 0)
            totals["total_failed"] += st.get("total_failed", 0)
            totals["total_ads_detected"] += st.get("total_ads_detected", 0)
            totals["total_skipped_not_media"] += st.get("total_skipped_not_media", 0)
            totals["forwards_this_hour"] += st.get("forwards_this_hour", 0)
            for field in ["last_forward_time", "last_ad_detected_time"]:
                val = st.get(field)
                if val and (not totals[field] or val > totals[field]):
                    totals[field] = val
        return totals


# Global session manager
sm = SessionManager()


# ── FastAPI App ──────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    config = load_config()

    logger.info("=" * 60)
    logger.info("  OPERATOR BOT V2 — Multi-Session")
    logger.info(f"  WPP Server: {config['wpp_server_url']}")
    logger.info(f"  Monitored groups: {len(config.get('monitored_groups', []))}")
    logger.info(f"  Target groups: {len(config.get('target_groups', []))}")
    logger.info(f"  Moderator IDs: {config.get('moderator_bot_ids', [])}")
    logger.info("=" * 60)

    # Restore all saved sessions
    saved_sessions = config.get("sessions", [])
    # Backwards compat: if no sessions list, use single session_name
    if not saved_sessions:
        saved_sessions = [config.get("session_name", "operator_1")]

    for name in saved_sessions:
        op = sm.add(name, config["wpp_server_url"], config["wpp_secret_key"])
        try:
            await op.wpp.generate_token()
            await op.wpp.start_session()
            await op.wpp.subscribe_webhook()
            await asyncio.sleep(2)
            status = await op.wpp.check_session_status()
            if status.get("connected"):
                logger.info(f"✅ Session '{name}' auto-reconnected!")
                op.start_polling()  # Start active polling
            else:
                logger.info(f"Session '{name}' status: {status.get('status')}")
                # Start polling anyway — it will wait for connection
                op.start_polling()
        except Exception as e:
            logger.error(f"Startup error for session '{name}': {e}")

    yield
    # Stop all polling tasks on shutdown
    for op in sm.all():
        op.stop_polling()
    logger.info("Operator bot shutdown complete")


app = FastAPI(title="Operator Bot V2", lifespan=lifespan)

# Middleware: log important requests only (skip noisy endpoints)
@app.middleware("http")
async def log_requests(request: Request, call_next):
    path = request.url.path
    # Skip noisy polling endpoints
    skip_paths = ("/health", "/api/logs", "/api/bots-status", "/config", "/favicon.ico")
    if not any(path.startswith(sp) for sp in skip_paths):
        logger.info(f"📥 {request.method} {path} from {request.client.host if request.client else '?'}")
    response = await call_next(request)
    return response

# Templates & static files
os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# ── Dashboard Page ───────────────────────────────────────────────────────────
@app.get("/")
async def dashboard_page(request: Request):
    """Serve the operator dashboard."""
    return templates.TemplateResponse("operator_dashboard.html", {"request": request})


# ── QR Code Proxy ────────────────────────────────────────────────────────────
@app.get("/api/qr")
@app.get("/api/session/{session_name}/qr")
async def api_qr(session_name: str = None):
    """Return QR code for a specific session (or first available)."""
    # Find the right session
    op = None
    if session_name:
        op = sm.get(session_name) or sm.find_by_session(session_name)
    if not op:
        op = sm.get_first()
    if not op:
        return {"qr": None, "status": "not_initialized"}

    wpp = op.wpp
    qr_cache = op.qr_cache

    # Skip webhook cache — direct fetch from WPP server gives cleaner PNG images

    # 2. Ensure token
    if not wpp.token:
        try:
            await wpp.generate_token()
        except Exception:
            return {"qr": None, "status": "no_token"}

    # 3. Check session status first
    status_url = f"{wpp.base_url}/api/{wpp.session}/status-session"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            status_resp = await client.get(status_url, headers=wpp.headers)
            if status_resp.status_code == 200:
                status_data = status_resp.json()
                wpp_status = status_data.get("status", "").upper()
                logger.info(f"QR FETCH [{wpp.session}]: session status = {wpp_status}")

                # If CLOSED, restart the session
                if wpp_status == "CLOSED":
                    logger.info(f"QR FETCH [{wpp.session}]: session CLOSED, restarting...")
                    webhook_url = os.environ.get(
                        "OPERATOR_WEBHOOK_URL",
                        "http://victorious-transformation.railway.internal:8001/webhook"
                    )
                    start_url = f"{wpp.base_url}/api/{wpp.session}/start-session"
                    await client.post(start_url, json={
                        "webhook": webhook_url,
                        "waitQrCode": False
                    }, headers=wpp.headers)
                    return {"qr": None, "status": "restarting", "message": "Sessão reiniciando, aguarde..."}

                # If INITIALIZING, just wait
                if wpp_status == "INITIALIZING":
                    logger.info(f"QR FETCH [{wpp.session}]: initializing, checking for QR...")
    except Exception as e:
        logger.warning(f"QR FETCH [{wpp.session}]: status check failed: {e}")

    # 4. Try to get QR directly from WPP server
    url = f"{wpp.base_url}/api/{wpp.session}/qrcode-session"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, headers=wpp.headers)
            if resp.status_code == 401:
                await wpp.generate_token()
                resp = await client.get(url, headers=wpp.headers)

            content_type = resp.headers.get("content-type", "")
            logger.info(f"QR FETCH [{wpp.session}]: qrcode-session returned {resp.status_code}, type={content_type[:30]}")

            if resp.status_code == 200:
                if "image" in content_type:
                    b64 = base64.b64encode(resp.content).decode("utf-8")
                    data_uri = f"data:{content_type.split(';')[0]};base64,{b64}"
                    qr_cache["qr"] = data_uri
                    logger.info(f"QR FETCH [{wpp.session}]: ✅ Got QR image!")
                    return {"qr": data_uri, "status": "waiting_scan", "session": wpp.session}
                try:
                    rdata = resp.json()
                    qr_data = rdata.get("qrcode") or rdata.get("base64Qr") or rdata.get("urlCode")
                    if qr_data:
                        qr_cache["qr"] = qr_data
                        logger.info(f"QR FETCH [{wpp.session}]: ✅ Got QR from JSON!")
                        return {"qr": qr_data, "status": "waiting_scan", "session": wpp.session}
                    wpp_status = rdata.get("status", "no_qr")
                    if "connected" in str(wpp_status).lower():
                        return {"qr": None, "status": "connected", "session": wpp.session}
                    logger.info(f"QR FETCH [{wpp.session}]: no QR yet, WPP status={wpp_status}")
                    return {"qr": None, "status": wpp_status}
                except Exception:
                    return {"qr": None, "status": "unknown_response"}
            return {"qr": None, "status": f"error_{resp.status_code}"}
    except Exception as e:
        logger.error(f"Error fetching QR: {e}")
        return {"qr": None, "status": "error", "detail": str(e)}


@app.post("/api/session/start")
@app.post("/api/session/{session_name}/start")
async def api_session_start(session_name: str = None):
    """Start or reconnect an operator session. Simple flow like the moderator bot:
    generate_token → start_session → subscribe_webhook. No renaming, no pivoting.
    """
    config = get_cfg()

    # Find or create session
    op = None
    if session_name:
        op = sm.get(session_name) or sm.find_by_session(session_name)
    if not op:
        op = sm.get_first()
    if not op:
        op = sm.add("operator_1", config["wpp_server_url"], config["wpp_secret_key"])

    name = op.wpp.session
    op.qr_cache["qr"] = None
    logger.info(f"START [{name}]: Starting session...")

    try:
        # Full flow: token → start (with webhook URL) → subscribe → poll
        await op.wpp.generate_token()
        await asyncio.sleep(2)
        await op.wpp.start_session()
        await asyncio.sleep(2)
        await op.wpp.subscribe_webhook()

        # Start polling as safety fallback
        op.start_polling()

        logger.info(f"📱 START [{name}]: Session started with webhook + polling!")
        return {
            "status": "success",
            "message": f"Sessão '{name}' iniciada! Aguarde o QR Code.",
            "new_session": name
        }
    except Exception as e:
        logger.error(f"START [{name}]: Error: {e}")
        return {"status": "error", "message": str(e)}


@app.post("/api/session/new")
async def api_session_new():
    """Create a brand NEW operator session alongside existing ones."""
    config = get_cfg()

    # Find lowest available operator number (starting from 1)
    import re
    existing_nums = set()
    for op in sm.all():
        m = re.search(r'(\d+)$', op.wpp.session)
        if m:
            existing_nums.add(int(m.group(1)))
    # Find first gap starting from 1
    next_num = 1
    while next_num in existing_nums:
        next_num += 1
    new_name = f"operator_{next_num}"

    try:
        # Create new session in manager
        op = sm.add(new_name, config["wpp_server_url"], config["wpp_secret_key"])

        # Start it up
        await op.wpp.generate_token()
        await op.wpp.start_session()
        await op.wpp.subscribe_webhook()

        # Persist
        _persist_sessions()

        logger.info(f"NEW OPERATOR: Created session '{new_name}'")
        return {
            "status": "success",
            "message": f"Operador '{new_name}' criado! Aguarde o QR Code.",
            "new_session": new_name
        }
    except Exception as e:
        logger.error(f"Error creating new operator: {e}")
        return {"status": "error", "message": str(e)}


def _persist_sessions():
    """Save current session names to config for persistence."""
    cfg = get_cfg()
    cfg["sessions"] = [op.wpp.session for op in sm.all()]
    save_config(cfg)


@app.post("/api/sessions/reset")
async def api_sessions_reset():
    """Close ALL sessions and start fresh with operator_1."""
    config = get_cfg()
    closed = []

    for op in list(sm.all()):
        name = op.wpp.session
        try:
            await op.wpp.close_session()
        except Exception:
            pass
        try:
            await op.wpp.delete_session()
        except Exception:
            pass
        sm.remove(name)
        closed.append(name)

    op = sm.add("operator_1", config["wpp_server_url"], config["wpp_secret_key"])
    try:
        await op.wpp.generate_token()
        await op.wpp.start_session()
        await op.wpp.subscribe_webhook()
    except Exception as e:
        logger.warning(f"Reset: operator_1 started but may need QR: {e}")

    _persist_sessions()
    logger.info(f"RESET: Closed {closed}, created fresh operator_1")
    return {
        "status": "success",
        "closed": closed,
        "message": f"Reset completo! Fechados: {', '.join(closed) or 'nenhum'}. Criado: operator_1",
        "new_session": "operator_1"
    }


# ── Webhook Handler ──────────────────────────────────────────────────────────
@app.post("/webhook")
async def receive_webhook(request: Request):
    """Handle WPPConnect webhook events.
    Routes events to the correct OperatorSession by the 'session' field.
    """
    try:
        data = await request.json()
    except Exception:
        return {"status": "error"}

    event = (data.get("event", "") or "").lower()
    config = get_cfg()
    # Identify which session this event belongs to
    session_name = data.get("session", "")
    op = sm.find_by_session(session_name) if session_name else sm.get_first()
    if not op:
        op = sm.get_first()  # Fallback

    logger.info(f"WEBHOOK [{session_name}] EVENT: '{event}' | Keys: {list(data.keys())[:10]} | Preview: {json.dumps(data, default=str)[:200]}")

    # ── Capture QR Code from webhook ─────────────────────────────────────
    if event in ("onqrcode", "qrcode", "on-qr-code"):
        response = data.get("response") or data.get("data") or {}
        qr_value = None
        if isinstance(response, dict):
            qr_value = response.get("qrcode") or response.get("base64Qr") or response.get("urlCode")
        elif isinstance(response, str) and len(response) > 10:
            qr_value = response
        if not qr_value:
            qr_value = data.get("qrcode") or data.get("base64Qr") or data.get("urlCode")
        if qr_value and op:
            # Ensure QR has proper data URI prefix for <img src>
            qr_str = str(qr_value)
            if not qr_str.startswith("data:"):
                qr_str = f"data:image/png;base64,{qr_str}"
            op.qr_cache["qr"] = qr_str
            op.qr_cache["updated_at"] = datetime.utcnow().isoformat()
            logger.info(f"📱 QR captured for session '{session_name}' ({len(qr_str)} chars)")
            return {"status": "qr_captured"}
        logger.warning(f"onqrcode but no QR value: {str(data)[:200]}")
        return {"status": "qr_empty"}

    # ── Clear QR cache on connect ─────────────────────────────────────────
    if event in ("onconnectionupdate", "onstatechange") and "connected" in str(data).lower():
        if op:
            op.qr_cache["qr"] = None

    # ── Handle reaction events ───────────────────────────────────────────
    if event in ("onreactionmessage", "on-reaction-message", "onreaction"):
        if not op:
            return {"status": "no_session"}

        response = data.get("response") or data.get("data") or data
        if not isinstance(response, dict):
            return {"status": "invalid_reaction"}

        reaction_text = response.get("reactionText", "") or response.get("reaction", "") or ""
        approved_emoji = config.get("approved_emoji", "🔁")
        if reaction_text != approved_emoji:
            return {"status": "wrong_emoji"}

        chat_id = (
            response.get("chatId", "")
            or response.get("chat", {}).get("id", {}).get("_serialized", "")
            if isinstance(response.get("chat"), dict) else ""
        )
        msg_id_obj = response.get("msgId") or response.get("id") or response.get("reactionBy") or {}

        reacted_msg_id = ""
        if isinstance(msg_id_obj, dict):
            reacted_msg_id = msg_id_obj.get("_serialized", "")
            if not chat_id:
                remote = msg_id_obj.get("remote", {})
                if isinstance(remote, dict):
                    chat_id = remote.get("_serialized", "")
                elif isinstance(remote, str):
                    chat_id = remote
        elif isinstance(msg_id_obj, str):
            reacted_msg_id = msg_id_obj

        if not reacted_msg_id:
            for field in ["msgId", "id", "messageId"]:
                obj = response.get(field, {})
                if isinstance(obj, dict):
                    reacted_msg_id = obj.get("_serialized", "")
                    if reacted_msg_id: break
                elif isinstance(obj, str) and obj:
                    reacted_msg_id = obj
                    break

        if not reacted_msg_id:
            logger.warning(f"REACTION: emoji but no msg_id. Data: {json.dumps(response, default=str)[:500]}")
            return {"status": "no_msg_id"}

        monitored = config.get("monitored_groups", [])
        if monitored and chat_id and chat_id not in monitored:
            return {"status": "not_monitored_group"}

        # Extract who reacted — WPP puts it in id.participant or senderUserJid
        sender_id = ""
        # Priority 1: id.participant (most reliable for reactions)
        id_obj = response.get("id") or data.get("id") or {}
        if isinstance(id_obj, dict):
            participant = id_obj.get("participant", "")
            if isinstance(participant, dict):
                sender_id = participant.get("_serialized", "") or participant.get("user", "")
            elif isinstance(participant, str):
                sender_id = participant
        # Priority 2: senderUserJid (some WPP versions)
        if not sender_id:
            sender_id = response.get("senderUserJid") or data.get("senderUserJid") or ""
        # Priority 3: sender field
        if not sender_id:
            sender_obj = response.get("sender", {}) or response.get("senderId", "")
            if isinstance(sender_obj, dict):
                sender_id = sender_obj.get("id", "") or sender_obj.get("_serialized", "") or sender_obj.get("user", "")
            elif isinstance(sender_obj, str):
                sender_id = sender_obj
        # Priority 4: reactionBy
        if not sender_id:
            reaction_by = response.get("reactionBy", "")
            if isinstance(reaction_by, str):
                sender_id = reaction_by
            elif isinstance(reaction_by, dict):
                sender_id = reaction_by.get("_serialized", "") or reaction_by.get("user", "")

        sender_num = sender_id.split("@")[0] if sender_id else ""
        mod_ids = config.get("moderator_bot_ids", [])
        mod_set = set(mod_ids)
        is_lid = "@lid" in sender_id

        logger.info(f"⚡ REACTION: sender_id={sender_id}, sender_num={sender_num}, mod_ids={mod_ids}")

        if mod_ids and sender_num:
            # FAST CHECK: LID or number directly in mod_ids (no API call)
            if sender_num in mod_set or sender_id in mod_set:
                logger.info(f"⚡ REACTION: ✅ direct match for {sender_num}")
            # FALLBACK: resolve LID → phone via group members API
            elif is_lid:
                resolved_phone = ""
                try:
                    lid_map = await op.wpp.get_group_members_phones(chat_id)
                    resolved_phone = lid_map.get(sender_num, "")
                    if resolved_phone and resolved_phone != sender_num:
                        logger.info(f"⚡ LID_RESOLVE: {sender_num} → {resolved_phone}")
                except Exception as e:
                    logger.warning(f"⚡ LID_RESOLVE error: {e}")
                if resolved_phone and resolved_phone in mod_set:
                    logger.info(f"⚡ REACTION: ✅ phone match via LID resolve: {resolved_phone}")
                else:
                    logger.info(f"⚡ REACTION: from non-moderator {sender_id[:30]}, ignoring. Add '{sender_num}' or '{resolved_phone or sender_id}' to moderator_bot_ids.")
                    return {"status": "not_moderator"}
            else:
                logger.info(f"⚡ REACTION: from non-moderator {sender_num}, ignoring.")
                return {"status": "not_moderator"}
        elif not mod_ids:
            logger.warning(f"⚡ REACTION: no moderator_bot_ids configured, accepting all reactions")

        logger.info(f"REACTION [{op.name}]: 🔁 approved ad! msg_id={reacted_msg_id[:50]}, chat={chat_id[:25]}")

        # Forward using THIS session's engine and WPP client
        asyncio.create_task(op.engine.process_approved_ad(op.wpp, reacted_msg_id, chat_id))
        return {"status": "forwarding"}

    return {"status": "ignored"}


# ── API Endpoints ────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    """Health check — aggregated across all sessions."""
    config = get_cfg()
    # Use first session for backwards compat
    first = sm.get_first()
    session_status = {"connected": False, "status": "unknown"}
    if first:
        session_status = await first.wpp.check_session_status()

    return {
        "status": "ok",
        "version": "v3-multi-session",
        "session": first.wpp.session if first else "?",
        "connected": session_status.get("connected", False),
        "monitored_groups": len(config.get("monitored_groups", [])),
        "target_groups": len(config.get("target_groups", [])),
        "engine": sm.aggregated_stats,
        "total_sessions": len(sm.sessions),
        "timestamp": datetime.now().isoformat(),
    }


@app.get("/api/logs")
async def api_logs(since: str = None):
    """Return recent logs from memory buffer."""
    logs = _mem_handler.get_logs(since=since)
    return {"logs": logs, "count": len(logs), "timestamp": datetime.now().isoformat()}


@app.get("/stats")
async def stats():
    """Detailed stats — aggregated."""
    return {
        "engine": sm.aggregated_stats,
        "sessions": {op.name: op.engine.stats for op in sm.all()},
        "config": {
            "sessions": [op.wpp.session for op in sm.all()],
            "monitored_groups": get_cfg().get("monitored_groups", []),
            "target_groups": get_cfg().get("target_groups", []),
            "moderator_bot_ids": get_cfg().get("moderator_bot_ids", []),
            "approved_emoji": get_cfg().get("approved_emoji", "🔁"),
            "forward_delay": get_cfg().get("forward_delay_seconds", 0),
            "max_per_hour": get_cfg().get("max_forwards_per_hour", 0),
        },
    }


@app.get("/api/bots-status")
async def bots_status():
    """Return status of ALL managed operator sessions."""
    config = get_cfg()
    sessions = []

    # Use SessionManager — check each registered session
    for op in sm.all():
        status_info = {"connected": False, "status": "DISCONNECTED"}
        try:
            status_info = await op.wpp.check_session_status()
        except:
            pass

        sessions.append({
            "name": op.wpp.session,
            "connected": status_info.get("connected", False),
            "status": "CONNECTED" if status_info.get("connected") else status_info.get("status", "DISCONNECTED"),
            "active": True,
            "stats": op.engine.stats,
        })

    # If no sessions in manager, show fallback
    if not sessions:
        sessions.append({
            "name": config.get("session_name", "operator_1"),
            "connected": False,
            "status": "CLOSED",
            "active": False,
        })

    return {
        "webserver": True,
        "sessions": sessions,
        "engine": sm.aggregated_stats,
    }



@app.get("/api/session/{session_name}/groups")
async def api_session_groups(session_name: str):
    """List all groups this operator session is part of."""
    op = sm.get(session_name) or sm.find_by_session(session_name)
    if not op:
        return {"status": "error", "message": f"Session '{session_name}' not found", "groups": []}
    try:
        groups = await op.wpp.get_all_groups()
        return {"status": "ok", "groups": groups, "count": len(groups)}
    except Exception as e:
        logger.error(f"Error fetching groups for {session_name}: {e}")
        return {"status": "error", "message": str(e), "groups": []}


@app.post("/api/session/{session_name}/logout")
async def api_session_logout(session_name: str):
    """Logout session (clears auth, forces fresh QR). Then clear session data."""
    op = sm.get(session_name) or sm.find_by_session(session_name)
    if not op:
        return {"status": "error", "message": f"Session '{session_name}' not found"}
    try:
        await op.wpp.delete_session()  # logout + close + clear-session-data
        op.qr_cache["qr"] = None
        logger.info(f"LOGOUT [{session_name}]: Session cleared, ready for fresh start")
        return {"status": "ok", "message": f"Sessão '{session_name}' deslogada. Clique 'Iniciar' para novo QR."}
    except Exception as e:
        logger.error(f"Logout error: {e}")
        return {"status": "error", "message": str(e)}


@app.post("/api/session/{session_name}/close")
async def close_session_api(session_name: str):
    """Close and delete a specific session."""
    config = get_cfg()
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            tok_resp = await client.post(
                f"{config['wpp_server_url']}/api/{session_name}/{config['wpp_secret_key']}/generate-token",
                timeout=5
            )
            token = tok_resp.json().get("token", "") if tok_resp.status_code in (200, 201) else ""
            headers = {"Authorization": f"Bearer {token}"}

            for endpoint in ["logout-session", "close-session", "clear-session-data"]:
                try:
                    await client.post(f"{config['wpp_server_url']}/api/{session_name}/{endpoint}", headers=headers, timeout=8)
                except: pass
                await asyncio.sleep(1)
        # Also remove from SessionManager
        op = sm.find_by_session(session_name)
        if op:
            sm.remove(op.name)
            _persist_sessions()

        logger.info(f"Session '{session_name}' closed and removed")
        return {"status": "ok", "message": f"Sessão '{session_name}' removida"}
    except Exception as e:
        logger.error(f"Error closing session {session_name}: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/config")
async def get_config_api():
    c = get_cfg().copy()
    c["wpp_secret_key"] = "***"
    return c


@app.post("/config")
async def update_config_api(request: Request):
    try:
        new_values = await request.json()
        current = get_cfg().copy()
        new_values.pop("wpp_secret_key", None)
        current.update(new_values)
        save_config(current)
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.post("/reconnect")
@app.post("/api/session/{session_name}/reconnect")
async def reconnect(session_name: str = None):
    op = None
    if session_name:
        op = sm.get(session_name) or sm.find_by_session(session_name)
    if not op:
        op = sm.get_first()
    if op:
        await op.wpp.full_reconnect()
        return {"status": "reconnecting", "session": op.wpp.session}
    return {"status": "error", "message": "No sessions available"}


# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    config = load_config()
    port = config.get("port", 8001)
    logger.info(f"Starting operator bot on port {port}...")
    uvicorn.run("operator_bot:app", host="0.0.0.0", port=port, reload=True)
