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

# ── Configuration ────────────────────────────────────────────────────────────
CONFIG_FILE = os.environ.get("OPERATOR_CONFIG", "./operator_config.json")

DEFAULTS = {
    # WPPConnect Server connection (Railway internal URL by default)
    "wpp_server_url": os.environ.get("WPP_SERVER_URL", "http://server-cli.railway.internal:8080"),
    "session_name": os.environ.get("OPERATOR_SESSION", "operator_1"),
    "wpp_secret_key": os.environ.get("WPP_SECRET_KEY", "THISISMYSECURETOKEN"),

    # Which advertiser groups this operator monitors (same ones the moderator is in)
    "monitored_groups": [],               # e.g. ["120363406870144681@g.us"]

    # Moderator bot identification (phone numbers without @c.us)
    "moderator_bot_ids": [],              # e.g. ["5511983426767"]

    # The emoji the moderator uses to approve ads
    "approved_emoji": "🔁",

    # Target groups this operator forwards approved ads to
    "target_groups": [],                  # List of group JIDs

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


# ── WPPConnect Client (minimal — operator only needs forward + read) ─────────
class OperatorWPPClient:

    def __init__(self, base_url: str, session: str, secret_key: str):
        self.base_url = base_url.rstrip("/")
        self.session = session
        self.secret_key = secret_key
        self.token = None
        self.headers = {}

    async def generate_token(self):
        url = f"{self.base_url}/api/{self.session}/{self.secret_key}/generate-token"
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(url)
            if resp.status_code in (200, 201):
                data = resp.json()
                self.token = data.get("token", "")
                self.headers = {
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                }
                logger.info(f"Token generated for session '{self.session}'")
                return True
        logger.error(f"Failed to generate token: {resp.status_code}")
        return False

    async def start_session(self, webhook_url: str = ""):
        if not self.token:
            await self.generate_token()
        url = f"{self.base_url}/api/{self.session}/start-session"
        payload = {"waitQrCode": False}
        if webhook_url:
            payload["webhook"] = webhook_url
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(url, json=payload, headers=self.headers)
            logger.info(f"Start session: {resp.status_code} - {resp.text[:200]}")
            return resp.status_code in (200, 201)

    async def subscribe_webhook(self, webhook_url: str):
        url = f"{self.base_url}/api/{self.session}/subscribe"
        for payload in [
            {"webhookUrl": webhook_url, "webhook": webhook_url},
            {"url": webhook_url},
        ]:
            try:
                async with httpx.AsyncClient(timeout=15.0) as client:
                    resp = await client.post(url, json=payload, headers=self.headers)
                    if resp.status_code in (200, 201):
                        logger.info(f"Webhook subscribed: {webhook_url}")
                        return True
            except Exception:
                continue
        logger.warning("Could not subscribe webhook via API — check WPP server config")
        return False

    async def check_session_status(self) -> dict:
        url = f"{self.base_url}/api/{self.session}/check-connection-session"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(url, headers=self.headers)
                if resp.status_code == 200:
                    data = resp.json()
                    connected = data.get("response", False)
                    return {"connected": connected, "status": "connected" if connected else "disconnected"}
        except Exception as e:
            logger.error(f"Session status check failed: {e}")
        return {"connected": False, "status": "error"}

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
        NO FALLBACK — only forward-messages. If it fails, we log and move on."""
        url = f"{self.base_url}/api/{self.session}/forward-messages"
        payload = {
            "phone": to_chat_id,
            "messageId": message_id,
            "isGroup": "@g.us" in to_chat_id,
        }
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(url, json=payload, headers=self.headers)
                if resp.status_code in (200, 201):
                    body = resp.json() if resp.text else {}
                    if body.get("status") != "error":
                        return True
                logger.warning(f"forward failed: {resp.status_code} - {resp.text[:200]}")
        except Exception as e:
            logger.error(f"forward error: {e}")
        return False

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

        # Validate: only forward image/video with caption
        msg_data = await wpp.get_message_by_id(chat_id, msg_id)
        if msg_data:
            msg_type = msg_data.get("type", "")
            caption = msg_data.get("caption", "") or ""
            if msg_type not in ("image", "video"):
                logger.info(f"ENGINE: skip non-media msg type={msg_type}")
                self._stats["total_skipped_not_media"] += 1
                return
            if not caption.strip():
                logger.info(f"ENGINE: skip media without caption")
                self._stats["total_skipped_not_media"] += 1
                return
            logger.info(f"ENGINE: ✅ validated — type={msg_type}, caption={caption[:50]}")
        else:
            # Could not fetch message details — forward anyway (moderator already validated)
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


# ── Global instances ─────────────────────────────────────────────────────────
wpp: OperatorWPPClient | None = None
engine = ForwardEngine()


# ── FastAPI App ──────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global wpp
    config = load_config()

    logger.info("=" * 60)
    logger.info("  OPERATOR BOT V2 — Reaction-Based")
    logger.info(f"  Session: {config['session_name']}")
    logger.info(f"  WPP Server: {config['wpp_server_url']}")
    logger.info(f"  Monitored groups: {len(config.get('monitored_groups', []))}")
    logger.info(f"  Target groups: {len(config.get('target_groups', []))}")
    logger.info(f"  Moderator IDs: {config.get('moderator_bot_ids', [])}")
    logger.info(f"  Approved emoji: {config.get('approved_emoji', '🔁')}")
    logger.info(f"  Forward delay: {config.get('forward_delay_seconds', 0)}s")
    max_h = config.get('max_forwards_per_hour', 0)
    logger.info(f"  Max forwards/hour: {'unlimited' if max_h <= 0 else max_h}")
    logger.info("=" * 60)

    if not config.get("monitored_groups"):
        logger.warning("⚠️  No monitored_groups configured!")
    if not config.get("target_groups"):
        logger.warning("⚠️  No target_groups configured!")
    if not config.get("moderator_bot_ids"):
        logger.warning("⚠️  No moderator_bot_ids configured — will accept reactions from anyone!")

    wpp = OperatorWPPClient(
        config["wpp_server_url"],
        config["session_name"],
        config["wpp_secret_key"],
    )

    try:
        await wpp.generate_token()

        port = config.get("port", 8001)
        webhook_url = os.environ.get(
            "OPERATOR_WEBHOOK_URL",
            f"http://victorious-transformation.railway.internal:{port}/webhook"
        )
        logger.info(f"  Webhook URL: {webhook_url}")

        await wpp.start_session(webhook_url=webhook_url)
        await wpp.subscribe_webhook(webhook_url)
    except Exception as e:
        logger.error(f"Startup error: {e}")

    yield
    logger.info("Operator bot shutdown complete")


app = FastAPI(title="Operator Bot V2", lifespan=lifespan)

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
async def api_qr():
    """Proxy the QR code from WPPConnect Server."""
    if not wpp or not wpp.token:
        try:
            await wpp.generate_token()
        except Exception:
            return {"qr": None, "status": "no_token"}

    url = f"{wpp.base_url}/api/{wpp.session}/qrcode-session"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, headers=wpp.headers)
            if resp.status_code == 401:
                await wpp.generate_token()
                resp = await client.get(url, headers=wpp.headers)

            content_type = resp.headers.get("content-type", "")

            if resp.status_code == 200:
                # Binary image response
                if "image" in content_type:
                    b64 = base64.b64encode(resp.content).decode("utf-8")
                    data_uri = f"data:{content_type.split(';')[0]};base64,{b64}"
                    return {"qr": data_uri, "status": "waiting_scan"}

                # JSON response
                try:
                    data = resp.json()
                    qr_data = data.get("qrcode") or data.get("base64Qr") or data.get("urlCode")
                    if qr_data:
                        return {"qr": qr_data, "status": "waiting_scan"}
                    # Session might already be connected
                    status = data.get("status", "no_qr")
                    if "connected" in str(status).lower() or data.get("response") is True:
                        return {"qr": None, "status": "connected"}
                    return {"qr": None, "status": status}
                except Exception:
                    return {"qr": None, "status": "unknown_response"}
            else:
                return {"qr": None, "status": f"error_{resp.status_code}"}
    except Exception as e:
        logger.error(f"Error fetching QR: {e}")
        return {"qr": None, "status": "error", "detail": str(e)}


@app.post("/api/session/start")
async def api_session_start():
    """Start or restart the WPP session to generate a new QR code."""
    if wpp:
        config = get_cfg()
        port = config.get("port", 8001)
        webhook_url = os.environ.get(
            "OPERATOR_WEBHOOK_URL",
            f"http://victorious-transformation.railway.internal:{port}/webhook"
        )
        await wpp.start_session(webhook_url=webhook_url)
        return {"status": "success", "message": "Sessão iniciada! Aguarde o QR Code."}
    return {"status": "error", "message": "WPP client not initialized"}


# ── Webhook Handler ──────────────────────────────────────────────────────────
@app.post("/webhook")
async def receive_webhook(request: Request):
    """Handle WPPConnect webhook events.
    Core logic: detect moderator's 🔁 reaction on messages in monitored groups,
    then forward the original message to target groups.
    """
    try:
        data = await request.json()
    except Exception:
        return {"status": "error"}

    event = (data.get("event", "") or "").lower()
    config = get_cfg()

    # ── Handle reaction events ───────────────────────────────────────────
    if event in ("onreactionmessage", "on-reaction-message", "onreaction"):
        response = data.get("response") or data.get("data") or data
        if not isinstance(response, dict):
            return {"status": "invalid_reaction"}

        # Extract reaction details
        reaction_text = response.get("reactionText", "") or response.get("reaction", "") or ""
        approved_emoji = config.get("approved_emoji", "🔁")

        # Check if this is the approved emoji
        if reaction_text != approved_emoji:
            return {"status": "wrong_emoji"}

        # Check which chat this reaction happened in
        # Reaction events can have chatId at different levels
        chat_id = (
            response.get("chatId", "")
            or response.get("chat", {}).get("id", {}).get("_serialized", "")
            if isinstance(response.get("chat"), dict) else ""
        )
        # Also try msgId to extract chatId
        msg_id_obj = response.get("msgId") or response.get("id") or response.get("reactionBy") or {}

        # Try to get the _serialized ID of the REACTED-TO message
        reacted_msg_id = ""
        if isinstance(msg_id_obj, dict):
            reacted_msg_id = msg_id_obj.get("_serialized", "")
            # Extract chatId from the msg_id if not found yet
            if not chat_id:
                remote = msg_id_obj.get("remote", {})
                if isinstance(remote, dict):
                    chat_id = remote.get("_serialized", "")
                elif isinstance(remote, str):
                    chat_id = remote
        elif isinstance(msg_id_obj, str):
            reacted_msg_id = msg_id_obj

        # WPPConnect sometimes puts the reacted message ID in different fields
        if not reacted_msg_id:
            # Try response.msgId._serialized
            for field in ["msgId", "id", "messageId"]:
                obj = response.get(field, {})
                if isinstance(obj, dict):
                    reacted_msg_id = obj.get("_serialized", "")
                    if reacted_msg_id:
                        break
                elif isinstance(obj, str) and obj:
                    reacted_msg_id = obj
                    break

        if not reacted_msg_id:
            logger.warning(f"REACTION: approved emoji detected but no msg_id found. Data: {json.dumps(response, default=str)[:500]}")
            return {"status": "no_msg_id"}

        # Check if chat is a monitored group
        monitored = config.get("monitored_groups", [])
        if monitored and chat_id and chat_id not in monitored:
            return {"status": "not_monitored_group"}

        # Check if reactor is a moderator bot
        sender_id = ""
        sender_obj = response.get("sender", {}) or response.get("senderId", "")
        if isinstance(sender_obj, dict):
            sender_id = sender_obj.get("id", "") or sender_obj.get("_serialized", "") or sender_obj.get("user", "")
        elif isinstance(sender_obj, str):
            sender_id = sender_obj
        # Also check reactionBy field
        if not sender_id:
            reaction_by = response.get("reactionBy", "")
            if isinstance(reaction_by, str):
                sender_id = reaction_by
            elif isinstance(reaction_by, dict):
                sender_id = reaction_by.get("_serialized", "") or reaction_by.get("user", "")

        sender_phone = sender_id.split("@")[0] if sender_id else ""

        mod_ids = config.get("moderator_bot_ids", [])
        if mod_ids and sender_phone not in mod_ids:
            logger.info(f"REACTION: emoji from non-moderator {sender_phone[:15]}, ignoring")
            return {"status": "not_moderator"}

        # If no moderator filter configured, log a warning but proceed
        if not mod_ids:
            logger.warning(f"REACTION: no moderator_bot_ids configured, accepting reaction from {sender_phone[:15]}")

        logger.info(f"REACTION: 🔁 approved ad detected! msg_id={reacted_msg_id[:50]}, chat={chat_id[:25]}, reactor={sender_phone[:15]}")

        # Forward in background
        asyncio.create_task(engine.process_approved_ad(wpp, reacted_msg_id, chat_id))

        return {"status": "forwarding"}

    # ── Ignore all other events ──────────────────────────────────────────
    # We only care about reactions — the moderator handles everything else
    return {"status": "ignored"}


# ── API Endpoints ────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    """Health check."""
    config = get_cfg()
    session_status = {"connected": False, "status": "unknown"}
    if wpp:
        session_status = await wpp.check_session_status()

    return {
        "status": "ok",
        "version": "v2-reaction",
        "session": config.get("session_name"),
        "connected": session_status.get("connected", False),
        "monitored_groups": len(config.get("monitored_groups", [])),
        "target_groups": len(config.get("target_groups", [])),
        "engine": engine.stats,
        "timestamp": datetime.now().isoformat(),
    }


@app.get("/stats")
async def stats():
    """Detailed stats."""
    return {
        "engine": engine.stats,
        "config": {
            "session": get_cfg().get("session_name"),
            "monitored_groups": get_cfg().get("monitored_groups", []),
            "target_groups": get_cfg().get("target_groups", []),
            "moderator_bot_ids": get_cfg().get("moderator_bot_ids", []),
            "approved_emoji": get_cfg().get("approved_emoji", "🔁"),
            "forward_delay": get_cfg().get("forward_delay_seconds", 0),
            "max_per_hour": get_cfg().get("max_forwards_per_hour", 0),
        },
    }


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
async def reconnect():
    if wpp:
        await wpp.full_reconnect()
        return {"status": "reconnecting"}
    return {"status": "error", "message": "WPP client not initialized"}


# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    config = load_config()
    port = config.get("port", 8001)
    logger.info(f"Starting operator bot on port {port}...")
    uvicorn.run("operator_bot:app", host="0.0.0.0", port=port, reload=True)
