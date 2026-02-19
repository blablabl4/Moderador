"""
Persistent settings module. Stores bot configuration in a JSON file
so settings survive deployments (when /data volume is mounted).
"""
import json
import os
import logging

logger = logging.getLogger(__name__)

# Path: use /data if available (Railway volume), otherwise local
if os.path.exists("/data") and os.access("/data", os.W_OK):
    SETTINGS_FILE = "/data/settings.json"
else:
    SETTINGS_FILE = "./settings.json"

# Default settings
DEFAULTS = {
    "flood_max_messages": 1,
    "flood_max_photos": 3,
    "flood_max_text_length": 300,
    "flood_window_minutes": 5,
    "flood_action": "delete",       # "delete", "delete_warn", "warn"
    "flood_window_hours": 2,         # Hours before flood counters reset per user
    "warn_message": "⚠️ Você excedeu o limite de mensagens neste grupo. Aguarde antes de enviar novamente.",
    "link_filter_enabled": True,
    "sticker_filter_enabled": True,  # Block stickers from non-admins
    "whitelist_domains": ["idsr.com.br", "tvzapao.com.br"],
    "link_action": "delete",        # "delete", "delete_warn"
    "link_warn_message": "⚠️ Links externos não são permitidos neste grupo.",
    "super_admins": ["5511983426767", "5511981771974"],
    "schedule_open_hour": 10,
    "schedule_open_minute": 0,
    "schedule_close_hour": 20,
    "schedule_close_minute": 0,
    "schedule_enabled": True,
    "private_reply_enabled": True,
    "private_reply_message": "👋 Olá! Sou o bot moderador do grupo. Não respondo mensagens privadas.",
    "moderation_enabled": True,
    "dry_run_mode": False,           # Log actions but don't actually delete/warn
    "ad_group_seconds": 30,          # Group messages within this window as 1 "ad"
    "exclusive_groups_enabled": False,  # Prevent users from being in multiple monitored groups
    "exclusive_groups_message": "⚠️ Você já faz parte de outro grupo moderado. Só é permitido participar de um grupo por vez.",
    "monitored_groups": [],           # Empty = ALL groups; list of group IDs = only those
}

_settings = None

def load_settings() -> dict:
    """Load settings from JSON file, merging with defaults."""
    global _settings
    saved = {}
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
        except Exception as e:
            logger.error(f"Error loading settings: {e}")
    
    # Merge: defaults + saved (saved overrides defaults)
    _settings = {**DEFAULTS, **saved}
    return _settings

def save_settings(new_settings: dict) -> dict:
    """Save settings to JSON file."""
    global _settings
    # Merge with current settings
    current = get_settings()
    current.update(new_settings)
    _settings = current
    
    try:
        os.makedirs(os.path.dirname(SETTINGS_FILE) if os.path.dirname(SETTINGS_FILE) else ".", exist_ok=True)
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(_settings, f, ensure_ascii=False, indent=2)
        logger.info(f"Settings saved to {SETTINGS_FILE}")
    except Exception as e:
        logger.error(f"Error saving settings: {e}")
    
    return _settings

def get_settings() -> dict:
    """Get current settings (cached)."""
    global _settings
    if _settings is None:
        return load_settings()
    return _settings

def get(key: str, default=None):
    """Get a single setting value."""
    return get_settings().get(key, default)
