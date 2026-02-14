from sqlalchemy import create_engine, Column, Integer, String, Date, DateTime, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import date, datetime
import os

# Ensure the data directory exists
try:
    os.makedirs("/data", exist_ok=True)
except OSError:
    # Likely on Windows or restricted env without /data access
    # We will use local directory in that case
    pass

# Database URL checks if running in container or local
if os.path.exists("/data") and os.access("/data", os.W_OK):
    DATABASE_URL = "sqlite:////data/bot_idsr.db"
else:
    DATABASE_URL = "sqlite:///./bot_idsr.db" # Fallback for local testing

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False, "timeout": 30},
    pool_pre_ping=True,
)

# Enable WAL mode for concurrent read access (critical for webhook + landing page)
from sqlalchemy import event as sa_event
@sa_event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_conn, connection_record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=30000")
    cursor.close()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class UserStats(Base):
    __tablename__ = "user_stats"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, index=True) # WhatsApp ID (e.g., 5511999999999@c.us)
    group_id = Column(String, index=True) # Group ID (e.g., 1203630234234@g.us)
    last_active_date = Column(Date, default=date.today)
    window_start = Column(DateTime, default=datetime.utcnow) # Start of current flood window
    message_count = Column(Integer, default=0)
    photo_count = Column(Integer, default=0)

    __table_args__ = (UniqueConstraint('user_id', 'group_id', name='_user_group_uc'),)

class WhatsAppGroup(Base):
    """Managed WhatsApp groups for the affiliate system."""
    __tablename__ = "whatsapp_groups"

    id = Column(Integer, primary_key=True, index=True)
    group_jid = Column(String, unique=True, index=True)  # WhatsApp group JID
    name = Column(String, default="")                      # Display name
    invite_link = Column(String, default="")               # https://chat.whatsapp.com/xxx
    max_members = Column(Integer, default=256)
    current_members = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)              # Accepting new members?
    display_order = Column(Integer, default=0)             # Fill order (lower = first)
    created_at = Column(DateTime, default=datetime.utcnow)

class Affiliate(Base):
    """Affiliate who creates referral links."""
    __tablename__ = "affiliates"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    phone = Column(String, default="")
    slug = Column(String, unique=True, index=True)         # Custom URL slug
    created_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    
    joins = relationship("JoinTracking", back_populates="affiliate")

class JoinTracking(Base):
    """Track each click and confirmed join via affiliate link."""
    __tablename__ = "join_tracking"

    id = Column(Integer, primary_key=True, index=True)
    affiliate_id = Column(Integer, ForeignKey("affiliates.id"), nullable=True, index=True)
    group_id = Column(Integer, ForeignKey("whatsapp_groups.id"), nullable=True)
    visitor_ip = Column(String, default="")
    clicked_at = Column(DateTime, default=datetime.utcnow)
    joined_at = Column(DateTime, nullable=True)
    user_phone = Column(String, default="")                 # Phone of who joined
    confirmed = Column(Boolean, default=False)

    affiliate = relationship("Affiliate", back_populates="joins")

def init_db():
    Base.metadata.create_all(bind=engine)
    # Migration: add window_start column if missing
    try:
        from sqlalchemy import inspect, text
        insp = inspect(engine)
        cols = [c['name'] for c in insp.get_columns('user_stats')]
        if 'window_start' not in cols:
            with engine.connect() as conn:
                conn.execute(text("ALTER TABLE user_stats ADD COLUMN window_start DATETIME"))
                conn.commit()
    except Exception:
        pass


