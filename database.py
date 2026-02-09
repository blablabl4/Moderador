from sqlalchemy import create_engine, Column, Integer, String, Date, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import date
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

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class UserStats(Base):
    __tablename__ = "user_stats"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, index=True) # WhatsApp ID (e.g., 5511999999999@c.us)
    group_id = Column(String, index=True) # Group ID (e.g., 1203630234234@g.us)
    last_active_date = Column(Date, default=date.today)
    message_count = Column(Integer, default=0)
    photo_count = Column(Integer, default=0)

    # Unique constraint to ensure one record per user per group per day logic (handled via date check)
    # Actually, we just want unique user_id + group_id, and reset if date changes.
    __table_args__ = (UniqueConstraint('user_id', 'group_id', name='_user_group_uc'),)

def init_db():
    Base.metadata.create_all(bind=engine)
