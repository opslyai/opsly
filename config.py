import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-key")
    SQLALCHEMY_DATABASE_URI = os.environ.get("SQLALCHEMY_DATABASE_URI", "sqlite:///opsly.db")
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    REZDY_API_KEY = os.environ.get("REZDY_API_KEY", "")
    REZDY_API_BASE = os.environ.get("REZDY_API_BASE", "https://api.rezdy.com/v1")

    MISSIVE_API_BASE = os.environ.get("MISSIVE_API_BASE", "https://public.missiveapp.com/v1")
    MISSIVE_PAT = os.environ.get("MISSIVE_PAT", "")
    MISSIVE_WAT_RESOURCE = os.environ.get("MISSIVE_WAT_RESOURCE", "")
    MISSIVE_FROM_ADDRESS = os.environ.get("MISSIVE_FROM_ADDRESS", "")
