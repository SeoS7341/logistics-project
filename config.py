import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")