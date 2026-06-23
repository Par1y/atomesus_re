import os

DEBUG = os.getenv("CHAT2API_DEBUG", "false").lower() in ("1", "true", "yes", "y")
