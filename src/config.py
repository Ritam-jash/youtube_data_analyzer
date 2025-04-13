"""
Configuration settings for the YouTube Data Analyzer application.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# YouTube API settings
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

# Data directories
DATA_RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
DATA_PROCESSED_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "processed")

# Ensure data directories exist
os.makedirs(DATA_RAW_DIR, exist_ok=True)
os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)

# Analysis settings
MAX_RESULTS_PER_PAGE = 50  # Maximum number of results to retrieve per API call
DEFAULT_CHANNEL_ID = "UC_x5XG1OV2P6uZZ5FSM9Ttw"  # Google Developers channel as default
DEFAULT_VIDEOS_COUNT = 100  # Default number of videos to analyze

# Dashboard settings
THEME_COLOR = "#FF0000"  # YouTube red color
DEFAULT_DATE_RANGE = 365  # Default date range in days for filtering