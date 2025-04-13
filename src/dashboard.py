"""
Streamlit dashboard for YouTube Data Analysis.
"""

import os
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import warnings
from typing import Dict, List, Any, Optional, Union

# Local imports
from src.fetch_data import YouTubeDataFetcher
from src.transform_data import YouTubeDataTransformer
from src.analyze_data import YouTubeDataAnalyzer
from src.config import (
    DEFAULT_CHANNEL_ID,
    DEFAULT_VIDEOS_COUNT,
    DEFAULT_DATE_RANGE,
    THEME_COLOR
)

# Ignore FutureWarnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Set page configuration
st.set_page_config(
    page_title="YouTube Data Analyzer",
    page_icon="🎥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #FF0000;
        margin-bottom: 10px;
    }
    .sub-header {
        font-size: 1.5rem;
        margin-top: 20px;
        margin-bottom: 10px;
    }
    .highlight {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
    }
    .metric-card {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 5px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        text-align: center;
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: bold;
        color: #FF0000;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #666666;
    }
    .stPlotlyChart {
        margin-top: 15px;
        margin-bottom: 25px;
    }
    .footer {
        margin-top: 50px;
        text-align: center;
        color: #888888;
        font-size: 0.8rem;
    }
</style>
""", unsafe_allow_html=True)

# Cache the data loading function
@st.cache_data(ttl=3600)
def load_analysis_results() -> Dict[str, Any]:
    """
    Load analysis results from the analyzer.
    
    Returns:
        Dict: Analysis results
    """
    analyzer = YouTubeDataAnalyzer()
    return analyzer.perform_analysis()

def format_number(num: Union[int, float], precision: int = 1) -> str:
    """
    Format large numbers for display.
    
    Args:
        num: Number to format
        precision: Decimal precision
        
    Returns:
        str: Formatted number
    """
    if num is None:
        return "0"
        
    if isinstance(num, str):
        try:
            num = float(num)
        except ValueError:
            return num
            
    if num < 1000:
        return str(int(num) if num.is_integer() else round(num, precision))
    elif num < 1000000:
        return f"{num/1000:.{precision}f}K"
    elif num < 1000000000:
        return f"{num/1000000:.{precision}f}M"
    else:
        return f"{num/1000000000:.{precision}f}B"

def create_sidebar() -> Dict[str, Any]:
    """
    Create sidebar with controls.
    
    Returns:
        Dict: Selected options
    """
    st.sidebar.markdown("## 🎥 YouTube Data Analyzer")
    st.sidebar.markdown("---")
    
    # File uploader for API key
    api_key = st.sidebar.text_input(
        "YouTube API Key",
        type="password",
        help="Enter your YouTube Data API Key"
    )
    
    # Channel ID input
    channel_id = st.sidebar.text_input(
        "Channel ID",
        value=DEFAULT_CHANNEL_ID,
        help="Enter a YouTube Channel ID"
    )
    
    # Videos count slider
    videos_count = st.sidebar.slider(
        "Number of Videos to Analyze",
        min_value=10,
        max_value=500,
        value=DEFAULT_VIDEOS_COUNT,
        step=10,
        help="Select the number of videos to analyze"
    )
    
    # Date range slider
    date_range = st.sidebar.slider(
        "Date Range (days)",
        min_value=30,
        max_value=3650,
        value=DEFAULT_DATE_RANGE,
        step=30,
        help="Filter videos by publish date (in days from now)"
    )
    
    st.sidebar.markdown("---")
    
    # Fetch button
    fetch_button = st.sidebar.button(
        "Fetch New Data",
        type="primary",
        help="Click to fetch new data from YouTube"
    )
    
    # About section
    st.sidebar.markdown("---")
    st.sidebar.markdown("### About")
    st.sidebar.info(
        "📊 This dashboard analyzes YouTube channel performance using PySpark "
        "and visualizes the results with Streamlit."
    )
    
    st.sidebar.markdown("### Features")
    st.sidebar.markdown("- 📈 Channel growth analysis")
    st.sidebar.markdown("- 🏆 Top performing videos")
    st.sidebar.markdown("- 📆 Publishing patterns")
    st.sidebar.markdown("- ⏱ Video length analysis")
    st.sidebar.markdown("- 📚 Category performance")
    
    return {
        "api_key": api_key,
        "channel_id": channel_id,
        "videos_count": videos_count,
        "date_range": date_range,
        "fetch_button": fetch_button
    }

def fetch_and_process_data(api_key: str, channel_id: str, videos_count: int) -> bool:
    """
    Fetch and process YouTube data.
    
    Args:
        api_key: YouTube API key
        channel_id: YouTube channel ID
        videos_count: Number of videos to fetch
        
    Returns:
        bool: Success flag
    """
    try:
        with st.spinner("Fetching data from YouTube API..."):
            # Create fetcher with API key
            fetcher = YouTubeDataFetcher(api_key)
            
            # Fetch channel information
            channel_info = fetcher.get_channel_info(channel_id)
            
            # Fetch channel videos
            videos = fetcher.get_channel_videos(channel_id, videos_count)
            
            st.success(f"Successfully fetched data for channel: {channel_info['snippet']['title']}")
            
            # Fetch comments for each video (up to 20 videos to avoid API quota issues)
            with st.spinner("Fetching comments for videos..."):
                video_subset = videos[:min(20, len(videos))]
                for video in video_subset:
                    fetcher.get_video_comments(video['id'], 100)
            
            st.success(f"Successfully fetched comments")
        
        with st.spinner("Processing data with PySpark..."):
            # Transform data
            transformer = YouTubeDataTransformer()
            transformer.transform_and_save_data()
            
            st.success("Data processing complete!")
        
        return True
            
    except Exception as e:
        st.error(f"Error fetching/processing data: {str(e)}")
        return False

def display_channel_header(channel_summary: Dict[str, Any]):
    """
    Display channel header information.
    
    Args:
        channel_summary: Channel summary data
    """
    st.markdown(f"<h1 class='main-header'>📊 {channel_summary.get('title', 'YouTube Channel')} Analytics</h1>", unsafe_allow_html=True)
    
    # Create columns for metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(
            f"""
            <div class='metric-card'>