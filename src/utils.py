"""
Utility functions for the YouTube Data Analyzer application.
"""

import os
import json
import datetime
from typing import Dict, List, Any, Optional, Union

def save_json(data: Union[Dict, List], filename: str, directory: str) -> str:
    """
    Save data as a JSON file.
    
    Args:
        data: Data to save (dict or list)
        filename: Name of the file
        directory: Directory to save the file in
        
    Returns:
        str: Path to the saved file
    """
    # Ensure the directory exists
    os.makedirs(directory, exist_ok=True)
    
    # Create full file path
    filepath = os.path.join(directory, filename)
    
    # Save data as JSON
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    return filepath

def load_json(filepath: str) -> Union[Dict, List]:
    """
    Load data from a JSON file.
    
    Args:
        filepath: Path to the JSON file
        
    Returns:
        Union[Dict, List]: Loaded data
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return data

def format_date(date_string: str) -> datetime.datetime:
    """
    Convert YouTube API date format to datetime object.
    
    Args:
        date_string: Date string from YouTube API (ISO 8601 format)
        
    Returns:
        datetime.datetime: Converted datetime object
    """
    try:
        # Handle both date and datetime formats from YouTube API
        if 'T' in date_string:
            # Format: 2022-01-01T12:00:00Z
            return datetime.datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%SZ')
        else:
            # Format: 2022-01-01
            return datetime.datetime.strptime(date_string, '%Y-%m-%d')
    except ValueError:
        try:
            # Try alternative format with milliseconds
            return datetime.datetime.strptime(date_string.split('.')[0], '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            # Return current date if parsing fails
            print(f"Warning: Could not parse date '{date_string}'. Using current date instead.")
            return datetime.datetime.now()

def format_duration(duration: str) -> int:
    """
    Convert YouTube ISO 8601 duration format to seconds.
    Example: PT1H30M15S -> 5415 seconds
    
    Args:
        duration: Duration string in ISO 8601 format
        
    Returns:
        int: Duration in seconds
    """
    if not duration:
        return 0
        
    # Remove 'PT' prefix
    duration = duration[2:]
    
    # Initialize seconds
    seconds = 0
    
    # Extract hours
    if 'H' in duration:
        hours, duration = duration.split('H')
        seconds += int(hours) * 3600
    
    # Extract minutes
    if 'M' in duration:
        minutes, duration = duration.split('M')
        seconds += int(minutes) * 60
    
    # Extract seconds
    if 'S' in duration:
        s, duration = duration.split('S')
        seconds += int(s)
    
    return seconds

def format_number(number: Union[int, float], precision: int = 1) -> str:
    """
    Format large numbers for display (e.g., 1.5K, 2.6M).
    
    Args:
        number: Number to format
        precision: Number of decimal places
        
    Returns:
        str: Formatted number string
    """
    if number is None:
        return "0"
        
    if number < 1000:
        return str(number)
    elif number < 1000000:
        return f"{number/1000:.{precision}f}K"
    elif number < 1000000000:
        return f"{number/1000000:.{precision}f}M"
    else:
        return f"{number/1000000000:.{precision}f}B"

def get_video_category_name(category_id: str) -> str:
    """
    Map YouTube category ID to category name.
    
    Args:
        category_id: YouTube category ID
        
    Returns:
        str: Category name
    """
    categories = {
        "1": "Film & Animation",
        "2": "Autos & Vehicles",
        "10": "Music",
        "15": "Pets & Animals",
        "17": "Sports",
        "18": "Short Movies",
        "19": "Travel & Events",
        "20": "Gaming",
        "21": "Videoblogging",
        "22": "People & Blogs",
        "23": "Comedy",
        "24": "Entertainment",
        "25": "News & Politics",
        "26": "Howto & Style",
        "27": "Education",
        "28": "Science & Technology",
        "29": "Nonprofits & Activism",
        "30": "Movies",
        "31": "Anime/Animation",
        "32": "Action/Adventure",
        "33": "Classics",
        "34": "Comedy",
        "35": "Documentary",
        "36": "Drama",
        "37": "Family",
        "38": "Foreign",
        "39": "Horror",
        "40": "Sci-Fi/Fantasy",
        "41": "Thriller",
        "42": "Shorts",
        "43": "Shows",
        "44": "Trailers"
    }
    
    return categories.get(category_id, "Unknown")

def calculate_engagement_rate(likes: int, comments: int, views: int) -> float:
    """
    Calculate engagement rate for a video.
    
    Engagement Rate = (Likes + Comments) / Views * 100
    
    Args:
        likes: Number of likes
        comments: Number of comments
        views: Number of views
        
    Returns:
        float: Engagement rate percentage
    """
    if not views:
        return 0.0
    
    return (likes + comments) / views * 100