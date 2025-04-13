"""
Module for fetching data from YouTube Data API.
"""

import os
import json
import time
from typing import Dict, List, Optional, Any
from datetime import datetime

import googleapiclient.discovery
import googleapiclient.errors

from src.config import (
    YOUTUBE_API_KEY,
    YOUTUBE_API_SERVICE_NAME,
    YOUTUBE_API_VERSION,
    MAX_RESULTS_PER_PAGE,
    DATA_RAW_DIR,
    DEFAULT_CHANNEL_ID
)
from src.utils import save_json

class YouTubeDataFetcher:
    """Class to fetch data from YouTube Data API."""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the YouTube Data API client.
        
        Args:
            api_key: YouTube Data API key (defaults to config)
        """
        self.api_key = api_key or YOUTUBE_API_KEY
        
        if not self.api_key:
            raise ValueError("YouTube API key is required. Set it in .env file or pass it to the constructor.")
        
        # Initialize the YouTube API client
        self.youtube = googleapiclient.discovery.build(
            YOUTUBE_API_SERVICE_NAME,
            YOUTUBE_API_VERSION,
            developerKey=self.api_key
        )
    
    def get_channel_info(self, channel_id: str) -> Dict[str, Any]:
        """
        Fetch basic information about a YouTube channel.
        
        Args:
            channel_id: YouTube channel ID
            
        Returns:
            Dict: Channel information
        """
        request = self.youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
        
        response = request.execute()
        
        if not response.get('items'):
            raise ValueError(f"Channel with ID '{channel_id}' not found")
        
        # Save raw response
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_json(
            response,
            f"channel_{channel_id}_{timestamp}.json",
            DATA_RAW_DIR
        )
        
        return response['items'][0]
    
    def get_playlist_items(self, playlist_id: str, max_results: int = MAX_RESULTS_PER_PAGE) -> List[Dict[str, Any]]:
        """
        Fetch videos from a playlist (including channel uploads playlist).
        
        Args:
            playlist_id: YouTube playlist ID
            max_results: Maximum number of results to return
            
        Returns:
            List[Dict]: List of playlist items
        """
        items = []
        next_page_token = None
        
        # Calculate how many API calls we need
        num_calls = (max_results + MAX_RESULTS_PER_PAGE - 1) // MAX_RESULTS_PER_PAGE
        
        for _ in range(num_calls):
            request = self.youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=playlist_id,
                maxResults=min(MAX_RESULTS_PER_PAGE, max_results - len(items)),
                pageToken=next_page_token
            )
            
            response = request.execute()
            items.extend(response.get('items', []))
            
            next_page_token = response.get('nextPageToken')
            
            if not next_page_token or len(items) >= max_results:
                break
            
            # Sleep to avoid hitting API rate limits
            time.sleep(0.1)
        
        # Save raw response
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_json(
            {'items': items},
            f"playlist_{playlist_id}_{timestamp}.json",
            DATA_RAW_DIR
        )
        
        return items
    
    def get_videos_details(self, video_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch detailed information about specific videos.
        
        Args:
            video_ids: List of YouTube video IDs
            
        Returns:
            List[Dict]: List of video details
        """
        # YouTube API can only handle 50 video IDs at once
        items = []
        
        # Process videos in batches of 50
        for i in range(0, len(video_ids), 50):
            batch_ids = video_ids[i:i+50]
            
            request = self.youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=",".join(batch_ids)
            )
            
            response = request.execute()
            items.extend(response.get('items', []))
            
            # Sleep to avoid hitting API rate limits
            time.sleep(0.1)
        
        # Save raw response
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_json(
            {'items': items},
            f"videos_details_{timestamp}.json",
            DATA_RAW_DIR
        )
        
        return items
    
    def get_video_comments(self, video_id: str, max_results: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch comments for a specific video.
        
        Args:
            video_id: YouTube video ID
            max_results: Maximum number of comments to fetch
            
        Returns:
            List[Dict]: List of comments
        """
        items = []
        next_page_token = None
        
        # Calculate how many API calls we need
        num_calls = (max_results + MAX_RESULTS_PER_PAGE - 1) // MAX_RESULTS_PER_PAGE
        
        try:
            for _ in range(num_calls):
                request = self.youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=min(MAX_RESULTS_PER_PAGE, max_results - len(items)),
                    pageToken=next_page_token
                )
                
                response = request.execute()
                items.extend(response.get('items', []))
                
                next_page_token = response.get('nextPageToken')
                
                if not next_page_token or len(items) >= max_results:
                    break
                
                # Sleep to avoid hitting API rate limits
                time.sleep(0.1)
        except googleapiclient.errors.HttpError as e:
            # Handle cases where comments are disabled
            print(f"Could not fetch comments for video {video_id}: {e}")
        
        # Save raw response
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_json(
            {'items': items},
            f"comments_{video_id}_{timestamp}.json",
            DATA_RAW_DIR
        )
        
        return items
    
    def get_channel_videos(self, channel_id: str, max_results: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch videos from a channel and their detailed information.
        
        Args:
            channel_id: YouTube channel ID
            max_results: Maximum number of videos to fetch
            
        Returns:
            List[Dict]: List of video details
        """
        # First, get channel information to extract uploads playlist ID
        channel_info = self.get_channel_info(channel_id)
        uploads_playlist_id = channel_info['contentDetails']['relatedPlaylists']['uploads']
        
        # Then, get playlist items (videos)
        playlist_items = self.get_playlist_items(uploads_playlist_id, max_results)
        
        # Extract video IDs from playlist items
        video_ids = [item['contentDetails']['videoId'] for item in playlist_items]
        
        # Finally, get detailed information for each video
        videos = self.get_videos_details(video_ids)
        
        return videos

def main():
    """Main function to test the YouTube data fetcher."""
    try:
        # Create a YouTubeDataFetcher instance
        fetcher = YouTubeDataFetcher()
        
        # Get channel ID (use default or specified)
        channel_id = os.getenv("CHANNEL_ID") or DEFAULT_CHANNEL_ID
        
        print(f"Fetching data for channel: {channel_id}")
        
        # Get channel information
        channel_info = fetcher.get_channel_info(channel_id)
        print(f"Channel Title: {channel_info['snippet']['title']}")
        print(f"Subscriber Count: {channel_info['statistics']['subscriberCount']}")
        
        # Get channel videos
        videos_count = int(os.getenv("VIDEOS_COUNT", 10))
        print(f"Fetching {videos_count} videos...")
        
        videos = fetcher.get_channel_videos(channel_id, videos_count)
        
        print(f"Fetched {len(videos)} videos")
        
        # Print some information about the first video
        if videos:
            first_video = videos[0]
            print("\nFirst Video:")
            print(f"Title: {first_video['snippet']['title']}")
            print(f"Views: {first_video['statistics'].get('viewCount', 'N/A')}")
            print(f"Likes: {first_video['statistics'].get('likeCount', 'N/A')}")
            
            # Get comments for the first video
            video_id = first_video['id']
            comments = fetcher.get_video_comments(video_id, 10)
            
            print(f"\nFetched {len(comments)} comments")
            
            # Print the first comment if available
            if comments:
                first_comment = comments[0]['snippet']['topLevelComment']['snippet']
                print("\nFirst Comment:")
                print(f"Author: {first_comment['authorDisplayName']}")
                print(f"Text: {first_comment['textDisplay']}")
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()