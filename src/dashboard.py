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
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud

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
        return str(int(num) if isinstance(num, int) or num.is_integer() else round(num, precision))
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
                <div class='metric-value'>{format_number(channel_summary.get('subscriber_count', 0))}</div>
                <div class='metric-label'>Subscribers</div>
            </div>
            """, 
            unsafe_allow_html=True
        )
    
    with col2:
        st.markdown(
            f"""
            <div class='metric-card'>
                <div class='metric-value'>{format_number(channel_summary.get('view_count', 0))}</div>
                <div class='metric-label'>Total Views</div>
            </div>
            """, 
            unsafe_allow_html=True
        )
    
    with col3:
        st.markdown(
            f"""
            <div class='metric-card'>
                <div class='metric-value'>{format_number(channel_summary.get('video_count', 0))}</div>
                <div class='metric-label'>Total Videos</div>
            </div>
            """, 
            unsafe_allow_html=True
        )
    
    with col4:
        # Calculate average views per video
        if channel_summary.get('video_count', 0) > 0:
            avg_views = channel_summary.get('view_count', 0) / channel_summary.get('video_count', 1)
        else:
            avg_views = 0
            
        st.markdown(
            f"""
            <div class='metric-card'>
                <div class='metric-value'>{format_number(avg_views)}</div>
                <div class='metric-label'>Avg. Views per Video</div>
            </div>
            """, 
            unsafe_allow_html=True
        )

def plot_top_videos(top_videos: List[Dict[str, Any]], title: str):
    """
    Plot top videos by a specific metric.
    
    Args:
        top_videos: List of top videos
        title: Title of the chart
    """
    if not top_videos:
        st.info("No video data available.")
        return
    
    # Extract data for the chart
    titles = [video.get('title', 'Unknown')[:40] + ('...' if len(video.get('title', '')) > 40 else '') for video in top_videos]
    values = [video.get('view_count', 0) if 'view' in title.lower() else 
              video.get('like_count', 0) if 'like' in title.lower() else
              video.get('comment_count', 0) if 'comment' in title.lower() else
              video.get('engagement_rate', 0) for video in top_videos]
    
    # Create horizontal bar chart
    fig = px.bar(
        x=values, 
        y=titles,
        orientation='h',
        title=title,
        labels={'x': title.split('by ')[1] if 'by ' in title else 'Value', 'y': 'Video Title'},
        color=values,
        color_continuous_scale='Reds'
    )
    
    # Set layout properties
    fig.update_layout(
        height=400,
        xaxis_title=title.split('by ')[1] if 'by ' in title else 'Value',
        yaxis_title='',
        yaxis=dict(autorange="reversed"),  # Reverse y-axis to show highest value at the top
        coloraxis_showscale=False
    )
    
    st.plotly_chart(fig, use_container_width=True)

def plot_performance_by_day(performance_by_day: List[Dict[str, Any]]):
    """
    Plot video performance by day of week.
    
    Args:
        performance_by_day: Performance metrics by day
    """
    if not performance_by_day:
        st.info("No performance by day data available.")
        return
        
    # Create DataFrame from the data
    df = pd.DataFrame(performance_by_day)
    
    # Sort by day of week
    day_order = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    df['day_of_week'] = pd.Categorical(df['day_of_week'], categories=day_order, ordered=True)
    df = df.sort_values('day_of_week')
    
    # Create subplots: 2 rows, 2 columns
    fig = make_subplots(
        rows=2, 
        cols=2,
        subplot_titles=(
            'Average Views by Day of Week', 
            'Average Likes by Day of Week', 
            'Average Comments by Day of Week', 
            'Average Engagement by Day of Week'
        ),
        vertical_spacing=0.15
    )
    
    # Add traces for each metric
    fig.add_trace(
        go.Bar(x=df['day_of_week'], y=df['avg_views'], name='Avg Views', marker_color='#FF0000'),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Bar(x=df['day_of_week'], y=df['avg_likes'], name='Avg Likes', marker_color='#0099FF'),
        row=1, col=2
    )
    
    fig.add_trace(
        go.Bar(x=df['day_of_week'], y=df['avg_comments'], name='Avg Comments', marker_color='#00CC00'),
        row=2, col=1
    )
    
    fig.add_trace(
        go.Bar(x=df['day_of_week'], y=df['avg_engagement'], name='Avg Engagement', marker_color='#FF9900'),
        row=2, col=2
    )
    
    # Update layout
    fig.update_layout(
        height=600,
        showlegend=False,
        title='Video Performance by Day of Week'
    )
    
    # Update x-axis and y-axis titles
    fig.update_xaxes(title_text='Day of Week')
    fig.update_yaxes(title_text='Average Value')
    
    st.plotly_chart(fig, use_container_width=True)

def plot_performance_by_month(performance_by_month: List[Dict[str, Any]]):
    """
    Plot video performance by month.
    
    Args:
        performance_by_month: Performance metrics by month
    """
    if not performance_by_month:
        st.info("No performance by month data available.")
        return
        
    # Create DataFrame from the data
    df = pd.DataFrame(performance_by_month)
    
    # Ensure data is sorted by date
    df = df.sort_values('year_month')
    
    # Create figure
    fig = go.Figure()
    
    # Add traces for different metrics
    fig.add_trace(
        go.Scatter(
            x=df['year_month'], 
            y=df['avg_views'], 
            mode='lines+markers', 
            name='Avg Views',
            line=dict(color='#FF0000', width=2)
        )
    )
    
    fig.add_trace(
        go.Scatter(
            x=df['year_month'], 
            y=df['avg_likes'], 
            mode='lines+markers', 
            name='Avg Likes',
            line=dict(color='#0099FF', width=2)
        )
    )
    
    fig.add_trace(
        go.Scatter(
            x=df['year_month'], 
            y=df['avg_comments'], 
            mode='lines+markers', 
            name='Avg Comments',
            line=dict(color='#00CC00', width=2)
        )
    )
    
    fig.add_trace(
        go.Scatter(
            x=df['year_month'], 
            y=df['avg_engagement'], 
            mode='lines+markers', 
            name='Avg Engagement (%)',
            line=dict(color='#FF9900', width=2),
            yaxis='y2'  # Use secondary y-axis
        )
    )
    
    # Update layout
    fig.update_layout(
        title='Video Performance by Month',
        xaxis=dict(title='Month'),
        yaxis=dict(
            title='Average Count',
            side='left'
        ),
        yaxis2=dict(
            title='Average Engagement (%)',
            side='right',
            overlaying='y',
            showgrid=False
        ),
        height=500,
        hovermode='x unified',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='center',
            x=0.5
        )
    )
    
    st.plotly_chart(fig, use_container_width=True)

def plot_category_performance(category_performance: List[Dict[str, Any]]):
    """
    Plot video performance by category.
    
    Args:
        category_performance: Performance metrics by category
    """
    if not category_performance:
        st.info("No category performance data available.")
        return
        
    # Create DataFrame from the data
    df = pd.DataFrame(category_performance)
    
    # Sort by total views
    df = df.sort_values('total_views', ascending=False)
    
    # Select top 10 categories by total views
    top_categories = df.head(10)
    
    # Create figure with subplots: 1 row, 2 columns
    fig = make_subplots(
        rows=1, 
        cols=2,
        subplot_titles=(
            'Total Views by Category', 
            'Average Engagement by Category'
        ),
        specs=[[{"type": "bar"}, {"type": "bar"}]],
        horizontal_spacing=0.12
    )
    
    # Add traces for total views and engagement rate
    fig.add_trace(
        go.Bar(
            x=top_categories['category_name'], 
            y=top_categories['total_views'], 
            name='Total Views',
            marker_color='#FF0000'
        ),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Bar(
            x=top_categories['category_name'], 
            y=top_categories['avg_engagement'], 
            name='Avg Engagement (%)',
            marker_color='#0099FF'
        ),
        row=1, col=2
    )
    
    # Update layout
    fig.update_layout(
        height=500,
        title='Category Performance',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='center',
            x=0.5
        )
    )
    
    # Update x-axis
    fig.update_xaxes(
        tickangle=45,
        title_text='Category'
    )
    
    # Update y-axis titles
    fig.update_yaxes(title_text='Total Views', row=1, col=1)
    fig.update_yaxes(title_text='Average Engagement (%)', row=1, col=2)
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Display data table for all categories
    with st.expander("View Complete Category Data"):
        st.dataframe(df, use_container_width=True)

def plot_video_length_performance(length_performance: List[Dict[str, Any]]):
    """
    Plot video performance by video length.
    
    Args:
        length_performance: Performance metrics by video length
    """
    if not length_performance:
        st.info("No video length performance data available.")
        return
        
    # Create DataFrame from the data
    df = pd.DataFrame(length_performance)
    
    # Define category order
    duration_order = ['< 1 min', '1-5 mins', '5-10 mins', '10-20 mins', '> 20 mins']
    df['duration_category'] = pd.Categorical(
        df['duration_category'], 
        categories=duration_order, 
        ordered=True
    )
    
    # Sort by duration category
    df = df.sort_values('duration_category')
    
    # Create figure with subplots: 2 rows, 2 columns
    fig = make_subplots(
        rows=2, 
        cols=2,
        subplot_titles=(
            'Number of Videos by Duration', 
            'Average Views by Duration', 
            'Average Likes by Duration', 
            'Average Engagement by Duration'
        ),
        vertical_spacing=0.15
    )
    
    # Add traces for different metrics
    fig.add_trace(
        go.Bar(
            x=df['duration_category'], 
            y=df['video_count'], 
            name='Video Count',
            marker_color='#FF0000'
        ),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Bar(
            x=df['duration_category'], 
            y=df['avg_views'], 
            name='Avg Views',
            marker_color='#0099FF'
        ),
        row=1, col=2
    )
    
    fig.add_trace(
        go.Bar(
            x=df['duration_category'], 
            y=df['avg_likes'], 
            name='Avg Likes',
            marker_color='#00CC00'
        ),
        row=2, col=1
    )
    
    fig.add_trace(
        go.Bar(
            x=df['duration_category'], 
            y=df['avg_engagement'], 
            name='Avg Engagement',
            marker_color='#FF9900'
        ),
        row=2, col=2
    )
    
    # Update layout
    fig.update_layout(
        height=600,
        showlegend=False,
        title='Video Performance by Length'
    )
    
    # Update x-axis and y-axis titles
    fig.update_xaxes(title_text='Video Duration')
    fig.update_yaxes(title_text='Value')
    
    st.plotly_chart(fig, use_container_width=True)

def plot_channel_growth(channel_growth: List[Dict[str, Any]]):
    """
    Plot channel growth over time.
    
    Args:
        channel_growth: Channel growth metrics by month
    """
    if not channel_growth:
        st.info("No channel growth data available.")
        return
        
    # Create DataFrame from the data
    df = pd.DataFrame(channel_growth)
    
    # Ensure data is sorted by date
    df = df.sort_values('year_month')
    
    # Create figure with secondary y-axis
    fig = go.Figure()
    
    # Add traces for cumulative metrics (total videos and total views)
    fig.add_trace(
        go.Scatter(
            x=df['year_month'], 
            y=df['total_videos'], 
            mode='lines+markers', 
            name='Total Videos',
            line=dict(color='#FF0000', width=2)
        )
    )
    
    fig.add_trace(
        go.Scatter(
            x=df['year_month'], 
            y=df['total_views'], 
            mode='lines+markers', 
            name='Total Views',
            line=dict(color='#0099FF', width=2),
            yaxis='y2'  # Use secondary y-axis
        )
    )
    
    # Add bar chart for monthly published videos
    fig.add_trace(
        go.Bar(
            x=df['year_month'], 
            y=df['videos_published'], 
            name='Videos Published',
            marker_color='rgba(255, 0, 0, 0.3)',
            opacity=0.7
        )
    )
    
    # Update layout
    fig.update_layout(
        title='Channel Growth Over Time',
        xaxis=dict(title='Month'),
        yaxis=dict(
            title='Videos Count',
            side='left'
        ),
        yaxis2=dict(
            title='Views Count',
            side='right',
            overlaying='y',
            showgrid=False
        ),
        height=500,
        hovermode='x unified',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='center',
            x=0.5
        )
    )
    
    st.plotly_chart(fig, use_container_width=True)

def plot_keyword_performance(keyword_performance: List[Dict[str, Any]]):
    """
    Plot performance of videos with specific keywords.
    
    Args:
        keyword_performance: Performance metrics by keyword
    """
    if not keyword_performance:
        st.info("No keyword performance data available.")
        return
        
    # Create DataFrame from the data
    df = pd.DataFrame(keyword_performance)
    
    # Sort by video count
    df = df.sort_values('video_count', ascending=False)
    
    # Create figure with subplots
    fig = make_subplots(
        rows=2, 
        cols=2,
        subplot_titles=(
            'Video Count by Keyword', 
            'Average Views by Keyword', 
            'Average Engagement by Keyword', 
            'Average Likes by Keyword'
        ),
        vertical_spacing=0.15
    )
    
    # Add traces for different metrics
    fig.add_trace(
        go.Bar(x=df['keyword'], y=df['video_count'], name='Video Count', marker_color='#FF0000'),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Bar(x=df['keyword'], y=df['avg_views'], name='Avg Views', marker_color='#0099FF'),
        row=1, col=2
    )
    
    fig.add_trace(
        go.Bar(x=df['keyword'], y=df['avg_engagement'], name='Avg Engagement', marker_color='#00CC00'),
        row=2, col=1
    )
    
    fig.add_trace(
        go.Bar(x=df['keyword'], y=df['avg_likes'], name='Avg Likes', marker_color='#FF9900'),
        row=2, col=2
    )
    
    # Update layout
    fig.update_layout(
        height=600,
        showlegend=False,
        title='Video Performance by Keyword'
    )
    
    # Update x-axis and y-axis titles
    fig.update_xaxes(title_text='Keyword')
    fig.update_yaxes(title_text='Value')
    
    st.plotly_chart(fig, use_container_width=True)

def display_top_commenters(top_commenters: List[Dict[str, Any]]):
    """
    Display top commenters.
    
    Args:
        top_commenters: List of top commenters
    """
    if not top_commenters:
        st.info("No commenter data available.")
        return
    
    st.markdown("<h3 class='sub-header'>Top Commenters</h3>", unsafe_allow_html=True)
    
    # Create DataFrame from the data
    df = pd.DataFrame(top_commenters)
    
    # Sort by comment count
    df = df.sort_values('comment_count', ascending=False)
    
    # Display table with top commenters
    st.dataframe(
        df.assign(
            comment_count=df['comment_count'],
            total_likes=df['total_likes']
        ),
        column_config={
            "author_name": "Commenter Name",
            "comment_count": "Number of Comments",
            "total_likes": "Total Likes Received"
        },
        use_container_width=True,
        hide_index=True
    )

def create_dashboard_tabs(results: Dict[str, Any]):
    """
    Create dashboard tabs with visualizations.
    
    Args:
        results: Analysis results
    """
    # Create tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📈 Overview", 
        "🏆 Top Videos", 
        "📊 Video Performance", 
        "📆 Publishing Patterns", 
        "🔍 Content Analysis",
        "💬 Comments"
    ])
    
    # Tab 1: Overview
    with tab1:
        st.markdown("<h3 class='sub-header'>Channel Growth</h3>", unsafe_allow_html=True)
        plot_channel_growth(results.get('channel_growth', []))
        
        st.markdown("<h3 class='sub-header'>Engagement Metrics</h3>", unsafe_allow_html=True)
        
        # Display engagement metrics in columns
        metrics = results.get('engagement_metrics', {})
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Average Views", 
                format_number(metrics.get('avg_views', 0)),
                delta=None
            )
        
        with col2:
            st.metric(
                "Average Likes", 
                format_number(metrics.get('avg_likes', 0)),
                delta=None
            )
        
        with col3:
            st.metric(
                "Average Comments", 
                format_number(metrics.get('avg_comments', 0)),
                delta=None
            )
        
        with col4:
            st.metric(
                "Average Engagement", 
                format_number(metrics.get('avg_engagement', 0)) + "%",
                delta=None
            )
            
        # Display video length performance
        st.markdown("<h3 class='sub-header'>Video Length Performance</h3>", unsafe_allow_html=True)
        plot_video_length_performance(results.get('length_performance', []))
    
    # Tab 2: Top Videos
    with tab2:
        st.markdown("<h3 class='sub-header'>Top Videos by Views</h3>", unsafe_allow_html=True)
        plot_top_videos(results.get('top_videos_by_views', []), "Top Videos by Views")
        
        st.markdown("<h3 class='sub-header'>Top Videos by Likes</h3>", unsafe_allow_html=True)
        plot_top_videos(results.get('top_videos_by_likes', []), "Top Videos by Likes")
        
        st.markdown("<h3 class='sub-header'>Top Videos by Comments</h3>", unsafe_allow_html=True)
        plot_top_videos(results.get('top_videos_by_comments', []), "Top Videos by Comments")
        
        st.markdown("<h3 class='sub-header'>Top Videos by Engagement</h3>", unsafe_allow_html=True)
        plot_top_videos(results.get('top_videos_by_engagement', []), "Top Videos by Engagement")
    
    # Tab 3: Video Performance
    with tab3:
        st.markdown("<h3 class='sub-header'>Category Performance</h3>", unsafe_allow_html=True