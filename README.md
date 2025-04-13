# YouTube Data Analyzer

An advanced analytics tool for YouTube channels and videos using PySpark and Streamlit.

## Overview

This application fetches data from the YouTube Data API, processes it using PySpark, and visualizes insights through an interactive Streamlit dashboard. It provides comprehensive analytics to understand channel performance, audience engagement, and content optimization opportunities.

## Features

- **Channel Analytics**: View subscriber growth, total views, and overall channel performance.
- **Video Performance**: Analyze top-performing videos by views, likes, comments, and engagement rate.
- **Publishing Patterns**: Discover optimal posting schedules by analyzing performance by day of week and time of day.
- **Content Analysis**: Understand which video categories, lengths, and topics perform best.
- **Audience Engagement**: Track comment sentiment, top commenters, and engagement trends over time.
- **Custom Dashboards**: Filter data by date ranges and specific metrics for tailored insights.

## Project Structure

```
youtube-data-analyzer/
├── .github/workflows/    # CI/CD workflows
├── data/                 # Data storage
│   ├── raw/              # Raw JSON data from YouTube API
│   └── processed/        # Processed Parquet files
├── notebooks/            # Jupyter notebooks for exploration
├── src/                  # Source code
│   ├── __init__.py
│   ├── analyze_data.py   # PySpark analysis module
│   ├── config.py         # Configuration settings
│   ├── dashboard.py      # Streamlit dashboard
│   ├── fetch_data.py     # YouTube API data fetcher
│   ├── transform_data.py # Data transformation with PySpark
│   └── utils.py          # Utility functions
└── tests/                # Test modules
```

## Requirements

- Python 3.8+
- PySpark 3.4.0
- Streamlit 1.25.0
- YouTube Data API key
- Other dependencies listed in requirements.txt

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/youtube-data-analyzer.git
   cd youtube-data-analyzer
   ```

2. Create a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Create a `.env` file with your YouTube API key:
   ```
   YOUTUBE_API_KEY=your_api_key_here
   ```

## Usage

1. Run the Streamlit dashboard:
   ```
   streamlit run src/dashboard.py
   ```

2. Enter your YouTube API key and a channel ID to analyze.

3. Click "Fetch New Data" to retrieve the latest data from YouTube.

4. Explore the analytics dashboard.

## Data Processing Pipeline

1. **Data Fetching**: The application fetches channel information, videos, and comments from the YouTube Data API.

2. **Data Transformation**: Raw JSON data is transformed using PySpark into a structured format.

3. **Data Analysis**: PySpark performs complex analytics on the processed data.

4. **Visualization**: Streamlit displays the insights through interactive charts and metrics.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.