"""
Module for analyzing YouTube data using PySpark.
"""

import os
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, desc, asc, count, avg, sum as spark_sum, 
    min as spark_min, max as spark_max, 
    month, year, dayofweek, date_format,
    datediff, current_date, to_date, 
    when, round as spark_round, lit,
    expr, percentile_approx
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, LongType, DoubleType, 
    ArrayType, TimestampType, DateType
)

from src.config import DATA_PROCESSED_DIR

class YouTubeDataAnalyzer:
    """Class to analyze YouTube data using PySpark."""
    
    def __init__(self):
        """Initialize the Spark session."""
        self.spark = SparkSession.builder \
            .appName("YouTubeDataAnalyzer") \
            .config("spark.driver.memory", os.getenv("PYSPARK_DRIVER_MEMORY", "4g")) \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
    
    def load_data(self) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Load processed data.
        
        Returns:
            Tuple: (channel_df, videos_df, comments_df)
        """
        # Load channel data
        channel_df = self.spark.read.parquet(os.path.join(DATA_PROCESSED_DIR, "channel"))
        
        # Load video data
        videos_df = self.spark.read.parquet(os.path.join(DATA_PROCESSED_DIR, "videos"))
        
        # Load comments data if available
        comments_path = os.path.join(DATA_PROCESSED_DIR, "comments")
        if os.path.exists(comments_path):
            comments_df = self.spark.read.parquet(comments_path)
        else:
            # Create empty DataFrame with expected schema
            schema = StructType([
                StructField("comment_id", StringType(), True),
                StructField("video_id", StringType(), True),
                StructField("author_name", StringType(), True),
                StructField("author_channel_id", StringType(), True),
                StructField("text", StringType(), True),
                StructField("published_at", TimestampType(), True),
                StructField("updated_at", TimestampType(), True),
                StructField("like_count", IntegerType(), True)
            ])
            comments_df = self.spark.createDataFrame([], schema)
        
        return channel_df, videos_df, comments_df
    
    def get_channel_summary(self, channel_df: DataFrame) -> Dict[str, Any]:
        """
        Get summary statistics for the channel.
        
        Args:
            channel_df: Channel DataFrame
            
        Returns:
            Dict: Channel summary
        """
        # Collect channel data
        channel_data = channel_df.collect()
        
        if not channel_data:
            return {}
        
        # Get the first row (should only be one channel)
        channel = channel_data[0]
        
        return {
            "channel_id": channel["channel_id"],
            "title": channel["channel_title"],
            "description": channel["channel_description"],
            "custom_url": channel["custom_url"],
            "published_at": channel["published_at"],
            "country": channel["country"],
            "view_count": channel["view_count"],
            "subscriber_count": channel["subscriber_count"],
            "video_count": channel["video_count"]
        }
    
    def get_top_videos(self, videos_df: DataFrame, metric: str = "view_count", limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get top videos based on a specific metric.
        
        Args:
            videos_df: Videos DataFrame
            metric: Metric to sort by (view_count, like_count, comment_count, engagement_rate)
            limit: Number of videos to return
            
        Returns:
            List[Dict]: Top videos
        """
        # Sort videos by the specified metric
        top_videos_df = videos_df.orderBy(desc(metric)).limit(limit)
        
        # Convert to list of dictionaries
        return [row.asDict() for row in top_videos_df.collect()]
    
    def get_video_performance_by_day(self, videos_df: DataFrame) -> List[Dict[str, Any]]:
        """
        Analyze video performance by day of week.
        
        Args:
            videos_df: Videos DataFrame
            
        Returns:
            List[Dict]: Performance metrics by day
        """
        performance_by_day = videos_df \
            .withColumn("day_of_week", date_format(col("publish_date"), "E")) \
            .groupBy("day_of_week") \
            .agg(
                count("*").alias("video_count"),
                spark_round(avg("view_count"), 2).alias("avg_views"),
                spark_round(avg("like_count"), 2).alias("avg_likes"),
                spark_round(avg("comment_count"), 2).alias("avg_comments"),
                spark_round(avg("engagement_rate"), 2).alias("avg_engagement")
            ) \
            .orderBy(
                expr("""
                    CASE day_of_week
                        WHEN 'Mon' THEN 1
                        WHEN 'Tue' THEN 2
                        WHEN 'Wed' THEN 3
                        WHEN 'Thu' THEN 4
                        WHEN 'Fri' THEN 5
                        WHEN 'Sat' THEN 6
                        WHEN 'Sun' THEN 7
                    END
                """)
            )
        
        # Convert to list of dictionaries
        return [row.asDict() for row in performance_by_day.collect()]
    
    def get_video_performance_by_month(self, videos_df: DataFrame) -> List[Dict[str, Any]]:
        """
        Analyze video performance by month.
        
        Args:
            videos_df: Videos DataFrame
            
        Returns:
            List[Dict]: Performance metrics by month
        """
        performance_by_month = videos_df \
            .withColumn("year_month", date_format(col("publish_date"), "yyyy-MM")) \
            .groupBy("year_month") \
            .agg(
                count("*").alias("video_count"),
                spark_round(avg("view_count"), 2).alias("avg_views"),
                spark_round(avg("like_count"), 2).alias("avg_likes"),
                spark_round(avg("comment_count"), 2).alias("avg_comments"),
                spark_round(avg("engagement_rate"), 2).alias("avg_engagement")
            ) \
            .orderBy("year_month")
        
        # Convert to list of dictionaries
        return [row.asDict() for row in performance_by_month.collect()]
    
    def get_category_performance(self, videos_df: DataFrame) -> List[Dict[str, Any]]:
        """
        Analyze video performance by category.
        
        Args:
            videos_df: Videos DataFrame
            
        Returns:
            List[Dict]: Performance metrics by category
        """
        category_performance = videos_df \
            .groupBy("category_name") \
            .agg(
                count("*").alias("video_count"),
                spark_round(avg("view_count"), 2).alias("avg_views"),
                spark_round(avg("like_count"), 2).alias("avg_likes"),
                spark_round(avg("comment_count"), 2).alias("avg_comments"),
                spark_round(avg("engagement_rate"), 2).alias("avg_engagement"),
                spark_round(spark_sum("view_count"), 2).alias("total_views")
            ) \
            .orderBy(desc("total_views"))
        
        # Convert to list of dictionaries
        return [row.asDict() for row in category_performance.collect()]
    
    def get_video_length_performance(self, videos_df: DataFrame) -> List[Dict[str, Any]]:
        """
        Analyze video performance by video length.
        
        Args:
            videos_df: Videos DataFrame
            
        Returns:
            List[Dict]: Performance metrics by video length category
        """
        # Create duration categories
        duration_categories_df = videos_df \
            .withColumn(
                "duration_category", 
                when(col("duration_seconds") < 60, "< 1 min")
                .when(col("duration_seconds") < 300, "1-5 mins")
                .when(col("duration_seconds") < 600, "5-10 mins")
                .when(col("duration_seconds") < 1200, "10-20 mins")
                .otherwise("> 20 mins")
            )
        
        # Calculate performance by duration category
        length_performance = duration_categories_df \
            .groupBy("duration_category") \
            .agg(
                count("*").alias("video_count"),
                spark_round(avg("view_count"), 2).alias("avg_views"),
                spark_round(avg("like_count"), 2).alias("avg_likes"),
                spark_round(avg("comment_count"), 2).alias("avg_comments"),
                spark_round(avg("engagement_rate"), 2).alias("avg_engagement")
            ) \
            .orderBy(
                expr("""
                    CASE duration_category
                        WHEN '< 1 min' THEN 1
                        WHEN '1-5 mins' THEN 2
                        WHEN '5-10 mins' THEN 3
                        WHEN '10-20 mins' THEN 4
                        WHEN '> 20 mins' THEN 5
                    END
                """)
            )
        
        # Convert to list of dictionaries
        return [row.asDict() for row in length_performance.collect()]
    
    def get_channel_growth(self, videos_df: DataFrame) -> List[Dict[str, Any]]:
        """
        Analyze channel growth over time.
        
        Args:
            videos_df: Videos DataFrame
            
        Returns:
            List[Dict]: Channel growth metrics by month
        """
        # Calculate cumulative views and video count by month
        month_window = Window.orderBy("year_month")
        
        channel_growth = videos_df \
            .withColumn("year_month", date_format(col("publish_date"), "yyyy-MM")) \
            .groupBy("year_month") \
            .agg(
                count("*").alias("videos_published"),
                spark_sum("view_count").alias("views_in_month"),
                spark_sum("like_count").alias("likes_in_month"),
                spark_sum("comment_count").alias("comments_in_month")
            ) \
            .withColumn("total_videos", spark_sum("videos_published").over(month_window)) \
            .withColumn("total_views", spark_sum("views_in_month").over(month_window)) \
            .withColumn("total_likes", spark_sum("likes_in_month").over(month_window)) \
            .withColumn("total_comments", spark_sum("comments_in_month").over(month_window)) \
            .orderBy("year_month")
        
        # Convert to list of dictionaries
        return [row.asDict() for row in channel_growth.collect()]
    
    def get_engagement_metrics(self, videos_df: DataFrame) -> Dict[str, Any]:
        """
        Calculate overall engagement metrics.
        
        Args:
            videos_df: Videos DataFrame
            
        Returns:
            Dict: Engagement metrics
        """
        metrics_df = videos_df.agg(
            spark_round(avg("view_count"), 2).alias("avg_views"),
            spark_round(avg("like_count"), 2).alias("avg_likes"),
            spark_round(avg("comment_count"), 2).alias("avg_comments"),
            spark_round(avg("engagement_rate"), 2).alias("avg_engagement"),
            
            spark_round(percentile_approx("view_count", 0.5), 2).alias("median_views"),
            spark_round(percentile_approx("like_count", 0.5), 2).alias("median_likes"),
            spark_round(percentile_approx("comment_count", 0.5), 2).alias("median_comments"),
            spark_round(percentile_approx("engagement_rate", 0.5), 2).alias("median_engagement"),
            
            spark_sum("view_count").alias("total_views"),
            spark_sum("like_count").alias("total_likes"),
            spark_sum("comment_count").alias("total_comments")
        )
        
        # Convert to dictionary
        return metrics_df.collect()[0].asDict()
    
    def get_top_commenters(self, comments_df: DataFrame, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get top commenters.
        
        Args:
            comments_df: Comments DataFrame
            limit: Number of commenters to return
            
        Returns:
            List[Dict]: Top commenters
        """
        if comments_df.count() == 0:
            return []
        
        top_commenters = comments_df \
            .groupBy("author_name", "author_channel_id") \
            .agg(
                count("*").alias("comment_count"),
                spark_sum("like_count").alias("total_likes")
            ) \
            .orderBy(desc("comment_count")) \
            .limit(limit)
        
        # Convert to list of dictionaries
        return [row.asDict() for row in top_commenters.collect()]
    
    def get_keyword_performance(self, videos_df: DataFrame, keywords: List[str]) -> List[Dict[str, Any]]:
        """
        Analyze performance of videos containing specific keywords in title.
        
        Args:
            videos_df: Videos DataFrame
            keywords: List of keywords to analyze
            
        Returns:
            List[Dict]: Performance metrics by keyword
        """
        results = []
        
        for keyword in keywords:
            # Filter videos containing the keyword in title (case insensitive)
            keyword_videos = videos_df.filter(
                col("title").contains(keyword) | 
                col("description").contains(keyword)
            )
            
            # Count number of matching videos
            video_count = keyword_videos.count()
            
            if video_count > 0:
                # Calculate metrics
                metrics = keyword_videos.agg(
                    spark_round(avg("view_count"), 2).alias("avg_views"),
                    spark_round(avg("like_count"), 2).alias("avg_likes"),
                    spark_round(avg("comment_count"), 2).alias("avg_comments"),
                    spark_round(avg("engagement_rate"), 2).alias("avg_engagement")
                ).collect()[0]
                
                results.append({
                    "keyword": keyword,
                    "video_count": video_count,
                    "avg_views": metrics["avg_views"],
                    "avg_likes": metrics["avg_likes"],
                    "avg_comments": metrics["avg_comments"],
                    "avg_engagement": metrics["avg_engagement"]
                })
        
        # Sort by video count
        results.sort(key=lambda x: x["video_count"], reverse=True)
        
        return results
    
    def perform_analysis(self) -> Dict[str, Any]:
        """
        Perform complete analysis on the YouTube data.
        
        Returns:
            Dict: Complete analysis results
        """
        # Load the data
        channel_df, videos_df, comments_df = self.load_data()
        
        # Initialize results dictionary
        results = {}
        
        # Get channel summary
        results["channel_summary"] = self.get_channel_summary(channel_df)
        
        # Get top videos by different metrics
        results["top_videos_by_views"] = self.get_top_videos(videos_df, "view_count")
        results["top_videos_by_likes"] = self.get_top_videos(videos_df, "like_count")
        results["top_videos_by_comments"] = self.get_top_videos(videos_df, "comment_count")
        results["top_videos_by_engagement"] = self.get_top_videos(videos_df, "engagement_rate")
        
        # Get performance by day, month, category, and video length
        results["performance_by_day"] = self.get_video_performance_by_day(videos_df)
        results["performance_by_month"] = self.get_video_performance_by_month(videos_df)
        results["category_performance"] = self.get_category_performance(videos_df)
        results["length_performance"] = self.get_video_length_performance(videos_df)
        
        # Get channel growth
        results["channel_growth"] = self.get_channel_growth(videos_df)
        
        # Get engagement metrics
        results["engagement_metrics"] = self.get_engagement_metrics(videos_df)
        
        # Get top commenters
        results["top_commenters"] = self.get_top_commenters(comments_df)
        
        # Get keyword performance (customize the keywords as needed)
        common_keywords = ["how to", "tutorial", "review", "guide", "tips", "introduction"]
        results["keyword_performance"] = self.get_keyword_performance(videos_df, common_keywords)
        
        return results

def main():
    """Main function to test data analysis."""
    analyzer = YouTubeDataAnalyzer()
    
    print("Starting data analysis...")
    
    results = analyzer.perform_analysis()
    
    if results and "channel_summary" in results:
        channel = results["channel_summary"]
        print(f"\nChannel: {channel.get('title', 'Unknown')}")
        print(f"Subscribers: {channel.get('subscriber_count', 0)}")
        print(f"Total Videos: {channel.get('video_count', 0)}")
        
        if "engagement_metrics" in results:
            metrics = results["engagement_metrics"]
            print(f"\nEngagement Metrics:")
            print(f"Average Views: {metrics.get('avg_views', 0)}")
            print(f"Average Likes: {metrics.get('avg_likes', 0)}")
            print(f"Average Comments: {metrics.get('avg_comments', 0)}")
            print(f"Average Engagement Rate: {metrics.get('avg_engagement', 0)}%")
        
        if "top_videos_by_views" in results and results["top_videos_by_views"]:
            top_video = results["top_videos_by_views"][0]
            print(f"\nTop Video: {top_video.get('title', 'Unknown')}")
            print(f"Views: {top_video.get('view_count', 0)}")
    
    print("\nAnalysis complete.")
    
    # Stop the Spark session
    analyzer.spark.stop()

if __name__ == "__main__":
    main()