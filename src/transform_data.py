"""
Module for transforming YouTube data using PySpark.
"""

import os
import glob
from typing import Dict, List, Optional, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, explode, from_json, json_tuple, 
    to_timestamp, from_unixtime, lit, 
    unix_timestamp, array, struct, udf, 
    datediff, current_date, to_date, 
    when, count, avg, sum as spark_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, LongType, DoubleType, 
    ArrayType, TimestampType, DateType,
    BooleanType
)

from src.config import DATA_RAW_DIR, DATA_PROCESSED_DIR
from src.utils import format_duration, get_video_category_name, calculate_engagement_rate

class YouTubeDataTransformer:
    """Class to transform YouTube data using PySpark."""
    
    def __init__(self):
        """Initialize the Spark session."""
        self.spark = SparkSession.builder \
            .appName("YouTubeDataAnalyzer") \
            .config("spark.driver.memory", os.getenv("PYSPARK_DRIVER_MEMORY", "4g")) \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
            
        # Register UDFs
        self.register_udfs()
    
    def register_udfs(self):
        """Register User Defined Functions for PySpark."""
        # UDF for formatting duration
        self.spark.udf.register(
            "format_duration", 
            lambda x: format_duration(x), 
            IntegerType()
        )
        
        # UDF for getting category name
        self.spark.udf.register(
            "get_category_name", 
            lambda x: get_video_category_name(x), 
            StringType()
        )
        
        # UDF for calculating engagement rate
        self.spark.udf.register(
            "calculate_engagement", 
            lambda likes, comments, views: calculate_engagement_rate(
                int(likes) if likes else 0, 
                int(comments) if comments else 0, 
                int(views) if views else 1
            ), 
            DoubleType()
        )
    
    def load_channel_info(self, file_pattern: str = "channel_*.json") -> DataFrame:
        """
        Load channel information from JSON files.
        
        Args:
            file_pattern: File pattern to load
            
        Returns:
            DataFrame: Channel information
        """
        file_path = os.path.join(DATA_RAW_DIR, file_pattern)
        files = glob.glob(file_path)
        
        if not files:
            raise FileNotFoundError(f"No files found matching {file_path}")
        
        # Use the most recent file
        latest_file = max(files, key=os.path.getctime)
        
        # Load JSON file
        raw_df = self.spark.read.json(latest_file)
        
        # Extract relevant fields from the nested structure
        channel_df = raw_df.select(
            explode(col("items")).alias("channel")
        ).select(
            col("channel.id").alias("channel_id"),
            col("channel.snippet.title").alias("channel_title"),
            col("channel.snippet.description").alias("channel_description"),
            col("channel.snippet.customUrl").alias("custom_url"),
            col("channel.snippet.publishedAt").alias("published_at"),
            col("channel.snippet.country").alias("country"),
            col("channel.statistics.viewCount").cast(LongType()).alias("view_count"),
            col("channel.statistics.subscriberCount").cast(LongType()).alias("subscriber_count"),
            col("channel.statistics.videoCount").cast(IntegerType()).alias("video_count"),
            col("channel.contentDetails.relatedPlaylists.uploads").alias("uploads_playlist_id")
        )
        
        # Convert timestamp strings to timestamp type
        channel_df = channel_df.withColumn(
            "published_at", 
            to_timestamp(col("published_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        )
        
        return channel_df
    
    def load_videos(self, file_pattern: str = "videos_details_*.json") -> DataFrame:
        """
        Load video information from JSON files.
        
        Args:
            file_pattern: File pattern to load
            
        Returns:
            DataFrame: Video information
        """
        file_path = os.path.join(DATA_RAW_DIR, file_pattern)
        files = glob.glob(file_path)
        
        if not files:
            raise FileNotFoundError(f"No files found matching {file_path}")
        
        # Use the most recent file
        latest_file = max(files, key=os.path.getctime)
        
        # Load JSON file
        raw_df = self.spark.read.json(latest_file)
        
        # Extract relevant fields from the nested structure
        videos_df = raw_df.select(
            explode(col("items")).alias("video")
        ).select(
            col("video.id").alias("video_id"),
            col("video.snippet.channelId").alias("channel_id"),
            col("video.snippet.title").alias("title"),
            col("video.snippet.description").alias("description"),
            col("video.snippet.publishedAt").alias("published_at"),
            col("video.snippet.channelTitle").alias("channel_title"),
            col("video.snippet.categoryId").alias("category_id"),
            col("video.snippet.tags").alias("tags"),
            col("video.contentDetails.duration").alias("duration"),
            col("video.statistics.viewCount").alias("view_count"),
            col("video.statistics.likeCount").alias("like_count"),
            col("video.statistics.favoriteCount").alias("favorite_count"),
            col("video.statistics.commentCount").alias("comment_count")
        )
        
        # Convert timestamp strings to timestamp type
        videos_df = videos_df.withColumn(
            "published_at", 
            to_timestamp(col("published_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        )
        
        # Convert string counts to numeric
        videos_df = videos_df \
            .withColumn("view_count", col("view_count").cast(LongType())) \
            .withColumn("like_count", col("like_count").cast(LongType())) \
            .withColumn("favorite_count", col("favorite_count").cast(LongType())) \
            .withColumn("comment_count", col("comment_count").cast(LongType()))
        
        # Format duration and add category name
        videos_df = videos_df \
            .withColumn("duration_seconds", self.spark.sql.functions.expr("format_duration(duration)")) \
            .withColumn("category_name", self.spark.sql.functions.expr("get_category_name(category_id)"))
        
        # Calculate engagement rate
        videos_df = videos_df.withColumn(
            "engagement_rate", 
            self.spark.sql.functions.expr("calculate_engagement(like_count, comment_count, view_count)")
        )
        
        # Add publish date (for easier date-based analysis)
        videos_df = videos_df.withColumn(
            "publish_date", 
            to_date(col("published_at"))
        )
        
        # Add days since publication
        videos_df = videos_df.withColumn(
            "days_since_published", 
            datediff(current_date(), col("publish_date"))
        )
        
        return videos_df
    
    def load_comments(self, file_pattern: str = "comments_*.json") -> DataFrame:
        """
        Load comments from JSON files.
        
        Args:
            file_pattern: File pattern to load
            
        Returns:
            DataFrame: Comments information
        """
        file_path = os.path.join(DATA_RAW_DIR, file_pattern)
        files = glob.glob(file_path)
        
        if not files:
            print(f"Warning: No comment files found matching {file_path}")
            # Create an empty DataFrame with the expected schema
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
            return self.spark.createDataFrame([], schema)
        
        # Combine all comment files
        all_dfs = []
        
        for file in files:
            # Extract video ID from filename
            filename = os.path.basename(file)
            parts = filename.split('_')
            if len(parts) >= 3:
                video_id = parts[1]
            else:
                continue
            
            # Load JSON file
            raw_df = self.spark.read.json(file)
            
            # Skip if empty or invalid
            if "items" not in raw_df.columns:
                continue
            
            # Extract relevant fields from the nested structure
            comments_df = raw_df.select(
                explode(col("items")).alias("comment")
            ).select(
                col("comment.id").alias("comment_id"),
                lit(video_id).alias("video_id"),
                col("comment.snippet.topLevelComment.snippet.authorDisplayName").alias("author_name"),
                col("comment.snippet.topLevelComment.snippet.authorChannelId.value").alias("author_channel_id"),
                col("comment.snippet.topLevelComment.snippet.textDisplay").alias("text"),
                col("comment.snippet.topLevelComment.snippet.publishedAt").alias("published_at"),
                col("comment.snippet.topLevelComment.snippet.updatedAt").alias("updated_at"),
                col("comment.snippet.topLevelComment.snippet.likeCount").cast(IntegerType()).alias("like_count")
            )
            
            # Convert timestamp strings to timestamp type
            comments_df = comments_df \
                .withColumn("published_at", to_timestamp(col("published_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
                .withColumn("updated_at", to_timestamp(col("updated_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
            
            all_dfs.append(comments_df)
        
        # Combine all comment DataFrames
        if all_dfs:
            return self.spark.createDataFrame(
                self.spark.sparkContext.union([df.rdd for df in all_dfs]),
                all_dfs[0].schema
            )
        else:
            # Create an empty DataFrame with the expected schema
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
            return self.spark.createDataFrame([], schema)
    
    def transform_and_save_data(self):
        """
        Main method to transform all YouTube data and save to processed directory.
        """
        try:
            # Load channel information
            channel_df = self.load_channel_info()
            
            # Load video information
            videos_df = self.load_videos()
            
            # Load comments information
            comments_df = self.load_comments()
            
            # Save processed data
            channel_df.write.mode("overwrite").parquet(os.path.join(DATA_PROCESSED_DIR, "channel"))
            videos_df.write.mode("overwrite").parquet(os.path.join(DATA_PROCESSED_DIR, "videos"))
            
            if comments_df.count() > 0:
                comments_df.write.mode("overwrite").parquet(os.path.join(DATA_PROCESSED_DIR, "comments"))
            
            # Create denormalized view for easy analysis
            if videos_df.count() > 0:
                # Add channel information to videos
                denormalized_df = videos_df.join(
                    channel_df.select("channel_id", "channel_title", "subscriber_count"),
                    on="channel_id",
                    how="left"
                )
                
                # Add comment counts
                if comments_df.count() > 0:
                    comment_counts = comments_df.groupBy("video_id").agg(
                        count("*").alias("actual_comment_count")
                    )
                    
                    denormalized_df = denormalized_df.join(
                        comment_counts,
                        on="video_id",
                        how="left"
                    ).withColumn(
                        "actual_comment_count",
                        when(col("actual_comment_count").isNull(), 0).otherwise(col("actual_comment_count"))
                    )
                
                # Save denormalized data
                denormalized_df.write.mode("overwrite").parquet(
                    os.path.join(DATA_PROCESSED_DIR, "denormalized")
                )
                
                # Also save as CSV for easy viewing (optional)
                denormalized_df.write.mode("overwrite").option("header", "true").csv(
                    os.path.join(DATA_PROCESSED_DIR, "denormalized_csv")
                )
                
                print(f"Processed data saved to {DATA_PROCESSED_DIR}")
                
                return {
                    "channel_count": channel_df.count(),
                    "videos_count": videos_df.count(),
                    "comments_count": comments_df.count()
                }
                
        except Exception as e:
            print(f"Error transforming data: {e}")
            raise

def main():
    """Main function to test data transformation."""
    transformer = YouTubeDataTransformer()
    
    print("Starting data transformation...")
    
    results = transformer.transform_and_save_data()
    
    if results:
        print(f"Transformation complete.")
        print(f"Processed {results['channel_count']} channels.")
        print(f"Processed {results['videos_count']} videos.")
        print(f"Processed {results['comments_count']} comments.")
    
    # Stop the Spark session
    transformer.spark.stop()

if __name__ == "__main__":
    main()