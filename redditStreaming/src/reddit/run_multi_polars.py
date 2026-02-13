#!/usr/bin/env python3
"""
Multi-subreddit streaming script using multiprocessing.

This script allows streaming multiple subreddits simultaneously using
separate processes for each subreddit.
"""

import sys
import os
import yaml
import json
from multiprocessing import Process
import signal

# Add parent directory to path
sys.path.insert(0, os.path.dirname(__file__))

from reddit_streaming_polars import stream_subreddit, read_files


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print('\n\nReceived interrupt signal. Shutting down all streams...')
    sys.exit(0)


def main():
    """
    Start streaming processes for all configured subreddits.
    """
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        creds, config = read_files()
        
        # Get configuration
        kafka_host = config.get("kafka_host", "localhost")
        subreddit_list = config.get("subreddit", ["technology"])
        processing_interval = config.get("processing_interval", 180)
        batch_size = config.get("batch_size", 100)
        
        print("=" * 60)
        print("Multi-Subreddit Polars Streaming")
        print("=" * 60)
        print(f"Kafka host: {kafka_host}")
        print(f"Subreddits: {', '.join(subreddit_list)}")
        print(f"Batch size: {batch_size}")
        print(f"Processing interval: {processing_interval}s")
        print("=" * 60)
        print()
        
        # Create a process for each subreddit
        processes = []
        for subreddit in subreddit_list:
            print(f"Starting process for: {subreddit}")
            p = Process(
                target=stream_subreddit,
                args=(subreddit, kafka_host, creds, batch_size, processing_interval),
                name=f"reddit_{subreddit}"
            )
            p.start()
            processes.append((subreddit, p))
        
        print(f"\nStarted {len(processes)} streaming processes")
        print("Press Ctrl+C to stop all streams\n")
        
        # Wait for all processes
        for subreddit, p in processes:
            p.join()
            print(f"Process for {subreddit} has stopped")
    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
