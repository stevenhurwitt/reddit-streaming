#!/usr/bin/env python3
"""
Quick Start Guide - Polars Reddit Streaming

This script provides a simple way to test and start the Polars-based streaming.
"""

import os
import sys


def print_header(text):
    """Print a formatted header"""
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70 + "\n")


def check_dependencies():
    """Check if required packages are installed"""
    print_header("Checking Dependencies")
    
    required = {
        'polars': 'pip install polars',
        'deltalake': 'pip install deltalake',
        'confluent_kafka': 'pip install confluent-kafka',
        's3fs': 'pip install s3fs',
        'yaml': 'pip install PyYAML',
    }
    
    missing = []
    for package, install_cmd in required.items():
        try:
            __import__(package)
            print(f"✓ {package:20} - installed")
        except ImportError:
            print(f"✗ {package:20} - MISSING")
            missing.append((package, install_cmd))
    
    if missing:
        print("\n⚠️  Missing dependencies. Install with:")
        for _, cmd in missing:
            print(f"   {cmd}")
        print("\nOr install all at once:")
        print("   pip install -r requirements_polars.txt")
        return False
    else:
        print("\n✓ All dependencies installed!")
        return True


def check_config():
    """Check if configuration files exist"""
    print_header("Checking Configuration")
    
    files = {
        'creds.json': ['./creds.json', '/opt/workspace/redditStreaming/creds.json'],
        'config.yaml': ['./config.yaml', '/opt/workspace/redditStreaming/src/reddit/config.yaml']
    }
    
    all_found = True
    for filename, paths in files.items():
        found = False
        for path in paths:
            if os.path.exists(path):
                print(f"✓ {filename:20} - found at {path}")
                found = True
                break
        
        if not found:
            print(f"✗ {filename:20} - NOT FOUND")
            all_found = False
    
    return all_found


def check_directories():
    """Create necessary directories"""
    print_header("Checking/Creating Directories")
    
    dirs = [
        '/opt/workspace/checkpoints',
        '/opt/workspace/tmp',
    ]
    
    for d in dirs:
        try:
            os.makedirs(d, exist_ok=True)
            print(f"✓ {d}")
        except Exception as e:
            print(f"✗ {d} - Error: {e}")


def show_usage():
    """Show usage instructions"""
    print_header("Usage Instructions")
    
    print("1. Single Subreddit (from config.yaml):")
    print("   python reddit_streaming_polars.py")
    print()
    
    print("2. Specific Subreddit (environment variable):")
    print("   export subreddit=technology")
    print("   python reddit_streaming_polars.py")
    print()
    
    print("3. Multiple Subreddits (multiprocessing):")
    print("   ./run_multi_polars.py")
    print("   # or")
    print("   python run_multi_polars.py")
    print()
    
    print("4. Background with screen:")
    print("   screen -S reddit_polars")
    print("   python reddit_streaming_polars.py")
    print("   # Press Ctrl+A, D to detach")
    print("   # screen -r reddit_polars  (to reattach)")
    print()


def show_monitoring():
    """Show monitoring commands"""
    print_header("Monitoring")
    
    print("Check checkpoint directories:")
    print("   ls -lh /opt/workspace/checkpoints/polars_*/")
    print()
    
    print("Check S3 Delta tables:")
    print("   aws s3 ls s3://reddit-streaming-stevenhurwitt-2/")
    print()
    
    print("View process status:")
    print("   ps aux | grep polars")
    print()
    
    print("Check Kafka topics:")
    print("   kafka-topics.sh --list --bootstrap-server localhost:9092")
    print()


def show_comparison():
    """Show comparison with Spark version"""
    print_header("Spark vs Polars Comparison")
    
    print("RESOURCE USAGE:")
    print("   Spark:  ~2-4 GB RAM, 4+ CPU cores, JVM overhead")
    print("   Polars: ~500 MB - 1 GB RAM, 1 CPU core, Python only")
    print()
    
    print("STARTUP TIME:")
    print("   Spark:  30-60 seconds")
    print("   Polars: 1-2 seconds")
    print()
    
    print("DEPLOYMENT:")
    print("   Spark:  Requires cluster, workers, master node")
    print("   Polars: Single process or simple multiprocessing")
    print()
    
    print("BEST FOR:")
    print("   Spark:  Very high throughput (millions/hour)")
    print("   Polars: Moderate throughput (thousands-hundreds of thousands/hour)")
    print()


def main():
    """Run all checks"""
    print_header("Polars Reddit Streaming - Quick Start")
    
    # Run checks
    deps_ok = check_dependencies()
    config_ok = check_config()
    check_directories()
    
    # Show information
    show_usage()
    show_monitoring()
    show_comparison()
    
    # Final status
    print_header("Status")
    if deps_ok and config_ok:
        print("✓ System ready! You can start streaming.")
        print("\nTo begin:")
        print("   python reddit_streaming_polars.py")
    else:
        print("⚠️  Please resolve the issues above before starting.")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
