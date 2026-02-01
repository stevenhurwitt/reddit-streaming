#!/bin/bash
# prepare_for_tonight.sh - Prepare system for tonight's curation jobs

echo "🛠️  Preparing system for tonight's curation jobs..."

# Test and run the cleanup script
echo "📋 Testing cleanup script..."
cd /home/steven/reddit-streaming/local_jobs
./kill_stuck_jobs.sh --force

if [ $? -eq 0 ]; then
    echo "✅ Cleanup completed successfully"
else
    echo "⚠️  Cleanup had issues, but continuing..."
fi

# Update cron jobs with new configuration
echo "🕐 Updating cron jobs..."
./setup_cron.sh

# Verify Docker containers are running
echo "🐳 Checking Docker services..."
cd /home/steven/reddit-streaming

echo "📊 Current container status:"
docker-compose ps

# Check if key services are running
SPARK_MASTER_STATUS=$(docker-compose ps spark-master | grep -c "Up")
SPARK_WORKER_STATUS=$(docker-compose ps | grep spark-worker | grep -c "Up")

if [ "$SPARK_MASTER_STATUS" -eq 1 ]; then
    echo "✅ Spark Master is running"
else
    echo "❌ Spark Master is not running - attempting restart..."
    docker-compose up -d spark-master
    sleep 10
fi

if [ "$SPARK_WORKER_STATUS" -ge 1 ]; then
    echo "✅ Spark Workers are running ($SPARK_WORKER_STATUS workers)"
else
    echo "❌ No Spark Workers running - attempting restart..."
    docker-compose up -d spark-worker-1 spark-worker-2
    sleep 15
fi

# Test Spark connectivity
echo "🔗 Testing Spark cluster connectivity..."
docker exec reddit-spark-master python3 -c "
from pyspark.sql import SparkSession
try:
    spark = SparkSession.builder.appName('TestConnectivity').master('spark://spark-master:7077').getOrCreate()
    print('✅ Spark connection successful')
    spark.stop()
except Exception as e:
    print(f'❌ Spark connection failed: {e}')
    exit(1)
" 2>/dev/null

if [ $? -eq 0 ]; then
    echo "🎯 Spark cluster is ready for jobs"
else
    echo "⚠️  Spark connectivity test failed - may need manual intervention"
    echo "💡 Try: docker-compose restart spark-master spark-worker-1 spark-worker-2"
fi

# Check disk space
echo "💾 Checking disk space..."
DISK_USAGE=$(df /home | awk 'NR==2 {print $5}' | sed 's/%//')
if [ "$DISK_USAGE" -gt 85 ]; then
    echo "⚠️  Disk usage high: ${DISK_USAGE}%"
    echo "🧹 Cleaning up old log files..."
    find /home/steven/reddit-streaming/local_jobs/logs -name "*.log" -mtime +3 -exec ls -la {} \; -exec rm {} \;
    find /tmp -name "spark-*" -mtime +1 -exec rm -rf {} \; 2>/dev/null || true
else
    echo "✅ Disk usage OK: ${DISK_USAGE}%"
fi

# Check memory
echo "🧠 Checking available memory..."
FREE_MEM=$(free -m | awk 'NR==2{printf "%.1f", $7/$2*100}')
echo "💾 Free memory: ${FREE_MEM}%"

# Show current cron jobs
echo "📅 Current cron jobs:"
crontab -l | grep REDDIT_CRON

echo ""
echo "🎉 Preparation complete!"
echo ""
echo "📋 Summary:"
echo "• Cleanup script installed and tested"
echo "• Cron jobs updated with timeouts and cleanup"
echo "• Docker services verified"
echo "• Spark cluster tested"
echo ""
echo "⏰ Tonight's schedule (UTC):"
echo "• 11:55 PM - Stop streaming"
echo "• 11:57 PM - Kill stuck jobs & cleanup"
echo "• 12:00 AM - News curation (1hr timeout)"
echo "• 12:30 AM - Technology curation (1hr timeout)"
echo "• 01:00 AM - ProgrammerHumor curation (1hr timeout)"
echo "• 01:30 AM - Worldnews curation (1hr timeout)"
echo "• 02:15 AM - Start streaming"
echo ""
echo "🔍 Monitor tonight with:"
echo "  watch -n 30 'tail -5 /home/steven/reddit-streaming/local_jobs/logs/*.log'"
echo ""
echo "🚨 Emergency cleanup (if jobs get stuck again):"
echo "  /home/steven/reddit-streaming/local_jobs/kill_stuck_jobs.sh --force"