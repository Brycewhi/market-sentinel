#!/bin/bash

echo "========================================="
echo "  MARKET SENTINEL - FULL HEALTH CHECK"
echo "========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check service health
check_service() {
    local service=$1
    local status=$(docker-compose ps $service 2>/dev/null | grep -c "Up")
    if [ $status -eq 1 ]; then
        echo -e "${GREEN}✓${NC} $service"
    else
        echo -e "${RED}✗${NC} $service (DOWN)"
    fi
}

echo "🐳 DOCKER CONTAINERS:"
check_service "postgres"
check_service "minio"
check_service "airflow-postgres"
check_service "airflow-webserver"
check_service "airflow-scheduler"
echo ""

echo "💾 DOCKER RESOURCE USAGE:"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" | grep -E "(NAME|market-sentinel|postgres|minio|airflow)"
echo ""

echo "🗄️  POSTGRESQL HEALTH:"
# Check if PostgreSQL is accepting connections
PG_STATUS=$(docker-compose exec postgres pg_isready -U market_sentinel 2>&1)
if echo "$PG_STATUS" | grep -q "accepting connections"; then
    echo -e "${GREEN}✓${NC} PostgreSQL accepting connections"
else
    echo -e "${RED}✗${NC} PostgreSQL connection failed"
fi

# Check database exists
DB_EXISTS=$(docker-compose exec postgres psql -U market_sentinel -lqt 2>/dev/null | cut -d \| -f 1 | grep -w market_sentinel | wc -l)
if [ $DB_EXISTS -eq 1 ]; then
    echo -e "${GREEN}✓${NC} Database 'market_sentinel' exists"
else
    echo -e "${RED}✗${NC} Database 'market_sentinel' not found"
fi

# Check table exists and has data
TABLE_COUNT=$(docker-compose exec postgres psql -U market_sentinel -d market_sentinel -t -c "SELECT COUNT(*) FROM staging.sentiment_logs;" 2>/dev/null | xargs)
if [ ! -z "$TABLE_COUNT" ] && [ "$TABLE_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓${NC} staging.sentiment_logs has $TABLE_COUNT records"
else
    echo -e "${YELLOW}⚠${NC} staging.sentiment_logs is empty or unreachable"
fi

# Check schemas exist
STAGING_SCHEMA=$(docker-compose exec postgres psql -U market_sentinel -d market_sentinel -t -c "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'staging';" 2>/dev/null | xargs)
ANALYTICS_SCHEMA=$(docker-compose exec postgres psql -U market_sentinel -d market_sentinel -t -c "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'analytics';" 2>/dev/null | xargs)

if [ "$STAGING_SCHEMA" -eq 1 ]; then
    echo -e "${GREEN}✓${NC} Schema 'staging' exists"
else
    echo -e "${RED}✗${NC} Schema 'staging' missing"
fi

if [ "$ANALYTICS_SCHEMA" -eq 1 ]; then
    echo -e "${GREEN}✓${NC} Schema 'analytics' exists"
else
    echo -e "${RED}✗${NC} Schema 'analytics' missing"
fi
echo ""

echo "📦 MINIO (DATA LAKE) HEALTH:"
# Check MinIO is reachable
MINIO_STATUS=$(docker-compose exec -T airflow-scheduler python3 << 'PYTHON'
try:
    import boto3
    s3 = boto3.client('s3', endpoint_url='http://minio:9000', 
                      aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin')
    buckets = s3.list_buckets()
    print("CONNECTED")
except Exception as e:
    print(f"ERROR: {e}")
PYTHON
)

if echo "$MINIO_STATUS" | grep -q "CONNECTED"; then
    echo -e "${GREEN}✓${NC} MinIO server reachable"
else
    echo -e "${RED}✗${NC} MinIO connection failed: $MINIO_STATUS"
fi

# Check bucket exists and has files
BUCKET_INFO=$(docker-compose exec -T airflow-scheduler python3 << 'PYTHON'
try:
    import boto3
    s3 = boto3.client('s3', endpoint_url='http://minio:9000', 
                      aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin')
    
    # Check if bucket exists
    buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
    if 'raw-news' not in buckets:
        print("BUCKET_MISSING")
    else:
        # Count files
        response = s3.list_objects_v2(Bucket='raw-news')
        file_count = len(response.get('Contents', []))
        print(f"FILES:{file_count}")
except Exception as e:
    print(f"ERROR:{e}")
PYTHON
)

if echo "$BUCKET_INFO" | grep -q "BUCKET_MISSING"; then
    echo -e "${RED}✗${NC} Bucket 'raw-news' does not exist"
elif echo "$BUCKET_INFO" | grep -q "FILES:"; then
    FILE_COUNT=$(echo "$BUCKET_INFO" | cut -d: -f2)
    echo -e "${GREEN}✓${NC} Bucket 'raw-news' has $FILE_COUNT JSON files"
else
    echo -e "${RED}✗${NC} MinIO bucket check failed"
fi
echo ""

echo "⚙️  AIRFLOW HEALTH:"
# Check Airflow webserver is up
WEBSERVER_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health 2>/dev/null)
if [ "$WEBSERVER_STATUS" -eq 200 ]; then
    echo -e "${GREEN}✓${NC} Airflow webserver responding (http://localhost:8080)"
else
    echo -e "${YELLOW}⚠${NC} Airflow webserver status: $WEBSERVER_STATUS"
fi

# Check scheduler is running
SCHEDULER_STATUS=$(docker-compose exec airflow-scheduler airflow jobs check --job-type SchedulerJob --hostname "*" 2>&1)
if echo "$SCHEDULER_STATUS" | grep -q "alive"; then
    echo -e "${GREEN}✓${NC} Airflow scheduler is alive"
else
    echo -e "${YELLOW}⚠${NC} Airflow scheduler check: $SCHEDULER_STATUS"
fi

# Check DAGs are loaded
DAG_COUNT=$(docker-compose exec airflow-scheduler airflow dags list 2>/dev/null | grep -E "(ingest_news|process_sentiment)" | wc -l)
if [ $DAG_COUNT -eq 2 ]; then
    echo -e "${GREEN}✓${NC} Both DAGs loaded (ingest_news, process_sentiment)"
else
    echo -e "${YELLOW}⚠${NC} Expected 2 DAGs, found $DAG_COUNT"
fi

# Check for recent DAG runs
RECENT_RUNS=$(docker-compose exec airflow-scheduler airflow dags list-runs -d process_sentiment --limit 1 2>/dev/null | tail -1 | awk '{print $3}')
if [ ! -z "$RECENT_RUNS" ]; then
    echo -e "${GREEN}✓${NC} Recent DAG run: $RECENT_RUNS"
else
    echo -e "${YELLOW}⚠${NC} No recent DAG runs found"
fi
echo ""

echo "📊 DATA QUALITY CHECKS:"
# Check sentiment scores sum to ~1.0
BAD_SCORES=$(docker-compose exec postgres psql -U market_sentinel -d market_sentinel -t -c "SELECT COUNT(*) FROM staging.sentiment_logs WHERE ABS((positive_score + negative_score + neutral_score) - 1.0) > 0.05;" 2>/dev/null | xargs)
if [ "$BAD_SCORES" -eq 0 ]; then
    echo -e "${GREEN}✓${NC} All sentiment scores sum to ~1.0"
else
    echo -e "${YELLOW}⚠${NC} $BAD_SCORES records with invalid sentiment scores"
fi

# Check for null values
NULL_COUNT=$(docker-compose exec postgres psql -U market_sentinel -d market_sentinel -t -c "SELECT COUNT(*) FROM staging.sentiment_logs WHERE positive_score IS NULL OR negative_score IS NULL OR neutral_score IS NULL;" 2>/dev/null | xargs)
if [ "$NULL_COUNT" -eq 0 ]; then
    echo -e "${GREEN}✓${NC} No null sentiment scores"
else
    echo -e "${YELLOW}⚠${NC} $NULL_COUNT records with null scores"
fi

# Check for duplicates (should be 0 with unique constraint)
DUPLICATE_COUNT=$(docker-compose exec postgres psql -U market_sentinel -d market_sentinel -t -c "SELECT COUNT(*) - COUNT(DISTINCT (ticker, headline)) FROM staging.sentiment_logs;" 2>/dev/null | xargs)
if [ ! -z "$DUPLICATE_COUNT" ] && [ "$DUPLICATE_COUNT" -eq 0 ]; then
    echo -e "${GREEN}✓${NC} No duplicate headlines (unique constraint working)"
else
    echo -e "${YELLOW}⚠${NC} Unable to check for duplicates or duplicates detected"
fi
echo ""

echo "📈 SENTIMENT ANALYSIS RESULTS:"
docker-compose exec postgres psql -U market_sentinel -d market_sentinel -t -c "SELECT ticker || ': ' || COUNT(*) || ' articles, avg sentiment: +' || ROUND((AVG(positive_score) - AVG(negative_score))::numeric, 3) FROM staging.sentiment_logs GROUP BY ticker ORDER BY ticker;"
echo ""

echo "⏰ NEXT SCHEDULED RUNS:"
NEXT_INGEST=$(docker-compose exec airflow-scheduler airflow dags next-execution ingest_news 2>/dev/null | tail -1)
NEXT_SENTIMENT=$(docker-compose exec airflow-scheduler airflow dags next-execution process_sentiment 2>/dev/null | tail -1)
echo "News Ingestion (22:00 UTC): $NEXT_INGEST"
echo "Sentiment Analysis (23:00 UTC): $NEXT_SENTIMENT"
echo ""

echo "💽 DISK USAGE:"
echo "PostgreSQL:"
docker-compose exec postgres df -h / | tail -1
echo "MinIO:"
docker-compose exec minio df -h / | tail -1
echo ""

echo "========================================="
echo -e "  ${GREEN}✓ Health Check Complete${NC}"
echo "========================================="
