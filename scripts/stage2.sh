#!/bin/bash
export PATH="$HOME/bin:$PATH"
# Password Verification
if [ -z "$PASSWORD" ]; then
    echo "Error: PASSWORD not set"
    exit 1
fi

# Settings
# shellcheck disable=SC2034
BEELINE="beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/team6_projectdb -n team6 -p $PASSWORD"
HDFS_BASE="/user/team6/project/output"
# shellcheck disable=SC2034
LOCAL_BASE="/home/team6/project/my-big-data-project-2025/output"
QUERIES_DIR="/home/team6/project/my-big-data-project-2025/hive/queries"

# Creating local directories
mkdir -p $LOCAL_BASE/insights
mkdir -p $LOCAL_BASE/logs


# Step 1: Create database and external tables
echo "Creating database and external tables..."
$BEELINE -f '/home/team6/project/my-big-data-project-2025/hive/db.hql' > $LOCAL_BASE/logs/db.log 2>&1

# Step 2: Check column datatypes
echo "Checking table schemas..."
$BEELINE -e "
    DESCRIBE dim_base;
    DESCRIBE dim_location;
    DESCRIBE dim_calendar;
    DESCRIBE fact_fhv_trips;
" > $LOCAL_BASE/logs/table_schemas.log 2>&1

# Step 3: Create partitioned and bucketed table
echo "Creating partitioned table..."
$BEELINE -f '/home/team6/project/my-big-data-project-2025/hive/create_part_table.hql'> $LOCAL_BASE/logs/create_part_table.log 2>&1
echo "Creating buckuted table ..."
$BEELINE -f '/home/team6/project/my-big-data-project-2025/hive/create_bucketed_table.hql' > $LOCAL_BASE/logs/create_bucketed_table.log 2>&1
echo "Cheking table for Null, Anomalies ..."
$BEELINE -f "/home/team6/project/my-big-data-project-2025/hive/check_data.hql" > "$LOCAL_BASE/logs/check_data.log" 2>&1
echo "Creating clean table..."
$BEELINE -f "/home/team6/project/my-big-data-project-2025/hive/clean_data.hql" > "$LOCAL_BASE/logs/clean_data.log" 2>&1


# # Step 4: Verify queries
# echo "Testing queries..."
# $BEELINE -e "
#     SELECT * FROM dim_base LIMIT 10;
#     SELECT * FROM dim_location LIMIT 10;
#     SELECT * FROM dim_calendar LIMIT 10;
#     SELECT * FROM fact_fhv_trips LIMIT 10;
#     SELECT * FROM fact_fhv_trips_part WHERE month=1 LIMIT 10;
# " > $LOCAL_BASE/logs/query_test.log 2>&1

# -------------------------
# Running Q1-Q5
# -------------------------
for q in 1 2 3 4 5 6 ; do
    echo "Running q$q..."
    $BEELINE -f "$QUERIES_DIR/q${q}_results.hql" > "$LOCAL_BASE/logs/q${q}_results.log" 2>&1
    $BEELINE -e "
        SET hive.resultset.use.unique.column.names=false;
        INSERT OVERWRITE DIRECTORY '${HDFS_BASE}/insights/q${q}'
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        SELECT * FROM q${q}_results;
    " >> "$LOCAL_BASE/logs/q${q}_results.log" 2>&1
    case $q in
        1)
            echo "hour_of_day,trip_count,avg_driver_income,income_per_hour,tip_percentage" > "$LOCAL_BASE/insights/q${q}.csv"
            ;;
        2)
            echo "zone,borough,trip_count,avg_tip,tip_percentage,avg_fare,fare_to_tip_ratio,zone_category" > "$LOCAL_BASE/insights/q${q}.csv"
            ;;
        3)
            echo "zone,trip_count,total_airport_fees,avg_company_profit,avg_driver_income" > "$LOCAL_BASE/insights/q${q}.csv"
            ;;
        4)
            echo "from_zone,to_zone,empty_runs_count,avg_distance_miles" > "$LOCAL_BASE/insights/q${q}.csv"
            ;;
        5)
            echo "is_weekend,hour,trip_count,avg_fare,avg_duration_min,avg_tips,avg_wait_time_min,day_part" > "$LOCAL_BASE/insights/q${q}.csv"
            ;;
        6)
            echo "month,avg_driver_income, avg_company_profit" > "$LOCAL_BASE/insights/q${q}.csv"
            ;;
    esac
    hdfs dfs -cat "${HDFS_BASE}/insights/q${q}/*" >> "$LOCAL_BASE/insights/q${q}.csv"
    echo "Saved to $LOCAL_BASE/insights/q${q}.csv"
done

# Step 6: Drop unpartitioned tables
echo "Dropping unpartitioned tables..."
$BEELINE -e "
    DROP TABLE fact_fhv_trips;
" > $LOCAL_BASE/logs/drop_tables.log 2>&1


echo "Checking script quality..."
echo "Current PATH: $PATH" >> "$LOCAL_BASE/logs/shellcheck.log"
echo "shellcheck location: $(which shellcheck)" >> "$LOCAL_BASE/logs/shellcheck.log"

if [ -x "$HOME/bin/shellcheck" ]; then
    if "$HOME/bin/shellcheck" "$0" >> "$LOCAL_BASE/logs/shellcheck.log" 2>&1; then
        echo "Shellcheck passed: no issues found." >> "$LOCAL_BASE/logs/shellcheck.log"
    else
        echo "Shellcheck found issues, see $LOCAL_BASE/logs/shellcheck.log" >&2
        cat "$LOCAL_BASE/logs/shellcheck.log" >&2
    fi
else
    echo "Error: shellcheck not installed at $HOME/bin/shellcheck. Please install it or run on another machine." >> "$LOCAL_BASE/logs/shellcheck.log"
    echo "See instructions in report for installing shellcheck without sudo." >> "$LOCAL_BASE/logs/shellcheck.log"
fi


echo "Stage 2 completed successfully."