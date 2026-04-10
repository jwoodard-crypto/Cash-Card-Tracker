#!/usr/bin/env python3
"""
Cash Card Fraud Dashboard Refresh Script
Queries Snowflake for hourly dispute volumes and updates the dashboard HTML
"""

import os
import sys
import json
import re
from datetime import datetime
from typing import Dict, List, Any

try:
    import snowflake.connector
    import requests
except ImportError:
    print("Installing required packages...")
    os.system("pip install snowflake-connector-python requests")
    import snowflake.connector
    import requests


def get_snowflake_connection():
    """Create Snowflake connection using environment variables"""
    return snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'ADHOC__LARGE'),
        database=os.environ.get('SNOWFLAKE_DATABASE', 'APP_CASH_CS'),
        schema=os.environ.get('SNOWFLAKE_SCHEMA', 'DISPUTES')
    )


def execute_query(conn) -> List[Dict[str, Any]]:
    """Execute the hourly volumes query"""
    query = """
    WITH hourly_volumes AS (
        SELECT 
            DATE_TRUNC('hour', CLAIM_SUBMITTED_AT_ET) as submission_hour,
            COUNT(*) as total_claims,
            COUNT(DISTINCT CUSTOMER_TOKEN) as unique_customers,
            COUNT(*) - COUNT(DISTINCT CUSTOMER_TOKEN) as repeat_claim_count,
            SUM(DISPUTED_AMOUNT) as total_disputed_amount,
            AVG(DISPUTED_AMOUNT) as avg_disputed_amount,
            LISTAGG(DISTINCT DISPUTE_CLASSIFICATION, ', ') as dispute_types,
            COUNT(DISTINCT MERCHANT_NAME) as unique_merchants
        FROM APP_CASH_CS.DISPUTES.CC_DISPUTES_CLAIM_DETAILS
        WHERE CLAIM_SUBMITTED_AT_ET >= DATEADD(hour, -168, CURRENT_TIMESTAMP())
        GROUP BY 1
    ),
    repeat_customers AS (
        SELECT 
            DATE_TRUNC('hour', CLAIM_SUBMITTED_AT_ET) as submission_hour,
            COUNT(DISTINCT CUSTOMER_TOKEN) as repeat_customers_this_hour
        FROM APP_CASH_CS.DISPUTES.CC_DISPUTES_CLAIM_DETAILS
        WHERE CLAIM_SUBMITTED_AT_ET >= DATEADD(hour, -168, CURRENT_TIMESTAMP())
        GROUP BY 1, CUSTOMER_TOKEN
        HAVING COUNT(*) > 1
    ),
    with_rolling_avg AS (
        SELECT 
            hv.*,
            COALESCE(rc.repeat_customers_this_hour, 0) as repeat_customers_this_hour,
            AVG(hv.total_claims) OVER (ORDER BY hv.submission_hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) as rolling_24hr_avg,
            STDDEV(hv.total_claims) OVER (ORDER BY hv.submission_hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) as rolling_24hr_stddev,
            AVG(hv.total_claims) OVER (ORDER BY hv.submission_hour ROWS BETWEEN 167 PRECEDING AND CURRENT ROW) as rolling_7day_avg,
            LAG(hv.total_claims, 1) OVER (ORDER BY hv.submission_hour) as prev_hour_claims,
            LAG(hv.total_claims, 24) OVER (ORDER BY hv.submission_hour) as same_hour_yesterday
        FROM hourly_volumes hv
        LEFT JOIN (
            SELECT submission_hour, COUNT(*) as repeat_customers_this_hour
            FROM repeat_customers
            GROUP BY submission_hour
        ) rc ON hv.submission_hour = rc.submission_hour
    )
    SELECT 
        submission_hour,
        TO_DATE(submission_hour) as date,
        TO_TIME(submission_hour) as time_utc,
        HOUR(submission_hour) as hour_of_day,
        CASE 
            WHEN DAYOFWEEK(submission_hour) IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END as day_type,
        total_claims,
        unique_customers,
        repeat_claim_count,
        total_disputed_amount,
        avg_disputed_amount,
        dispute_types as most_common_dispute_type,
        unique_merchants,
        rolling_24hr_avg,
        rolling_7day_avg as same_hour_7day_avg,
        CASE 
            WHEN total_claims > rolling_24hr_avg + (2 * rolling_24hr_stddev) THEN 'SPIKE'
            WHEN total_claims > rolling_24hr_avg + rolling_24hr_stddev THEN 'WARNING'
            ELSE 'NORMAL'
        END as volume_flag,
        CASE 
            WHEN prev_hour_claims > 0 THEN ((total_claims - prev_hour_claims) * 100.0 / prev_hour_claims)
            ELSE 0
        END as pct_change_from_prev_hour,
        CASE 
            WHEN same_hour_yesterday > 0 THEN ((total_claims - same_hour_yesterday) * 100.0 / same_hour_yesterday)
            ELSE 0
        END as pct_change_from_yesterday,
        CASE 
            WHEN rolling_24hr_avg > 0 THEN ((total_claims - rolling_24hr_avg) * 100.0 / rolling_24hr_avg)
            ELSE 0
        END as pct_change_from_24hr_avg,
        repeat_customers_this_hour
    FROM with_rolling_avg
    ORDER BY submission_hour DESC
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    
    columns = [desc[0] for desc in cursor.description]
    results = []
    
    for row in cursor:
        row_dict = {}
        for i, col in enumerate(columns):
            value = row[i]
            # Convert datetime/date/time to string
            if hasattr(value, 'isoformat'):
                value = value.isoformat()
            # Convert Decimal to float
            elif hasattr(value, '__float__'):
                value = float(value)
            row_dict[col] = value
        results.append(row_dict)
    
    cursor.close()
    return results


def check_for_spikes(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Check if the most recent hour has a spike"""
    if not data:
        return {"has_spike": False, "message": "No data available"}
    
    latest = data[0]  # Most recent hour (query orders DESC)
    
    if latest.get('VOLUME_FLAG') == 'SPIKE':
        return {
            "has_spike": True,
            "hour": latest.get('SUBMISSION_HOUR'),
            "total_claims": latest.get('TOTAL_CLAIMS'),
            "rolling_avg": latest.get('ROLLING_24HR_AVG'),
            "pct_change": latest.get('PCT_CHANGE_FROM_24HR_AVG'),
            "unique_customers": latest.get('UNIQUE_CUSTOMERS'),
            "repeat_customers": latest.get('REPEAT_CUSTOMERS_THIS_HOUR')
        }
    
    return {"has_spike": False}


def update_dashboard_html(data: List[Dict[str, Any]], html_path: str = 'index.html'):
    """Update the dashboard HTML with new data"""
    
    with open(html_path, 'r') as f:
        html_content = f.read()
    
    # Convert data to JavaScript array format
    js_data = json.dumps(data, indent=12)
    
    # Find and replace the hourlyData array
    pattern = r'const hourlyData = \[.*?\];'
    replacement = f'const hourlyData = {js_data};'
    
    updated_html = re.sub(pattern, replacement, html_content, flags=re.DOTALL)
    
    # Update the timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
    updated_html = re.sub(
        r'<div class="timestamp">Last updated:.*?</div>',
        f'<div class="timestamp">Last updated: {timestamp}</div>',
        updated_html
    )
    
    with open(html_path, 'w') as f:
        f.write(updated_html)
    
    print(f"✅ Dashboard updated with {len(data)} hours of data")


def send_slack_alert(spike_info: Dict[str, Any]):
    """Send Slack alert if spike detected"""
    webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
    
    if not webhook_url:
        print("⚠️  No SLACK_WEBHOOK_URL configured, skipping alert")
        return
    
    message = {
        "text": "🚨 *CASH CARD FRAUD SPIKE DETECTED*",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🚨 Cash Card Fraud Spike Detected"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Hour:*\n{spike_info['hour']}"},
                    {"type": "mrkdwn", "text": f"*Total Claims:*\n{spike_info['total_claims']:.0f}"},
                    {"type": "mrkdwn", "text": f"*24hr Avg:*\n{spike_info['rolling_avg']:.1f}"},
                    {"type": "mrkdwn", "text": f"*% Change:*\n{spike_info['pct_change']:.1f}%"},
                    {"type": "mrkdwn", "text": f"*Unique Customers:*\n{spike_info['unique_customers']}"},
                    {"type": "mrkdwn", "text": f"*Repeat Customers:*\n{spike_info['repeat_customers']}"}
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "View the dashboard for more details."
                }
            }
        ]
    }
    
    response = requests.post(webhook_url, json=message)
    
    if response.status_code == 200:
        print("✅ Slack alert sent successfully")
    else:
        print(f"❌ Failed to send Slack alert: {response.status_code}")


def main():
    """Main execution function"""
    print("🔄 Starting Cash Card Dashboard refresh...")
    
    try:
        # Connect to Snowflake
        print("📊 Connecting to Snowflake...")
        conn = get_snowflake_connection()
        
        # Execute query
        print("⚡ Executing query...")
        data = execute_query(conn)
        conn.close()
        
        if not data:
            print("⚠️  No data returned from query")
            sys.exit(1)
        
        print(f"✅ Retrieved {len(data)} hours of data")
        
        # Check for spikes
        spike_info = check_for_spikes(data)
        
        if spike_info.get('has_spike'):
            print(f"🚨 SPIKE DETECTED at {spike_info['hour']}")
            send_slack_alert(spike_info)
        else:
            print("✅ No spikes detected")
        
        # Update dashboard
        print("📝 Updating dashboard HTML...")
        update_dashboard_html(data)
        
        print("✅ Dashboard refresh complete!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
