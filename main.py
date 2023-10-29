from flask import Flask, jsonify, send_file
import random
from datetime import datetime, timedelta
from pytz import timezone
from pymongo import MongoClient

app = Flask(__name__)

# MongoDB connection parameters
mongo_params = {
    'host': 'localhost',
    'port': 27017,
    'database_name': 'loop_db',
}

# Report to store calculated reports
reports = {}

def calculate_uptime_downtime(store_id, store_activity_data, business_hours_data, timezone_data):
    # Get the store's business hours data
    business_hours = business_hours_data.get(store_id, {})
    start_time_local = business_hours.get("start_time_local")
    end_time_local = business_hours.get("end_time_local")
    timezone_str = timezone_data.get(store_id, "America/Chicago")

    if start_time_local and end_time_local:
        # Create a timezone object using the provided timezone_str
        tz = timezone(timezone_str)

        # Get the current timestamp in the store's timezone
        current_timestamp = datetime.now(tz)

        # Calculate time intervals
        last_hour_start = current_timestamp - timedelta(hours=1)
        last_day_start = current_timestamp - timedelta(days=1)
        last_week_start = current_timestamp - timedelta(weeks=1)

        # Initialize counters for each time interval
        uptime_last_hour = 0
        downtime_last_hour = 0
        uptime_last_day = 0
        downtime_last_day = 0
        uptime_last_week = 0
        downtime_last_week = 0

        # Iterate through the store activity data
        for entry in store_activity_data.get(store_id, []):
            try:
                entry_timestamp = datetime.strptime(entry["timestamp_utc"], "%Y-%m-%d %H:%M:%S.%f UTC")
            except ValueError:
                entry_timestamp = datetime.strptime(entry["timestamp_utc"], "%Y-%m-%d %H:%M:%S UTC")
            entry_timestamp_local = tz.localize(entry_timestamp)

            # Check if the entry is within the last hour
            if last_hour_start <= entry_timestamp_local <= current_timestamp:
                if entry["status"] == "active":
                    uptime_last_hour += 1
                else:
                    downtime_last_hour += 1

            # Check if the entry is within the last day
            if last_day_start <= entry_timestamp_local <= current_timestamp:
                if entry["status"] == "active":
                    uptime_last_day += 1
                else:
                    downtime_last_day += 1

            # Check if the entry is within the last week
            if last_week_start <= entry_timestamp_local <= current_timestamp:
                if entry["status"] == "active":
                    uptime_last_week += 1
                else:
                    downtime_last_week += 1

        # Create a report with calculated values
        report = {
            "store_id": store_id,
            "uptime_last_hour": uptime_last_hour,
            "downtime_last_hour": downtime_last_hour,
            "uptime_last_day": uptime_last_day,
            "downtime_last_day": downtime_last_day,
            "uptime_last_week": uptime_last_week,
            "downtime_last_week": downtime_last_week
        }

        return report
    else:
        return None


# MongoDB client
mongo_client = MongoClient(mongo_params['host'], mongo_params['port'])
db = mongo_client[mongo_params['database_name']]

# /trigger_report endpoint
@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    # Query MongoDB collections to retrieve data
    store_activity_collection = db["store_status"]
    business_hours_collection = db["business_hours"]
    timezone_data_collection = db["timezone"]

    store_activity_data = {}
    for entry in store_activity_collection.find({}):
        store_id = entry["store_id"]
        if store_id not in store_activity_data:
            store_activity_data[store_id] = []
        store_activity_data[store_id].append(entry)

    business_hours_data = {}
    for entry in business_hours_collection.find({}):
        store_id = entry["store_id"]
        business_hours_data[store_id] = entry

    timezone_data = {entry["store_id"]: entry["timezone_str"] for entry in timezone_data_collection.find({})}

    # Calculate uptime and downtime for each store
    report_data = {}
    for store_id in store_activity_data:
        report = calculate_uptime_downtime(store_id, store_activity_data, business_hours_data, timezone_data)
        if report:
            report_data[store_id] = report

    # Generate a random report_id
    report_id = ''.join(random.choice('0123456789ABCDEF') for _ in range(8))
    reports[report_id] = report_data

    return jsonify({'report_id': report_id})


# /get_report endpoint
@app.route('/get_report/<report_id>', methods=['GET'])
def get_report(report_id):
    if report_id in reports:
        report_data = reports[report_id]
        if report_data:
            # Generate the CSV report
            csv_data = "store_id,uptime_last_hour,uptime_last_day,uptime_last_week,downtime_last_hour,downtime_last_day,downtime_last_week\n"
            for store_id, report in report_data.items():
                csv_data += f"{store_id},{report['uptime_last_hour']},{report['uptime_last_day']},{report['uptime_last_week']},{report['downtime_last_hour']},{report['downtime_last_day']},{report['downtime_last_week']}\n"

            # Generate a temporary CSV file and send it as a response
            with open(f'report_{report_id}.csv', 'w', newline='') as csvfile:
                csvfile.write(csv_data)

            return send_file(f'report_{report_id}.csv', as_attachment=True, download_name=f'report_{report_id}.csv')

        else:
            return "Running", 202
    else:
        return "Report not found", 404

if __name__ == '__main__':
    app.run(debug=True)
