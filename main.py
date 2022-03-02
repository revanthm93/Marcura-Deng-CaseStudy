from flask import Flask, request,jsonify,json
from google.cloud import bigquery
import os
import pandas
import json

# Initialize Flask App
app = Flask(__name__)

#To-do [Move these credentails path to a seperate config]
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\revam\\Documents\\instant-basis-293115-6b820077ba7e.json"

# Initialize BQ client
client = bigquery.Client()

# Query to aggregate the data to get the desired result as per the given case study
query = (
    """
        SELECT passenger_count as passengerCount, 
        count(*) as numberOfTrips,
        SUM(TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE)) as totalTimeInMinutes
        FROM `bigquery-public-data.new_york.tlc_yellow_trips_2016`
        WHERE pickup_longitude BETWEEN -90 AND 90
        AND dropoff_longitude BETWEEN -90 AND 90
        AND pickup_latitude BETWEEN -90 AND 90
        AND dropoff_latitude BETWEEN -90 AND 90
        AND passenger_count BETWEEN 1 AND 6
        GROUP BY passenger_count
    """
)

# Flask APP route or endpoint
@app.route('/get_cab_info/', methods=['GET'])
def getCabInfo():
    dataframe = (client.query(query).result().to_dataframe(create_bqstorage_client=True, ))
    # Convert Pandas DF to Json Object
    result = dataframe.to_json(orient="records")
    parsed = json.loads(result)
    json_result = json.dumps(parsed, indent=4)
    return json_result


if __name__ == '__main__':
   app.run()