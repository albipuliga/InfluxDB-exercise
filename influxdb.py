import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from sqlalchemy import create_engine
import pandas as pd

token = os.environ.get("INFLUXDB_TOKEN")
org = "IEU"
url = "http://localhost:8086"

write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
bucket="sakila_db"

write_api = write_client.write_api(write_options=SYNCHRONOUS)
   
engine = create_engine('mysql://root:djangodata@localhost/sakila')

query = """
SELECT c.name AS category, COUNT(r.rental_id) AS rental_count
FROM category c
JOIN film_category fc ON c.category_id = fc.category_id
JOIN film f ON fc.film_id = f.film_id
JOIN inventory i ON f.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
GROUP BY category;
"""

# Execute the SQL query and load the results into a pandas DataFrame
rental_data = pd.read_sql(query, engine)
   

for value in range(5):
  point = (
    Point("measurement1")
    .tag("tagname1", "tagvalue1")
    .field("field1", value)
  )
  write_api.write(bucket=bucket, org="IEU", record=point)
  time.sleep(1) # separate points by 1 second
