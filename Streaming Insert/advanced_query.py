from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= "D:\Codes\pops-dab1c699446f.json"
client = bigquery.Client()
# Perform a query.
QUERY = ('''your query''' )
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish
for row in rows:
    print(row)