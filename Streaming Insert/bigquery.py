from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= "D:\Codes\pops-dab1c699446f.json"
client = bigquery.Client()
# Perform a query.
QUERY = ('SELECT * FROM `pops-204909.2017.Demo_YouTube_popsww_M_2017`' 'LIMIT 100')
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row)