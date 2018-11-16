from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= r"D:\Codes\pops-dab1c699446f.json"
# TODO(developer): Uncomment the lines below and replace with your values.
from google.cloud import bigquery
import time
client = bigquery.Client()
def _millis():
    return int(time.time() * 1000)
"""Insert / fetch table data."""
dataset_id = 'table_insert_rows_dataset_{}'.format(_millis())
table_id = 'table_insert_rows_table_{}'.format(_millis())
dataset = bigquery.Dataset(client.dataset(dataset_id))
dataset = client.create_dataset(dataset)
dataset.location = 'US'

table = bigquery.Table(dataset.table(table_id))
table = client.create_table(table)

# [START bigquery_table_insert_rows]
# TODO(developer): Uncomment the lines below and replace with your values.
from google.cloud import bigquery
client = bigquery.Client()
dataset_id = '2017'  # replace with your dataset ID
#For this sample, the table must already exist and have a defined schema
table_id = 'Demo_YouTube_popsww_M_2017'  # replace with your table ID
table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref)  # API request

rows_to_insert = [
    (u'2017-06-01', u'-04EHhvxtm8',u'Partner-provided',251,u'TeTLLdLD7lpRukrSpzTN2g',u'A345551560910413',u'1257/2016/HDHT-Pops-BillsWiliam',47,0.007768),
    (u'2017-06-01', u'-3mc9nPUyTA',u'Partner-provided',688,u'gb17I1QqqCmZg0QBYye3NA',u'A964471849055516',u'1752/2016/HDHT-Pops-VNEsports',47,0.007768),
]

errors = client.insert_rows(table, rows_to_insert)  # API request

assert errors == []
# [END bigquery_table_insert_rows]