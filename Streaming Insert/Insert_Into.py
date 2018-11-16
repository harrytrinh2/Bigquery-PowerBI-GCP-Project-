# -*- coding: utf-8 -*-
from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= r"D:\Codes\pops-dab1c699446f.json"
# TODO(developer): Uncomment the lines below and replace with your values.
from google.cloud import bigquery
client = bigquery.Client()
dataset_id = u'2017'  # replace with your dataset ID
#For this sample, the table must already exist and have a defined schema

table_id = u'Demo_YouTube_popsww_M_2017'  # replace with your table ID
table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref)  # API request

rows_to_insert = [
    (u'2000-11-11', u'-11111',u'1111111111111',111,u'111111111111111',u'11111111111111','phúc còi',111,11.11)
]

client.insert_rows(table, rows_to_insert)  # API request