'''
Step1: COMMAND PROMPT:
set GOOGLE_APPLICATION_CREDENTIALS= "D:\Codes\pops-dab1c699446f.json"
'''
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= "D:\Codes\pops-dab1c699446f.json"
from google.cloud import bigquery_datatransfer

client = bigquery_datatransfer.DataTransferServiceClient()
project = 'pops-204909'
# get full path to your project
parent = client.project_path(project)

print ("Supported Data Sources:")
# Interate over all possible data scource
for data_source in client.list_data_sources(parent):
    print ("{}".format(data_source.display_name))
    print ("\tID: {}".format(data_source.data_source_id))
    print ("\tFull path: {}".format(data_source.name))
    print ("\tDescription: {}".format(data_source.description))
