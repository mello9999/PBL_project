import os
from azure.storage.blob import BlobServiceClient,ContainerClient, BlobClient
import datetime


# Assuming your Azure connection string environment variable set.
# If not, create BlobServiceClient using trl & credentials.
#Example: https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient 

connection_string = ("DefaultEndpointsProtocol=https;AccountName=fuze1;AccountKey=4wE3ziTUR57rqg6QvQVuXtqzNpa9GHoKSsQT9XmbrhLFlDjDT/o/eY1zXTQePfYlevp02v0+FD8r52r2GSNbwg==;EndpointSuffix=core.windows.net")

blob_service_client = BlobServiceClient.from_connection_string(connection_string) 
# create container client
container_name = 'data'
container_client = blob_service_client.get_container_client(container_name)

#Check if there is a top level local folder exist for container.
#If not, create one
data_dir ='D:/D'
data_dir = data_dir+ "/" + container_name
if not(os.path.isdir(data_dir)):
    print("[{}]:[INFO] : Creating local directory for container".format(datetime.datetime.utcnow()))
    os.makedirs(data_dir, exist_ok=True)

def download_blob(blob_client, destination_file):
    print("[{}]:[INFO] : Downloading {} ...".format(datetime.datetime.utcnow(),destination_file))
    with open(destination_file, "wb") as my_blob:
        blob_data = blob_client.download_blob()
        blob_data.readinto(my_blob)
    print("[{}]:[INFO] : download finished".format(datetime.datetime.utcnow()))    


#code below lists all the blobs in the container and downloads them one after another
blob_list = container_client.list_blobs()
for blob in blob_list:
    print("[{}]:[INFO] : Blob name: {}".format(datetime.datetime.utcnow(), blob.name))
    #check if the path contains a folder structure, create the folder structure
    if "/" in "{}".format(blob.name):
        #extract the folder path and check if that folder exists locally, and if not create it
        head, tail = os.path.split("{}".format(blob.name))
        if not (os.path.isdir(data_dir+ "/" + head)):
            #create the diretcory and download the file to it
            print("[{}]:[INFO] : {} directory doesn't exist, creating it now".format(datetime.datetime.utcnow(),data_dir+ "/" + head))
            os.makedirs(data_dir+ "/" + head, exist_ok=True)
    # Finally, download the blob
    blob_client = container_client.get_blob_client(blob.name)
    download_blob(blob_client,data_dir+ "/"+blob.name)

