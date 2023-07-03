import os
from azure.storage.blob import BlobServiceClient, BlobClient

def load_blob_storage(storage_connection_string, container_name, source_directory):
    # Create a BlobServiceClient using the storage connection string
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)

    # Get a reference to the container
    container_client = blob_service_client.get_container_client(container_name)

    # List all files in the source directory
    for file_name in os.listdir(source_directory):
        source_file_path = os.path.join(source_directory, file_name)

        blob_client = container_client.get_blob_client(file_name)

        with open(source_file_path, "rb") as data:
            blob_client.upload_blob(data)

        print(f"File '{source_file_path}' uploaded to Blob Storage.")

# Example usage
storage_connection_string = ""
container_name = "testtech"
source_directory = "../Airport_Pipeline/Airport_data"

load_blob_storage(storage_connection_string, container_name, source_directory)
