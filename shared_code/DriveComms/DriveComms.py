from abc import abstractmethod, ABC
import json
import os
from pathlib import Path
from azure.storage.blob import BlobServiceClient

class DriveComms(ABC):

    @abstractmethod
    def save_file_bytes(self, io, filename, folder):
        pass

    @abstractmethod
    def copy_file(self, source_container:str, source_path:str, destination_container:str, destination_path:str):
        pass

    @abstractmethod
    def list_files(self, source_container:str, source_folder:str):
        pass


class LocalDriveComms(DriveComms):

    def __init__(self, local_root):
        self.local_root = local_root


    def save_file_bytes(self, io, filename, folder):
        folder = self.create_folder(folder)
        filepath = folder/filename
        with open(filepath, 'wb') as file:
            file.write(io)
        return filepath


    def create_folder(self, folder_name) -> Path:
        new_path = Path(f'{self.local_root}/{folder_name}')
        if not new_path.exists():
            os.mkdir(new_path)
        return new_path

    def copy_file(self, source_container:str, source_path:str, destination_container:str, destination_path:str):
        pass

    def list_files(self, source_container:str, source_folder:str):
        pass

class AzureDriveComms(DriveComms):

    def __init__(self, configs_path):
        with open(configs_path) as f:
            configs = json.load(f)
        self.service_client = BlobServiceClient.from_connection_string(configs['connection_string'])
        self.source_container = configs['source_container']
        self.destination_container = configs['destination_container']

    def save_file_bytes(self, io, filename, folder):
        file_path = f'{folder}/{filename}'
        client = self.service_client.get_blob_client(container=self.source_container, blob=file_path)
        client.upload_blob(io, overwrite=True)
        return filename

    def copy_file(self, source_path:str, destination_path:str):
        source_blob = self.service_client.get_blob_client(container = self.source_container, blob=source_path)
        destination_blob = self.service_client.get_blob_client(container=self.destination_container, blob=destination_path)
        destination_blob.start_copy_from_url(source_blob.url)
        return destination_blob.url

    def list_files(self, source_folder:str):
        files = self.service_client.get_container_client(self.source_container).list_blobs(name_starts_with=source_folder)
        return [file['name'] for file in files]