import io
import os

import pandas as pd
from googleapiclient.http import MediaIoBaseDownload

parent_folder = os.getenv("SEESAW_FOLDER")


class Drive:
    def __init__(self, service):
        self.service = service

    def get_file_metadata(self):
        """Get the latest SeeSaw activity csv and store its name and id."""
        options = {
            "q": f"'{parent_folder}' in parents and mimeType='text/csv'",  # exclude folder type
        }
        response = self.service.files().list(**options).execute()
        records = response.get("files", [])
        df = pd.DataFrame(records).sort_values("name", ascending=False)
        self.file_name = df["name"].iloc[0]
        self.file_id = df["id"].iloc[0]

    def download_file(self):
        """Download the data csv and save in project folder"""
        request = self.service.files().get_media(fileId=self.file_id)
        fh = io.FileIO(self.file_name, mode="wb")
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
