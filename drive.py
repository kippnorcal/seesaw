import io
import os

import pandas as pd
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from google.oauth2 import service_account

parent_folder = os.getenv("SEESAW_FOLDER")


class DriveFolder:
    def __init__(self):
        self.creds = self.get_credentials()
        self.service = build("drive", "v3", credentials=self.creds)

    def get_credentials(self):
        """Generate service account credentials object"""
        SCOPES = [
            "https://www.googleapis.com/auth/drive",
        ]
        return service_account.Credentials.from_service_account_file(
            "service.json", scopes=SCOPES, subject=os.getenv("ACCOUNT_EMAIL")
        )

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
