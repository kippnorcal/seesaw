import os
from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth


folder_id = os.getenv("FOLDER")
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

file_list = drive.ListFile({'driveId': folder_id}).GetList()
for file1 in file_list:
    print(file1['title'])
# def uploader(title, path):
#     gfile = drive.CreateFile({"parents": [{"kind": "drive#fileLink", "id": folder_id}]})
#     gfile.SetContentFile(path)
#     gfile["title"] = title
#     gfile.Upload()