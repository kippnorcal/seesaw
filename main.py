# Step 1: Connect to Google Drive (PyDrive)
# Step 2: Download files from Drive
# Step 2.5: Ensure we downloaded correct files (1 for each of 3 schools)
# Step 3: Read csv into dataframe using pandas, union into 1 DF
# Step 3.5: Clean the data inclusive of deleting columns (A-D)
# new column = last updated date & formatting
# Step 4: Write DF into data warehouse

# Goal 1: Connect to PyDrive, download the files into our local folder
# Goal 2: Read into pandas & data transformation
# Goal 3: Load the df into warehouse
import os
from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
from sqlsorcery import MSSQL
import pandas as pd
import glob
from datetime import datetime
import re


# TO DO:
# folder_id = os.getenv("FOLDER")
# gauth = GoogleAuth()
# gauth.LocalWebserverAuth()
# drive = GoogleDrive(gauth)
# file_list = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
# file_list
# # file_list = drive.ListFile({'driveId': folder_id, 'includeItemsFromAllDrives': True, 'corpora': 'allDrives'
# # , 'supportsAllDrives': True}).GetList()
# print(file_list)
# for file1 in file_list:
#     print(file1['title'])


# Function that will create my dataframe
def create(files_array):
    df = pd.read_csv(files_array[0], sep=",", header=1)
    df["Date Uploaded"] = datetime.date(datetime.now())
    return df


# Function that will delete the unnecessary columns we do not need
def clean_col(dataframe):
    dataframe = dataframe.drop(
        columns=[
            "Days Active in Past Week",
            "Posts Added to Student Journal in Past Week",
            "Comments in Past Week",
            "Posts Added to Student Journal Yesterday",
            "Comments Yesterday",
            "Days with Posts Added to Student Journal In Past Week",
            "Days Commented in Past Week",
            "Connected Family Members",
            "Active Yesterday (1 = yes)",
            "Active in Last 7 Days (1 = yes)",
            "Link to School Dashboard",
        ]
    )
    # rename the columns to add '_' as spaces to easily query from DB
    dataframe.columns = dataframe.columns.str.replace(" ", "_")
    return dataframe


# Function that will pivot the table to have different rows for different active dates for students
def change_table(dataframe):
    active_dates = []
    for col in dataframe.columns:
        if "Active" in col:
            active_dates.append(col)
    df = dataframe.melt(
        id_vars=[
            "School_Name",
            "Student_Name",
            "Student_ID",
            "Grade_Level",
            "Last_Active_Date",
            "Date_Uploaded",
            "Link_to_Student_Portfolio",
        ],
        value_vars=active_dates,
        var_name="Active_Date",
        value_name="WasActive",
    )
    # rearrange the columns
    df = df[
        [
            "School_Name",
            "Student_Name",
            "Student_ID",
            "Grade_Level",
            "Active_Date",
            "WasActive",
            "Last_Active_Date",
            "Date_Uploaded",
            "Link_to_Student_Portfolio",
        ]
    ]
    # reformat the 'Active Date' values to only date
    # Before the transformation, 'Active Date' had 'Active_MM/DD' but we want to strip it down to only the date
    # Example: 'Active_08/16' is now '08/16/2020'
    df["Active_Date"] = (
        df["Active_Date"].str.replace("Active_", "")
        + "/"
        + str(datetime.date(datetime.now()).year)
    )
    return df


# Function to load data that we don't have
def load_newest_data(sql, df):
    time = sql.query("SELECT MAX(Active_Date) FROM custom.SeeSaw_Student_Activity")
    if time != None:
        latest_timestamp = time["Active_Date"][0]
        df = df[df["Active_Date"] > latest_timestamp]
        sql.insert_into("SeeSaw_Student_Activity", df)
    else:
        sql.insert_into("SeeSaw_Student_Activity", df, if_exists="replace")


def main(files):
    df = create(files)
    df = clean_col(df)
    # print(df)
    # print(df.columns)
    # df = change_table(df)
    # print(df)
    sql = MSSQL()
    # load_newest_data(sql, df)


if __name__ == "__main__":
    files = glob.glob("KIPP_Bay_Area_Schools*.csv")  # one each week
    main(files)
