from datetime import datetime
import glob
import os
import re

from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd
from sqlsorcery import MSSQL

from drive import Drive


def get_credentials():
    """Generate service account credentials object"""
    SCOPES = [
        "https://www.googleapis.com/auth/drive",
    ]
    return service_account.Credentials.from_service_account_file(
        "service.json", scopes=SCOPES, subject=os.getenv("ACCOUNT_EMAIL")
    )


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
        if col.startswith("Active"):  # only gets columns with 'Active_MM/DD'
            active_dates.append(col)
    dataframe = dataframe.melt(
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
    dataframe = dataframe[
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
    dataframe["Active_Date"] = (
        dataframe["Active_Date"].str.replace("Active_", "")
        + "/"
        + str(datetime.date(datetime.now()).year)
    )
    dataframe["WasActive"] = dataframe["WasActive"].fillna(
        0
    )  # fill null values with 0 to indicate the student is not active
    return dataframe


# Function to load data that we don't have
def load_newest_data(sql, df):
    table = sql.query(
        "SELECT TABLE_NAME FROM information_schema.tables WHERE table_name = 'SeeSaw_Student_Activity'"
    )
    if table["TABLE_NAME"][0] != None:
        time = sql.query(
            "SELECT MAX(Active_Date) AS Active_Date FROM custom.SeeSaw_Student_Activity"
        )
        latest_timestamp = time["Active_Date"][0]
        df = df[df["Active_Date"] > latest_timestamp]
        sql.insert_into("SeeSaw_Student_Activity", df)
    else:
        sql.insert_into("SeeSaw_Student_Activity", df, if_exists="replace")


def main():
    creds = get_credentials()
    service = build("drive", "v3", credentials=creds)
    drive = Drive(service)
    drive.get_file_metadata()
    drive.download_file()
    files = glob.glob("KIPP_Bay_Area_Schools*.csv")  # one each week
    df = create(files)
    df = clean_col(df)
    df = change_table(df)
    sql = MSSQL()
    load_newest_data(sql, df)


if __name__ == "__main__":
    main()
