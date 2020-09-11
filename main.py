from datetime import datetime
import glob
import os
import re
import logging
import traceback


import pandas as pd
from sqlsorcery import MSSQL

from drive import DriveFolder
from mailer import Mailer


def configure_logging(config):
    logging.basicConfig(
        handlers=[
            logging.FileHandler(filename="data/app.log", mode="w+"),
            logging.StreamHandler(sys.stdout),
        ],
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %I:%M:%S%p %Z",
    )
    logging.getLogger("google_auth_oauthlib").setLevel(logging.ERROR)
    logging.getLogger("googleapiclient").setLevel(logging.ERROR)
    logging.getLogger("google").setLevel(logging.ERROR)


def create(files_array):
    """Function that will create my dataframe"""
    df = pd.read_csv(files_array[0], sep=",", header=1)
    df["Date Uploaded"] = datetime.date(datetime.now())
    return df


def clean_col(df):
    """Function that will delete the unnecessary columns we do not need"""
    df = df.drop(
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
    df.columns = df.columns.str.replace(" ", "_")
    return df


def change_table(df):
    """Function that will pivot the table to have different rows for different active dates for students"""
    active_dates = []
    for col in df.columns:
        if col.startswith("Active"):  # only gets columns with 'Active_MM/DD'
            active_dates.append(col)
    df = df.melt(
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
    return df


def change_col(df):
    """reformat the 'Active Date' values to only date
    Before the transformation, 'Active Date' had 'Active_MM/DD' but we want to strip it down to only the date
    Example: 'Active_08/16' is now '08/16/2020'"""
    # df["Active_Date"] = (
    #     df["Active_Date"].str.replace("Active_", "")
    #     + "/"
    #     + str(datetime.date(datetime.now()).year)
    # )
    df["Active_Date"] = df["Active_Date"].str.replace("Active_", "")
    df["Active_Date"] = df["Active_Date"].apply(
        lambda x: f"{x}/{str(datetime.date(datetime.now()).year)}"
    )
    not_active = 0
    df["WasActive"] = df["WasActive"].fillna(not_active)
    df["Active_Date"] = df["Active_Date"].astype("datetime64[ns]")
    return df


def load_newest_data(sql, df):
    """Function to load data that we don't have"""
    time = sql.query(
        "SELECT MAX(Active_Date) AS Active_Date FROM custom.SeeSaw_Student_Activity"
    )
    latest_timestamp = time["Active_Date"][0]
    if latest_timestamp != None:
        df = df[df["Active_Date"] > latest_timestamp]
    sql.insert_into("SeeSaw_Student_Activity", df)
    logging.info(f"Inserted {len(df)} new records into SeeSaw_Student_Activity.")


def main():
    drive = DriveFolder()
    drive.get_file_metadata()
    drive.download_file()
    files = glob.glob("KIPP_Bay_Area_Schools*.csv")  # one each week
    df = create(files)
    df = clean_col(df)
    df = change_table(df)
    df = change_col(df)
    sql = MSSQL()
    load_newest_data(sql, df)


if __name__ == "__main__":
    try:
        main()
        error_message = None
    except Exception as e:
        logging.exception(e)
        error_message = traceback.format_exc()
    Mailer("SeeSaw_Activity").notify(error_message=error_message)
