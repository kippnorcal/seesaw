import base64
import datetime as dt
from datetime import datetime
import logging
import os
import requests
import sys
import time
import traceback

from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from sqlsorcery import MSSQL
from tenacity import retry, stop_after_attempt, wait_exponential, TryAgain

from mailer import Mailer


def configure_logging():
    logging.basicConfig(
        handlers=[
            logging.FileHandler(filename="app.log", mode="w+"),
            logging.StreamHandler(sys.stdout),
        ],
        level=logging.DEBUG if int(os.getenv("DEBUG_MODE")) else logging.INFO,
        format="%(asctime)s | %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %I:%M:%S%p %Z",
    )
    logging.getLogger("google_auth_oauthlib").setLevel(logging.ERROR)
    logging.getLogger("googleapiclient").setLevel(logging.ERROR)
    logging.getLogger("google").setLevel(logging.ERROR)


def request_report_export():
    """Selenium steps to request report export in SeeSaw."""
    browser = create_driver()
    browser.implicitly_wait(5)
    login(browser)
    get_student_activity_report(browser)
    logging.debug("Requested report export.")


def create_driver():
    """Create browser driver."""
    profile = webdriver.FirefoxProfile()
    return webdriver.Firefox(firefox_profile=profile)


def login(browser):
    """Log into SeeSaw application as an administrator."""
    browser.get("https://app.seesaw.me/#/login?role=org_admin")
    time.sleep(5)
    user_field = browser.find_element_by_id("sign_in_email")
    user_field.send_keys(os.getenv("SEESAW_USER"))
    password_field = browser.find_element_by_id("sign_in_password")
    password_field.send_keys(os.getenv("SEESAW_PASSWORD"))
    submit_button = browser.find_element_by_xpath(
        "//button[text()=' Administrator Sign In ']"
    )
    submit_button.click()


def get_student_activity_report(browser):
    """Click to request student activity report export"""
    WebDriverWait(browser, 100).until(
        EC.element_to_be_clickable(
            (By.XPATH, "//a[@ng-click='startStudentActivityReportForDistrict()']")
        )
    ).click()
    browser.implicitly_wait(5)
    WebDriverWait(browser, 100).until(
        EC.element_to_be_clickable(
            (By.XPATH, "//div[text()='Get Student Activity Report']")
        )
    ).click()


def get_credentials():
    """Generate service account credentials object (used for Google API)"""
    SCOPES = [
        "https://www.googleapis.com/auth/gmail.readonly",
    ]
    return service_account.Credentials.from_service_account_file(
        "service.json", scopes=SCOPES, subject=os.getenv("SERVICE_ACCOUNT_EMAIL")
    )


def save_data_file(link_text):
    """Use requests to download the file through the link."""
    r = requests.get(link_text)
    with open("activity_data.csv", "wb") as f:
        f.write(r.content)
    logging.info("Downloaded SeeSaw Activity file from email.")


def retrieve_activity_data(gmail_service):
    """Find the download link in email and get the file"""
    message_id = retrieve_message_id(gmail_service)
    link_text = parse_email_message(gmail_service, message_id)
    save_data_file(link_text)
    df = pd.read_csv("activity_data.csv", sep=",", header=1)
    logging.info(f"Read {len(df)} records from csv into df.")
    return df


@retry(
    wait=wait_exponential(multiplier=2, min=30, max=120), stop=stop_after_attempt(10)
)
def retrieve_message_id(service):
    """Find the message id from today that matches the subject."""
    today = dt.datetime.now().strftime("%A, %B %d, %Y")
    query = f"from:do-not-reply@seesaw.me Student Activity Report for KIPP Bay Area Schools on {today}"
    results = (
        service.users()
        .messages()
        .list(userId="me", q=query,)  # same user as service account login
        .execute()
    )
    if results.get("resultSizeEstimate") != 0:
        # only need one message if there are multiple within a day
        # SeeSaw data returns from previous day onward
        logging.info(f"Found email message for {today}.")
        return results.get("messages")[0].get("id")
    else:
        raise Exception("Email message not found in inbox.")


@retry(wait=wait_exponential(multiplier=2, min=10, max=40), stop=stop_after_attempt(5))
def parse_email_message(gmail_service, message_id):
    """Get download link from message parts of the given message id."""
    results = gmail_service.users().messages().get(userId="me", id=message_id).execute()
    parts = results.get("payload").get("parts")
    if not parts:
        raise TryAgain  # sometimes the message is found but parts returns empty
    else:
        link_text = None
        for part in parts:
            if part.get("mimeType") == "text/html":
                link_text = find_download_link(part)
                return link_text


def find_download_link(part):
    """Find download link within the email html body"""
    # decode base64 message part
    body_data = part.get("body").get("data").replace("-", "+").replace("_", "/")
    message = base64.b64decode(bytes(body_data, "UTF-8"))
    # use beautifulsoup to find the csv hyperlink text
    soup = BeautifulSoup(message, features="html.parser")
    message_body = soup.body()
    links = soup.find_all("a")
    for link in links:
        if ".csv" in link.get("href"):
            return link.get("href")
    return None


def create_extract_date(df):
    """Function that will create a column in dataframe that specifies when file was uploaded"""
    df["Date Uploaded"] = datetime.date(datetime.now())
    return df


def process_daily_activity(sql, df):
    """ETL daily activity columns from df into data warehouse."""
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
        ],
    )
    df.columns = df.columns.str.replace(" ", "_")
    df = pivot_by_date(df)
    df = reformat_active_date(df)
    load_newest_table_data(sql, df, "SeeSaw_Student_Activity")


def pivot_by_date(df):
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


def reformat_active_date(df):
    """reformat the 'Active Date' values to only date
    Before the transformation, 'Active Date' had 'Active_MM/DD' but we want to strip it down to only the date
    Example: 'Active_08/16' is now '08/16/2020'"""
    df["Active_Date"] = df["Active_Date"].str.replace("Active_", "")
    df["Active_Date"] = df["Active_Date"].apply(
        lambda x: f"{x}/{str(datetime.date(datetime.now()).year)}"
    )
    not_active = 0
    df["WasActive"] = df["WasActive"].fillna(not_active)
    df["Active_Date"] = df["Active_Date"].astype("datetime64[ns]")
    return df


def process_weekly_activity(sql, df):
    """ETL weekly activity columns from df into data warehouse."""
    columns = {
        "School Name": "School_Name",
        "Student Name": "Student_Name",
        "Student ID": "Student_ID",
        "Grade Level": "Grade_Level",
        "Last Active Date": "Last_Active_Date",
        "Days Active in Past Week": "Days_Active_Past_Week",
        "Posts Added to Student Journal in Past Week": "Posts_Added_Past_Week",
        "Comments in Past Week": "Comments_Past_Week",
        "Posts Added to Student Journal Yesterday": "Days_with_Posts_Added_Past_Week",
        "Days Commented in Past Week": "Days_Commented_Past_Week",
        "Date Uploaded": "Date_Uploaded",
    }
    df = df[columns.keys()].copy()
    df.rename(columns=columns, inplace=True)
    df = read_week_date_range_from_file(df)
    sql.insert_into("SeeSaw_Student_Activity_Weekly", df)
    logging.info(f"Inserted {len(df)} new records into SeeSaw_Student_Activity_Weekly.")


def read_week_date_range_from_file(df):
    """Get the week range from the file and store in df.
    
    The first row of the file is a string that indicates the date range.
    The csv data doesn't start until the second row.
    """
    with open("activity_data.csv") as f:
        first_line = f.readline()
    dates = [string.strip() for string in first_line.split(" - ")]
    df["WeekStart"] = dt.datetime.strptime(dates[0], "%Y-%m-%d %H:%M %Z%z").date()
    df["WeekEnd"] = dt.datetime.strptime(dates[1], "%Y-%m-%d %H:%M %Z%z").date()
    return df


def load_newest_table_data(sql, df, table_name):
    """Insert the newest data into the given database table, based on Last_Active_Date column.
    
    table_name: the name of the table that we're inserting data into
    """
    if sql.engine.has_table(table_name, schema="custom"):
        time = sql.query(
            f"SELECT MAX(Last_Active_Date) AS Last_Active_Date FROM custom.{table_name}"
        )
        latest_timestamp = time["Last_Active_Date"][0]
        if latest_timestamp != None:
            df = df[df["Last_Active_Date"] > latest_timestamp]
    sql.insert_into(table_name, df)
    logging.info(f"Inserted {len(df)} new records into {table_name}.")


def main():
    sql = MSSQL()
    configure_logging()
    creds = get_credentials()
    gmail_service = build("gmail", "v1", credentials=creds)
    request_report_export()
    df = retrieve_activity_data(gmail_service)
    df = create_extract_date(df)
    process_daily_activity(sql, df)
    process_weekly_activity(sql, df)


if __name__ == "__main__":
    try:
        main()
        error_message = None
    except Exception as e:
        logging.exception(e)
        error_message = traceback.format_exc()
    Mailer("SeeSaw_Activity").notify(error_message=error_message)
