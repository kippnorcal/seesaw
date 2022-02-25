# SeeSaw
Request SeeSaw activity report export, retrieve the file from email, and load the data into database.


## Dependencies:
- Python3.7
- [Pipenv](https://pipenv.readthedocs.io/en/latest/)
- [Docker](https://www.docker.com/)


## Getting Started


### Setup Environment

1. Clone this repo
```
git clone https://github.com/kippnorcal/seesaw.git
```

2. Install dependencies

3. Create .env file with project secrets

```
# Google Drive Folder ID to extract from
SERVICE_ACCOUNT_EMAIL=

SEESAW_USER=
SEESAW_PASSWORD=

EXPORT_EMAIL_ADDRESS=email address that the export is sent to (should be same as SeeSaw login)
EXPORT_EMAIL_PASSWORD=email password

SCHOOLYEAR_4DIGIT=

# Database variables
DB_TYPE=The type of database you are using. Current options: mssql, postgres, sqlite
DB_SERVER=name of server
DB=name of database
DB_USER=username for database
DB_PWD=password for database
DB_SCHEMA=type of schema we are using in the database

# Email notification variables
SENDER_EMAIL=email of person sending
SENDER_PWD=password of person sending
RECIPIENT EMAIL=email of person receiving
# If using a standard Gmail account you can set these to smtp.gmail.com on port 465
EMAIL_SERVER=
EMAIL_PORT=

# optional
DEBUG_MODE=0
```

4. Create a service account with gmail read access, and save the service.json file in your project folder.

 - [Google documentation on service accounts](https://support.google.com/a/answer/7378726?hl=en)

5. Create the SeeSaw tables in your database.

 - Create the two tables `SeeSaw_Student_Activity` and `SeeSaw_Student_Activity_Weekly` using the table definitions in the sql folder.


## Running the job

### Using Docker:
```
docker build -t seesaw .
```
```
docker run --rm -it seesaw
```


## Maintenance
* Update SCHOOLYEAR_4DIGIT in `.env` file. This feeds into a column in the SeeSaw_Student_Activity_Weekly table.
* If a new school opens and uses SeeSaw, confirm that that school's data flows into the Student Activity Report export in SeeSaw.
* The connector can be turned off when school is out of session for the summer.
