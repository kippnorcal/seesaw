# seesaw

## Dependencies:
- Python3.7
- [Pipenv](https://pipenv.readthedocs.io/en/latest/)
- [Docker](https://www.docker.com/)

## Getting Started

### Setup Environment

1. Clone this repo
```
git clone https://github.com/kipp-bayarea/seesaw.git
```

2. Install dependencies
- Docker can be installed from docker.com

3. Create .env file with project secrets

The environment file should fit the following template:
# Google Drive Folder ID to extract from
FOLDER_ID=id number that comes after drive.google.com/drive/folders/{FOLDER_ID}

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

## Running the job

### Using Docker:
```
docker build -t seesaw .
```
```
docker run --rm -it seesaw
```

Pull SeeSaw activity logs from a Google Drive and load into Data Warehouse.
Before running the script, you need to create the table using the definition in the SeeSaw_Student_Activity.sql
