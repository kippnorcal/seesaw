CREATE TABLE custom.SeeSaw_Student_Activity_Weekly (
    School_Name VARCHAR(MAX),
    Student_Name VARCHAR(MAX),
    Student_ID VARCHAR(MAX),
    Grade_Level VARCHAR(MAX),
    Last_Active_Date VARCHAR(MAX),
    Days_Active_Past_Week INT,
    Posts_Added_Past_Week INT,
    Comments_Past_Week INT,
    Days_with_Posts_Added_Past_Week INT,
    Days_Commented_Past_Week INT,
    Date_Uploaded DATE,
    WeekStart DATE,
    WeekEnd DATE,
    SchoolYear4Digit INT
)
GO