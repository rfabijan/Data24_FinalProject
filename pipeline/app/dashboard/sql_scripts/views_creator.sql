/*
This script generates all the Views required for constructing the Dashboard.
A View is a contained query that generates a Table.

This file will be read by a Python script which seperates queries on the '--\n' character

The ORDER in which views are created matters. The pre-requisites for running this script are:
1. Data24ETL Database exists or is created
2. 19 Tables are present in the Data24ETL Database (as outlined in the ERD)
*/

USE [Data24ETLTest];

--
CREATE VIEW [SpartaDaysAtAcademies] AS
SELECT AcademyName, COUNT(*) AS 'SpartaDaysEventCount'
FROM [Data24ETLTest].[dbo].Academy a LEFT JOIN [Data24ETLTest].[dbo].SpartaDay sd ON a.AcademyID=sd.AcademyID
GROUP BY AcademyName;

--
CREATE VIEW [SpartaDaysPerMonth] AS
SELECT MONTH(SpartaDayDate) AS 'Month', COUNT(*) AS 'NumberOfSpartaDays'
FROM SpartaDay
GROUP BY MONTH(SpartaDayDate)

SELECT * FROM Applicants