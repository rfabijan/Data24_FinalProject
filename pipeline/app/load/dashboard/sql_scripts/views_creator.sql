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
CREATE VIEW TrackerSummary AS
SELECT      SpartanID,
            MAX(Week) AS 'LengthOnCourse',
            AVG(cast(SkillValue AS FLOAT)) AS 'AvgSkillValue',
            MIN(SkillValue) AS 'MinSkillValue',
            MAX(SkillValue) AS 'MaxSkilLValue'
FROM        Tracker
GROUP BY    SpartanID;

--
CREATE VIEW ActualSpartansPerWeek AS
SELECT      Week,
            COUNT(DISTINCT (SpartanID)) AS 'ActualSpartans'
FROM        Tracker
GROUP BY    Week;

--
CREATE VIEW SpartansPerCourse AS
SELECT      CourseID, 
            COUNT(SpartanID) AS 'SpartansOnCourse'
FROM        Spartans
GROUP BY    CourseID;

--
CREATE VIEW CoursesSummary AS
SELECT      c.*,
            spc.SpartansOnCourse
FROM        SpartansPerCourse spc LEFT JOIN Course c ON spc.CourseID=c.CourseID;

--
CREATE VIEW ExpectedSpartansPerWeek AS
SELECT      DISTINCT(WeekLength) AS "Week", 
            SUM(SpartansOnCourse) OVER(ORDER BY WeekLength DESC) AS 'ExpectedSpartans'
FROM        CoursesSummary;

--
CREATE VIEW SpartanExpectedCourseLength AS
SELECT      s.SpartanID, s.ApplicantID, 
            c.CourseID, c.WeekLength AS 'ExpectedLength'
FROM        Spartans s LEFT JOIN Course c ON s.CourseID=c.CourseID;

--
CREATE VIEW SpartanActualCourseLength AS
SELECT      SpartanID, MAX(Week) AS 'ActualLength'
FROM        Tracker
GROUP BY    SpartanID;

--
CREATE VIEW SpartanLengthActualVsExpected AS
SELECT      e.SpartanID, e.ApplicantID, e.CourseID, e.ExpectedLength,
            CASE WHEN a.ActualLength IS NULL THEN 0 ELSE a.ActualLength END AS "ActualLength"
FROM        SpartanExpectedCourseLength e LEFT JOIN SpartanActualCourseLength a ON e.SpartanID=a.SpartanID;

--
CREATE VIEW SpartansRemoved AS
SELECT      *
FROM        SpartanLengthActualVsExpected
WHERE       ActualLength < ExpectedLength;

--
CREATE VIEW SpartansRemovedTechSkills AS
SELECT      sr.SpartanID, sr.ApplicantID,
            CONCAT(a.FirstName, ' ', a.LastName) AS "Name",
            ts.SkillName, tss.Score
FROM        SpartansRemoved sr LEFT JOIN Applicants a ON sr.ApplicantID=a.ApplicantID  -- Dont need this join really
            LEFT JOIN TechSelfScore tss ON sr.ApplicantID=tss.ApplicantID
            LEFT JOIN TechSkill ts ON tss.TechSkillID=ts.TechSkillID;


--
CREATE VIEW SpartansRemovedStrength AS
SELECT      sr.SpartanID, sr.ApplicantID,
            CONCAT(a.FirstName, ' ', a.LastName) AS "Name",
            st.Strength
FROM        SpartansRemoved sr LEFT JOIN Applicants a ON sr.ApplicantID=a.ApplicantID
            LEFT JOIN ApplicantStrengths ast ON sr.ApplicantID=ast.ApplicantID
            LEFT JOIN Strengths st ON ast.StrengthID=st.StrengthID;

--
CREATE VIEW SpartansRemovedWeaknesses AS
SELECT      sr.SpartanID, sr.ApplicantID,
            CONCAT(a.FirstName, ' ', a.LastName) AS "Name",
            w.Weakness
FROM        SpartansRemoved sr LEFT JOIN Applicants a ON sr.ApplicantID=a.ApplicantID
            LEFT JOIN ApplicantWeaknesses aw ON sr.ApplicantID=aw.ApplicantID
            LEFT JOIN Weaknesses w ON aw.WeaknessID=w.WeaknessID;

--
CREATE VIEW SpartansRemovedTestScores AS
SELECT      sr.SpartanID, sr.ApplicantID,
            asd.PsychometricScore, asd.PresentationScore
FROM        SpartansRemoved sr LEFT JOIN ApplicantSpartaDay asd ON sr.ApplicantID=asd.ApplicantID;

--
-- CREATE VIEW SpartansComplete AS
-- SELECT      *
-- FROM        SpartanLengthActualVsExpected
-- WHERE       ActualLength >= ExpectedLength;

CREATE VIEW SpartansCompleteTrackerSummary AS
SELECT      CONCAT(a.FirstName, ' ', a.LastName) AS "Name",
            ts.*
FROM        SpartansComplete sc LEFT JOIN TrackerSummary ts ON sc.SpartanID=ts.SpartanID
            LEFT JOIN Applicants a ON sc.ApplicantID=a.ApplicantID

--
CREATE VIEW SpartansCompleteTechSkills AS
SELECT      sr.SpartanID, sr.ApplicantID,
            CONCAT(a.FirstName, ' ', a.LastName) AS "Name",
            ts.SkillName, tss.Score
FROM        SpartansComplete sr LEFT JOIN Applicants a ON sr.ApplicantID=a.ApplicantID  -- Dont need this join really
            LEFT JOIN TechSelfScore tss ON sr.ApplicantID=tss.ApplicantID
            LEFT JOIN TechSkill ts ON tss.TechSkillID=ts.TechSkillID;


--
CREATE VIEW SpartansCompleteStrength AS
SELECT      sr.SpartanID, sr.ApplicantID,
            CONCAT(a.FirstName, ' ', a.LastName) AS "Name",
            st.Strength
FROM        SpartansComplete sr LEFT JOIN Applicants a ON sr.ApplicantID=a.ApplicantID
            LEFT JOIN ApplicantStrengths ast ON sr.ApplicantID=ast.ApplicantID
            LEFT JOIN Strengths st ON ast.StrengthID=st.StrengthID;

--
CREATE VIEW SpartansCompleteWeaknesses AS
SELECT      sr.SpartanID, sr.ApplicantID,
            CONCAT(a.FirstName, ' ', a.LastName) AS "Name",
            w.Weakness
FROM        SpartansComplete sr LEFT JOIN Applicants a ON sr.ApplicantID=a.ApplicantID
            LEFT JOIN ApplicantWeaknesses aw ON sr.ApplicantID=aw.ApplicantID
            LEFT JOIN Weaknesses w ON aw.WeaknessID=w.WeaknessID;

--
CREATE VIEW SpartansCompleteTestScores AS
SELECT      sr.SpartanID, sr.ApplicantID,
            asd.PsychometricScore, asd.PresentationScore
FROM        SpartansComplete sr LEFT JOIN ApplicantSpartaDay asd ON sr.ApplicantID=asd.ApplicantID;
