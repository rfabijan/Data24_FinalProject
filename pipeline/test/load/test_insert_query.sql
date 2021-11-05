-- Test insertion into the Data24ETL Database
/*
TechSkill
Strengths
Weaknesses
Academy
SpartaDay
Trainer
Course
CourseTrainer
CoreSkills
Streams
Invitors
Addresses
Applicants
ApplicantSpartaDay
TechSelfScore
ApplicantStrengths
ApplicantWeaknesses
Spartans
Tracker
*/

USE [Data24ETL];

DELETE FROM TechSkill;
INSERT INTO TechSkill
VALUES 
    ('C#'), 
    ('Java'), 
    ('R'), 
    ('JavaScript'), 
    ('SQL');
-- SELECT * FROM TechSkill;


DELETE FROM Strengths;
INSERT INTO Strengths
VALUES 
    ('Charisma'), 
    ('Charm'), 
    ('Ambitious'), 
    ('Teamplayer'), 
    ('Fast');
-- SELECT * FROM Strengths;


DELETE FROM Weaknesses;
INSERT INTO Weaknesses
VALUES 
    ('Weak'), 
    ('Slow'), 
    ('Intolerant'), 
    ('Distracted'), 
    ('Introverted');
-- SELECT * FROM Weaknesses;


DELETE FROM Academy;
INSERT INTO Academy
VALUES 
    ('London'), 
    ('Birmingham');
-- SELECT * FROM Academy;


DELETE FROM SpartaDay;
INSERT INTO SpartaDay
VALUES 
    (1, '2021-11-05'), 
    (1, '2021-10-25');
-- SELECT * FROM SpartaDay;
