/*
This Script will create a Data24ETLTest Database
*/

-- 1. Create the Database
USE master;
GO
DROP DATABASE IF EXISTS [Data24ETLTest];
GO
CREATE DATABASE [Data24ETLTest];
GO
USE [Data24ETLTest];
GO



-- 2. Build the tables
CREATE TABLE TechSkill
(
    TechSkillID             INT IDENTITY(1,1) PRIMARY KEY,
    SkillName               VARCHAR(32) NOT NULL
)

CREATE TABLE Strengths
(
    StrengthID              INT IDENTITY(1,1) PRIMARY KEY,
    Strength                VARCHAR(32) NOT NULL
)

CREATE TABLE Weaknesses
(
    WeaknessID              INT IDENTITY(1,1) PRIMARY KEY,
    Weakness                VARCHAR(32) NOT NULL
)

CREATE TABLE Academy
(
    AcademyID               INT IDENTITY(1,1) PRIMARY KEY,
    AcademyName             VARCHAR(64) NOT NULL
)

CREATE TABLE SpartaDay
(
    SpartaDayID             INT IDENTITY(1,1) PRIMARY KEY,
    AcademyID               INT NOT NULL FOREIGN KEY REFERENCES Academy(AcademyID),
    SpartaDayDate           DATE
)

CREATE TABLE Trainer
(
    TrainerID               INT IDENTITY(1,1) PRIMARY KEY,
    FirstName               VARCHAR(64) NOT NULL,
    LastName                VARCHAR(64)
)

CREATE TABLE Course
(
    CourseID                INT IDENTITY(1,1) PRIMARY KEY,
    CourseName              VARCHAR(64) NOT NULL,
    WeekLength              INT NOT NULL,
    StartDate               DATE NOT NULL
)

CREATE TABLE CourseTrainer
(
    CourseID                INT NOT NULL FOREIGN KEY REFERENCES Course(CourseID),
    TrainerID               INT NOT NULL FOREIGN KEY REFERENCES Trainer(TrainerID)
)

CREATE TABLE CoreSkills
(
    CoreSkillID             INT IDENTITY(1,1) PRIMARY KEY,
    SkillName               VARCHAR(16) NOT NULL
)

CREATE TABLE Streams
(
    StreamID                INT IDENTITY(1,1) PRIMARY KEY,
    StreamName              VARCHAR(64) NOT NULL
)

CREATE TABLE Invitors
(
    InvitorID               INT IDENTITY(1,1) PRIMARY KEY,
    FirstName               VARCHAR(32) NOT NULL,
    LastName                VARCHAR(32)
)

CREATE TABLE Addresses
(
    AddressID               INT IDENTITY(1,1) PRIMARY KEY,
    HouseNumber             INT,
    AddressLine             VARCHAR(256) NOT NULL,
    Postcode                VARCHAR(8),
    City                    VARCHAR(58)
)

CREATE TABLE Applicants
(
    ApplicantID             VARCHAR(64) PRIMARY KEY,
    StreamInterestID        INT FOREIGN KEY REFERENCES Streams(StreamID),
    InvitedByID             INT FOREIGN KEY REFERENCES Invitors(InvitorID),
    AddressID               INT FOREIGN KEY REFERENCES Addresses(AddressID),
    FirstName               VARCHAR(32) NOT NULL,
    LastName                VARCHAR(32),
    Gender                  VARCHAR(6),
    DOB                     DATE,
    Email                   VARCHAR(256),
    PhoneNumber             VARCHAR(13),
    Uni                     VARCHAR(128),
    Degree                  VARCHAR(4),
    GeoFlex                 BIT,
    FinancialSupportSelf    BIT,
    Result                  BIT
)

CREATE TABLE ApplicantSpartaDay
(
    ApplicantID             VARCHAR(64) FOREIGN KEY REFERENCES Applicants(ApplicantID),
    SpartaDayID             INT FOREIGN KEY REFERENCES SpartaDay(SpartaDayID),
    PsychometricScore       FLOAT NOT NULL,
    PresentationScore       FLOAT NOT NULL
)

CREATE TABLE TechSelfScore
(
    ApplicantID             VARCHAR(64) FOREIGN KEY REFERENCES Applicants(ApplicantID),
    TechSkillID             INT FOREIGN KEY REFERENCES TechSkill(TechSkillID),
    Score                   INT NOT NULL
)

CREATE TABLE ApplicantStrengths
(
    ApplicantID             VARCHAR(64) FOREIGN KEY REFERENCES Applicants(ApplicantID),
    StrengthID              INT FOREIGN KEY REFERENCES Strengths(StrengthID)
)

CREATE TABLE ApplicantWeaknesses
(
    ApplicantID             VARCHAR(64) FOREIGN KEY REFERENCES Applicants(ApplicantID),
    WeaknessID              INT FOREIGN KEY REFERENCES Weaknesses(WeaknessID)
)

CREATE TABLE Spartans
(
    SpartanID               INT IDENTITY(1,1) PRIMARY KEY,
    ApplicantID             VARCHAR(64) FOREIGN KEY REFERENCES Applicants(ApplicantID) UNIQUE,
    CourseID                INT FOREIGN KEY REFERENCES Course(CourseID)
)

CREATE TABLE Tracker
(
    SpartanID               INT FOREIGN KEY REFERENCES Spartans(SpartanID),
    CoreSkillID             INT FOREIGN KEY REFERENCES CoreSkills(CoreSkillID),
    Week                    INT NOT NULL,
    SkillValue              INT NOT NULL
)



-- 3. Populate the Database
INSERT INTO Streams
VALUES ('Data'), ('SDET'), ('DevOps'), ('Engineering')

INSERT INTO Invitors
VALUES ('Bruno', 'Bellbrook'), ('Doris', 'Bellasis'), ('Fifi', 'Eton')

INSERT INTO Addresses
VALUES (1, 'Main Street', 'AB1', 'Aberdeen'), (2, 'High Road', 'CV2', 'Coventry'), (3, 'Curly Avenue', 'B3', 'Birmingham')

INSERT INTO Applicants
VALUES 
    ('Ani', 1, 1, 1, 'Anita', 'Bath', 'Female', '1991-01-01', 'anita1@gmail.com', '+447111111111', 'Oxford', '1st', 1, 1, 1),
    ('Ben', 2, 2, 2, 'Ben', 'Derhover', 'Male', '1992-02-02', 'ben2@gmail.com', '+447222222222', 'Cambridge', '2:2', 1, 0, 1),
    ('Ken', 2, 2, 2, 'Kenny', 'Dewitt', 'Male', '1993-03-03', 'kenny3@gmail.com', '+447333333333', 'Derby', '3rd', 0, 0, 0)

INSERT INTO TechSkill
VALUES ('C#'), ('Java'), ('R'), ('JS'), ('Python'), ('C++')

INSERT INTO TechSelfScore
VALUES
    ('Ani', 1, 5), ('Ani', 3, 4),
    ('Ben', 4, 6), ('Ben', 6, 3), ('Ben', 3, 3),
    ('Ken', 4, 1)

INSERT INTO Strengths
VALUES ('Creative'), ('Decisive'), ('Disciplined'), ('Empathetic')

INSERT INTO ApplicantStrengths
VALUES
    ('Ani', 1), ('Ani', 4),
    ('Ben', 1), ('Ben', 2), ('Ben', 3),
    ('Ken', 4)

INSERT INTO Weaknesses
VALUES ('Impatient'), ('Lazy'), ('Passive'), ('Shy')

INSERT INTO ApplicantWeaknesses
VALUES
    ('Ani', 1),
    ('Ben', 4),
    ('Ken', 2), ('Ken', 3)

INSERT INTO Academy
VALUES ('London'), ('Birmingham');

INSERT INTO SpartaDay
VALUES (1, '2021-11-10'), (2, '2021-11-09'), (1, '2021-11-08'), (2, '2021-11-07')

INSERT INTO ApplicantSpartaDay
VALUES
    ('Ani', 1, 0.6, 0.7),
    ('Ben', 2, 0.8, 0.5),
    ('Ken', 3, 0.3, 0.4)

INSERT INTO Trainer
VALUES ('Daniel', 'Jew'), ('Paula', 'Kedra'), ('John', 'Doe')

INSERT INTO Course
VALUES ('Data 24', 2, '2021-11-10'), ('Engineering 99', 2, '2021-12-10'), ('Data 26', 2, '2022-01-10')

INSERT INTO CourseTrainer
VALUES (1, 1), (2, 2), (3, 3)

INSERT INTO Spartans
VALUES ('Ani', 1), ('Ben', 2)

INSERT INTO CoreSkills
VALUES ('Analytic'), ('Independent'), ('Determined')

INSERT INTO Tracker
VALUES
    (1, 1, 1, 5), (1, 2, 1, 4), (1, 3, 1, 7), (1, 1, 2, 7), (1, 2, 2, 8), (1, 3, 2, 8),
    (2, 1, 1, 3), (2, 2, 1, 6), (2, 3, 1, 5)
