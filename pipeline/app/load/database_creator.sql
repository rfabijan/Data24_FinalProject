/*
This Script will create a Data24ETL Database
It will create the necessary tables
*/

-- 1. Create the Database
USE master;
DROP DATABASE IF EXISTS [Data24ETL];
CREATE DATABASE [Data24ETL];
USE [Data24ETL];


-- 2. Build the tables
CREATE TABLE TechSkill
(
    TechSkillID             INT PRIMARY KEY,
    SkillName               VARCHAR(32) NOT NULL
)

CREATE TABLE Strengths
(
    StrengthID              INT PRIMARY KEY,
    Strength                VARCHAR(32) NOT NULL
)

CREATE TABLE Weaknesses
(
    WeaknessID              INT PRIMARY KEY,
    Weakness                VARCHAR(32) NOT NULL
)

CREATE TABLE Academy
(
    AcademyID               INT PRIMARY KEY,
    AcademyName             VARCHAR(64) NOT NULL
)

CREATE TABLE SpartaDay
(
    SpartaDayID             INT PRIMARY KEY,
    AcademyID               INT NOT NULL FOREIGN KEY REFERENCES Academy(AcademyID),
    SpartaDayDate           DATE
)

CREATE TABLE Trainer
(
    TrainerID               INT PRIMARY KEY,
    FirstName               VARCHAR(64) NOT NULL,
    LastName                VARCHAR(64)
)

CREATE TABLE Course
(
    CourseID                INT PRIMARY KEY,
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
    CoreSkillID             INT PRIMARY KEY,
    SkillName               VARCHAR(16) NOT NULL
)

CREATE TABLE Streams
(
    StreamID                INT PRIMARY KEY,
    StreamName              VARCHAR(64) NOT NULL
)

CREATE TABLE Invitors
(
    InvitorID               INT PRIMARY KEY,
    FirstName               VARCHAR(32) NOT NULL,
    LastName                VARCHAR(32)
)

CREATE TABLE Addresses
(
    AddressID               INT PRIMARY KEY,
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
    PhoneNumber             VARCHAR(12),
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

    ApplicantID             VARCHAR(64) FOREIGN KEY REFERENCES Applicants(ApplicantID),
    SpartanID               INT PRIMARY KEY,
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
