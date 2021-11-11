USE [Data24ETLTest]

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
