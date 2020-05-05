CREATE DATABASE eform_tracker;
USE eform_tracker;
CREATE USER user_test IDENTIFIED BY 'passwd_test';
GRANT ALL ON `eform_tracker`.* TO user_test;
FLUSH PRIVILEGES;
CREATE TABLE eform (
  Eform INT PRIMARY KEY,
  PRI INT,
  Seq INT,
  Queue VARCHAR(12),
  `Added By` VARCHAR(8),
  `Sent By` VARCHAR(8),
  `Sent On` TIMESTAMP,
  Summary VARCHAR(250),
  Workflow ENUM(
    '1-In Review',
    '2-For Approval',
    '3-Approved To Do',
    '3-Hold',
    '4-Project',
    '5-In Progress',
    '6-Work Complete'
  ),
  `Changed On` TIMESTAMP,
  `Changed By` VARCHAR(8),
  `Need By` DATE
);