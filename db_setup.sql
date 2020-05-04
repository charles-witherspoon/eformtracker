CREATE DATABASE eform_tracker;
USE eform_tracker;
CREATE USER user_test IDENTIFIED BY 'passwd_test';
GRANT ALL ON `eform_tracker`.* TO user_test;
FLUSH PRIVILEGES;
CREATE TABLE eform (
  eform INT PRIMARY KEY,
  pri INT,
  seq INT,
  queue VARCHAR(12),
  added_by VARCHAR(8),
  sent_by VARCHAR(8),
  sent_on TIMESTAMP,
  summary VARCHAR(250),
  workflow ENUM(
    '1-In Review',
    '2-For Approval',
    '3-Approved To Do',
    '3-Hold',
    '4-Project',
    '5-In Progress',
    '6-Work Complete'
  ),
  changed_on TIMESTAMP,
  changed_by VARCHAR(8),
  need_by DATE
);