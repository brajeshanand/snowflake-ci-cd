CREATE OR ALTER SCHEMA DEV_SCHEMA;

CREATE OR ALTER TABLE DEV_SCHEMA.EMPLOYEES (
    ID INT AUTOINCREMENT,
    NAME STRING,
    ROLE STRING,
    DEPARTMENT STRING,
    JOIN_DATE DATE
);

CREATE OR ALTER VIEW DEV_SCHEMA.EMPLOYEE_VIEW AS
SELECT 
    ID, 
    NAME, 
    ROLE, 
    DEPARTMENT, 
    JOIN_DATE
FROM 
    DEV_SCHEMA.EMPLOYEES;

