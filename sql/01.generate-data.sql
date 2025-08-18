/*
--------------------------------------------------------
CREATE TABLES
--------------------------------------------------------
PURPOSE:
    - Creates RAW tables for importing CSV data (all columns as TEXT to avoid type errors).
    - Creates STAGING tables with proper data types.
    - Loads data from RAW into STAGING with casting and cleaning.

NOTES:
    - All existing RAW and STAGING tables will be dropped and recreated.
    - Dependencies in higher layers (CORE, REPORTING) must be dropped first if they exist.
    - Run this script only when resetting or reloading the base data.

WARNING:
    Proceed with caution: running this script will delete existing RAW and STAGING tables.
    Ensure proper backups or that the data can be regenerated before executing.
*/




-- DROP ALL ENTITIES --------------------------------------
DROP TABLE IF EXISTS raw_users;
DROP TABLE IF EXISTS raw_subscriptions;
DROP TABLE IF EXISTS raw_workouts;

DROP TABLE IF EXISTS stg_workouts;
DROP TABLE IF EXISTS stg_subscriptions;
DROP TABLE IF EXISTS stg_users;



--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--

-- RAW DATA TABLES --------------------------------------
CREATE TABLE raw_users (
    user_id TEXT,
    name TEXT,
	email TEXT,
    signup_date TEXT,
    branch_id TEXT,
    gender TEXT,
	age_group TEXT
);


CREATE TABLE raw_subscriptions (
    subscription_id TEXT,
	user_id TEXT,
    plan TEXT,
    start_date TEXT,
    end_date TEXT,
    price TEXT
);


CREATE TABLE raw_workouts (
    workout_id TEXT,
	user_id TEXT,
    workout_date TEXT,
    workout_time TEXT,
    workout_type TEXT
);




--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--

-- STAGING DATA TABLES --------------------------------------
CREATE TABLE stg_users (
    user_id INT PRIMARY KEY,
    name VARCHAR(100),
	email VARCHAR(100),
    signup_date DATE NOT NULL,
    branch_id INT NOT NULL,
    gender VARCHAR(10) NOT NULL,
	age_group VARCHAR (10) NOT NULL
);


CREATE TABLE stg_subscriptions (
    subscription_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES stg_users(user_id),
    plan VARCHAR(20) CHECK (plan IN ('Basic', 'Standard', 'Pro')),
    start_date DATE NOT NULL,
    end_date DATE,
    price NUMERIC(10,2)
);


CREATE TABLE stg_workouts (
    workout_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES stg_users(user_id),
    workout_date DATE NOT NULL,
    workout_time TIME NOT NULL,
    workout_type VARCHAR(50)
);



--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--

-- POPULATING STAGING TABLES --------------------------------------
INSERT INTO stg_users(user_iD, name, email, signup_date, branch_id, gender, age_group)
SELECT
	user_id::INT,
	name,
	email,
	signup_date::DATE,
	branch_id::INT,
	gender,
	age_group
FROM raw_users;


INSERT INTO stg_subscriptions (user_id, plan, start_date, end_date, price)
SELECT
    user_id::INT,
    plan,
    start_date::DATE,
    NULLIF(end_date, '')::DATE,
    price::NUMERIC
FROM raw_subscriptions;


INSERT INTO stg_workouts (user_id, workout_date, workout_time, workout_type)
SELECT
    user_id::INT,
    workout_date::DATE,
    workout_time::TIME,
    workout_type
FROM raw_workouts;
