/*
--------------------------------------------------------
CREATE CORE LAYER
--------------------------------------------------------
PURPOSE:
  - Build the CORE layer: reusable datasets.
  - Create Materialized Views that perform joins, calculations, and aggregations.
  - Create the `calendar_months` dimension (physical table) derived from staging.

PREREQUISITES
  - STAGING layer is populated (`stg_users`, `stg_subscriptions`, `stg_workouts`).
  - This script assumes the public schema.

NOTES:
    - All existing CORE objects will be dropped and recreated.
    - Dependencies in higher layers (REPORTING) must be dropped first if they exist.

WARNING:
    Proceed with caution: running this script will delete existing CORE objects.
*/




-- DROP ALL ENTITIES --------------------------------------
DROP MATERIALIZED VIEW IF EXISTS core_mau_monthly;
DROP MATERIALIZED VIEW IF EXISTS core_cohort_workouts_users;
DROP MATERIALIZED VIEW IF EXISTS core_lost_memberships_monthly;
DROP MATERIALIZED VIEW IF EXISTS core_active_memberships_monthly;
DROP MATERIALIZED VIEW IF EXISTS core_active_memberships_during_month;
DROP MATERIALIZED VIEW IF EXISTS core_active_cohorts_monthly;
DROP MATERIALIZED VIEW IF EXISTS core_cohorts;
DROP MATERIALIZED VIEW core_memberships;
DROP MATERIALIZED VIEW IF EXISTS core_activity_by_month;
DROP TABLE IF EXISTS calendar_months;




-- CALENDAR MONTHS (table) --------------------------------------
CREATE TABLE calendar_months AS
-- months between min and max in the data, plus month name and year
WITH generate_series AS (
	SELECT generate_series(
		    (SELECT MIN(DATE_TRUNC('month', start_date)) FROM stg_subscriptions),
		    (SELECT MAX(DATE_TRUNC('month', COALESCE(end_date, '2025-02-28'))) FROM stg_subscriptions),
		    INTERVAL '1 month'
	)::DATE AS month
)

SELECT
	month,
	EXTRACT(YEAR FROM month)::INT AS year,
	TO_CHAR(month, 'Month') AS month_name
FROM generate_series;




--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--
-- ALL MEMBERSHIPS (MV) -------------------
CREATE MATERIALIZED VIEW core_memberships AS
-- continuous memberships for each user 
SELECT
	user_id,
	plan,
	MIN(start_date) AS start_date,
	CASE WHEN MAX(end_date) > '2025-02-28' THEN NULL
		ELSE MAX(end_date) 
	END AS end_date,
	SUM(price) AS ltv
FROM stg_subscriptions
GROUP BY 1, 2;



--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--
-- CHURN
--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--

-- ACTIVE MEMBERSHIPS (MV) -------------------
CREATE MATERIALIZED VIEW core_active_memberships_monthly AS 
-- number of users with active membership at the beginning of each month
SELECT
	c.month,
	u.branch_id,
	m.plan,
	COUNT(DISTINCT m.user_id) AS active_memberships
FROM
	calendar_months c
LEFT JOIN core_memberships m
	ON m.start_date < c.month
	AND (m.end_date IS NULL OR m.end_date >= c.month)
LEFT JOIN stg_users u
	ON m.user_id = u.user_id
GROUP BY 1, 2, 3;



-- LOST MEMBERSHIPS (MV) -------------------
CREATE MATERIALIZED VIEW core_lost_memberships_monthly AS 
-- number of users that canceled their memberships each month
SELECT 
	c.month,
	u.branch_id,
	m.plan,
	COUNT(DISTINCT m.user_id) AS lost_memberships
FROM
	calendar_months c
LEFT JOIN core_memberships m
	ON DATE_TRUNC('month', m.end_date) = c.month
LEFT JOIN stg_users u
	ON m.user_id = u.user_id
GROUP BY 1, 2, 3;


--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--
-- ENGAGEMENT
--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--

-- ACTIVE MEMBERSHIPS EOM (MV) -------------------
CREATE MATERIALIZED VIEW core_active_memberships_during_month AS 
-- number of users with active membership during each month
SELECT
	c.month,
	u.branch_id,
	m.plan,
	COUNT(DISTINCT m.user_id) AS active_memberships
FROM
	calendar_months c
LEFT JOIN core_memberships m
	ON DATE_TRUNC('month', m.start_date) <= DATE_TRUNC('month', c.month)
	AND (m.end_date IS NULL OR m.end_date >= c.month)
LEFT JOIN stg_users u
	ON m.user_id = u.user_id
GROUP BY 1, 2, 3;




-- MAU: MONTHLY ACTIVE USERS (Materialized View) -------------------
CREATE MATERIALIZED VIEW core_mau_monthly AS
-- ative users and active memberships per month for each branch and plan
WITH mau AS (
SELECT
	c.month AS month,
	u.branch_id,
	m.plan,
	COUNT(DISTINCT w.user_id) AS active_users
FROM 
	calendar_months c
LEFT JOIN stg_workouts w
	ON DATE_TRUNC('month', w.workout_date) = c.month
LEFT JOIN stg_users u
	ON w.user_id = u.user_id
JOIN core_memberships m
	ON w.user_id = m.user_id 
	AND w.workout_date >= m.start_date 
	AND (w.workout_date <= m.end_date OR m.end_date IS NULL)
GROUP BY 
	c.month,
	u.branch_id,
	m.plan
)

SELECT
	mau.*,
	act.active_memberships
FROM mau
LEFT JOIN core_active_memberships_during_month act
	ON act.month = mau.month
	AND act.branch_id = mau.branch_id
	AND act.plan = mau.plan;
	


--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--
-- COHORTS
--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--

-- AVERAGE WORKOUTS PER ACTIVE USER (MV) -------------------
CREATE MATERIALIZED VIEW core_cohorts AS 
-- Cohort by first subscription month (start of membership)
SELECT
	m.user_id,
	DATE_TRUNC('MONTH', start_date)::DATE AS cohort,
	u.branch_id,
	m.plan
FROM
	core_memberships m
JOIN stg_users u
	ON m.user_id = u.user_id;


-- TOTAL WORKOUTS PER ACTIVE USER MONTHLY (MV) -------------------
DROP MATERIALIZED VIEW IF EXISTS core_activity_by_month;
-- Total number of workouts each user had each month
CREATE MATERIALIZED VIEW core_activity_by_month AS 
SELECT
	user_id,
	DATE_TRUNC('MONTH', workout_date)::DATE AS activity_month,
	COUNT(*) AS workouts_done
FROM stg_workouts
GROUP BY 1, 2;



-- COHORT AVERAGE WORKOUTS (MV) -------------------
CREATE MATERIALIZED VIEW core_cohort_workouts_users AS
--Average number of workouts in each period for each cohort
SELECT
	c.cohort,
	c.plan,
	c.branch_id,
	EXTRACT(YEAR FROM AGE(a.activity_month, c.cohort)) * 12 + EXTRACT(MONTH FROM AGE(a.activity_month, c.cohort)) AS cohort_period,
	SUM(a.workouts_done) AS total_workouts,
	COUNT(DISTINCT a.user_id) AS total_users
FROM 
	core_activity_by_month a
LEFT JOIN core_cohorts c
	ON c.user_id = a.user_id
GROUP BY 1, 2, 3, 4;



-- COHORT CHURN (MV) -------------------
CREATE MATERIALIZED VIEW core_active_cohorts_monthly AS
-- active users for each cohort in each period (denominator in churn calc)
WITH all_possible_activity AS (
    SELECT
        c.user_id,
        c.cohort,
        c.branch_id,
        c.plan,
        cm.month AS calendar_month
    FROM
        core_cohorts c
    CROSS JOIN
        calendar_months cm
    WHERE
        cm.month >= c.cohort
),

user_monthly_status AS (
    SELECT
        a.*,
        CASE
            WHEN m.user_id IS NOT NULL THEN 1
            ELSE 0
        END AS is_active
    FROM
        all_possible_activity a
    LEFT JOIN
        core_memberships m
        ON a.user_id = m.user_id
        AND a.calendar_month >= m.start_date
        AND (a.calendar_month <= DATE_TRUNC('month', m.end_date) OR m.end_date IS NULL)
)

SELECT
    cohort,
    branch_id,
    plan,
    EXTRACT(YEAR FROM AGE(calendar_month, cohort)) * 12 + EXTRACT(MONTH FROM AGE(calendar_month, cohort)) AS cohort_period,
    SUM(is_active) AS active_users
FROM
    user_monthly_status
WHERE
    is_active = 1 -- Only count active rows
GROUP BY
    1, 2, 3, 4
ORDER BY
    cohort, branch_id, plan, cohort_period;

