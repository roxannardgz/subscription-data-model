/*
--------------------------------------------------------
CREATE REPORTING LAYER
--------------------------------------------------------
PURPOSE:
  - Build the REPORTING layer: business-ready datasets.
  - Create Views (and a few Materialized Views where helpful) that perform joins, calculations, and aggregations.

PREREQUISITES
  - CORE layer has been run.
  - This script assumes the public schema.

NOTES:
    - All existing REPORTING objects will be dropped and recreated.

WARNING:
    Proceed with caution: running this script will delete existing REPORTING objects.
*/




-- DROP ALL ENTITIES --------------------------------------
DROP VIEW IF EXISTS rep_churn_monthly;
DROP VIEW rep_average_workouts_active_user;
DROP VIEW IF EXISTS rep_cohort_churned_users;
DROP MATERIALIZED VIEW IF EXISTS rep_cohort_retention;
DROP MATERIALIZED VIEW IF EXISTS rep_cohort_ltv_user; 
DROP VIEW IF EXISTS rep_cohort_engagement;



-- CHURN --------------------------------------
CREATE VIEW rep_churn_monthly AS
-- churn based on active memberships and users lost for each month
SELECT 
	act.month,
	act.branch_id,
	act.plan,
	act.active_memberships,
	lost.lost_memberships,
	ROUND((COALESCE(lost.lost_memberships, 0)::numeric / NULLIF(act.active_memberships, 0)) * 100, 2) AS churn
FROM
	core_active_memberships_monthly act
LEFT JOIN core_lost_memberships_monthly lost
	ON lost.month = act.month
	AND lost.plan = act.plan
	AND lost.branch_id = act.branch_id
ORDER BY month, branch_id;



	
-- AVERAGE WORKOUTS PER ACTIVE USER -------------------
CREATE VIEW rep_average_workouts_active_user AS
-- Total workouts for each month, branch, and plan
WITH total_workouts AS (
SELECT
	c.month,
	u.branch_id,
	m.plan,
	COUNT(workout_id) AS total_workouts
FROM calendar_months c
LEFT JOIN stg_workouts w
	ON DATE_TRUNC('month', c.month) = DATE_TRUNC('month', w.workout_date)
LEFT JOIN stg_users u
	ON w.user_id = u.user_id
LEFT JOIN core_memberships m
	ON m.user_id = u.user_id
	AND w.workout_date >= m.start_date
	AND (w.workout_date <= m.end_date OR m.end_date IS NULL)
GROUP BY 1, 2, 3
)

SELECT
	tw.month,
	tw.branch_id,
	tw.plan,
	tw.total_workouts,
	mau.active_users,
	ROUND(tw.total_workouts::numeric / mau.active_users, 2) AS avg_workouts
FROM
	total_workouts tw
LEFT JOIN core_mau_monthly mau
	ON tw.month = mau.month
	AND tw.branch_id = mau.branch_id
	AND tw.plan = mau.plan;




-- COHORT RETENTION -------------------
CREATE MATERIALIZED VIEW rep_cohort_retention AS
SELECT
	a.user_id,
	c.plan,
	c.branch_id,
	c.cohort,
	a.activity_month,
	EXTRACT(YEAR FROM AGE(a.activity_month, c.cohort)) * 12 + EXTRACT(MONTH FROM AGE(a.activity_month, c.cohort)) AS cohort_period
FROM 
	core_activity_by_month a
LEFT JOIN core_cohorts c
	ON a.user_id = c.user_id;




-- COHORT LTV -------------------
CREATE MATERIALIZED VIEW rep_cohort_ltv_user AS
WITH monthly_payments AS (
SELECT
	user_id,
	DATE_TRUNC('month', start_date)::DATE AS subscription_month,
	SUM(price) AS payment
FROM
	stg_subscriptions s
GROUP BY
	1, 2
),

cohort_revenue_per_user AS (
SELECT
	c.user_id,
	c.cohort,
	c.branch_id,
	c.plan,
	p.subscription_month,
	EXTRACT(YEAR FROM AGE(p.subscription_month, c.cohort)) * 12 + EXTRACT(MONTH FROM AGE(p.subscription_month, c.cohort)) AS cohort_period,
	payment
FROM
	core_cohorts c
JOIN monthly_payments p
	ON c.user_id = p.user_id
WHERE
    p.subscription_month >= c.cohort
),

aggreggate_cohorts AS (
SELECT
	cohort,
	branch_id,
	plan,
	cohort_period,
	SUM(payment) AS total_revenue_period,
	COUNT(DISTINCT user_id) AS active_users
FROM
	cohort_revenue_per_user
GROUP BY
	1, 2, 3, 4
)

SELECT
	cohort,
	branch_id,
	plan,
	cohort_period,
	total_revenue_period,
	active_users,
	SUM(total_revenue_period) OVER(PARTITION BY cohort, branch_id, plan ORDER BY cohort_period) AS cumulative_cohort_revenue,
	ROUND(total_revenue_period / active_users) AS average_revenue_per_user
FROM 
	aggreggate_cohorts 
ORDER BY
	1, 2, 3, 4;




-- users churned and churn rate
CREATE VIEW rep_cohort_churned_users AS
WITH churned_users AS (
SELECT
	m.user_id,
	DATE_TRUNC('MONTH', end_date)::DATE AS end_month
FROM
	core_memberships m
JOIN stg_users u
	USING (user_id)
WHERE 
	end_date IS NOT NULL
),

churned_cohorts AS (
SELECT
	c.user_id,
	c.cohort,
	c.branch_id,
	c.plan,
	cu.end_month,
	EXTRACT(YEAR FROM AGE(cu.end_month, c.cohort)) * 12 + EXTRACT(MONTH FROM AGE(cu.end_month, c.cohort)) AS cohort_period
FROM
	core_cohorts c
JOIN churned_users cu
	ON c.user_id = cu.user_id
),

users_churned_per_period AS (
SELECT
	cohort,
	branch_id,
	plan,
	cohort_period,
	COUNT(DISTINCT user_id) AS users_churned
FROM
	churned_cohorts
GROUP BY
	1, 2, 3, 4
)

SELECT
	ucp.cohort,
	ucp.branch_id,
	ucp.plan,
	ucp.cohort_period,
	ucp.users_churned,
	acm.active_users
FROM
	users_churned_per_period ucp
JOIN core_active_cohorts_monthly acm
	ON acm.cohort = ucp.cohort
	AND acm.branch_id = ucp.branch_id
	AND acm.plan = ucp.plan
	AND acm.cohort_period = ucp.cohort_period
ORDER BY
	cohort, branch_id, plan, cohort_period;




-- COHORT ENGAGEMENT -------------------
-- Workout engagement metrics per cohort period

CREATE VIEW rep_cohort_engagement AS
-- Initial size of each cohort
WITH initial_cohort_size AS (
    SELECT
        cohort,
        branch_id,
        plan,
        COUNT(DISTINCT user_id) AS initial_users
    FROM
        core_cohorts
    GROUP BY
        1, 2, 3
),

-- Combine all the data
cohort_engagement_summary AS (
SELECT
    cwu.cohort,
    cwu.branch_id,
    cwu.plan,
    cwu.cohort_period,
    cwu.total_workouts,
    cwu.total_users AS active_users_in_period,
    ics.initial_users
FROM
    core_cohort_workouts_users cwu
JOIN initial_cohort_size ics
    ON ics.cohort = cwu.cohort
    AND ics.branch_id = cwu.branch_id
    AND ics.plan = cwu.plan
)

SELECT
    *,
    ROUND(total_workouts::numeric / NULLIF(active_users_in_period, 0), 2) AS avg_workouts_per_active_user,
    ROUND(total_workouts::numeric / NULLIF(initial_users, 0), 2) AS avg_workouts_per_initial_user
FROM
    cohort_engagement_summary
ORDER BY
    cohort, branch_id, plan, cohort_period;


