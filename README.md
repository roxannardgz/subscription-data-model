# Reporting Data Model for Subscription Analysis
*Data model with PostgreSQL for cohort, retention, and engagement analysis for Momentum Fitness.*

<br><br>
> [!NOTE]
> This project focuses on the **reporting data model** and **transformation layer** of an analytics pipeline. It demonstrates SQL data modeling and uses industry best practices in data engineering and analytics.
<br>


## Goal
Design and implement a PostgreSQL-based reporting data model that transforms raw subscription and workout data into clean, reusable tables and business-ready metrics for dashboards. 


## Overview
Momentum Fitness is a national gym chain offering group classes. Founded in 2021, and with 4 branches, Momentum currently serves around 4000 active users. The company offers tiered membership plans (Basic, Standard, Pro) and tracks member activity through workout check-ins. The company wants better **retention** and **engagement** insights via monthly dashboards.



This project includes:
- Data Architecture 
- ETL Pipelines
- Data Modeling
- Analytics and Reporting
- Documentation


## Data Architecture
Layered data warehouse model (raw → staging → core → reporting) to transform raw subscription and workout data into reusable datasets and business-ready metrics for dashboards.

![Data Architecture](https://github.com/roxannardgz/subscription-data-model/blob/main/diagrams/architecture-momentum.png)


- **Raw layer** stores unprocessed data exactly as received from source systems.
- **Staging layer** cleans and standardizes formats for consistent joins.
- **Core layer** consolidates and enriches the data into reusable tables.
- **Reporting layer** contains final metrics tables and views used by dashboards.


## Key Metrics & Definitions
- **Churn % (monthly)**  
  `lost_memberships / active_memberships_opening * 100`, where  
  - `active_memberships_opening` = members active at the **start** of month  
  - `lost_memberships` = members whose membership ended **in** month
- **MAU (Monthly Active Users)**  
  Distinct users with ≥1 workout in the month, joined to active memberships for branch/plan alignment.
- **Avg Workouts per Active User**  
  `total_workouts_in_month / MAU`.
- **Cohort (month)**  
  First membership month per user.
- **Cohort Period (months since cohort)**  
  `months_between(activity_month, cohort_month)`.
- **Cohort LTV**  
  Revenue aggregated by cohort & period; cumulative LTV uses window sum.  
  (Assumes subscription prices reflect monthly billing.)


## Data Sources
The data used is synthetic, generated using this Python script. It simulates real data from different sources, accounting for seasonality and other characteristics such as the probablity for skipping workouts.

- `users` → CRM / auth system (Salesforce, HubSpot, Auth0).
- `subscriptions` → Billing/ERP system (Stripe/Recurly or membership system).
- `workouts` → Axtivity logs / IoT (class bookings, check-ins, equipment IoT).
- `calendar_months` → Generated warehouse dimension.


## Assumptions and Limitations
- Synthetic data; simplified membership rules.
- Single active plan per user at a time.
- End date `NULL` = still active.
- Timezone & date boundaries simplified to month-level logic.

## Future Work
- **dbt** refactor (tests, docs, sources; `stg/`, `core/`, `marts/`).
- BI dashboards in **Tableau/Looker**.
  

## How to Replicate Locally
Follow these steps to set up and run the data model on your machine.
1. **Generate data:** Run the python script to generate the syntethic CSV files.
2. **Database setup:** Create a database in PostgreSQL, using a tool like pgAdmin or psql.
4. **Build raw/stg tables:** Run `sql/01_generate_data.sql` to create raw/stg tables.
   - In a tool like pgAdmin, open and run the `sql/01_generate_data.sql` script.
   - Manually import the CSV files from the data/ folder into the `raw_*` tables.
   - After importing, run the INSERT statements at the bottom of the same script to populate the staging tables.
6. **Build core and reporting layers:** Run `sql/02_core_models.sql` then `sql/03_reporting_models.sql`.




