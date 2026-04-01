WITH
-- Collect account-level dimensions
cte_account_activity AS (
    SELECT
        s.date,
        sp.country,
        a.id AS account_id,
        a.send_interval,
        a.is_verified,
        a.is_unsubscribed
    FROM `DA.account` a
    JOIN `DA.account_session` acs
        ON a.id = acs.account_id
    JOIN `DA.session` s
        ON acs.ga_session_id = s.ga_session_id
    JOIN `DA.session_params` sp
        ON acs.ga_session_id = sp.ga_session_id
),

-- Collect email events with account and country attributes
cte_email_activity AS (
    SELECT
        DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS date,
        sp.country,
        a.id AS account_id,
        a.send_interval,
        a.is_verified,
        a.is_unsubscribed,
        es.id_message AS sent_message_id,
        eo.id_message AS opened_message_id,
        ev.id_message AS visited_message_id
    FROM `DA.email_sent` es
    JOIN `DA.account_session` acs
        ON es.id_account = acs.account_id
    JOIN `DA.session` s
        ON acs.ga_session_id = s.ga_session_id
    LEFT JOIN `DA.email_open` eo
        ON es.id_message = eo.id_message
    LEFT JOIN `DA.email_visit` ev
        ON es.id_message = ev.id_message
    JOIN `DA.account` a
        ON acs.account_id = a.id
    JOIN `DA.session_params` sp
        ON s.ga_session_id = sp.ga_session_id
),

-- Combine account and email metrics
cte_union_metrics AS (
    SELECT
        date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        COUNT(DISTINCT account_id) AS created_accounts,
        0 AS sent_emails,
        0 AS opened_emails,
        0 AS visited_emails
    FROM cte_account_activity
    GROUP BY date, country, send_interval, is_verified, is_unsubscribed

    UNION ALL

    SELECT
        date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        0 AS created_accounts,
        COUNT(DISTINCT sent_message_id) AS sent_emails,
        COUNT(DISTINCT opened_message_id) AS opened_emails,
        COUNT(DISTINCT visited_message_id) AS visited_emails
    FROM cte_email_activity
    GROUP BY date, country, send_interval, is_verified, is_unsubscribed
),

-- Aggregate metrics into one row per grain
cte_daily_metrics AS (
    SELECT
        date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        SUM(created_accounts) AS created_accounts,
        SUM(sent_emails) AS sent_emails,
        SUM(opened_emails) AS opened_emails,
        SUM(visited_emails) AS visited_emails
    FROM cte_union_metrics
    GROUP BY date, country, send_interval, is_verified, is_unsubscribed
),

-- Add country totals
cte_country_totals AS (
    SELECT
        date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        created_accounts,
        sent_emails,
        opened_emails,
        visited_emails,
        SUM(created_accounts) OVER (PARTITION BY country) AS total_country_created_accounts,
        SUM(sent_emails) OVER (PARTITION BY country) AS total_country_sent_emails
    FROM cte_daily_metrics
),

-- Add country ranks
cte_country_ranks AS (
    SELECT *,
        DENSE_RANK() OVER (ORDER BY total_country_created_accounts DESC) AS rank_by_created_accounts,
        DENSE_RANK() OVER (ORDER BY total_country_sent_emails DESC) AS rank_by_sent_emails
    FROM cte_country_totals
)

SELECT
    date,
    country,
    send_interval,
    CASE
        WHEN is_verified = 1 THEN 'Verified'
        ELSE 'Not Verified'
    END AS verification_status,
    CASE
        WHEN is_unsubscribed = 1 THEN 'Unsubscribed'
        ELSE 'Subscribed'
    END AS subscription_status,
    created_accounts,
    sent_emails,
    opened_emails,
    visited_emails,
    total_country_created_accounts,
    total_country_sent_emails,
    rank_by_created_accounts,
    rank_by_sent_emails
FROM cte_country_ranks
WHERE rank_by_created_accounts <= 10 OR rank_by_sent_emails <= 10
ORDER BY date
