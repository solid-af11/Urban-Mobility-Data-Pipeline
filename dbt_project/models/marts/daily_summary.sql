SELECT
    DATE_TRUNC('day', pickup_time) as trip_day,
    COUNT(*) as total_trips,
    SUM(fare) as daily_revenue
FROM {{ ref('stg_trips') }}
GROUP BY 1