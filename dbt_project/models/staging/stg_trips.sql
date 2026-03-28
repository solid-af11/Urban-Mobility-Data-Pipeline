SELECT
    CAST(tpep_pickup_datetime AS TIMESTAMP) as pickup_time,
    CAST(trip_distance AS FLOAT) as distance,
    CAST(total_amount AS FLOAT) as fare,
    "PULocationID" as location_id
FROM {{ source('nyc_raw', 'raw_nyc_trips') }}
WHERE trip_distance > 0 AND total_amount > 0