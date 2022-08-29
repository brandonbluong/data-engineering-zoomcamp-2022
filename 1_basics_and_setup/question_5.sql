SELECT
    COALESCE(dozones."Zone", 'Unknown') as zone,
    COUNT(*) as num_trips
FROM yellow_taxi_data as taxi

INNER JOIN zones as puzones
ON taxi."PULocationID" = puzones."LocationID"

LEFT JOIN zones as dozones
ON taxi."DOLocationID" = dozones."LocationID"

WHERE
    puzones."Zone" ilike '%central park%'
    AND tpep_pickup_datetime::date = '2021-01-14'
GROUP BY 1
ORDER BY num_trips DESC
LIMIT 1;