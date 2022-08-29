SELECT
	tpep_pickup_datetime::date pickup_date,
	MAX(tip_amount) largest_tip
FROM yellow_taxi_data
GROUP BY tpep_pickup_datetime::date
ORDER BY largest_tip DESC
LIMIT 1;