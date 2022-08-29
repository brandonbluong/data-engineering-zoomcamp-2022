SELECT 
	COUNT(*) num_trips
FROM yellow_taxi_data
WHERE tpep_pickup_datetime::date = '2021-01-15';