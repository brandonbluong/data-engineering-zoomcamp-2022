SELECT 
	CONCAT(
		COALESCE(puzones."Zone", 'Unknown'),
		'/',
		COALESCE(dozones."Zone", 'Unknown')) as pickup_dropoff_zone,
	AVG(total_amount) as avg_price
FROM yellow_taxi_data as taxi
LEFT JOIN zones as puzones
ON taxi."PULocationID" = puzones."LocationID"
LEFT JOIN zones as dozones
ON taxi."DOLocationID" = dozones."LocationID"
GROUP BY 1
ORDER BY avg_price DESC
LIMIT 1;