# Question 1. Knowing docker tags
--iidfile string

# Question 2. Understanding docker first run
3

# Question 3. Count records
20530

```
SELECT 
CAST(lpep_pickup_datetime AS DATE) AS pickup_date,
CAST(lpep_dropoff_datetime AS DATE) AS dropoff_date,
COUNT(*)
FROM green_taxi_trips
GROUP BY
CAST(lpep_pickup_datetime AS DATE),
CAST(lpep_dropoff_datetime AS DATE)
ORDER BY 
pickup_date ASC, 
dropoff_date ASC;
```

# Question 4. Largest trip for each day
2019-01-15
```
SELECT 
CAST(lpep_pickup_datetime AS DATE) AS pickup_date,
CAST(lpep_dropoff_datetime AS DATE) AS dropoff_date,
trip_distance
FROM green_taxi_trips
ORDER BY
trip_distance DESC;
```

# Question 5. The number of passengers

2: 1282 ; 3: 254

```
WITH temp AS (
SELECT 
CAST(lpep_pickup_datetime AS DATE) AS pickup_date,
CAST(lpep_dropoff_datetime AS DATE) AS dropoff_date,
passenger_count,
COUNT(1) AS count
FROM green_taxi_trips
GROUP BY
CAST(lpep_pickup_datetime AS DATE),
CAST(lpep_dropoff_datetime AS DATE),
passenger_count)

SELECT 
passenger_count,
SUM(count)
FROM temp 
WHERE
(
pickup_date = '2019-01-01'
OR
dropoff_date = '2019-01-01'
)	
AND
(passenger_count = 2
OR
passenger_count = 3) 
GROUP BY
passenger_count;

```

# Question 6. Largest tip
Long Island City/Queens Plaza
```
SELECT 
zpu."Zone" AS pick_up_zone,
zdo."Zone" AS drop_off_zone,
t.tip_amount
FROM green_taxi_trips AS t
LEFT JOIN 
zone zpu
ON
t."PULocationID" = zpu."LocationID"
JOIN
zone zdo
ON
t."DOLocationID" = zdo."LocationID"
WHERE 
zpu."Zone" = 'Astoria'
ORDER BY
tip_amount DESC

```