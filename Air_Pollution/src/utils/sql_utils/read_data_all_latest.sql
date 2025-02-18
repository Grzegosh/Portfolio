SELECT
	r.location_id,
    r.sensors_id,
    r.city,
    r.street,
    r.`datetime`,
    r.lat,
    r.lon,
    r.o3,
    r.pm10,
    r.pm25,
    r.so2,
    LOWER(v.voivodeship) as `voivodeship`
FROM
(

SELECT
	location_id,
    sensors_id,
	SUBSTRING_INDEX(location, ",", 1) AS `city`,
	SUBSTRING_INDEX(
		SUBSTRING(location,
		LOCATE('ul.', location),
		99),
	"-",1) AS `street`,
	CAST(datetime AS DATE) `datetime`,
    lat,
    lon,
    o3,
    pm10,
    pm25,
    so2,
    RANK() OVER(PARTITION BY location_id, sensors_id ORDER BY datetime DESC) AS `rnk`
FROM
	air_pollution.raw_data
WHERE
	UPPER(location) LIKE "%%,%%UL%%" AND
    UPPER(location) NOT LIKE "%%PRAGUE%%"
) AS r
LEFT JOIN
	air_pollution.cities_voivodeships v
ON
	LOWER(TRIM(r.city)) = LOWER(TRIM(v.city))
WHERE
	r.rnk = 1




