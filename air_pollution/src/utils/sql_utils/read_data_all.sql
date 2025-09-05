SELECT
	r.location_id,
    r.sensors_id,
	SUBSTRING_INDEX(r.location, ",", 1) AS `city`,
	SUBSTRING_INDEX(
		SUBSTRING(r.location,
		LOCATE('ul.', r.location),
		99),
	"-",1) AS `street`,
	CAST(datetime AS DATE) `datetime`,
    r.lat,
    r.lon,
    r.o3,
    r.pm10,
    r.pm25,
    r.so2,
    LOWER(v.voivodeship) AS `voivodeship`
FROM
	air_pollution.raw_data r
LEFT JOIN
	air_pollution.cities_voivodeships v
ON
	LOWER(TRIM(SUBSTRING_INDEX(r.location, ",", 1))) = v.city
WHERE
	UPPER(r.location) LIKE "%%,%%UL%%" AND
    UPPER(r.location) NOT LIKE "%%PRAGUE%%"