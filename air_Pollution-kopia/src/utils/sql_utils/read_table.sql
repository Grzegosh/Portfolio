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
    so2
FROM
	air_pollution.raw_data
WHERE
	UPPER(location) LIKE "%%,%%UL%%" AND
    UPPER(location) NOT LIKE "%%PRAGUE%%"






