CREATE VIEW IF NOT EXISTS air_pollution.raw_data_view AS

SELECT * FROM
	air_pollution.raw_data
WHERE
	load_date = (SELECT MAX(load_date) FROM air_pollution.raw_data);