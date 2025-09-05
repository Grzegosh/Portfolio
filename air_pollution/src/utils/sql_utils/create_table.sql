CREATE TABLE IF NOT EXISTS raw_data(
location_id INT,
sensord_id INT,
location VARCHAR(255),
`datetime` timestamp,
lat DECIMAL(9,6),
lon DECIMAL(9,6),
o3 FLOAT,
pm10 FLOAT,
pm25 FLOAT,
so2 FLOAT,
load_date INT
);