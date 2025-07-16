WITH airport_flights AS (
			SELECT flight_date, origin, name, city, country,
			sum(cancelled) AS sum_cancelled,
			sum(diverted) AS sum_diverted
FROM {{ref('prep_airports')}}
JOIN {{ref('prep_flights')}}
ON faa = origin
GROUP BY flight_date, origin, name, city, country)
SELECT flight_date, origin, name, city, country,
			sum_cancelled,
			sum_diverted,
			avg(avg_temp_c) AS avg_temp_c,
			avg(precipitation_mm) AS avg_precipitation_mm,
			avg(max_snow_mm) AS max_snow_mm,
			avg(avg_wind_speed_kmh) AS avg_wind_speed_kmh
FROM airport_flights
right JOIN {{ref('prep_weather_daily')}}
ON airport_code = ORIGIN
GROUP BY flight_date, origin, name, city, country, sum_cancelled, sum_diverted