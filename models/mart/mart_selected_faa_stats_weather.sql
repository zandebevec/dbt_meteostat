WITH calculations AS (SELECT date,
airport_code,
min(min_temp_c) AS min_temp,
max(max_temp_c) AS max_temp,
sum(precipitation_mm) AS all_percip_rain_mm,
sum(max_snow_mm) AS all_snow_mm,
avg(avg_wind_direction) AS avg_wind_direct,
avg(avg_wind_speed_kmh) AS avg_wind_speed_kmh,
avg(wind_peakgust_kmh) AS wind_peak_kmh
FROM {{ref('prep_weather_daily')}}
GROUP BY date, airport_code)
SELECT date, airport_code, country, city, name, count_planned_departures, count_planned_arrivals, count_planned_total, count_all_cancelled, count_all_diverted, total_distinct_planes, total_distinct_airlines
min_temp, max_temp, all_percip_rain_mm, all_snow_mm, avg_wind_direct, avg_wind_speed_kmh, wind_peak_kmh
FROM calculations c
LEFT JOIN {{ref('mart_faa_stats')}} m
ON c.airport_code = m.faa
ORDER BY date