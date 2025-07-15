WITH calculations AS 
(SELECT origin, dest,
		count(*) AS count_flights,
		count(DISTINCT tail_number) AS count_unique_planes,
		count(DISTINCT airline) AS count_unique_airline,
		avg(actual_elapsed_time) AS avg_actual_elapsed_time,
		avg(arr_delay) AS avg_arrival_delay,
		max(arr_delay) AS max_arr_delay,
		min(arr_delay) AS min_arr_delay,
		sum(cancelled) AS count_cancelled,
		sum(diverted) AS count_diverted
FROM {{ref('prep_flights')}}
GROUP BY origin, dest)
SELECT origin, a.city as origin_city, a.country as origin_country, a.name as origin_name, dest, a2.city as dest_city, a2.country as dest_country, a2.name as dest_name, count_flights, count_unique_planes,
count_unique_airline, avg_actual_elapsed_time, avg_arrival_delay, max_arr_delay, min_arr_delay, count_cancelled, count_diverted
FROM calculations
LEFT JOIN {{ref('prep_airports')}} a ON origin = a.faa
LEFT JOIN {{ref('prep_airports')}} a2 ON dest = a2.faa