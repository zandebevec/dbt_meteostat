WITH airport_departures AS (
    SELECT
        origin AS faa, -- Rename origin to faa for consistency
        COUNT(*) AS departure_count, -- Counts all rows for this origin
        SUM(cancelled) AS dep_cancelled_count,
        SUM(diverted) AS dep_diverted_count,
        COUNT(DISTINCT tail_number) AS dep_distinct_tails, -- Correctly counts distinct tails for departures
        COUNT(DISTINCT airline) AS dep_distinct_airlines   -- Correctly counts distinct airlines for departures
    FROM {{ref('prep_flights')}}
    GROUP BY origin -- Group ONLY by origin to get total stats per airport
),
airport_arrivals AS (
    SELECT
        dest AS faa, -- Rename dest to faa for consistency
        COUNT(*) AS arrival_count, -- Counts all rows for this dest
        SUM(cancelled) AS arr_cancelled_count,
        SUM(diverted) AS arr_diverted_count,
        COUNT(DISTINCT tail_number) AS arr_distinct_tails, -- Correctly counts distinct tails for arrivals
        COUNT(DISTINCT airline) AS arr_distinct_airlines   -- Correctly counts distinct airlines for arrivals
    FROM {{ref('prep_flights')}}
    GROUP BY dest -- Group ONLY by dest to get total stats per airport
)
SELECT
    a.faa, -- From prep_airports to ensure we have all airport details
    a.country,
    a.city,
    a.name,
    COALESCE(ad.departure_count, 0) AS count_planned_departures,
    COALESCE(aa.arrival_count, 0) AS count_planned_arrivals,
    (COALESCE(ad.departure_count, 0) + COALESCE(aa.arrival_count, 0)) AS count_planned_total,
    (COALESCE(ad.dep_cancelled_count, 0) + COALESCE(aa.arr_cancelled_count, 0)) AS count_all_cancelled,
    (COALESCE(ad.dep_diverted_count, 0) + COALESCE(aa.arr_diverted_count, 0)) AS count_all_diverted,
    -- This calculation depends on what you mean by 'actual flights'.
    -- If 'actual' means NOT cancelled AND NOT diverted:
    (COALESCE(ad.departure_count, 0) + COALESCE(aa.arrival_count, 0))
    - (COALESCE(ad.dep_cancelled_count, 0) + COALESCE(aa.arr_cancelled_count, 0))
    AS count_actual_flights,
    (COALESCE(ad.dep_distinct_tails, 0) + COALESCE(aa.arr_distinct_tails, 0)) AS total_distinct_planes,
    (COALESCE(ad.dep_distinct_airlines, 0) + COALESCE(aa.arr_distinct_airlines, 0)) AS total_distinct_airlines
FROM
    {{ref('prep_airports')}} a
LEFT JOIN
    airport_departures ad ON a.faa = ad.faa
LEFT JOIN
    airport_arrivals aa ON a.faa = aa.faa