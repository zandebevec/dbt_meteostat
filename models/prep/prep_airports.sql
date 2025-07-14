WITH airports_reorder AS (
    SELECT faa
    	   ,country
    	   ,region
    	   ,city
    	   ,name
    	   ,lat
    	   ,lon
    	   ,alt
    	   ,tz
    	   ,dst
    FROM {{ref('staging_airports')}}
)
SELECT * FROM airports_reorder