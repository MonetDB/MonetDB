CREATE TABLE "sensor_readings_2678" (
        "src_ip"    VARCHAR(15),
        "recv_time" TIMESTAMP,
        "emit_time" TIMESTAMP,
        "location"  VARCHAR(30),
        "type"      VARCHAR(30),
        "value"     VARCHAR(30)
);

SELECT
	CAST(emit_time AS date) AS "date",
	CAST(avg(CAST(value AS numeric(5,2))) AS numeric(5,2)) AS avgtemperature
FROM sensor_readings_2678
WHERE "type" LIKE 'temperature'
	AND location LIKE 'L318'
GROUP BY "date"
ORDER BY "date";
