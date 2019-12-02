SELECT str_to_date('23-09-1987', '%d-%m-%Y') AS "date",
       str_to_time('11:40', '%H:%M') AS "time",
       str_to_timestamp('23-09-1987 11:40', '%d-%m-%Y %H:%M') AS "timestamp";

EXPLAIN SELECT str_to_date('23-09-1987', '%d-%m-%Y') AS "date",
       str_to_time('11:40', '%H:%M') AS "time",
       str_to_timestamp('23-09-1987 11:40', '%d-%m-%Y %H:%M') AS "timestamp";
