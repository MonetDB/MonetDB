CREATE FUNCTION test_timestamp_with_nulls(actual_takeoff_datetime timestamp)
RETURNS STRING LANGUAGE PYTHON {
    import json
    return json.dumps(actual_takeoff_datetime.tolist());
};


CREATE TABLE example (
    "column1" timestamp
);

insert into example ("column1") values ('2017-01-01 00:00:01');
insert into example ("column1") values (NULL);

select test_timestamp_with_nulls("column1") from example;

drop table example;
drop function test_timestamp_with_nulls(timestamp);
