-- should not raise an assertion 
create function places3()
returns table( "schema" string,  "table" string, "schema" string)
external name iot.places;

select * from places3();

