-- Simple MonetDB C UDF interface

-- The arguments are named after the signature
-- The return variable is called 'ret'

create function one() returns integer language C { *ret = 1; return NULL; }
create function addtwo(i integer, j integer) returns integer language C { *ret = *i + *j; return NULL;}
create function adderror(i integer, j integer) returns integer language C { if ( *i == int_nil || *j == int_nil) throw(SQL,"udf","Nil not allowed"); *ret = *i + *j; return NULL;}

select one();
select addtwo(one(),one());
select adderror(one(),null);
