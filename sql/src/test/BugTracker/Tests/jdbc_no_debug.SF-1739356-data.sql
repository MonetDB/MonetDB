debug select count(*) from tables;
trace select count(*) from tables;
profile select count(*) from tables;

-- next is allowed as it doesn't change the output of the statement
record select count(*) from tables;
