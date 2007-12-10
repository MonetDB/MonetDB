

create function str_to_date(s string, format string) returns date
	external name mtime."str_to_date";

create function date_to_str(d date, format string) returns string
	external name mtime."date_to_str";
