include MapiLib

m = mapi_connect("localhost", 50000, "monetdb", "monetdb", "sql","demo")

q = mapi_prepare(m, "select * from tables;");
r = mapi_execute(q);

while (nrcols = mapi_fetch_row(q)) > 0
	0.step(nrcols, 1) { |i| p=mapi_fetch_field(q, i); print p; print "\t" }
	print "\n"
end
