-- next statement would crash mserver5 (before the fix: https://dev.monetdb.org/hg/MonetDB?cmd=changeset;node=59538edd0f12)
CREATE TABLE mytable FROM LOADER non_existing_loader();

