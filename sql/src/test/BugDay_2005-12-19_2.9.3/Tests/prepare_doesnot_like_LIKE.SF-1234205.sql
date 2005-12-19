prepare select name from tables where name like ?;
execute 1 ('%');
prepare select name from tables where name like 's%';
execute 2 ();
