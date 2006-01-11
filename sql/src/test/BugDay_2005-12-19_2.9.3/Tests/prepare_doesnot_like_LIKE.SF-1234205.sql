prepare select name from tables where name like ?;
execute 0 ('%');
prepare select name from tables where name like 's%';
execute 1 ();
