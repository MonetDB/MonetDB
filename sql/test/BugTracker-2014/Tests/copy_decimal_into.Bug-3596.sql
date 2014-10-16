create table load_decimals ( n string, d decimal(10,2));
copy 3 records into load_decimals from STDIN USING DELIMITERS ',','\n';
t1,  0.1  
t2,0.2
t3, 1.1  

select * from load_decimals;
drop table load_decimals;
