create table starships(class string, speed int, flux int);
copy into sys.starships from '/home/niels/data/rc/clean/sql/benchmarks/tpch/fileleak/Tests/starships.csv.noheader.bz2' using delimiters ',','\r\n';

select "location","count" from storage() where "table"='starships';
delete from starships;
select "location","count" from storage() where "table"='starships';
