create table number (isanumber integer);
COPY 2 RECORDS INTO number FROM stdin USING DELIMITERS E'\n';
1
bla
drop table number;
