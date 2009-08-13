create table number (isanumber integer);
COPY 2 RECORDS INTO number FROM stdin USING DELIMITERS '\n';
1
bla
drop table number;
