CREATE TABLE nodes (id serial, p GEOMETRY(POINT));
insert into nodes (id, p) values (45111955, 'POINT(52.0697 4.3723)');
COPY 1 RECORDS INTO nodes from STDIN USING DELIMITERS ',', '\n';
45111956, 'POINT(52.0697 4.3723)'

drop table nodes;
