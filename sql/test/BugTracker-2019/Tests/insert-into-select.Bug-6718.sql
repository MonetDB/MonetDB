CREATE TABLE test2 (
order_number SERIAL,
order_id INTEGER NOT NULL,
qnt INTEGER NOT NULL
);

\d test2

COPY 5 RECORDS INTO test2 FROM stdin USING DELIMITERS '|','\n';
1|32|57
2|15|105
3|33|0
4|57|20
5|67|134
;

SELECT * FROM test2;


CREATE TABLE test1 (
id INTEGER NOT NULL,
qnt INTEGER,
order_number INTEGER NOT NULL,
CONSTRAINT pk_test1_id PRIMARY KEY ("id", "qnt"),
CONSTRAINT pk_order_number FOREIGN KEY ("order_number") REFERENCES 
test2("order_number")
);

\d test1

SELECT test2.order_id, 
test2.qnt, test2.order_number from test2;

INSERT INTO test1 (id, qnt, order_number) SELECT test2.order_id, 
test2.qnt, test2.order_number from test2;

SELECT * FROM test1;


CREATE TABLE test3 (id, qnt, order_number) AS SELECT test2.order_id, 
test2.qnt, test2.order_number from test2;

\d test3

SELECT * FROM test3;

ALTER TABLE test3 ALTER id SET NOT NULL;
ALTER TABLE test3 ALTER order_number SET NOT NULL;
ALTER TABLE test3 ADD CONSTRAINT pk_test3_id PRIMARY KEY ("id", "qnt");
ALTER TABLE test3 ADD CONSTRAINT fk_order_number FOREIGN KEY ("order_number") REFERENCES 
test2("order_number");

\d test3

DROP TABLE test3;
DROP TABLE test1;
DROP TABLE test2;

