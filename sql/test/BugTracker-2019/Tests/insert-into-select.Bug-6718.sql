CREATE TABLE test2 (
order_number SERIAL,
order_id INTEGER NOT NULL,
qnt INTEGER NOT NULL
);

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

SELECT test2.order_id, 
test2.qnt, test2.order_number from test2;

INSERT INTO test1 (id, qnt, order_number) SELECT test2.order_id, 
test2.qnt, test2.order_number from test2;

SELECT * FROM test1;

DROP TABLE test1;
DROP TABLE test2;

