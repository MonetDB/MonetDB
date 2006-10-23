
create table lineitem ( l_orderkey INTEGER NOT NULL,
l_partkey INTEGER NOT NULL, l_suppkey INTEGER
NOT NULL, l_linenumber INTEGER NOT NULL, l_quantity
DECIMAL(15,2) NOT NULL, l_extendedprice DECIMAL(15,2)
NOT NULL, l_discount DECIMAL(15,2) NOT NULL, l_tax
DECIMAL(15,2) NOT NULL, l_returnflag CHAR(1)
NOT NULL, l_linestatus CHAR(1) NOT NULL, l_shipdate
DATE NOT NULL, l_commitdate DATE NOT NULL,
l_receiptdate dATE NOT NULL, l_shipinstruct CHAR(25)
NOT NULL, l_shipmode CHAR(10) NOT NULL, l_comment
VARCHAR(44) NOT NULL);

COPY 1 RECORDS INTO LINEITEM FROM stdin USING DELIMITERS '|','|\n';
1|156|4|1|17|17954.55|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|blithely regular ideas caj|

select * from lineitem;
COPY 1 RECORDS INTO LINEITEM FROM stdin USING DELIMITERS '|','|\n';
1|156|4|1|17.0|17954.55|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|blithely regular ideas caj|

select * from lineitem;
COPY 1 RECORDS INTO LINEITEM FROM stdin USING DELIMITERS '|','|\n';
1|156|4|1|17.00|17954.55|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|blithely regular ideas caj|

select * from lineitem;
COPY 1 RECORDS INTO LINEITEM FROM stdin USING DELIMITERS '|','|\n';
1|156|4|1|17.000|17954.55|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|blithely regular ideas caj|

drop table lineitem;
