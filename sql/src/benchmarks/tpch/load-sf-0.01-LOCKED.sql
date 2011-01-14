COPY 5 RECORDS INTO region from 'PWD/region.tbl' USING DELIMITERS '|', '|\n' LOCKED;
COPY 25 RECORDS INTO nation from 'PWD/nation.tbl' USING DELIMITERS '|', '|\n' LOCKED;
COPY 100 RECORDS INTO supplier from 'PWD/supplier.tbl' USING DELIMITERS '|', '|\n' LOCKED;
COPY 1500 RECORDS INTO customer from 'PWD/customer.tbl' USING DELIMITERS '|', '|\n' LOCKED;
COPY 2000 RECORDS INTO part from 'PWD/part.tbl' USING DELIMITERS '|', '|\n' LOCKED;
COPY 8000 RECORDS INTO partsupp from 'PWD/partsupp.tbl' USING DELIMITERS '|', '|\n' LOCKED;
COPY 15000 RECORDS INTO orders from 'PWD/orders.tbl' USING DELIMITERS '|', '|\n' LOCKED;
COPY 70000 RECORDS INTO lineitem from 'PWD/lineitem.tbl' USING DELIMITERS '|', '|\n' LOCKED;
