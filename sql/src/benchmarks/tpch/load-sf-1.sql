COPY 150000 RECORDS INTO customer from 'DIR/customer.tbl' USING DELIMITERS '|', '|\n';
COPY 25 RECORDS INTO nation from 'DIR/nation.tbl' USING DELIMITERS '|', '|\n';
COPY 1500000 RECORDS INTO orders from 'DIR/orders.tbl' USING DELIMITERS '|', '|\n';
COPY 800000 RECORDS INTO partsupp from 'DIR/partsupp.tbl' USING DELIMITERS '|', '|\n';
COPY 200000 RECORDS INTO part from 'DIR/part.tbl' USING DELIMITERS '|', '|\n';
COPY 5 RECORDS INTO region from 'DIR/region.tbl' USING DELIMITERS '|', '|\n';
COPY 10000 RECORDS INTO supplier from 'DIR/supplier.tbl' USING DELIMITERS '|', '|\n';
COPY 7000000 RECORDS INTO lineitem from 'DIR/lineitem.tbl' USING DELIMITERS '|', '|\n';
COMMIT;
