select count(*) from DWDATE;
select count(*) from SUPPLIER;
select count(*) from CUSTOMER;
select count(*) from PART;
select count(*) from LINEORDER;

select * from DWDATE    order by D_DATEKEY limit 9;
select * from SUPPLIER  order by S_SUPPKEY limit 9;
select * from CUSTOMER  order by C_CUSTKEY limit 9;
select * from PART      order by P_PARTKEY limit 9;
select * from LINEORDER order by LO_ORDERKEY, LO_LINENUMBER limit 9;
