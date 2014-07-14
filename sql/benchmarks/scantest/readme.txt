sqlplus scantest/scantest@csys:

set verify off
set timing on

@ct1
@ft1
exec ft1(100000)
@st1 10000
etc.



===================================================================
select sum(v1)+sum(v2)+sum(v3)+sum(v4)+sum(v5) from t5;
is faster (0.37 vs 0.45) than
select sum(v1+v2+v3+v4+v5) from t5;

Using sum(v1+v2+v3+v4+v5) as it is a more "realistic" feature measure.

===================================================================
select sum(v1+v2+v3+v4+v5) from t5;
is faster (0.45 vs 0.50) than
select sum(v1+v2+v3+v4+v5) from t5 where rownum < &1;
when doing the same number of rows

Using rownum for easier experimentation.
