-- crashes:
create table crash_me10 (UNION int not null);
select substring('abcd' from 2 for 2);

-- hangs:
select replace('abbaab','ab','ba');
select timestampadd(SQL_TSI_SECOND,1,'1997-01-01 00:00:00');
select concat('a','b','c','d');
select elt(2,'ONE','TWO','THREE');
select field('IBM','NCA','ICL','SUN','IBM','DIGITAL');
select greatest('HARRY','HARRIOT','HAROLD');
select least('HARRY','HARRIOT','HAROLD');
select lpad('hi',4,'??');
select rpad('hi',4,'??');
select translate('abc','bc','de');
select rpad('abcd',2,'+-',8);
select lpad('abcd',2,'+-',8);
select timestamp('19630816','00200212');

