-- show behavior of optimizer plans

select optimizer;
set optimizer='off';
select optimizer;
set optimizer='on';
select optimizer;
set optimizer='off';
set optimizer='default';
select optimizer;
set optimizer='default,costModel';
