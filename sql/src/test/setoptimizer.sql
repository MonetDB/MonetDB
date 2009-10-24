-- show behavior of optimizer plans

select optimizer;
set optimizer='off';
select optimizer;
set optimizer='off';
select optimizer;

set optimizer='on';
select optimizer;
set optimizer='on';
select optimizer;

set optimizer='mitosis_pipe';
select optimizer;

-- and some errors
set optimizer='default,costModel';
select optimizer;

set optimizer='myfamous_pipe';
select optimizer;
