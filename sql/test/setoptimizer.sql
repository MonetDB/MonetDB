-- show behavior of optimizer plans
set optimizer='default_pipe'; -- overrule others

select optimizer;
set optimizer='off';
select optimizer;
set optimizer='off';
select optimizer;

set optimizer='on';
select optimizer;
set optimizer='on';
select optimizer;

set optimizer='nov2009_pipe';
select optimizer;

-- and some errors
set optimizer='default,costModel';
select optimizer;

set optimizer='myfamous_pipe';
select optimizer;
