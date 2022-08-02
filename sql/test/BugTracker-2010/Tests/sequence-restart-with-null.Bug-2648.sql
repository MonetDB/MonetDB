create sequence testseq_2648 as integer start with 1;
create table testtbl_2648 (id integer);

select next value for testseq_2648;

alter sequence testseq_2648 restart with null no cycle;

alter sequence testseq_2648 restart with (select max(id) from testtbl_2648);

select next value for testseq_2648;

drop table testtbl_2648;
drop sequence testseq_2648;
