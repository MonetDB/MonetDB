start transaction;
create sequence seq;
create sequence seq1 AS int;
create sequence seq2 START WITH 2;
create sequence seq3 INCREMENT BY 3;
create sequence seq4 MINVALUE 4;
create sequence seq5 NO MINVALUE;
create sequence seq6 MAXVALUE 6;
create sequence seq7 NO MAXVALUE;
create sequence seq8 CACHE 8;
create sequence seq9 NO CYCLE;
create sequence seq0 CYCLE;

select name, start, minvalue, maxvalue, increment, cacheinc, cycle from sequences
where name in ('seq', 'seq1', 'seq2', 'seq3', 'seq4', 'seq5', 'seq6', 'seq7', 'seq8', 'seq9', 'seq0');
rollback;
