create table slice_test (x int, y int, val int);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

explain select x,y from slice_test limit 1;

explain select cast(x as string)||'-bla-'||cast(y as string) from slice_test limit 1;

drop table slice_test;

