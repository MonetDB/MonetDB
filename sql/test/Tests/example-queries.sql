insert into example values( 1, 'test', 0.1 );
insert into example values( 5, 'xtest', 0.9 );
insert into example values( 9, 'testx', 9.1 );
insert into example values( 0, 'texst', 99999.1 );
insert into example values( 8, 'texxst', 0.0001 );

select * from example order by val1, val2;

select * from example_view order by val1, val2;
