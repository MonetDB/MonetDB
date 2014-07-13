start transaction;

-- create our table to test with
create table like_test (
	v1 varchar
);

-- insert some interesting values
insert into like_test values ('a math assignment');
insert into like_test values ('pathfinder is fun!');

-- issue a select with a like query, this should only match the first one!
select * from like_test where v1 like '%math%';

select * from like_test where v1 like 'a%math%';
select * from like_test where v1 like 'a_math%';
select * from like_test where v1 like '%m_th_a%t';
select * from like_test where v1 like '%in%_!';

-- clean up mess we made
rollback;
