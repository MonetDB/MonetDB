CREATE TABLE test (id int, s varchar(5));
INSERT INTO test VALUES (1,'a'),(2,'b'),(3,'c'),(4,'aap');
select char_length(a.s)-char_length(b.s) 
from   test a, test b 
where  a.id < b.id 
and    char_length(a.s)-char_length(b.s)=0;
