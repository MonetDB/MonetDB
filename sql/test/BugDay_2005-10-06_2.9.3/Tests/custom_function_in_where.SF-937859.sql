CREATE TABLE t937859 (id int, s varchar(5));
INSERT INTO t937859 VALUES (1,'a'),(2,'b'),(3,'c'),(4,'aap');
select char_length(a.s)-char_length(b.s) 
from   t937859 a, t937859 b 
where  a.id < b.id 
and    char_length(a.s)-char_length(b.s)=0;
