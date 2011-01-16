create table test_property(subject integer, p1 integer, p2 integer, unique(subject, p1), unique(subject, p2));
\D
drop table test_property;
