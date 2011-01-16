
create table scene (id int, victim_id int, anme string);
create table victim (id int, name string);


select v1.name
from scene s, scene b, victim v1, victim v2 where
not v1.id=s.victim_id and v1.id=b.victim_id;

drop table scene;
drop table victim;
