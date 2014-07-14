start transaction;

create table "t951206" (name varchar(1024), isloco boolean);
insert into "t951206" values ('niels', false), ('fabian', true),
('martin', false);

select x from (select count(*) as x from "t951206" where "isloco" = true) as t;
select x from (select name as x from "t951206" where "isloco" = true) as t;
