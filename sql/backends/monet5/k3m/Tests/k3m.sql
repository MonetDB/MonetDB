start transaction;


create table catalog(id int, ra double, decl double);
insert into catalog values (1, 222.3, 79.5 ), (2, 122.3, 88.5), (3, 22.3, 79.5 ), (4, 88.0, 38.0);

create table sourcelist(id int, ra double, decl double);
insert into sourcelist values (11, 22.305, 79.499 ), (12,122.305, 88.499), (13, 222.305, 79.499 );

select * from k3m_free();

select * from k3m_build((select id, ra, decl from catalog as s));
select * from k3m_query((select id, ra, decl, 0.01745329 from sourcelist));

select * from k3m_free();

ROLLBACK;
