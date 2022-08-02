start transaction;
create schema schema1;
create schema schema2;
create table schema1.basetable(id serial);
create table schema1.childtable(id serial, fk int references schema1.basetable(id));
create table schema2.childtable(id serial, fk int references schema1.basetable(id));

rollback;
