
create table content (id integer primary key);
create table test_ref (id integer primary key, contentid integer references content);
create table test_foreign (id integer primary key, contentid integer, foreign key(contentid) references content);

drop table test_foreign;
drop table test_ref;
drop table content;
