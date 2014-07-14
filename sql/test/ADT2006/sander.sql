create table suspect ( name varchar(100), picture_uri
varchar(200), notes varchar(500), victim_name
varchar(100), primary key (name));

insert into suspect (name, picture_uri, notes,
victim_name) values ("Dr. Thomas Neill Cream ",
"http://blabla/toolong", "did commit murders, but by
poisoning", "");

select * from suspect;

insert into suspect (name, picture_uri, notes,
victim_name) values ("Sander Borsboom", "lives
centuries later, so not much chance", "blah.jpg",
"noone (yet :D)");

select * from suspect;

drop table suspect;
