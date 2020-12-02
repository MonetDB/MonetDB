SELECT * FROM version;

insert into version (name ,i) values ('test2' ,2) ;

SELECT insertversion('test3', 3);

SELECT * FROM version;

update version set i = 2 where i = 10;

SELECT updateversion('test1', 4);

SELECT * FROM version;

delete from version where i = 100;

SELECT deleteversion(4);

SELECT * FROM version;

truncate version;

SELECT truncateversion();

SELECT * FROM version;
