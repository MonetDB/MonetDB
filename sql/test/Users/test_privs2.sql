SELECT * FROM version;

insert into version (name ,i) values ('test2' ,2) ;

SELECT insertversion('test3', 3);

SELECT * FROM version;

SELECT updateversion('test1', 4);

SELECT * FROM version;

SELECT deleteversion(4);

SELECT * FROM version;
