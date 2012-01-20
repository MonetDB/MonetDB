create table t(u url);
insert into t values('http://www.cwi.nl/~mk/vision2011.pdf');
insert into t values('http://www.monetdb.com?x=2');
insert into t values('http://www.monetdb.org:8080/Documentation/Manuals/SQLreference/Datamanipulation');


select getAnchor(u) from t;

select getBasename(u) from t;

select getContent(u) from t;

select getContext(u) from t;

select getDomain(u) from t;

select getExtension(u) from t;

select getFile(u) from t;

select getHost(u) from t;

select getPort(u) from t;

select getQuery(u) from t;

select getUser(u) from t;

select getRobotURL(u) from t;

select isaURL(u) from t;

select isaURL('http://www.monetdb.org');
select isaURL('http://www.monetdb.org');
select isaURL('htp:///www.monetdb.org');

select newurl('https','www.monetdb.org',8080,'blah');
select newurl('https','localhost','boe');

drop table t;
