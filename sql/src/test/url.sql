
CREATE TYPE url EXTERNAL NAME url;

--CREATE table url_test ( theUrl url, name string )
--declare local temporary table ...
--INSERT into url_test values ( url 'http://monetdb.cwi.nl/', 'MonetDB')
--select * from url_test;

CREATE function getAnchor( theUrl url ) RETURNS STRING 
	EXTERNAL NAME 'getAnchor';
CREATE function getBasename(theUrl url) RETURNS STRING       
	EXTERNAL NAME 'getBasename';
CREATE function getContent(theUrl url)   RETURNS STRING       
	EXTERNAL NAME 'getContent';
CREATE function getContext(theUrl url)   RETURNS STRING       
	EXTERNAL NAME 'getContext';
CREATE function getDomain(theUrl url) RETURNS STRING       
	EXTERNAL NAME 'getDomain';
CREATE function getExtension(theUrl url) RETURNS STRING       
	EXTERNAL NAME 'getExtension';
CREATE function getFile(theUrl url) RETURNS STRING       
	EXTERNAL NAME 'getFile';
CREATE function getHost(theUrl url)   RETURNS STRING       
	EXTERNAL NAME 'getHost';
CREATE function getPort(theUrl url) RETURNS STRING       
	EXTERNAL NAME 'getPort';
CREATE function getProtocol(theUrl url) RETURNS STRING       
	EXTERNAL NAME 'getProtocol';
CREATE function getQuery(theUrl url) RETURNS STRING       
	EXTERNAL NAME 'getQuery';
CREATE function getUser(theUrl url) RETURNS STRING       
	EXTERNAL NAME 'getUser';
CREATE function getRobotURL(theUrl url) RETURNS STRING       
	EXTERNAL NAME 'getRobotURL';
CREATE function isaURL(theUrl url) RETURNS BOOL
	EXTERNAL NAME 'isaURL';
CREATE function newurl(protocol STRING, hostname STRING,port INT, file STRING) 
	RETURNS url       
	EXTERNAL NAME 'newurl';
CREATE function newurl(protocol STRING, hostname STRING, file STRING) 
	RETURNS url 
	EXTERNAL NAME 'newurl';
