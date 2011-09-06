-- The contents of this file are subject to the MonetDB Public License
-- Version 1.1 (the "License"); you may not use this file except in
-- compliance with the License. You may obtain a copy of the License at
-- http://www.monetdb.org/Legal/MonetDBLicense
--
-- Software distributed under the License is distributed on an "AS IS"
-- basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
-- License for the specific language governing rights and limitations
-- under the License.
--
-- The Original Code is the MonetDB Database System.
--
-- The Initial Developer of the Original Code is CWI.
-- Copyright August 2008-2011 MonetDB B.V.
-- All Rights Reserved.

CREATE TYPE url EXTERNAL NAME url;

CREATE function getAnchor( theUrl url ) RETURNS STRING 
	EXTERNAL NAME url."getAnchor";
CREATE function getBasename(theUrl url) RETURNS STRING       
	EXTERNAL NAME url."getBasename";
CREATE function getContent(theUrl url)   RETURNS STRING       
	EXTERNAL NAME url."getContent";
CREATE function getContext(theUrl url)   RETURNS STRING       
	EXTERNAL NAME url."getContext";
CREATE function getDomain(theUrl url) RETURNS STRING       
	EXTERNAL NAME url."getDomain";
CREATE function getExtension(theUrl url) RETURNS STRING       
	EXTERNAL NAME url."getExtension";
CREATE function getFile(theUrl url) RETURNS STRING       
	EXTERNAL NAME url."getFile";
CREATE function getHost(theUrl url)   RETURNS STRING       
	EXTERNAL NAME url."getHost";
CREATE function getPort(theUrl url) RETURNS STRING       
	EXTERNAL NAME url."getPort";
CREATE function getProtocol(theUrl url) RETURNS STRING       
	EXTERNAL NAME url."getProtocol";
CREATE function getQuery(theUrl url) RETURNS STRING       
	EXTERNAL NAME url."getQuery";
CREATE function getUser(theUrl url) RETURNS STRING       
	EXTERNAL NAME url."getUser";
CREATE function getRobotURL(theUrl url) RETURNS STRING       
	EXTERNAL NAME url."getRobotURL";
CREATE function isaURL(theUrl url) RETURNS BOOL
	EXTERNAL NAME url."isaURL";
CREATE function newurl(protocol STRING, hostname STRING, "port" INT, file STRING) 
	RETURNS url       
	EXTERNAL NAME url."new";
CREATE function newurl(protocol STRING, hostname STRING, file STRING) 
	RETURNS url 
	EXTERNAL NAME url."new";
