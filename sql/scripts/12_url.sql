-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

CREATE TYPE url EXTERNAL NAME url;

CREATE function getAnchor( theUrl url ) RETURNS STRING
	EXTERNAL NAME url."getAnchor";
GRANT EXECUTE ON FUNCTION getAnchor(url) TO PUBLIC;
CREATE function getBasename(theUrl url) RETURNS STRING
	EXTERNAL NAME url."getBasename";
GRANT EXECUTE ON FUNCTION getBasename(url) TO PUBLIC;
CREATE function getContent(theUrl url)   RETURNS STRING
	EXTERNAL NAME url."getContent";
GRANT EXECUTE ON FUNCTION getContent(url) TO PUBLIC;
CREATE function getContext(theUrl url)   RETURNS STRING
	EXTERNAL NAME url."getContext";
GRANT EXECUTE ON FUNCTION getContext(url) TO PUBLIC;
CREATE function getDomain(theUrl url) RETURNS STRING
	EXTERNAL NAME url."getDomain";
GRANT EXECUTE ON FUNCTION getDomain(url) TO PUBLIC;
CREATE function getExtension(theUrl url) RETURNS STRING
	EXTERNAL NAME url."getExtension";
GRANT EXECUTE ON FUNCTION getExtension(url) TO PUBLIC;
CREATE function getFile(theUrl url) RETURNS STRING
	EXTERNAL NAME url."getFile";
GRANT EXECUTE ON FUNCTION getFile(url) TO PUBLIC;
CREATE function getHost(theUrl url)   RETURNS STRING
	EXTERNAL NAME url."getHost";
GRANT EXECUTE ON FUNCTION getHost(url) TO PUBLIC;
CREATE function getPort(theUrl url) RETURNS STRING
	EXTERNAL NAME url."getPort";
GRANT EXECUTE ON FUNCTION getPort(url) TO PUBLIC;
CREATE function getProtocol(theUrl url) RETURNS STRING
	EXTERNAL NAME url."getProtocol";
GRANT EXECUTE ON FUNCTION getProtocol(url) TO PUBLIC;
CREATE function getQuery(theUrl url) RETURNS STRING
	EXTERNAL NAME url."getQuery";
GRANT EXECUTE ON FUNCTION getQuery(url) TO PUBLIC;
CREATE function getUser(theUrl url) RETURNS STRING
	EXTERNAL NAME url."getUser";
GRANT EXECUTE ON FUNCTION getUser(url) TO PUBLIC;
CREATE function getRobotURL(theUrl url) RETURNS STRING
	EXTERNAL NAME url."getRobotURL";
GRANT EXECUTE ON FUNCTION getRobotURL(url) TO PUBLIC;
CREATE function isaURL(theUrl url) RETURNS BOOL
	EXTERNAL NAME url."isaURL";
GRANT EXECUTE ON FUNCTION isaURL(url) TO PUBLIC;
CREATE function newurl(protocol STRING, hostname STRING, "port" INT, file STRING)
	RETURNS url
	EXTERNAL NAME url."new";
GRANT EXECUTE ON FUNCTION newurl(STRING, STRING, INT, STRING) TO PUBLIC;
CREATE function newurl(protocol STRING, hostname STRING, file STRING)
	RETURNS url
	EXTERNAL NAME url."new";
GRANT EXECUTE ON FUNCTION newurl(STRING, STRING, STRING) TO PUBLIC;
