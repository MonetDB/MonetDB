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
-- Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
-- Copyright August 2008-2011 MonetDB B.V.
-- All Rights Reserved.


CREATE TYPE xml EXTERNAL NAME xml;
CREATE FUNCTION xml (s STRING) RETURNS xml external name xml.xml;
CREATE FUNCTION str (s XML) RETURNS STRING external name xml.str;
CREATE FUNCTION comment (s STRING) RETURNS xml external name xml.comment;
CREATE FUNCTION parse (doccont STRING, val STRING, "option" STRING) RETURNS xml external name xml.parse;
CREATE FUNCTION pi (nme STRING, val STRING) RETURNS xml external name xml.pi;
CREATE FUNCTION root (val STRING, version STRING, standalone STRING) RETURNS xml external name xml.root;
CREATE FUNCTION attribute (nme STRING, val STRING) RETURNS xml external name xml.attribute;
CREATE FUNCTION "element" (nme STRING, ns xml, attr xml, s xml) RETURNS xml external name xml.element;
CREATE FUNCTION concat (val1 xml, val2 xml) RETURNS xml external name xml.concat;
CREATE FUNCTION forest (val1 xml, val2 xml) RETURNS xml external name xml.forest;
CREATE FUNCTION isdocument (val STRING) RETURNS xml external name xml.isdocument;


