
CREATE TYPE xml EXTERNAL NAME xml;
CREATE FUNCTION xml (s STRING) RETURNS xml external name xml.xml;
CREATE FUNCTION str (s XML) RETURNS STRING external name xml.str;
CREATE FUNCTION "comment" (s STRING) RETURNS xml external name xml.comment;
CREATE FUNCTION parse (val STRING, "option" STRING) RETURNS xml external name xml.parse;
CREATE FUNCTION pi (nme STRING, val STRING) RETURNS xml external name xml.pi;
CREATE FUNCTION root (val STRING, version STRING, standalone STRING) RETURNS xml external name xml.root;
CREATE FUNCTION attribute (nme STRING, val STRING) RETURNS xml external name xml.attribute;
CREATE FUNCTION "element" (nme STRING, ns STRING, attr xml, s xml) RETURNS xml external name xml.element;
CREATE FUNCTION concat (val1 xml, val2 xml) RETURNS xml external name xml.concat;
CREATE FUNCTION forest (val1 xml, val2 xml) RETURNS xml external name xml.forest;
CREATE FUNCTION isdocument (val STRING) RETURNS xml external name xml.isdocument;
CREATE AGGREGATE "xmlagg"( x xml ) RETURNS xml external name xml.agg;
