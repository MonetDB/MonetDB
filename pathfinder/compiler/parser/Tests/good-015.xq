xquery version "1.0" encoding "iso-8859-15";

declare boundary-space strip;
declare default collation "foo";
declare base-uri "http://www.base-uri.de/";
declare construction preserve;
declare ordering ordered;
declare default order empty least;
declare copy-namespaces preserve, inherit;
declare namespace foo = "http://www.foo.de/";

import schema "myschema" at "http://www.myschema.org/";

declare function myfun () { 42 };
declare variable $myvar := 42;

17
