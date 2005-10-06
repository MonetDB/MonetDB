module namespace foo = "http://www.foo.bar/";

(: we may not redefine a variable defined in another module :)
declare variable $foo:bar := 13;
