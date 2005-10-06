module namespace foo = "http://www.foo.bar/";

(: a module must not see the variables exported by another module :)
declare variable $foo:baz := $foo:bar;
