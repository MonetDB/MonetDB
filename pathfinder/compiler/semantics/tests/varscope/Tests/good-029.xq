import module namespace foo = "http://www.foo.bar/" at "good-029-mod.xq";

(: we may, of course, use variables defined in the module :)
$foo:bar
