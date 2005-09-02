(: If a function does not have an explicit type declaration,
   static typing shall use `item*' as the return type. :)
declare function foo ()
{
    42
};

(: Basically we check here if element construction can deal with
   the `item*' type. (see bug 1274903) :)
<foo>{ foo() }</foo>
